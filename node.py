"""
AutoMesh — Node process

Phase 1: Peer Discovery + Heartbeat + Failure Detection.
Phase 2: Link-State Routing — nodes declare weighted links to their direct
neighbors (not a full mesh), flood that as link-state advertisements (LSAs,
same idea as OSPF), build a shared topology graph, and run Dijkstra to get
a real multi-hop routing table. Failure detection from Phase 1 now triggers
automatic reconvergence: pull the dead node's edges out of the graph and
recompute, with timing logged.

Every node is the exact same code — identity comes from env vars only.
There is no central server: BOOTSTRAP_URL is just "the first peer I happen
to talk to", not an authority. Once a node has joined, it is a peer like
any other, and other nodes could just as easily bootstrap through it.

Run one process per node (see run_local.sh for a 5-node local demo).
"""

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

import httpx
import networkx as nx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("automesh")

# ---- identity & config (all via env vars so every node runs the same code) ----
NODE_ID = os.environ["NODE_ID"]
NODE_URL = os.environ["NODE_URL"]
BOOTSTRAP_URL = os.environ.get("BOOTSTRAP_URL")  # unset => this node IS an entry point
PORT = int(os.environ.get("PORT", "8000"))

# Direct weighted links to neighbors, e.g. LINKS='{"B": 10, "C": 5}'
# (weight = simulated latency in ms). This is the actual topology —
# separate from known_peers, which is just "who's out there" for discovery.
try:
    OWN_LINKS: Dict[str, float] = json.loads(os.environ.get("LINKS", "{}"))
except json.JSONDecodeError as e:
    log.error(f"LINKS env var is not valid JSON ({e}); starting with no links")
    OWN_LINKS = {}

HEARTBEAT_INTERVAL = 2.0   # seconds between outgoing heartbeats
DEAD_THRESHOLD = 6.0       # no heartbeat for this long => DEAD
CHECK_INTERVAL = 1.0       # how often the failure detector sweeps
LSA_INTERVAL = 3.0         # how often we (re-)flood our link-state advertisement

# ---- in-memory state (per-node, no shared/central store) ----
known_peers: Dict[str, str] = {NODE_ID: NODE_URL}   # node_id -> base url
last_seen: Dict[str, float] = {NODE_ID: time.time()}
status: Dict[str, str] = {NODE_ID: "ALIVE"}

# lsa_db[node_id] = {"links": {neighbor_id: weight}, "seq": int}
# This is every node's view of "what links does X advertise" — merge these
# together across all nodes and you get the full topology graph.
lsa_db: Dict[str, Dict] = {NODE_ID: {"links": OWN_LINKS, "seq": 0}}

# routing_table[dest_id] = {"next_hops": [...], "cost": ..., "paths": [[...],...]}
# next_hops has >1 entry exactly when ECMP applies (multiple paths tie for
# lowest cost) — that's the "multi-path routing" feature.
routing_table: Dict[str, Dict] = {}
last_convergence_ms: Optional[float] = None
last_convergence_reason: str = "startup"

# link_load[neighbor_id] = a decaying counter of recent traffic sent that way.
# Used to pick among ECMP candidates by "least loaded" instead of always the
# same one — this is what makes it load-aware rather than plain round-robin.
link_load: Dict[str, float] = {}
LOAD_DECAY = 0.75          # multiply every neighbor's load by this each tick
LOAD_DECAY_INTERVAL = 2.0  # seconds between decay ticks
messages_forwarded = 0
messages_delivered = 0
messages_dropped = 0
drop_reasons: Dict[str, int] = {}  # categorized so this stays diagnosable, not a pile of one-off strings
convergence_count = 0  # how many times recompute_routes has run, total

# ---- failure injection state ----
# Simulating "down" via a flag (rather than actually killing the process)
# means the node keeps its memory and can be revived instantly for repeatable
# demos/tests, while still behaving exactly like a real outage to everyone
# else: it stops sending, and stops responding (503) to anything incoming.
SIMULATED_DOWN = False
last_failure_event: Optional[Dict] = None  # last injected fault, for /debug


class JoinRequest(BaseModel):
    node_id: str
    url: str


class HeartbeatMsg(BaseModel):
    node_id: str
    known_peers: Dict[str, str] = {}


class LSAMsg(BaseModel):
    lsa_db: Dict[str, Dict] = {}


class MessagePacket(BaseModel):
    source: str
    destination: str
    payload: str = ""
    ttl: int = 10
    trace: List[str] = []


def merge_lsa(incoming: Dict[str, Dict]) -> bool:
    """Adopt any link-state advertisement we haven't seen (or a newer seq of
    one we have). Returns True if our view of the topology changed, which
    means it's time to recompute routes."""
    changed = False
    for nid, entry in incoming.items():
        current = lsa_db.get(nid)
        if current is None or entry.get("seq", 0) > current.get("seq", -1):
            lsa_db[nid] = entry
            changed = True
    return changed


def recompute_routes(reason: str) -> None:
    """Rebuild the topology graph from lsa_db (skipping anything we currently
    believe is DEAD) and compute, for every reachable destination, ALL paths
    tied for lowest cost (not just one) — that's Equal-Cost Multi-Path.
    This is what makes the network self-healing: call this after any
    topology change (new LSA) or liveness change (failure detected) and
    traffic automatically reroutes around the problem."""
    global routing_table, last_convergence_ms, last_convergence_reason, convergence_count
    start = time.perf_counter()

    graph = nx.Graph()
    graph.add_node(NODE_ID)
    for nid, entry in lsa_db.items():
        if status.get(nid) == "DEAD":
            continue  # don't route through/to nodes we believe are down
        for neighbor, weight in entry.get("links", {}).items():
            if status.get(neighbor) == "DEAD":
                continue
            # A link only exists if BOTH endpoints currently advertise it.
            # This matters for single-sided failure injection: if C kills
            # its link to B, B's LSA might still list C for a moment (or
            # until B independently notices) -- requiring agreement means
            # C's side alone is enough to pull the edge out immediately,
            # exactly like a real point-to-point interface going down.
            neighbor_entry = lsa_db.get(neighbor)
            if neighbor_entry and nid in neighbor_entry.get("links", {}):
                graph.add_edge(nid, neighbor, weight=weight)

    new_table: Dict[str, Dict] = {}
    if NODE_ID in graph:
        lengths = nx.single_source_dijkstra_path_length(graph, NODE_ID, weight="weight")
        for dest, cost in lengths.items():
            if dest == NODE_ID:
                continue
            try:
                # every path tied for the minimum weight, not just the first one found
                tied_paths = list(nx.all_shortest_paths(graph, NODE_ID, dest, weight="weight"))
            except nx.NetworkXNoPath:
                continue
            next_hops = sorted({p[1] for p in tied_paths if len(p) > 1})
            if not next_hops:
                continue
            new_table[dest] = {
                "next_hops": next_hops,
                "cost": cost,
                "paths": tied_paths,
            }

    routing_table = new_table
    elapsed_ms = (time.perf_counter() - start) * 1000
    last_convergence_ms = elapsed_ms
    last_convergence_reason = reason
    convergence_count += 1
    ecmp_count = sum(1 for r in new_table.values() if len(r["next_hops"]) > 1)
    log.info(
        f"[{NODE_ID}] routes recomputed in {elapsed_ms:.2f}ms "
        f"(reason={reason}, {len(new_table)} destinations, {ecmp_count} with ECMP)"
    )


def choose_next_hop(dest: str) -> Optional[str]:
    """Pick which neighbor to forward toward `dest` through. If there's only
    one shortest-cost option, use it. If multiple tie (ECMP), pick whichever
    currently has the least recent traffic — load-aware distribution."""
    route = routing_table.get(dest)
    if not route:
        return None
    candidates = route["next_hops"]
    if len(candidates) == 1:
        return candidates[0]
    return min(candidates, key=lambda h: link_load.get(h, 0.0))


async def load_decay_loop() -> None:
    """Recent traffic should matter more than traffic from a while ago, so
    load counters decay geometrically instead of growing forever."""
    while True:
        await asyncio.sleep(LOAD_DECAY_INTERVAL)
        for nid in list(link_load.keys()):
            link_load[nid] *= LOAD_DECAY
            if link_load[nid] < 0.01:
                link_load[nid] = 0.0


def merge_peers(new_peers: Dict[str, str]) -> None:
    """Gossip-merge: adopt any peer we didn't already know about."""
    for nid, url in new_peers.items():
        if nid not in known_peers:
            known_peers[nid] = url
            last_seen[nid] = time.time()
            status[nid] = "ALIVE"
            log.info(f"[{NODE_ID}] learned new peer {nid} -> {url}")


async def join_network() -> None:
    """Contact one bootstrap peer to learn the current network topology."""
    if not BOOTSTRAP_URL:
        log.info(f"[{NODE_ID}] starting as an entry point (no bootstrap set)")
        return

    async with httpx.AsyncClient(timeout=5.0) as client:
        for attempt in range(10):
            try:
                resp = await client.post(
                    f"{BOOTSTRAP_URL}/join",
                    json={"node_id": NODE_ID, "url": NODE_URL},
                )
                resp.raise_for_status()
                merge_peers(resp.json()["known_peers"])
                log.info(f"[{NODE_ID}] joined network, known peers: {list(known_peers)}")
                return
            except Exception as e:
                log.warning(f"[{NODE_ID}] join attempt {attempt + 1}/10 failed: {e}")
                await asyncio.sleep(1.0)
    log.error(f"[{NODE_ID}] could not reach bootstrap {BOOTSTRAP_URL} after 10 attempts")


async def heartbeat_loop() -> None:
    """Periodically ping every known peer and gossip our peer table along."""
    async with httpx.AsyncClient(timeout=3.0) as client:
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            if SIMULATED_DOWN:
                continue
            for nid, url in list(known_peers.items()):
                if nid == NODE_ID:
                    continue
                try:
                    resp = await client.post(
                        f"{url}/heartbeat",
                        json={"node_id": NODE_ID, "known_peers": known_peers},
                    )
                    resp.raise_for_status()
                    merge_peers(resp.json().get("known_peers", {}))
                except Exception:
                    pass  # silence is handled by the failure detector below


async def failure_detector_loop() -> None:
    """Independently sweep last_seen timestamps and flip ALIVE/DEAD.
    Any change in liveness immediately triggers a routing reconvergence —
    this is the actual self-healing mechanism."""
    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        now = time.time()
        changed_node = None
        for nid in list(known_peers.keys()):
            if nid == NODE_ID:
                continue
            age = now - last_seen.get(nid, 0)
            if age > DEAD_THRESHOLD and status.get(nid) != "DEAD":
                status[nid] = "DEAD"
                changed_node = nid
                log.warning(f"[{NODE_ID}] {nid} marked DEAD (no heartbeat for {age:.1f}s)")
            elif age <= DEAD_THRESHOLD and status.get(nid) == "DEAD":
                status[nid] = "ALIVE"
                changed_node = nid
                log.info(f"[{NODE_ID}] {nid} recovered")
        if changed_node:
            recompute_routes(reason=f"liveness change: {changed_node}")


async def lsa_flood_loop() -> None:
    """Periodically send our whole lsa_db to our direct link neighbors.
    Simplified link-state flooding: real OSPF only forwards what changed
    and uses sequence numbers to prevent loops forever, but for a 5-node
    demo network, "just resend everything you know periodically" converges
    fine and is much easier to reason about."""
    async with httpx.AsyncClient(timeout=3.0) as client:
        while True:
            await asyncio.sleep(LSA_INTERVAL)
            if SIMULATED_DOWN:
                continue
            for neighbor_id, weight in list(OWN_LINKS.items()):
                url = known_peers.get(neighbor_id)
                if not url:
                    continue
                try:
                    resp = await client.post(f"{url}/lsa", json={"lsa_db": lsa_db})
                    resp.raise_for_status()
                    if merge_lsa(resp.json().get("lsa_db", {})):
                        recompute_routes(reason="new LSA learned")
                except Exception:
                    pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    await join_network()
    recompute_routes(reason="startup")
    hb_task = asyncio.create_task(heartbeat_loop())
    fd_task = asyncio.create_task(failure_detector_loop())
    lsa_task = asyncio.create_task(lsa_flood_loop())
    decay_task = asyncio.create_task(load_decay_loop())
    yield
    hb_task.cancel()
    fd_task.cancel()
    lsa_task.cancel()
    decay_task.cancel()


app = FastAPI(title=f"AutoMesh Node {NODE_ID}", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # local demo only -- browser dashboard runs on a different port
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/join")
async def join(req: JoinRequest):
    """A new node introduces itself. We add it and hand back our full peer table."""
    if SIMULATED_DOWN:
        raise HTTPException(status_code=503, detail="node is simulated down")
    known_peers[req.node_id] = req.url
    last_seen[req.node_id] = time.time()
    status[req.node_id] = "ALIVE"
    log.info(f"[{NODE_ID}] {req.node_id} joined via /join")
    return {"known_peers": known_peers}


@app.post("/heartbeat")
async def heartbeat(msg: HeartbeatMsg):
    if SIMULATED_DOWN:
        raise HTTPException(status_code=503, detail="node is simulated down")
    was_dead = status.get(msg.node_id) == "DEAD"
    last_seen[msg.node_id] = time.time()
    status[msg.node_id] = "ALIVE"
    merge_peers(msg.known_peers)
    if was_dead:
        # heartbeat() beats failure_detector_loop to flipping this status,
        # so if we don't recompute here, the "recovered" branch in the
        # detector loop never actually fires (status is already ALIVE by
        # the time it looks) and routes silently never come back.
        recompute_routes(reason=f"heartbeat received from recovered node {msg.node_id}")
    return {"known_peers": known_peers}


@app.post("/lsa")
async def receive_lsa(msg: LSAMsg):
    """A neighbor is flooding us their view of the topology. Merge it, and
    if anything changed, recompute our own routes. Return our own view back
    so the exchange is bidirectional in one round trip."""
    if SIMULATED_DOWN:
        raise HTTPException(status_code=503, detail="node is simulated down")
    if merge_lsa(msg.lsa_db):
        recompute_routes(reason="LSA received")
    return {"lsa_db": lsa_db}


@app.post("/message")
async def receive_message(pkt: MessagePacket):
    """Real packet forwarding, hop by hop, using our local routing table.
    This is where ECMP + load-aware distribution actually do something:
    every hop with multiple equal-cost next-hops picks the least-loaded one
    independently."""
    global messages_forwarded, messages_delivered, messages_dropped
    if SIMULATED_DOWN:
        raise HTTPException(status_code=503, detail="node is simulated down")
    pkt.trace.append(NODE_ID)

    def drop(category: str, detail_reason: str):
        global messages_dropped
        messages_dropped += 1
        drop_reasons[category] = drop_reasons.get(category, 0) + 1
        return {"delivered": False, "reason": detail_reason, "trace": pkt.trace}

    if pkt.destination == NODE_ID:
        messages_delivered += 1
        log.info(f"[{NODE_ID}] delivered message from {pkt.source}, trace={pkt.trace}")
        return {"delivered": True, "trace": pkt.trace}

    if pkt.ttl <= 0:
        log.warning(f"[{NODE_ID}] dropped message {pkt.source}->{pkt.destination}: TTL expired")
        return drop("ttl_expired", "ttl_expired")

    next_hop = choose_next_hop(pkt.destination)
    if not next_hop:
        return drop("no_route", "no_route")

    # Try every ECMP candidate in least-loaded order, not just the first
    # pick -- if a node's failure detector hasn't caught up to a crash yet
    # (independent clocks across nodes can be a second or so out of sync),
    # this fails over to a surviving path at forward-time instead of
    # bubbling up whatever error the dead hop returned.
    candidates = sorted(routing_table[pkt.destination]["next_hops"], key=lambda h: link_load.get(h, 0.0))
    pkt.ttl -= 1
    last_error = "no_route"
    last_category = "no_route"
    async with httpx.AsyncClient(timeout=5.0) as client:
        for hop in candidates:
            url = known_peers.get(hop)
            if not url:
                last_error, last_category = "next_hop_unreachable", "next_hop_unreachable"
                continue
            try:
                resp = await client.post(f"{url}/message", json=pkt.model_dump())
                if resp.status_code == 200:
                    link_load[hop] = link_load.get(hop, 0.0) + 1.0
                    messages_forwarded += 1
                    return resp.json()
                last_error, last_category = f"next_hop_returned_{resp.status_code}", "next_hop_error"
            except Exception as e:
                last_error, last_category = f"forward_failed: {e}", "forward_failed"

    return drop(last_category, last_error)


@app.post("/admin/kill")
async def admin_kill():
    """Simulate this node crashing: stop sending AND stop responding to
    everything, without losing in-memory state, so it can be revived
    instantly for repeatable demos. From every other node's point of view
    this is indistinguishable from a real process crash."""
    global SIMULATED_DOWN, last_failure_event
    SIMULATED_DOWN = True
    last_failure_event = {"type": "node_kill", "node": NODE_ID, "at": time.time()}
    log.warning(f"[{NODE_ID}] *** SIMULATED FAILURE INJECTED (node down) ***")
    return {"node_id": NODE_ID, "simulated_down": True}


@app.post("/admin/revive")
async def admin_revive():
    """Bring a simulated-down node back. Other nodes will notice on their
    next successful heartbeat and flip it back to ALIVE automatically."""
    global SIMULATED_DOWN, last_failure_event
    SIMULATED_DOWN = False
    last_failure_event = {"type": "node_revive", "node": NODE_ID, "at": time.time()}
    last_seen[NODE_ID] = time.time()
    log.info(f"[{NODE_ID}] *** REVIVED ***")
    return {"node_id": NODE_ID, "simulated_down": False}


@app.post("/admin/kill-link/{neighbor_id}")
async def admin_kill_link(neighbor_id: str):
    """Simulate a single LINK failing (not the whole node) — e.g. a fiber
    cut between two routers that are both otherwise healthy. Removes the
    edge from our own LSA and immediately floods the update instead of
    waiting for the next periodic flood, so reconvergence starts right away."""
    global last_failure_event
    if neighbor_id not in OWN_LINKS:
        raise HTTPException(status_code=404, detail=f"no link to {neighbor_id}")
    removed_weight = OWN_LINKS.pop(neighbor_id)
    lsa_db[NODE_ID] = {
        "links": dict(OWN_LINKS),
        "seq": lsa_db[NODE_ID].get("seq", 0) + 1,
    }
    last_failure_event = {"type": "link_kill", "node": NODE_ID, "neighbor": neighbor_id, "at": time.time()}
    recompute_routes(reason=f"link to {neighbor_id} killed")
    log.warning(f"[{NODE_ID}] *** LINK TO {neighbor_id} KILLED (weight was {removed_weight}) ***")
    # Broadcast to everyone we know, not just remaining direct links -- once
    # a node's last link is cut, OWN_LINKS is empty and periodic lsa_flood_loop
    # would never announce this again, stranding the update.
    asyncio.create_task(_flood_to([nid for nid in known_peers if nid != NODE_ID]))
    return {"node_id": NODE_ID, "removed_link": neighbor_id}


@app.post("/admin/restore-link/{neighbor_id}")
async def admin_restore_link(neighbor_id: str, weight: float = 10.0):
    """Restore a previously-killed link."""
    global last_failure_event
    OWN_LINKS[neighbor_id] = weight
    lsa_db[NODE_ID] = {
        "links": dict(OWN_LINKS),
        "seq": lsa_db[NODE_ID].get("seq", 0) + 1,
    }
    last_failure_event = {"type": "link_restore", "node": NODE_ID, "neighbor": neighbor_id, "at": time.time()}
    recompute_routes(reason=f"link to {neighbor_id} restored")
    log.info(f"[{NODE_ID}] *** LINK TO {neighbor_id} RESTORED (weight={weight}) ***")
    asyncio.create_task(_flood_to([nid for nid in known_peers if nid != NODE_ID]))
    return {"node_id": NODE_ID, "restored_link": neighbor_id, "weight": weight}


async def _flood_to(neighbor_ids: List[str]) -> None:
    """One-off immediate LSA push to specific nodes, used right after an
    injected fault so we don't have to wait up to LSA_INTERVAL seconds for
    the change to start spreading."""
    async with httpx.AsyncClient(timeout=3.0) as client:
        for nid in neighbor_ids:
            url = known_peers.get(nid)
            if not url:
                continue
            try:
                resp = await client.post(f"{url}/lsa", json={"lsa_db": lsa_db})
                if resp.status_code == 200:
                    merge_lsa(resp.json().get("lsa_db", {}))
            except Exception:
                pass


@app.get("/metrics")
async def metrics():
    return {
        "node_id": NODE_ID,
        "link_load": link_load,
        "messages_forwarded": messages_forwarded,
        "messages_delivered": messages_delivered,
        "messages_dropped": messages_dropped,
    }


@app.get("/routes")
async def routes():
    return {
        "node_id": NODE_ID,
        "routing_table": routing_table,
        "last_convergence_ms": last_convergence_ms,
        "last_convergence_reason": last_convergence_reason,
    }


@app.get("/topology")
async def topology():
    """Full topology graph as this node currently sees it (edges + which
    nodes are excluded because they're believed DEAD)."""
    edges = []
    for nid, entry in lsa_db.items():
        for neighbor, weight in entry.get("links", {}).items():
            edges.append({"from": nid, "to": neighbor, "weight": weight})
    return {
        "node_id": NODE_ID,
        "edges": edges,
        "status": status,
    }


@app.get("/peers")
async def peers():
    now = time.time()
    return {
        "node_id": NODE_ID,
        "known_peers": known_peers,
        "status": status,
        "last_seen_ago_sec": {nid: round(now - t, 1) for nid, t in last_seen.items()},
    }


@app.get("/debug")
async def debug():
    """Everything in one call — convenient for a chaos-test harness (or the
    dashboard) that's polling many nodes at once instead of hitting several
    endpoints per node."""
    path_lengths = [len(r["paths"][0]) - 1 for r in routing_table.values() if r.get("paths")]
    avg_path_length = round(sum(path_lengths) / len(path_lengths), 2) if path_lengths else None
    return {
        "node_id": NODE_ID,
        "simulated_down": SIMULATED_DOWN,
        "last_failure_event": last_failure_event,
        "status": status,
        "routing_table": routing_table,
        "last_convergence_ms": last_convergence_ms,
        "last_convergence_reason": last_convergence_reason,
        "convergence_count": convergence_count,
        "link_load": link_load,
        "own_links": OWN_LINKS,
        "messages_forwarded": messages_forwarded,
        "messages_delivered": messages_delivered,
        "messages_dropped": messages_dropped,
        "drop_reasons": drop_reasons,
        "lsa_version": lsa_db.get(NODE_ID, {}).get("seq", 0),
        "topology_size": len(lsa_db),
        "routes_known": len(routing_table),
        "avg_path_length": avg_path_length,
    }


@app.get("/health")
async def health():
    return {"node_id": NODE_ID, "status": "ok", "simulated_down": SIMULATED_DOWN}
