"""
AutoMesh -- Dashboard (fully data-driven, no fixed node-set assumptions)

Polls every node's /health + /debug every second (ground truth), diffs
consecutive polls to generate a real event feed AND detect actual routing
cost changes (old -> new, with the reason), tracks per-node heartbeat
freshness and LSA version, and can spawn/delete/connect nodes at runtime.
Serves a single-page ops dashboard designed to fit one viewport: header +
metrics are fixed height, the three-column body below scrolls internally
per-panel instead of the whole page scrolling.

Run: python3 dashboard.py   (after run_local.py/.sh has started the seed nodes)
Then open http://localhost:9000
"""

import asyncio
import json
import os
import subprocess
import sys
import time
from collections import deque

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

HERE = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(HERE, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

NODES = json.loads(os.environ.get("NODES_JSON", json.dumps({
    "A": "http://localhost:8001", "B": "http://localhost:8002",
    "C": "http://localhost:8003", "D": "http://localhost:8004",
    "E": "http://localhost:8005",
})))

managed_procs = {}
POLL_INTERVAL = 1.0
CONGESTION_THRESHOLD = 3.0

app = FastAPI(title="AutoMesh Dashboard")

latest_snapshot = {"nodes": {}, "edges": [], "sample_path": [], "events": [], "metrics": {}, "ts": 0}
connected_sockets: set = set()

event_log = deque(maxlen=100)
_prev_up = None
_prev_edges = None
_prev_path = None
_congested_logged = set()
_prev_routes = {}          # node_id -> {dest: cost}, for cost-change diffing
_prev_metrics_totals = None  # for packets/sec calc
_last_seen_by_dashboard = {}  # node_id -> time.time() of last successful poll


def log_event(text: str):
    event_log.appendleft({"ts": time.strftime("%H:%M:%S"), "text": text})


def _next_free_port() -> int:
    used = set()
    for url in NODES.values():
        try:
            used.add(int(url.rsplit(":", 1)[1]))
        except Exception:
            pass
    port = 8100
    while port in used:
        port += 1
    return port


async def poll_once() -> dict:
    nodes_state = {}
    debug_by_id = {}
    node_items = list(NODES.items())
    async with httpx.AsyncClient(timeout=1.5) as client:
        async def check(nid, url):
            try:
                h = await client.get(f"{url}/health")
                h.raise_for_status()
                d = await client.get(f"{url}/debug")
                d.raise_for_status()
                dbg = d.json()
                debug_by_id[nid] = dbg
                nodes_state[nid] = {"up": not dbg.get("simulated_down", False), "url": url}
                _last_seen_by_dashboard[nid] = time.time()
            except Exception:
                nodes_state[nid] = {"up": False, "url": url}

        await asyncio.gather(*(check(nid, url) for nid, url in node_items))

    edges = []
    seen_pairs = set()
    for nid, dbg in debug_by_id.items():
        if not nodes_state.get(nid, {}).get("up"):
            continue
        for neighbor, weight in dbg.get("own_links", {}).items():
            if neighbor not in nodes_state or not nodes_state[neighbor].get("up"):
                continue
            neighbor_dbg = debug_by_id.get(neighbor)
            if not neighbor_dbg or nid not in neighbor_dbg.get("own_links", {}):
                continue
            pair = tuple(sorted([nid, neighbor]))
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            load = max(dbg.get("link_load", {}).get(neighbor, 0.0), neighbor_dbg.get("link_load", {}).get(nid, 0.0))
            edges.append({"from": pair[0], "to": pair[1], "weight": weight, "load": round(load, 2), "congested": load >= CONGESTION_THRESHOLD})

    up_ids = sorted([nid for nid, s in nodes_state.items() if s["up"]])
    sample_path = []
    if len(up_ids) >= 2:
        src, dst = up_ids[0], up_ids[-1]
        route = debug_by_id.get(src, {}).get("routing_table", {}).get(dst)
        if route:
            sample_path = route.get("paths", [[]])[0]

    now = time.time()
    for nid in nodes_state:
        dbg = debug_by_id.get(nid, {})
        rt = dbg.get("routing_table", {})
        nodes_state[nid]["heartbeat_age"] = round(now - _last_seen_by_dashboard.get(nid, now), 2)
        nodes_state[nid]["lsa_version"] = dbg.get("lsa_version", 0)
        nodes_state[nid]["detail"] = {
            "link_load": dbg.get("link_load", {}),
            "messages_forwarded": dbg.get("messages_forwarded"),
            "messages_delivered": dbg.get("messages_delivered"),
            "messages_dropped": dbg.get("messages_dropped"),
            "drop_reasons": dbg.get("drop_reasons", {}),
            "own_links": dbg.get("own_links", {}),
            "last_convergence_ms": dbg.get("last_convergence_ms"),
            "last_convergence_reason": dbg.get("last_convergence_reason"),
            "convergence_count": dbg.get("convergence_count"),
            "topology_size": dbg.get("topology_size"),
            "routes_known": dbg.get("routes_known"),
            "avg_path_length": dbg.get("avg_path_length"),
            "lsa_version": dbg.get("lsa_version", 0),
            "routing_table": [
                {"destination": dest, "next_hop": "/".join(r["next_hops"]), "cost": r["cost"]}
                for dest, r in sorted(rt.items())
            ],
        }

    total_delivered = sum(d.get("messages_delivered") or 0 for d in debug_by_id.values())
    total_forwarded = sum(d.get("messages_forwarded") or 0 for d in debug_by_id.values())
    total_dropped = sum(d.get("messages_dropped") or 0 for d in debug_by_id.values())
    total_convergences = sum(d.get("convergence_count") or 0 for d in debug_by_id.values())
    conv_values = [d.get("last_convergence_ms") for d in debug_by_id.values() if d.get("last_convergence_ms") is not None]
    avg_convergence = round(sum(conv_values) / len(conv_values), 2) if conv_values else None
    routes_known_values = [d.get("routes_known") for d in debug_by_id.values() if d.get("routes_known") is not None]
    avg_routes_known = round(sum(routes_known_values) / len(routes_known_values), 1) if routes_known_values else 0
    path_len_values = [d.get("avg_path_length") for d in debug_by_id.values() if d.get("avg_path_length") is not None]
    avg_path_length = round(sum(path_len_values) / len(path_len_values), 2) if path_len_values else None
    topology_version = sum(d.get("lsa_version", 0) for d in debug_by_id.values())

    global _prev_metrics_totals
    packets_per_sec = 0.0
    if _prev_metrics_totals is not None:
        delta = (total_delivered + total_forwarded) - _prev_metrics_totals
        packets_per_sec = round(max(delta, 0) / POLL_INTERVAL, 2)
    _prev_metrics_totals = total_delivered + total_forwarded

    metrics = {
        "nodes_up": len(up_ids), "nodes_total": len(node_items),
        "links_up": len(edges),
        "links_total_declared": sum(len(d.get("own_links", {})) for d in debug_by_id.values()) // 2 or len(edges),
        "packets_delivered": total_delivered, "packets_forwarded": total_forwarded, "packets_dropped": total_dropped,
        "packets_per_sec": packets_per_sec,
        "avg_convergence_ms": avg_convergence,
        "convergence_count": total_convergences,
        "topology_version": topology_version,
        "active_routes": avg_routes_known,
        "avg_path_length": avg_path_length,
    }

    return {"nodes": nodes_state, "edges": edges, "sample_path": sample_path, "metrics": metrics,
            "debug_by_id": debug_by_id, "ts": time.time()}


def diff_and_log_events(snapshot: dict):
    global _prev_up, _prev_edges, _prev_path, _prev_routes
    cur_up = {nid: s["up"] for nid, s in snapshot["nodes"].items()}
    cur_edges = {(e["from"], e["to"]) for e in snapshot["edges"]}
    cur_path = snapshot["sample_path"]
    debug_by_id = snapshot.pop("debug_by_id", {})

    toasts = []

    if _prev_up is not None:
        for nid, up in cur_up.items():
            was_up = _prev_up.get(nid)
            if was_up is True and up is False:
                log_event(f"Node {nid} went DOWN")
                toasts.append({"type": "down", "text": f"Node {nid} went DOWN"})
            elif was_up is False and up is True:
                log_event(f"Node {nid} is back UP")
                toasts.append({"type": "up", "text": f"Node {nid} is back UP"})

    if _prev_edges is not None:
        for pair in cur_edges - _prev_edges:
            log_event(f"Link {pair[0]}-{pair[1]} is up")
        for pair in _prev_edges - cur_edges:
            log_event(f"Link {pair[0]}-{pair[1]} is down")

    if _prev_path is not None and cur_path and cur_path != _prev_path:
        log_event(f"Sample route recomputed: {' -> '.join(cur_path)}")

    for edge in snapshot["edges"]:
        key = f"{edge['from']}-{edge['to']}"
        if edge["congested"]:
            if key not in _congested_logged:
                log_event(f"Link {edge['from']}-{edge['to']} is congested (load={edge['load']})")
                toasts.append({"type": "congested", "text": f"Link {edge['from']}-{edge['to']} congested"})
                _congested_logged.add(key)
        else:
            _congested_logged.discard(key)

    # Real cost-change detection: diff each node's own routing table against
    # its previous poll. This is what lets a viewer SEE a routing decision
    # change, not just infer it from the topology moving.
    for nid, dbg in debug_by_id.items():
        rt = dbg.get("routing_table", {})
        cur_costs = {dest: r["cost"] for dest, r in rt.items()}
        prev_costs = _prev_routes.get(nid)
        if prev_costs is not None:
            for dest, cost in cur_costs.items():
                old = prev_costs.get(dest)
                if old is not None and old != cost:
                    next_hop = "/".join(rt[dest]["next_hops"])
                    log_event(f"Node {nid}: route to {dest} changed (cost {old} -> {cost}, via {next_hop})")
                    toasts.append({"type": "reroute", "text": f"{nid}->{dest} cost {old} -> {cost}"})
        _prev_routes[nid] = cur_costs

    _prev_up = cur_up
    _prev_edges = cur_edges
    _prev_path = cur_path
    return toasts


async def poll_loop():
    global latest_snapshot
    while True:
        snapshot = await poll_once()
        toasts = diff_and_log_events(snapshot)
        snapshot["events"] = list(event_log)
        snapshot["toasts"] = toasts
        latest_snapshot = snapshot
        dead = []
        for ws in connected_sockets:
            try:
                await ws.send_json(latest_snapshot)
            except Exception:
                dead.append(ws)
        for ws in dead:
            connected_sockets.discard(ws)
        await asyncio.sleep(POLL_INTERVAL)


@app.on_event("startup")
async def start_polling():
    asyncio.create_task(poll_loop())


@app.on_event("shutdown")
async def shutdown_managed():
    for nid, proc in managed_procs.items():
        proc.terminate()


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_sockets.add(websocket)
    try:
        await websocket.send_json(latest_snapshot)
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connected_sockets.discard(websocket)


@app.get("/snapshot")
async def get_snapshot():
    return latest_snapshot


@app.post("/log-event")
async def post_event(payload: dict):
    text = payload.get("text", "").strip()
    if text:
        log_event(text)
    return {"ok": True}


@app.post("/manage/spawn")
async def spawn_node(payload: dict):
    node_id = (payload.get("node_id") or "").strip()
    links = payload.get("links") or {}
    if not node_id:
        return {"ok": False, "error": "node_id required"}
    if node_id in NODES:
        return {"ok": False, "error": f"node {node_id} already exists"}
    for nid in links:
        if nid not in NODES:
            return {"ok": False, "error": f"unknown neighbor {nid}"}

    port = _next_free_port()
    url = f"http://localhost:{port}"
    bootstrap_url = None
    for nid, s in latest_snapshot.get("nodes", {}).items():
        if s.get("up"):
            bootstrap_url = NODES[nid]
            break
    if bootstrap_url is None and NODES:
        bootstrap_url = next(iter(NODES.values()))

    env = os.environ.copy()
    env.update({"NODE_ID": node_id, "NODE_URL": url, "PORT": str(port), "LINKS": json.dumps(links)})
    if bootstrap_url:
        env["BOOTSTRAP_URL"] = bootstrap_url

    logfile = open(os.path.join(LOG_DIR, f"node_{node_id}.log"), "w")
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "node:app", "--host", "0.0.0.0", "--port", str(port), "--log-level", "warning"],
        cwd=HERE, env=env, stdout=logfile, stderr=subprocess.STDOUT,
    )
    managed_procs[node_id] = proc
    NODES[node_id] = url
    log_event(f"Node {node_id} joined network (spawned on port {port}), links: {links or '(none yet)'}")

    await asyncio.sleep(1.5)

    async with httpx.AsyncClient(timeout=3.0) as client:
        for neighbor_id, weight in links.items():
            neighbor_url = NODES.get(neighbor_id)
            if not neighbor_url:
                continue
            try:
                await client.post(f"{neighbor_url}/admin/restore-link/{node_id}?weight={weight}")
            except Exception:
                pass
    log_event(f"LSA flood initiated for node {node_id}")

    return {"ok": True, "node_id": node_id, "port": port}


@app.post("/manage/delete")
async def delete_node(payload: dict):
    node_id = (payload.get("node_id") or "").strip()
    if node_id not in NODES:
        return {"ok": False, "error": f"unknown node {node_id}"}
    url = NODES[node_id]
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            await client.post(f"{url}/admin/kill")
    except Exception:
        pass
    if node_id in managed_procs:
        proc = managed_procs.pop(node_id)
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
    del NODES[node_id]
    log_event(f"Node {node_id} deleted")
    return {"ok": True}


@app.post("/manage/connect")
async def connect_nodes(payload: dict):
    a, b = payload.get("a"), payload.get("b")
    weight = payload.get("weight", 10)
    if a not in NODES or b not in NODES:
        return {"ok": False, "error": "both nodes must exist"}
    if a == b:
        return {"ok": False, "error": "cannot connect a node to itself"}
    ok = True
    async with httpx.AsyncClient(timeout=3.0) as client:
        for src, dst in [(a, b), (b, a)]:
            try:
                r = await client.post(f"{NODES[src]}/admin/restore-link/{dst}?weight={weight}")
                if r.status_code != 200:
                    ok = False
            except Exception:
                ok = False
    log_event(f"Connected {a} <-> {b} (w={weight})")
    return {"ok": ok}


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML_PAGE


HTML_PAGE = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>AutoMesh Dashboard</title>
<style>
  * { box-sizing: border-box; }
  html, body { height:100%; margin:0; overflow:hidden; }
  body { background:#0b0d13; color:#e6e6e6; font-family: -apple-system, Segoe UI, sans-serif; display:flex; flex-direction:column; padding:14px 20px; }
  h1 { font-size:17px; font-weight:700; margin:0 0 2px; }
  .sub { color:#8a8f98; font-size:11px; margin-bottom:10px; }

  .metrics-row { display:grid; grid-template-columns: repeat(10, 1fr); gap:8px; margin-bottom:10px; flex-shrink:0; }
  .metric-card { background:#141824; border:1px solid #23283a; border-radius:8px; padding:8px 10px; }
  .metric-card .label { font-size:9px; color:#7c8291; text-transform:uppercase; letter-spacing:0.04em; margin-bottom:3px; white-space:nowrap; }
  .metric-card .value { font-size:16px; font-weight:700; color:#f0f2f6; font-variant-numeric: tabular-nums; }
  .metric-card .value.warn { color:#e0a555; }

  .main-grid { display:grid; grid-template-columns: 1.3fr 0.85fr 0.85fr; gap:12px; flex:1; min-height:0; }
  .col { display:flex; flex-direction:column; gap:12px; min-height:0; }
  .panel { background:#141824; border:1px solid #23283a; border-radius:10px; padding:12px 14px; display:flex; flex-direction:column; min-height:0; }
  .panel h2 { font-size:11px; margin:0 0 8px; color:#9aa0ac; text-transform:uppercase; letter-spacing:0.04em; flex-shrink:0; }
  .panel h2.sep { margin-top:10px; padding-top:10px; border-top:1px solid #23283a; }
  .panel-scroll { overflow-y:auto; min-height:0; flex:1; }

  svg { background:#0f131e; border-radius:8px; width:100%; display:block; }
  .node-label { font-size:12px; font-weight:700; fill:#0b0d13; text-anchor:middle; dominant-baseline:middle; pointer-events:none; }
  .edge-label { font-size:9px; fill:#5c6270; text-anchor:middle; }
  .node-circle { cursor:pointer; stroke-width:2.5; }
  .node-up { fill:#3ecf6b; stroke:#2a9950; animation: pulse-green 2.2s ease-in-out infinite; }
  .node-down { fill:#e0555c; stroke:#a83f45; animation: pulse-red 1.6s ease-in-out infinite; }
  .node-congested { stroke:#e0a555; stroke-width:4; }
  @keyframes pulse-green { 0%,100% { filter: drop-shadow(0 0 0px #3ecf6b); } 50% { filter: drop-shadow(0 0 7px #3ecf6b); } }
  @keyframes pulse-red { 0%,100% { filter: drop-shadow(0 0 0px #e0555c); } 50% { filter: drop-shadow(0 0 5px #e0555c); } }
  .packet { fill:#ffd76a; filter: drop-shadow(0 0 6px #ffd76a); transition: cx 0.45s linear, cy 0.45s linear; }
  .packet-trail { fill:#ffd76a; opacity:0.35; transition: opacity 0.5s ease-out; pointer-events:none; }

  button { background:#1d2230; color:#e6e6e6; border:1px solid #2e3548; border-radius:6px; padding:5px 9px; font-size:11px; cursor:pointer; }
  button:hover { background:#2a3145; }
  button.danger:hover { background:#5a2a2a; border-color:#7a3a3a; }
  button.primary { background:#2d5a3d; border-color:#3d8b52; font-weight:600; }
  button.primary:hover { background:#357048; }
  button:disabled { opacity:0.4; cursor:not-allowed; }
  select, input[type=text], input[type=number] { background:#1d2230; color:#e6e6e6; border:1px solid #2e3548; border-radius:6px; padding:5px; font-size:11px; }
  input[type=text] { width:70px; } input[type=number] { width:50px; }
  .row { display:flex; gap:6px; margin-bottom:7px; align-items:center; flex-wrap:wrap; }
  .row-label { font-size:10px; color:#7c8291; width:66px; flex-shrink:0; }
  .chip { background:#1d2230; border:1px solid #2e3548; border-radius:12px; padding:2px 8px; font-size:10px; display:inline-flex; align-items:center; gap:4px; }
  .chip .x { cursor:pointer; color:#e0555c; font-weight:700; }
  #pending-links { display:flex; gap:5px; flex-wrap:wrap; margin: 3px 0 7px 66px; }

  .legend { font-size:10px; color:#7c8291; margin-top:6px; line-height:1.5; flex-shrink:0; }
  .dot { display:inline-block; width:8px; height:8px; border-radius:50%; margin-right:5px; vertical-align:middle; }
  #path-display { font-size:11px; color:#ffd76a; margin:6px 0 0; min-height:14px; flex-shrink:0; }

  table { width:100%; border-collapse:collapse; font-size:11px; }
  th { text-align:left; color:#7c8291; font-weight:600; font-size:9px; text-transform:uppercase; padding:3px 4px; border-bottom:1px solid #23283a; position:sticky; top:0; background:#141824; }
  td { padding:3px 4px; color:#c7cbd4; border-bottom:1px solid #1a1e2a; font-variant-numeric: tabular-nums; }
  tr.node-row { cursor:pointer; }
  tr.node-row:hover { background:#1a1e2c; }
  .status-up { color:#3ecf6b; font-weight:600; }
  .status-down { color:#e0555c; font-weight:600; }

  .events { font-size:11px; font-family: ui-monospace, monospace; }
  .event-row { padding:3px 0; border-bottom:1px solid #1c2130; color:#c7cbd4; }
  .event-row .ts { color:#5c6270; margin-right:6px; }

  .node-detail .nd-row { display:flex; justify-content:space-between; padding:2px 0; color:#c7cbd4; font-size:11px; }
  .node-detail .nd-row span:first-child { color:#7c8291; }
  .node-detail h3 { margin:8px 0 5px; font-size:12px; }
  .node-detail h3:first-child { margin-top:0; }

  .demo-btn { width:100%; padding:8px; font-size:12px; margin-top:4px; }

  #toast-container { position:fixed; top:16px; right:16px; z-index:999; display:flex; flex-direction:column; gap:8px; }
  .toast { background:#1a1e2c; border:1px solid #2e3548; border-left:3px solid #3ecf6b; border-radius:6px; padding:8px 12px; font-size:12px; box-shadow:0 4px 16px rgba(0,0,0,0.4); animation: toast-in 0.25s ease-out; max-width:280px; }
  .toast.down { border-left-color:#e0555c; }
  .toast.congested { border-left-color:#e0a555; }
  .toast.reroute { border-left-color:#ffd76a; }
  @keyframes toast-in { from { opacity:0; transform:translateX(20px); } to { opacity:1; transform:translateX(0); } }
  .toast.fade-out { animation: toast-out 0.4s ease-in forwards; }
  @keyframes toast-out { to { opacity:0; transform:translateX(20px); } }
</style>
</head>
<body>
<h1>AutoMesh — Live Network Operations</h1>
<div class="sub">Self-healing overlay network · fully data-driven from the live topology API</div>

<div class="metrics-row" id="metrics-row"></div>

<div class="main-grid">
  <div class="col">
    <div class="panel" style="flex:1.3;">
      <h2>Topology</h2>
      <svg id="graph" viewBox="0 0 600 380"></svg>
      <div id="path-display"></div>
      <div class="legend">
        <div><span class="dot" style="background:#3ecf6b"></span>Up &nbsp; <span class="dot" style="background:#e0555c"></span>Down &nbsp; <span class="dot" style="background:#ffd76a"></span>Active route &nbsp; orange ring = congested</div>
      </div>
    </div>
    <div class="panel" style="flex:1;">
      <h2>Controls</h2>
      <div class="panel-scroll">
        <div class="row">
          <span class="row-label">Packet</span>
          <select id="pktFrom"></select>→<select id="pktTo"></select>
          <button class="primary" onclick="sendPacket()">Send</button>
        </div>
        <div class="row">
          <span class="row-label">Node</span>
          <select id="nodeSelect"></select>
          <button onclick="failNode()" class="danger">Fail</button>
          <button onclick="restoreNode()">Restore</button>
        </div>
        <div class="row">
          <span class="row-label">Link</span>
          <select id="linkFrom"></select>—<select id="linkTo"></select>
          <button onclick="killLink()" class="danger">Break</button>
        </div>
        <div class="row">
          <span class="row-label">Traffic</span>
          <button onclick="simulateCongestion()">Simulate congestion</button>
        </div>
        <button class="primary demo-btn" id="demoBtn" onclick="runDemo()">▶ Run AutoMesh Demo</button>

        <h2 class="sep">Topology Editing</h2>
        <div class="row">
          <span class="row-label">Spawn</span>
          <input type="text" id="spawnId" placeholder="ID">
          <select id="spawnNeighbor"></select>
          <input type="number" id="spawnWeight" value="10">
          <button onclick="addPendingLink()">+link</button>
        </div>
        <div id="pending-links"></div>
        <div class="row">
          <button class="primary" onclick="spawnNode()">Spawn Node</button>
        </div>
        <div class="row">
          <span class="row-label">Delete</span>
          <select id="deleteSelect"></select>
          <button onclick="deleteNode()" class="danger">Delete Node</button>
        </div>
        <div class="row">
          <span class="row-label">Connect</span>
          <select id="connectA"></select>↔<select id="connectB"></select>
          <input type="number" id="connectWeight" value="10">
          <button onclick="connectNodes()">Connect</button>
        </div>
      </div>
    </div>
  </div>

  <div class="col">
    <div class="panel" style="flex:1;">
      <h2>Live Nodes</h2>
      <div class="panel-scroll">
        <table>
          <thead><tr><th>ID</th><th>Address</th><th>Status</th><th>Heartbeat</th><th>LSA v</th></tr></thead>
          <tbody id="live-nodes-body"></tbody>
        </table>
      </div>
    </div>
    <div class="panel" style="flex:1.4;">
      <h2>Node Detail</h2>
      <div class="panel-scroll">
        <div style="color:#7c8291; font-size:11px;" id="detail-empty">Click a node to inspect it.</div>
        <div class="node-detail" id="detail-content" style="display:none;"></div>
      </div>
    </div>
  </div>

  <div class="col">
    <div class="panel" style="flex:1;">
      <h2>Network Events</h2>
      <div class="events panel-scroll" id="events"></div>
    </div>
  </div>
</div>

<div id="toast-container"></div>

<script>
let latestSnapshot = null;
let selectedNode = null;
let lastIdsKey = null;
let pendingLinks = {};

function currentNodeUrls() {
  const out = {};
  if (latestSnapshot) for (const id in latestSnapshot.nodes) out[id] = latestSnapshot.nodes[id].url;
  return out;
}
function currentIds() { return latestSnapshot ? Object.keys(latestSnapshot.nodes).sort() : []; }

function computeLayout(ids) {
  const n = ids.length;
  const cx = 300, cy = 190;
  const r = n <= 1 ? 0 : Math.min(150, 60 + n * 10);
  const layout = {};
  ids.forEach((id, i) => {
    const angle = (2 * Math.PI * i / Math.max(n, 1)) - Math.PI / 2;
    layout[id] = [cx + r * Math.cos(angle), cy + r * Math.sin(angle)];
  });
  return layout;
}

function rebuildSelectsIfNeeded(ids) {
  const key = ids.join(',');
  if (key === lastIdsKey) return;
  lastIdsKey = key;
  const selectIds = ['pktFrom','pktTo','nodeSelect','linkFrom','linkTo','spawnNeighbor','deleteSelect','connectA','connectB'];
  for (const selId of selectIds) {
    const sel = document.getElementById(selId);
    const prevValue = sel.value;
    sel.innerHTML = '';
    for (const id of ids) { const o = document.createElement('option'); o.value = id; o.textContent = id; sel.appendChild(o); }
    if (ids.includes(prevValue)) sel.value = prevValue;
  }
  if (ids.length > 1) {
    document.getElementById('pktTo').selectedIndex = ids.length - 1;
    document.getElementById('linkTo').selectedIndex = Math.min(1, ids.length - 1);
    document.getElementById('connectB').selectedIndex = Math.min(1, ids.length - 1);
  }
}

async function logEvent(text) {
  try { await fetch('/log-event', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({text})}); } catch(e) {}
}

async function failNode() { const u=currentNodeUrls(); await fetch(`${u[document.getElementById('nodeSelect').value]}/admin/kill`, {method:'POST'}); }
async function restoreNode() { const u=currentNodeUrls(); await fetch(`${u[document.getElementById('nodeSelect').value]}/admin/revive`, {method:'POST'}); }
async function killLink() {
  const u=currentNodeUrls(); const a=document.getElementById('linkFrom').value, b=document.getElementById('linkTo').value;
  await fetch(`${u[a]}/admin/kill-link/${b}`, {method:'POST'});
}
async function simulateCongestion() {
  const u=currentNodeUrls(); const a=document.getElementById('linkFrom').value, b=document.getElementById('linkTo').value;
  await logEvent(`Simulating congestion between ${a} and ${b}`);
  const bursts=[];
  for (let i=0;i<12;i++) bursts.push(fetch(`${u[a]}/message`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({source:a,destination:b,payload:'congestion-test',ttl:10,trace:[]})}));
  await Promise.all(bursts);
}

async function sendPacket() {
  const u=currentNodeUrls(); const a=document.getElementById('pktFrom').value, b=document.getElementById('pktTo').value;
  if (a===b || !u[a]) return;
  const resp = await fetch(`${u[a]}/message`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({source:a,destination:b,payload:'dashboard-packet',ttl:10,trace:[]})});
  const data = await resp.json();
  if (data.delivered) { await logEvent(`Packet ${a} -> ${b} routed via ${data.trace.join(' -> ')}`); await animatePacket(data.trace); }
  else { await logEvent(`Packet ${a} -> ${b} FAILED (${data.reason})`); }
  return data;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function animatePacket(trace) {
  if (!trace || trace.length < 1) return;
  const layout = computeLayout(currentIds());
  const svg = document.getElementById('graph');
  const [x0,y0] = layout[trace[0]] || [300,190];
  const packet = document.createElementNS('http://www.w3.org/2000/svg','circle');
  packet.setAttribute('r',6); packet.setAttribute('cx',x0); packet.setAttribute('cy',y0); packet.setAttribute('class','packet');
  svg.appendChild(packet);
  for (let i=0;i<trace.length;i++) {
    const [x,y] = layout[trace[i]] || [300,190];
    const trail = document.createElementNS('http://www.w3.org/2000/svg','circle');
    trail.setAttribute('r',5); trail.setAttribute('cx',packet.getAttribute('cx')); trail.setAttribute('cy',packet.getAttribute('cy')); trail.setAttribute('class','packet-trail');
    svg.appendChild(trail);
    requestAnimationFrame(() => { trail.style.opacity='0'; });
    setTimeout(() => trail.remove(), 550);
    packet.setAttribute('cx',x); packet.setAttribute('cy',y);
    await sleep(420);
  }
  await sleep(200); packet.remove();
}

async function runDemo() {
  const btn = document.getElementById('demoBtn'); btn.disabled = true;
  try {
    const ids = currentIds();
    if (ids.length < 2) { await logEvent('Demo needs at least 2 nodes'); return; }
    const src = ids[0], dst = ids[ids.length-1], mid = ids[Math.floor(ids.length/2)];
    const u = currentNodeUrls();
    await logEvent('=== Demo started ===');
    document.getElementById('pktFrom').value = src; document.getElementById('pktTo').value = dst;
    await logEvent(`Demo: sending packet ${src} -> ${dst} on the healthy network`);
    await sendPacket(); await sleep(800);
    await logEvent(`Demo: failing node ${mid}`);
    await fetch(`${u[mid]}/admin/kill`, {method:'POST'});
    await logEvent('Demo: waiting for the network to detect the failure...');
    let converged = false;
    for (let i=0;i<15;i++) {
      await sleep(700);
      try { const r = await fetch(`${u[src]}/routes`); const d = await r.json(); if (!(mid in (d.routing_table||{}))) { converged=true; break; } } catch(e) {}
    }
    await logEvent(converged ? `Demo: route recomputed, ${mid} excluded` : 'Demo: still waiting on convergence...');
    await logEvent(`Demo: sending packet ${src} -> ${dst} again (should reroute around ${mid})`);
    await sendPacket(); await sleep(800);
    await logEvent(`Demo: reviving node ${mid}`);
    await fetch(`${u[mid]}/admin/revive`, {method:'POST'});
    await sleep(500); await logEvent('=== Demo complete ===');
  } finally { btn.disabled = false; }
}

function addPendingLink() {
  const neighbor = document.getElementById('spawnNeighbor').value;
  const weight = parseFloat(document.getElementById('spawnWeight').value) || 10;
  if (!neighbor) return;
  pendingLinks[neighbor] = weight;
  renderPendingLinks();
}
function removePendingLink(id) { delete pendingLinks[id]; renderPendingLinks(); }
function renderPendingLinks() {
  document.getElementById('pending-links').innerHTML = Object.entries(pendingLinks).map(([id,w]) =>
    `<span class="chip">${id} (w=${w}) <span class="x" onclick="removePendingLink('${id}')">×</span></span>`).join('');
}

async function spawnNode() {
  const id = document.getElementById('spawnId').value.trim();
  if (!id) { await logEvent('Enter a node ID before spawning'); return; }
  const resp = await fetch('/manage/spawn', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({node_id:id, links:pendingLinks})});
  const data = await resp.json();
  if (!data.ok) { await logEvent(`Spawn failed: ${data.error}`); return; }
  document.getElementById('spawnId').value=''; pendingLinks={}; renderPendingLinks();
}
async function deleteNode() {
  const id = document.getElementById('deleteSelect').value; if (!id) return;
  await fetch('/manage/delete', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({node_id:id})});
  if (selectedNode === id) selectedNode = null;
}
async function connectNodes() {
  const a=document.getElementById('connectA').value, b=document.getElementById('connectB').value;
  const weight=parseFloat(document.getElementById('connectWeight').value)||10;
  if (a===b) { await logEvent('Cannot connect a node to itself'); return; }
  await fetch('/manage/connect', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({a,b,weight})});
}

const svg = document.getElementById('graph');
function el(tag, attrs) { const e=document.createElementNS('http://www.w3.org/2000/svg',tag); for (const k in attrs) e.setAttribute(k,attrs[k]); return e; }

function showNodeDetail(id) { selectedNode = id; renderDetail(); }

function renderDetail() {
  const empty = document.getElementById('detail-empty'), content = document.getElementById('detail-content');
  if (!selectedNode || !latestSnapshot || !latestSnapshot.nodes[selectedNode]) { empty.style.display='block'; content.style.display='none'; return; }
  empty.style.display='none'; content.style.display='block';
  const node = latestSnapshot.nodes[selectedNode]; const d = node.detail || {};
  const neighbors = Object.keys(d.own_links || {}).join(', ') || '(none)';
  const loadStr = Object.entries(d.link_load || {}).map(([k,v]) => `${k}:${(v.toFixed?v.toFixed(1):v)}`).join(', ') || '(none)';
  const dropStr = Object.entries(d.drop_reasons || {}).map(([k,v]) => `${k}:${v}`).join(', ') || '(none)';
  const routeRows = (d.routing_table || []).map(r => `<tr><td>${r.destination}</td><td>${r.next_hop}</td><td>${r.cost}</td></tr>`).join('');
  content.innerHTML = `
    <h3>Node ${selectedNode}</h3>
    <div class="nd-row"><span>Status</span><span class="${node.up?'status-up':'status-down'}">${node.up?'ALIVE':'DOWN'}</span></div>
    <div class="nd-row"><span>Neighbors</span><span>${neighbors}</span></div>
    <div class="nd-row"><span>Link load</span><span>${loadStr}</span></div>
    <div class="nd-row"><span>Topology size known</span><span>${d.topology_size ?? '-'}</span></div>
    <div class="nd-row"><span>LSA version</span><span>${d.lsa_version ?? '-'}</span></div>
    <div class="nd-row"><span>Convergences</span><span>${d.convergence_count ?? '-'}</span></div>
    <div class="nd-row"><span>Last recompute</span><span>${d.last_convergence_ms!=null?d.last_convergence_ms.toFixed(2)+'ms':'-'} (${d.last_convergence_reason||'-'})</span></div>
    <h3>Routing Table</h3>
    <table><thead><tr><th>Dest</th><th>Next hop</th><th>Cost</th></tr></thead><tbody>${routeRows || '<tr><td colspan=3 style="color:#5c6270;">no routes yet</td></tr>'}</tbody></table>
    <h3>Traffic</h3>
    <div class="nd-row"><span>Forwarded</span><span>${d.messages_forwarded ?? '-'}</span></div>
    <div class="nd-row"><span>Delivered</span><span>${d.messages_delivered ?? '-'}</span></div>
    <div class="nd-row"><span>Dropped</span><span>${d.messages_dropped ?? '-'}</span></div>
    <div class="nd-row"><span>Drop reasons</span><span>${dropStr}</span></div>
  `;
}

function renderMetrics(m) {
  if (!m) return;
  const cards = [
    {label:'Nodes', value:`${m.nodes_up}/${m.nodes_total}`, warn: m.nodes_up < m.nodes_total},
    {label:'Links', value:`${m.links_up}/${m.links_total_declared}`, warn: m.links_up < m.links_total_declared},
    {label:'Delivered', value:m.packets_delivered},
    {label:'Dropped', value:m.packets_dropped, warn: m.packets_dropped > 0},
    {label:'Pkts/sec', value:m.packets_per_sec},
    {label:'Active routes', value:m.active_routes},
    {label:'Avg path len', value: m.avg_path_length ?? '-'},
    {label:'Topology ver', value:m.topology_version},
    {label:'Convergences', value:m.convergence_count},
    {label:'Avg recompute', value: m.avg_convergence_ms!=null ? m.avg_convergence_ms.toFixed(2)+'ms' : '-'},
  ];
  document.getElementById('metrics-row').innerHTML = cards.map(c => `<div class="metric-card"><div class="label">${c.label}</div><div class="value ${c.warn?'warn':''}">${c.value}</div></div>`).join('');
}

function renderEvents(events) {
  document.getElementById('events').innerHTML = (events||[]).map(e => `<div class="event-row"><span class="ts">${e.ts}</span>${e.text}</div>`).join('');
}

function renderLiveNodes(nodes) {
  const ids = Object.keys(nodes).sort();
  document.getElementById('live-nodes-body').innerHTML = ids.map(id => {
    const n = nodes[id];
    const addr = (n.url || '').replace('http://','');
    return `<tr class="node-row" onclick="showNodeDetail('${id}')">
      <td><b>${id}</b></td><td>${addr}</td>
      <td class="${n.up?'status-up':'status-down'}">${n.up?'UP':'DOWN'}</td>
      <td>${n.heartbeat_age!=null ? n.heartbeat_age.toFixed(2)+'s ago' : '-'}</td>
      <td>${n.lsa_version ?? 0}</td>
    </tr>`;
  }).join('');
}

let toastSeq = 0;
function showToast(toast) {
  const id = `toast-${toastSeq++}`;
  const div = document.createElement('div');
  div.className = `toast ${toast.type}`;
  div.id = id;
  div.textContent = toast.text;
  document.getElementById('toast-container').appendChild(div);
  setTimeout(() => { div.classList.add('fade-out'); setTimeout(() => div.remove(), 450); }, 3800);
}

function render(snapshot) {
  latestSnapshot = snapshot;
  const ids = Object.keys(snapshot.nodes).sort();
  rebuildSelectsIfNeeded(ids);

  svg.innerHTML = '';
  if (ids.length === 0) {
    const t = el('text', {x:300, y:190, 'text-anchor':'middle', fill:'#5c6270', 'font-size':12});
    t.textContent = 'No nodes yet -- spawn one in Controls'; svg.appendChild(t);
  } else {
    const layout = computeLayout(ids);
    const pathSet = new Set();
    const path = snapshot.sample_path || [];
    for (let i=0;i<path.length-1;i++) pathSet.add([path[i],path[i+1]].sort().join('-'));

    for (const edge of snapshot.edges) {
      const p1=layout[edge.from], p2=layout[edge.to]; if (!p1||!p2) continue;
      const isPath = pathSet.has([edge.from,edge.to].sort().join('-'));
      const thickness = Math.min(2 + (edge.load||0)*1.5, 9);
      svg.appendChild(el('line', {x1:p1[0],y1:p1[1],x2:p2[0],y2:p2[1], stroke: isPath?'#ffd76a':(edge.congested?'#e0a555':'#2e3548'), 'stroke-width': isPath?thickness+1:thickness}));
      const mx=(p1[0]+p2[0])/2, my=(p1[1]+p2[1])/2;
      const label = el('text', {x:mx, y:my-5, class:'edge-label'}); label.textContent = `w=${edge.weight}`; svg.appendChild(label);
    }
    for (const id of ids) {
      const [x,y] = layout[id]; const node = snapshot.nodes[id]; const up = node && node.up;
      const congested = (snapshot.edges||[]).some(e => e.congested && (e.from===id || e.to===id));
      const circle = el('circle', {cx:x, cy:y, r:22, class:`node-circle ${up?'node-up':'node-down'} ${congested?'node-congested':''}`});
      circle.addEventListener('click', () => showNodeDetail(id));
      svg.appendChild(circle);
      const label = el('text', {x,y,class:'node-label'}); label.textContent = id; svg.appendChild(label);
    }
    document.getElementById('path-display').textContent = path.length ? `Sample route ${path[0]} -> ${path[path.length-1]}: ${path.join(' -> ')}` : '';
  }

  renderMetrics(snapshot.metrics);
  renderEvents(snapshot.events);
  renderLiveNodes(snapshot.nodes);
  renderDetail();
  for (const t of (snapshot.toasts || [])) showToast(t);
}

function connect() {
  const ws = new WebSocket(`ws://${location.host}/ws`);
  ws.onmessage = (ev) => render(JSON.parse(ev.data));
  ws.onclose = () => setTimeout(connect, 1000);
}
connect();
</script>
</body>
</html>
"""
