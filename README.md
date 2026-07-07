# AutoMesh — Self-Healing Distributed Overlay Network

A real distributed system (5 separate async HTTP processes, not a simulation)
that finds its own peers, builds a real weighted topology, routes traffic
with link-state routing + ECMP + load-aware distribution, detects failures
independently on every node, and reconverges automatically — plus a live
dashboard and an API for injecting failures on demand.

```
        A
        |
        B
       / \
      C   D
       \ /
        E
```

## Quick start

Works on **Windows, macOS, and Linux** — the primary launcher is pure Python
(no bash required):

```bash
pip install -r requirements.txt
python run_local.py     # launches A-E on ports 8001-8005, plus the dashboard on 9000
```

Leave that running in its terminal — it prints logs live and Ctrl+C stops
everything cleanly. Wait ~6-8s for discovery + routing to converge, then
open **http://localhost:9000** for the live topology dashboard.

(macOS/Linux/WSL users who prefer shell scripts can use `./run_local.sh` /
`./stop_local.sh` instead — same behavior, just not usable on native Windows
since there's no bash there.)

```bash
curl -s localhost:8001/peers | python3 -m json.tool     # A's view of the network
curl -s localhost:8001/routes | python3 -m json.tool    # A's routing table (ECMP, cost, paths)
curl -s localhost:8001/topology | python3 -m json.tool  # graph edges as A sees them
```

Send real traffic through the network:

```bash
curl -X POST localhost:8001/message -H "Content-Type: application/json" \
  -d '{"source":"A","destination":"E","payload":"hello","ttl":10,"trace":[]}'
```

Inject a failure (via API, no PIDs needed):

```bash
curl -X POST localhost:8003/admin/kill              # simulate C crashing
curl -X POST localhost:8003/admin/revive            # bring it back
curl -X POST localhost:8002/admin/kill-link/C       # kill just the B-C link
curl -X POST "localhost:8002/admin/restore-link/C?weight=5"
```

Run the full regression suite:

```bash
python chaos_test.py    # or ./chaos_test.sh on macOS/Linux/WSL
```

Stop everything: press **Ctrl+C** in the terminal running `run_local.py`
(or `./stop_local.sh` if you used the shell-script version).

## Files

| File | Purpose |
|---|---|
| `node.py` | The entire node implementation — every node runs this same code |
| `dashboard.py` | Live topology dashboard (WebSocket push + failure-injection UI) |
| `run_local.py` | **Cross-platform** launcher (Windows/macOS/Linux) — Ctrl+C to stop |
| `chaos_test.py` | **Cross-platform** regression suite (7 scenarios, real assertions) |
| `run_local.sh` / `stop_local.sh` | macOS/Linux/WSL shell-script equivalents |
| `chaos_test.sh` | macOS/Linux/WSL shell-script equivalent of the test suite |
| `requirements.txt` | Python dependencies |

## Architecture

**Every node is identical code**, differentiated only by env vars
(`NODE_ID`, `NODE_URL`, `BOOTSTRAP_URL`, `LINKS`, `PORT`). There is no
central server anywhere — `BOOTSTRAP_URL` is just "the first peer I happen
to talk to," not an authority.

**Two separate layers of "who do I know about":**
- `known_peers` — discovery layer. Full mesh of addresses, learned via
  bootstrap + gossip. Used for heartbeats and reachability.
- `lsa_db` / `OWN_LINKS` — topology layer. Sparse, weighted, matches the
  actual diagram (A-B, B-C, B-D, C-E, D-E). This is what routing runs over.
  Without this split, Dijkstra would be pointless — discovery alone makes
  every node directly "reachable" by address.

**Link-state routing (OSPF-style):** each node periodically floods its own
`OWN_LINKS` to its direct neighbors (`lsa_flood_loop` / `POST /lsa`);
whoever receives new information merges it and re-floods next cycle, so it
eventually spreads network-wide. An edge only exists in the routing graph
if **both** endpoints currently advertise it — this matters for failure
injection: one side withdrawing a link is enough to remove it everywhere,
modeling a real point-to-point interface going down.

**Routing table (`recompute_routes`):** Dijkstra over the graph, but using
`nx.all_shortest_paths` to keep *every* path tied for lowest cost, not just
one — that's Equal-Cost Multi-Path. In the demo topology, B has two
equal-cost routes to E (`B->C->E` and `B->D->E`, both cost 13).

**Load-aware forwarding (`choose_next_hop`, real `POST /message` forwarding):**
when there's an ECMP tie, forward toward whichever neighbor has the least
recent traffic (`link_load`, which decays geometrically over time). If the
chosen hop is actually down (a race between independent per-node failure
clocks — see below), forwarding fails over to the next ECMP candidate
instead of giving up, and only reports failure once every candidate is
exhausted.

**Failure detection is independent per node.** Every node sweeps its own
`last_seen` timestamps every second, with no coordinator — this is what
lets the network tolerate a partition instead of needing consensus on who's
alive. One consequence: two nodes can briefly disagree about a third node's
liveness (their clocks aren't synchronized), which is why forwarding needs
the failover behavior above rather than trusting the routing table blindly.

**Failure injection (`/admin/*`):** implemented as an in-memory flag
(`SIMULATED_DOWN`) rather than actually killing the process, so a node can
be revived instantly for repeatable demos while behaving exactly like a
real crash to everyone else (stops sending, returns 503 to everything
incoming). A **node crash** can only be discovered by others timing out
(~6s, since a crashed node can't announce anything) — a **single link
failure** is detected by a still-alive node, which floods the change
immediately (converges in well under a second). Both are demonstrated and
timed in `chaos_test.sh`.

## The dashboard

`dashboard.py` is a separate small service — it is not a node, it's an
external observer. It polls every node's `/health` + `/debug` once a
second (ground truth), and is fully data-driven: the frontend has **zero
hardcoded node IDs or positions** — layout, dropdowns, and topology all
come from the live snapshot. Designed to fit one viewport: header +
metrics are fixed height, the three columns below scroll independently,
so you never scroll the page itself.

- **Live Nodes panel**: a real table — ID, address, UP/DOWN, seconds since
  the dashboard last successfully reached it, and its current LSA version
  (link-state sequence number). Click any row to inspect that node.
- **Node Detail — real routing table**: click a node and see its actual
  `destination / next hop / cost` table pulled straight from `/debug`, plus
  topology size known, LSA version, convergence count, and (see below)
  drop-reason breakdown. This is the same shape OSPF/routing-daemon tools
  show — destination, next hop, cost — not a toy status card.
- **10 metric cards**: nodes/links up, delivered, dropped, packets/sec,
  active routes (avg routes known per node), avg path length, topology
  version (sum of every node's LSA sequence number — a monotonic measure
  of total link-state churn), convergence count, avg recompute time.
- **Toast notifications**: pop up for node down/up, congestion, and route
  cost changes — generated from the same real poll-diffing that drives the
  event feed, not scripted.
- **Route cost-change detection**: the dashboard diffs each node's routing
  table between polls and logs the actual old->new cost whenever a route
  changes (`Node D: route to C changed (cost 10 -> 16, via E)`), so a
  reroute is something you can *read*, not just infer from the graph
  moving.
- **Animated packet flow, congestion simulation, one-click demo, and node/
  link spawn/delete/connect** — all as before, all backed by real calls.

### "Why are packets dropping?" (investigated, not hand-waved)

Every node now tracks **categorized** drop reasons (`no_route`,
`ttl_expired`, `next_hop_unreachable`, `next_hop_error`,
`forward_failed`), visible in that node's detail panel and in `/debug`. In
practice, drops you see during normal use come from one of:
- sending to a node mid-failure-detection-window (briefly `no_route` or a
  failed hop before the network catches up -- expected, see the "detection
  lag" note below),
- deliberately sending to an isolated/dead node during testing (correctly
  reported, not a bug -- see `chaos_test.py` Test 6),
- a low starting `ttl` combined with a longer real path than expected.

If you ever see drops you can't explain, the `drop_reasons` breakdown in
the node's detail panel tells you exactly which category -- that's the
answer to "why," not a guess.

**One deliberate, informative "lag"** you'll see in the event feed and
sample route: when a node crashes, the dashboard's own node color flips to
red within ~1s (ground truth), but the highlighted sample route can
briefly still reference that node, because the route comes from another
node's *own* routing table, and that node's failure detector hasn't timed
out yet (usually within ~6s). Same tradeoff as everywhere else in this
project, just visualized live.

### Confirming the architecture question an interviewer would ask first

**A, B, C, D, E are five separate OS processes** (`uvicorn` running
`node.py` five times, on five different ports), not one Python object or a
single-process simulation. Proof, not assertion:
- `run_local.py` launches 5 distinct `subprocess.Popen` calls, each its own
  PID (visible in `ps aux`).
- They only ever talk to each other over real HTTP (`httpx` calls to
  `http://localhost:800X`) -- there's no shared Python object, no shared
  memory, no direct function calls between them.
- Killing one (`kill -9 <pid>`, or `/admin/kill`) doesn't touch the others'
  process space at all; they detect the absence over the network, the same
  way real routers detect a downed peer.
- The dashboard itself is a *sixth*, separate process that only observes
  the other five via HTTP polling -- it has no privileged in-process access
  to any node's state.

## Bugs found and fixed during development

This was built with a real regression suite (`chaos_test.sh`), and it
caught genuine bugs rather than just confirming the happy path:

1. **Route recovery silently never fired.** `heartbeat()` flips a peer's
   status straight to `ALIVE` on receipt, which meant the "node recovered"
   branch in `failure_detector_loop` — the only place calling
   `recompute_routes` for a recovery — never actually ran (by the time it
   looked, the status was already `ALIVE`, so no change was detected).
   Fixed by triggering recompute directly in `heartbeat()` on a DEAD->ALIVE
   transition.

2. **Isolating a node could strand its own last update.** `admin/kill-link`
   only flooded to "remaining direct links + the one just removed." Once a
   node's *last* link is cut, that list can be too small (or empty), and
   the node has no periodic flood targets left to ever announce its final
   state to everyone. Fixed by broadcasting injected topology changes to
   every known peer directly.

3. **Forwarding blindly trusted a single ECMP hop.** If node clocks for
   failure detection are offset by even a second (normal -- they're
   independent), a forwarding node can still pick a next-hop that just
   died, get back a 503, and -- before the fix -- return that error body
   upward as if it were a valid response. Fixed by trying every ECMP
   candidate in load order and only failing once all are exhausted; this
   both fixes the bug and adds genuine request-time failover on top of the
   topology-level self-healing.

4. **Latent thread-safety issue.** Several endpoints were originally sync
   `def`, which FastAPI runs in a background thread pool -- a different OS
   thread than the asyncio loop running the background tasks. Once
   `admin/kill-link` started mutating shared dicts at runtime, that
   combination could raise `RuntimeError: dictionary changed size during
   iteration` under bad timing. Converted every endpoint to `async def` so
   everything runs cooperatively on one thread.

5. **Malformed `LINKS` env var would crash on boot** with an unhandled
   `JSONDecodeError`. Wrapped in a try/except that logs a clear warning and
   falls back to no links instead.

6. **`/debug` silently omitted the message counters.** They were added to
   `/metrics` when packet forwarding was built, but never backported to
   `/debug` — so the dashboard (which uses `/debug` for its one-call
   convenience) showed "Delivered: 0" no matter how much traffic actually
   flowed. Fixed by adding the missing fields, re-verified against a real
   delivered packet.

7. **Drop reasons were unstructured strings.** Early on, drop reasons
   included raw exception text (`f"forward_failed: {e}"`), which is fine
   for a single log line but useless as an aggregate metric — every
   distinct exception message would be its own bucket forever. Refactored
   into a small fixed set of categories (`no_route`, `ttl_expired`,
   `next_hop_unreachable`, `next_hop_error`, `forward_failed`) tracked as
   real counts per node, so "why are packets dropping" has an actual
   queryable answer instead of a pile of one-off strings.

All of the above are covered by explicit assertions in `chaos_test.sh` /
`chaos_test.py`, not just narrated here — rerun them any time the code
changes.

## API reference

| Endpoint | Method | Purpose |
|---|---|---|
| `/join` | POST | New node introduces itself, gets back the peer table |
| `/heartbeat` | POST | Liveness ping + peer-table gossip |
| `/lsa` | POST | Link-state advertisement exchange |
| `/message` | POST | Real packet forwarding (source, destination, ttl, trace) |
| `/admin/kill` | POST | Simulate this node crashing |
| `/admin/revive` | POST | Undo the above |
| `/admin/kill-link/{id}` | POST | Simulate a single link failing |
| `/admin/restore-link/{id}?weight=X` | POST | Undo the above |
| `/peers` | GET | Known peers + believed status + last-seen ages |
| `/topology` | GET | Graph edges as this node currently sees them |
| `/routes` | GET | Routing table (next_hops, cost, paths, convergence timing) |
| `/metrics` | GET | Per-link load, forwarded/delivered/dropped counters |
| `/debug` | GET | Everything above in one call |
| `/health` | GET | Liveness + simulated-down flag |

## What's not built (honest scope)

- **Leader election** -- not implemented; nothing in this project currently
  needs a coordinator, so there was nothing for it to elect a leader for.
- **Network partition *merge* semantics** -- a partition (two surviving
  halves that can't see each other) works correctly as two independent
  reconverged sub-networks, but there's no special reconciliation logic for
  when they reconnect beyond normal LSA re-flooding naturally re-merging
  the topology.
- **Persistence** -- all state is in-memory; a real process crash (not
  `/admin/kill`) loses that node's state and it re-joins from scratch via
  bootstrap, which is realistic but worth knowing going in.
