"""
AutoMesh — cross-platform chaos/regression test suite

Same 7 scenarios as chaos_test.sh, reimplemented in pure Python (subprocess +
httpx) so it runs identically on Windows, macOS, and Linux.

Usage:
    python chaos_test.py
"""

import json
import os
import subprocess
import sys
import time

import httpx

HERE = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(HERE, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

NODES = [
    ("A", 8001, None, {"B": 10}),
    ("B", 8002, 8001, {"A": 10, "C": 5, "D": 5}),
    ("C", 8003, 8001, {"B": 5, "E": 8}),
    ("D", 8004, 8001, {"B": 5, "E": 8}),
    ("E", 8005, 8001, {"C": 8, "D": 8}),
]

processes = []
failures = 0


def ok(msg):
    print(f"  PASS: {msg}")


def bad(msg):
    global failures
    failures += 1
    print(f"  FAIL: {msg}")


def start(label, args, env, stagger=0.0):
    if stagger:
        time.sleep(stagger)
    logfile = open(os.path.join(LOG_DIR, f"{label}.log"), "w")
    full_env = os.environ.copy()
    full_env.update(env)
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", *args],
        cwd=HERE, env=full_env, stdout=logfile, stderr=subprocess.STDOUT,
    )
    processes.append((label, proc, logfile))


def launch_network():
    print("=== launching 5-node network ===")
    for i, (nid, port, bootstrap_port, links) in enumerate(NODES):
        env = {"NODE_ID": nid, "NODE_URL": f"http://localhost:{port}", "PORT": str(port), "LINKS": json.dumps(links)}
        if bootstrap_port:
            env["BOOTSTRAP_URL"] = f"http://localhost:{bootstrap_port}"
        args = ["node:app", "--host", "0.0.0.0", "--port", str(port), "--log-level", "warning"]
        start(f"node_{nid}", args, env, stagger=1.0 if i == 1 else 0.0)
    print("waiting 9s for discovery + LSA flooding to converge...")
    time.sleep(9)


def cleanup():
    print("\n=== cleanup ===")
    for label, proc, logfile in processes:
        proc.terminate()
    for label, proc, logfile in processes:
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        logfile.close()


def get(port, path):
    return httpx.get(f"http://localhost:{port}{path}", timeout=3).json()


def post(port, path, json_body=None):
    r = httpx.post(f"http://localhost:{port}{path}", json=json_body, timeout=3)
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, {}


def main():
    launch_network()

    print("\n=== TEST 1: discovery + ECMP present at startup ===")
    peers = get(8001, "/peers")
    if len(peers["known_peers"]) == 5:
        ok("A discovered all 5 peers")
    else:
        bad(f"A only sees {len(peers['known_peers'])} peers")

    routes_b = get(8002, "/routes")
    ecmp = len(routes_b["routing_table"].get("E", {}).get("next_hops", []))
    if ecmp == 2:
        ok("B has ECMP (2 next_hops) to E")
    else:
        bad(f"B has {ecmp} next_hops to E, expected 2")

    print("\n=== TEST 2: node crash via API (admin/kill) ===")
    t0 = time.time()
    post(8003, "/admin/kill")
    converged = False
    for i in range(15):
        time.sleep(1)
        routes_a = get(8001, "/routes")
        if "C" not in routes_a["routing_table"]:
            print(f"  A excluded C from routing table after {time.time()-t0:.2f}s ({i+1} polls)")
            converged = True
            break
    ok("network converged after node crash") if converged else bad("A never excluded C from routes")

    routes_a = get(8001, "/routes")
    e_paths = routes_a["routing_table"].get("E", {}).get("paths", [[]])
    path = e_paths[0] if e_paths else []
    print(f"  A's new path to E: {path}")
    ok("A's path to E avoids dead node C") if "C" not in path else bad("A's path to E still routes through C")

    print("\n=== TEST 3: real traffic still delivers correctly after the crash ===")
    _, msg = post(8001, "/message", {"source": "A", "destination": "E", "payload": "post-crash-test", "ttl": 10, "trace": []})
    print(f"  {msg}")
    if msg.get("delivered"):
        ok("message delivered A->E despite C being down")
    else:
        bad(f"message failed to deliver: {msg}")
    if "C" not in msg.get("trace", []):
        ok("trace correctly avoids C")
    else:
        bad("delivered message trace still passed through dead node C")

    print("\n=== TEST 4: revive C, confirm ECMP comes back ===")
    post(8003, "/admin/revive")
    recovered = False
    for i in range(10):
        time.sleep(1)
        routes_b = get(8002, "/routes")
        if len(routes_b["routing_table"].get("E", {}).get("next_hops", [])) == 2:
            print(f"  B's ECMP to E restored after {i+1}s")
            recovered = True
            break
    ok("ECMP restored after revival") if recovered else bad("ECMP never came back after reviving C")

    print("\n=== TEST 5: single-link failure converges fast ===")
    t0 = time.time()
    post(8002, "/admin/kill-link/C")
    converged = False
    for i in range(20):
        time.sleep(0.3)
        routes_b = get(8002, "/routes")
        if len(routes_b["routing_table"].get("E", {}).get("next_hops", [])) == 1:
            print(f"  B's ECMP collapsed to single path in {time.time()-t0:.2f}s")
            converged = True
            break
    ok("link-kill converged fast (sub-heartbeat-timeout)") if converged else bad("B still shows ECMP after killing link B-C")
    post(8002, "/admin/restore-link/C?weight=5")
    time.sleep(2)

    print("\n=== TEST 6: full node isolation ===")
    post(8003, "/admin/kill-link/B")
    post(8003, "/admin/kill-link/E")
    time.sleep(2)
    routes_a = get(8001, "/routes")
    if "C" not in routes_a["routing_table"]:
        ok("A correctly has no route to isolated C")
    else:
        bad("A still thinks it can reach isolated C")
    _, msg = post(8001, "/message", {"source": "A", "destination": "C", "payload": "x", "ttl": 10, "trace": []})
    print(f"  message to isolated C: {msg}")
    if msg.get("reason") == "no_route":
        ok("message to isolated node correctly reports no_route")
    else:
        bad(f"unexpected response sending to isolated node: {msg}")
    post(8003, "/admin/restore-link/B?weight=5")
    post(8003, "/admin/restore-link/E?weight=8")

    print("\n=== TEST 7: malformed LINKS env var doesn't crash the node ===")
    env = {"NODE_ID": "Z", "NODE_URL": "http://localhost:8009", "PORT": "8009", "LINKS": "not-json"}
    start("node_Z", ["node:app", "--host", "0.0.0.0", "--port", "8009", "--log-level", "warning"], env)
    time.sleep(2)
    with open(os.path.join(LOG_DIR, "node_Z.log")) as f:
        z_log = f.read()
    ok("malformed LINKS logged and handled gracefully") if "not valid JSON" in z_log else bad("malformed LINKS did not produce expected warning")

    cleanup()

    print("")
    if failures == 0:
        print("ALL TESTS PASSED")
        sys.exit(0)
    else:
        print(f"{failures} TEST(S) FAILED -- see above")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        cleanup()
        raise
