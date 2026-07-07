"""
AutoMesh — cross-platform launcher

Works on Windows, macOS, and Linux (pure subprocess, no shell scripting).
Launches the 5-node demo network + dashboard, waits for Ctrl+C, then
cleanly terminates everything.

Usage:
    python run_local.py
"""

import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(HERE, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# (node_id, port, bootstrap_port_or_None, links_json)
# Weighted links match the original diagram: A-B, B-C, B-D, C-E, D-E
NODES = [
    ("A", 8001, None, {"B": 10}),
    ("B", 8002, 8001, {"A": 10, "C": 5, "D": 5}),
    ("C", 8003, 8001, {"B": 5, "E": 8}),
    ("D", 8004, 8001, {"B": 5, "E": 8}),
    ("E", 8005, 8001, {"C": 8, "D": 8}),
]
DASHBOARD_PORT = 9000

processes = []  # list of (label, Popen, logfile handle)


def start(label: str, args: list, env: dict, stagger: float = 0.0):
    if stagger:
        time.sleep(stagger)
    logfile = open(os.path.join(LOG_DIR, f"{label}.log"), "w")
    full_env = os.environ.copy()
    full_env.update(env)
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", *args],
        cwd=HERE,
        env=full_env,
        stdout=logfile,
        stderr=subprocess.STDOUT,
    )
    processes.append((label, proc, logfile))
    print(f"  started {label} (pid {proc.pid})")


def main():
    print("=== launching AutoMesh 5-node network ===")
    for i, (node_id, port, bootstrap_port, links) in enumerate(NODES):
        env = {
            "NODE_ID": node_id,
            "NODE_URL": f"http://localhost:{port}",
            "PORT": str(port),
            "LINKS": json.dumps(links),
        }
        if bootstrap_port:
            env["BOOTSTRAP_URL"] = f"http://localhost:{bootstrap_port}"
        args = ["node:app", "--host", "0.0.0.0", "--port", str(port), "--log-level", "warning"]
        # give the bootstrap node (A) a 1s head start, same as the original .sh version
        start(f"node_{node_id}", args, env, stagger=1.0 if i == 1 else 0.0)

    nodes_json = json.dumps({nid: f"http://localhost:{port}" for nid, port, _, _ in NODES})
    time.sleep(2)
    start(
        "dashboard",
        ["dashboard:app", "--host", "0.0.0.0", "--port", str(DASHBOARD_PORT), "--log-level", "warning"],
        {"NODES_JSON": nodes_json},
    )

    print("")
    print("All 5 nodes + dashboard launched. Give them ~6-8s to converge, then open:")
    print(f"  http://localhost:{DASHBOARD_PORT}")
    print("Or from another terminal:")
    print("  curl http://localhost:8001/peers")
    print("")
    print("Logs are in ./logs/. Press Ctrl+C here to stop everything.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n=== stopping all processes ===")
        for label, proc, logfile in processes:
            proc.terminate()
        for label, proc, logfile in processes:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            logfile.close()
            print(f"  stopped {label}")
        print("done")


if __name__ == "__main__":
    main()
