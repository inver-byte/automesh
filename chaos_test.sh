#!/bin/bash
# Chaos test harness for AutoMesh. Exercises node crash, single-link failure,
# full node isolation, and revival -- measuring real network-wide convergence
# time for each, not just checking a single node's local view.
set -e
cd "$(dirname "$0")"
mkdir -p logs pids
rm -f pids/all.pid logs/*.log

start_node() {
  local id=$1 port=$2 bootstrap=$3 links=$4
  if [ -n "$bootstrap" ]; then
    NODE_ID=$id NODE_URL="http://localhost:$port" PORT=$port BOOTSTRAP_URL=$bootstrap LINKS="$links" \
      python3 -m uvicorn node:app --host 0.0.0.0 --port "$port" --log-level warning > "logs/node_$id.log" 2>&1 &
  else
    NODE_ID=$id NODE_URL="http://localhost:$port" PORT=$port LINKS="$links" \
      python3 -m uvicorn node:app --host 0.0.0.0 --port "$port" --log-level warning > "logs/node_$id.log" 2>&1 &
  fi
  echo $! >> pids/all.pid
}

pass() { echo "  PASS: $1"; }
fail() { echo "  FAIL: $1"; FAILURES=$((FAILURES+1)); }
FAILURES=0

echo "=== launching 5-node network ==="
start_node A 8001 "" '{"B":10}'
sleep 1
start_node B 8002 "http://localhost:8001" '{"A":10,"C":5,"D":5}'
start_node C 8003 "http://localhost:8001" '{"B":5,"E":8}'
start_node D 8004 "http://localhost:8001" '{"B":5,"E":8}'
start_node E 8005 "http://localhost:8001" '{"C":8,"D":8}'
sleep 9

echo ""
echo "=== TEST 1: discovery + ECMP present at startup ==="
A_PEERS=$(curl -s localhost:8001/peers | python3 -c "import json,sys; print(len(json.load(sys.stdin)['known_peers']))")
[ "$A_PEERS" = "5" ] && pass "A discovered all 5 peers" || fail "A only sees $A_PEERS peers"

B_ECMP=$(curl -s localhost:8002/routes | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d['routing_table'].get('E',{}).get('next_hops',[])))")
[ "$B_ECMP" = "2" ] && pass "B has ECMP (2 next_hops) to E" || fail "B has $B_ECMP next_hops to E, expected 2"

echo ""
echo "=== TEST 2: node crash via API (admin/kill), no self-notification possible ==="
T0=$(python3 -c "import time; print(time.time())")
curl -s -X POST localhost:8003/admin/kill > /dev/null
echo "killed C at injection time, polling A until C is excluded from A's routes..."
CONVERGED=0
for i in $(seq 1 15); do
  sleep 1
  HAS_C=$(curl -s localhost:8001/routes | python3 -c "import json,sys; d=json.load(sys.stdin); print('C' in d['routing_table'])" 2>/dev/null || echo "True")
  if [ "$HAS_C" = "False" ]; then
    T1=$(python3 -c "import time; print(time.time())")
    ELAPSED=$(python3 -c "print(f'{$T1 - $T0:.2f}')")
    echo "  A excluded C from routing table after ${ELAPSED}s (${i} polls)"
    CONVERGED=1
    break
  fi
done
[ "$CONVERGED" = "1" ] && pass "network converged after node crash" || fail "A never excluded C from routes"

E_ROUTE=$(curl -s localhost:8001/routes | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['routing_table'].get('E',{}).get('paths',[[]])[0])")
echo "  A's new path to E: $E_ROUTE"
echo "$E_ROUTE" | grep -q "'C'" && fail "A's path to E still routes through dead node C" || pass "A's path to E avoids dead node C"

echo ""
echo "=== TEST 3: real traffic still delivers correctly after the crash ==="
TRACE=$(curl -s -X POST localhost:8001/message -H "Content-Type: application/json" \
  -d '{"source":"A","destination":"E","payload":"post-crash-test","ttl":10,"trace":[]}' \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(d)")
echo "  $TRACE"
echo "$TRACE" | grep -q "'delivered': True" && pass "message delivered A->E despite C being down" || fail "message failed to deliver: $TRACE"
echo "$TRACE" | grep -q "'C'" && fail "delivered message trace still passed through dead node C" || pass "trace correctly avoids C"

echo ""
echo "=== TEST 4: revive C, confirm it rejoins and ECMP comes back ==="
curl -s -X POST localhost:8003/admin/revive > /dev/null
RECOVERED=0
for i in $(seq 1 10); do
  sleep 1
  B_ECMP2=$(curl -s localhost:8002/routes | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d['routing_table'].get('E',{}).get('next_hops',[])))" 2>/dev/null || echo 0)
  if [ "$B_ECMP2" = "2" ]; then
    echo "  B's ECMP to E restored after ${i}s"
    RECOVERED=1
    break
  fi
done
[ "$RECOVERED" = "1" ] && pass "ECMP restored after revival" || fail "ECMP never came back after reviving C"

echo ""
echo "=== TEST 5: single-link failure (not a node crash) converges fast ==="
T0=$(python3 -c "import time; print(time.time())")
curl -s -X POST localhost:8002/admin/kill-link/C > /dev/null
CONVERGED=0
for i in $(seq 1 20); do
  sleep 0.3
  B_ECMP3=$(curl -s localhost:8002/routes | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d['routing_table'].get('E',{}).get('next_hops',[])))" 2>/dev/null || echo -1)
  if [ "$B_ECMP3" = "1" ]; then
    T1=$(python3 -c "import time; print(time.time())")
    ELAPSED=$(python3 -c "print(f'{$T1 - $T0:.2f}')")
    echo "  B's ECMP collapsed to single path in ${ELAPSED}s (link-only failures don't need the 6s heartbeat timeout)"
    CONVERGED=1
    break
  fi
done
[ "$CONVERGED" = "1" ] && pass "link-kill converged fast (sub-heartbeat-timeout)" || fail "B still shows ECMP after killing link B-C"

curl -s -X POST "localhost:8002/admin/restore-link/C?weight=5" > /dev/null
sleep 2

echo ""
echo "=== TEST 6: full node isolation (kill all of C's links) -> C unreachable ==="
curl -s -X POST localhost:8003/admin/kill-link/B > /dev/null
curl -s -X POST localhost:8003/admin/kill-link/E > /dev/null
sleep 2
A_HAS_C=$(curl -s localhost:8001/routes | python3 -c "import json,sys; d=json.load(sys.stdin); print('C' in d['routing_table'])")
[ "$A_HAS_C" = "False" ] && pass "A correctly has no route to isolated C" || fail "A still thinks it can reach isolated C"

MSG_TO_C=$(curl -s -X POST localhost:8001/message -H "Content-Type: application/json" \
  -d '{"source":"A","destination":"C","payload":"should not arrive","ttl":10,"trace":[]}')
echo "  message to isolated C: $MSG_TO_C"
echo "$MSG_TO_C" | grep -q '"no_route"' && pass "message to isolated node correctly reports no_route" || fail "unexpected response sending to isolated node: $MSG_TO_C"

# restore for cleanliness
curl -s -X POST "localhost:8003/admin/restore-link/B?weight=5" > /dev/null
curl -s -X POST "localhost:8003/admin/restore-link/E?weight=8" > /dev/null

echo ""
echo "=== TEST 7: malformed LINKS env var doesn't crash the node ==="
NODE_ID=Z NODE_URL="http://localhost:8009" PORT=8009 LINKS='not-json' \
  timeout 2 python3 -m uvicorn node:app --host 0.0.0.0 --port 8009 --log-level warning > logs/node_Z.log 2>&1 || true
grep -q "not valid JSON" logs/node_Z.log && pass "malformed LINKS logged and handled gracefully" || fail "malformed LINKS did not produce expected warning (check logs/node_Z.log)"

echo ""
echo "=== cleanup ==="
while read -r pid; do kill -9 "$pid" 2>/dev/null; done < pids/all.pid
rm -f pids/all.pid

echo ""
if [ "$FAILURES" = "0" ]; then
  echo "ALL TESTS PASSED"
else
  echo "$FAILURES TEST(S) FAILED -- see above"
  exit 1
fi
