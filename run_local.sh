#!/bin/bash
# Launches the A-B-C-D-E demo network from the diagram, all on localhost.
# A acts as the first contact point purely because it starts first --
# it has no special authority once the network is up.
set -e
cd "$(dirname "$0")"
mkdir -p logs pids
rm -f pids/all.pid

start_node() {
  local id=$1 port=$2 bootstrap=$3 links=$4
  if [ -n "$bootstrap" ]; then
    NODE_ID=$id NODE_URL="http://localhost:$port" PORT=$port BOOTSTRAP_URL=$bootstrap LINKS="$links" \
      nohup python3 -m uvicorn node:app --host 0.0.0.0 --port "$port" --log-level warning \
      > "logs/node_$id.log" 2>&1 &
  else
    NODE_ID=$id NODE_URL="http://localhost:$port" PORT=$port LINKS="$links" \
      nohup python3 -m uvicorn node:app --host 0.0.0.0 --port "$port" --log-level warning \
      > "logs/node_$id.log" 2>&1 &
  fi
  echo $! >> pids/all.pid
  echo "started node $id on port $port (pid $!)"
}

# Weighted links match the original diagram: A-B, B-C, B-D, C-E, D-E
# (weight = simulated latency in ms). Declared on both ends of each edge.
start_node A 8001 "" '{"B":10}'
sleep 1
start_node B 8002 "http://localhost:8001" '{"A":10,"C":5,"D":5}'
start_node C 8003 "http://localhost:8001" '{"B":5,"E":8}'
start_node D 8004 "http://localhost:8001" '{"B":5,"E":8}'
start_node E 8005 "http://localhost:8001" '{"C":8,"D":8}'

sleep 2
NODES_JSON='{"A":"http://localhost:8001","B":"http://localhost:8002","C":"http://localhost:8003","D":"http://localhost:8004","E":"http://localhost:8005"}' \
  nohup python3 -m uvicorn dashboard:app --host 0.0.0.0 --port 9000 --log-level warning > logs/dashboard.log 2>&1 &
echo $! >> pids/all.pid
echo "started dashboard on port 9000"

echo ""
echo "All 5 nodes + dashboard launching. Give them ~6s to converge, then open:"
echo "  http://localhost:9000"
echo "Or via terminal:"
echo "  curl -s localhost:8001/peers | python3 -m json.tool"
echo "Logs are in ./logs/, stop everything with ./stop_local.sh"
