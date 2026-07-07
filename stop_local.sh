#!/bin/bash
cd "$(dirname "$0")"
if [ -f pids/all.pid ]; then
  while read -r pid; do
    kill "$pid" 2>/dev/null && echo "stopped pid $pid"
  done < pids/all.pid
  rm -f pids/all.pid
else
  echo "no pids file found"
fi
