#!/usr/bin/env bash
# End-to-end: install deps, build, start worker, execute workflow, assert result.
set -euo pipefail
cd "$(dirname "$0")"

npm install --no-fund --no-audit --loglevel=error
npm run build

node lib/worker.js &
WORKER_PID=$!
trap 'kill $WORKER_PID 2>/dev/null || true' EXIT
sleep 3

node lib/starter.js
