#!/usr/bin/env bash
# End-to-end: fetch deps, start worker, execute workflow, assert result.
set -euo pipefail
cd "$(dirname "$0")"

go mod tidy
go build -o bin/worker ./worker
go build -o bin/starter ./starter

./bin/worker &
WORKER_PID=$!
trap 'kill $WORKER_PID 2>/dev/null || true' EXIT
sleep 2

./bin/starter
