#!/usr/bin/env bash
# End-to-end: install deps, start worker, execute workflow, assert result.
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -d .venv ]; then
  python3 -m venv .venv
fi
./.venv/bin/pip install -q -r requirements.txt

./.venv/bin/python worker.py &
WORKER_PID=$!
trap 'kill $WORKER_PID 2>/dev/null || true' EXIT
sleep 2

./.venv/bin/python starter.py
