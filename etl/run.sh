#!/usr/bin/env bash
# End-to-end batch ETL: requires the Temporal compose cluster (localhost:7233)
# and an AWS emulator (AWS_ENDPOINT_URL, default http://localhost:4566).
set -euo pipefail
cd "$(dirname "$0")"

export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localhost:4566}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

if ! curl -s -o /dev/null --max-time 5 "$AWS_ENDPOINT_URL"; then
  echo "ERROR: no AWS emulator at $AWS_ENDPOINT_URL (pip install localemu && localemu start)" >&2
  exit 1
fi

if [ ! -d .venv ]; then
  python3 -m venv .venv
fi
./.venv/bin/pip install -q -r requirements.txt

# Light fleet (workflows + launcher/validation) and heavy fleet (the
# Spark-spawning transform) poll separate queues — noisy-neighbor isolation.
./.venv/bin/python worker.py &
WORKER_PID=$!
./.venv/bin/python heavy_worker.py &
HEAVY_PID=$!
trap 'kill $WORKER_PID $HEAVY_PID 2>/dev/null || true' EXIT
sleep 2

./.venv/bin/python starter.py
