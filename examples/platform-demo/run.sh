#!/usr/bin/env bash
# A non-ETL team onboarding onto the worker platform: three fleets (queue +
# profile + code) and a starter. Reuses the etl venv purely for the
# temporalio dependency — the code and queues are billing's own.
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(git rev-parse --show-toplevel)"
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
PY="$ROOT/etl/.venv/bin/python"

pkill -f "worker_platfor[m] --queue billing" 2>/dev/null || true
# pkill can't reach CONTAINERIZED stale workers (a yesterday's-image
# worker polling this queue serves stale code) — stop those too.
docker ps -q --filter "ancestor=temporal-testing/billing-worker:local" | xargs -r docker stop 2>/dev/null || true
sleep 1

PIDS=()
"$PY" -m worker_platform --queue billing --profile small \
  --workflows billing &                              PIDS+=($!)
"$PY" -m worker_platform --queue billing --profile medium \
  --activities billing:prepare_invoice &             PIDS+=($!)
"$PY" -m worker_platform --queue billing-render --profile large \
  --activities billing:render_invoice &              PIDS+=($!)
trap 'kill "${PIDS[@]}" 2>/dev/null || true' EXIT
sleep 3

"$PY" starter.py
