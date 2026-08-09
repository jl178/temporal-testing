#!/usr/bin/env bash
# End-to-end batch ETL: requires the Temporal compose cluster (localhost:7233)
# and an AWS emulator (AWS_ENDPOINT_URL, default http://localhost:4566).
set -euo pipefail
cd "$(dirname "$0")"
# worker_platform is a repo-level package (usable by any workload)
export PYTHONPATH="$(git rev-parse --show-toplevel)${PYTHONPATH:+:$PYTHONPATH}"

export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localhost:4566}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
# Default: the prod shape — light workers, compute on the external Spark
# service (locally a real Spark container; on AWS an EMR Serverless
# session). SPARK_CONNECT_URI="" opts into the in-process fallback.
export SPARK_CONNECT_URI="${SPARK_CONNECT_URI-sc://localhost:15002}"

if ! curl -s -o /dev/null --max-time 5 "$AWS_ENDPOINT_URL"; then
  echo "ERROR: no AWS emulator at $AWS_ENDPOINT_URL (pip install localemu && localemu start)" >&2
  exit 1
fi

if [ -n "$SPARK_CONNECT_URI" ]; then
  # The Spark service is part of the local stack; start it if needed.
  if ! docker ps --format '{{.Names}}' | grep -q '^etl-spark-connect$'; then
    (cd .. 2>/dev/null || true; docker compose -f "$(git rev-parse --show-toplevel)/docker-compose.spark.yml" up -d)
  fi
  echo "==> Waiting for Spark Connect server at ${SPARK_CONNECT_URI}"
  port="${SPARK_CONNECT_URI##*:}"
  for _ in $(seq 1 60); do
    if (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then exec 3>&-; break; fi
    sleep 5
  done
fi

if [ ! -d .venv ]; then
  python3 -m venv .venv
fi
./.venv/bin/pip install -q -r requirements.txt

# Generic worker platform: queue + size profile + code. Workflow
# coordination is small; I/O-bound activities are medium; the big-compute
# lane (large) only spins up for the in-process fallback — in the default
# prod-shaped mode all fleets are light and Spark is an external service.
# Clean up workers orphaned by interrupted earlier runs — a stale poller
# racing a live one corrupts shared state.
pkill -f "worker_platfor[m] --queue" 2>/dev/null || true
sleep 1

PIDS=()
./.venv/bin/python -m worker_platform --queue etl-pipeline --profile small \
  --workflows workflow &                                          PIDS+=($!)
./.venv/bin/python -m worker_platform --queue etl-pipeline --profile medium \
  --activities activities &                                       PIDS+=($!)
if [ -z "$SPARK_CONNECT_URI" ]; then
  ./.venv/bin/python -m worker_platform --queue compute-large --profile large \
    --activities activities:run_local_transform &                 PIDS+=($!)
fi
trap 'kill "${PIDS[@]}" 2>/dev/null || true' EXIT
sleep 2

./.venv/bin/python starter.py
