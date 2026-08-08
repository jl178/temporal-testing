#!/usr/bin/env bash
# End-to-end file-ingestion pipeline:
#   SFTP -> S3 landing -> rules-based parse -> S3 staged -> classify ->
#   dispatch transform-spec via child workflow -> S3 curated
#
# Requires: Temporal (localhost:7233), an AWS emulator (:4566), and the SFTP
# test container (nix run .#sftp-up). Seeds a demo vendor file over SFTP.
set -euo pipefail
cd "$(dirname "$0")/.."

export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localhost:4566}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

if ! curl -s -o /dev/null --max-time 5 "$AWS_ENDPOINT_URL"; then
  echo "ERROR: no AWS emulator at $AWS_ENDPOINT_URL (pip install localemu && localemu start)" >&2
  exit 1
fi
# SFTP test server is part of the dev compose config; start it if needed.
if ! docker ps --format '{{.Names}}' | grep -q '^etl-sftp$'; then
  (cd .. && docker compose up -d sftp)
  sleep 2
fi

if [ ! -d .venv ]; then
  python3 -m venv .venv
fi
./.venv/bin/pip install -q -r requirements.txt

# Seed a messy vendor file onto the SFTP server.
TMP_CSV=$(mktemp --suffix=.csv)
cat > "$TMP_CSV" <<'EOF'
Order ID,Customer,Order Date,Amount,Status,record_type
1,acme,2026-08-01,120.50,completed,orders
2,acme,2026-08-01,80.00,completed,orders
3,globex,2026-08-01,42.25,CANCELLED,orders
4,globex,2026-08-02,310.10,completed,orders
5,initech,2026-08-02,55.99,Completed,orders
6,initech,2026-08-03,12.00,pending,orders
7,acme,2026-08-03,99.95,completed,orders
EOF
docker cp "$TMP_CSV" etl-sftp:/home/demo/upload/orders_2026-08.csv
rm -f "$TMP_CSV"

# Transform worker (child workflows) + ingest worker (parent workflow).
./.venv/bin/python worker.py &
ETL_WORKER_PID=$!
./.venv/bin/python -m ingest.worker &
INGEST_WORKER_PID=$!
trap 'kill $ETL_WORKER_PID $INGEST_WORKER_PID 2>/dev/null || true' EXIT
sleep 3

./.venv/bin/python -m ingest.starter
