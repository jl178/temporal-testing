#!/usr/bin/env bash
# End-to-end file-ingestion pipeline (the complex batch):
#   SFTP -> S3 landing -> route by filename pattern -> child transform per
#   route (cluster-side hygiene + dbt via the canonical model) -> curated,
#   with quarantine for nonconforming files and optional consolidation.
#
# Requires: Temporal (localhost:7233) and an AWS emulator (:4566); the SFTP
# and Spark containers auto-start. Seeds a 4-file vendor batch over SFTP.
set -euo pipefail
cd "$(dirname "$0")/.."
# worker_platform is a repo-level package (usable by any workload)
export PYTHONPATH="$(git rev-parse --show-toplevel)${PYTHONPATH:+:$PYTHONPATH}"

export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localhost:4566}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
# Default: prod shape — light workers, compute on the external Spark service.
export SPARK_CONNECT_URI="${SPARK_CONNECT_URI-sc://localhost:15002}"

if [ -n "$SPARK_CONNECT_URI" ]; then
  if ! docker ps --format '{{.Names}}' | grep -q '^etl-spark-connect$'; then
    docker compose -f "$(git rev-parse --show-toplevel)/docker-compose.spark.yml" up -d
  fi
  port="${SPARK_CONNECT_URI##*:}"
  for _ in $(seq 1 60); do
    if (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then exec 3>&-; break; fi
    sleep 5
  done
fi

if ! curl -s -o /dev/null --max-time 5 "$AWS_ENDPOINT_URL"; then
  echo "ERROR: no AWS emulator at $AWS_ENDPOINT_URL (pip install localemu && localemu start)" >&2
  exit 1
fi
# Custom search attributes used by the pipeline (idempotent).
for attr in BatchId Route SourceFile; do
  temporal operator search-attribute create --name "$attr" --type Keyword \
    --address "${TEMPORAL_ADDRESS:-localhost:7233}" >/dev/null 2>&1 || true
done

# SFTP test server is part of the dev compose config; start it if needed.
if ! docker ps --format '{{.Names}}' | grep -q '^etl-sftp$'; then
  (cd .. && docker compose up -d sftp)
  sleep 2
fi

if [ ! -d .venv ]; then
  python3 -m venv .venv
fi
./.venv/bin/pip install -q -r requirements.txt

# Seed a multi-source vendor batch onto the SFTP server: three routed files
# (messy headers on purpose) plus one structurally broken file that must end
# up quarantined without failing the batch.
TMP_DIR=$(mktemp -d)
cat > "$TMP_DIR/orders_2026-08.csv" <<'EOF'
Order ID,Customer,Order Date,Amount,Status,record_type
1,acme,2026-08-01,120.50,completed,orders
2,acme,2026-08-01,80.00,completed,orders
3,globex,2026-08-01,42.25,CANCELLED,orders
4,globex,2026-08-02,310.10,completed,orders
5,initech,2026-08-02,55.99,Completed,orders
6,initech,2026-08-03,12.00,pending,orders
7,acme,2026-08-03,99.95,completed,orders
EOF
# Deliberately vendor-messy headers: Seg/RGN alias to segment/region via
# the route spec's column_aliases (semantic normalization, cluster-side).
cat > "$TMP_DIR/customers_2026-08.csv" <<'EOF'
Customer,Seg,RGN,record_type
acme,Enterprise,US,customers
globex,SMB,EU,customers
initech,SMB,US,customers
EOF
cat > "$TMP_DIR/payments_2026-08.csv" <<'EOF'
Payment ID,Order ID,Paid Amount,Paid Date,record_type
901,1,120.50,2026-08-02,payments
902,2,80.00,2026-08-02,payments
903,4,310.10,2026-08-03,payments
904,5,55.99,2026-08-03,payments
EOF
head -c 200 /dev/urandom > "$TMP_DIR/zz_broken.csv"
for f in "$TMP_DIR"/*.csv; do
  docker cp "$f" "etl-sftp:/home/demo/upload/$(basename "$f")"
done
rm -rf "$TMP_DIR"

# Split fleets per queue — workflow-only workers separate from activity
# workers, and the heavy (Spark-spawning, resource-tuned) fleet on its own
# queue. No activity can delay workflow progress; heavy work can only hurt
# heavy work.
# Generic worker platform: each fleet = queue + size profile + code.
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
./.venv/bin/python -m worker_platform --queue file-ingest --profile small \
  --workflows ingest.workflow &                                   PIDS+=($!)
./.venv/bin/python -m worker_platform --queue file-ingest --profile medium \
  --activities ingest.activities &                                PIDS+=($!)
trap 'kill "${PIDS[@]}" 2>/dev/null || true' EXIT
sleep 3

./.venv/bin/python -m ingest.starter
