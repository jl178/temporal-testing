#!/usr/bin/env bash
# Order pricing & settlement — the lifecycle-tenant e2e:
#   vendor batch file over SFTP -> ingest (land + route, no transforms)
#   -> one durable lifecycle per order (validate -> contract lookup ->
#   price -> variance gate: auto | reviewer signal | SLA escalation)
#   -> outbound remittance file (S3 + vendor file server)
#   -> settlements mart via the generic dbt pipeline.
#
# Requires: Temporal (:7233) and the AWS emulator (:4566); SFTP and Spark
# containers auto-start. The review console acts mid-run — that's the demo.
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(git rev-parse --show-toplevel)"
export PYTHONPATH="$ROOT:$ROOT/etl${PYTHONPATH:+:$PYTHONPATH}"
PY="$ROOT/etl/.venv/bin/python"

export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localhost:4566}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export SPARK_CONNECT_URI="${SPARK_CONNECT_URI-sc://localhost:15002}"

if [ -n "$SPARK_CONNECT_URI" ]; then
  if ! docker ps --format '{{.Names}}' | grep -q '^etl-spark-connect$'; then
    docker compose -f "$ROOT/docker-compose.spark.yml" up -d
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

# Search attributes BEFORE anything starts: start-time typed attributes
# fail on unregistered names.
for attr in BatchId Route SourceFile OrderId Stage; do
  temporal operator search-attribute create --name "$attr" --type Keyword \
    --address "${TEMPORAL_ADDRESS:-localhost:7233}" >/dev/null 2>&1 || true
done

# SFTP server — unconditional `up -d`: compose recreates the container
# when its command changed (the outbound dir is provisioned at creation).
(cd "$ROOT" && docker compose up -d sftp)
sleep 2

if [ ! -d "$ROOT/etl/.venv" ]; then
  python3 -m venv "$ROOT/etl/.venv"
fi
"$PY" -m pip install -q -r "$ROOT/etl/requirements.txt"

# Seed ONE vendor batch. Stale drops from other e2e legs would join our
# ingest child's batch — clear both directions first.
docker exec etl-sftp sh -c 'rm -f /home/demo/upload/* /home/demo/outbound/*' 2>/dev/null || true
TMP_DIR=$(mktemp -d)
cat > "$TMP_DIR/vendor_orders_2026-08.csv" <<'EOF'
order_ref,vendor,item_count,submitted_amount
ORD-1001,acme,10,385.00
ORD-1002,acme,4,152.00
ORD-1003,globex,6,150.00
ORD-1004,globex,8,260.00
ORD-1005,initech,20,300.00
ORD-1006,initech,,
EOF
docker cp "$TMP_DIR/vendor_orders_2026-08.csv" etl-sftp:/home/demo/upload/
rm -rf "$TMP_DIR"

# Stale-worker guards (broad: we start ingest + etl fleets ourselves).
pkill -f "worker_platfor[m] --queue" 2>/dev/null || true
docker ps -q --filter "ancestor=temporal-testing/etl-worker:local" | xargs -r docker stop 2>/dev/null || true
docker ps -q --filter "ancestor=temporal-testing/settlement-worker:local" | xargs -r docker stop 2>/dev/null || true
sleep 1

# Fleets: queue + profile + code (platform reuse + this tenant's three).
# etl modules (workflow/activities/ingest.*) resolve via PYTHONPATH=$ROOT/etl.
PIDS=()
"$PY" -m worker_platform --queue file-ingest --profile small \
  --workflows ingest.workflow &                                  PIDS+=($!)
"$PY" -m worker_platform --queue file-ingest --profile medium \
  --activities ingest.activities &                               PIDS+=($!)
"$PY" -m worker_platform --queue etl-pipeline --profile small \
  --workflows workflow &                                         PIDS+=($!)
"$PY" -m worker_platform --queue etl-pipeline --profile medium \
  --activities activities &                                      PIDS+=($!)
if [ -z "$SPARK_CONNECT_URI" ]; then
  "$PY" -m worker_platform --queue compute-large --profile large \
    --activities activities:run_local_transform &                PIDS+=($!)
fi
"$PY" -m worker_platform --queue settlement-intake --profile small \
  --workflows pricing:OrderBatchWorkflow &                       PIDS+=($!)
"$PY" -m worker_platform --queue settlement-intake --profile medium \
  --activities pricing:split_batch,write_remittance,resolve_settlements_spec & PIDS+=($!)
"$PY" -m worker_platform --queue settlement-orders --profile small \
  --workflows pricing:OrderPricingWorkflow &                     PIDS+=($!)
"$PY" -m worker_platform --queue settlement-contracts --profile medium \
  --activities contracts:lookup_contract &                       PIDS+=($!)
trap 'kill "${PIDS[@]}" 2>/dev/null || true; pkill -f "worker_platfor[m] --queue" 2>/dev/null || true' EXIT
sleep 3

# Short SLA so the un-reviewed order escalates within the run; the console
# approves ORD-1004 well inside it.
export REVIEW_SLA_SECONDS="${REVIEW_SLA_SECONDS:-45}"
export VARIANCE_THRESHOLD_PCT="${VARIANCE_THRESHOLD_PCT:-10}"

WF_ID=$("$PY" starter.py start)
echo "batch: $WF_ID"

# The reviewer's queue is a visibility query (Stage='awaiting_review').
"$PY" review_console.py list --wait-seconds 90 --min 1
"$PY" review_console.py approve --order ORD-1004 --note "rate verified" --wait-seconds 90
# ORD-1005 deliberately gets no decision -> escalates at the SLA.

"$PY" starter.py await --id "$WF_ID"

# The outbound leg reached the vendor's file server.
docker exec etl-sftp sh -c 'ls /home/demo/outbound' | grep -q "remit_" \
  && echo "OUTBOUND REMITTANCE: PASS"
