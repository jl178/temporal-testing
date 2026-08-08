#!/usr/bin/env bash
# End-to-end local validation: bring up the Temporal cluster via Docker
# Compose, wait for it to be healthy, then run every language example
# (each starts a worker, executes a workflow, and asserts the result).
set -uo pipefail
cd "$(dirname "$0")/.."

ADDRESS="${TEMPORAL_ADDRESS:-localhost:7233}"

docker compose up -d

echo "==> Waiting for Temporal to be healthy at $ADDRESS"
healthy=""
for _ in $(seq 1 60); do
  if temporal operator cluster health --address "$ADDRESS" 2>/dev/null | grep -q SERVING; then
    healthy=1
    break
  fi
  sleep 2
done
if [ -z "$healthy" ]; then
  echo "ERROR: Temporal did not become healthy" >&2
  exit 1
fi
echo "    SERVING"

rc=0
for lang in python go typescript csharp; do
  echo "==> examples/$lang"
  if "./examples/$lang/run.sh"; then
    echo "    $lang: PASS"
  else
    echo "    $lang: FAIL"
    rc=1
  fi
done

echo "==> Recent workflows on the cluster:"
temporal workflow list --address "$ADDRESS" --limit 8 2>/dev/null | sed 's/^/    /' || true
exit $rc
