"""In-VPC probe: assert the frontend reports heartbeating workers.

ListWorkers lives on the frontend HTTP API (:7243, internal) — the UI
server does not proxy it — so aws-deploy-validate runs this as a one-shot
ECS task next to the fleet. Exit 0 = workers are heartbeating.
"""
import os
import sys
import time
import urllib.request

host = os.environ["TEMPORAL_ADDRESS"].rsplit(":", 1)[0]
namespace = os.environ.get("TEMPORAL_NAMESPACE", "default")
url = f"http://{host}:7243/api/v1/namespaces/{namespace}/workers?pageSize=5"

for _ in range(16):
    try:
        body = urllib.request.urlopen(url, timeout=5).read().decode()
        if "workerInstanceKey" in body:
            print("WORKERS:", body[:400])
            sys.exit(0)
        print("no workers yet:", body[:200])
    except Exception as exc:  # noqa: BLE001 — every failure is just "retry"
        print("retry:", exc)
    time.sleep(15)
sys.exit(1)
