"""Batch starter/verifier — the only place env becomes payload.

    python starter.py start          # start a batch, print its workflow id
    python starter.py await --id ID  # block on the result, assert, PASS

Split so the review console can act between the two (run.sh does exactly
that; in a live walkthrough, `start`, browse the UI, decide, `await`).
"""
import argparse
import asyncio
import json
import os
import uuid

from temporalio.client import Client
from temporalio.common import SearchAttributePair, TypedSearchAttributes

from pricing import BATCH_ID, INTAKE_QUEUE, BatchInput, OrderBatchWorkflow

# etl/ is on PYTHONPATH (run.sh): reuse the platform's env contract.
from runtime_env import DEFAULT_BUCKET, catalog_from_env, spark_remote_from_env


def batch_input() -> BatchInput:
    return BatchInput(
        bucket=DEFAULT_BUCKET,
        source={"dispatch_transforms": False},
        variance_threshold_pct=float(os.environ.get("VARIANCE_THRESHOLD_PCT", "10")),
        review_sla_seconds=int(os.environ.get("REVIEW_SLA_SECONDS", "300")),
        sla_action=os.environ.get("SLA_ACTION", "escalate"),
        remit_sftp={"host": "localhost", "port": 2222, "username": "demo",
                    "password": "demo", "path": "/outbound"},
        catalog=catalog_from_env(),
        spark_remote=spark_remote_from_env(),
    )


async def cmd_start() -> None:
    client = await _client()
    workflow_id = f"order-batch-{uuid.uuid4().hex[:12]}"
    await client.start_workflow(
        OrderBatchWorkflow.run,
        batch_input(),
        id=workflow_id,
        task_queue=INTAKE_QUEUE,
        search_attributes=TypedSearchAttributes(
            [SearchAttributePair(BATCH_ID, workflow_id)]
        ),
    )
    print(workflow_id)


async def cmd_await(workflow_id: str) -> None:
    client = await _client()
    result = await client.get_workflow_handle(workflow_id).result()
    print("Batch report:")
    print(json.dumps(result, indent=2))

    assert result["orders"] == 6, result
    assert result["outcomes"] == {
        "settled": 4, "denied": 1, "escalated": 1, "duplicate": 0, "errored": 0,
    }, result["outcomes"]
    assert result["by_source"] == {
        "auto": 3, "reviewer": 1, "validation": 1, "sla_timeout": 1,
    }, result["by_source"]
    assert result["ingest"]["landed"] == 1, result["ingest"]
    assert result["remittance"]["rows"] == 6, result["remittance"]
    assert result["remittance"]["key"].startswith("remittance/")

    mart = result["etl"]["validation"]["outputs"][0]
    assert mart["rows"] == 3, mart
    by_vendor = {r["vendor"]: r for r in mart["data"]}
    assert abs(float(by_vendor["acme"]["payable_total"]) - 532.00) < 0.01
    assert abs(float(by_vendor["acme"]["savings"]) - 5.00) < 0.01
    assert abs(float(by_vendor["globex"]["payable_total"]) - 350.00) < 0.01
    assert abs(float(by_vendor["globex"]["savings"]) - 60.00) < 0.01
    assert float(by_vendor["initech"]["payable_total"]) == 0.0
    assert int(by_vendor["initech"]["escalated_count"]) == 1
    assert int(by_vendor["initech"]["denied_count"]) == 1

    print("ORDER SETTLEMENT: PASS")


async def _client() -> Client:
    return await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )


def main() -> None:
    parser = argparse.ArgumentParser(prog="starter")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("start")
    p_await = sub.add_parser("await")
    p_await.add_argument("--id", required=True)
    args = parser.parse_args()
    if args.command == "start":
        asyncio.run(cmd_start())
    else:
        asyncio.run(cmd_await(args.id))


if __name__ == "__main__":
    main()
