import asyncio
import json
import os
import uuid

from temporalio.client import Client
from temporalio.common import SearchAttributePair, TypedSearchAttributes

from ingest.activities import IngestConfig, SmbSource
from ingest.workflow import BATCH_ID, TASK_QUEUE, FileIngestWorkflow


def smb_from_env() -> SmbSource | None:
    """SMB_HOST set = this batch's source is an SMB share."""
    if not os.environ.get("SMB_HOST"):
        return None
    return SmbSource(
        host=os.environ["SMB_HOST"],
        port=int(os.environ.get("SMB_PORT", "1445")),
        username=os.environ.get("SMB_USERNAME", "demo"),
        password=os.environ.get("SMB_PASSWORD", "demopass"),
        share=os.environ.get("SMB_SHARE", "upload"),
        path=os.environ.get("SMB_PATH", ""),
    )


async def main() -> None:
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )
    from runtime_env import catalog_from_env, spark_remote_from_env

    catalog = catalog_from_env()
    # INGEST_ONLY=1: land + route but spawn no transforms — a downstream
    # consumer owns transformation.
    ingest_only = os.environ.get("INGEST_ONLY") == "1"
    cfg = IngestConfig(
        smb=smb_from_env(),
        catalog=catalog,
        spark_remote=spark_remote_from_env(),
        # Cross-route join needs the per-route tables to persist across jobs.
        consolidation_spec="consolidation" if catalog and not ingest_only else None,
        dispatch_transforms=not ingest_only,
    )
    workflow_id = f"file-ingest-{uuid.uuid4()}"
    result = await client.execute_workflow(
        FileIngestWorkflow.run,
        cfg,
        id=workflow_id,
        task_queue=TASK_QUEUE,
        search_attributes=TypedSearchAttributes(
            [SearchAttributePair(BATCH_ID, workflow_id)]
        ),
    )
    print("Workflow result:")
    print(json.dumps(result, indent=2))

    if ingest_only:
        assert result["landed"] == 3, result
        assert result["transformed"] == 0, result
        assert result["quarantined"] == 1, result
        landed = [r for r in result["results"] if r["status"] == "landed"]
        assert sorted(r["route"] for r in landed) == ["customers", "orders", "payments"]
        assert all(r["landed_key"].startswith("landing/") for r in landed)
        print("INGEST-ONLY: PASS (landed + routed; transformation left to a consumer)")
        return

    by_route = {
        r["route"]: r for r in result["results"] if r["status"] == "transformed"
    }

    assert result["transformed"] == 3, result
    assert result["quarantined"] == 1, result
    quarantined = next(r for r in result["results"] if r["status"] == "quarantined")
    assert quarantined["file"] == "zz_broken.csv", quarantined

    orders_mart = by_route["orders"]["transform"]["validation"]["outputs"][0]
    assert orders_mart["rows"] == 3, orders_mart
    revenue = sum(float(r["total_revenue"]) for r in orders_mart["data"])
    assert abs(revenue - 666.54) < 0.01, revenue

    assert by_route["customers"]["transform"]["validation"]["outputs"][0]["rows"] == 3
    assert by_route["payments"]["transform"]["validation"]["outputs"][0]["rows"] == 4

    if cfg.consolidation_spec:
        summary = result["consolidation"]["validation"]["outputs"][0]
        assert summary["rows"] == 2, summary
        rows = {r["segment"]: r for r in summary["data"]}
        assert abs(float(rows["enterprise"]["revenue"]) - 300.45) < 0.01, rows
        assert abs(float(rows["enterprise"]["collected"]) - 200.50) < 0.01, rows
        assert abs(float(rows["smb"]["collection_rate"]) - 1.0) < 0.001, rows
        print("CONSOLIDATION: PASS (executive_summary joined 3 routes)")

    print("INGEST PIPELINE: PASS")


if __name__ == "__main__":
    asyncio.run(main())
