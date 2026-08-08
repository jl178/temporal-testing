import asyncio
import json
import os
import uuid

from temporalio.client import Client

from ingest.activities import IngestConfig
from ingest.workflow import TASK_QUEUE, FileIngestWorkflow


async def main() -> None:
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )
    catalog = None
    if os.environ.get("ICEBERG_REST_URI"):
        catalog = {
            "type": "rest",
            "name": "lake",
            "uri": os.environ["ICEBERG_REST_URI"],
            "warehouse": "s3://etl-data/warehouse",
        }
    result = await client.execute_workflow(
        FileIngestWorkflow.run,
        IngestConfig(catalog=catalog),
        id=f"file-ingest-{uuid.uuid4()}",
        task_queue=TASK_QUEUE,
    )
    print("Workflow result:")
    print(json.dumps(result, indent=2))

    assert result["files_processed"] >= 1, result
    first = result["results"][0]
    assert first["route"] == "orders", first
    mart = first["transform"]["transform"]["outputs"][0]
    assert mart["rows"] == 3, mart
    data = first["transform"]["validation"]["outputs"][0]["data"]
    revenue = sum(float(r["total_revenue"]) for r in data)
    assert abs(revenue - 666.54) < 0.01, revenue
    print("INGEST PIPELINE: PASS")


if __name__ == "__main__":
    asyncio.run(main())
