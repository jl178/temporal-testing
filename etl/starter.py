import asyncio
import json
import os
import uuid

from temporalio.client import Client

from activities import EtlConfig
from workflow import TASK_QUEUE, EtlPipelineWorkflow


async def main() -> None:
    client = await Client.connect(os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"))
    result = await client.execute_workflow(
        EtlPipelineWorkflow.run,
        EtlConfig(),
        id=f"etl-pipeline-{uuid.uuid4()}",
        task_queue=TASK_QUEUE,
    )
    print("Workflow result:")
    print(json.dumps(result, indent=2))

    assert result["emr"]["state"] == "SUCCESS", result["emr"]
    assert result["transform"]["mart_rows"] == 3, result["transform"]
    assert abs(result["transform"]["total_revenue"] - 666.54) < 0.01, result["transform"]
    assert result["validation"]["mart_rows"] == 3, result["validation"]
    print("ETL PIPELINE: PASS")


if __name__ == "__main__":
    asyncio.run(main())
