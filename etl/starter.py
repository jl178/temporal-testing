"""Demo entrypoint: run the demo job through the generic pipeline and
verify its known answers. Job definition + assertions live in demo.py;
this file is only orchestration."""
import asyncio
import json
import os
import uuid

from temporalio.client import Client

import demo
from workflow import TASK_QUEUE, EtlPipelineWorkflow


async def main() -> None:
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )
    job = demo.demo_job()
    result = await client.execute_workflow(
        EtlPipelineWorkflow.run,
        job,
        id=f"etl-{job.name}-{uuid.uuid4()}",
        task_queue=TASK_QUEUE,
    )
    print("Workflow result:")
    print(json.dumps(result, indent=2))
    demo.verify(result)
    print("ETL PIPELINE: PASS")


if __name__ == "__main__":
    asyncio.run(main())
