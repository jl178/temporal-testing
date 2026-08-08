import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from activities import run_local_transform, seed_raw_data, submit_emr_job, validate_output
from workflow import TASK_QUEUE, EtlPipelineWorkflow


async def main() -> None:
    client = await Client.connect(os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"))
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[EtlPipelineWorkflow],
        activities=[seed_raw_data, submit_emr_job, run_local_transform, validate_output],
    )
    print(f"ETL worker listening on task queue {TASK_QUEUE!r}", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
