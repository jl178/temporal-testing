import asyncio
import os
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from activities import seed_raw_data, submit_emr_job, validate_output
from workflow import TASK_QUEUE, EtlPipelineWorkflow

# Light fleet: workflow tasks + cheap I/O-bound activities. High slots are
# fine — nothing here is memory- or CPU-heavy. The Spark-spawning transform
# is deliberately NOT registered here (see heavy_worker.py).
MAX_ACTIVITIES = 16


async def main() -> None:
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[EtlPipelineWorkflow],
        activities=[seed_raw_data, submit_emr_job, validate_output],
        # Sync (blocking, boto3) activities run on this pool so they can
        # never stall the async event loop.
        activity_executor=ThreadPoolExecutor(max_workers=MAX_ACTIVITIES),
        max_concurrent_activities=MAX_ACTIVITIES,
        max_concurrent_workflow_tasks=8,
    )
    print(f"ETL worker listening on task queue {TASK_QUEUE!r}", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
