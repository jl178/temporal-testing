import asyncio
import os
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from activities import seed_raw_data, submit_emr_job, validate_output
from workflow import TASK_QUEUE

# Activity-only light fleet: cheap I/O-bound activities (boto3 launchers,
# validation). High slots are fine; blocking (sync) activities run on the
# thread pool so nothing stalls an event loop.
MAX_ACTIVITIES = 16


async def main() -> None:
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        activities=[seed_raw_data, submit_emr_job, validate_output],
        activity_executor=ThreadPoolExecutor(max_workers=MAX_ACTIVITIES),
        max_concurrent_activities=MAX_ACTIVITIES,
    )
    print(f"ETL activity worker listening on task queue {TASK_QUEUE!r}", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
