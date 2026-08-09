import asyncio
import os
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from activities import (
    run_local_transform,
    seed_raw_data,
    submit_emr_job,
    validate_output,
)
from workflow import TASK_QUEUE

# Activity-only light fleet: cheap I/O-bound activities (boto3 launchers,
# validation, and the transform CLIENT — in spark_remote mode dbt just
# compiles SQL and the external Spark service does the compute, so the
# workflow routes it here; the in-process fallback routes to etl-heavy).
# High slots are fine; blocking (sync) activities run on the thread pool so
# nothing stalls an event loop.
MAX_ACTIVITIES = 16


async def main() -> None:
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        activities=[seed_raw_data, submit_emr_job, validate_output, run_local_transform],
        activity_executor=ThreadPoolExecutor(max_workers=MAX_ACTIVITIES),
        max_concurrent_activities=MAX_ACTIVITIES,
    )
    print(f"ETL activity worker listening on task queue {TASK_QUEUE!r}", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
