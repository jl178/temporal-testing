import asyncio
import os
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from ingest.activities import (
    classify_route,
    discover_sftp_files,
    land_sftp_file,
    quarantine_file,
    resolve_consolidation_spec,
    resolve_transform_spec,
)
from ingest.workflow import TASK_QUEUE


async def main() -> None:
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        activities=[
            discover_sftp_files,
            land_sftp_file,
            classify_route,
            quarantine_file,
            resolve_transform_spec,
            resolve_consolidation_spec,
        ],
        # Sync (blocking) activities run on this pool, never the event loop.
        activity_executor=ThreadPoolExecutor(max_workers=16),
        max_concurrent_activities=16,
    )
    print(f"Ingest activity worker listening on task queue {TASK_QUEUE!r}", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
