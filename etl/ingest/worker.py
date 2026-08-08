import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from ingest.activities import (
    classify_file,
    discover_sftp_files,
    land_sftp_file,
    parse_file,
    resolve_transform_spec,
)
from ingest.workflow import TASK_QUEUE, FileIngestWorkflow


async def main() -> None:
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[FileIngestWorkflow],
        activities=[
            discover_sftp_files,
            land_sftp_file,
            parse_file,
            classify_file,
            resolve_transform_spec,
        ],
    )
    print(f"Ingest worker listening on task queue {TASK_QUEUE!r}", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
