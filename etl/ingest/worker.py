import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from ingest.workflow import TASK_QUEUE, FileIngestWorkflow

# Workflow-only worker for the ingest queue (activities live in
# ingest/activity_worker.py) — workflow progress can never queue behind a
# slow SFTP transfer.


async def main() -> None:
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[FileIngestWorkflow],
        max_concurrent_workflow_tasks=16,
    )
    print(f"Ingest workflow worker listening on task queue {TASK_QUEUE!r}", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
