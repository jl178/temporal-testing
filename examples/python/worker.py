import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from activities import compose_greeting
from workflows import GreetingWorkflow

TASK_QUEUE = "greeting-tasks-python"


async def main() -> None:
    client = await Client.connect(os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"))
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[GreetingWorkflow],
        activities=[compose_greeting],
    )
    print(f"Worker listening on task queue {TASK_QUEUE!r}", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
