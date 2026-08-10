import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from activities import compose_greeting
from workflows import GreetingWorkflow

TASK_QUEUE = "greeting-tasks-python"


async def main() -> None:
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[GreetingWorkflow],
        activities=[compose_greeting],
        # Load-test knob: a small per-worker slot cap means throughput
        # scales with FLEET SIZE, so backlog autoscaling is observable.
        max_concurrent_activities=int(os.environ.get("WORKER_MAX_ACTIVITIES", "100")),
    )
    print(f"Worker listening on task queue {TASK_QUEUE!r}", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
