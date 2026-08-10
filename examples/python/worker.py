import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from activities import compose_greeting
from workflows import GreetingWorkflow

TASK_QUEUE = "greeting-tasks-python"


async def main() -> None:
    # On a fresh deploy the frontend's NLB targets register minutes after
    # this container starts — retry instead of crash-looping through ECS
    # restarts (which back off and lag the whole fleet behind).
    address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    namespace = os.environ.get("TEMPORAL_NAMESPACE", "default")
    for attempt in range(30):
        try:
            client = await Client.connect(address, namespace=namespace)
            break
        except RuntimeError as exc:
            print(f"connect attempt {attempt + 1} failed: {exc}", flush=True)
            await asyncio.sleep(10)
    else:
        raise SystemExit(f"could not reach {address} after 30 attempts")
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
