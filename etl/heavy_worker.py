import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from workflow import HEAVY_TASK_QUEUE

from activities import run_local_transform

# Heavy fleet: each occupied slot is a Spark JVM subprocess (~1-2GB), so the
# slot count is the memory budget. In production this fleet runs on its own
# big-memory instances; a wedged transform can only hurt other transforms.
MAX_HEAVY_ACTIVITIES = 2


async def main() -> None:
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )
    worker = Worker(
        client,
        task_queue=HEAVY_TASK_QUEUE,
        activities=[run_local_transform],
        max_concurrent_activities=MAX_HEAVY_ACTIVITIES,
    )
    print(f"Heavy worker listening on task queue {HEAVY_TASK_QUEUE!r}", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
