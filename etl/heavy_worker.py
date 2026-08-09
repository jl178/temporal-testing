import asyncio
import os
from datetime import timedelta

from temporalio.client import Client
from temporalio.worker import ResourceBasedSlotConfig, Worker, WorkerTuner

from workflow import HEAVY_TASK_QUEUE

from activities import run_local_transform

# Heavy fleet: each occupied slot is a Spark JVM subprocess (~1-2GB).
# Slots are admitted by a RESOURCE-BASED tuner — observed host CPU/memory —
# rather than a fixed count, bounded to [1, 2] so a quiet host still can't
# over-admit JVMs. In production this fleet runs on its own big-memory
# instances.
TUNER = WorkerTuner.create_resource_based(
    target_memory_usage=0.8,
    target_cpu_usage=0.9,
    activity_config=ResourceBasedSlotConfig(
        minimum_slots=1,
        maximum_slots=2,
        ramp_throttle=timedelta(seconds=1),
    ),
)


async def main() -> None:
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )
    worker = Worker(
        client,
        task_queue=HEAVY_TASK_QUEUE,
        activities=[run_local_transform],
        tuner=TUNER,
        # Server-enforced dispatch cap for the WHOLE queue (all workers
        # combined) — protects the shared substrate (here: one machine's
        # RAM; in prod: e.g. a metastore or database) from a scaled-out
        # fleet collectively over-launching heavy jobs.
        max_task_queue_activities_per_second=4,
    )
    print(f"Heavy worker listening on task queue {HEAVY_TASK_QUEUE!r}", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
