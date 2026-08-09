import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from workflow import TASK_QUEUE, EtlPipelineWorkflow

# Workflow-only worker: replay/decision processing is CPU-light and must
# never queue behind activities — no activity, however badly behaved, can
# delay workflow progress on this fleet (activities live in light_worker.py
# and heavy_worker.py).


async def main() -> None:
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[EtlPipelineWorkflow],
        max_concurrent_workflow_tasks=16,
    )
    print(f"ETL workflow worker listening on task queue {TASK_QUEUE!r}", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
