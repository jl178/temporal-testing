import asyncio
import os
import uuid

from temporalio.client import Client

from workflows import GreetingWorkflow
from worker import TASK_QUEUE


async def main() -> None:
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )
    # Burst knob (default 1): N concurrent workflows to build a backlog —
    # used to exercise backlog autoscaling against a slot-capped fleet.
    n = int(os.environ.get("STARTER_ITERATIONS", "1"))
    run_id = uuid.uuid4()

    async def one(i: int) -> str:
        return await client.execute_workflow(
            GreetingWorkflow.run,
            "Temporal",
            id=f"greeting-python-{run_id}-{i}",
            task_queue=TASK_QUEUE,
        )

    results = await asyncio.gather(*(one(i) for i in range(n)))
    print(f"Workflow result: {results[0]}")
    assert all(r == "Hello, Temporal!" for r in results), results
    if n > 1:
        print(f"BURST: {n}/{n} workflows completed")
    print("PYTHON EXAMPLE: PASS")


if __name__ == "__main__":
    asyncio.run(main())
