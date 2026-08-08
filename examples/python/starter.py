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
    result = await client.execute_workflow(
        GreetingWorkflow.run,
        "Temporal",
        id=f"greeting-python-{uuid.uuid4()}",
        task_queue=TASK_QUEUE,
    )
    print(f"Workflow result: {result}")
    assert result == "Hello, Temporal!", f"unexpected result: {result!r}"
    print("PYTHON EXAMPLE: PASS")


if __name__ == "__main__":
    asyncio.run(main())
