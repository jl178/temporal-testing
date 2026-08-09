import asyncio
import os
import uuid

from temporalio.client import Client

from billing import TASK_QUEUE, InvoiceWorkflow


async def main() -> None:
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )
    result = await client.execute_workflow(
        InvoiceWorkflow.run,
        "acme",
        id=f"invoice-acme-{uuid.uuid4()}",
        task_queue=TASK_QUEUE,
    )
    print(f"Workflow result: {result}")
    assert "invoice.pdf for acme" in result, result
    print("PLATFORM DEMO: PASS")


if __name__ == "__main__":
    asyncio.run(main())
