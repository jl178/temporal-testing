"""A second, non-ETL workload on the worker platform.

Nothing here knows about the ETL: a different domain (billing), its own
queues, its own code — deployed with the same generic runner and the same
size profiles. This is the proof that worker_platform is a platform, not an
ETL helper.
"""
from datetime import timedelta

from temporalio import activity, workflow

TASK_QUEUE = "billing"
RENDER_QUEUE = "billing-render"


@activity.defn
def prepare_invoice(customer: str) -> dict:
    """I/O-shaped step (imagine: fetch line items) — medium profile."""
    return {"customer": customer, "lines": 3, "total": 249.00}


@activity.defn
def render_invoice(invoice: dict) -> str:
    """Compute-shaped step (imagine: PDF rendering) — its own large-profile
    lane, so a rendering pile-up can never slow billing coordination."""
    return f"invoice.pdf for {invoice['customer']}: {invoice['lines']} lines, ${invoice['total']:.2f}"


@workflow.defn
class InvoiceWorkflow:
    @workflow.run
    async def run(self, customer: str) -> str:
        invoice = await workflow.execute_activity(
            prepare_invoice,
            customer,
            start_to_close_timeout=timedelta(seconds=30),
        )
        return await workflow.execute_activity(
            render_invoice,
            invoice,
            task_queue=RENDER_QUEUE,
            start_to_close_timeout=timedelta(minutes=2),
        )
