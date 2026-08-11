"""Reviewer console — the operator's view of the pricing gate.

The work queue is a VISIBILITY QUERY, not a table this tenant maintains:
orders park themselves at `Stage = 'awaiting_review'` (search-attribute
upsert inside the workflow) and this console lists/decides them.

    python review_console.py list    [--wait-seconds N --min K]
    python review_console.py approve --order ORD-1004 [--note ..] [--wait-seconds N]
    python review_console.py deny    --order ORD-1004 [--note ..] [--wait-seconds N]

Polling absorbs visibility lag (SQL visibility: ~immediate; the
prod-mimic stack's Elasticsearch refreshes ~1s). A decision landing after
the SLA timer closed the workflow is a normal race — reported, not an
error, unless the wait budget runs out entirely.
"""
import argparse
import asyncio
import os

from temporalio.client import Client
from temporalio.service import RPCError

from pricing import OrderPricingWorkflow

PENDING = (
    "WorkflowType = 'OrderPricingWorkflow' AND "
    "ExecutionStatus = 'Running' AND Stage = 'awaiting_review'"
)


async def _client() -> Client:
    return await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )


async def _pending(client: Client, extra: str = "") -> list:
    return [wf async for wf in client.list_workflows(PENDING + extra)]


async def cmd_list(args) -> int:
    client = await _client()
    deadline = asyncio.get_event_loop().time() + args.wait_seconds
    while True:
        pending = await _pending(client)
        if len(pending) >= args.min or asyncio.get_event_loop().time() >= deadline:
            break
        await asyncio.sleep(2)
    print(f"pending review: {len(pending)}")
    for wf in pending:
        handle = client.get_workflow_handle(wf.id)
        status = await handle.query(OrderPricingWorkflow.status)
        print(
            f"  {wf.id}: submitted={status.get('submitted')} "
            f"priced={status.get('priced')} variance={status.get('variance_pct')}%"
        )
    return 0 if len(pending) >= args.min else 1


async def cmd_decide(args, decision: str) -> int:
    client = await _client()
    query = PENDING + f" AND OrderId = '{args.order}'"
    deadline = asyncio.get_event_loop().time() + args.wait_seconds
    while True:
        hits = await _pending(client, f" AND OrderId = '{args.order}'")
        if hits:
            handle = client.get_workflow_handle(hits[0].id)
            try:
                await handle.signal(
                    OrderPricingWorkflow.review_decision, args=[decision, args.note]
                )
                print(f"{decision}: {args.order} ({hits[0].id})")
                return 0
            except RPCError as err:
                # Closed between list and signal: the SLA already decided.
                print(f"{args.order}: workflow already closed ({err.status})")
                return 0
        if asyncio.get_event_loop().time() >= deadline:
            print(f"{args.order}: never appeared at {query!r} "
                  f"within {args.wait_seconds}s")
            return 1
        await asyncio.sleep(2)


def main() -> int:
    parser = argparse.ArgumentParser(prog="review_console")
    sub = parser.add_subparsers(dest="command", required=True)
    p_list = sub.add_parser("list")
    p_list.add_argument("--wait-seconds", type=int, default=0)
    p_list.add_argument("--min", type=int, default=0)
    for name in ("approve", "deny"):
        p = sub.add_parser(name)
        p.add_argument("--order", required=True)
        p.add_argument("--note", default="")
        p.add_argument("--wait-seconds", type=int, default=30)
    args = parser.parse_args()
    if args.command == "list":
        return asyncio.run(cmd_list(args))
    return asyncio.run(cmd_decide(args, args.command))


if __name__ == "__main__":
    raise SystemExit(main())
