"""Reviewer console — the operator's view of the pricing gate.

The work queue is a VISIBILITY QUERY, not a table this tenant maintains:
orders park themselves at `Stage = 'awaiting_review'` (search-attribute
upsert inside the workflow) and this console lists/decides them.

    python review_console.py list    [--wait-seconds N --min K]
    python review_console.py approve --order ORD-1004 [--note ..] [--wait-seconds N]
    python review_console.py deny    --order ORD-1004 [--note ..] [--wait-seconds N]
    python review_console.py trace   --order ORD-1004 [--batch ID]

`trace` is the record-centric view the workflow-centric UI doesn't give
you directly: one order's entire journey — every stage transition,
activity, signal, and timer, plus where the batch, remittance file, and
analytics run live — distilled from the workflow history (which is a
complete audit log, not telemetry).

Polling absorbs visibility lag (SQL visibility: ~immediate; the
prod-mimic stack's Elasticsearch refreshes ~1s). A decision landing after
the SLA timer closed the workflow is a normal race — reported, not an
error, unless the wait budget runs out entirely.
"""
import argparse
import asyncio
import json
import os

from temporalio.api.enums.v1 import EventType
from temporalio.client import Client
from temporalio.service import RPCError

from pricing import BATCH_ID, OrderPricingWorkflow

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


def _decode(payload) -> object:
    try:
        return json.loads(payload.data)
    except Exception:  # noqa: BLE001 — trace output degrades gracefully
        return "<binary>"


async def cmd_trace(args) -> int:
    client = await _client()
    query = f"WorkflowType = 'OrderPricingWorkflow' AND OrderId = '{args.order}'"
    if args.batch:
        query += f" AND BatchId = '{args.batch}'"
    hits = [wf async for wf in client.list_workflows(query)]
    if not hits:
        print(f"no lifecycle found for {args.order!r} (query: {query})")
        return 1
    hits.sort(key=lambda w: w.start_time, reverse=True)
    wf = hits[0]
    batch_id = wf.typed_search_attributes.get(BATCH_ID)

    print(f"ORDER {args.order} — {wf.id} [{wf.status.name}]")
    print(f"  batch: {batch_id}")
    print(f"  UI: workflow list query  OrderId = '{args.order}'   "
          f"(whole tree: BatchId = '{batch_id}')")
    print("  journey:")

    handle = client.get_workflow_handle(wf.id, run_id=wf.run_id)
    history = await handle.fetch_history()
    for ev in history.events:
        t = ev.event_time.ToDatetime().strftime("%H:%M:%S")
        et = ev.event_type
        if et == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
            print(f"    {t}  received (lifecycle started, queue "
                  f"{ev.workflow_execution_started_event_attributes.task_queue.name!r})")
        elif et == EventType.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
            fields = ev.upsert_workflow_search_attributes_event_attributes \
                .search_attributes.indexed_fields
            if "Stage" in fields:
                print(f"    {t}  stage -> {_decode(fields['Stage'])}")
        elif et == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
            a = ev.activity_task_scheduled_event_attributes
            print(f"    {t}  activity {a.activity_type.name!r} "
                  f"-> queue {a.task_queue.name!r}")
        elif et == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
            print(f"    {t}    …completed")
        elif et == EventType.EVENT_TYPE_TIMER_STARTED:
            secs = ev.timer_started_event_attributes.start_to_fire_timeout.seconds
            print(f"    {t}  SLA timer armed ({secs}s)")
        elif et == EventType.EVENT_TYPE_TIMER_FIRED:
            print(f"    {t}  SLA timer FIRED (no decision in time)")
        elif et == EventType.EVENT_TYPE_TIMER_CANCELED:
            print(f"    {t}  SLA timer canceled (decision arrived first)")
        elif et == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
            s = ev.workflow_execution_signaled_event_attributes
            inputs = [_decode(p) for p in s.input.payloads]
            print(f"    {t}  SIGNAL {s.signal_name!r} {inputs}")
        elif et == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
            payloads = ev.workflow_execution_completed_event_attributes \
                .result.payloads
            row = _decode(payloads[0]) if payloads else {}
            print(f"    {t}  DONE: outcome={row.get('outcome')} "
                  f"decided_by={row.get('decision_source')} "
                  f"payable={row.get('payable_amount')} "
                  f"(submitted {row.get('submitted_amount')}, "
                  f"priced {row.get('priced_amount')})")
        elif et == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
            print(f"    {t}  FAILED")

    # Where the record went next (only known once the batch closed).
    if batch_id:
        try:
            report = await client.get_workflow_handle(batch_id).result()
            remit = report.get("remittance", {})
            print(f"  remittance: s3 key {remit.get('key')!r}"
                  + (f", vendor copy {remit.get('sftp_path')!r}"
                     if remit.get("sftp_path") else ""))
            outputs = report.get("etl", {}).get("validation", {}).get("outputs", [])
            if outputs:
                print(f"  analytics: {outputs[0]['key']} "
                      f"({outputs[0]['rows']} vendor rows) — "
                      f"workflow {batch_id}-settlements")
        except Exception:  # noqa: BLE001 — batch still running or gone
            print("  (batch still running — remittance/analytics pending)")
    return 0


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
    p_trace = sub.add_parser("trace")
    p_trace.add_argument("--order", required=True)
    p_trace.add_argument("--batch", default=None)
    args = parser.parse_args()
    if args.command == "list":
        return asyncio.run(cmd_list(args))
    if args.command == "trace":
        return asyncio.run(cmd_trace(args))
    return asyncio.run(cmd_decide(args, args.command))


if __name__ == "__main__":
    raise SystemExit(main())
