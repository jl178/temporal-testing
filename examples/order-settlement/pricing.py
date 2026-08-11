"""Order pricing & settlement — a lifecycle tenant on the platform.

Vendors drop order batch files on the file server; every order then lives
its own durable lifecycle: validate -> contract lookup (another team's
fleet) -> price -> variance gate (auto-settle | human review | SLA
escalation) -> settle into an outbound remittance file; a dbt mart
summarizes outcomes. One workflow tree carries the WHOLE lineage: batch ->
ingest -> every order -> remittance -> analytics, queryable in the UI by
BatchId / OrderId / Stage.

First tenant to use signals, queries, `workflow.wait_condition`, timers,
and mid-workflow search-attribute upserts. Determinism rules this module
must never break:
  - policy (thresholds, SLAs) travels IN PAYLOADS — never os.environ in
    workflow code (replay on a differently-configured worker must not
    diverge);
  - after a wait, decide from recorded STATE, never from which branch woke
    (signal-vs-timer race);
  - signal handlers only mutate state; first valid decision wins.
"""
import asyncio
from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import (
    RetryPolicy,
    SearchAttributeKey,
    SearchAttributePair,
    TypedSearchAttributes,
)
from temporalio.exceptions import (
    ActivityError,
    ApplicationError,
    ChildWorkflowError,
    WorkflowAlreadyStartedError,
)

with workflow.unsafe.imports_passed_through():
    import csv
    import io
    import json
    import os
    import tempfile

    import asyncssh
    import boto3

INTAKE_QUEUE = "settlement-intake"
ORDERS_QUEUE = "settlement-orders"
CONTRACTS_QUEUE = "settlement-contracts"

# Lineage: BatchId ties the whole tree together; OrderId addresses one
# record's journey; Stage is the live, queryable pipeline state
# (`Stage = 'awaiting_review'` IS the reviewers' work queue).
BATCH_ID = SearchAttributeKey.for_keyword("BatchId")
ORDER_ID = SearchAttributeKey.for_keyword("OrderId")
ROUTE = SearchAttributeKey.for_keyword("Route")
SOURCE_FILE = SearchAttributeKey.for_keyword("SourceFile")
STAGE = SearchAttributeKey.for_keyword("Stage")

LIGHT_RETRY = RetryPolicy(maximum_attempts=5)


class Stage:
    """Closed vocabulary — every value is queryable in visibility."""

    # order lifecycle
    RECEIVED = "received"
    VALIDATED = "validated"
    ENRICHED = "enriched"
    PRICED = "priced"
    AWAITING_REVIEW = "awaiting_review"
    SETTLED = "settled"
    DENIED = "denied"
    ESCALATED = "escalated"
    # batch lifecycle
    INGESTING = "ingesting"
    PRICING = "pricing"
    REMITTING = "remitting"
    ANALYTICS = "analytics"
    COMPLETE = "complete"


@dataclass
class OrderInput:
    """One order's payload. Appended fields MUST carry neutral defaults —
    histories recorded before a field existed still replay under new code."""

    order_ref: str
    vendor: str
    source_file: str
    batch_id: str
    item_count: int | None = None
    submitted_amount: float | None = None
    parse_error: str | None = None
    variance_threshold_pct: float = 10.0
    review_sla_seconds: int = 300
    sla_action: str = "escalate"  # or "deny"


@dataclass
class BatchInput:
    """The batch payload the starter builds from env — workflow code never
    reads the environment."""

    bucket: str
    # IngestConfig-shaped dict for the by-name FileIngestWorkflow child
    # (dispatch_transforms=False: land + route only; this tenant is the
    # downstream consumer its docstring promises).
    source: dict = field(default_factory=dict)
    variance_threshold_pct: float = 10.0
    review_sla_seconds: int = 300
    sla_action: str = "escalate"
    remit_prefix: str = "remittance"
    # {host, port, username, password, path} — outbound remittance drop.
    remit_sftp: dict | None = None
    catalog: dict | None = None
    spark_remote: str | None = None
    max_records: int = 2000


REMIT_COLUMNS = [
    "batch_id", "order_ref", "vendor", "item_count", "submitted_amount",
    "priced_amount", "variance_pct", "outcome", "decision_source",
    "payable_amount", "note",
]


@workflow.defn
class OrderPricingWorkflow:
    """One order's full lifecycle — the record the organization tracks."""

    def __init__(self) -> None:
        self._stage: str = Stage.RECEIVED
        self._decision: dict | None = None
        self._amounts: dict = {}

    @workflow.signal
    def review_decision(self, decision: str, note: str = "") -> None:
        # Mutate state only; first valid decision wins — duplicate or late
        # signals are recorded facts and must be harmless.
        if self._decision is None and decision in ("approve", "deny"):
            self._decision = {"decision": decision, "note": note}

    @workflow.query
    def status(self) -> dict:
        # Read-only snapshot; queries never mutate or block.
        return {"stage": self._stage, **self._amounts}

    def _set_stage(self, stage: str) -> None:
        self._stage = stage
        workflow.upsert_search_attributes([STAGE.value_set(stage)])

    def _outcome(self, inp: OrderInput, outcome: str, source: str,
                 payable: float, note: str = "") -> dict:
        return {
            "batch_id": inp.batch_id,
            "order_ref": inp.order_ref,
            "vendor": inp.vendor,
            "item_count": inp.item_count or 0,
            "submitted_amount": inp.submitted_amount or 0.0,
            "priced_amount": self._amounts.get("priced", 0.0),
            "variance_pct": self._amounts.get("variance_pct", 0.0),
            "outcome": outcome,
            "decision_source": source,
            "payable_amount": payable,
            "note": note,
        }

    @workflow.run
    async def run(self, inp: OrderInput) -> dict:
        # --- validate: deterministic rejection, visible lineage ----------
        if (
            inp.parse_error
            or not inp.item_count or inp.item_count <= 0
            or not inp.submitted_amount or inp.submitted_amount <= 0
        ):
            self._set_stage(Stage.DENIED)
            return self._outcome(
                inp, "denied", "validation", payable=0.0,
                note=inp.parse_error or "missing or non-positive fields",
            )
        self._set_stage(Stage.VALIDATED)

        # --- enrich: cross-service handoff (another team's fleet) --------
        contract = await workflow.execute_activity(
            "lookup_contract",
            inp.vendor,
            task_queue=CONTRACTS_QUEUE,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=LIGHT_RETRY,
        )
        self._set_stage(Stage.ENRICHED)

        # --- price: pure decision logic belongs IN the workflow ----------
        priced = round(
            inp.item_count * contract["unit_rate"]
            * (1 - contract["discount_pct"] / 100),
            2,
        )
        variance_pct = round((inp.submitted_amount - priced) / priced * 100, 2)
        self._amounts = {
            "submitted": inp.submitted_amount,
            "priced": priced,
            "variance_pct": variance_pct,
        }
        self._set_stage(Stage.PRICED)

        if abs(variance_pct) <= inp.variance_threshold_pct:
            self._set_stage(Stage.SETTLED)
            return self._outcome(inp, "settled", "auto", payable=priced)

        # --- gate: human review with a durable SLA timer -----------------
        self._set_stage(Stage.AWAITING_REVIEW)
        try:
            await workflow.wait_condition(
                lambda: self._decision is not None,
                timeout=timedelta(seconds=inp.review_sla_seconds),
            )
        except asyncio.TimeoutError:
            pass  # decide from STATE below, never from which branch woke

        if self._decision is not None:
            approved = self._decision["decision"] == "approve"
            self._set_stage(Stage.SETTLED if approved else Stage.DENIED)
            return self._outcome(
                inp,
                "settled" if approved else "denied",
                "reviewer",
                payable=priced if approved else 0.0,
                note=self._decision["note"],
            )

        terminal = Stage.DENIED if inp.sla_action == "deny" else Stage.ESCALATED
        self._set_stage(terminal)
        return self._outcome(
            inp, terminal, "sla_timeout", payable=0.0,
            note=f"no decision within {inp.review_sla_seconds}s",
        )


@workflow.defn
class OrderBatchWorkflow:
    """The batch: ingest (child, by name) -> split -> one child per order
    -> remittance out -> analytics (child, by name). One tree = the whole
    lineage under BatchId."""

    def __init__(self) -> None:
        self._stage: str = Stage.RECEIVED

    @workflow.query
    def status(self) -> dict:
        return {"stage": self._stage}

    def _set_stage(self, stage: str) -> None:
        self._stage = stage
        workflow.upsert_search_attributes([STAGE.value_set(stage)])

    @workflow.run
    async def run(self, inp: BatchInput) -> dict:
        wid = workflow.info().workflow_id

        # --- land + route via the platform's ingest, invoked BY NAME -----
        self._set_stage(Stage.INGESTING)
        ingest = await workflow.execute_child_workflow(
            "FileIngestWorkflow",
            inp.source,
            id=f"{wid}-ingest",
            task_queue="file-ingest",
            search_attributes=TypedSearchAttributes(
                [SearchAttributePair(BATCH_ID, wid)]
            ),
        )
        batch_files = [
            r for r in ingest["results"]
            if r["status"] == "landed" and r["route"] == "vendor-orders"
        ]
        if not batch_files:
            raise ApplicationError(
                "no vendor-orders files landed in this batch", non_retryable=True
            )

        # --- split + fan out one lifecycle per order ---------------------
        self._set_stage(Stage.PRICING)
        rows: list = []
        counts = {"settled": 0, "denied": 0, "escalated": 0,
                  "duplicate": 0, "errored": 0}
        sources = {"auto": 0, "reviewer": 0, "validation": 0, "sla_timeout": 0}

        for landed in batch_files:
            try:
                records = await workflow.execute_activity(
                    split_batch,
                    args=[inp.bucket, landed["landed_key"], inp.max_records],
                    start_to_close_timeout=timedelta(minutes=2),
                    retry_policy=LIGHT_RETRY,
                )
            except ActivityError as err:
                # Never fail the batch for one bad file: quarantine
                # server-side (platform activity, by name) and continue.
                reason = str(err.cause) if err.cause else str(err)
                await workflow.execute_activity(
                    "quarantine_file",
                    args=[inp.source, landed["landed_key"], reason],
                    task_queue="file-ingest",
                    start_to_close_timeout=timedelta(minutes=1),
                    retry_policy=LIGHT_RETRY,
                )
                counts["errored"] += 1
                continue

            async def one(rec: dict, source_file: str) -> None:
                order = OrderInput(
                    order_ref=rec["order_ref"],
                    vendor=rec["vendor"],
                    source_file=source_file,
                    batch_id=wid,
                    item_count=rec.get("item_count"),
                    submitted_amount=rec.get("submitted_amount"),
                    parse_error=rec.get("parse_error"),
                    variance_threshold_pct=inp.variance_threshold_pct,
                    review_sla_seconds=inp.review_sla_seconds,
                    sla_action=inp.sla_action,
                )
                try:
                    row = await workflow.execute_child_workflow(
                        OrderPricingWorkflow.run,
                        order,
                        # Parent-suffix keeps reruns collision-free while a
                        # duplicate ref WITHIN the batch still collides
                        # deterministically.
                        id=f"order-{order.vendor}-{order.order_ref}-{wid[-12:]}",
                        task_queue=ORDERS_QUEUE,
                        search_attributes=TypedSearchAttributes([
                            SearchAttributePair(BATCH_ID, wid),
                            SearchAttributePair(ORDER_ID, order.order_ref),
                            SearchAttributePair(ROUTE, "vendor-orders"),
                            SearchAttributePair(SOURCE_FILE, source_file),
                            SearchAttributePair(STAGE, Stage.RECEIVED),
                        ]),
                    )
                except WorkflowAlreadyStartedError:
                    counts["duplicate"] += 1
                    return
                except ChildWorkflowError:
                    counts["errored"] += 1
                    return
                rows.append(row)
                counts[row["outcome"]] += 1
                sources[row["decision_source"]] += 1

            await asyncio.gather(
                *(one(rec, landed["file"]) for rec in records)
            )

        # --- outbound remittance -----------------------------------------
        self._set_stage(Stage.REMITTING)
        remit_key = f"{inp.remit_prefix}/remit_{wid}.csv"
        remit = await workflow.execute_activity(
            write_remittance,
            args=[inp.bucket, remit_key, rows, inp.remit_sftp],
            start_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=timedelta(minutes=1),
            retry_policy=LIGHT_RETRY,
        )

        # --- analytics slot-in: the generic dbt pipeline, BY NAME --------
        self._set_stage(Stage.ANALYTICS)
        spec_kwargs = await workflow.execute_activity(
            resolve_settlements_spec,
            args=[inp.bucket, remit_key, inp.catalog, inp.spark_remote],
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=LIGHT_RETRY,
        )
        etl = await workflow.execute_child_workflow(
            "EtlPipelineWorkflow",
            spec_kwargs,
            id=f"{wid}-settlements",
            task_queue="etl-pipeline",
            search_attributes=TypedSearchAttributes([
                SearchAttributePair(BATCH_ID, wid),
                SearchAttributePair(ROUTE, "settlements"),
            ]),
        )

        self._set_stage(Stage.COMPLETE)
        return {
            "orders": len(rows) + counts["duplicate"] + counts["errored"],
            "outcomes": {k: counts[k] for k in
                         ("settled", "denied", "escalated", "duplicate", "errored")},
            "by_source": sources,
            "ingest": {
                "landed": ingest["landed"],
                "quarantined": ingest["quarantined"],
            },
            "remittance": remit,
            "etl": etl,
        }


# --------------------------------------------------------------------------
# Activities (the settlement-intake fleet's code)
# --------------------------------------------------------------------------

# Bounded record-splitting for workflow fan-out (four-test policy:
# byte-shaped, BOUNDED, streamed source, profiled fleet — precedent:
# preprocess_file). The caps exist because every record transits workflow
# history as a child input; Temporal payloads must stay small. At real
# scale (100k-record batches) children fetch their own record by S3
# pointer + index instead — see docs/order-settlement.md.
SPLIT_MAX_BYTES = 8 * 1024**2
_NUMERIC = {"item_count": int, "submitted_amount": float}


def _s3():
    return boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL") or None)


@activity.defn
def split_batch(bucket: str, landed_key: str, max_records: int) -> list:
    """Parse a landed batch file into order records. Rows with bad numeric
    fields are NOT dropped — they carry `parse_error` so their lifecycle
    workflow denies them visibly (lineage for bad data)."""
    s3 = _s3()
    size = s3.head_object(Bucket=bucket, Key=landed_key)["ContentLength"]
    if size > SPLIT_MAX_BYTES:
        raise ApplicationError(
            f"{landed_key}: {size} bytes exceeds the {SPLIT_MAX_BYTES} split cap",
            non_retryable=True,
        )
    body = s3.get_object(Bucket=bucket, Key=landed_key)["Body"]
    reader = csv.DictReader(io.TextIOWrapper(body, encoding="utf-8", errors="replace"))
    records: list = []
    for line, row in enumerate(reader, start=2):
        if len(records) >= max_records:
            raise ApplicationError(
                f"{landed_key}: more than {max_records} records", non_retryable=True
            )
        rec: dict = {
            "order_ref": (row.get("order_ref") or f"line-{line}").strip(),
            "vendor": (row.get("vendor") or "unknown").strip().lower(),
        }
        for fld, cast in _NUMERIC.items():
            raw = (row.get(fld) or "").strip()
            try:
                rec[fld] = cast(raw)
            except ValueError:
                rec[fld] = None
                rec["parse_error"] = f"bad {fld}: {raw!r}"
        records.append(rec)
        activity.heartbeat(f"{len(records)} records")
    if not records:
        raise ApplicationError(f"{landed_key}: no records", non_retryable=True)
    return records


@activity.defn
async def write_remittance(
    bucket: str, key: str, rows: list, remit_sftp: dict | None
) -> dict:
    """The outbound leg: outcome rows -> canonical CSV -> S3, and (when
    configured) delivered back to the vendor file server. Bounded by the
    split cap; header is already-canonical snake_case so the analytics
    entity needs no aliasing."""
    s3 = _s3()
    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as out:
        writer = csv.DictWriter(out, fieldnames=REMIT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
        local = out.name
    try:
        await asyncio.to_thread(s3.upload_file, local, bucket, key)
        activity.heartbeat("uploaded to s3")
        sftp_path = None
        if remit_sftp:
            async with asyncssh.connect(
                remit_sftp["host"],
                port=remit_sftp.get("port", 2222),
                username=remit_sftp.get("username", "demo"),
                password=remit_sftp.get("password", "demo"),
                known_hosts=None,  # test server; pin for anything real
            ) as conn:
                async with conn.start_sftp_client() as sftp:
                    sftp_path = f"{remit_sftp.get('path', '/outbound')}/{os.path.basename(key)}"
                    await sftp.put(local, sftp_path)
            activity.heartbeat("delivered to vendor server")
    finally:
        os.unlink(local)
    return {"key": key, "rows": len(rows), "sftp_path": sftp_path}


@activity.defn
def resolve_settlements_spec(
    bucket: str, remit_key: str, catalog: dict | None, spark_remote: str | None
) -> dict:
    """Spec envelope -> DbtSparkJob kwargs for the analytics child — the
    same resolution shape the ingest pipeline uses, applied to this
    tenant's OUTBOUND file. The landing gate derives from the canonical
    model (single source of truth for the settlements entity)."""
    from ingest.activities import canonical_columns  # etl/ on PYTHONPATH

    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.abspath(os.path.join(here, "..", ".."))
    with open(os.path.join(root, "etl", "specs", "settlements.json")) as f:
        spec = json.load(f)
    return {
        "bucket": bucket,
        "name": spec["name"],
        "project_dir": spec.get("project_dir", "dbt"),
        "dbt_args": spec.get("dbt_args", ["build"]),
        "dbt_vars": spec.get("dbt_vars", {}),
        "inputs": [
            {
                "key": remit_key,
                "table": spec["source_table"],
                "format": "csv",
                "hygiene": {
                    "sanitize_headers": True,
                    "require_columns": canonical_columns("stg_settlements"),
                },
            }
        ],
        "outputs": spec["outputs"],
        "catalog": catalog,
        "spark_remote": spark_remote,
        "seed_demo_data": False,
    }
