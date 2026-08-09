import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import (
    RetryPolicy,
    SearchAttributeKey,
    SearchAttributePair,
    TypedSearchAttributes,
)
from temporalio.exceptions import ChildWorkflowError

with workflow.unsafe.imports_passed_through():
    from activities import DbtSparkJob
    from ingest.activities import (
        IngestConfig,
        classify_route,
        discover_sftp_files,
        land_sftp_file,
        quarantine_file,
        resolve_consolidation_spec,
        resolve_transform_spec,
    )
    from workflow import EtlPipelineWorkflow

TASK_QUEUE = "file-ingest"

# Custom search attributes (registered by run.sh / the prod namespace-setup
# container). These make the UI list queryable by business dimensions:
#   BatchId = "<parent workflow id>"   -> one batch's whole tree
#   Route = "payments"                 -> one route across all batches
#   SourceFile STARTS_WITH "orders"    -> lineage for a file
BATCH_ID = SearchAttributeKey.for_keyword("BatchId")
ROUTE = SearchAttributeKey.for_keyword("Route")
SOURCE_FILE = SearchAttributeKey.for_keyword("SourceFile")

LIGHT_RETRY = RetryPolicy(maximum_attempts=5)


@workflow.defn
class FileIngestWorkflow:
    """SFTP -> S3 landing -> route by filename pattern -> dispatch a
    per-route transform spec as a child EtlPipelineWorkflow -> S3 curated.

    The workers only move bytes and metadata; ALL data processing (hygiene,
    typing, transformation) happens in Spark + dbt on the cluster, driven by
    the route's spec. Unroutable files and files that fail their column
    contract are quarantined server-side without failing the batch.

    Files fan out in parallel. When a consolidation spec is configured (and
    a persistent catalog makes cross-job tables visible), a final child job
    joins the per-route outputs.
    """

    @workflow.run
    async def run(self, cfg: IngestConfig) -> dict:
        files = await workflow.execute_activity(
            discover_sftp_files,
            cfg,
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=LIGHT_RETRY,
        )

        results = list(
            await asyncio.gather(*(self._process_file(cfg, f) for f in files))
        )

        transformed = [r for r in results if r["status"] == "transformed"]
        consolidation = None
        if cfg.consolidation_spec and transformed:
            job_kwargs = await workflow.execute_activity(
                resolve_consolidation_spec,
                args=[cfg, cfg.consolidation_spec],
                start_to_close_timeout=timedelta(minutes=1),
                retry_policy=LIGHT_RETRY,
            )
            consolidation = await workflow.execute_child_workflow(
                EtlPipelineWorkflow.run,
                DbtSparkJob(**job_kwargs),
                id=f"consolidate-{cfg.consolidation_spec}-{workflow.info().workflow_id}",
                task_queue="etl-pipeline",
                search_attributes=TypedSearchAttributes(
                    [
                        SearchAttributePair(BATCH_ID, workflow.info().workflow_id),
                        SearchAttributePair(ROUTE, "consolidation"),
                    ]
                ),
            )

        return {
            "files_processed": len(results),
            "transformed": len(transformed),
            "quarantined": sum(1 for r in results if r["status"] == "quarantined"),
            "results": results,
            "consolidation": consolidation,
        }

    async def _process_file(self, cfg: IngestConfig, filename: str) -> dict:
        landed_key = await workflow.execute_activity(
            land_sftp_file,
            args=[cfg, filename],
            start_to_close_timeout=timedelta(minutes=10),
            heartbeat_timeout=timedelta(minutes=1),
            retry_policy=LIGHT_RETRY,
        )

        route = await workflow.execute_activity(
            classify_route,
            filename,
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=LIGHT_RETRY,
        )
        if route is None:
            quarantine_key = await workflow.execute_activity(
                quarantine_file,
                args=[cfg, landed_key, "no route pattern matched"],
                start_to_close_timeout=timedelta(minutes=1),
                retry_policy=LIGHT_RETRY,
            )
            return {
                "file": filename,
                "status": "quarantined",
                "quarantine_key": quarantine_key,
                "error": "no route pattern matched",
            }

        job_kwargs = await workflow.execute_activity(
            resolve_transform_spec,
            args=[cfg, route, landed_key],
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=LIGHT_RETRY,
        )

        try:
            transform = await workflow.execute_child_workflow(
                EtlPipelineWorkflow.run,
                DbtSparkJob(**job_kwargs),
                id=f"transform-{route}-{filename}",
                task_queue="etl-pipeline",
                search_attributes=TypedSearchAttributes(
                    [
                        SearchAttributePair(BATCH_ID, workflow.info().workflow_id),
                        SearchAttributePair(ROUTE, route),
                        SearchAttributePair(SOURCE_FILE, filename),
                    ]
                ),
            )
        except ChildWorkflowError as err:
            # Transform rejected the file (e.g. column contract violation) —
            # quarantine it server-side; the batch continues.
            reason = str(err.cause) if err.cause else str(err)
            quarantine_key = await workflow.execute_activity(
                quarantine_file,
                args=[cfg, landed_key, reason],
                start_to_close_timeout=timedelta(minutes=1),
                retry_policy=LIGHT_RETRY,
            )
            return {
                "file": filename,
                "status": "quarantined",
                "route": route,
                "quarantine_key": quarantine_key,
                "error": reason[:500],
            }

        return {
            "file": filename,
            "status": "transformed",
            "landed_key": landed_key,
            "route": route,
            "transform": transform,
        }
