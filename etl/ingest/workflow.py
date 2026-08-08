import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import (
    SearchAttributeKey,
    SearchAttributePair,
    TypedSearchAttributes,
)
from temporalio.exceptions import ActivityError

# Custom search attributes (registered by run.sh / the prod namespace-setup
# container). These make the UI list queryable by business dimensions:
#   BatchId = "<parent workflow id>"   -> one batch's whole tree
#   Route = "payments"                 -> one route across all batches
#   SourceFile STARTS_WITH "orders"    -> lineage for a file
BATCH_ID = SearchAttributeKey.for_keyword("BatchId")
ROUTE = SearchAttributeKey.for_keyword("Route")
SOURCE_FILE = SearchAttributeKey.for_keyword("SourceFile")

with workflow.unsafe.imports_passed_through():
    from activities import DbtSparkJob
    from ingest.activities import (
        IngestConfig,
        classify_file,
        discover_sftp_files,
        land_sftp_file,
        parse_file,
        resolve_consolidation_spec,
        resolve_transform_spec,
    )
    from workflow import EtlPipelineWorkflow

TASK_QUEUE = "file-ingest"


@workflow.defn
class FileIngestWorkflow:
    """SFTP -> S3 landing -> hygiene parse -> S3 staged -> classify on a
    routing field -> dispatch a per-route transform spec as a child
    EtlPipelineWorkflow -> S3 curated.

    Files fan out in parallel; structurally broken files are quarantined
    without failing the batch. When a consolidation spec is configured (and
    a persistent catalog makes cross-job tables visible), a final child job
    joins the per-route outputs.
    """

    @workflow.run
    async def run(self, cfg: IngestConfig) -> dict:
        files = await workflow.execute_activity(
            discover_sftp_files,
            cfg,
            start_to_close_timeout=timedelta(minutes=1),
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
        )
        try:
            parsed = await workflow.execute_activity(
                parse_file,
                args=[cfg, landed_key],
                start_to_close_timeout=timedelta(minutes=5),
            )
        except ActivityError as err:
            # Structurally broken file: it was quarantined by the activity;
            # record it and let the rest of the batch continue.
            return {"file": filename, "status": "quarantined", "error": str(err.cause)}

        route = await workflow.execute_activity(
            classify_file,
            args=[cfg, parsed["staged_key"]],
            start_to_close_timeout=timedelta(minutes=1),
        )
        job_kwargs = await workflow.execute_activity(
            resolve_transform_spec,
            args=[cfg, route, parsed["staged_key"]],
            start_to_close_timeout=timedelta(minutes=1),
        )

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

        return {
            "file": filename,
            "status": "transformed",
            "landed_key": landed_key,
            "staged": parsed,
            "route": route,
            "transform": transform,
        }
