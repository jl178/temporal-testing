from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import DbtSparkJob
    from ingest.activities import (
        IngestConfig,
        classify_file,
        discover_sftp_files,
        land_sftp_file,
        parse_file,
        resolve_transform_spec,
    )
    from workflow import EtlPipelineWorkflow

TASK_QUEUE = "file-ingest"


@workflow.defn
class FileIngestWorkflow:
    """SFTP -> S3 landing -> generic rules-based parse -> S3 staged ->
    classify on a routing field -> dispatch a per-route transform spec as a
    child EtlPipelineWorkflow -> S3 curated.

    Files are processed sequentially for readability; swap the loop for
    asyncio.gather over per-file child workflows to fan out.
    """

    @workflow.run
    async def run(self, cfg: IngestConfig) -> dict:
        files = await workflow.execute_activity(
            discover_sftp_files,
            cfg,
            start_to_close_timeout=timedelta(minutes=1),
        )

        processed = []
        for filename in files:
            landed_key = await workflow.execute_activity(
                land_sftp_file,
                args=[cfg, filename],
                start_to_close_timeout=timedelta(minutes=10),
                heartbeat_timeout=timedelta(minutes=1),
            )
            parsed = await workflow.execute_activity(
                parse_file,
                args=[cfg, landed_key],
                start_to_close_timeout=timedelta(minutes=5),
            )
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
            )

            processed.append(
                {
                    "file": filename,
                    "landed_key": landed_key,
                    "staged": parsed,
                    "route": route,
                    "transform": transform,
                }
            )

        return {"files_processed": len(processed), "results": processed}
