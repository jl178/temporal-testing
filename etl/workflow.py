from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import (
        DbtSparkJob,
        run_local_transform,
        seed_raw_data,
        submit_emr_job,
        validate_output,
    )

TASK_QUEUE = "etl-pipeline"


@workflow.defn
class EtlPipelineWorkflow:
    """Generic dbt-Spark batch ETL: seed raw data (demo extract) -> submit
    the job to EMR Serverless (emulated control plane) -> run the identical
    spec locally as real Spark + dbt compute -> validate every declared
    output landed in S3."""

    @workflow.run
    async def run(self, job: DbtSparkJob) -> dict:
        raw_rows = None
        if job.seed_demo_data:
            raw_rows = await workflow.execute_activity(
                seed_raw_data,
                job,
                start_to_close_timeout=timedelta(minutes=1),
            )

        emr = await workflow.execute_activity(
            submit_emr_job,
            job,
            start_to_close_timeout=timedelta(minutes=10),
            heartbeat_timeout=timedelta(seconds=30),
        )
        if emr["state"] != "SUCCESS":
            raise RuntimeError(f"EMR Serverless job ended in {emr['state']}")

        transform = await workflow.execute_activity(
            run_local_transform,
            job,
            start_to_close_timeout=timedelta(minutes=15),
            heartbeat_timeout=timedelta(minutes=2),
        )

        validation = await workflow.execute_activity(
            validate_output,
            job,
            start_to_close_timeout=timedelta(minutes=1),
        )

        return {
            "raw_rows": raw_rows,
            "emr": emr,
            "transform": transform,
            "validation": validation,
        }
