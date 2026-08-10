from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from activities import (
        DbtSparkJob,
        run_local_transform,
        seed_raw_data,
        submit_emr_job,
        validate_output,
    )

TASK_QUEUE = "etl-pipeline"
# Noisy-neighbor isolation: in-process compute runs on the platform's
# generic big-compute lane, polled by a `large`-profile fleet (resource-
# tuned slots, queue rate cap). A misbehaving heavy activity can only hurt
# other heavy activities — coordination and I/O fleets stay unaffected.
HEAVY_TASK_QUEUE = "compute-large"

LIGHT_RETRY = RetryPolicy(maximum_attempts=5)
HEAVY_RETRY = RetryPolicy(maximum_attempts=3)


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
                retry_policy=LIGHT_RETRY,
            )

        emr = await workflow.execute_activity(
            submit_emr_job,
            job,
            # Real EMR runs pay a cold start + provisioning; the emulated
            # control plane returns in seconds. One generous cap covers both.
            start_to_close_timeout=timedelta(minutes=30),
            heartbeat_timeout=timedelta(seconds=30),
            retry_policy=LIGHT_RETRY,
        )
        if emr["state"] != "SUCCESS":
            raise RuntimeError(f"EMR Serverless job ended in {emr['state']}")

        if job.emr_is_compute:
            # EMR executed the full spec (real AWS) — nothing left to compute.
            transform = None
        else:
            # In spark_remote mode the transform activity is a thin client
            # (dbt compiles SQL, the external Spark service executes it) — it
            # runs on the light queue like any other launcher, mirroring
            # production. Only the in-process-Spark fallback is heavy compute.
            transform = await workflow.execute_activity(
                run_local_transform,
                job,
                task_queue=TASK_QUEUE if job.spark_remote else HEAVY_TASK_QUEUE,
                start_to_close_timeout=timedelta(minutes=15),
                heartbeat_timeout=timedelta(minutes=2),
                retry_policy=HEAVY_RETRY,
            )

        validation = await workflow.execute_activity(
            validate_output,
            job,
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=LIGHT_RETRY,
        )

        return {
            "raw_rows": raw_rows,
            "emr": emr,
            "transform": transform,
            "validation": validation,
        }
