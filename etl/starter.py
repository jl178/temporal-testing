import asyncio
import json
import os
import uuid

from temporalio.client import Client

from activities import DbtSparkJob
from workflow import TASK_QUEUE, EtlPipelineWorkflow


async def main() -> None:
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )
    # Demo instance: orders -> daily_revenue. Stateless by default; set
    # ICEBERG_REST_URI (e.g. http://localhost:8181, `nix run .#catalog-up`)
    # to materialize models as Iceberg tables in a persistent catalog.
    from runtime_env import catalog_from_env, spark_remote_from_env

    # On AWS (EMR_APPLICATION_ID set) the EMR batch job is the compute:
    # catalog is Glue, and no local/remote Spark step runs.
    emr_compute = bool(os.environ.get("EMR_APPLICATION_ID"))
    job = DbtSparkJob(
        catalog=catalog_from_env(),
        spark_remote=None if emr_compute else spark_remote_from_env(),
        emr_is_compute=emr_compute,
    )
    result = await client.execute_workflow(
        EtlPipelineWorkflow.run,
        job,
        id=f"etl-{job.name}-{uuid.uuid4()}",
        task_queue=TASK_QUEUE,
    )
    print("Workflow result:")
    print(json.dumps(result, indent=2))

    assert result["emr"]["state"] == "SUCCESS", result["emr"]
    if not emr_compute:
        mart = result["transform"]["outputs"][0]
        assert mart["rows"] == 3, mart
    data = result["validation"]["outputs"][0]["data"]
    revenue = sum(float(r["total_revenue"]) for r in data)
    assert abs(revenue - 666.54) < 0.01, revenue
    print("ETL PIPELINE: PASS")


if __name__ == "__main__":
    asyncio.run(main())
