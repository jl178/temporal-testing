"""The DEMO instance of the generic pipeline: orders -> daily_revenue.

Everything demo-shaped lives here — seed data, the seeding activity, and
the fully-specified job — so `activities.py` stays a pure platform module.
Real workloads never import this; they construct `DbtSparkJob`s from
specs (see ingest/) or their own code.

Fleets serving the demo register it explicitly:
    python -m worker_platform --queue etl-pipeline --profile medium \
        --activities activities --activities demo
"""
import csv
import io
import os

from temporalio import activity

from activities import DbtSparkJob, _s3
from runtime_env import catalog_from_env, spark_remote_from_env

ORDERS = [
    ("1", "acme", "2026-08-01", "120.50", "completed"),
    ("2", "acme", "2026-08-01", "80.00", "completed"),
    ("3", "globex", "2026-08-01", "42.25", "CANCELLED"),
    ("4", "globex", "2026-08-02", "310.10", "completed"),
    ("5", "initech", "2026-08-02", "55.99", "Completed"),
    ("6", "initech", "2026-08-03", "12.00", "pending"),
    ("7", "acme", "2026-08-03", "99.95", "completed"),
]


def demo_job() -> DbtSparkJob:
    """The canonical demo workload, environment-aware like any tenant:
    Glue/REST catalog, Spark Connect, or EMR-as-compute per env vars."""
    emr_compute = bool(os.environ.get("EMR_APPLICATION_ID"))
    return DbtSparkJob(
        name="daily-revenue",
        dbt_args=["build", "--select", "tag:orders"],
        inputs=[{"key": "raw/orders.csv", "table": "raw.orders", "format": "csv"}],
        outputs=[
            {
                "table": "analytics.daily_revenue",
                "key": "marts/daily_revenue.json",
                "format": "json",
            }
        ],
        catalog=catalog_from_env(),
        spark_remote=None if emr_compute else spark_remote_from_env(),
        emr_is_compute=emr_compute,
        seed_demo_data=True,
    )


@activity.defn
def seed_raw_data(job: DbtSparkJob) -> int:
    """Demo extract step: land the raw orders CSV in the S3 data lake."""
    s3 = _s3()
    try:
        s3.create_bucket(Bucket=job.bucket)
    except s3.exceptions.ClientError:
        pass  # already exists
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["order_id", "customer", "order_date", "amount", "status"])
    writer.writerows(ORDERS)
    s3.put_object(Bucket=job.bucket, Key=job.inputs[0]["key"], Body=buf.getvalue())
    return len(ORDERS)


def verify(result: dict) -> None:
    """Assert the demo's KNOWN answers — deliberately concrete: a bare
    'workflow succeeded' would pass on silently-wrong data (the doubled-
    revenue incident was caught by exactly these numbers)."""
    assert result["emr"]["state"] == "SUCCESS", result["emr"]
    if result["transform"] is not None:  # None when EMR was the compute
        mart = result["transform"]["outputs"][0]
        assert mart["rows"] == 3, mart
    data = result["validation"]["outputs"][0]["data"]
    revenue = sum(float(r["total_revenue"]) for r in data)
    assert abs(revenue - 666.54) < 0.01, revenue
