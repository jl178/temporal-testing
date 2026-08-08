"""Activities for the batch ETL pipeline.

The EMR Serverless submission runs against whatever AWS_ENDPOINT_URL points
at (a local emulator such as LocalEmu, or real AWS). Local emulators only
emulate the EMR Serverless control plane — job runs reach a terminal state
but execute nothing — so the pipeline also runs the identical transformation
(spark_job.py: Spark + dbt build) locally as its compute step.
"""
import asyncio
import csv
import io
import json
import os
import subprocess
import sys
from dataclasses import dataclass

import boto3
from temporalio import activity

HERE = os.path.dirname(os.path.abspath(__file__))

ORDERS = [
    ("1", "acme", "2026-08-01", "120.50", "completed"),
    ("2", "acme", "2026-08-01", "80.00", "completed"),
    ("3", "globex", "2026-08-01", "42.25", "CANCELLED"),
    ("4", "globex", "2026-08-02", "310.10", "completed"),
    ("5", "initech", "2026-08-02", "55.99", "Completed"),
    ("6", "initech", "2026-08-03", "12.00", "pending"),
    ("7", "acme", "2026-08-03", "99.95", "completed"),
]


def _s3():
    return boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL") or None)


def _emr():
    return boto3.client(
        "emr-serverless", endpoint_url=os.environ.get("AWS_ENDPOINT_URL") or None
    )


@dataclass
class EtlConfig:
    bucket: str = "etl-data"
    raw_key: str = "raw/orders.csv"
    output_key: str = "marts/daily_revenue.json"
    job_script_key: str = "jobs/spark_job.py"


@activity.defn
async def seed_raw_data(config: EtlConfig) -> int:
    """Extract step: land the raw orders CSV in the S3 data lake."""
    s3 = _s3()
    try:
        s3.create_bucket(Bucket=config.bucket)
    except s3.exceptions.ClientError:
        pass  # already exists
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["order_id", "customer", "order_date", "amount", "status"])
    writer.writerows(ORDERS)
    s3.put_object(Bucket=config.bucket, Key=config.raw_key, Body=buf.getvalue())
    return len(ORDERS)


@activity.defn
async def submit_emr_job(config: EtlConfig) -> dict:
    """Submit the job to EMR Serverless and poll it to a terminal state."""
    emr = _emr()
    s3 = _s3()
    with open(os.path.join(HERE, "spark_job.py"), "rb") as f:
        s3.put_object(Bucket=config.bucket, Key=config.job_script_key, Body=f.read())

    apps = emr.list_applications().get("applications", [])
    app_id = next((a["id"] for a in apps if a["name"] == "temporal-etl"), None)
    if app_id is None:
        app_id = emr.create_application(
            name="temporal-etl", type="SPARK", releaseLabel="emr-7.0.0"
        )["applicationId"]
    try:
        emr.start_application(applicationId=app_id)
    except Exception:
        pass  # some backends auto-start

    run_id = emr.start_job_run(
        applicationId=app_id,
        executionRoleArn="arn:aws:iam::000000000000:role/emr-serverless-etl",
        jobDriver={
            "sparkSubmit": {
                "entryPoint": f"s3://{config.bucket}/{config.job_script_key}",
                "entryPointArguments": [
                    "--bucket", config.bucket,
                    "--raw-key", config.raw_key,
                    "--output-key", config.output_key,
                    "--work-dir", "/tmp/etl-work",
                ],
            }
        },
    )["jobRunId"]

    terminal = {"SUCCESS", "FAILED", "CANCELLED"}
    while True:
        state = emr.get_job_run(applicationId=app_id, jobRunId=run_id)["jobRun"]["state"]
        activity.heartbeat(state)
        if state in terminal:
            return {"applicationId": app_id, "jobRunId": run_id, "state": state}
        await asyncio.sleep(2)


@activity.defn
async def run_local_transform(config: EtlConfig) -> dict:
    """Compute step: run spark_job.py (Spark + dbt build) in this environment."""
    work_dir = os.path.join(HERE, ".work")
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        os.path.join(HERE, "spark_job.py"),
        "--bucket", config.bucket,
        "--raw-key", config.raw_key,
        "--output-key", config.output_key,
        "--work-dir", work_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    assert proc.stdout is not None
    result: dict | None = None
    async for line in proc.stdout:
        text = line.decode(errors="replace").rstrip()
        activity.heartbeat(text[-120:])
        if text.startswith("ETL_RESULT "):
            result = json.loads(text[len("ETL_RESULT "):])
    rc = await proc.wait()
    if rc != 0 or result is None or not result.get("success"):
        raise RuntimeError(f"transform failed (rc={rc}): {result}")
    return result


@activity.defn
async def validate_output(config: EtlConfig) -> dict:
    """Load-validation step: the mart landed in S3 and has sane contents."""
    s3 = _s3()
    body = s3.get_object(Bucket=config.bucket, Key=config.output_key)["Body"].read()
    rows = json.loads(body)
    if not rows:
        raise RuntimeError("mart output is empty")
    for row in rows:
        if float(row["total_revenue"]) <= 0:
            raise RuntimeError(f"non-positive revenue row: {row}")
    return {"mart_rows": len(rows), "dates": [r["order_date"] for r in rows]}
