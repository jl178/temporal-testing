"""Activities for the dbt-Spark batch ETL pipeline.

The pipeline is generic: `DbtSparkJob` describes any dbt-spark workload
(inputs to land as Spark tables, a dbt project + CLI args, outputs to export
to S3). `spark_job.py` executes the spec; the demo defaults below are just
one instance (orders -> daily_revenue).

The EMR Serverless submission runs against whatever AWS_ENDPOINT_URL points
at (a local emulator such as LocalEmu, or real AWS). Local emulators only
emulate the EMR Serverless control plane — job runs reach a terminal state
but execute nothing — so the pipeline also runs the identical spec locally
(spark_job.py: Spark + dbt) as its compute step.
"""
import asyncio
import csv
import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
from dataclasses import dataclass, field

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
class DbtSparkJob:
    """A generic dbt-spark workload. Keys are S3 keys within `bucket`."""

    bucket: str = "etl-data"
    name: str = "daily-revenue"
    # dbt project directory, relative to etl/ (uploaded to S3 for EMR runs)
    project_dir: str = "dbt"
    dbt_args: list = field(default_factory=lambda: ["build"])
    dbt_vars: dict = field(default_factory=dict)
    # [{key, table, format}] — landed as Spark tables before dbt runs
    inputs: list = field(
        default_factory=lambda: [
            {"key": "raw/orders.csv", "table": "raw.orders", "format": "csv"}
        ]
    )
    # [{table, key, format}] — exported to S3 after dbt runs
    outputs: list = field(
        default_factory=lambda: [
            {
                "table": "analytics.daily_revenue",
                "key": "marts/daily_revenue.json",
                "format": "json",
            }
        ]
    )
    # Optional persistent table catalog (see spark_job.py docstring).
    # None -> ephemeral in-memory catalog, e.g.:
    #   {"type": "rest", "name": "lake", "uri": "http://localhost:8181",
    #    "warehouse": "s3://etl-data/warehouse"}         (local Iceberg REST)
    #   {"type": "glue", "name": "lake", "warehouse": "s3://bucket/warehouse"}
    catalog: dict | None = None
    # Demo-only: seed the orders CSV as the first input. False when the
    # inputs were produced upstream (e.g. by the ingest pipeline).
    seed_demo_data: bool = True

    def artifact_prefix(self) -> str:
        return f"jobs/{self.name}"

    def to_spec(self, project: str) -> dict:
        """The spec spark_job.py consumes; `project` is a local dir or s3:// URI."""
        return {
            "name": self.name,
            "project": project,
            "catalog": self.catalog,
            "dbt_args": list(self.dbt_args),
            "dbt_vars": dict(self.dbt_vars),
            "inputs": [
                {
                    "source": f"s3://{self.bucket}/{i['key']}",
                    "table": i["table"],
                    "format": i.get("format", "csv"),
                }
                for i in self.inputs
            ],
            "outputs": [
                {
                    "table": o["table"],
                    "destination": f"s3://{self.bucket}/{o['key']}",
                    "format": o.get("format", "json"),
                }
                for o in self.outputs
            ],
        }


@activity.defn
async def seed_raw_data(job: DbtSparkJob) -> int:
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


@activity.defn
async def submit_emr_job(job: DbtSparkJob) -> dict:
    """Package the job (runner + dbt project + spec), submit to EMR
    Serverless, and poll to a terminal state."""
    emr = _emr()
    s3 = _s3()
    prefix = job.artifact_prefix()

    # Upload the generic runner.
    with open(os.path.join(HERE, "spark_job.py"), "rb") as f:
        s3.put_object(Bucket=job.bucket, Key=f"{prefix}/spark_job.py", Body=f.read())

    # Upload the dbt project as a tarball.
    project_local = os.path.join(HERE, job.project_dir)
    with tempfile.NamedTemporaryFile(suffix=".tar.gz") as tmp:
        with tarfile.open(tmp.name, "w:gz") as tar:
            tar.add(project_local, arcname=".")
        tmp.seek(0)
        s3.put_object(Bucket=job.bucket, Key=f"{prefix}/dbt-project.tar.gz", Body=tmp.read())

    # Upload the job spec, pointing at the uploaded project.
    spec = job.to_spec(project=f"s3://{job.bucket}/{prefix}/dbt-project.tar.gz")
    s3.put_object(Bucket=job.bucket, Key=f"{prefix}/spec.json", Body=json.dumps(spec))

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
                "entryPoint": f"s3://{job.bucket}/{prefix}/spark_job.py",
                "entryPointArguments": [
                    "--spec", f"s3://{job.bucket}/{prefix}/spec.json",
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
async def run_local_transform(job: DbtSparkJob) -> dict:
    """Compute step: run the same spec through spark_job.py in this environment."""
    work_dir = os.path.join(HERE, ".work")
    os.makedirs(work_dir, exist_ok=True)
    spec_path = os.path.join(work_dir, "spec.json")
    with open(spec_path, "w") as f:
        json.dump(job.to_spec(project=os.path.join(HERE, job.project_dir)), f)

    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        os.path.join(HERE, "spark_job.py"),
        "--spec", spec_path,
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
async def validate_output(job: DbtSparkJob) -> dict:
    """Every declared output landed in S3 and is non-empty; json outputs are
    returned inline (they are small marts) so callers can assert on content."""
    s3 = _s3()
    results = []
    for out in job.outputs:
        obj = s3.get_object(Bucket=job.bucket, Key=out["key"])
        body = obj["Body"].read()
        if not body:
            raise RuntimeError(f"output {out['key']} is empty")
        entry: dict = {"key": out["key"], "bytes": len(body)}
        if out.get("format", "json") == "json":
            rows = json.loads(body)
            if not rows:
                raise RuntimeError(f"output {out['key']} has no rows")
            entry["rows"] = len(rows)
            entry["data"] = rows
        results.append(entry)
    return {"outputs": results}
