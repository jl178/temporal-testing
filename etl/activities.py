"""Activities for the dbt-Spark batch ETL pipeline.

The pipeline is generic: `DbtSparkJob` describes any dbt-spark workload
(inputs to land as Spark tables, a dbt project + CLI args, outputs to export
to S3). `spark_job.py` executes the spec. The demo instance
(orders -> daily_revenue) lives in demo.py; specs under specs/ build real
instances (see ingest/).

Execution posture (see docs/etl.md): by default the transform activity is a
thin client — dbt compiles SQL and an EXTERNAL Spark service executes it
(spark_remote: a local Spark Connect container, or an EMR Serverless session
on AWS). The EMR batch submission is always exercised (real control-plane
calls; local emulators don't run the compute). In-process Spark exists only
as the explicit fallback, routed to the compute-large queue.
"""
import asyncio
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from dataclasses import dataclass, field

import boto3
from temporalio import activity

from runtime_env import DEFAULT_BUCKET
from temporalio.exceptions import ApplicationError

HERE = os.path.dirname(os.path.abspath(__file__))


def _s3():
    return boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL") or None)


def _emr():
    return boto3.client(
        "emr-serverless", endpoint_url=os.environ.get("AWS_ENDPOINT_URL") or None
    )


@dataclass
class DbtSparkJob:
    """A generic dbt-spark workload — a plain dataclass on purpose.

    Temporal payloads are data contracts, not SDK objects: the SDK
    serializes whatever you pass (dataclasses via the default JSON
    converter), so there is no base class to extend — and coupling this
    contract to SDK types would tie payload evolution to SDK upgrades.
    Fields added later MUST carry a neutral default (that's what lets old
    histories and in-flight workflows deserialize under new code); fields
    below with defaults are exactly the optional/appended ones. The
    demo instance lives in demo.py, never here.

    Keys are S3 keys within `bucket`.
    """

    name: str
    # [{key, table, format}] — landed as Spark tables before dbt runs
    inputs: list
    # [{table, key, format}] — exported to S3 after dbt runs
    outputs: list
    # The deployment's data lake (CDK injects ETL_BUCKET on AWS).
    bucket: str = DEFAULT_BUCKET
    # dbt project directory, relative to etl/ (uploaded to S3 for EMR runs)
    project_dir: str = "dbt"
    # Select this job's models — the shared dbt project holds every
    # route's models, and unselected ones would fail on missing sources.
    dbt_args: list = field(default_factory=lambda: ["build"])
    dbt_vars: dict = field(default_factory=dict)
    # Optional persistent table catalog (see spark_job.py docstring).
    # None -> ephemeral in-memory catalog, e.g.:
    #   {"type": "rest", "name": "lake", "uri": "http://localhost:8181",
    #    "warehouse": "s3://etl-data/warehouse"}         (local Iceberg REST)
    #   {"type": "glue", "name": "lake", "warehouse": "s3://bucket/warehouse"}
    catalog: dict | None = None
    # Demo-only: run the demo seeding activity first (see demo.py). Real
    # workloads' inputs are produced upstream (e.g. by the ingest pipeline).
    seed_demo_data: bool = False
    # Optional Spark Connect endpoint: dbt runs in the activity and the SQL
    # executes on the remote cluster. Locally sc://localhost:15002
    # (nix run .#spark-up); on AWS an EMR Serverless interactive session
    # endpoint (emr-7.13+). Mutually exclusive with `catalog` (configure the
    # catalog on the server/application in this mode).
    spark_remote: str | None = None
    # True on AWS: the EMR Serverless batch job IS the compute (it runs the
    # full spec), so the local transform step is skipped. False locally,
    # where emulators run EMR's control plane but not its compute.
    emr_is_compute: bool = False

    def artifact_prefix(self) -> str:
        return f"jobs/{self.name}"

    def to_spec(self, project: str) -> dict:
        """The spec spark_job.py consumes; `project` is a local dir or s3:// URI."""
        return {
            "name": self.name,
            "project": project,
            "catalog": self.catalog,
            "spark_remote": self.spark_remote,
            "dbt_args": list(self.dbt_args),
            "dbt_vars": dict(self.dbt_vars),
            "inputs": [
                {
                    "source": f"s3://{self.bucket}/{i['key']}",
                    "table": i["table"],
                    "format": i.get("format", "csv"),
                    "hygiene": i.get("hygiene"),
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


# NOTE on sync vs async: boto3 is blocking, and a blocking call inside an
# `async def` activity stalls the worker's entire event loop (every other
# activity and workflow task on that worker). Blocking activities here are
# therefore plain `def` — the SDK runs them on the worker's thread pool.
# Only genuinely-async work (the transform subprocess) stays `async def`.


@activity.defn
def submit_emr_job(job: DbtSparkJob) -> dict:
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

    # Deployment provides the real application/role (CDK injects these on
    # AWS); the lookup-or-create fallback serves the local emulator.
    app_id = os.environ.get("EMR_APPLICATION_ID")
    if not app_id:
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

    spark_submit: dict = {
        "entryPoint": f"s3://{job.bucket}/{prefix}/spark_job.py",
        "entryPointArguments": [
            "--spec", f"s3://{job.bucket}/{prefix}/spec.json",
            "--work-dir", "/tmp/etl-work",
        ],
    }
    # Runtime-owned Spark confs (e.g. the EMR-bundled Iceberg jar) come from
    # the deployment, not code — local and AWS stay one codepath.
    if os.environ.get("EMR_SPARK_SUBMIT_PARAMS"):
        spark_submit["sparkSubmitParameters"] = os.environ["EMR_SPARK_SUBMIT_PARAMS"]

    run_id = emr.start_job_run(
        applicationId=app_id,
        executionRoleArn=os.environ.get(
            "EMR_EXECUTION_ROLE_ARN", "arn:aws:iam::000000000000:role/emr-serverless-etl"
        ),
        jobDriver={"sparkSubmit": spark_submit},
    )["jobRunId"]

    terminal = {"SUCCESS", "FAILED", "CANCELLED"}
    while True:
        run = emr.get_job_run(applicationId=app_id, jobRunId=run_id)["jobRun"]
        state = run["state"]
        activity.heartbeat(state)
        if state in terminal:
            result = {"applicationId": app_id, "jobRunId": run_id, "state": state}
            if state != "SUCCESS":
                # Surface the platform's reason (counters/state only, no data).
                result["stateDetails"] = run.get("stateDetails", "")
            return result
        time.sleep(5)


@activity.defn
async def run_local_transform(job: DbtSparkJob) -> dict:
    """Compute step: run the same spec through spark_job.py in this environment."""
    # Scratch space is unique per workflow RUN, not just per job name — a
    # stale run's retried task must never collide with a live one (spec
    # file, derby metastore, warehouse).
    run_id = activity.info().workflow_run_id[:8]
    work_dir = os.path.join(HERE, ".work", f"{job.name}-{run_id}")
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
        # The launcher owns the default master; under EMR spark-submit the
        # runner must NOT set one (the platform provides it).
        env={
            **os.environ,
            "PYTHONUNBUFFERED": "1",
            "SPARK_MASTER": os.environ.get("SPARK_MASTER", "local[2]"),
        },
    )
    assert proc.stdout is not None
    # Log hygiene: raw Spark/dbt output can embed data fragments (e.g. a
    # malformed row quoted in an error). It goes to a worker-local log file,
    # never into heartbeats or exception messages — those carry only
    # counters and our own controlled ETL_RESULT payload (aggregates).
    #
    # Heartbeats are wall-clock driven, NOT output-driven: Spark goes quiet
    # for minutes during long stages, and a missed heartbeat would make the
    # server retry while this attempt's subprocess still runs — two
    # concurrent writers corrupting the same tables.
    log_path = os.path.join(work_dir, "job.log")
    result: dict | None = None
    lines = 0

    async def _pulse() -> None:
        while True:
            activity.heartbeat(f"transform {job.name} running, {lines} log lines")
            await asyncio.sleep(15)

    pulse = asyncio.create_task(_pulse())
    try:
        with open(log_path, "w") as log:
            async for line in proc.stdout:
                text = line.decode(errors="replace").rstrip()
                log.write(text + "\n")
                lines += 1
                if text.startswith("ETL_RESULT "):
                    result = json.loads(text[len("ETL_RESULT "):])
        rc = await proc.wait()
    except asyncio.CancelledError:
        # Cancelled (timeout, workflow cancel): take the subprocess down
        # with us so a retry never races a zombie attempt.
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=10)
        except (TimeoutError, asyncio.TimeoutError):
            proc.kill()
        raise
    finally:
        pulse.cancel()
    if result is not None and not result.get("success"):
        # The job itself reported a deterministic failure (contract
        # violation, dbt error) — retrying cannot help.
        raise ApplicationError(
            f"transform rejected the job: {result.get('error')} (full log: {log_path})",
            non_retryable=True,
        )
    if rc != 0 or result is None:
        # Died without a verdict (crash, OOM, lost connection) — transient.
        # The work dir (incl. job.log) is kept for debugging.
        raise RuntimeError(f"transform died (rc={rc}, log: {log_path})")
    shutil.rmtree(work_dir, ignore_errors=True)
    return result


MAX_INLINE_BYTES = 5 * 1024 * 1024


@activity.defn
def validate_output(job: DbtSparkJob) -> dict:
    """Every declared output landed in S3 and is non-empty — verified from
    metadata, never by pulling large data through the worker. Small json
    outputs (capped marts) are returned inline so callers can assert on
    content; parquet outputs are distributed part-file prefixes and are
    validated by listing."""
    s3 = _s3()
    results = []
    for out in job.outputs:
        fmt = out.get("format", "json")
        if fmt == "parquet":
            resp = s3.list_objects_v2(Bucket=job.bucket, Prefix=out["key"])
            objects = [o for o in resp.get("Contents", []) if o["Size"] > 0]
            if not any("part-" in o["Key"] for o in objects):
                raise RuntimeError(f"output prefix {out['key']} has no part files")
            results.append(
                {
                    "key": out["key"],
                    "objects": len(objects),
                    "bytes": sum(o["Size"] for o in objects),
                }
            )
            continue
        size = s3.head_object(Bucket=job.bucket, Key=out["key"])["ContentLength"]
        if size == 0:
            raise RuntimeError(f"output {out['key']} is empty")
        entry: dict = {"key": out["key"], "bytes": size}
        if size <= MAX_INLINE_BYTES:
            rows = json.loads(
                s3.get_object(Bucket=job.bucket, Key=out["key"])["Body"].read()
            )
            if not rows:
                raise RuntimeError(f"output {out['key']} has no rows")
            entry["rows"] = len(rows)
            entry["data"] = rows
        results.append(entry)
    return {"outputs": results}
