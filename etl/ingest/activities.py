"""Activities for the file-ingestion pipeline.

Architecture rule: THE WORKER NEVER TOUCHES DATA CONTENT. Every activity
here is either a byte stream (SFTP -> S3, the unavoidable minimum of the
pull model), pure metadata (filename routing, registry lookups), or a
server-side S3 copy (quarantine). All parsing, hygiene, typing, and
transformation happen in Spark + dbt on the cluster (see spark_job.py:
permissive read, header sanitization, column contracts — driven by each
route's spec).

On AWS, even the SFTP stream disappears: AWS Transfer Family lands vendor
SFTP directly into S3 and an S3 event starts the workflow — the worker then
touches no bytes at all.
"""
import asyncio
import fnmatch
import json
import os
import tempfile
from dataclasses import dataclass, field

import asyncssh
import boto3
from temporalio import activity

HERE = os.path.dirname(os.path.abspath(__file__))
SPECS_DIR = os.path.join(os.path.dirname(HERE), "specs")


def _s3():
    return boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL") or None)


@dataclass
class SftpSource:
    host: str = "localhost"
    port: int = 2222
    username: str = "demo"
    password: str = "demo"
    path: str = "/upload"
    pattern: str = "*.csv"


@dataclass
class IngestConfig:
    sftp: SftpSource = field(default_factory=SftpSource)
    bucket: str = "etl-data"
    landing_prefix: str = "landing"
    quarantine_prefix: str = "quarantine"
    # Passed through to the spawned DbtSparkJob (None = ephemeral catalog).
    catalog: dict | None = None
    # Optional Spark Connect endpoint passed to spawned jobs.
    spark_remote: str | None = None
    # Optional spec (etl/specs/<name>.json) run once after the per-file
    # transforms, to join their outputs. Requires a persistent catalog —
    # cross-job tables only exist when one is configured.
    consolidation_spec: str | None = None


def _sftp_conn(cfg: SftpSource):
    # known_hosts=None: throwaway test server whose host key changes on
    # every container recreation. Pin known_hosts for anything real.
    return asyncssh.connect(
        cfg.host,
        port=cfg.port,
        username=cfg.username,
        password=cfg.password,
        known_hosts=None,
    )


@activity.defn
async def discover_sftp_files(cfg: IngestConfig) -> list:
    """List remote files matching the source pattern. Metadata only."""
    async with _sftp_conn(cfg.sftp) as conn:
        async with conn.start_sftp_client() as sftp:
            names = await sftp.listdir(cfg.sftp.path)
    return sorted(n for n in names if fnmatch.fnmatch(n, cfg.sftp.pattern))


@activity.defn
async def land_sftp_file(cfg: IngestConfig, filename: str) -> str:
    """Stream one remote file into the S3 landing zone (disk-buffered, not
    memory). Returns the S3 key. This is the only byte-moving activity."""
    s3 = _s3()
    try:
        await asyncio.to_thread(s3.create_bucket, Bucket=cfg.bucket)
    except s3.exceptions.ClientError:
        pass
    with tempfile.TemporaryDirectory() as tmp:
        local = os.path.join(tmp, filename)
        async with _sftp_conn(cfg.sftp) as conn:
            async with conn.start_sftp_client() as sftp:
                await sftp.get(f"{cfg.sftp.path}/{filename}", local)
        activity.heartbeat(f"downloaded {filename}")
        key = f"{cfg.landing_prefix}/{filename}"
        # boto3 is blocking; never call it directly on the event loop.
        await asyncio.to_thread(s3.upload_file, local, cfg.bucket, key)
    return key


@activity.defn
def classify_route(filename: str) -> str | None:
    """Route from filename pattern — pure metadata, zero data read. Routing
    by content would force the worker to open the file; vendor contracts
    are expressed as name patterns in the registry instead. Returns None
    for unrecognized files (caller quarantines)."""
    with open(os.path.join(SPECS_DIR, "registry.json")) as f:
        registry = json.load(f)
    for entry in registry["routes"]:
        if fnmatch.fnmatch(filename, entry["pattern"]):
            return entry["route"]
    return None


@activity.defn
def quarantine_file(cfg: IngestConfig, landed_key: str, reason: str) -> str:
    """Server-side S3 copy into quarantine/ — no bytes through the worker."""
    s3 = _s3()
    quarantine_key = f"{cfg.quarantine_prefix}/{os.path.basename(landed_key)}"
    s3.copy_object(
        Bucket=cfg.bucket,
        CopySource={"Bucket": cfg.bucket, "Key": landed_key},
        Key=quarantine_key,
        Metadata={"quarantine-reason": reason[:1024]},
        MetadataDirective="REPLACE",
    )
    return quarantine_key


def _job_kwargs(cfg: IngestConfig, spec: dict, inputs: list) -> dict:
    """Shared DbtSparkJob construction for every dispatched spec."""
    return {
        "bucket": cfg.bucket,
        "name": spec["name"],
        "project_dir": spec.get("project_dir", "dbt"),
        "dbt_args": spec.get("dbt_args", ["build"]),
        "dbt_vars": spec.get("dbt_vars", {}),
        "inputs": inputs,
        "outputs": spec["outputs"],
        "catalog": cfg.catalog,
        "spark_remote": cfg.spark_remote,
        "seed_demo_data": False,
    }


def _load_spec(name: str) -> dict:
    with open(os.path.join(SPECS_DIR, f"{name}.json")) as f:
        return json.load(f)


@activity.defn
def resolve_transform_spec(cfg: IngestConfig, route: str, landed_key: str) -> dict:
    """Registry lookup: route -> spec -> DbtSparkJob kwargs. The landed file
    is wired in as a raw csv input with cluster-side hygiene (sanitized
    headers + the spec's column contract); the route's dbt project holds
    all semantic transformation."""
    with open(os.path.join(SPECS_DIR, "registry.json")) as f:
        registry = json.load(f)
    entry = next(e for e in registry["routes"] if e["route"] == route)
    spec = _load_spec(entry["spec"])
    inputs = [
        {
            "key": landed_key,
            "table": spec["source_table"],
            "format": "csv",
            "hygiene": {
                "sanitize_headers": True,
                "require_columns": spec.get("require_columns", []),
            },
        }
    ]
    return _job_kwargs(cfg, spec, inputs)


@activity.defn
def resolve_consolidation_spec(cfg: IngestConfig, spec_name: str) -> dict:
    """A named spec with no landed input: its dbt models read the tables the
    per-file transform jobs left in the persistent catalog."""
    return _job_kwargs(cfg, _load_spec(spec_name), inputs=[])