"""Activities for the file-ingestion pipeline.

Data-through-workers policy (docs/workers.md): a worker may process data
only when the operation is BYTE-shaped (not query-shaped), BOUNDED by an
enforced cap, STREAMED (never whole-payload-in-RAM), and running on an
appropriately PROFILED fleet. That admits the SFTP stream, the gunzip
preprocess, metadata routing, and server-side S3 copies here. Everything
query-shaped or unbounded — parsing, hygiene, typing, transformation —
happens in Spark + dbt on the cluster (see spark_job.py: permissive read,
header sanitization, column contracts — driven by each route's spec).

Sources are pluggable (SFTP or SMB — one per IngestConfig); discovery and
landing dispatch on which source is configured. Prod bindings differ by
protocol: AWS Transfer Family lands vendor SFTP directly into S3 (the
worker then touches no bytes at all), but Transfer speaks no SMB — an SMB
share (FSx, or on-prem over VPN/DX) keeps this worker-streamed landing
path even in production, which is why it must stay policy-compliant.
"""
import asyncio
import fnmatch
import gzip
import json
import os
import tempfile
from dataclasses import dataclass, field

import asyncssh
import boto3
import smbclient
import yaml
from temporalio import activity
from temporalio.exceptions import ApplicationError

from runtime_env import DEFAULT_BUCKET

HERE = os.path.dirname(os.path.abspath(__file__))
SPECS_DIR = os.path.join(os.path.dirname(HERE), "specs")
CANONICAL_MODEL = os.path.join(
    os.path.dirname(HERE), "dbt", "models", "staging", "schema.yml"
)


def canonical_columns(model_name: str) -> list:
    """The landing gate derives from the canonical data model — one source
    of truth for what a route's entity looks like."""
    with open(CANONICAL_MODEL) as f:
        doc = yaml.safe_load(f)
    model = next(m for m in doc["models"] if m["name"] == model_name)
    return [c["name"] for c in model.get("columns", [])]


def _s3():
    return boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL") or None)


@dataclass
class SftpSource:
    host: str = "localhost"
    port: int = 2222
    username: str = "demo"
    password: str = "demo"
    path: str = "/upload"
    pattern: str = "*.csv*"  # includes compressed drops (.csv.gz)


@dataclass
class SmbSource:
    host: str = "localhost"
    port: int = 1445
    username: str = "demo"
    password: str = "demopass"
    share: str = "upload"
    path: str = ""  # subdirectory within the share ("" = share root)
    pattern: str = "*.csv*"


@dataclass
class IngestConfig:
    sftp: SftpSource = field(default_factory=SftpSource)
    # When set, SMB is the batch's source and `sftp` is ignored. (Appended
    # optional field — payloads recorded before it existed still replay.)
    smb: SmbSource | None = None
    bucket: str = DEFAULT_BUCKET
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
    # False = ingest-only: land, preprocess, route, quarantine — but spawn
    # no transform children; a downstream consumer picks up from landing/
    # using the batch result (file -> route -> landed_key). Default True
    # preserves the full land->transform pipeline.
    dispatch_transforms: bool = True


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


_LAND_CHUNK = 8 * 1024**2


def _smb_root(cfg: SmbSource) -> str:
    base = rf"\\{cfg.host}\{cfg.share}"
    return rf"{base}\{cfg.path}" if cfg.path else base


def _smb_kwargs(cfg: SmbSource) -> dict:
    return {"username": cfg.username, "password": cfg.password, "port": cfg.port}


def _smb_list(cfg: SmbSource) -> list:
    return [
        n
        for n in smbclient.listdir(_smb_root(cfg), **_smb_kwargs(cfg))
        if fnmatch.fnmatch(n, cfg.pattern)
    ]


def _smb_fetch(cfg: SmbSource, filename: str, local: str) -> None:
    """Chunked SMB read to disk — streamed, never whole-payload-in-RAM
    (smbprotocol is synchronous; callers run this on a thread)."""
    with smbclient.open_file(
        rf"{_smb_root(cfg)}\{filename}", mode="rb", **_smb_kwargs(cfg)
    ) as remote, open(local, "wb") as out:
        while chunk := remote.read(_LAND_CHUNK):
            out.write(chunk)


@activity.defn
async def discover_files(cfg: IngestConfig) -> list:
    """List remote files matching the source's pattern. Metadata only.
    Dispatches on the configured source (SMB when set, else SFTP)."""
    if cfg.smb:
        return sorted(await asyncio.to_thread(_smb_list, cfg.smb))
    async with _sftp_conn(cfg.sftp) as conn:
        async with conn.start_sftp_client() as sftp:
            names = await sftp.listdir(cfg.sftp.path)
    return sorted(n for n in names if fnmatch.fnmatch(n, cfg.sftp.pattern))


@activity.defn
async def land_file(cfg: IngestConfig, filename: str) -> str:
    """Stream one remote file into the S3 landing zone (disk-buffered, not
    memory). Returns the S3 key. This is the only byte-moving activity."""
    s3 = _s3()
    try:
        await asyncio.to_thread(s3.create_bucket, Bucket=cfg.bucket)
    except s3.exceptions.ClientError:
        pass
    with tempfile.TemporaryDirectory() as tmp:
        local = os.path.join(tmp, filename)
        if cfg.smb:
            # Blocking protocol client; never run it on the event loop.
            await asyncio.to_thread(_smb_fetch, cfg.smb, filename, local)
        else:
            async with _sftp_conn(cfg.sftp) as conn:
                async with conn.start_sftp_client() as sftp:
                    await sftp.get(f"{cfg.sftp.path}/{filename}", local)
        activity.heartbeat(f"downloaded {filename}")
        key = f"{cfg.landing_prefix}/{filename}"
        # boto3 is blocking; never call it directly on the event loop.
        await asyncio.to_thread(s3.upload_file, local, cfg.bucket, key)
    return key



# Byte-shaped preprocess (policy-compliant: bounded, streamed, profiled).
# Extension-driven: .gz is mechanical; steps needing per-vendor knowledge
# (e.g. PGP decrypt with a vendor key) would come from the route spec.
PREPROCESS_MAX_DECOMPRESSED = 1 * 1024**3  # gzip-bomb guard
_PREPROCESS_CHUNK = 8 * 1024**2


def decompressed_name(filename: str) -> str:
    """The name a file will carry after preprocess (pure; used for routing)."""
    return filename[:-3] if filename.endswith(".gz") else filename


@activity.defn
def preprocess_file(cfg: IngestConfig, landed_key: str) -> str:
    """Streaming gunzip: landing/x.csv.gz -> landing/x.csv. Disk-buffered
    chunks, hard cap on decompressed size, deterministic failures are
    non-retryable (the caller quarantines the original object)."""
    s3 = _s3()
    filename = os.path.basename(landed_key)
    target_key = f"{cfg.landing_prefix}/{decompressed_name(filename)}"
    body = s3.get_object(Bucket=cfg.bucket, Key=landed_key)["Body"]
    total = 0
    try:
        with tempfile.NamedTemporaryFile() as out:
            with gzip.GzipFile(fileobj=body) as stream:
                while chunk := stream.read(_PREPROCESS_CHUNK):
                    total += len(chunk)
                    if total > PREPROCESS_MAX_DECOMPRESSED:
                        raise ApplicationError(
                            f"{filename}: decompressed size exceeds "
                            f"{PREPROCESS_MAX_DECOMPRESSED} bytes",
                            non_retryable=True,
                        )
                    out.write(chunk)
                    activity.heartbeat(f"decompressed {total} bytes")
            out.flush()
            s3.upload_file(out.name, cfg.bucket, target_key)
    except (gzip.BadGzipFile, EOFError) as exc:
        raise ApplicationError(
            f"{filename}: not valid gzip ({exc})", non_retryable=True
        )
    return target_key

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
    # Landing gate = the canonical model's columns (stg_<table> by
    # convention, or the spec's explicit "model").
    staging_model = spec.get("model") or "stg_" + spec["source_table"].split(".")[-1]
    inputs = [
        {
            "key": landed_key,
            "table": spec["source_table"],
            "format": "csv",
            "hygiene": {
                "sanitize_headers": True,
                "column_aliases": spec.get("column_aliases", {}),
                "require_columns": canonical_columns(staging_model),
            },
        }
    ]
    return _job_kwargs(cfg, spec, inputs)


@activity.defn
def resolve_consolidation_spec(cfg: IngestConfig, spec_name: str) -> dict:
    """A named spec with no landed input: its dbt models read the tables the
    per-file transform jobs left in the persistent catalog."""
    return _job_kwargs(cfg, _load_spec(spec_name), inputs=[])