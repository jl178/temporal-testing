"""Activities for the file-ingestion pipeline.

SFTP -> s3://<bucket>/landing/ -> rules-based parse -> s3://<bucket>/staged/
-> classify (routing field) -> resolve a transform spec from the registry.
The resolved spec becomes a DbtSparkJob executed as a child workflow
(EtlPipelineWorkflow) which writes curated output back to S3.
"""
import fnmatch
import json
import os
import tempfile
from dataclasses import dataclass, field

import asyncssh
import boto3
import pandas as pd
from temporalio import activity

HERE = os.path.dirname(os.path.abspath(__file__))
SPECS_DIR = os.path.join(os.path.dirname(HERE), "specs")

# Demo parse rules: normalize the messy vendor headers into the schema the
# dbt project expects. Each rule is applied in order by apply_rules().
DEFAULT_PARSE_RULES = [
    {"op": "lowercase_headers"},
    {"op": "rename", "mapping": {"order id": "order_id", "order date": "order_date"}},
    {"op": "cast", "column": "amount", "type": "float"},
    {"op": "strip", "column": "status"},
]


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
    staged_prefix: str = "staged"
    parse_rules: list = field(default_factory=lambda: list(DEFAULT_PARSE_RULES))
    # Column whose value selects the downstream transform spec.
    routing_field: str = "record_type"
    # Passed through to the spawned DbtSparkJob (None = ephemeral catalog).
    catalog: dict | None = None


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
    """List remote files matching the source pattern."""
    async with _sftp_conn(cfg.sftp) as conn:
        async with conn.start_sftp_client() as sftp:
            names = await sftp.listdir(cfg.sftp.path)
    return sorted(n for n in names if fnmatch.fnmatch(n, cfg.sftp.pattern))


@activity.defn
async def land_sftp_file(cfg: IngestConfig, filename: str) -> str:
    """Copy one remote file into the S3 landing zone. Returns the S3 key."""
    s3 = _s3()
    try:
        s3.create_bucket(Bucket=cfg.bucket)
    except s3.exceptions.ClientError:
        pass
    with tempfile.TemporaryDirectory() as tmp:
        local = os.path.join(tmp, filename)
        async with _sftp_conn(cfg.sftp) as conn:
            async with conn.start_sftp_client() as sftp:
                await sftp.get(f"{cfg.sftp.path}/{filename}", local)
        activity.heartbeat(f"downloaded {filename}")
        key = f"{cfg.landing_prefix}/{filename}"
        s3.upload_file(local, cfg.bucket, key)
    return key


def apply_rules(df: pd.DataFrame, rules: list, context: dict) -> pd.DataFrame:
    """Tiny declarative parse engine. `{{ name }}` macros in constant values
    are substituted from context (file_name, landed_key)."""

    def render(value: str) -> str:
        for k, v in context.items():
            value = value.replace("{{ " + k + " }}", str(v))
        return value

    for rule in rules:
        op = rule["op"]
        if op == "lowercase_headers":
            df.columns = [c.strip().lower() for c in df.columns]
        elif op == "rename":
            df = df.rename(columns=rule["mapping"])
        elif op == "cast":
            df[rule["column"]] = df[rule["column"]].astype(rule["type"])
        elif op == "strip":
            df[rule["column"]] = df[rule["column"]].astype(str).str.strip()
        elif op == "drop":
            df = df.drop(columns=rule["columns"])
        elif op == "constant":
            df[rule["column"]] = render(str(rule["value"]))
        else:
            raise ValueError(f"unknown parse rule op: {op}")
    return df


@activity.defn
async def parse_file(cfg: IngestConfig, landed_key: str) -> dict:
    """Generic parse: apply the config's rules to a landed CSV and write
    normalized parquet to the staged zone."""
    s3 = _s3()
    filename = os.path.basename(landed_key)
    with tempfile.TemporaryDirectory() as tmp:
        local = os.path.join(tmp, filename)
        s3.download_file(cfg.bucket, landed_key, local)
        df = pd.read_csv(local)
        df = apply_rules(df, cfg.parse_rules, {"file_name": filename, "landed_key": landed_key})
        staged_key = f"{cfg.staged_prefix}/{os.path.splitext(filename)[0]}.parquet"
        out = os.path.join(tmp, "staged.parquet")
        df.to_parquet(out, index=False)
        s3.upload_file(out, cfg.bucket, staged_key)
    return {"staged_key": staged_key, "rows": len(df), "columns": list(df.columns)}


@activity.defn
async def classify_file(cfg: IngestConfig, staged_key: str) -> str:
    """Read the routing field from the staged data to pick a transform route."""
    s3 = _s3()
    with tempfile.TemporaryDirectory() as tmp:
        local = os.path.join(tmp, "staged.parquet")
        s3.download_file(cfg.bucket, staged_key, local)
        df = pd.read_parquet(local, columns=[cfg.routing_field])
    values = df[cfg.routing_field].dropna().unique()
    if len(values) != 1:
        raise ValueError(f"expected one {cfg.routing_field!r} value, got {list(values)}")
    return str(values[0])


@activity.defn
async def resolve_transform_spec(cfg: IngestConfig, route: str, staged_key: str) -> dict:
    """Registry lookup: route value -> transform spec file -> DbtSparkJob
    kwargs with the staged file wired in as the input."""
    with open(os.path.join(SPECS_DIR, "registry.json")) as f:
        registry = json.load(f)
    if route not in registry:
        raise ValueError(f"no transform spec registered for route {route!r}")
    with open(os.path.join(SPECS_DIR, f"{registry[route]}.json")) as f:
        spec = json.load(f)

    return {
        "bucket": cfg.bucket,
        "name": spec["name"],
        "project_dir": spec.get("project_dir", "dbt"),
        "dbt_args": spec.get("dbt_args", ["build"]),
        "dbt_vars": spec.get("dbt_vars", {}),
        "inputs": [
            {
                "key": staged_key,
                "table": spec["source_table"],
                "format": spec.get("source_format", "parquet"),
            }
        ],
        "outputs": spec["outputs"],
        "catalog": cfg.catalog,
        "seed_demo_data": False,
    }
