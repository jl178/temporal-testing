"""Generic dbt-on-Spark job runner.

Runs ANY dbt-spark workload described by a JSON job spec — this is the entry
point an EMR Serverless spark-submit executes, and the same script the
Temporal pipeline runs locally (dbt-spark's `session` method attaches to this
process's SparkSession, so behavior is identical either way).

Job spec:
{
  "name": "daily-revenue",
  "project": "s3://bucket/jobs/x/dbt-project.tar.gz" | "/local/path/to/dbt/project",
  "dbt_args": ["build"],                      # any dbt CLI verb + flags
  "dbt_vars": {"key": "value"},               # passed as --vars
  "inputs":  [{"source": "s3://bucket/raw/orders.csv",
               "table": "raw.orders", "format": "csv"}],
  "outputs": [{"table": "analytics.daily_revenue",
               "destination": "s3://bucket/marts/daily_revenue.json",
               "format": "json"}],             # json | parquet
  "catalog": {                                 # OPTIONAL — omit for an
               "type": "rest",                 #   ephemeral in-memory catalog
               "name": "lake",                 # "rest" (Iceberg REST, local)
               "uri": "http://localhost:8181", #   or "glue" (real AWS)
               "warehouse": "s3://bucket/warehouse"},
  "spark_remote": "sc://localhost:15002"       # OPTIONAL — execute on a remote
}                                              #   cluster via Spark Connect

Without `catalog`, table metadata lives only for this job run (each run is
self-contained). With it, dbt models materialize as Iceberg tables in a
persistent catalog that other engines (and later runs) can query by name.

Execution modes:
  - default: in-process Spark (local[*] here; the cluster when this script is
    the EMR spark-submit entry point)
  - spark_remote: this process is a Spark Connect CLIENT — dbt compiles SQL
    here, the remote cluster executes it. Locally that's a spark-connect
    container; on AWS it's an EMR Serverless interactive session endpoint
    (emr-7.13+), per https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/tutorials-dbt.html
    The catalog and S3 connectivity are configured server-side in this mode
    (on the EMR application; the local compose file for the dev server).

Data plane: the CLUSTER moves all data — inputs are read from object storage
by executors in parallel, parquet outputs are written back the same way.
Nothing large ever transits this process; json outputs are an explicitly
capped small-results path so marts can be asserted inline.

Prints a final `ETL_RESULT {json}` line for the calling process to parse.
"""
import argparse
import json
import os
import shutil
import sys
import tarfile

import boto3


def s3_client():
    return boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL") or None)


def parse_s3(uri: str) -> tuple[str, str]:
    bucket, _, key = uri[len("s3://"):].partition("/")
    return bucket, key


def resolve_project(project: str, work: str, s3) -> str:
    """Return a local dbt project dir, downloading/extracting from S3 if needed."""
    if not project.startswith("s3://"):
        if os.path.isabs(project):
            return project
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), project)
    bucket, key = parse_s3(project)
    tar_path = os.path.join(work, "dbt-project.tar.gz")
    s3.download_file(bucket, key, tar_path)
    project_dir = os.path.join(work, "dbt-project")
    shutil.rmtree(project_dir, ignore_errors=True)
    with tarfile.open(tar_path) as tar:
        tar.extractall(project_dir, filter="data")
    if not os.path.exists(os.path.join(project_dir, "dbt_project.yml")):
        entries = os.listdir(project_dir)
        if len(entries) == 1:
            project_dir = os.path.join(project_dir, entries[0])
    return project_dir


ICEBERG_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,"
    "org.apache.iceberg:iceberg-aws-bundle:1.11.0"
)
# Matches the hadoop-client jars shipped with pyspark 4.1 / spark:4.1.1.
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.4.2"

# collect()-to-JSON is intentionally a small-results path (assertable marts);
# big outputs must use parquet, which the cluster writes straight to S3.
MAX_INLINE_ROWS = 100_000


def data_uri(uri: str) -> str:
    """The cluster reads/writes object storage directly. Against an emulator
    (AWS_ENDPOINT_URL set) that means the s3a:// connector; on AWS the
    native s3:// committer."""
    if os.environ.get("AWS_ENDPOINT_URL") and uri.startswith("s3://"):
        return "s3a://" + uri[len("s3://"):]
    return uri


def configure_s3a(builder):
    """S3A connector for local/emulator runs. On AWS (no AWS_ENDPOINT_URL)
    this is a no-op — EMR provides native S3 access via the execution role."""
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if not endpoint:
        return builder
    return (
        builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", "test"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", "test"))
        .config("spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )


def configure_catalog(builder, catalog: dict | None):
    """No catalog -> Spark's ephemeral in-memory catalog (the default).
    With one -> Iceberg tables in a persistent external catalog.
    (Jar packages are assembled by main() so S3A and Iceberg can combine.)"""
    if not catalog:
        return builder
    name = catalog.get("name", "lake")
    builder = (
        builder.config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{name}", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.defaultCatalog", name)
    )
    if catalog.get("warehouse"):
        builder = builder.config(f"spark.sql.catalog.{name}.warehouse", catalog["warehouse"])
    ctype = catalog.get("type", "rest")
    if ctype == "rest":
        builder = builder.config(f"spark.sql.catalog.{name}.type", "rest").config(
            f"spark.sql.catalog.{name}.uri", catalog["uri"]
        )
        # Point Iceberg's S3FileIO at the same emulator the rest of the job uses.
        endpoint = catalog.get("s3_endpoint") or os.environ.get("AWS_ENDPOINT_URL")
        if endpoint:
            builder = (
                builder.config(
                    f"spark.sql.catalog.{name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
                )
                .config(f"spark.sql.catalog.{name}.s3.endpoint", endpoint)
                .config(f"spark.sql.catalog.{name}.s3.path-style-access", "true")
            )
    elif ctype == "glue":
        builder = builder.config(
            f"spark.sql.catalog.{name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"
        )
    else:
        raise ValueError(f"unsupported catalog type: {ctype}")
    return builder


def sanitize_header(name: str) -> str:
    """Mechanical header cleanup so columns are SQL-addressable:
    'Order ID' -> 'order_id'. Semantic renames belong in dbt staging."""
    import re

    return re.sub(r"[^0-9a-zA-Z]+", "_", name.strip().lower()).strip("_")


def load_inputs(spec: dict, spark) -> None:
    """The cluster reads inputs straight from object storage — no data ever
    transits this process (executors read splits in parallel). File hygiene
    happens here too, in Spark: permissive all-strings read, header
    sanitization, and a column contract that fails fast on files that are
    not what they claim to be."""
    for inp in spec.get("inputs", []):
        uri = data_uri(inp["source"])
        fmt = inp.get("format", "csv")
        if fmt == "csv":
            df = (
                spark.read.option("header", True)
                .option("mode", "PERMISSIVE")
                .csv(uri)
            )
        else:
            df = spark.read.format(fmt).load(uri)

        hygiene = inp.get("hygiene") or {}
        if hygiene.get("sanitize_headers"):
            df = df.toDF(*[sanitize_header(c) for c in df.columns])
        required = hygiene.get("require_columns") or []
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(
                f"input {inp['source']} does not satisfy its column contract; "
                f"missing {missing} (found {df.columns})"
            )

        table = inp["table"]
        if "." in table:
            spark.sql(f"create database if not exists {table.split('.')[0]}")
        df.write.mode("overwrite").saveAsTable(table)


def export_outputs(spec: dict, spark, work: str, s3) -> list[dict]:
    results = []
    for i, out in enumerate(spec.get("outputs", [])):
        df = spark.table(out["table"])
        rows = df.count()
        fmt = out.get("format", "json")
        if fmt == "json":
            # Explicitly a small-results path: assertable marts returned
            # inline. Anything bigger must use parquet.
            if rows > MAX_INLINE_ROWS:
                raise ValueError(
                    f"{out['table']} has {rows} rows; json outputs are capped at "
                    f"{MAX_INLINE_ROWS} (use format=parquet for large outputs)"
                )
            bucket, key = parse_s3(out["destination"])
            local = os.path.join(work, f"output_{i}.json")
            with open(local, "w") as f:
                json.dump([r.asDict() for r in df.collect()], f, default=str)
            s3.upload_file(local, bucket, key)
        elif fmt == "parquet":
            # Distributed write: the cluster writes part files directly to
            # the destination prefix — nothing transits this process.
            df.write.mode("overwrite").parquet(data_uri(out["destination"]))
        else:
            raise ValueError(f"unsupported output format: {fmt}")
        results.append({"table": out["table"], "destination": out["destination"], "rows": rows})
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", required=True, help="path or s3:// URI of the job spec JSON")
    parser.add_argument("--work-dir", required=True)
    args = parser.parse_args()

    work = os.path.abspath(args.work_dir)
    os.makedirs(work, exist_ok=True)
    s3 = s3_client()

    if args.spec.startswith("s3://"):
        bucket, key = parse_s3(args.spec)
        spec = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    else:
        with open(args.spec) as f:
            spec = json.load(f)

    project_dir = resolve_project(spec["project"], work, s3)

    remote = bool(spec.get("spark_remote"))
    if remote:
        # Spark Connect client: dbt-spark's `session` method also honors
        # SPARK_REMOTE, so dbt attaches to the same remote session.
        os.environ["SPARK_REMOTE"] = spec["spark_remote"]

    from pyspark.sql import SparkSession

    if remote:
        # Session already exists server-side; S3 connectivity and catalog
        # DEFINITIONS are the server's configuration (EMR: execution role +
        # Glue on the application; locally: the compose file's confs). The
        # job only SELECTS the catalog for its session.
        spark = SparkSession.builder.getOrCreate()
        if spec.get("catalog"):
            spark.conf.set(
                "spark.sql.defaultCatalog", spec["catalog"].get("name", "lake")
            )
    else:
        if not spec.get("catalog"):
            # Stateless mode: the local warehouse is scratch space for this
            # run; stale table locations would collide with saveAsTable.
            shutil.rmtree(os.path.join(work, "warehouse"), ignore_errors=True)
        builder = (
            SparkSession.builder.appName(spec.get("name", "dbt-spark-job"))
            .master(os.environ.get("SPARK_MASTER", "local[2]"))
            .config("spark.sql.warehouse.dir", os.path.join(work, "warehouse"))
            .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={work}")
            .config("spark.ui.enabled", "false")
        )
        packages = []
        if os.environ.get("AWS_ENDPOINT_URL"):
            # Emulator runs need the S3A connector; on AWS the platform
            # provides native S3 access, no extra jars.
            packages.append(HADOOP_AWS_PACKAGE)
            builder = configure_s3a(builder)
        if spec.get("catalog"):
            packages.append(spec["catalog"].get("packages", ICEBERG_PACKAGES))
        if packages:
            builder = builder.config("spark.jars.packages", ",".join(packages))
        spark = configure_catalog(builder, spec.get("catalog")).getOrCreate()

    # dbt models follow the catalog choice (dbt_project.yml reads this).
    os.environ["DBT_FILE_FORMAT"] = "iceberg" if spec.get("catalog") else "parquet"

    try:
        load_inputs(spec, spark)
    except ValueError as exc:
        # Deterministic rejection (e.g. column contract) — not retryable.
        print("ETL_RESULT " + json.dumps({"success": False, "error": str(exc)}))
        sys.exit(1)

    # dbt-spark `session` method reuses this process's active SparkSession.
    from dbt.cli.main import dbtRunner

    dbt_cli = list(spec.get("dbt_args", ["build"]))
    dbt_cli += ["--project-dir", project_dir, "--profiles-dir", project_dir]
    if spec.get("dbt_vars"):
        dbt_cli += ["--vars", json.dumps(spec["dbt_vars"])]
    result = dbtRunner().invoke(dbt_cli)
    if not result.success:
        print("ETL_RESULT " + json.dumps({"success": False, "error": "dbt invocation failed"}))
        sys.exit(1)

    outputs = export_outputs(spec, spark, work, s3)
    print("ETL_RESULT " + json.dumps({"success": True, "outputs": outputs}))
    spark.stop()


if __name__ == "__main__":
    main()
