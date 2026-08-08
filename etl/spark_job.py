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
               "warehouse": "s3://bucket/warehouse"}
}

Without `catalog`, table metadata lives only for this job run (each run is
self-contained). With it, dbt models materialize as Iceberg tables in a
persistent catalog that other engines (and later runs) can query by name.

Prints a final `ETL_RESULT {json}` line for the calling process to parse.
"""
import argparse
import glob
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


def configure_catalog(builder, catalog: dict | None):
    """No catalog -> Spark's ephemeral in-memory catalog (the default).
    With one -> Iceberg tables in a persistent external catalog."""
    if not catalog:
        return builder
    name = catalog.get("name", "lake")
    builder = (
        builder.config("spark.jars.packages", catalog.get("packages", ICEBERG_PACKAGES))
        .config(
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


def load_inputs(spec: dict, spark, work: str, s3) -> None:
    for inp in spec.get("inputs", []):
        bucket, key = parse_s3(inp["source"])
        local = os.path.join(work, "inputs", key.replace("/", "_"))
        os.makedirs(os.path.dirname(local), exist_ok=True)
        s3.download_file(bucket, key, local)
        fmt = inp.get("format", "csv")
        if fmt == "csv":
            df = spark.read.option("header", True).csv(local)
        else:
            df = spark.read.format(fmt).load(local)
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
        bucket, key = parse_s3(out["destination"])
        if fmt == "json":
            local = os.path.join(work, f"output_{i}.json")
            with open(local, "w") as f:
                json.dump([r.asDict() for r in df.collect()], f, default=str)
            s3.upload_file(local, bucket, key)
        elif fmt == "parquet":
            tmp = os.path.join(work, f"output_{i}_parquet")
            df.coalesce(1).write.mode("overwrite").parquet(tmp)
            part = glob.glob(os.path.join(tmp, "part-*.parquet"))[0]
            s3.upload_file(part, bucket, key)
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

    from pyspark.sql import SparkSession

    if not spec.get("catalog"):
        # Stateless mode: the local warehouse is scratch space for this run;
        # stale table locations from prior runs would collide with saveAsTable.
        shutil.rmtree(os.path.join(work, "warehouse"), ignore_errors=True)

    builder = (
        SparkSession.builder.appName(spec.get("name", "dbt-spark-job"))
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.sql.warehouse.dir", os.path.join(work, "warehouse"))
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={work}")
        .config("spark.ui.enabled", "false")
    )
    spark = configure_catalog(builder, spec.get("catalog")).getOrCreate()

    # dbt models follow the catalog choice (dbt_project.yml reads this).
    os.environ["DBT_FILE_FORMAT"] = "iceberg" if spec.get("catalog") else "parquet"

    load_inputs(spec, spark, work, s3)

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
