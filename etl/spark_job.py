"""Batch ETL job: S3 raw CSV -> Spark table -> dbt build -> mart back to S3.

This is the entry point an EMR Serverless spark-submit would run. Locally it
executes against a local[*] SparkSession with dbt-spark's `session` method
attaching to the same session, so the transformation is identical either way.

Prints a final `ETL_RESULT {json}` line for the calling process to parse.
"""
import argparse
import json
import os
import sys

import boto3


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--raw-key", required=True)
    parser.add_argument("--output-key", required=True)
    parser.add_argument("--work-dir", required=True)
    args = parser.parse_args()

    here = os.path.dirname(os.path.abspath(__file__))
    work = os.path.abspath(args.work_dir)
    os.makedirs(work, exist_ok=True)

    s3 = boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL") or None)
    raw_path = os.path.join(work, "orders.csv")
    s3.download_file(args.bucket, args.raw_key, raw_path)

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("etl-daily-revenue")
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.sql.warehouse.dir", os.path.join(work, "warehouse"))
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={work}")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    spark.sql("create database if not exists raw")
    raw_df = spark.read.option("header", True).csv(raw_path)
    raw_df.write.mode("overwrite").saveAsTable("raw.orders")

    # dbt-spark `session` method reuses this process's active SparkSession.
    from dbt.cli.main import dbtRunner

    dbt_dir = os.path.join(here, "dbt")
    result = dbtRunner().invoke(
        ["build", "--project-dir", dbt_dir, "--profiles-dir", dbt_dir]
    )
    if not result.success:
        print("ETL_RESULT " + json.dumps({"success": False, "error": "dbt build failed"}))
        sys.exit(1)

    mart = spark.table("analytics.daily_revenue")
    rows = [r.asDict() for r in mart.collect()]
    out_path = os.path.join(work, "daily_revenue.json")
    with open(out_path, "w") as f:
        json.dump(rows, f, default=str)
    s3.upload_file(out_path, args.bucket, args.output_key)

    print(
        "ETL_RESULT "
        + json.dumps(
            {
                "success": True,
                "raw_rows": raw_df.count(),
                "mart_rows": len(rows),
                "total_revenue": sum(float(r["total_revenue"]) for r in rows),
                "output": f"s3://{args.bucket}/{args.output_key}",
            }
        )
    )
    spark.stop()


if __name__ == "__main__":
    main()
