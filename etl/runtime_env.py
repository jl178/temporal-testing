"""Shared runtime constants + env-driven options (DRY across entrypoints)."""
import os

# On AWS the bucket is stack-created (CDK injects ETL_BUCKET); the literal
# default is the local emulator convention.
DEFAULT_BUCKET = os.environ.get("ETL_BUCKET", "etl-data")


def catalog_from_env() -> dict | None:
    # Glue (AWS): the warehouse URI is the only required knob — table
    # metadata lives in the account's Glue Data Catalog.
    if os.environ.get("GLUE_WAREHOUSE"):
        return {
            "type": "glue",
            "name": "lake",
            "warehouse": os.environ["GLUE_WAREHOUSE"],
        }
    if os.environ.get("ICEBERG_REST_URI"):
        return {
            "type": "rest",
            "name": "lake",
            "uri": os.environ["ICEBERG_REST_URI"],
            "warehouse": f"s3://{DEFAULT_BUCKET}/warehouse",
        }
    return None


def spark_remote_from_env() -> str | None:
    # Empty string = explicit opt-out (in-process Spark fallback).
    return os.environ.get("SPARK_CONNECT_URI") or None
