"""Shared runtime constants + env-driven options (DRY across entrypoints)."""
import os

DEFAULT_BUCKET = "etl-data"


def catalog_from_env() -> dict | None:
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
