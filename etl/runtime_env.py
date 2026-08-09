"""Shared env-driven runtime options for starters (DRY across entrypoints)."""
import os


def catalog_from_env() -> dict | None:
    if os.environ.get("ICEBERG_REST_URI"):
        return {
            "type": "rest",
            "name": "lake",
            "uri": os.environ["ICEBERG_REST_URI"],
            "warehouse": "s3://etl-data/warehouse",
        }
    return None


def spark_remote_from_env() -> str | None:
    return os.environ.get("SPARK_CONNECT_URI")
