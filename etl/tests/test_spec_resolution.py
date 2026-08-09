"""Routing + spec resolution: registry patterns, canonical-model derivation,
alias plumbing — the config machinery that turns a landed file into a job."""
from ingest.activities import (
    IngestConfig,
    canonical_columns,
    classify_route,
    resolve_consolidation_spec,
    resolve_transform_spec,
)


def test_classify_route_matches_registry_patterns():
    assert classify_route("orders_2026-08.csv") == "orders"
    assert classify_route("customers_2026-09.csv") == "customers"
    assert classify_route("payments_x.csv") == "payments"
    assert classify_route("zz_broken.csv") is None
    assert classify_route("orders.txt") is None


def test_canonical_columns_come_from_the_model():
    assert canonical_columns("stg_orders") == [
        "order_id",
        "customer",
        "order_date",
        "amount",
        "status",
    ]
    assert "segment" in canonical_columns("stg_customers")


def test_resolve_binds_file_into_spec_with_derived_gate():
    cfg = IngestConfig(spark_remote="sc://example:15002")
    job = resolve_transform_spec(cfg, "customers", "landing/customers_x.csv")

    assert job["seed_demo_data"] is False
    assert job["spark_remote"] == "sc://example:15002"
    (inp,) = job["inputs"]
    assert inp["key"] == "landing/customers_x.csv"
    assert inp["table"] == "raw.customers"
    hygiene = inp["hygiene"]
    assert hygiene["sanitize_headers"] is True
    # Aliases come from the vendor spec; the gate derives from schema.yml.
    assert "seg" in hygiene["column_aliases"]["segment"]
    assert hygiene["require_columns"] == canonical_columns("stg_customers")


def test_consolidation_spec_is_tables_only():
    job = resolve_consolidation_spec(IngestConfig(), "consolidation")
    assert job["inputs"] == []
    assert job["dbt_args"] == ["build", "--select", "tag:consolidated"]
