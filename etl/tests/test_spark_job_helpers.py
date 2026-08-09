"""Pure helpers of the generic runner (no Spark needed)."""
import spark_job


def test_sanitize_header_collapses_case_and_punctuation():
    assert spark_job.sanitize_header("Order ID") == "order_id"
    assert spark_job.sanitize_header("  ADDRESS  ") == "address"
    assert spark_job.sanitize_header("Paid-Amount ($)") == "paid_amount"
    assert spark_job.sanitize_header("customer") == "customer"


def test_parse_s3_splits_bucket_and_key():
    assert spark_job.parse_s3("s3://etl-data/landing/x.csv") == (
        "etl-data",
        "landing/x.csv",
    )


def test_data_uri_uses_s3a_only_against_an_emulator(monkeypatch):
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    assert spark_job.data_uri("s3://b/k") == "s3://b/k"
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:4566")
    assert spark_job.data_uri("s3://b/k") == "s3a://b/k"


def test_json_outputs_are_capped():
    assert spark_job.MAX_INLINE_ROWS == 100_000
