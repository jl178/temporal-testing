"""The byte-shaped preprocess stage: routing names and streaming gunzip."""
import gzip
import io

import pytest
from temporalio.exceptions import ApplicationError

from ingest.activities import (
    IngestConfig,
    PREPROCESS_MAX_DECOMPRESSED,
    decompressed_name,
    preprocess_file,
)


def test_decompressed_name_strips_only_gz():
    assert decompressed_name("payments_2026-08.csv.gz") == "payments_2026-08.csv"
    assert decompressed_name("orders.csv") == "orders.csv"
    assert decompressed_name("archive.tar.gz") == "archive.tar"


def test_routing_uses_the_decompressed_name():
    # The workflow classifies on the post-preprocess name — a gzipped drop
    # must land on the same route as its plain twin.
    from ingest.activities import classify_route

    assert classify_route(decompressed_name("payments_x.csv.gz")) == "payments"


class _FakeBody(io.BytesIO):
    """boto3 StreamingBody stand-in."""


class _FakeS3:
    def __init__(self, payload: bytes):
        self.payload = payload
        self.uploaded: tuple | None = None

    def get_object(self, Bucket, Key):
        return {"Body": _FakeBody(self.payload)}

    def upload_file(self, path, bucket, key):
        with open(path, "rb") as f:
            self.uploaded = (key, f.read())


def test_preprocess_streams_gunzip_to_the_plain_key(monkeypatch):
    import ingest.activities as acts

    fake = _FakeS3(gzip.compress(b"a,b\n1,2\n"))
    monkeypatch.setattr(acts, "_s3", lambda: fake)
    monkeypatch.setattr(acts.activity, "heartbeat", lambda *_: None)

    key = preprocess_file(IngestConfig(), "landing/orders_x.csv.gz")
    assert key == "landing/orders_x.csv"
    assert fake.uploaded == ("landing/orders_x.csv", b"a,b\n1,2\n")


def test_preprocess_rejects_bad_gzip_non_retryably(monkeypatch):
    import ingest.activities as acts

    fake = _FakeS3(b"not gzip at all")
    monkeypatch.setattr(acts, "_s3", lambda: fake)
    monkeypatch.setattr(acts.activity, "heartbeat", lambda *_: None)

    with pytest.raises(ApplicationError) as err:
        preprocess_file(IngestConfig(), "landing/zz.csv.gz")
    assert err.value.non_retryable


def test_bomb_guard_is_a_real_cap():
    assert PREPROCESS_MAX_DECOMPRESSED == 1 * 1024**3
