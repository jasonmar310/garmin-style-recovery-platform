"""
Unit tests for the pure routing/key logic in ingest/router.py.

Scope discipline: we test ONLY the metadata->routing derivation and the cold-object
key construction — the two places a bug silently MISROUTES data (wrong table) or
breaks idempotency (duplicate cold files on replay). The sink wiring itself —
flush_hot's SQL execution, main()'s Kafka/Postgres/MinIO connections — needs live
infra and is covered by chaos/ + `make verify`.

  - build_routing()  : topic -> {table, columns, cold_prefix}
  - flush_cold() key : event-time partitioning + content-hash naming (idempotency)
"""
import hashlib

import pytest

import router


META = {
    "streams": [
        {
            "name": "heart_rate",
            "kafka_topic": "telemetry.heart_rate",
            "signals": [{"field": "bpm"}],
            "sink": {"hot": "timescale.hr_readings",
                     "cold": "s3://telemetry/raw/heart_rate/"},
        },
        {
            "name": "hrv",
            "kafka_topic": "telemetry.hrv",
            "signals": [{"field": "rmssd_ms"}, {"field": "respiratory_rate"}],
            "sink": {"hot": "timescale.hrv_readings",
                     "cold": "s3://telemetry/raw/hrv/"},
        },
    ]
}


# --- build_routing() --------------------------------------------------------

def test_build_routing_is_keyed_by_kafka_topic():
    routing = router.build_routing(META)
    assert set(routing) == {"telemetry.heart_rate", "telemetry.hrv"}


def test_build_routing_strips_schema_prefix_from_table():
    routing = router.build_routing(META)
    assert routing["telemetry.heart_rate"]["table"] == "hr_readings"
    assert routing["telemetry.hrv"]["table"] == "hrv_readings"


def test_build_routing_columns_are_base_plus_signal_fields_in_order():
    routing = router.build_routing(META)
    assert routing["telemetry.heart_rate"]["columns"] == [
        "event_id", "device_id", "device_type", "ts", "bpm"]
    # multi-signal stream keeps signal order after the base columns
    assert routing["telemetry.hrv"]["columns"] == [
        "event_id", "device_id", "device_type", "ts", "rmssd_ms", "respiratory_rate"]


def test_build_routing_cold_prefix_drops_bucket_and_trailing_slash():
    routing = router.build_routing(META)
    # s3://telemetry/raw/heart_rate/  ->  raw/heart_rate  (bucket + trailing / gone)
    assert routing["telemetry.heart_rate"]["cold_prefix"] == "raw/heart_rate"


def test_build_routing_cold_prefix_handles_deep_path_without_trailing_slash():
    meta = {"streams": [{
        "name": "x", "kafka_topic": "t.x", "signals": [{"field": "v"}],
        "sink": {"hot": "timescale.x", "cold": "s3://telemetry/raw/sub/x"},
    }]}
    assert router.build_routing(meta)["t.x"]["cold_prefix"] == "raw/sub/x"


# --- flush_cold() key construction (idempotency) ----------------------------

class _FakeS3:
    """Captures put_object kwargs instead of hitting MinIO."""
    def __init__(self):
        self.calls = []

    def put_object(self, **kwargs):
        self.calls.append(kwargs)


@pytest.fixture
def no_parquet(monkeypatch):
    # The key logic is independent of the parquet payload; stub the encoder so the
    # test needs neither pyarrow nor a real object body.
    monkeypatch.setattr(router, "parquet_bytes", lambda rows: b"")


def _expected_digest(event_ids):
    return hashlib.sha1("".join(event_ids).encode()).hexdigest()[:16]


def test_flush_cold_partitions_by_event_time_and_hashes_event_ids(no_parquet):
    s3 = _FakeS3()
    route = {"cold_prefix": "raw/heart_rate"}
    rows = [{"event_id": "a", "ts": "2026-01-15T09:30:00+00:00"},
            {"event_id": "b", "ts": "2026-01-15T11:00:00+00:00"}]

    router.flush_cold(s3, route, rows)

    assert len(s3.calls) == 1
    call = s3.calls[0]
    assert call["Bucket"] == router.BUCKET
    digest = _expected_digest(["a", "b"])
    # partitioned by the FIRST row's event time (dt + hr), not wall clock
    assert call["Key"] == f"raw/heart_rate/dt=2026-01-15/hr=09/{digest}.parquet"


def test_flush_cold_is_idempotent_for_same_event_ids(no_parquet):
    route = {"cold_prefix": "raw/heart_rate"}
    rows = [{"event_id": "a", "ts": "2026-01-15T09:30:00+00:00"},
            {"event_id": "b", "ts": "2026-01-15T09:31:00+00:00"}]

    s3a, s3b = _FakeS3(), _FakeS3()
    router.flush_cold(s3a, route, rows)
    router.flush_cold(s3b, route, list(rows))  # replay of the same offsets
    # same content -> same key -> overwrite, not a duplicate file
    assert s3a.calls[0]["Key"] == s3b.calls[0]["Key"]


def test_flush_cold_key_changes_when_event_ids_differ(no_parquet):
    route = {"cold_prefix": "raw/heart_rate"}
    base_ts = "2026-01-15T09:30:00+00:00"

    s3a, s3b = _FakeS3(), _FakeS3()
    router.flush_cold(s3a, route, [{"event_id": "a", "ts": base_ts}])
    router.flush_cold(s3b, route, [{"event_id": "z", "ts": base_ts}])
    assert s3a.calls[0]["Key"] != s3b.calls[0]["Key"]
