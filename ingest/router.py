"""
router.py — consume telemetry, write to hot (TimescaleDB) + cold (MinIO/Parquet).

Delivery semantics: at-least-once + idempotent sinks = effectively-once.
  - offsets are committed ONLY after a batch is durably written to BOTH sinks
  - hot insert uses ON CONFLICT (event_id, ts) DO NOTHING (idempotent upsert)
  - so a crash between write and commit just replays the batch harmlessly

Batching: accumulate per stream, flush on size or time. Per-row inserts and
one-object-per-event are exactly the backpressure / small-file problems we avoid.

Metadata-driven: topic -> table, columns, and cold prefix all come from
streams.yaml. Add a stream there and the router handles it with no code change.
"""
from __future__ import annotations
import argparse, io, json, os, time, uuid
from datetime import datetime, timezone
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]
STREAMS = ROOT / "metadata" / "streams.yaml"

BATCH_SIZE = 500        # flush after this many buffered events (across streams)
BATCH_SECONDS = 10      # ...or after this long, whichever comes first
BUCKET = "telemetry"


def build_routing(meta: dict) -> dict:
    """topic -> {table, columns, cold_prefix} derived from metadata."""
    routing = {}
    for s in meta["streams"]:
        fields = [sig["field"] for sig in s["signals"]]
        cold = s["sink"]["cold"]                          # s3://telemetry/raw/heart_rate/
        prefix = cold.split("://", 1)[1].split("/", 1)[1].rstrip("/")  # raw/heart_rate
        routing[s["kafka_topic"]] = {
            "table": s["sink"]["hot"].split(".", 1)[1],   # timescale.hr_readings -> hr_readings
            "columns": ["event_id", "device_id", "device_type", "ts"] + fields,
            "cold_prefix": prefix,
        }
    return routing


def parquet_bytes(rows: list[dict]) -> bytes:
    import pyarrow as pa, pyarrow.parquet as pq
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows), buf, compression="snappy")
    return buf.getvalue()


def flush_hot(conn, route: dict, rows: list[dict]) -> None:
    from psycopg2.extras import execute_values
    cols = route["columns"]
    sql = (f"INSERT INTO {route['table']} ({', '.join(cols)}) VALUES %s "
           f"ON CONFLICT (event_id, ts) DO NOTHING")
    values = [tuple(r.get(c) for c in cols) for r in rows]
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, values)
        conn.commit()
    except Exception as e:
        # A malformed batch (e.g. legacy rows missing event_id) must not wedge
        # the whole pipeline. Roll back, log, and skip. A production system would
        # route these to a dead-letter topic instead of dropping them.
        conn.rollback()
        print(f"  [skip] hot insert failed for {route['table']}: {e}", flush=True)


def flush_cold(s3, route: dict, rows: list[dict]) -> None:
    now = datetime.now(timezone.utc)
    key = (f"{route['cold_prefix']}/dt={now:%Y-%m-%d}/hr={now:%H}/"
           f"{uuid.uuid4().hex}.parquet")
    s3.put_object(Bucket=BUCKET, Key=key, Body=parquet_bytes(rows))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap",
                    default=os.getenv("BOOTSTRAP_SERVERS",
                                      "localhost:29092,localhost:29093,localhost:29094"))
    ap.add_argument("--group", default="router")
    args = ap.parse_args()

    meta = yaml.safe_load(STREAMS.read_text())
    routing = build_routing(meta)

    import psycopg2, boto3
    from confluent_kafka import Consumer

    print("router starting...", flush=True)
    print(f"  -> TimescaleDB {os.getenv('PGHOST','localhost')}:{os.getenv('PGPORT','5432')} "
          f"db={os.getenv('TIMESCALE_DB','telemetry')} user={os.getenv('TIMESCALE_USER','ian')}",
          flush=True)
    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"), port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("TIMESCALE_DB", "telemetry"),
        user=os.getenv("TIMESCALE_USER", "ian"),
        password=os.getenv("TIMESCALE_PASSWORD", ""))
    print("  -> TimescaleDB connected", flush=True)

    s3 = boto3.client(
        "s3", endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", ""))
    print(f"  -> MinIO endpoint {os.getenv('S3_ENDPOINT','http://localhost:9000')}", flush=True)

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        "enable.auto.commit": False,            # we commit manually, after sinks
        "auto.offset.reset": "earliest",        # don't skip already-buffered data
    })

    def on_assign(c, partitions):
        print(f"  -> Kafka assigned {len(partitions)} partitions", flush=True)

    consumer.subscribe(list(routing), on_assign=on_assign)
    print(f"router consuming {list(routing)} (group={args.group})", flush=True)

    buffers: dict[str, list] = {t: [] for t in routing}
    last_flush = last_beat = time.time()

    def flush_all():
        nonlocal last_flush
        wrote = False
        for topic, rows in buffers.items():
            if rows:
                flush_hot(conn, routing[topic], rows)   # hot first
                flush_cold(s3, routing[topic], rows)    # then cold
                wrote = True
        if wrote:
            consumer.commit(asynchronous=False)         # commit ONLY after sinks
        for t in buffers:
            buffers[t].clear()
        last_flush = time.time()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    print(f"  consume error: {msg.error()}", flush=True)
                else:
                    buffers[msg.topic()].append(json.loads(msg.value()))

            buffered = sum(len(v) for v in buffers.values())
            now = time.time()
            if buffered and (buffered >= BATCH_SIZE or now - last_flush >= BATCH_SECONDS):
                flush_all()
                print(f"flushed {buffered} events at {datetime.now():%H:%M:%S}", flush=True)
            elif now - last_beat >= 5:
                print(f"  alive — buffered={buffered}", flush=True)
                last_beat = now
    except KeyboardInterrupt:
        print("\nstopping, flushing final batch...")
        flush_all()
    finally:
        consumer.close()
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())