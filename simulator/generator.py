"""
generator.py — rate-configurable synthetic telemetry producer.

Reads metadata/streams.yaml (WHAT to emit) and metadata/seed_params.yaml (HOW
the values are distributed, learned from real Whoop data) and produces synthetic
events to each stream's Kafka topic.

Two ideas carry the design:
  1. --rate is the anomaly engine. rate=1 is the realistic baseline; crank it to
     10 to create a throughput surge and watch consumer lag climb.
  2. key = device_id, so every event from one device lands on the same partition
     (ordered per device) while the fleet spreads evenly across partitions.

Serialization is JSON for now — a walking skeleton to get data flowing end to
end. _serialize() is the single swap point for an Avro + Schema Registry encoder
later (schema enforcement / evolution), without touching the rest of the code.

Usage:
  python simulator/generator.py --dry-run                  # print samples, no Kafka
  python simulator/generator.py --devices 200 --rate 1     # baseline load
  python simulator/generator.py --devices 200 --rate 10    # surge (anomaly demo)
"""
from __future__ import annotations
import argparse, json, math, os, random, sys, time, uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path

import numpy as np
import yaml

ROOT = Path(__file__).resolve().parents[1]
STREAMS = ROOT / "metadata" / "streams.yaml"
SEED = ROOT / "metadata" / "seed_params.yaml"

DEVICE_TYPES = ["vivoactive", "forerunner", "fenix", "venu", "instinct"]
# Avg workout sessions per device per day -> a per-second probability for the
# "event" stream. Multiplied by --rate during a surge.
SESSIONS_PER_DEVICE_PER_DAY = 1.5
TICK = 1.0          # scheduler granularity (seconds)
STAT_EVERY = 5.0    # how often to print throughput stats


# --- Value sampling: every draw is anchored in the real Whoop distribution ---
def sample(model: str, p: dict, hour: float) -> float | str:
    if model == "categorical":
        cats = list(p["categories"].keys())
        probs = np.array(list(p["categories"].values()), dtype=float)
        probs = probs / probs.sum()
        return str(np.random.choice(cats, p=probs))

    mean, std = p.get("mean", 0.0), max(p.get("std", 1.0), 1e-6)
    lo, hi = p.get("min", mean - 4 * std), p.get("max", mean + 4 * std)

    if model == "gamma":
        # method of moments: shape k, scale theta from mean & std
        k = (mean / std) ** 2
        theta = std ** 2 / mean
        return float(np.clip(np.random.gamma(k, theta), lo, hi))

    if model == "circadian_gaussian":
        # mean is the resting anchor (e.g. RHR ~67). Add a day/night envelope:
        # low ~3am, peak ~3pm, then gaussian noise on top.
        day_factor = 0.5 - 0.5 * math.cos(2 * math.pi * (hour - 3) / 24)
        bpm = mean + 45.0 * day_factor + np.random.normal(0, max(std, 5))
        return float(np.clip(bpm, 40, 190))

    # gaussian / nightly_gaussian
    return float(np.clip(np.random.normal(mean, std), lo, hi))


def build_event(stream: dict, seed: dict, device: dict, now: datetime) -> dict:
    evt = {
        "event_id": uuid.uuid4().hex,                     # idempotency key for sinks
        "device_id": device["id"],
        "device_type": device["type"],
        "stream": stream["name"],
        "ts": now.astimezone(timezone.utc).isoformat(),   # always store UTC
    }
    hour = now.hour + now.minute / 60.0
    for sig in stream["signals"]:
        key = f"{stream['name']}.{sig['field']}"
        params = seed["params"].get(key)
        if params:
            evt[sig["field"]] = sample(sig["model"], params, hour)
    return evt


def _serialize(evt: dict) -> bytes:
    return json.dumps(evt).encode("utf-8")   # <- swap point for Avro+SR


def load() -> tuple[dict, dict]:
    return yaml.safe_load(STREAMS.read_text()), yaml.safe_load(SEED.read_text())


def make_fleet(n: int) -> list[dict]:
    return [{"id": f"dev-{i:05d}", "type": random.choice(DEVICE_TYPES)} for i in range(n)]


def events_per_tick(stream: dict, devices: int, rate: float) -> float:
    freq = stream["frequency_hz"]
    if freq == "event":
        per_sec = SESSIONS_PER_DEVICE_PER_DAY / 86_400.0
        return devices * per_sec * rate * TICK
    return devices * float(freq) * rate * TICK


def run_dry(streams, seed, fleet):
    print("=== sample events (dry-run, no Kafka) ===")
    now = datetime.now(timezone.utc)
    for s in streams["streams"]:
        print(f"\n-- {s['kafka_topic']} --")
        for _ in range(3):
            print("  ", json.dumps(build_event(s, seed, random.choice(fleet), now)))


def run_live(streams, seed, fleet, rate, bootstrap, duration):
    from confluent_kafka import Producer
    produced = {"ok": 0, "err": 0}

    def on_delivery(err, msg):
        if err:
            produced["err"] += 1
        else:
            produced["ok"] += 1

    p = Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",                 # wait for min.insync.replicas -> durability
        "enable.idempotence": True,    # safe retries, no duplicates on the broker
        "linger.ms": 50,               # small batching window for throughput
        "compression.type": "lz4",
    })

    print(f"producing to {bootstrap} | devices={len(fleet)} rate={rate} "
          f"(Ctrl-C to stop)")
    remainder = {s["name"]: 0.0 for s in streams["streams"]}
    start = last_stat = time.time()
    try:
        while True:
            tick_start = time.time()
            now = datetime.now(timezone.utc)
            for s in streams["streams"]:
                want = events_per_tick(s, len(fleet), rate) + remainder[s["name"]]
                n = int(want)
                remainder[s["name"]] = want - n
                for _ in range(n):
                    dev = random.choice(fleet)
                    evt = build_event(s, seed, dev, now)
                    p.produce(s["kafka_topic"], key=dev["id"].encode(),
                              value=_serialize(evt), on_delivery=on_delivery)
                p.poll(0)              # serve delivery callbacks

            if time.time() - last_stat >= STAT_EVERY:
                elapsed = time.time() - start
                rps = produced["ok"] / elapsed if elapsed else 0
                print(f"  t+{elapsed:5.0f}s  delivered={produced['ok']:>8d}  "
                      f"errors={produced['err']:>4d}  ~{rps:,.0f} msg/s  "
                      f"in_flight={len(p)}")
                last_stat = time.time()

            if duration and time.time() - start >= duration:
                break
            time.sleep(max(0, TICK - (time.time() - tick_start)))
    except KeyboardInterrupt:
        print("\nstopping, flushing buffered messages...")
    finally:
        p.flush(30)
        print(f"final: delivered={produced['ok']} errors={produced['err']}")


def run_backfill(streams, seed, fleet, days, bootstrap, step_min):
    """Burst-produce historical events spanning the past `days` days, then exit.

    Gives Airflow multi-day history to aggregate into daily gold metrics. Unlike
    the live loop, continuous streams are sampled every `step_min` minutes (much
    coarser than 1 Hz) so N days x 200 devices stays a sane volume — daily gold
    only needs enough points per device per day, not full resolution. Event
    streams get a Poisson count per device. Timestamps are historical, so the
    circadian model still shapes each day correctly.
    """
    from confluent_kafka import Producer
    p = Producer({"bootstrap.servers": bootstrap, "acks": "all",
                  "enable.idempotence": True, "linger.ms": 50,
                  "compression.type": "lz4"})
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)
    continuous = [s for s in streams["streams"] if s["frequency_hz"] != "event"]
    events = [s for s in streams["streams"] if s["frequency_hz"] == "event"]
    produced = 0

    t = start
    while t < now:
        for s in continuous:
            for dev in fleet:
                p.produce(s["kafka_topic"], key=dev["id"].encode(),
                          value=_serialize(build_event(s, seed, dev, t)))
                produced += 1
        p.poll(0)
        t += timedelta(minutes=step_min)

    for s in events:                                   # workouts: Poisson per device
        for dev in fleet:
            for _ in range(np.random.poisson(SESSIONS_PER_DEVICE_PER_DAY * days)):
                ts = start + timedelta(seconds=random.random() * days * 86_400)
                p.produce(s["kafka_topic"], key=dev["id"].encode(),
                          value=_serialize(build_event(s, seed, dev, ts)))
                produced += 1
        p.poll(0)

    print(f"backfilling {days}d ({step_min}min step) — flushing {produced:,} events...")
    p.flush(60)
    print(f"backfill done: produced {produced:,} events across {days} days")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--devices", type=int, default=200)
    ap.add_argument("--rate", type=float, default=1.0, help="throughput multiplier")
    ap.add_argument("--duration", type=float, default=0, help="seconds; 0 = forever")
    ap.add_argument("--bootstrap",
                    default=os.getenv("BOOTSTRAP_SERVERS",
                                      "localhost:29092,localhost:29093,localhost:29094"))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--seed", type=int, default=None, help="RNG seed for reproducibility")
    ap.add_argument("--exclude", default="",
                    help="comma-separated stream names to NOT produce (e.g. hrv) — "
                         "drives chaos scenario 4 (a source stream goes silent)")
    ap.add_argument("--backfill-days", type=int, default=0,
                    help="burst-produce this many past days of history, then exit")
    ap.add_argument("--backfill-step-min", type=int, default=30,
                    help="minutes between continuous-stream samples during backfill")
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)
        np.random.seed(args.seed)

    streams, seed = load()
    if args.exclude:
        drop = {x.strip() for x in args.exclude.split(",") if x.strip()}
        streams["streams"] = [s for s in streams["streams"] if s["name"] not in drop]
        active = [s["name"] for s in streams["streams"]]
        print(f"[exclude] silencing {sorted(drop)} — active streams: {active}")
    fleet = make_fleet(args.devices)

    if args.dry_run:
        run_dry(streams, seed, fleet)
        return 0
    if args.backfill_days > 0:
        run_backfill(streams, seed, fleet, args.backfill_days,
                     args.bootstrap, args.backfill_step_min)
        return 0
    run_live(streams, seed, fleet, args.rate, args.bootstrap, args.duration)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())