"""
create_topics.py — metadata-driven Kafka topic provisioning.

Reads metadata/streams.yaml and creates each stream's topic with the declared
partition count + the cluster-wide replication factor, plus topic-level configs
(min.insync.replicas, retention, cleanup policy).

Why explicit creation instead of Kafka auto-create:
  - auto-created topics default to replication.factor=1 -> a single broker loss
    would lose the partition, silently breaking the broker-failure demo.
  - we control partition count, which sets max consumer parallelism and can only
    ever be increased, never decreased.
Idempotent: re-running skips topics that already exist.

Usage:
  python ingest/create_topics.py --dry-run     # print the plan, no connection
  python ingest/create_topics.py --verify      # create, then describe results
  BOOTSTRAP_SERVERS=localhost:29092 python ingest/create_topics.py
"""
from __future__ import annotations
import argparse, os, sys, time
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]
META = ROOT / "metadata" / "streams.yaml"

# Kafka is a replay buffer, not the system of record — MinIO (cold tier) holds
# data long-term. So we keep only a short retention window in Kafka itself.
DEFAULT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000   # 7 days


def planned_topics(meta: dict) -> list[dict]:
    rf = meta["defaults"]["replication_factor"]
    min_isr = meta["defaults"]["min_insync_replicas"]
    plan = []
    for s in meta.get("streams", []):
        plan.append({
            "topic": s["kafka_topic"],
            "partitions": int(s["partitions"]),
            "replication_factor": int(rf),
            "config": {
                "min.insync.replicas": str(min_isr),   # topic-level, travels with the topic
                "retention.ms": str(DEFAULT_RETENTION_MS),
                "cleanup.policy": "delete",
            },
        })
    return plan


def print_plan(plan: list[dict]) -> None:
    print(f"  {'topic':26s} {'parts':>5s} {'RF':>3s} {'minISR':>7s} {'retention':>9s}")
    for t in plan:
        days = int(t["config"]["retention.ms"]) // 86_400_000
        print(f"  {t['topic']:26s} {t['partitions']:>5d} {t['replication_factor']:>3d} "
              f"{t['config']['min.insync.replicas']:>7s} {str(days)+'d':>9s}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap",
                    default=os.getenv("BOOTSTRAP_SERVERS",
                                      "localhost:29092,localhost:29093,localhost:29094"))
    ap.add_argument("--dry-run", action="store_true", help="print plan, do not connect")
    ap.add_argument("--verify", action="store_true", help="describe topics after creating")
    args = ap.parse_args()

    meta = yaml.safe_load(META.read_text())
    plan = planned_topics(meta)

    print("=== planned topics (derived from metadata/streams.yaml) ===")
    print_plan(plan)

    if args.dry_run:
        print("\n[dry-run] nothing created, no connection made.")
        return 0

    # Imported here so --dry-run works without the kafka client installed.
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    new_topics = [
        NewTopic(t["topic"], num_partitions=t["partitions"],
                 replication_factor=t["replication_factor"], config=t["config"])
        for t in plan
    ]

    print(f"\n=== creating on {args.bootstrap} ===")
    for topic, fut in admin.create_topics(new_topics).items():
        try:
            fut.result()                          # block until the broker confirms
            print(f"  created: {topic}")
        except Exception as e:
            if "already exists" in str(e).lower() or "TOPIC_ALREADY_EXISTS" in str(e):
                print(f"  exists (skipped): {topic}")
            else:
                print(f"  ERROR {topic}: {e}", file=sys.stderr)

    if args.verify:
        # Topic creation is async: the create future resolving does NOT mean the
        # new metadata has propagated to the broker we query. Poll until every
        # planned topic appears (or we give up), instead of reading once and
        # racing the propagation.
        wanted = {t["topic"] for t in plan}
        deadline = time.time() + 15
        md = admin.list_topics(timeout=10)
        while not wanted.issubset(md.topics.keys()) and time.time() < deadline:
            time.sleep(1)
            md = admin.list_topics(timeout=10)

        print("\n=== verify: actual partitions / RF reported by the cluster ===")
        for t in plan:
            tm = md.topics.get(t["topic"])
            if tm is None:
                print(f"  {t['topic']:26s} not visible yet (propagation lag) — re-run to confirm")
                continue
            parts = len(tm.partitions)
            rf = len(tm.partitions[0].replicas) if parts else 0
            flag = "" if rf == t["replication_factor"] else "  <-- RF mismatch!"
            print(f"  {t['topic']:26s} partitions={parts} RF={rf}{flag}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())