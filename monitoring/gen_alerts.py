"""
gen_alerts.py — generate Prometheus alert rules from streams.yaml SLAs.

Metadata-driven (ADR-0003): the lag threshold lives next to the stream it
governs (sla.alert_lag_threshold). Re-run after editing streams.yaml to
regenerate monitoring/prometheus/alerts.yml — thresholds are never hand-edited
in the alert file.

Usage:  python monitoring/gen_alerts.py
"""
from __future__ import annotations
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]
META = ROOT / "metadata" / "streams.yaml"
OUT = ROOT / "monitoring" / "prometheus" / "alerts.yml"


def main() -> int:
    m = yaml.safe_load(META.read_text())

    # Pipeline layer: one consumer-lag alert per stream, threshold from its SLA.
    lag_rules = []
    for s in m["streams"]:
        topic, thr = s["kafka_topic"], s["sla"]["alert_lag_threshold"]
        lag_rules.append({
            "alert": f"ConsumerLagHigh_{s['name']}",
            "expr": f'sum(kafka_consumergroup_lag{{consumergroup="router",topic="{topic}"}}) > {thr}',
            "for": "1m",
            "labels": {"severity": "warning", "layer": "pipeline", "stream": s["name"]},
            "annotations": {
                "summary": f"Consumer lag on {topic} exceeds {thr}",
                "description": (f"router lag on {topic} has held above {thr} for 1m "
                               f"(SLA from streams.yaml). Downstream readiness scores "
                               f"may go stale."),
            },
        })

    # Cluster layer: broker loss + under-replication (fixed, not per-stream).
    cluster_rules = [
        {
            "alert": "KafkaBrokerDown",
            "expr": "kafka_brokers < 3",
            "for": "30s",
            "labels": {"severity": "critical", "layer": "cluster"},
            "annotations": {
                "summary": "A Kafka broker is down",
                "description": "kafka_brokers < 3 — a broker has left the cluster.",
            },
        },
        {
            "alert": "UnderReplicatedPartitions",
            "expr": "sum(kafka_topic_partition_under_replicated_partition) > 0",
            "for": "30s",
            "labels": {"severity": "critical", "layer": "cluster"},
            "annotations": {
                "summary": "Under-replicated partitions present",
                "description": ("At least one partition is under-replicated — "
                                "a replica has fallen out of the ISR; durability at risk."),
            },
        },
    ]

    # Data layer: surfaced from dq_results via postgres-exporter (single pane of
    # glass). These fire even when infra is fully green — the whole point of
    # two-layer observability.
    data_rules = [
        {
            "alert": "DataFreshnessStale",
            "expr": "max(dq_freshness_lag_days) > 0",
            "for": "1m",
            "labels": {"severity": "warning", "layer": "data"},
            "annotations": {
                "summary": "Gold metric is stale (data-layer anomaly)",
                "description": ("A gold metric lags its source stream — a required "
                                "input may have gone silent. Infra can look healthy."),
            },
        },
        {
            "alert": "DataQualityCheckFailing",
            "expr": "min(dq_status) < 1",
            "for": "1m",
            "labels": {"severity": "warning", "layer": "data"},
            "annotations": {
                "summary": "A data-quality check is failing",
                "description": ("dq_status=0 for a gold metric (empty, drifted, or "
                                "stale). See the failing Airflow dq_check task."),
            },
        },
    ]

    doc = {"groups": [
        {"name": "pipeline_lag", "rules": lag_rules},
        {"name": "cluster_health", "rules": cluster_rules},
        {"name": "data_quality", "rules": data_rules},
    ]}

    OUT.write_text(
        "# GENERATED from metadata/streams.yaml by monitoring/gen_alerts.py.\n"
        "# Do not edit by hand — change the SLA in streams.yaml and re-run.\n"
        + yaml.safe_dump(doc, sort_keys=False, allow_unicode=True))
    print(f"[ok] {len(lag_rules)} lag rules + {len(cluster_rules)} cluster rules "
          f"+ {len(data_rules)} data rules -> {OUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())