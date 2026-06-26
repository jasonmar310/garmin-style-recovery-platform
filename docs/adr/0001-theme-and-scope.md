# ADR-0001: Project theme and scope

Status: Accepted — 2026-06-26

## Context
The take-home grades on anomaly detection and troubleshooting; service setup
and monitoring are scaffolding for that. A common-denominator submission
(raw telemetry → Grafana) does not differentiate. The role is at Garmin.

## Decision
Build a **Garmin-style recovery & training-readiness platform**: ingest raw
wearable signals and derive Garmin's signature metrics (Recovery, HRV status,
Sleep score, Day strain). Scope = streaming core + object-storage cold tier:
Kafka (KRaft) + TimescaleDB (hot) + MinIO (cold) + Airflow + Prometheus/Grafana.
Excluded for now: MQTT front-end, Elasticsearch.

## Consequences
- (+) On-brand; uses Garmin's own metric vocabulary, not a competitor's.
- (+) Derived metrics give a real bronze→silver→gold story, not just aggregation.
- (+) Anomaly impact becomes business-level ("stale readiness score"), not just
      "lag went up".
- (−) Gold-layer logic is extra work vs. pass-through; mitigated by keeping the
      derivations approximate and metadata-driven.
- Trade-off: depth over breadth — three service categories done well beats six
      done shallowly.
