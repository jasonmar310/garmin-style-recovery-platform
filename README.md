# Garmin Recovery & Training-Readiness Platform

A metadata-driven streaming pipeline that ingests synthetic wearable telemetry,
lands it in a hot (TimescaleDB) and cold (MinIO) tier, derives Garmin-style
recovery metrics via Airflow, and is monitored end-to-end with Prometheus +
Grafana. The focus is anomaly detection and troubleshooting.

## Data flow

```
simulator ──> Kafka (3-broker KRaft) ──> router ──┬─> TimescaleDB (hot)
   ▲                                               └─> MinIO / Parquet (cold, bronze)
   │ seed_params.yaml                                        │
   │                                            Airflow (bronze→silver→gold + DQ)
metadata/streams.yaml  ◄── etl/profile_seed.py ◄── real Whoop/Fitbit data
        │
        └── drives: topics · routing · DAGs · alert thresholds

Prometheus (kafka-exporter, jmx, postgres, node) ──> Grafana (3-layer dashboards)
```

## Control plane
`metadata/streams.yaml` is the single source of truth. Adding a stream or
signal is a one-line metadata edit — no application-code change (see ADR-0003).

## Seed flow (source-data-driven)
1. Drop real exports in `data/whoop/` (gitignored).
2. `python etl/profile_seed.py` → writes `metadata/seed_params.yaml`
   (non-identifying distribution parameters).
3. The simulator generates raw events matching those distributions.
4. Airflow-derived gold scores are validated against the real Whoop distribution.

## Decisions
See `docs/adr/` — theme/scope, data strategy, metadata-driven design, Kafka
topology.

## Status
- [x] Repo scaffold + ADRs
- [x] Metadata control plane (`streams.yaml`)
- [x] Source-data-driven profiler → `seed_params.yaml`
- [ ] Docker Compose: Kafka KRaft 3-broker + Timescale + MinIO
- [ ] Simulator (rate-configurable)
- [ ] Router → hot/cold sinks
- [ ] Airflow DAG factory + DQ checks
- [ ] Prometheus + Grafana dashboards
- [ ] Anomaly scenarios + runbook
