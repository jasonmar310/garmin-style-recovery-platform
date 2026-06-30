# Garmin Recovery & Training-Readiness Platform

![CI](https://github.com/<OWNER>/<REPO>/actions/workflows/ci.yml/badge.svg)

A metadata-driven streaming pipeline that ingests synthetic wearable telemetry,
lands it in a hot (TimescaleDB) and cold (MinIO) tier, derives Garmin-style
recovery metrics via Airflow, and is monitored end-to-end with Prometheus +
Grafana. **The focus is anomaly detection and troubleshooting.**

> New here? Read [`docs/PROJECT_OVERVIEW.md`](docs/PROJECT_OVERVIEW.md) for the
> full orientation, [`docs/architecture.md`](docs/architecture.md) for the
> diagram, and [`docs/runbook.md`](docs/runbook.md) for the incident SOPs.

## Data flow

```
simulator ──> Kafka (3-broker KRaft) ──> router ──┬─> TimescaleDB (hot)
   ▲                                               └─> MinIO / Parquet (cold, bronze)
   │ seed_params.yaml                                        │
   │                                            Airflow (bronze→gold + DQ)
metadata/streams.yaml  ◄── etl/profile_seed.py ◄── real Whoop data
        │
        └── drives: topics · routing · DAGs · alert thresholds

Prometheus (kafka / node / postgres / router exporters) ──> Grafana (3 dashboards + alerts)
```

Two observability layers land on one Grafana ("single pane of glass"):
**infrastructure** metrics and **data-quality** metrics (Airflow DQ surfaced via
postgres-exporter). The data layer catches anomalies the infra layer is blind to.

## Service categories (assignment requirement)

| Category | Service | Role |
|---|---|---|
| Messaging / streaming | Kafka (3-broker KRaft, RF=3 / min.insync=2) | ingestion backbone, replay buffer |
| Object storage | MinIO (S3-compatible) | cold tier, Parquet bronze archive |
| Relational database | TimescaleDB (PostgreSQL) | hot tier + derived gold metrics + `dq_results` |

## Control plane
`metadata/streams.yaml` is the single source of truth. Adding a stream or
signal is a one-line metadata edit — no application-code change (see ADR-0003).

## Quick start

```bash
cp .env.example .env          # set passwords, CLUSTER_ID, PGPORT=5433
make up                       # Kafka x3 + TimescaleDB + MinIO
make verify                   # KRaft quorum healthy
make topics                   # create topics from metadata
make monitoring-up            # Prometheus + Grafana + exporters
make backfill                 # seed ~14 days of history
make route & make simulate &  # consumer + baseline load
make airflow-up               # medallion + DQ
make dag-run                  # trigger gold DAG, wait, show DQ status
```

Full command list: `make help`.

## Anomaly scenarios (the scored core)

Each is reproducible on demand; diagnose with the cheat sheet in the runbook. The
key discriminator is **throughput** (load problem = throughput up; downstream
problem = throughput flat).

| # | Inject | Detect | Restore |
|---|---|---|---|
| 1 surge | `make chaos-surge` | lag ↑ + throughput ↑ | stop the surge |
| 2 broker | `make chaos-kill` | brokers 3→2, under-replicated ↑ | `make chaos-restore` |
| 3 backpressure | `make chaos-choke` | lag ↑, throughput flat, flush p95 ↑ | auto-releases |
| 4 DQ freshness | `make chaos-stale` | infra green, DQ status FAIL | `make chaos-stale-restore` |

Diagnostic helpers: `make check-lag` / `check-cluster` / `check-db` / `dq-status`
/ `check-all`.

## Seed flow (source-data-driven)
1. Drop real exports in `data/whoop/` (gitignored).
2. `python etl/profile_seed.py` → writes `metadata/seed_params.yaml`
   (non-identifying distribution parameters).
3. The simulator generates raw events matching those distributions.
4. Airflow-derived gold scores are validated against the real Whoop distribution.

## Decisions
See [`docs/adr/`](docs/adr/) — theme/scope (0001), data strategy (0002),
metadata-driven design (0003), Kafka topology (0004), Airflow (0005), and
deliberate constraints kept as demonstrable design (0006).

## CI
`.github/workflows/ci.yml` runs static validation on every push/PR: Python, YAML,
JSON, and shell parse; Compose config is valid; and the generated `alerts.yml` is
verified in sync with `streams.yaml` (the metadata-driven design proves itself).

## Status
- [x] Repo scaffold + ADRs (0001–0006)
- [x] Metadata control plane (`streams.yaml`)
- [x] Source-data-driven profiler → `seed_params.yaml`
- [x] Docker Compose: Kafka KRaft 3-broker + Timescale + MinIO
- [x] Simulator (rate-configurable)
- [x] Router → hot/cold sinks (effectively-once), exposes `/metrics`
- [x] Airflow DAG factory + DQ checks
- [x] Prometheus + Grafana (3 dashboards, infra + data-quality alerts)
- [x] Four anomaly scenarios + chaos scripts + runbook
- [x] Lightweight CI