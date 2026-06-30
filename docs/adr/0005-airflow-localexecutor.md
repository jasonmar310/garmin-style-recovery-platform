# ADR-0005: Airflow on LocalExecutor (2.10), profile-gated

Status: Accepted — 2026-06-27

## Context
Day 4 adds Airflow to derive Garmin-style gold metrics (Recovery, Sleep,
Strain) from the bronze tier and run data-quality checks. Choices: executor
(Local vs Celery vs Kubernetes), version (2.10 vs 3.x), and how it coexists with
the foundation stack on one laptop/EC2.

## Decision
- **LocalExecutor**, not Celery/K8s. Single-node, no Redis/worker/flower —
  enough for a take-home and far lighter on the host.
- **Airflow 2.10.x**, not 3.x. 3.x splits services further (api-server,
  standalone dag-processor, triggerer) with less-settled local-compose docs.
  The graded core is anomaly detection; Airflow is supporting cast, so demo
  reliability beats chasing the newest major.
- **Separate metadata Postgres** (`airflow-postgres`), isolated from the
  telemetry TimescaleDB — scheduler state and telemetry data are different
  concerns.
- **`profiles: [airflow]`** — Airflow only starts with
  `docker compose --profile airflow up -d`, so day-to-day pipeline work doesn't
  pay its memory cost.
- DAGs reach the stores over the Docker network (`timescaledb:5432`,
  `minio:9000`) — internal ports/names, not the host-published 5433.

## Consequences
- (+) Lean, reliable local setup; one command to add or drop Airflow.
- (+) DAG factory reads the same `metadata/streams.yaml` (mounted read-only),
  keeping the metadata-driven story consistent.
- (−) `_PIP_ADDITIONAL_REQUIREMENTS` installs deps at container start (slower
  boot); a production image would bake them in. Noted as a divergence.
- Trade-off: 2.10 over 3.x is a deliberate "stable now, 3.x is the forward
  path" call — documented so it reads as choice, not oversight.
```