# ADR-0003: Metadata-driven control plane

Status: Accepted — 2026-06-26

## Context
The pipeline has many per-stream concerns (topic names, partition counts,
sinks, SLA thresholds, which real column seeds which signal, which gold metric
derives from what). Hardcoding these across the simulator, topic creation,
router, Airflow DAGs, and alert rules creates drift and makes adding a sensor
type a multi-file change.

## Decision
Make metadata/streams.yaml the single source of truth. Every component reads
it rather than embedding config:
- simulator → which signals, rates, seeded distributions
- ingest/create_topics.py → topics + partitions + RF
- ingest/router.py → routing to hot/cold sinks
- airflow/dags/dag_factory.py → dynamically generated per-stream DAGs
- monitoring/prometheus/alerts.yml → thresholds templated from sla blocks
- etl/profile_seed.py → which real columns to profile

Adding a new stream/signal = one metadata edit, zero application-code change.

## Consequences
- (+) Extensibility and consistency; the demo of "add a sensor in one line" is
      a strong interview talking point.
- (+) Alert thresholds and SLAs live next to the stream they govern.
- (−) Upfront indirection cost; a reader must understand the metadata schema
      before the code. Mitigated by this ADR and inline comments in streams.yaml.
- (−) Risk of an over-general framework. Mitigated by only generalizing the
      axes that actually vary across streams (topic, partitions, sink, sla, seed).
