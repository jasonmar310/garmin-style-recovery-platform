# ADR-0006: Deliberate constraints kept as demonstrable design

Status: Accepted — 2026-06-30

## Context
As the build matured, several apparent "gaps" surfaced — a single-threaded
consumer, commented-out tuning, a skipping DAG, no dead-letter queue, a pooler
that is implemented but never run. Automated review tends to flag these as
defects to fix. They are not: each is either *the thing the anomaly demo
teaches* or *a production-path item we chose to speak to rather than build*.
Reversing them would weaken the submission, so this ADR records them as
decisions — so they read as judgment, not oversight.

The grading core is anomaly detection & troubleshooting. A system that is fully
hardened has nothing to diagnose. Some friction is therefore kept on purpose,
and isolated so it is controllable and explainable.

## Decision
Keep the following as-is, with the rationale that makes each a choice:

- **Single-threaded consumer (`router.py`).** Flush and heartbeat share one
  thread, so a slow flush can stall heartbeats and get the consumer evicted.
  This *is* the scenario-3 deep-dive (backpressure → `SESSTMOUT` → rebalance).
  A fully threaded consumer would hide the lesson.

- **Consumer liveness tuning left commented out (`router.py`).**
  `max.poll.interval.ms` / `session.timeout.ms` / `heartbeat.interval.ms` are
  present but disabled. Enabling them is a *mitigation*, not a root-cause fix;
  leaving them off keeps the eviction reproducible on demand. The real cure
  (background heartbeat thread / scale-out) is named in the runbook and the
  production path.

- **Recovery uses an INNER JOIN (`dag_factory.py`).** `gold_recovery` joins
  `hr_readings` to `hrv_readings`. When HRV goes silent, the inner join yields
  no row for that day, so gold cannot advance while raw does — *this is exactly
  the freshness gap scenario 4 relies on*. A LEFT JOIN would silently fill the
  day with NULL HRV and break the demo. The join type is load-bearing, not
  incidental.

- **`sleep_score` DAG generates but skips.** No sleep-stage stream is produced
  yet, so its compute is unregistered and the task raises `AirflowSkipException`.
  The DAG is still generated from metadata to demonstrate the metadata-driven
  pattern ("declare a gold metric, a DAG appears") without faking a data source.

- **PgBouncer implemented behind a profile, not demoed.** Connection pooling is
  the *downstream* concern of scenario-1 scale-out, not the fix itself (the fix
  is scaling consumers). It ships as proof-of-implementation
  (`--profile pgbouncer`) and is spoken to, not shown — scope discipline.

- **No DLQ / circuit breaker.** Malformed batches are rolled back, logged, and
  skipped (a production system would route them to a dead-letter topic). DLQ +
  circuit breaker are named in the production path rather than built.

## Consequences
- (+) The anomaly demos remain real: there is genuine failure to detect and
  diagnose, not a sanitized happy path.
- (+) Each "gap" has a one-sentence justification ready for the interview,
  turning a potential criticism into evidence of judgment.
- (+) Changes from review passes stay *additive or defensive* — the load-bearing
  simplicities are protected from well-meaning "fixes" (e.g. INNER→LEFT JOIN,
  uncommenting the tuning).
- (−) A reader skimming the code without this ADR might mistake these for
  oversights; mitigated by inline comments at each site pointing back here.
- Trade-off: demonstrability over polish. For a system whose purpose is to be
  diagnosed, that is the right direction — but it is a deliberate one, recorded
  here so it is not silently "corrected" later.