# ADR-0002: Synthetic-first data, seeded by layered real sources

Status: Accepted — 2026-06-26

## Context
The pipeline needs a workload we can throttle on demand to induce consumer lag
and backpressure (the headline anomalies). Static real datasets cannot be
"sped up", and they do not stream. Two real sources are available: a personal
Whoop export (per-cycle / daily derived metrics: Recovery, HRV, RHR, sleep
stages, strain, workouts) and the public Fitbit Fitness Tracker dataset
(minute-level HR/steps across ~30 users).

Inspection of the Whoop export showed it is **derived daily output**, not raw
signal — effectively gold-layer ground truth. It lacks intra-day raw texture.

## Decision
Generate synthetic raw events from a rate-configurable simulator, **seeded by
real data at the layer each source actually covers** (not a row-level merge):
- Whoop → seeds daily target distributions AND serves as the gold-layer
  validation set (synthetic-derived scores are checked against its distribution).
- Fitbit (when added) → seeds intra-day raw HR/steps circadian texture.
- Until Fitbit is wired in, intra-day shape is a parametric circadian model.

Distributions are extracted by a metadata-driven profiler (etl/profile_seed.py)
into metadata/seed_params.yaml.

## Consequences
- (+) Controllable throughput for anomaly demos, plus realistic distributions.
- (+) Closed validation loop: synthetic gold vs. real Whoop distribution.
- (+) Privacy by design — raw personal data is gitignored; only non-identifying
      distribution parameters are committed.
- (−) Row-level cross-source correlations are not preserved (different people,
      grains). Acceptable: we want realistic marginals, not joint truth.
- Trade-off: Whoop's 43-day window is small (n≈41 daily rows). Fine for seeding
      marginals; not enough for time-series modelling — hence parametric
      circadian shape rather than learned.
