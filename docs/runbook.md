# Runbook — Garmin Recovery Platform

On-call SOPs for the telemetry pipeline. Each incident follows the same shape:
**symptom → detect → diagnose → mitigate → prevent**, with a measured
Mean-Time-To-Detect (MTTD). Chaos scripts under `chaos/` reproduce every
incident on demand.

---

## 0. Healthy baseline (know this before diagnosing)

| Signal | Healthy value | Dashboard |
|---|---|---|
| Brokers online | 3 | 1 · Cluster Health |
| Under-replicated partitions | 0 | 1 · Cluster Health |
| ISR per topic | steady (≈ RF) | 1 · Cluster Health |
| Consumer lag (router) | ~0, spikes converge | 2 · Pipeline Flow |
| Throughput | ~200 msg/s at baseline | 2 · Pipeline Flow |
| Router flush latency | tens of ms (`router_flush_duration_seconds`) | 2 · Pipeline Flow |
| TimescaleDB | UP, commit ~2 ops/s | 3 · Data Stores |
| Gold freshness | 0 days lag; DQ status = PASS | 3 · Data Stores / Airflow `dq_check` |

A lag *spike that converges* is normal (startup catch-up + batch cadence). A lag
that *diverges monotonically* is an incident.

---

## 1. Metric → component cheat sheet

The fast path from "a number moved" to "which component is wrong":

| Observation | Likely root cause | Confirm with |
|---|---|---|
| Lag ↑ **and** throughput ↑ | Load surge (producers outrunning consumer) | Throughput panel spikes |
| Lag ↑ **but** throughput **flat** | Downstream backpressure (slow sink) | Commit rate ↓, flush latency ↑ |
| Lag ↑ **and** throughput **flat at 0** | Consumer down / evicted from group | router process dead, no `flushed` logs |
| Brokers < 3, under-replicated > 0 | Broker / node failure | ISR drop on Cluster Health |
| Infra all green, scores stale | Data-layer anomaly (silent input stream) | Airflow `dq_check` red |

> The single most useful discriminator is **throughput**: it separates a *load*
> problem (throughput up) from a *downstream* problem (throughput flat) when both
> present the same lag symptom.

---

## 2. Incident: Consumer lag high (load surge)

- **Alert:** `ConsumerLagHigh_<stream>` (threshold from `streams.yaml` SLA, for 1m)
- **Reproduce:** `make chaos-surge` (`./chaos/surge.sh 30 300` — 30× baseline, 300s, 450 devices)
- **Symptom:** Total consumer lag climbs; `heart_rate` leads (6 partitions, highest rate).

**Detect** — 2 · Pipeline Flow → "Consumer lag by topic" rising; alert fires.

**Diagnose**
1. Is throughput also up? → Pipeline Flow "Throughput" panel. **Yes → load surge.**
2. Which stream/partition? → "Per-partition lag" panel localizes the hot stream.

**Mitigate (stop the bleed)**
- Scale out the consumer: start a second router in the same group `router` —
  `ROUTER_METRICS_PORT=8004 make route` (:8001 is held by the first instance;
  :8002/:8003 belong to hrv-alerter / readiness-api). Kafka rebalances partitions
  across instances; combined throughput rises; lag drains.

**Prevent (root cause)**
- Lag-based autoscaling (e.g. KEDA on `kafka_consumergroup_lag`).
- Capacity-plan partition counts (max consumer parallelism = partition count).
- Tiered alerting: warning vs critical thresholds so only real incidents page.

**Downstream caveat:** each consumer instance opens a DB connection. At scale this
approaches `max_connections`; the production answer is a connection pooler
(PgBouncer, transaction mode) in front of TimescaleDB — *not* per-instance pools
(the router is single-threaded and doesn't contend within a process).

**MTTD:** ≈ time to cross threshold + `for: 1m` + one scrape (15s).

---

## 3. Incident: Broker / node failure

- **Alerts:** `KafkaBrokerDown` (for 30s), `UnderReplicatedPartitions` (for 30s)
- **Reproduce:** `make chaos-kill` (`docker kill kafka2`); restore `make chaos-restore`
- **Symptom:** Brokers online 3 → 2; under-replicated partitions spike; ISR drops by 1.

**Detect** — 1 · Cluster Health → "Brokers online" and "Under-replicated partitions".

**Diagnose**
1. Confirm scope: writes still succeed? With RF=3 / min.insync=2, **one** broker
   down keeps partitions writable (2 in-sync replicas remain). This is *reduced
   redundancy*, not an outage — **does not page at critical-write severity**.
2. Killing a **second** broker would drop in-sync below 2 → writes blocked
   (`NOT_ENOUGH_REPLICAS`). That is the designed availability boundary.

**Mitigate**
- Restart the broker: `make chaos-restore` (prod: ASG/MSK replaces the node).
- ISR re-expands and under-replicated returns to 0 within ~30–60s.

**Prevent**
- Multi-AZ broker placement: one AZ loss costs one broker, cluster survives.
- `min.insync.replicas=2` makes the availability/consistency boundary explicit.
- Self-hosted exposes ISR / under-replicated / controller via JMX — these are the
  metrics that make broker diagnosis possible (managed MSK hides them).

**MTTD:** ≈ 30–45s (`for: 30s` + scrape 15s). Tunable: drop scrape to 5s for
faster detection at higher Prometheus load — a detection-speed vs. cost trade-off.

---

## 4. Incident: Downstream backpressure (slow sink)

- **Reproduce:** `make chaos-choke` (`./chaos/choke_sink.sh 120` — locks `hr_readings`)
- **Symptom:** Lag climbs **while throughput stays flat**; flush latency spikes;
  commit rate falls toward 0.

**Detect** — router log shows `flushed ... in <large>ms`; Pipeline Flow lag rises;
3 · Data Stores commit rate drops.

**Diagnose (the key discrimination)**
1. Lag is up — surge or backpressure? → **Throughput panel.** Flat throughput +
   rising lag = **downstream backpressure**, not load.
2. Confirm at the sink: 3 · Data Stores → commit rate ↓, active connections ↑.
   Attribution cross-check: `make check-lag-groups` — `hrv-alerter` (same broker,
   no DB dependency) stays flat while `router` lags ⇒ the fault is the router's
   sink, not Kafka.
3. Failure propagates **upstream**: slow hot sink → router INSERT blocks → offsets
   can't commit → Kafka lag backs up.

**Deep-dive — secondary failure (single-threaded consumer):** the router runs
flush and heartbeat on one thread. A long flush stalls heartbeats; if it exceeds
`session.timeout.ms` the coordinator evicts the consumer and triggers a rebalance
(observed as `SESSTMOUT` then `_NO_OFFSET` on the stale commit). So backpressure
can cascade into consumer-group instability — a deeper chain than lag alone.

**Mitigate**
- Remove the bottleneck (lock releases here; prod: kill the blocking query, raise
  the connection ceiling, isolate hot vs. cold sink writes).
- Router auto-recovers on restart and drains the backlog.

**Prevent**
- Decouple hot/cold sinks (async or separate consumers) so a slow cold tier
  doesn't stall hot.
- Connection pooling + batch tuning (batching already in place: 500 / 10s).
- Circuit breaker + dead-letter queue for persistent downstream failure.
- Background heartbeat thread (or larger `max.poll.interval.ms`) so processing
  stalls don't evict the consumer.

**MTTD:** lag alert as in §2. Flush latency is exported as
`router_flush_duration_seconds` (router `/metrics`, scraped as job `router`);
alert on its p95 to detect a slow sink before lag crosses its threshold.

---

## 5. Incident: Data-layer anomaly (silent input stream) — two-layer observability

- **Reproduce:** `make stop-ingest` first (a running generator/router would
  refill the deleted day — the script guards and aborts if either is alive),
  then `make chaos-stale` (removes latest HRV + gold day); restore
  `make chaos-stale-restore`. (`chaos/stop_hrv.sh` shows the real mechanism — a
  silenced HRV producer — but needs a day rollover to surface.)
- **Symptom:** **Infra stays green** (brokers 3, lag ~0, DB up). The
  `gold_recovery` DAG's `dq_check` task goes **red**.

**Detect** — Two surfaces, one root cause:
- Airflow: `gold_recovery_score` → `compute` green, `dq_check` red:
  `"latest day ... lags hr_readings ... a required input stream went silent"`.
- Grafana: 3 · Data Stores → "Gold DQ status" reads FAIL, "Gold freshness lag"
  rises above 0. Alerts `DataFreshnessStale` / `DataQualityCheckFailing` fire.
  (DQ results are written to `dq_results` and exported by postgres-exporter, so
  the data layer alerts on the same stack as infra — single pane of glass.)
- User view: `make readiness-check` — the readiness API (`make api`, :8003) stays
  green on its own RED metrics yet returns yesterday's `day`: the user-visible
  damage that infra monitoring cannot see.

**Diagnose**
1. Check infra first — Prometheus/Grafana are all green. No broker, lag, or DB
   error. Infra monitoring **cannot** see this.
2. The data-quality layer can: recovery needs HRV; HRV stopped; `gold_recovery`
   can't advance while `hr_readings` does → freshness gap.
3. Note: `compute` is green because its upsert only writes rows the SELECT
   produces; the missing day simply isn't built, so the freshness check (gold day
   vs. source day) is what catches it — not `compute`.

**Mitigate**
- Restore the input stream; re-run `gold_recovery_score`; freshness closes.

**Prevent**
- Data-layer guardrails (freshness, volume, distribution) as first-class checks.
- Schema enforcement (Schema Registry / Avro) on ingest.
- DQ results are surfaced as Prometheus metrics (`dq_freshness_lag_days`,
  `dq_status`) via a results table + postgres-exporter, so infra and data alert
  on one stack — a single pane of glass (implemented; see 3 · Data Stores).

**Why this matters:** an infra-only setup shows all-green while readiness scores
silently go stale. Two-layer observability — Prometheus for infrastructure,
Airflow DQ for data correctness — is the only thing that catches it.

---

## 6. Quick command reference

```
# stack
make startup-all / shutdown-all
make verify                      # KRaft quorum health
make monitoring-up               # Prometheus + Grafana + exporters

# load
make route                       # consumer (2nd instance: ROUTER_METRICS_PORT=8004 make route)
make simulate                    # baseline load
make backfill                    # 14 days of history

# chaos (each reproduces one incident)
make chaos-surge                 # §2 consumer lag
make chaos-kill / chaos-restore  # §3 broker failure
make chaos-choke                 # §4 backpressure
make chaos-stale / chaos-stale-restore   # §5 data-layer freshness

# diagnose
curl -s localhost:9090/api/v1/targets    # Prometheus target health
docker exec kafka1 kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group router              # native lag view
```

Dashboards: Grafana → Telemetry folder → 1 · Cluster Health / 2 · Pipeline Flow /
3 · Data Stores. Alerts: Prometheus → Alerts (rules generated from
`streams.yaml` SLAs via `make gen-alerts`).