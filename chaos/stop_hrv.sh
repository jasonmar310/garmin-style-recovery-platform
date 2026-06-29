#!/usr/bin/env bash
# ============================================================================
# Scenario 4 — data-layer anomaly (silent input stream)  ->  DQ catches it
# ----------------------------------------------------------------------------
# MECHANISM : produce ALL streams EXCEPT hrv. heart_rate + workout keep flowing,
#             so Kafka/Prometheus see nothing wrong. But recovery_score needs HRV
#             -> it can't compute new days -> gold_recovery falls behind hr_readings.
# WATCH     : Prometheus / Grafana stay GREEN (brokers=3, lag~0, no infra error).
#             Re-run the gold_recovery DAG in Airflow: the dq_check task goes RED
#             with "latest day lags hr_readings — input stream went silent".
# DIAGNOSE  : THIS is the point of two-layer observability — an infra-only setup
#             would show all-green while readiness scores silently go stale. The
#             data-quality layer (Airflow DQ) is the only thing that catches it.
# BUSINESS  : HRV sensor/stream outage -> recovery & readiness scores stop
#             updating; users see yesterday's number, no error anywhere in infra.
# REVERT    : Ctrl-C; run a normal `make simulate` (all streams) + re-run the DAG.
# ============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

DEVICES="${1:-200}"
DURATION="${2:-180}"     # seconds; long enough to produce a day with no HRV

echo ">> STOP-HRV: producing heart_rate + workout only (hrv silenced) for ${DURATION}s"
echo ">> Infra stays green. Then trigger gold_recovery in Airflow -> dq_check should FAIL."
exec python simulator/generator.py \
  --devices "${DEVICES}" --rate 1 --duration "${DURATION}" --exclude hrv