#!/usr/bin/env bash
# ============================================================================
# Scenario 4 (demo trigger) — make recovery STALE so the DQ freshness gate fires
# ----------------------------------------------------------------------------
# WHY a delete and not stop_hrv.sh: stop_hrv.sh silences the HRV producer, but
# its events are still timestamped "today", so hr and hrv share the same max day
# until midnight rolls over — you can't wait that long in a demo. This script
# creates the freshness gap instantly and reproducibly.
#
# MECHANISM : for the latest day D present in hr_readings, delete that day's HRV
#             rows (HRV "went silent") AND that day's gold_recovery rows (so a
#             recompute can't quietly keep the old score). Recovery needs HRV, so
#             re-running the DAG cannot rebuild day D -> gold_recovery.max_day
#             falls to D-1 while hr_readings.max_day stays D.
# WATCH     : Prometheus / Grafana stay GREEN (no infra error). Re-run
#             gold_recovery_score in Airflow -> dq_check goes RED:
#             "latest day ... lags hr_readings ... input stream went silent".
# DEMO STORY: two-layer observability — infra is green, only the data-quality
#             layer catches that readiness scores stopped updating.
# REVERT    : ./chaos/stale_hrv.sh --restore   (regenerates today's data;
#             needs `make route` running in another terminal)
# ============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

PGU="${TIMESCALE_USER:-ian}"; PGD="${TIMESCALE_DB:-telemetry}"
psql_exec() { docker exec -e PGPASSWORD="${TIMESCALE_PASSWORD:-}" timescaledb \
              psql -U "$PGU" -d "$PGD" -tAc "$1"; }

if [[ "${1:-}" == "--restore" ]]; then
  echo ">> RESTORE: regenerating recent data (ensure 'make route' is running)..."
  python simulator/generator.py --devices 200 --rate 5 --duration 40
  echo ">> Now re-trigger gold_recovery_score in Airflow — dq_check should go GREEN."
  exit 0
fi

D="$(psql_exec "SELECT max(date_trunc('day',ts)::date) FROM hr_readings;")"
echo ">> latest hr_readings day = ${D}; silencing HRV + removing gold for that day"
psql_exec "DELETE FROM hrv_readings  WHERE date_trunc('day',ts)::date = '${D}';" >/dev/null
psql_exec "DELETE FROM gold_recovery WHERE day = '${D}';" >/dev/null

echo ">> state now (hr should lead hrv & gold):"
docker exec -e PGPASSWORD="${TIMESCALE_PASSWORD:-}" timescaledb psql -U "$PGU" -d "$PGD" -c \
  "SELECT 'hr_readings' t, max(date_trunc('day',ts)::date) FROM hr_readings
   UNION ALL SELECT 'hrv_readings', max(date_trunc('day',ts)::date) FROM hrv_readings
   UNION ALL SELECT 'gold_recovery', max(day) FROM gold_recovery;"

echo ">> Now RE-TRIGGER gold_recovery_score in Airflow."
echo ">> Expect: compute GREEN, dq_check RED (freshness gap), while Grafana stays GREEN."
echo ">> Restore with: ./chaos/stale_hrv.sh --restore"