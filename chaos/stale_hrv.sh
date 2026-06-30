#!/usr/bin/env bash
# ============================================================================
# Scenario 4 (demo trigger) — make recovery STALE so the DQ freshness gate fires
# ----------------------------------------------------------------------------
# WHY a delete and not stop_hrv.sh: stop_hrv.sh silences the HRV producer, but
# its events are still timestamped "today", so hr and hrv share the same max day
# until midnight — too slow for a demo. This creates the gap instantly.
#
# MECHANISM : for the latest day D in hr_readings, BACK UP then delete that day's
#             HRV rows (HRV "went silent") AND delete that day's gold_recovery
#             rows. Recovery needs HRV, so a recompute cannot rebuild day D ->
#             gold_recovery.max_day falls to D-1 while hr_readings stays at D.
# RESTORE   : re-inserts the backed-up HRV rows (pipeline-independent — does NOT
#             need the generator/router running), then you re-trigger the DAG to
#             rebuild gold for day D and close the gap.
# WATCH     : Prometheus / Grafana stay GREEN. Re-run gold_recovery_score ->
#             dq_check goes RED: "latest day ... lags hr_readings ...".
# DEMO STORY: two-layer observability — infra green, only the data-quality layer
#             catches that readiness scores stopped updating.
# NOTE      : HRV must stay silent. STOP the generator/router before injecting,
#             or new HRV for day D refills the gap. This script guards for that.
# ============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

PGU="${TIMESCALE_USER:-ian}"; PGD="${TIMESCALE_DB:-telemetry}"
psql_exec() { docker exec -e PGPASSWORD="${TIMESCALE_PASSWORD:-}" timescaledb \
              psql -U "$PGU" -d "$PGD" -tAc "$1"; }
show_state() {
  docker exec -e PGPASSWORD="${TIMESCALE_PASSWORD:-}" timescaledb psql -U "$PGU" -d "$PGD" -c \
    "SELECT 'hr_readings' t, max(date_trunc('day',ts)::date) d FROM hr_readings
     UNION ALL SELECT 'hrv_readings', max(date_trunc('day',ts)::date) FROM hrv_readings
     UNION ALL SELECT 'gold_recovery', max(day) FROM gold_recovery;"
}

# ---------------------------------------------------------------- restore -----
if [[ "${1:-}" == "--restore" ]]; then
  EXISTS="$(psql_exec "SELECT to_regclass('public._chaos_stale_hrv') IS NOT NULL;")"
  if [[ "$EXISTS" != "t" ]]; then
    echo ">> no backup table found — nothing to restore (was it injected with this script?)."
    echo ">> current state:"; show_state; exit 0
  fi
  N="$(psql_exec "SELECT count(*) FROM _chaos_stale_hrv;")"
  echo ">> RESTORE: re-inserting ${N} backed-up HRV rows (pipeline-independent)..."
  psql_exec "INSERT INTO hrv_readings SELECT * FROM _chaos_stale_hrv
             ON CONFLICT (event_id, ts) DO NOTHING;" >/dev/null
  psql_exec "DROP TABLE _chaos_stale_hrv;" >/dev/null
  echo ">> state now (hrv back level with hr; gold still 1 day behind until recompute):"
  show_state
  echo ">> Now RE-TRIGGER gold_recovery_score — compute rebuilds day D, dq_check goes GREEN."
  exit 0
fi

# ----------------------------------------------------------------- inject -----
D="$(psql_exec "SELECT max(date_trunc('day',ts)::date) FROM hr_readings;")"
echo ">> latest hr_readings day = ${D}; silencing HRV + removing gold for that day"

# Back up the rows we're about to delete (only if no backup already exists, so a
# double-inject doesn't clobber the original backup with an already-empty set).
BU_EXISTS="$(psql_exec "SELECT to_regclass('public._chaos_stale_hrv') IS NOT NULL;")"
if [[ "$BU_EXISTS" == "t" ]]; then
  echo ">> backup table already exists — keeping it (already injected?). Re-deleting day ${D}."
else
  psql_exec "CREATE TABLE _chaos_stale_hrv AS
             SELECT * FROM hrv_readings WHERE date_trunc('day',ts)::date = '${D}';" >/dev/null
  echo ">> backed up $(psql_exec "SELECT count(*) FROM _chaos_stale_hrv;") HRV rows for ${D}"
fi

psql_exec "DELETE FROM hrv_readings  WHERE date_trunc('day',ts)::date = '${D}';" >/dev/null
psql_exec "DELETE FROM gold_recovery WHERE day = '${D}';" >/dev/null

# Guard: if data is still flowing, HRV for day D refills and the gap vanishes.
sleep 2
REFILL="$(psql_exec "SELECT count(*) FROM hrv_readings WHERE date_trunc('day',ts)::date = '${D}';")"
if [[ "${REFILL}" -gt 0 ]]; then
  echo ">> !! WARNING: HRV for ${D} already refilled (${REFILL} rows) — the generator/router"
  echo ">>    is still running. STOP it (pkill -f generator.py; pkill -f router.py) and"
  echo ">>    re-run 'make chaos-stale', or the freshness gap will not hold."
fi

echo ">> state now (hr should lead hrv & gold):"
show_state
echo ">> Now RE-TRIGGER gold_recovery_score — compute GREEN, dq_check RED (freshness gap)."
echo ">> Restore with: make chaos-stale-restore"