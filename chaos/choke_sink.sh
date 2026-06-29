#!/usr/bin/env bash
# ============================================================================
# Scenario 3 — downstream backpressure  ->  router stalls, lag backs up
# ----------------------------------------------------------------------------
# MECHANISM : hold an ACCESS EXCLUSIVE lock on hr_readings for N seconds, so the
#             router's INSERTs block. Chosen over `docker pause` because it is
#             controllable and AUTO-RELEASES (pause is hard to recover cleanly).
# WATCH     : router log "flushed ... in <big>ms" (flush latency spikes);
#             Grafana lag climbs (router can't commit offsets while blocked);
#             "3 · Data Stores" -> active connections tick up, commit rate dips.
# DIAGNOSE  : SAME lag symptom as a surge (scenario 1), DIFFERENT root cause.
#             Tell them apart with the THROUGHPUT panel: here throughput is FLAT
#             (no surge) yet lag rises -> the bottleneck is downstream, not load.
#             Failure propagates UPSTREAM: slow hot sink -> stalled consumer ->
#             Kafka lag.
# BUSINESS  : a slow database silently stalls the whole pipeline; readiness data
#             ages even though traffic is normal.
# REVERT    : auto-releases after the hold; nothing to undo.
# ============================================================================
set -euo pipefail

HOLD="${1:-60}"          # seconds to hold the lock
echo ">> CHOKE: locking hr_readings ACCESS EXCLUSIVE for ${HOLD}s — router INSERTs will block."
echo ">> Watch router flush latency + Grafana lag (throughput stays flat — that's the tell)."

docker exec -e PGPASSWORD="${TIMESCALE_PASSWORD:-}" timescaledb \
  psql -U "${TIMESCALE_USER:-ian}" -d "${TIMESCALE_DB:-telemetry}" -c \
  "BEGIN; LOCK TABLE hr_readings IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(${HOLD}); COMMIT;"

echo ">> Lock released. Router should drain the backlog; flush latency + lag recover."