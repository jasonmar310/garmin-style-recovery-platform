#!/usr/bin/env bash
# ============================================================================
# Scenario 1 — throughput surge  ->  consumer lag
# ----------------------------------------------------------------------------
# MECHANISM : produce at N x the baseline rate; the single router consumer can't
#             keep up, so unconsumed messages pile up = consumer lag.
# WATCH     : Grafana "2 · Pipeline Flow" -> "Consumer lag by topic" climbing;
#             Prometheus /alerts -> ConsumerLagHigh_* goes Inactive->Pending->Firing.
# DIAGNOSE  : lag climbs AND throughput is high (both panels up) = a real surge,
#             not backpressure. heart_rate leads (highest rate / most partitions).
# BUSINESS  : overnight HRV arrives late -> morning readiness score is stale.
# REVERT    : Ctrl-C this script; run `make route` (or a 2nd router) to drain lag.
# ============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

RATE="${1:-30}"          # multiplier over baseline (baseline = 1)
DURATION="${2:-300}"     # seconds; 0 = until Ctrl-C
DEVICES="${3:-450}"

echo ">> SURGE: rate=${RATE}x baseline, ${DEVICES} devices, ${DURATION}s"
echo ">> Make sure a router is running (make route) so lag is the visible symptom."
echo ">> Watch: Grafana Pipeline Flow -> Consumer lag by topic."
exec python simulator/generator.py \
  --devices "${DEVICES}" --rate "${RATE}" --duration "${DURATION}"