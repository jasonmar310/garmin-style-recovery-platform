#!/usr/bin/env bash
# ============================================================================
# Scenario 2 — broker failure  ->  under-replicated partitions
# ----------------------------------------------------------------------------
# MECHANISM : docker kill one broker (abrupt, like a node crash).
# WATCH     : Grafana "1 · Cluster Health" -> Brokers online 3->2,
#             Under-replicated partitions > 0, ISR per topic drops by 1;
#             Prometheus /alerts -> KafkaBrokerDown + UnderReplicatedPartitions Firing.
# DIAGNOSE  : RF=3 + min.insync=2 means killing ONE broker keeps partitions
#             WRITABLE (2 in-sync replicas remain). under-replicated>0 signals
#             reduced redundancy, NOT an outage. Killing a 2nd would block writes
#             (NOT_ENOUGH_REPLICAS) — the designed availability boundary.
# BUSINESS  : single node loss is survived with no data loss; durability margin
#             shrinks until the broker rejoins.
# REVERT    : ./chaos/kill_broker.sh --restore [broker]
# ============================================================================
set -euo pipefail

if [[ "${1:-}" == "--restore" ]]; then
  BROKER="${2:-kafka2}"
  echo ">> RESTORE: starting ${BROKER}"
  docker start "${BROKER}"
  echo ">> ISR should re-expand and under-replicated return to 0 in ~30-60s."
  exit 0
fi

BROKER="${1:-kafka2}"
echo ">> KILL: docker kill ${BROKER}  (abrupt broker crash)"
docker kill "${BROKER}"
echo ">> Expect: Brokers online 3->2, under-replicated partitions spike."
echo ">> Restore with:  ./chaos/kill_broker.sh --restore ${BROKER}"