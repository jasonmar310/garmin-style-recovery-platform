# ADR-0004: Kafka KRaft topology, RF=3 / min.insync=2

Status: Accepted — 2026-06-26

## Context
The cluster must be multi-node, observable, and easy to fail on purpose for the
anomaly chapter. Options considered: managed MSK, ZooKeeper-based Kafka, KRaft.

## Decision
Self-hosted **3-broker Kafka in KRaft combined mode** (broker+controller),
on a single EC2 via Docker Compose. Topics default to RF=3, min.insync.replicas=2.

## Consequences
- (+) KRaft removes the ZooKeeper component (one less moving part, current best
      practice).
- (+) RF=3 + min.insync=2: killing one broker keeps the partition writable
      (2 in-sync replicas remain); killing a second blocks writes — a clean,
      explainable availability/consistency boundary to demo.
- (+) Self-hosted exposes JMX internals (under-replicated partitions, ISR,
      controller) that managed MSK hides — essential for metric-based diagnosis.
- (+) `docker kill` is a clean, reproducible node-failure trigger.
- (−) We own broker ops; acceptable for a take-home, and a slide maps the path
      to MSK/EKS for production.
- Trade-off: combined mode (broker=controller) is simpler but co-locates control
      and data planes; fine at 3 nodes, noted as a production divergence.
