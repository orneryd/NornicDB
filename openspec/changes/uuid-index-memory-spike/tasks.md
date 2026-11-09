# Spike work and implementation gates

- [x] Create NOR-53 in v1.4.4 and assign Tim.
- [x] Trace canonical ID dictionary, property-value index and property-name dictionary.
- [x] Measure the real current owners with a million deterministic identifiers.
- [x] Compare compact representations, preserving exact supported string IDs.
- [x] Exercise sparse numeric IDs, deletions/reinsertion and bidirectional correctness.
- [x] Run compatibility/collision/concurrency experiments with the race detector.
- [x] Complete the final repeated results and architecture recommendation.
- [ ] Review the selected design and performance tradeoffs with Tim.
- [ ] Implement only in the canonical owner after the design decision.
- [ ] Verify integrated real-engine writes, rollback, concurrent publication,
  reopening, repair, backup/restore and all consumers at the final implementation.
- [ ] Demonstrate end-to-end large-graph memory reduction and required performance
  baselines before declaring production readiness.
