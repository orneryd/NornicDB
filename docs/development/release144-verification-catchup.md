# v1.4.4 verification catch-up

Tracking: [NOR-313](https://linear.app/nornicdb/issue/NOR-313).
Target: `release/v1.4.4`. The draft PR tracks ongoing work; it is not merge-ready.

## Acceptance criteria

- [ ] Correct every applicable full `make verify` failure and publish exact head/base results and skips.
- [ ] Restore scalar allocation ceilings without changing thresholds or weakening contracts.
- [ ] Publish and qualify the runtime Go metadata migration, including immutable copies, native storage, replication, backup/restore and frozen payload compatibility without storage-format changes.
- [ ] Reproduce and fix Northwind query performance with search enabled and the existing memory-decay enable setting respected.
- [ ] Profile an effective 100,000-entry cache and report ingest, query, allocation, memory and graph-count evidence; retain only supported improvements.
- [ ] Reconcile the duplicate inventory as advisory and explain retained separation with concrete correctness/performance/memory evidence.
- [ ] Finish integrated verification on the committed correction head and mark the PR ready only when the criteria are met.

## Current evidence

The frozen release baseline passed applicable build/test/conformance/differential/docs checks but failed allocation checks. Local fixes restore cache-hit allocation from 640 to 496 B/op with four allocations, and focused frozen payload and immutable-copy tests pass. Those product changes remain local until their publication gate passes.

Earlier cache-size comparisons are invalid because the external harness discarded the cache setting. A corrected single-iteration diagnostic used 100,000 entries, search enabled and memory decay explicitly disabled: 6.084s ingest, with 97,440 nodes and 312,050 relationships verified. This is diagnostic evidence, not a completed performance acceptance result.
