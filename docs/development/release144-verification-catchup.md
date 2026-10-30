# v1.4.4 verification catch-up

Scope: all remaining failures from full `make verify` on `release/v1.4.4`, plus the entire nine-finding duplicate investigation from PR #1638. This includes baseline and newly exposed integrated regressions; allocation and Northwind work do not limit the scope.
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

## Corrections now under integrated verification

| Area | Correction | Focused outcome and limit |
|---|---|---|
| Scalar execution | Reuse RETURN plans and immutable parameter rows, avoid unchanged source windows and temporary command-word slices, retain the statement clock directly | Internal approximately 995 B/op and 14 allocations; public approximately 2,113 B/op and 32 allocations, below the original released-main ceilings. Timing samples were not isolated performance qualification. |
| Node runtime metadata | Share immutable copy-on-write metadata, preserve embedding identity, and bound reuse of metadata-only namespace records | Native cache-hit 496 B/op and four allocations; original UNWIND candidate-allocation assertion passes unchanged; focused race and frozen native/transport/gob payload tests pass. |
| Historical replication snapshot | Decode the historical exported Go schema before constructing the current runtime representation | Original V4 snapshot metadata assertions pass; no new migration arm or stored-format change. |
| Resource lifetime | Use existing fixture owners for the composite label test and newly merged checkpoint test; add the interactive suite boundary | Fixture ownership and checkpoint reopen controls pass after rebase. |
| Northwind harness | Supply an explicit per-run config instead of inheriting the operator's home config | Controlled 100,000-entry runs have matching result hashes. With decay disabled and 30 measured iterations, most queries recover approximately 0.13–0.21 ms. This does not qualify current-source ingest performance or erase decay-on costs. |

The branch was rebased onto release commit
`f0c932b902b4112114eaeb1fd8a160a6b2bb05cc`; no merge commit was introduced.
Full rebased verification and the current-source workload comparison remain the
publication/readiness gates. The earlier failed integrated run is retained as
failure evidence, not reported as a pass.
