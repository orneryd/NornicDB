# Release integration verification follow-up

- [x] Preserve typed Fabric record imports and retire superseded private rewrite paths.
- [x] Reproduce the HTTP connection-metadata mismatch with an idle accepted peer; preserve all populated-field and non-empty assertions for established test-driver connections.
- [x] Verify the connection case on HTTP and Bolt in both transaction modes, including a missing-agent negative control.
- [x] Give cold-cache tests independent identities; preserve cold/warm and cancellation assertions across repeated runs.
- [x] Review the dispatch return and update only its retired/current source-inventory receipts without changing analysis rules.
- [x] Assert expected fault-injected WAL close errors through the existing strict fixture owner.
- [x] Restore canonical ambiguous-aggregation diagnostics for RETURN and WITH through both parsers.
- [x] Reproduce and correct zero-argument normalize panics through public Execute and Bolt; retain canonical arity diagnosis.
- [x] Correct remaining fault-injection cleanup fixtures without suppressing normal close failures.
- [x] Resolve write-behind stress derived-count underflow at its owning cause; fixed seed passes ten repetitions and reserved-deletion controls pass with race detection.
- [x] Preserve canonical CALL-conclusion diagnostics through Fabric; integrated focused checks pass with both parsers.
- [ ] Re-run interrupted Cypher coverage; distinguish runner termination from an actual test failure.
- [ ] Verify the integrated candidate with affected-package, conformance and differential checks and exact-head full CI.
- [x] Record dispositions for all three printed entries in the 2ea32da66 advisory report; no duplicates reported, stale graph baseline documented.
- [ ] Review the final-head advisory report and disposition any new entries.
- [ ] Clean task-owned artifacts while preserving active #1740 work and required evidence.
