# Temporal backfill write ownership

- [x] Preserve the common CI trace for temporal history/current writes on all five candidates.
- [x] Trace staged and physical constructors and actual writer selection; do not infer a runtime write from an overapproximated call path.
- [x] Isolate the follow-up from the five product branches; retain the shared typed-begin fixture migrations needed to compile the release baseline.
- [x] Confirm all five product candidates pass every non-shared CI check before opening this follow-up PR.
- [x] Separate pure temporal entry derivation from physical and staged write application without duplicated key/current-pointer logic or fabricated admission.
- [x] Preserve key/value bytes, current-version selection, cancellation, rollback and error behavior with focused and affected-package checks.
- [x] Measure representative staged/physical backfill allocations and CPU cost against the unchanged source; reject extra per-node buffering or unexplained regressions.
- [ ] Review source-backed duplicate/reuse dispositions; obtain explicit approval before any analyzer or reviewed-inventory edit if required.
- [ ] Open the scoped release PR and dispatch CI on its exact pushed head.
- [ ] Verify the census and rejection controls in CI; report incomplete or failed gates honestly.

Local whole-storage run was canceled because it includes repository-wide compiled audits; it is not a pass. Those heavy checks remain assigned to CI. Parent focused 2.755s, race 8.836s and ANTLR public controls 1.874s pass. A canceled-after-discovery iterator control failed before its checkpoint was corrected.
