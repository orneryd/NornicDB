# Preserve search publication consistency

- [x] Reproduce refused-clear index loss and failed replacement shown ONLINE on release ce22592d2.
- [x] Reproduce loss of unrelated property vectors under wholesale reset.
- [x] Retire wholesale clear; reuse canonical per-node storage notifications and catch-up before return, including partial resets.
- [x] Qualify retained/refused/property vectors and regeneration controls.
- [x] Report absent per-index state from replacement service build outcome; no persisted layout change.
- [x] Verify old-refresh ordering against newest vectors and text with eager/lazy rebuilds.
- [x] Focused search/cypher/nornicdb checks pass under default and ANTLR.
- [x] Preserve a successful per-index population across unfinished and failed global builds, while a failed replacement with no local population stays FAILED. Public CREATE/SHOW/query and component controls pass.
- [x] Separate schema-plan eligibility from service ONLINE state; exact storage reads remain usable before startup, and warm requests finish lazy startup. Original Graphiti scan-budget and ready-source zero-allocation assertions pass unchanged.
- [x] Parent ran all three previously excluded cases directly without skips. Earlier worker full-package timeout/canceled/excluded runs are incomplete, not qualification.
- [ ] Reproduce remaining report49 workload variants if qualification does not match the original scheduling.
- [ ] Exact-head CI, race/census and broader affected-package qualification.

Parent validation: search counts use an explicit 0.9 similarity threshold for orthogonal unit-vector fixtures; the default search returns zero-similarity candidates too. No assertion is relaxed to hide that distinction. Clear retains a database lifetime ticket and catches up published refreshes before returning rather than weakening immediate completion to eventual delivery.

No new test skip/ENV gate, storage/WAL format or public Go signature change.

- [x] Reconcile relationship startup/cached budget refusal with pinned Neo4j5.26.30: FAILED procedures error; scalar reads remain complete; healthy rebuilt procedures return both records. Full Cypher package passed before final rebase (615.833s); integrated current-head rerun follows.
