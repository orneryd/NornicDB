# v1.5.3 release verification follow-up

- [x] Identify the current release run, checked head, and failing jobs.
- [x] Reproduce the differential `db.awaitIndexes()` timeout on the full Bolt auto-commit route.
- [x] Confirm the unmanaged-service build trigger with a fail-first Bolt test and a lazy-service unit test.
- [x] Reuse the existing search warming owner in index await procedures.
- [x] Regenerate and review the current-head storage allocation census for the keyed WAL recovery wrappers.
- [ ] Correct any additional current-head release check regressions without weakening assertions.
- [ ] Run focused, package, and complete local release verification on the integrated candidate.
- [ ] Open a PR against `release/v1.5.3`, report exact verification, and confirm CI after integration.
