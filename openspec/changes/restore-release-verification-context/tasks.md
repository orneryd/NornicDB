# Restore release verification context

- [x] Preserve the full-suite failures from rebased release candidate `73509f1e4`.
- [x] Fetch current release `906a4ed2c` and check open PRs for overlap.
- [x] Reproduce the WAL context-census finding on current release `906a4ed2c`: one compiled substitution, zero nil flows.
- [x] Obtain approval for context-first WAL reader/recovery APIs without compatibility wrappers.
- [x] Complete canonical caller and documentation migration.
- [x] Carry the correct operation or accepted-work context through canonical recovery diagnostics.
- [x] Verify diagnostic context, valid/torn/corrupt WAL behavior and accepted-completion lifetimes.
- [x] Pass the unchanged context census and focused affected-package/race checks.
- [ ] Track main-only docs verification fixes and numeric-owner integration separately.
- [x] Push draft PR #1766 against release/v1.4.4 with precise AC and verification evidence.
- [ ] Run final-head full make test in CI and review duplicate advisory findings.

## Follow-up after #1766 merge

- [x] Confirm release 12c2b2b24 still fails capturecensus in CI run 42552016802, job 128949582578.
- [x] Preserve the exact diagnostic: one context substitution, zero nil-context flows; implementation was not part of #1766.
- [x] Implement and verify the caller-context correction in the follow-up PR; keep census assertions blocking.

- [x] Review and fix four lifetime-owner constructor violations from release CI.
- [x] Regenerate stored-entity artifacts using the unchanged generator and review the single witness-path difference.
- [x] Parent rerun: fixture ownership + affected storage tests pass; current artifact + stored-write ordering census pass.
- [x] Public WAL context contract approved.
- [x] Finish recovery/snapshot context propagation, caller migration, and unchanged census verification.

## Context migration verification

- [x] Public diagnostic regression fails on old head `6f02a0a26` (two lost-provenance cases).
- [x] Final canonical emitter source: unchanged context census passes (113.345s); focused WAL/recovery checks pass (36.914s); focused race checks pass (7.426s).
- [x] All test packages compile with zero tests run; affected public consumers, fixture guard, stored census and tagged vet pass.
- [ ] Full make-test CI on the pushed migration head; duplicate advisory review.
