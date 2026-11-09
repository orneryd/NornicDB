# Restore release verification context

- [x] Preserve the full-suite failures from rebased release candidate `73509f1e4`.
- [x] Fetch current release `906a4ed2c` and check open PRs for overlap.
- [ ] Reproduce the WAL context-census finding on the current release.
- [ ] Establish the WAL reader/recovery public Go contract and complete caller population.
- [ ] Carry the correct operation or accepted-work context through canonical recovery diagnostics.
- [ ] Verify diagnostic context, valid/torn/corrupt WAL behavior and accepted-completion lifetimes.
- [ ] Pass the unchanged context census and focused affected-package/race checks.
- [ ] Track main-only docs verification fixes and numeric-owner integration separately.
- [ ] Push a draft PR against release/v1.4.4 with precise AC and verification evidence.
- [ ] Run final-head full make test in CI and review duplicate advisory findings.
