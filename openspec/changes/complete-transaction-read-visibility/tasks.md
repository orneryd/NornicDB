# NOR-160 current release follow-up

Base: release/v1.5.0 at c4ac624b88a3814c4d12d4014ddfe6cb3a95c0a5.

- [x] Read Linear description/discussion and check existing PR coverage.
- [x] Start from the updated release in an isolated worktree.
- [x] Preserve fail-first public label-scan versus point-read visibility reproduction.
- [x] Correct the canonical visibility owner, preserving pinned data snapshots and stored-read bypass.
- [x] Verify cold/cached/projected/pending public consumers and cancellation/retry.
- [x] Qualify pinned snapshot point reads against the logical owner and matched public indexed reads.
- [x] Consolidate all three advisory reuse candidates and verify both current-source owner calls.
- [x] Reconcile performance follow-ups with #1771 to avoid conflicting planner work.
- [ ] Run focused affected-package and race checks; review duplicate findings.
- [ ] Push a release-targeted PR with explicit acceptance evidence and request full make-test CI.
- [ ] Resolve all failures, update Linear and clean task-owned artifacts after readiness.
