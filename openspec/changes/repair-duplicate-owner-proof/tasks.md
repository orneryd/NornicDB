# Repair duplicate ownership proof

- [x] Preserve the full five-finding report against release base a9265090.
- [x] Bind actual Go function objects; reject shadowed callbacks and guessed edges.
- [x] Verify surviving callers of extracted canonical owners on current source.
- [x] Verify shared test/benchmark resource construction and execution; reject
  reassignment, shadowing, repeated state writes and extra query execution.
- [x] Preserve code-copy thresholds, forwarding-wrapper rejection and candidate scope.
- [x] Keep every accepted proof visible in the report instead of adding exclusions.
- [x] Replay all five real findings against their original stored release bodies.
- [x] Package the helper with the standalone runtime and invalidate changed proof inputs.
- [x] Finish the complete corrected release-delta sweep and real hook integration checks.
- [x] Deploy the authorized correction locally with immutable protection restored.
- [x] Refresh and independently verify the latest release graph in remote NornicDB.
- [ ] Preserve required evidence and clean only stale/failed local experiments and builds.
- [ ] Fold these additions into the next existing critical fix PR; no separate checker PR.

PR #1525 was merged by explicit maintainer instruction after its exact tree passed
all 129 packages. The temporary Git-hook bypass was command-scoped; no disabling
configuration is part of this correction. The five earlier findings are not
silently waived: current object and ownership proofs are retained as evidence.

The corrected sweep against a9265090 on merged 0a56f9ac analyzed 1,675
functions, recorded 999 ownership proofs, skipped none and returned four
further source findings. This is a completed analysis, not a clean check.
Those findings are retained in source-followup.json and must be resolved in
the next existing fix PR alongside these tooling additions. They cover three
convergence families: mutation reservation, rerank-envelope assertions (both
directions) and WAL-manifest read/write ownership. The five original findings
also passed a separate replay against their original stored bodies.
