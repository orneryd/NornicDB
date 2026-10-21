- [x] Inspect #1406's current implementation, comments and approved contract.
- [x] Clean inactive task files and clean worktrees; preserve dirty trees and branches.
- [x] Capture the original PR's write-path CPU profile and scheduler microbenchmark.
- [x] Compare the original PR against the recorded develop baseline.
- [x] Optimize measured publication bookkeeping; revert unproven experiments.
- [x] Verify public publication matrices, disjoint execution, unique-value ordering,
      quota-admission ordering, listener reentrancy and the complete boundary.
- [x] Run fresh storage package and focused race checks; attempt applicable make verification.
      Package passed (150.859 s), focused race passed (59.792 s). Both make
      commands stop in verify-selftest because the live hook requires an ingested
      graph for its fresh plan repository. Downstream verifier checks remain unexecuted.
- [ ] Resolve verifier fixture ingestion and complete downstream required checks.
- [x] Record paired workload-specific latency, throughput and allocation evidence.
- [x] Commit validated changes and update the PR with evidence and limitations.
      Source commit 5874f2db6; benchmark/verification comment:
      https://github.com/orneryd/NornicDB/pull/1406#issuecomment-6252598000
