# C24 / N145-2: repeated-upload status query cost

- [x] Reconcile report C24 with NOR-118 and start an isolated release/v1.5.3 worktree.
- [x] Restore the recorded public-query fail-first test and start a current-head CPU/heap baseline.
- [x] Confirm the current-head failure, correct rows, same-result control, and dominant CPU/heap owners.
- [x] Select a narrow correction that preserves aggregate subscript errors and Cypher row multiplicity.
- [x] Verify the fail-first fixture, parser variants, invalid subscripts, and public query results for the validation correction.
- [x] Compare baseline/current latency, allocations, and peak heap on matched fixtures; document the remaining acceptance gap.
- [x] Defer the new ordered-aggregation optimizer to child N145-34 with full acceptance criteria and test matrix.
- [x] Run focused and affected-package tests, manually review reuse candidates, and leave the local duplicate checker to CI.
- [x] Open the partial release/v1.5.3 PR with exact head/base verification and link Plane.
