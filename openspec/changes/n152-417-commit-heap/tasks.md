# N152-417: commit memory with unchanged chunk vectors

- [x] Reconcile production report S28 with earlier WAL and vector-image fixes; retain one current Plane owner.
- [x] Start an isolated release worktree and inspect the recorded `TestCommitSpike` fixture.
- [x] Measure the current release baseline at 86, 172, and 258 nodes with matched heap and allocation profiles.
- [x] Identify the remaining live copy owner and confirm a narrow ownership-safe correction.
- [x] Add a fail-first regression for unchanged vector images and the affected commit consumer.
- [x] Correct the MVCC archive copy owner while preserving WAL, MVCC, rollback, and CDC payload paths.
- [x] Compare baseline/current latency, allocated bytes, and peak live heap on the same fixture.
- [x] Run focused and affected-package checks; review duplicate candidates manually and leave the local checker to CI.
- [ ] Open one PR against `release/v1.5.3`, post exact head/base verification, and link Plane.
