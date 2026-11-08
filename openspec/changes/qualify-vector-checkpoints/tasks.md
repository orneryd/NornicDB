# Qualify vector checkpoints (NOR-353 / NOR-351 / NOR-352)

Base: release/v1.4.4 9dcf1bba1. Historical candidate preserved separately.

- [x] Recover candidate on isolated current-release branch.
- [x] Reproduce stale/corrupt source and compaction defects on unchanged release.
- [x] Apply runtime-first contract: rebuild uncertified live-update checkpoints once, then reuse unchanged certified snapshots; preserve exact results.
- [x] Verify process-crash publication at all eight protocol boundaries, reordered and empty fixtures; power-loss semantics remain source-reviewed.
- [x] Verify valid restart, source changes, restore, unsupported wrappers and explicit disk failures with focused checks.
- [x] Measure mutation allocations and source-validation costs separately.
- [x] Review duplicate advisory and consolidate shared namespace capture and compaction ownership.
- [x] Correct publication retry error masking/panic; verify recovery through real filesystem failure.
- [x] Verify metadata wire round trip and interrupted cross-artifact publication recovery.
- [x] Complete temporal schema interleaving correction and repeated unchanged-schema restart controls.
- [x] Complete storage-census inventory correction and its rejection controls.
- [x] Measure checkpoint Save costs separately from normal mutation throughput.
- [ ] Push final corrections and complete exact-head make-test CI.
- [ ] Update PR/Linear evidence, mark ready, clean task-owned workspace resources.

No authoritative Badger key/payload migration, per-write source marker, acknowledgement ledger, dogfood mutation or checker-policy change.

## Verification boundary

Full local search passed at c588e1328; storage, multidb and replication packages passed. The earlier affected-package run contained the now-corrected snapshot-release search failure and is not an all-pass result. Final review corrections require their own checks and a new CI head. CI storage census found a raw suppression-counter access and unclassified release MVCC reservation fields; these are being corrected without weakening the audit.

Population certificates qualify restart admission. Failed final cross-artifact publication may replace a derived vector tuple before other artifacts; the mismatched completion certificate forces a rebuild rather than admitting mixed results. Process-exit tests do not emulate hardware power loss.

Final focused integration: checkpoint race tests pass11.848s; frozen-source plugin rerun pass7.416s. Temporal schema/restart controls pass; storage census records and exact rejection controls pass. Existing approved post-commit context record was repinned after a five-line source shift; runtime phase behavior unchanged and source-drift controls pass. Full exact-head CI remains pending.
