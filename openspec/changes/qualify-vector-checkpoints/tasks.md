# Qualify vector checkpoints (NOR-353 / NOR-351)

Base: release/v1.4.4 93db08990. Historical candidate preserved in vector-checkpoint-v144 worktree.

- [x] Recover candidate on isolated current-release branch.
- [x] Reproduce candidate live-update restart scan regression.
- [x] Reproduce stale/corrupt source and compaction defects on unchanged release.
- [x] Apply runtime-first contract: rebuild uncertified live-update checkpoints once, then reuse unchanged certified snapshots; preserve all exact results.
- [x] Verify process-crash publication at all eight protocol boundaries, reordered and empty fixtures; power-loss semantics remain source-reviewed.
- [ ] Verify valid restart, mutations, restore, unsupported wrappers and I/O failures.
- [ ] Measure source proof cost and affected allocations.
- [ ] Independently review integrated source and duplication.
- [ ] Push candidate PR, complete exact-head make test CI, update Linear.

No authoritative Badger key/payload migration, no dogfood mutation, no private NOR-56 disclosure.

## Current findings

- Rejected pre-save current-stamp certification: does not prove delivery of already committed mutations. Proposal preserved outside source tree.
- Native suppression direct callbacks bypassed source invalidation; storage owner correction underway.
- Rejected vector sources still reused stale BM25; public undelivered-source regression fails, correction delegated.
- Operator selected best steady-state allocations/memory/ops/sec; conservative rebuild avoids a new durable acknowledgement ledger.
