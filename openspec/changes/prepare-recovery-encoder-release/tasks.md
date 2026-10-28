# PR #1536 / NOR-36 release readiness

- [x] Confirm assigned ticket, original acceptance evidence and current release base b48076870985d23be9e26886012d8ff50eec3a1e.
- [x] Replay the three own PR commits without their obsolete parent stack; preserve current lossless JSON and recovery-authority owners.
- [x] Review serialized values/property kinds, buffer ownership and exact interrupted-snapshot filename scope.
- [x] Reproduce interrupted cleanup failure on current release and verify the correction through startup, compaction and idle ticks.
- [x] Qualify current-release/candidate lossless strings through actual WAL/snapshot readers, including supported UTF-16 units, Unicode, escaped spellings and large integers.
- [x] Migrate imported fixtures into the existing lifetime owner; unchanged ownership and inventory guards pass.
- [x] Complete the first full uncached make run, preserving its123-package pass/two-package failure result and all five guard failures.
- [x] Reconcile lexical coverage without losing live callers; retire two unused private wrappers with all consumers migrated, and pin callback/field-held owners explicitly. Untouched guard passes3914rows/686minimummemberships.
- [x] Classify only the frame writer's five-byte serialization scratch field, regenerate census and pass all four affected census guards without exemptions/assertion changes.
- [x] Resolve all three fresh duplicate findings through canonical recovery/frame owners and one shared snapshot lifecycle. Enabled unchanged analyzer passes44changedfunctions against the newly ingested release graph.
- [x] Complete the final fresh frozen `GOFLAGS=-count=1 make test` gate:125packages pass, exit0,1575.952s, source/index fingerprints stable.
- [x] Commit/push checked source and publish exact head/base/source evidence; update NOR-36 for review. Source commit45ff18346d317cc52d3121904c80d19b8ae40f21 and PR verification comment6469174654 record completion.

Maintainer local gate is `make test`; broader verification is deferred. The first completed run is historical failing evidence, not a final pass. Scoped profiled allocation observations and original benchmark receipts retain their workload/source limits. No new storage keys, payload formats, version change or migration arm are introduced. Duplicate hook/analyzer implementations and discovery/assertion implementations remain unchanged; only reviewed field and pin metadata is updated. The installed checkout-scoped hook entry point cannot select a registered PR worktree; the identical installed staged-check library is invoked directly with that root and the existing configuration.
