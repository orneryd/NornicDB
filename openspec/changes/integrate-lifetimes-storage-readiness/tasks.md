# Combined release/v1.4.4 readiness: #1693

The approved dependency order is current #1684 terminal-outcome prerequisite,
#1636 request/service/storage lifetimes, #1693 storage foundations, then #1667
label-index readiness and remaining preparation. Preserve each author's work;
port into current owners rather than restoring obsolete implementations.

- [x] Preserve original #1636, #1693 and #1667 heads and the separate #1667 prototype.
- [x] Rebase #1636 S1–S4 onto current #1684 (ccc5337ed), preserving release APOC I/O policy.
- [x] Rebase #1693 foundation commits onto the lifetime stack without merge commits.
- [x] Correct the remaining long-lived read-view panic release to use WorkLifetime.
- [ ] Verify lifetime shutdown contracts and correct obsolete gate schedules without weakening completion, rollback or admission assertions.
- [ ] Validate foundation read admission, concurrent-limit rejection and shared envelope consumers on the combined source.
- [ ] Reconcile compiled source/control populations with the integrated implementation; fix census portability and stale artifacts.
- [ ] Review #1667's own acceptance criteria against the combined foundations and preserve all still-required author work.
- [ ] Integrate proven label readiness/LowMemory fixes, retaining existing marker bytes and storage layout.
- [ ] Resolve all failures exposed by integration, including existing release regressions.
- [ ] Measure affected ready/building reads and preparation workloads; reject regressions and unsupported prototype behavior.
- [ ] Run full make test on the frozen combined source and fix every failure.
- [ ] Update PR #1693 and linked Linear descriptions with implementation, exact evidence and outstanding qualification.
- [ ] Publish the qualified head with remote-head protection; fold superseded PRs only after inclusion is proved.
- [ ] Clean task-owned obsolete worktrees/artifacts/cache with the cleanup skill, preserving active work and required evidence.

The user deferred the full make verify sweep and requested no new duplicate-checker
run this round. This does not assert a clean duplicate inventory. Remote code graph
refresh at de70c0e83 failed during relationship import and is incomplete; current
source is authoritative. No release tag, remote merge or storage migration is claimed.

## Current focused results (not final readiness)

- [x] Reproduce and fix native60k LowMemory transaction-size failure through bounded retries; combined completion/readiness/restart controls pass.
- [x] Reproduce incomplete paused-rebuild public reads on both parsers; shared candidate owner returns complete namespace-isolated rows.
- [x] Reproduce and fix loss of an older ready transaction's label index during forced rebuild using existing versioned metadata writes.
- [x] Confirm LowMemory/public/snapshot controls with focused race checks (96.864s before later cursor work).
- [x] Reproduce ready-ID streaming returning success after real cancellation; carry context into private cursor and verify first/final-delivery cancellation with focused race controls (3.821s).
- [x] Correct lifetime fixtures to join actual accepted work, use constructor/enrollment paths, and retain repeated error outcomes; canonical plugin/gRPC controls pass13.761s.
- [x] Correct the HTTP MERGE fault fixture to take native commit ownership before rejection; no publication and exactly-one-node healthy retry pass2.087s; direct classifier controls pass1.452s.
- [x] Migrate exported index/pending-merge context/error APIs (explicitly approved). Preserve original50k failure (all candidates read,229ms after cancellation); focused corrected run stops in14.708µs with0 candidate reads. Final frozen suite remains pending.
- [x] Integrate and independently run layered public continuation/retry plus scoring/reader-release component controls; final frozen full suite remains pending. No old unreachable probe is claimed as a successful injection.
- [ ] Complete namespace-retirement/body/property binding correction and census/cost qualification.

Current proposals, original failures, actual commands, source patches/new-file archive
and per-file hashes are retained under the task evidence directory
`~/.local/share/nornicdb-pr-readiness/pr144-storage-integration`. No combined head has
been pushed. The normal full make test gate uses30m per package; the earlier10m
server diagnostic was incomplete, not a full make test failure or pass. A separate
agent's release verification remains active, so timing qualification and shared
Go-cache cleanup must wait for an appropriate idle window. DeepSeek's DROP review
failed with malformed tool JSON and has no accepted findings; parent review remains.

Both contract decisions are now approved: canonical context-first Go index APIs with error propagation, and layered public continuation plus component scoring cancellation proof. DeepSeek implementation tasks run in separate registered worktrees; parent owns storage retirement architecture, integration, verification and publication.

Current index and unique API migration builds; focused50K cancellation, pending/copy/order/lock-admission controls pass. Additional metadata-discovery and composite-key/digest exported context contracts are approved; two bounded DeepSeek migration jobs are underway. Current #1684 moved toccc5337ed; restack and all final evidence must include that current prerequisite.

- [x] Reproduce composite-key delimiter ambiguity through exact lookup and native bulk node-key creation; frame separate values through the existing digest owner. Focused integrated controls pass1.671s; persisted temporal keys remain unchanged.
- [x] Separate generic cancellable lock admission from publication-hold observation; original zero-allocation budget and observer/cancellation controls pass1.231s.
- [ ] Integrate current storage/capture census proof changes and regenerate source-pinned artifacts; filteredJoin error-propagation control remains an explicit failure being corrected.
