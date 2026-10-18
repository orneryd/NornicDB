# Release #1285 backport

## Parked for 1.5.0 — user decision, 18 October 2026

The user moved the complete orphan-key family to **1.5.0** and the orchestrator closed PR #1474. Do not implement the proposed old-layout release correction: release/v1.4.4 already contains the two original KeyCopy fixes via a80749ed9, and rebuilding transaction/WAL ownership there would be throwaway work. Develop 16923aef778025fab3490f6a57549763b536f3d5 already passes the 63 storage checks using #1285's new layout.

WIP branch: `wip-1.5.0-1474-orphan-key-probes` on origin, based on the tested c26247cfc release head. It preserves the test-only probes, mechanically extracted inventory, graph evidence, proposed owner design, and results. The pushed commit is recorded in `sig/handoff/park-1.5.0-1474.md` and the final parking report. The original PR branch/head remains unchanged and closed; no new release implementation is authorized.

**These probes should become 1.5.0 regression tests against the new layout**, covering transaction-staged create/delete/shrink, inline and separate replacement, concurrent publication, rollback, buffered flush/WAL recovery, and **stale large vectors after close/reopen**. The storage probes already use `embeddingPrefix` and passed on develop. The persisted public Cypher probe's raw counter is specifically 0x08: adapt it to the new key layout before treating a green 1.5.0 run as proof; preserve BEGIN / DETACH DELETE / embed / COMMIT on both parsers. Warm-cache equality must not replace the durable reopen check.

The Rule 73 inventory and proposed integrated owner below are preserved as historical analysis. The old-layout implementation plan and its pending gate are superseded by this user decision. The 1.5.0 continuation should reuse the owner already on develop and adopt the regressions; do not construct a second owner or resurrect the 1.4.4 backport. No new jobs or benchmarks were started while parking. All owned jobs are DONE; none queued or running, so no cancellation or resubmission is required. Keep all local worktrees and the local Graphify graph/copy.

Dedicated parking handoff: `sig/handoff/park-1.5.0-1474.md`.

## Current state — 18 October review response

PR #1474 is **DRAFT / CHANGES**, head `c26247cfc4447fe0468738103ce4bb2db45c2af4`, base `24d08776cdb9cb624da1073007da437306518426`. Production source and branch history have not changed in this response. The ready/completion statements in the prior-round record below are superseded. The PR body now states that the copied-key-only implementation does not fulfill the expanded contract.

This response stops before production implementation at the Rule 73 analysis gate (`scratch1012/rule73-gate.txt`: "Do not write or change code yet"; "Then STOP and reply with a short summary and the path"). The user also explicitly requires stopping before building if the work becomes much bigger than a focused owner extension. The mechanically established causal unit extends into direct atomic publication and acknowledged buffered durability, rather than only extending the copied-key iterator. See the scope assessment and proposed integrated mechanism below.

## Rule 73 family analysis

### Contract and mechanical inventory

Contract: a successful node deletion or embedding replacement removes every obsolete node-owned physical embedding record, including transaction-staged records and sidecars published during an open transaction. A rollback or failed commit preserves the previous durable vectors. A committed replacement must retain its final vectors after cache eviction, restart, buffered flush and crash recovery. Keep the released `0x08` key layout readable by v1.4.3; no `0x27`/`0x28` writer or #1436 format advance. Historical repair is outside this change.

Inventory was extracted before production changes with these exact commands, from `wt-cp144-1285` unless a path is prefixed:

```text
rg -n 'prefixEmbedding|embedding(Key|Prefix|ChunkPartKey|MetaKey)\(|buildEmbeddingChunkWriteKVs\(|replaceSeparateEmbeddingChunks\(|visitEmbeddingKeys\(|deleteEmbeddingKeysInTxn\(' pkg -g '*.go' -g '!**/*test.go'
rg -n 'prefixEmbedding|embedding(Key|Prefix|ChunkPartKey|MetaKey)\(|buildEmbeddingChunkWriteKVs\(|replaceSeparateEmbeddingChunks\(|visitEmbeddingKeys\(|deleteEmbeddingKeysInTxn\(|embeddingChanges|pendingWrites|pendingDeletes' pkg/storage -g '*.go' -g '!**/*test.go'
rg -n 'UpdateNodeEmbeddingSidecar|BulkCreateNodes|BeginTransaction|CreateNode\(|UpdateNode\(' pkg/storage/badger_bulk.go pkg/storage/namespaced.go pkg/storage/wal_engine.go pkg/storage/composite_engine.go pkg/replication/storage_adapter.go pkg/nornicdb/embed_queue.go
rg -n '^func .*([Cc]reate|[Uu]pdate|[Dd]elete|BeginTransaction)|UpdateNodeEmbeddingSidecar|tx\.CreateNode|tx\.UpdateNode|tx\.DeleteNode' pkg/cypher -g '*transaction*go' -g '!**/*test.go'
rg -n 'func .*Commit|func .*Rollback|pendingWrites|pendingDeletes|commitWriter|beginWrite|beginSchemaWrite' pkg/storage/badger_transaction.go pkg/storage/badger_commit_writer.go
docker exec -e GRAPHIFY_MAX_GRAPH_BYTES=600000000 graphify graphify query 'buildEmbeddingChunkWriteKVs' --graph /graphs/nornic-wt-review-1474/graphify-out/graph.json --budget 1200
docker exec -e GRAPHIFY_MAX_GRAPH_BYTES=600000000 graphify graphify explain '.deleteNodeBuffered()' --graph /graphs/nornic-wt-review-1474/graphify-out/graph.json --budget 1000
docker exec -e GRAPHIFY_MAX_GRAPH_BYTES=600000000 graphify graphify explain '.flushBufferedWrites()' --graph /graphs/nornic-wt-review-1474/graphify-out/graph.json --budget 1200
docker exec -e GRAPHIFY_MAX_GRAPH_BYTES=600000000 graphify graphify explain '.reconcileManagedEmbeddingsLocked()' --graph /graphs/nornic-wt-review-1474/graphify-out/graph.json --budget 1200
```

Reused the review's completed, separate local Graphify root instead of starting a duplicate writer. Its container copy `/tmp/nornic-wt-review-1474` has byte-identical `badger_transaction.go`, `badger_nodes.go`, and `badger_embedding_sidecar.go` to c26247cfc (SHA256 checked). Graph has 52,501 nodes. The broad query truncates; bounded explain output, full `rg` populations and source reads supply the remaining inventory. Graph inferred callback edges are not proof. Saved extraction: `sig/handoff/reviews/release-1285-owner-{inventory,key-paths,entry-paths}.txt`; bounded graph outputs: `release-1285-owner-graph-{flush,reconcile}.txt`. Prior review graph owner/writer queries are supporting evidence, not a substitute for the new extraction.

R = earlier review exercised it; N = newly attacked in this response; S = statically inspected supporting path, without a fresh dedicated adversarial probe.

| Member and production location | State/form/consumer | Coverage |
| --- | --- | --- |
| `badger_helpers.go:782,793,832,869,1015` | 0x08 base chunks, shard marker/parts, node prefix; encoder chooses inline/separate by serialized body size | R+N: regular/sharded/Unicode, inline and large replacements |
| `badger_embedding_sidecar.go:37,90,131,146,159` | Reserved-index metadata; chunks-only, chunks+metadata, metadata-only, replacement, source validation | R+N: sidecar and metadata-only replacement/delete/invalidate/rollback; open-tx overlap |
| `badger_embedding_cleanup.go:8,14` | Owned-key enumeration used by four raw consumers; iterator must close before commit-writer batch rotation | R: retained-key and error tests; unchanged current-head full suite from prior round |
| `badger_nodes.go:20,144,337,505,568,612` | Direct create and upsert/update, inline cleanup, separate replacement; body and separate range are two commits | R+N: direct writer versus delete/shrink at body/range boundary |
| `badger_edges.go:634,639` / `badger_nodes.go:651` | Direct per-node delete and cascade use copied-key cleanup | R+N: ordinary delete and interposed competing delete |
| `badger_transaction.go:693,752,757,762` | **Tx CreateNode stages body and chunk/part KVs in pendingWrites.** Subsequent update/delete may replace the node while those physical writes remain | R+N: create/delete, inline shrink, repeated large replacement, rollback, buffered/recovery variants |
| `badger_transaction.go:804,854,863,866` | **Tx UpdateNode stages body, discards encoder's separate flag, stages no replacement chunks.** Create-operation coalescing changes Operation.Node only | R+N: committed and newly created large replacements, durable reopen and recovery |
| `badger_transaction.go:949,959,982,995` plus `transaction_sidecar_metadata.go:15` | **Tx reconciliation stages freshness metadata**, or scans committed snapshot to stage deletes on source invalidation; explicit different vectors return before cleanup | R+N: preserve/invalidate/explicit replace, metadata-only and sidecar states; repeated freshness+delete still requires regression in implementation round |
| `badger_transaction.go:1039,1056,1060,1393` | Tx delete snapshots physical keys, stages deletes; misses pendingWrites and keys created after begin; pending create/update/delete/recreate and deferred connected deletes feed this path | R+N: staged, committed and overlap cases; S: connected-delete forwarding |
| `badger_transaction.go:604,616,662,2776,2937,2949,3161,364` | pending write/delete ownership, sync commit, multi-batch writer, rollback/close, failed commit | R+N: all ordinary transitions; injected second-batch failure preserves old range and vectors |
| `badger_write_behind.go:65,89,275` / `commit_buffer.go:44` | **Buffered ACK transfers raw staged maps** to a generation; replay applies captured deletes/writes in order; missing range knowledge remains missing | N: all transition modes on real buffered ACK/flush |
| `wal.go:244,707` / `badger_write_behind_recovery.go:41,69` / `buffered_recovery_bodies.go:15,150` | **Durable buffered payload captures same raw maps**; crash recovery hydrates operation/cache bodies from persisted+staged records then replays | N: all transition modes after crashClose/RecoverBufferedCommits; stale vectors exposed |
| `badger_bulk.go:19,49,65` / `badger_edges.go:730` | Bulk create/delete route through BadgerTransaction, including change-capture routing via inCaptureTransaction | R+N public bulk create/delete; S capture route has same tx writer |
| `cypher/transaction_storage_wrapper.go:198,223,246,252,698,737` | Explicit/implicit public transaction, namespaced and bulk forwarding; SET/REMOVE/MERGE, DETACH DELETE, connected-delete staging | R+N persisted Cypher ordinary paths and BEGIN/delete/embed/COMMIT on both parsers |
| `namespaced.go`, `composite_engine.go:277,390,414`, `wal_engine.go:582,600,623,722`, `replication/storage_adapter.go`, `nornicdb/embed_queue.go` | Namespace/routing/WAL wrappers, replication apply, background embedding writebacks delegate to the three physical writers above; no extra raw embedding writer | R+N namespace/replication paths; S composite and actual worker forwarding |
| `wal_recovery_stream.go:165,203`, `badger_backup.go:76,96,213,305`, `serializer_migration.go:132` | Snapshot create routes through BulkCreateNodes; backup Restore replaces offline store; namespace prefix drop and serializer conversion act on entire key families | R public restore/delete, prior current-head full suites; S other lifecycle operations. They are separate whole-keyspace owners, not additional transaction-staged embedding writers; no fresh adversarial drop/migration overlap claim |
| `badger_helpers.go:1218`, `badger_transaction_snapshot_reads.go:119`, `buffered_recovery_bodies.go:153`, full-node/body caches and listeners | Consumers read persisted/staged chunks and metadata; caches can hide lost durable vector updates | N crash recovery and close/reopen; named vectors stay body-owned and are preserved in public SET/REMOVE/MERGE probes |

The transaction embedding staging population is complete for the extracted physical-key constructors: CreateNode's chunk/part writes, reconciliation's freshness metadata write, reconciliation/delete's snapshot-derived deletes, then generic map transfer/flush/WAL/recovery. UpdateNode is a **missing** physical writer, not another discovered one. `OpUpdateEmbedding` audit replay intentionally regenerates embeddings rather than writing an alternate raw key path. MemoryEngine has no physical embedding key family.

### Self-attack results

Owned test-only worktrees: `wt-release-1285-owner-probe` at c26247cfc; `wt-release-1285-owner-red` at release 24d08776c; `wt-release-1285-owner-develop` at 16923aef778025fab3490f6a57549763b536f3d5. GitHub heads verified unchanged after fetch. No production changes, pushes, new benchmark jobs or full-suite reruns. All probes use public mutation APIs; raw-key inspection is the physical postcondition. Direct publication uses the existing commit-tail seam to place a second public mutation precisely between body and range commits. Persisted public Cypher uses a real on-disk store, closes it, and counts raw 0x08 keys. Storage probes use embeddingPrefix so the develop key layout is actually tested, rather than falsely passing a hardcoded 0x08 count.

Kept the reviewer's `sol_review_1474_test.go` and `sol_review_1474_public_test.go` unchanged. New probes: `release_1285_owner_probe_test.go`, `release_1285_publication_probe_test.go`, `release_1285_persisted_probe_test.go`. Copies are retained in the owned worktrees and `sig/handoff/reviews/release-1285-probes/` for adoption as regressions after the gate.

| Probe group | PR head | Release base | Develop |
| --- | --- | --- | --- |
| Reviewer probes + 33 sync/buffered/recovery transitions + 12 shard/sidecar/metadata forms + persisted Cypher | `i20261018134614-65172-3f1a` FAIL, 43 pass / 20 fail **per parser**, default and antlr | `i20261018134633-79276-1bca` same 43/20 per parser | `i20261018134632-10344-ff35` PASS, 54 storage cases; public Cypher file deliberately not copied because its raw counter is release-layout-specific |
| Direct create/update × competing delete/shrink; large second-batch rollback | `i20261018134841-10824-cc63` FAIL four publication cases; rollback PASS | `i20261018134908-96812-2244` same | `i20261018134908-13120-1bd0` PASS all five checks |
| Close/reopen after tx large replacement, newly created/committed × sync/buffered | `i20261018135157-97132-a88f` FAIL all four | `i20261018135234-8396-e297` FAIL all four | `i20261018135234-18328-bba3` PASS all four |

Commands (all through Git Bash / TrueNAS, `-race`; substitute the worktree names above):

```text
bash sig/tnrun.sh go wt-release-1285-owner-probe --parser both --allow-dirty --fresh -- ./pkg/storage ./pkg/cypher -race -run 'TestSol1474|TestRelease1285Owner' -count=1 -v
bash sig/tnrun.sh go wt-release-1285-owner-probe --allow-dirty --fresh -- ./pkg/storage -race -run 'TestRelease1285DirectPublication|TestRelease1285LargeCommitRollback' -count=1 -v
bash sig/tnrun.sh go wt-release-1285-owner-probe --allow-dirty --fresh -- ./pkg/storage -race -run TestRelease1285PersistedReplacement -count=1 -v
```

Release-red/head-red results establish that the new failures are pre-existing on release, with no false release-red/head-green claim. They are the required regression targets for the integrated implementation.

Per-member outcomes, identical on release and head for the first group:

- Sync and buffered: create/delete, create/inline, committed/inline FAIL with 16 keys; invalidate source, retain source, delete/recreate and all three rollback modes PASS. Large/large replacements initially appear to PASS through the warm node cache, but fresh close/reopen FAILS; these cached reads are not accepted as durable proof.
- Crash recovery: create/delete, create/inline and committed/inline FAIL with stale keys; both large/large replacement modes FAIL vector equality; invalidate/retain source, delete/recreate and rollback modes PASS.
- Sharded, sidecar and metadata-only: delete/invalidate/rollback PASS; explicit replacement FAILS with respectively 10, 17 and 1 obsolete keys.
- Reviewer transaction cases: create/delete, create/shrink and committed/shrink FAIL with 16 keys; delete/recreate, rollback and committed delete PASS. Sidecar publication during an open delete/shrink transaction leaves 17 keys in both cases.
- Persisted Cypher: ordinary SET, REMOVE, MERGE, DELETE, bulk, replication and restore/delete PASS (eight); BEGIN / DETACH DELETE / sidecar / COMMIT FAILS with 17 keys on both parsers.
- New direct publication probes: direct create and direct update, each overlapped by delete or inline shrink after their body commit, leave 16 orphaned keys. Multi-batch injected failure rolls back correctly and retains all original vectors and 16 keys.

Logs: `scratch-release-1285/owner-{head,red,develop}.log`, `owner-publication-{head,red,develop}.log`, `owner-persisted-{head,red,develop}.log`. Full first-parser case lists: `sig/handoff/reviews/release-1285-owner-{head,red,develop}-results.txt`.

### Cause

The original KeyCopy corrections fixed the lifetime of **enumerated key bytes**. The copied-key consolidation makes that guarantee shared for four consumers, but cannot make their key **population and publication time** complete. The earlier narrow backport explicitly left the later transaction/publication correction out; that boundary is insufficient for the now-approved complete contract.

There is no canonical node-range lifecycle owner. Transaction CreateNode owns chunk staging, UpdateNode owns body encoding but ignores separate-vector output, reconciliation owns only selected snapshot deletions, sidecar freshness owns a metadata point write, delete owns a committed-prefix scan, and commit/ACK/recovery blindly consume the resulting maps. A copied key cannot represent a key not in the old snapshot, cancel a staged write the scan never visits, or write replacement chunks an update never stages. The node cache then returns the intended operation payload even when its persisted vectors are stale; recovery/reopen exposes that mismatch.

Direct engine writes also split one logical node mutation across a body commit and a later range commit. A legitimate delete/shrink between them can win the node mutation and still be followed by stale embedding publication. Sidecar writes do not change the node body/MVCC head, so node-only snapshot validation does not detect a sidecar published after transaction begin.

### Integrated plan and scope checkpoint

The complete mechanism is a **node embedding-range replacement owned through publication**:

1. One owner records a node's final desired separate chunks/parts and metadata, or an empty range. Create/update/delete and repeated transitions replace that intent; no earlier staged chunks survive. Preserve source-policy decisions and retained sidecar metadata, but route every physical consequence through this owner. Rollback/close drops intent; failed multi-batch publication aborts it with the rest of the node mutation.
2. The commit writer applies the owned replacement as one atomic body+range publication of any size. Collect copied keys before writes can rotate a Badger batch. Direct create/update must stop committing bodies separately from ranges; node deletion, inline replacement, separate replacement and sidecar replacement all use the same range operation.
3. Resolve deletion against the current owned range while excluding competing sidecar publication, rather than treating the transaction's begin snapshot as complete. Use the existing write barrier/commit writer machinery; release coordination before listeners and retain the release quota hook/notification queue. Sidecar publication must drain acknowledged buffered writes before validation, so an older acknowledged delete cannot be followed by a new orphan sidecar.
4. Buffered ACK, ordered flush and WAL recovery must preserve the same range intent, not degrade it to an incomplete old snapshot deletion list. This is the scope-critical durability boundary: develop carries `bufferedCommit.embeddingRanges` through `WALBufferedCommitData.EmbeddingRanges` and recovery. Blindly copying that optional WAL field is not yet justified as an unchanged-format/downgrade-safe backport. A completed 0x08 design must either materialize all legacy-readable raw mutations before ACK while preserving correct ordered replay, or prove any representation addition safe with actual v1.4.3 buffered/crash-recovery readers as well as normal stores. No format version advance or #1436 dependency has been shown necessary for the 0x08 keys themselves.
5. Verification must turn every failed case above release-red/head-green, retaining the passing preservation/rollback controls. Add repeated freshness-write/delete and cross-generation buffered replacement controls. Exercise the persisted Cypher case on both parsers; force cold/persisted vector reads, not just warm cache results. Re-run actual v1.4.3 compatibility both directions, including any durable buffered representation that changes, full storage/multidb race suites on both parsers, and paired A/B via `ab --no-wait`. Cover ordinary update/scan, delete/shrink, sidecar, and buffered replacement; report every remaining cost and its cause without a percentage gate.

This is substantially larger than a focused extension of `visitEmbeddingKeys`: it crosses node mutation atomicity, transaction staging/close, sidecar admission, buffered generation ownership and durable WAL recovery (approximately ten production files, before tests). The full develop owner is a useful design reference, but also includes newer layout/provenance/reference/repair machinery that must not be transplanted into this release. **Stop before implementing this larger causal unit; request the orchestrator's scope/design check as instructed.** No claim of final implementation, green corrected head, renewed compatibility, new A/B, or readiness is made.

### Develop result

Develop `16923aef778025fab3490f6a57549763b536f3d5` already has `stageEmbeddingReplacementLocked`, `materializeEmbeddingChangesLocked`, `collectLateEmbeddingChangesLocked`, `commitEmbeddingSidecar`, direct body/range commit-writer publication, and durable `EmbeddingRanges` propagation. All 54 storage family probes, four direct-publication probes, large rollback and four close/reopen checks pass (63 checks total). No same-gap develop fix is evidenced or needed by these probes. This does not establish downgrade compatibility for develop's different layout.

### Collection and handback

All nine owned TrueNAS jobs have reached DONE and their waiting clients have exited; all local Graphify commands have finished. Six release/head runs failed the intended postconditions; three develop runs passed. No job is queued, held or running. No benchmark jobs were submitted or manipulated. Test-only overlays and artifact copies are retained; `wt-cp144-1285` remains clean at c26247cfc.

`bash sig/release-parity.sh` exited **0**: PARITY CLEAN for release 24d08776cdb9 versus develop 16923aef7780, failures=0. Open PR mappings are planned, not shipped. Log: `scratch-release-1285/owner-release-parity.log`. PR #1474 remains draft, bug, milestone v1.4.4, linked to the parent and implementation threads, and unmerged. Compatibility/full race/A/B in the prior-round record remain unchanged evidence for c26247cfc only; new runs await the approved integrated implementation.

## Prior round record (superseded readiness; evidence applies only to c26247cfc)

- PR: https://github.com/orneryd/NornicDB/pull/1474 — READY for review, not merged.
- Parent thread: 075cf8e3-9632-45e4-a74b-ecbe4e778f84 (linked; implementation thread also linked).
- Branch/worktree: cherry/1.4.4-1285-embedding-keys-r2 / wt-cp144-1285.
- Head: c26247cfc4447fe0468738103ce4bb2db45c2af4.
- Base: origin/release/v1.4.4, 24d08776c. Fetch/rebase found the existing branch up to date.
- Milestone v1.4.4, label bug, requested title and footer. Do not merge.
- Complete: paired A/B a20261018112328-105884-3ed8 finished DONE exit 0 (150 s on box). All task-owned jobs have finished; the original result client ended normally. PR is ready and unmerged.

## Design and approved scope

The user explicitly permitted fixing the original leak without changing the layout. Both v1.4.3 and the release base already include the two original KeyCopy corrections via a80749ed9. This is a root-cause-only backport: one copied-key cleanup owner and #1285's original deletion/shrink regressions, plus retained-key/shard/sidecar/error-path coverage. Final diff vs release: five files, +176/-29.

The intermediate cherry-pick commits are retained in branch history. c26247cfc restores the release implementation before applying this narrow backport, so the existing shared branch was pushed normally without force-pushing.

Canonical owner: pkg/storage/badger_embedding_cleanup.go. Consumers: deleteNodeInTxn, UpdateNode inline-shrink, replaceSeparateEmbeddingChunks, UpdateNodeEmbeddingSidecar. Transaction scans already synchronously convert ephemeral keys to owned strings in bufferDelete; they do not retain iterator slices. The shared iterator keeps the existing key-only policy. Whole-node reads, key encoding, serializers, WAL, the release's single notification queue and quota-release-before-listeners hook are identical to the release base.

This integrates the complete original copied-key lifetime boundary, not another per-consumer layout patch. No new state or format is introduced. All embedding chunks, shard parts and metadata remain on released prefix 0x08. There is no 0x27/0x28 writer or fallback read, no extra whole-node lookup/allocation, and no #1436/storage-version dependency.

Limits stated in the PR: the later develop expansion (new layout, orphan repair, provenance, atomic body/range publication, buffered range replacement) is excluded. Historical orphaned ranges with no owning node are not automatically repaired. Existing release behavior in these areas is retained. The known released-layout ambiguity between embedded-NUL node IDs and shard-part keys is also explicitly stated in the PR; this narrow backport does not fix that separate layout defect.

## Completed TrueNAS evidence

- Historical red: i20261018112508-86008-7afc, wt-release-1285-red@2eca49f0b = pre-KeyCopy 6765c4d7c + original test-only fixture. Both original regressions fail, each leaving 14 of 16 keys.
- Released v1.4.3 baseline: i20261018112329-88168-b042, wt-release-1285-v143@0971b813d = tag f2ddd27cf + same fixture. Both regressions PASS, confirming the root fix already shipped.
- Full storage race suite, forced default and antlr: PASS, 2174 passed / 0 failed / 2 existing skips per parser, all 2176 listed tests covered in four built-in shards; no -skip flag. The two existing skips are TestInstalledSnapshotCombinedUpgrade (offline installed snapshot required) and TestRecoveryGenerateOldReaderFixtures (fixture-output opt-in required). Listing i20261018120649-53332-8fc3; shards j20261018120712-53332-c0aa, j20261018120712-53332-84d8, j20261018120713-53332-d35a, j20261018120714-53332-a483, all exit 0. Command: tnrun.sh go wt-cp144-1285 --parser both --shards 4 -- ./pkg/storage -race -count=1 -timeout=30m, TNRUN_FORCE_BOTH=1. Wall ~5 min; peak RSS up to 7.13 GiB. Log scratch-release-1285/storage-shards.log.
- Full multidb race suite, forced default and antlr: j20261018120648-20852-0500 PASS (package runtime 5.877 s / 6.398 s). Log scratch-release-1285/multidb-both.log.
- Four embedding regressions also passed under race in the initial run: delete 0.04 s, inline shrink 0.06 s, read-only error 0.01 s, owned-key/shard/sidecar 0.04 s.
- gofmt i20261018112306-91204-e90b PASS, go run cmd/gofmt -l -d on all five changed Go files.
- vet i20261018112538-52056-825a PASS, pkg/storage and pkg/multidb.
- Bidirectional compatibility j20261018112434-93260-8d6d PASS. The untracked helper overlay on c26247cfc clones the TrueNAS shared object database, checks out exact v1.4.3 f2ddd27cfae3788076778a0a0d48c827df046a3b, and builds both versions on TrueNAS. Release write -> 1.4.3 read/shrink/delete -> release read; reverse direction too. Separate chunks, sharded vectors, sidecar vectors and metadata, transaction-created nodes, Unicode IDs all pass. Both binaries leave zero keys on shrink/delete. No 0x27/0x28 keys. Source scratch-release-1285/release_1285_probe.go; output compat.log.

## Execution note

Initial unsharded combined job j20261018112306-52956-b800 was owner-stopped after ~43 min, exit 143; it is NOT acceptance evidence. Its label-ID allocation fixture took 306.79 s in badger and 1802.35 s in wal before reaching traced_wal. The same fixture on unchanged release base passes alone in 2.10 s, i20261018120307-98792-bb7e. The complete suite was then run using standard built-in sharding to keep each process smaller; all tests passed. Stop receipt scratch-release-1285/stop-stalled.log confirms only our job was signalled, otherJobsUntouched=true, workerRestarted=false. No other job was held, moved, removed or restarted.

For fresh clients use C:/Program Files/Git/bin/bash.exe. WSL's new watchdog self-check currently fails (null !== 0), while Git Bash passes. No shared tooling edits here. Read-only scratch TrueNAS clients collected diagnostic logs through the existing broker and pinned download helper, with no new login or broker restart.

## Measured costs

Paired A/B `a20261018112328-105884-3ed8` completed successfully: release `24d08776c` (identical benchmark fixture commit `7d8b27490`) versus `c26247cfc`, 10 ABBA rounds per side, 100ms per benchmark, Go 1.26.9, AMD EPYC 7713P. Slot 6 used the same cores 40–43 for both sides. All 20 side-rounds exited 0. Statistics below use the runner's own analyser (two-sided Mann–Whitney U).

Base → head medians, n=10+10:

- `StreamNodes_Full`: **4,041,613.5 → 3,747,280.5 ns/op** (−7.28%, p=0.089); **1,133,648 → 1,133,625 B/op** (−0.002%, p=0.364); **20,764 → 20,764 allocs/op** (unchanged, p=0.368). Time spread: 48.1% / 49.3%.
- `BadgerEngine_UpdateNode_MVCC`: **44,144 → 46,918 ns/op** (+6.28%, p=1.000); **10,472 → 10,472 B/op** (unchanged, p=0.425); **200 → 200 allocs/op** (unchanged, p=1.000). Time spread: 21.6% / 57.8%.
- `DeleteNode_SeparateEmbeddings`: **171,109 → 161,747.5 ns/op** (−5.47%, p=0.734); **235,816.5 → 256,087.5 B/op** (+8.60%, p=0.241); **508 → 508 allocs/op** (unchanged, p=1.000). Time spread: 45.4% / 49.5%; byte spread: 43.2% / 81.3%; allocation spread: 6.5% / 4.1%.
- `UpdateNodeEmbeddingSidecar`: **27,423.5 → 25,349.5 ns/op** (−7.56%, p=0.162); **10,930.5 → 10,509.5 B/op** (−3.85%, p=0.076); **132 → 129.5 allocs/op** (−1.89%, p=0.0000473). Time spread: 47.3% / 50.9%; byte spread: 13.2% / 7.6%; allocation spread: 0% / 0.8%.

The new-layout read penalty is absent: whole-node hydration and key encoding are identical to the release base, with no extra key-family probe or per-node allocation. Sidecar replacement now computes its scan prefix once through the shared owner instead of rebuilding it on every iterator-bound check. Stable per-key copies remain necessary because Badger retains deletion keys until commit; this cost already exists on the release base.

**Measurement limit and remaining uncertainty:** the runner recorded foreign CPU activity on the assigned cores: median **19.59%**, worst **29.31%**. The higher ordinary-update latency median (+6.28%) and delete byte median (+8.60%) are reported above, not hidden. With this interference and the wide spreads, this run cannot establish a latency improvement or regression, or attribute those increases to a removable code cost. No extra whole-node read work remains to remove, and no additional retained allocation has been identified. A more isolated run would be needed for a firm timing decision; that decision remains with the maintainer. The PR is ready and no percentage threshold is being applied.


Artifacts: sig/tnrun-results/ab-a20261018112328-105884-3ed8/{base.txt,head.txt,out.txt,summary.json}; each side has 40 benchmark rows (4 benchmarks × 10 rounds). Scratch source/result: scratch-release-1285/ab-result.log, ab-result.json, ab-summary.json, ab-summary.txt, finalize-ab.mjs. The canonical numerical analysis comes from sig/tnrun-ab.mjs analyse(); summarize.ps1 only formats that JSON. Metadata confirms DONE, exit 0, 10 rounds, slot 6, CPUs 40–43, Go 1.26.9. The high reported foreign CPU share and wide spreads are stated plainly in the PR, so the timings are not presented as proof of a speedup or lack of regression. No other job was held, moved or removed, and the positive medians did not change PR readiness.
