# PR #1474 review — CHANGES

**Verdict: CHANGES against the requested complete no-leak contract. Three P2 findings; no P1 or P3 findings.** The copied-key-lifetime consolidation itself is sound and introduces no format change. The blockers below are pre-existing release defects, not new regressions; they are included because the review explicitly requires every transaction/public path and concurrent delete/embed to be unable to leak. The original release-red/head-green claim also does not hold: those two tests pass on the exact release base.

Head: `c26247cfc4447fe0468738103ce4bb2db45c2af4`.
Base: `24d08776cdb9cb624da1073007da437306518426` (`release/v1.4.4`).
Reviewer checkout: `wt-review-1474`; test-only overlays: `wt-review-1474-probe` and `wt-review-1474-red`.
No GitHub comments, review submissions, pushes, or production edits. All review-owned TrueNAS jobs and graph-analysis commands have finished. Review completed 18 October 2026.

## Confirmed completeness gaps

These are pre-existing release defects, reproduced on both the exact release base and the requested head. They are not regressions introduced by this five-file diff. They prevent the requested proof that every deletion/shrink path removes every separately stored embedding key.

### P2 — Transactional deletion ignores embeddings created in that transaction

Repro: begin a transaction; `CreateNode` with sixteen 1024-dimensional chunk vectors; `DeleteNode` the same ID; commit. The node is removed, but **all 16 physical 0x08 keys remain**. Creating and then shrinking the node in that transaction also leaves 16 keys.

Cause: `BadgerTransaction.CreateNode` stages embedding KVs in `pendingWrites` (`badger_transaction.go:755`), while `deleteNodeBuffered` enumerates only `scanCommittedKeysWithPrefixLocked` (`:1060`). It never discovers/cancels those staged embedding writes; commit subsequently flushes them. Copying the committed iterator key to a string in `bufferDelete` is memory-safe but does not make this enumeration complete.

Reproduction command: `bash sig/tnrun.sh go wt-review-1474-probe --allow-dirty --fresh -- ./pkg/storage -race -run '^TestSol1474TransactionCleanup/(create-delete|create-shrink)$' -count=1 -v`.

Evidence: head `i20261018130815-67744-125f`, base `i20261018130815-46744-2e88`, both exit 1. Probe: `wt-review-1474-probe/pkg/storage/sol_review_1474_test.go` (also retained on base).

### P2 — Transactional inline replacement bypasses embedding range cleanup

Repro: create/commit a node with sixteen separately stored chunks; begin a transaction; replace its vectors with `[][]float32{{2,3,5}}` using `UpdateNode`; commit. The smaller body is stored, but **all 16 old physical embedding keys remain**.

Cause: `reconcileManagedEmbeddingsLocked` returns immediately when supplied nonempty vectors differ from the previous vectors (`badger_transaction.go:959`), before the committed-prefix cleanup at `:995`. `UpdateNode` then encodes the body (`:863`) without replacing its physical embedding range. This path does not use the new cleanup owner.

Reproduction command: `bash sig/tnrun.sh go wt-review-1474-probe --allow-dirty --fresh -- ./pkg/storage -race -run '^TestSol1474TransactionCleanup/committed-shrink$' -count=1 -v`.

Evidence: head `i20261018130815-67744-125f`, base `i20261018130815-46744-2e88`, both exit 1, each retaining 16 keys.

### P2 — A sidecar committed during an open deleting transaction survives deletion

Repro: create a node with no vectors; begin a user transaction and delete it; let the embedder successfully write sixteen sidecar vectors plus metadata before the user commits; commit the deletion. Both writes succeed and **17 orphaned 0x08 keys remain**. The same interleaving with transactional inline replacement also retains 17 keys.

Cause: the deleting transaction freezes its embedding-key population at its snapshot; the sidecar does not change the node body/head, so the node conflict check does not reject the subsequent deletion. There is no range conflict/ownership transition covering keys added after the snapshot. Owned key copies solve iterator lifetime, not this publication race. This creates new orphans during ordinary concurrent user transactions and background embedding; it is distinct from the explicitly excluded repair of historical orphans.

Reproduction command: `bash sig/tnrun.sh go wt-review-1474-probe --allow-dirty --fresh -- ./pkg/storage -race -run '^TestSol1474DeleteVersusEmbed$' -count=1 -v`.

Evidence: head `i20261018130815-67744-125f`, base `i20261018130815-46744-2e88`, both exit 1 with 17 keys after successful commits. Persisted-store Cypher reproduction also FAILS with 17 orphan keys on both parsers on head `i20261018131206-68280-e756` and base `i20261018131206-7832-a19f`. All eight ordinary public-path cases in that probe pass on both sides/parsers.

## Boundary inventory

Repeatable extraction: `rg -n 'prefixEmbedding|embedding(Key|Prefix|ChunkPartKey|MetaKey)\(|buildEmbeddingChunkWriteKVs\(|replaceSeparateEmbeddingChunks\(|visitEmbeddingKeys\(|deleteEmbeddingKeysInTxn\(' pkg -g '*.go' -g '!**/*test.go'`. Full output: `sig/handoff/reviews/sol-1474-key-inventory.txt`.

Graphify: exact head in `/repos/nornic-wt-review-1474`; the initial Windows-mounted extraction was stopped after its Go import resolver spent many minutes on filesystem lookups. An identical container-local copy `/tmp/nornic-wt-review-1474` reused its parsed-file cache and completed with `--code-only --no-cluster --max-workers 2`: 52,099 extracted nodes and 1,170,761 edges. The final 418MB graph is preserved at `/graphs/nornic-wt-review-1474/graphify-out/graph.json`. Bounded `query --context call --budget 2000` results for `visitEmbeddingKeys`, `scanCommittedKeysWithPrefixLocked` and `buildEmbeddingChunkWriteKVs` are retained as `sol-1474-graph-query-owner-calls.txt`, `sol-1474-graph-query-tx.txt`, `sol-1474-graph-query-writers.txt`. They show all four new consumers, the separate transaction scanner/`bufferDelete` consumers, and the three physical chunk-writer callers. The graph contains inferred edges (including a false callback-name resolution), so source inspection and executable probes, rather than graph reachability alone, establish correctness. Non-Go parsing warnings concern a SIMD header and files with no modeled symbols; all affected Go sources were inspected directly.

- **Physical formats/writers:** `badger_helpers.go:782,793,832,869`: 0x08 chunk base keys, sharded marker/part keys, node prefix. `badger_embedding_sidecar.go:30,37`: metadata is reserved chunk index 0x7fffffff in that same family. No other dedicated embedding-key builder/writer appears in the extracted production population.
- **Direct create/upsert/separate replacement:** `badger_nodes.go:144,568,612`. The replacement owner enumerates copied keys, then writes all `buildEmbeddingChunkWriteKVs` outputs. Body and range commits remain separate, as on release. This is not a proof against concurrent deletion between those commits.
- **Direct delete/inline shrink:** `badger_edges.go:639`, `badger_nodes.go:505` call `deleteEmbeddingKeysInTxn`, then `visitEmbeddingKeys`. The prefix includes chunk markers, all parts and sidecar metadata; key-only scans pass owned copies. This portion is integrated.
- **Worker sidecars:** `badger_embedding_sidecar.go:131,146,159` uses the same visitor for old keys and writes newly built chunks plus metadata in the commit writer. Namespace/WAL/composite wrappers forward this capability (`namespaced_maintenance.go:186`, `wal_engine.go:623`, `composite_capabilities.go:495`); production callers are `nornicdb/embed_queue.go:475,1250,1466`.
- **Transactions:** `badger_transaction.go:755,959,982,995,1060` independently stage writes, retain freshness metadata, scan committed keys for invalidation and delete. `bufferDelete` owns a string copy (`:616`), so no retained iterator-buffer alias remains; the confirmed population/cleanup gaps above remain.
- **MERGE/SET/REMOVE/DELETE:** Cypher writes route through `transaction_storage_wrapper.go` to transaction `CreateNode/UpdateNode/DeleteNode`, or the direct engine APIs. Property/label invalidation enters `reconcileManagedEmbeddingsLocked`; genuinely new replacement vectors take its early return. Public probes verify ordinary source-changing MERGE/SET/REMOVE, deletion, and preservation of independent named vectors; concurrent explicit-transaction DELETE is the adversarial case.
- **Named vectors/Qdrant:** `Node.NamedEmbeddings` is serialized in the node body. The encoder externalizes only `ChunkEmbeddings` (`badger_helpers.go:1015`); Qdrant mutations update named vectors on the node (`qdrantgrpc/points_service.go:177,1595,1619`). There is no separate named-vector key family to enumerate in this layout. A node containing named vectors and separate managed chunks still needs managed-range cleanup.
- **Bulk/import:** `badger_bulk.go:19,67` runs `BadgerTransaction.CreateNode` in one transaction; `badger_edges.go:730` uses transactional deletion. `adminimport/importer.go:287,295` calls bulk creation. Thus these do not introduce an independent physical writer; they inherit transaction semantics, including the pending-key gap when composed with later mutation.
- **Replication:** `replication/storage_adapter.go:348,357,464,496` delegates update/delete/bulk operations to the engine; `RestoreSnapshot:790` uses `CreateNode`. No raw 0x08 writer there. Public probes exercise apply-delete and apply-shrink.
- **WAL/recovery/write-behind:** `wal.go:2257,2264` replays update/delete through namespaced engine APIs; undo does the corresponding inverse engine operation. `OpUpdateEmbedding` is intentionally regenerable and not replayed as a new raw writer. `badger_write_behind.go:275` replays the transaction's captured raw string-key delete/write maps; `buffered_recovery_bodies.go:153` reads staged/committed embedding records to restore typed operation bodies. Replay owns its keys, but preserves any omission from the transaction's original physical write/delete population.
- **Rollback:** ordinary rollback never flushes transaction pending maps; large-commit rollback is shared commit-writer/managed-store behavior, unchanged by the patch. Both rollback-create and rollback-delete pass the fresh adversarial probe; committed delete and delete/recreate pass too.
- **Restore/namespace clearing:** native `badger_backup.go:119,122` does exclusive `DropAll` then `Load`, so it replaces all key families together; namespace drop includes 0x08 directly (`:305`) under exclusive lifecycle control. These wholesale lifecycle owners do not retain borrowed iterator keys. Public backup/restore/delete probe verifies cleanup after restore.
- **Administrative clearing and serializer migration:** `badger_stats.go:1043` clears vectors with `UpdateNode`, thus reaches direct cleanup. `serializer_migration.go:132` converts values under existing keys with copied keys; it creates no alternate embedding range/layout. Failure streaming and serializer detection are readers.

Rule 73 assessment: the patch integrates the four raw-Badger copied-key consumers into one owner. It does **not** integrate the full requested physical embedding lifecycle across transactional staged state and concurrent publication. Treating every transaction as safe solely because `bufferDelete` copies its argument misses those causal boundaries. Corrections should cover the complete range lifecycle in the released 0x08 layout; this review makes no production change and does not request the deferred 1.5.0 format.

## Independent TrueNAS verification

- Full fresh storage race suite, forced both parsers, four standard shards: `j20261018130505-8160-fa26`, `j20261018130506-8160-733c`, `j20261018130507-8160-2353`, `j20261018130507-8160-5172`, all exit 0. **2174 passed / 0 failed / 2 existing skips of 2176 listed, per parser**, all listed tests covered. Existing opt-in skips: `TestInstalledSnapshotCombinedUpgrade` (offline installed snapshot) and `TestRecoveryGenerateOldReaderFixtures` (fixture-generation opt-in). No `-skip` flag. Command: `TNRUN_FORCE_BOTH=1 bash sig/tnrun.sh go wt-review-1474 --fresh --parser both --shards 4 -- ./pkg/storage -race -count=1 -timeout=30m`. Log `sol-1474-storage.log`; wall 16m including queueing.
- Full fresh multidb race suite, forced both parsers: `j20261018130504-66964-7397` PASS, default 7.817s / antlr 7.958s package runtimes (Go 1.26.9). Command: `TNRUN_FORCE_BOTH=1 bash sig/tnrun.sh go wt-review-1474 --fresh --parser both -- ./pkg/multidb -race -count=1 -timeout=10m`. Log `sol-1474-multidb.log`.
- Actual v1.4.3 build and bidirectional disk compatibility: `j20261018130633-80380-2d30` PASS. Fresh `go run ./release_1285_probe.go` through tnrun built the old reader/writer from exact `f2ddd27cfae3788076778a0a0d48c827df046a3b` on TrueNAS. Head-write → v1.4.3-read/shrink/delete → head-read and reverse both pass, including separate chunks, sharded vectors, sidecar vectors/metadata, transaction-created nodes and Unicode IDs. Shrink/delete leave zero keys; no 0x27/0x28 keys. Source retained at `wt-review-1474-probe/release_1285_probe.go`; output `sol-1474-compat.log`.
- Paired A/B, 10 rounds, 500ms, submitted with `--no-wait`: `a20261018130548-43608-a940` COMPLETE, all 20 side-rounds exit 0; details below.
- #1481 at `e4695ce092499ba9780cd7aab75bb546f3b3c001` merges cleanly with head into local-only `4b88e07158f0f7bd06d73483502492bfe4e2465c`. Both packages compile with race on both parsers: `i20261018130812-99024-b227`. Actual original regressions plus renamed-helper transaction lifecycle and Cypher embedding-preservation tests all pass on both parsers: `i20261018131223-93660-3acf`.
- Original deletion/shrink tests on exact release base both **PASS**, race job `i20261018130635-45580-09ce`. The requested release-red/head-green claim is therefore unsupported for those original cases; v1.4.3 already contains KeyCopy via a80749ed9.

Initial public-path probe jobs `i20261018131052-65076-8461` / `i20261018131052-46608-d545` failed at test setup due to a reviewer-created import cycle. The corrected external test package runs as `i20261018131206-68280-e756` / `i20261018131206-7832-a19f`; the failed setup jobs are not product-defect evidence.

## Disk compatibility and scope

There is **no on-disk format change in this final diff**. `badger.go`, key builders, encoders/decoders, serializers, WAL codecs, transaction format and managed storage are identical to release. Chunk keys, shard parts and sidecar metadata remain in 0x08; the independently rebuilt v1.4.3 reader/writer succeeds in both directions. Intermediate branch-history changes are not the final tree. No 0x27/0x28 format or 1.5.0 storage-version change is requested by this review. The acknowledged embedded-NUL ID/shard-key ambiguity and automatic repair of pre-existing orphan ranges remain outside this accepted scope.

## Paired performance measurement

Command: `bash sig/tnrun.sh ab wt-review-1474-base wt-review-1474 --no-wait --rounds 10 -- ./pkg/storage -bench 'Benchmark(BadgerEngine_UpdateNode_MVCC|DeleteNode_SeparateEmbeddings|UpdateNodeEmbeddingSidecar)$' -benchtime 500ms -benchmem`.

Job `a20261018130548-43608-a940`, slot 5, same physical CPUs 36–39, Go 1.26.9 / AMD EPYC 7713P. Base `7d8b274906dde3b6679614f5a60fa9cdf6a97aaa` differs from exact release base only by the identical original test/benchmark fixture; no base production change. Ten samples per side, alternating ABBA order. Raw evidence `sol-1474-ab-result.log`; numerical artifact `sol-1474-ab-summary.json`. Medians and two-sided, tie-corrected Mann–Whitney p-values use the runner's calculation.

- **Ordinary UpdateNode:** 43,953 → 46,152.5 ns/op (**+5.00%, p=0.162**); 10,472 → 10,473 B/op (+0.010%, p=0.204); **200 → 200 allocs/op** (p=1). Time spread 42.4% / 29.3% of the side's median.
- **Delete separate embeddings:** 152,104.5 → 156,393.5 ns/op (**+2.82%, p=0.273**); 171,446.5 → 169,389.5 B/op (**−1.20%, p=0.385**); 592.5 → 583 allocs/op (−1.60%, p=0.088). Time spread 47.0% / 51.3%. The previous +8.60% byte median is not reproduced.
- **Sidecar replacement:** 28,664 → 30,166.5 ns/op (**+5.24%, p=0.521**); 7,736.5 → 7,616 B/op (**−1.56%, p=0.00169**); **133 → 129 allocs/op** (−3.01%, p=0.0000159). Time spread 41.4% / 28.1%. The stable allocation saving is consistent with computing the range prefix once instead of rebuilding it in the old loop condition.

Foreign CPU activity on the assigned cores remains substantial: **median 16.34%, worst 26.47%**, all 20 sides reported. This paired run does not establish a latency regression or improvement. The positive ordinary-update latency median remains visible; the unchanged 200 allocations and effectively unchanged bytes do not identify an added retained-allocation cost. No percentage threshold is applied, and the CHANGES verdict is based on correctness rather than these positive timing medians.

Targeted 8s ordinary-update CPU profiles also finished: head `j20261018133441-84008-db57`, base `j20261018133440-55696-c90a`, both exit 0. In the head profile `visitEmbeddingKeys` accounts for **10ms flat out of 15.43s sampled CPU (0.065%)**, and 1.35s cumulative (8.75%). Most cumulative work is iterator construction (440ms) and seek/iteration (810ms), operations present in the original inline scan too. The profiles' prominent costs are Badger operations, allocation/runtime work and synchronization on both versions. These ordinary-worker profiles are diagnostic; their unpaired wall-clock benchmark figures are not additional A/B evidence. Source and reports are retained as `sol1474_profile.go`, `sol-1474-profile-head.log`, `sol-1474-profile-base.log` in this review directory.

**Cost, cause, removability:** the shared owner adds a function/callback boundary and a prefix-validity check, while retaining the existing key-only scan and owned key copies. The ordinary-update benchmark has no embedding keys, so it never invokes the per-key callback or copies a key; its existing empty range scan still runs. No additional whole-node read, persistent state, or per-update allocation is introduced. The sampled flat visitor work does not explain the measured +5.00% timing median; the remaining timing cost cannot be causally assigned under the measured interference. Removing key copies is invalid because Badger retains deletion keys, and copies already exist on release. The demonstrated sidecar allocation reduction is already realized by computing the prefix once. No demonstrated new expensive operation or removable allocation remains in this diff; further latency attribution needs better-isolated measurements. This is an uncertainty statement, not proof of zero timing cost or performance parity.

## Required response to the findings

Re-establish one complete owner for a node's physical embedding range across committed keys, transaction-staged keys and publication of concurrent writes, retaining the released 0x08 layout. Verify the complete causal unit through the retained transaction and public-path probes, rather than adding isolated fixes to the three examples. This review does not authorize or implement the deferred 1.5.0 layout or historical-orphan repair.
