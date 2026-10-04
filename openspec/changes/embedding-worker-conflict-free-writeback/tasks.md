# Embedding Worker Writeback: Conflict-Free, No Node-Record Transaction

## Motivation

The embedding worker is the database's most important background writer
(`docs/compliance/background-workers-mvcc-audit-guide.md`). Today, when a
worker finishes generating managed embeddings it persists them by rewriting
the **node record** through the normal MVCC update path. Every worker
writeback therefore:

- opens a full node-update transaction (`pkg/storage/badger_nodes.go:543`
  `UpdateNodeEmbedding` → `withUpdate`),
- allocates a **new MVCC version** (`allocateMVCCVersion`) and archives the
  superseded body (`archiveNodeOnUpdateInTxn`) — a "whole new transaction" in
  the MVCC history for derived data,
- rewrites the same node key that a concurrent business write touches, so
  the two can contend/conflict at the Badger transaction level,
- bumps the node's `UpdatedAt` even though no business field changed.

None of that is necessary: embeddings are regenerable derived data. The write
should land in the **dedicated embedding key space** that already exists for
large embeddings (`embeddingPrefix` chunk keys, merged on read by
`loadNodeEmbeddings`, replaced atomically per #703), so a worker write can
never conflict with incoming changes and never creates a node MVCC version.

**No storage-format change:** the embedding metadata record is stored as an
ordinary chunk key in the existing `prefixEmbedding` (0x08) key space at a
reserved chunk index. No new key prefix is introduced, so the v2→v3
migration needs no new arm, and the existing cleanup/replacement of that
prefix (business writes, legacy embedding updates) removes the metadata
record together with the chunk vectors — no node write path is modified.

Audit evidence stays as documented: WAL records continue to note embedding
updates as the distinct `OpUpdateEmbedding` operation
(`pkg/storage/wal_engine.go:591`), worker operational logs and queue stats
remain, and the compliance guide is updated to describe the new write model.

Re-notification behavior is preserved: any change to a node after embedding
still triggers a mutation notification that enqueues it again
(`pkg/nornicdb/db.go:1162`, guarded by the 30s `recentlyProcessed` window in
`pkg/nornicdb/embed_queue.go`).

## Current behavior (grounded)

- Worker flow: `processClaimedNode` → `persistEmbeddedNode`
  (`pkg/nornicdb/embed_queue.go:1152`) calls
  `storage.UpdateNodeEmbedding(node)` after re-reading the node
  (`GetNode` → `existingNode`) and merging `ChunkEmbeddings`/`EmbedMeta`.
- Badger writeback (`pkg/storage/badger_nodes.go:543`): inside `withUpdate`
  it allocates an MVCC version, archives the prior body, decodes the existing
  node, overwrites `ChunkEmbeddings`, `EmbedMeta` and `UpdatedAt`, re-encodes
  and `Set`s the **node body key**, writes the MVCC head, and removes the
  pending-embeddings index key. Small embeddings are stored inline in the
  body; only oversized ones go to separate chunk keys
  (`replaceSeparateEmbeddingChunks`), and even then the body is still
  rewritten for metadata.
- Read side already supports separate storage: `decodeNodeWithEmbeddings` /
  `loadNodeEmbeddings` (`pkg/storage/badger_helpers.go:1101`) hydrate chunk
  vectors from the embedding key space; `EmbeddingsStoredSeparately` is the
  body-resident flag.
- Worker updates also pass through `AsyncEngine.UpdateNodeEmbedding`
  (`pkg/storage/async_engine.go:1034`), which stages the whole node in the
  async cache (body included), and `NamespacedEngine.UpdateNodeEmbedding`
  (`pkg/storage/namespaced_maintenance.go:176`).

## Target behavior

The worker's writeback becomes an **embedding-only write**:

1. It writes only to the embedding key space — chunk vectors and a per-node
   embedding metadata record — and never rewrites the node body key.
2. It allocates no MVCC version, writes no MVCC head and bumps no
   `UpdatedAt` on the node record; the embedding timestamp lives in the
   embedding metadata record.
3. Because business keys are untouched, worker writes cannot conflict with
   concurrent business writes; worker-vs-worker races resolve by per-key
   last-writer-wins, and readers see either the old or the new embedding
   state atomically (the #703 guarantee extended to metadata).
4. WAL `OpUpdateEmbedding` logging, worker logs and queue statistics are
   unchanged; `docs/compliance/background-workers-mvcc-audit-guide.md` and
   `docs/skills/managed-embeddings.skill.md` are updated to match.

## Tasks

- [ ] 1. Storage layout: the worker's embedding metadata record
  (`EmbedMeta`, model/dimensions, space, timestamps, `chunk_count`) is an
  ordinary chunk key at a reserved chunk index inside the existing
  `prefixEmbedding` key space (`embeddingKey(nodeID, reservedIndex)`); chunk
  vectors stay at indexes 0..`chunk_count`-1. No new prefix, no migration
  arm, no changes to node write paths: existing `UpdateNode` inline cleanup
  and `replaceSeparateEmbeddingChunks` already delete every key under the
  prefix, so a business write invalidates worker metadata for free.
- [x] 1.1 Keep inline storage for statements that embed in the same
  transaction (`WITH EMBEDDING`, `pkg/cypher`) so their semantics are
  unchanged; only the asynchronous worker path moves to embedding-only
  writes. The body flag path remains the fallback when no metadata record
  exists.
  - `embeddingMetaKey` = `embeddingKey(nodeID, reservedEmbeddingMetaChunkIndex
    = 0x7FFFFFFF)` in `pkg/storage/badger_embedding_sidecar.go`; body-flag
    hydration unchanged as the fallback (`badger_helpers.go`).
- [x] 2. Write path: new storage interface
  `EmbeddingSidecarUpdater.UpdateNodeEmbeddingSidecar(node)` on Badger (and
  forwarded by WAL/Async/Namespaced/Composite) that (a) verifies node
  existence without creating, (b) replaces chunk vectors atomically in one
  `withUpdateUnits` commit (#703) together with (c) the metadata record at
  the reserved chunk index and (d) the pending-embeddings index removal —
  never touching `nodeKey`, the MVCC head, or `UpdatedAt`. WAL still logs
  `OpUpdateEmbedding`.
- [x] 3. Read path: `loadNodeEmbeddings` merges the metadata record (sidecar
  takes precedence, then the body-flag path); the content stamp
  (`__nornic_content_updated_at` = body `UpdatedAt`) makes stale records
  invisible. `chunk_count` is canonicalized to `int` on read so callers see
  the same type the legacy cached writeback left in memory.
- [x] 4. Worker: `persistEmbeddedNode` no longer re-reads + re-encodes the
  node body; `markNodeEmbeddingFailed` and `RetryParkedEmbeddingFailures`
  write only embedding state. `UpdatedAt` is no longer bumped by workers.
- [x] 5. Async/namespaced layers: `AsyncEngine.UpdateNodeEmbeddingSidecar`
  passes through without staging a whole-node cache entry or bumping pending
  writes; `NamespacedEngine` prefixes and forwards; `CompositeEngine` routes
  to the constituent holding the node; parked-failure scans stream the
  metadata records (`StreamParkedEmbeddingFailures`) without decoding node
  bodies.
- [x] 6. Deletion hygiene without storage changes: node deletion performs no
  new cleanup; the metadata record is unreachable once the node is gone, and
  the content stamp makes a recreated same-ID node ignore the stale record
  (`TestEmbeddingSidecar_DeleteThenRecreateIgnoresStaleMeta`).
- [x] 7. Re-notification: the mutation-notification → `Enqueue` wiring and
  the 30s `recentlyProcessed` guard are unchanged; the debounce/helper suite
  (`TestEmbedQueueDebounceAndHelpers`) passes with the sidecar writeback.
- [x] 8. Audit: WAL `OpUpdateEmbedding` per sidecar writeback is pinned by
  `TestWALEngine_UpdateNodeEmbeddingSidecar`;
  `docs/compliance/background-workers-mvcc-audit-guide.md` now describes the
  embedding-only write model (no new MVCC node version, content stamp,
  business-write invalidation).
- [x] 9. Tests: MVCC head-unchanged invariant, business-write authority,
  stale-sidecar guard, delete-then-recreate, failure streaming, NotFound
  without create, WAL audit — all in `pkg/storage/embedding_sidecar_test.go`
  and `pkg/storage/wal_embedding_sidecar_test.go`.
- [x] 10. Benchmarks (pool-shaped, M2 Max, `-benchtime=3000x -count=3
  -cpu=1`): sidecar writeback 10.2–10.4 µs/op, 9,142 B/op, 122 allocs vs
  legacy node-record writeback 13.3–13.8 µs/op, 13,854 B/op, 139 allocs —
  the sidecar is ~24% faster and ~34% lighter in this shape.

## Acceptance criteria

- [x] AC1: A worker writeback creates **zero** new node MVCC versions — the
  MVCC head is identical before and after embedding
  (`TestEmbeddingSidecar_WritebackDoesNotTouchNodeRecord`).
- [x] AC2: Worker writebacks and concurrent business writes cannot conflict:
  the writeback touches only embedding keys; a business write is authoritative
  and drops the worker metadata
  (`TestEmbeddingSidecar_BusinessWriteIsAuthoritative`).
- [x] AC3: Embedding-only writes never modify business fields — `UpdatedAt`,
  properties and labels are unchanged after writeback; the embedding
  timestamp is in the metadata record.
- [x] AC4: Reads return the persisted embeddings and metadata after
  writeback; the single `withUpdateUnits` commit means readers see old or new
  embedding state, never a mix.
- [x] AC5: Audit evidence preserved: WAL `OpUpdateEmbedding` records
  (`TestWALEngine_UpdateNodeEmbeddingSidecar`), worker logs and queue stats
  unchanged, and the compliance guide describes the new model truthfully.
- [x] AC6: Deleting a node leaves the embedding key space unreachable; a
  writeback racing the delete returns `ErrNotFound` and writes no node; a
  recreated same-ID node never inherits the deleted node's embedding
  (content-stamp guard).
- [x] AC7: Post-embedding mutation notifications still re-enqueue the node,
  and the queue does not loop on its own writebacks (existing guards and
  suites pass).
- [x] AC8: `go test -tags noui,nolocalllm ./...` passes; `-race` on
  `pkg/storage` and `pkg/nornicdb` is clean (the wall-clock
  `TestScopedLabelReadCostIgnoresOtherDatabases` assertion trips under race
  instrumentation identically on baseline `origin/main`); the writeback
  benchmark is faster than the node-record writeback.

