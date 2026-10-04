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

- [ ] 1. Storage layout: define the per-node embedding record keys
  (chunks via `embeddingPrefix` + a metadata key holding `EmbedMeta`,
  model/dimensions, space, timestamps, `chunk_count`); document that the node
  body no longer carries `EmbedMeta`/`EmbeddingsStoredSeparately` for worker
  writebacks.
- [ ] 1.1 Keep inline storage for statements that embed in the same
  transaction (`WITH EMBEDDING`, `pkg/cypher`) so their semantics are
  unchanged; only the asynchronous worker path moves to embedding-only
  writes.
- [ ] 2. Write path: add/extend a storage interface
  (e.g. `UpdateNodeEmbeddingsOnly(node)` or re-purpose
  `UpdateNodeEmbedding` semantics) that writes chunk keys + metadata key
  atomically with `withUpdateUnits`, removes the pending-embeddings index
  key, and returns `ErrNotFound` without creating when the node is gone.
- [ ] 3. Read path: extend `loadNodeEmbeddings` (and cache hydration) to
  merge the metadata record; node bodies with separate embeddings read
  identically to today (properties, labels, embeddings, `EmbedMeta`).
- [ ] 4. Worker: `persistEmbeddedNode` stops re-reading + re-encoding the
  node body; it verifies node existence (`GetNode`, skip on delete) and
  writes only embedding state. `UpdatedAt` is no longer bumped by workers.
- [ ] 5. Async/namespaced layers: `AsyncEngine.UpdateNodeEmbedding` stages
  only embedding keys (not a whole-node cache entry); `NamespacedEngine`
  forwards the embedding-only write; flush semantics keep counts stable.
- [ ] 6. Deletion hygiene: node deletion removes the embedding key space;
  worker writeback after deletion leaves no orphan keys.
- [ ] 7. Re-notification: preserve the existing mutation-notification →
  `Enqueue` behavior and the 30s `recentlyProcessed` guard; add a regression
  that embedding writeback does not loop.
- [ ] 8. Audit: keep WAL `OpUpdateEmbedding` per writeback; update
  `docs/compliance/background-workers-mvcc-audit-guide.md` (worker writes no
  new MVCC node version; audit evidence now lives in the embedding record +
  WAL) and `docs/features/vector-embeddings.md` if it documents writeback
  details.
- [ ] 9. Tests: MVCC version-count invariant, concurrent business-write /
  worker-writeback stress (no conflicts, business write intact), delete-during
  embedding, crash/reopen hydration, snapshot read consistency.
- [ ] 10. Benchmarks: record worker writeback ns/op and allocs before/after;
  no regression in `UpdateNodeEmbedding` hot paths.

## Acceptance criteria

- [ ] AC1: A worker writeback creates **zero** new node MVCC versions — the
  node's version history is identical before and after embedding; pinned by a
  test on the Badger engine.
- [ ] AC2: Worker writebacks and concurrent business writes to the same node
  never conflict: no surfaced Badger conflict, no retry storm, and the
  business write's properties/labels are always preserved; pinned by a
  `-race` stress test.
- [ ] AC3: Embedding-only writes never modify business fields:
  `UpdatedAt`, properties and labels of the node record are unchanged after
  worker writeback; the embedding timestamp is in the metadata record.
- [ ] AC4: Reads return the persisted embeddings and metadata after
  writeback; snapshot readers see old or new embedding state, never a mix.
- [ ] AC5: Audit evidence is preserved: WAL contains `OpUpdateEmbedding`
  records for worker writebacks, worker success/failure logs and queue stats
  are unchanged, and the compliance guide describes the new model truthfully.
- [ ] AC6: Deleting a node cleans its embedding keys; a worker writeback
  that races the delete writes nothing and leaves no orphan data.
- [ ] AC7: Post-embedding mutation notifications still re-enqueue the node,
  and the queue does not loop on its own writebacks.
- [ ] AC8: `go test -tags noui,nolocalllm ./...` and the cypher/conformance
  gates pass; focused races pass twice; worker writeback benchmark is within
  noise or faster than the current node-record writeback.
