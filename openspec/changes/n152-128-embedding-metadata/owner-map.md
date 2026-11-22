# N152-128 (NOR-330) owner map: embedding-metadata write ownership on the encode path

Read-only source investigation. No Go test, build, vet or benchmark was executed for this
document, and nothing here is runtime verification. The RED evidence quoted below is the
recorded evidence already stored in `evidence/` (not re-run here). Baseline:
worktree HEAD `f77ec7706de099588ac232b32b6908860511d3da`. Every line reference below is
against that commit, which is what this worker read; see §9 for the working-tree changes
another agent made while this document was being written.

## 1. The defect in source

`(b *BadgerEngine).encodeNodeInTxn` — `pkg/storage/badger_helpers.go:1014`.

```go
1024	bodyCopy := *n
1025	bodyCopy.Properties = nil
...
1059	chunkCount := len(bodyCopy.ChunkEmbeddings)
1060	bodyCopy.ChunkEmbeddings = nil
1061	{
1062		metadata := bodyCopy.RuntimeMetadata()
1063		metadata.EmbeddingsStoredSeparately = true
1064		bodyCopy.SetRuntimeMetadata(metadata)
1065	}
1066	if bodyCopy.EmbedMeta == nil {
1067		bodyCopy.EmbedMeta = make(map[string]any)
1068	}
1069	if _, hasChunkCount := bodyCopy.EmbedMeta["chunk_count"]; !hasChunkCount {
1070		bodyCopy.EmbedMeta["chunk_count"] = chunkCount
1071	}
```

`bodyCopy := *n` is a shallow struct copy. Line 1025 detaches only `Properties`, line 1060
only `ChunkEmbeddings`. `EmbedMeta` continues to point at the caller's map, so the write at
line 1070 is a write into a caller-owned `map[string]any` whenever the caller supplied one.
The nil case (1066-1068) is already safe because it allocates a fresh map.

Branch entry, confirmed against `pkg/storage/badger.go:133` (`maxNodeSize = 50 * 1024`):
a body with `chunkEmbeddingValues(n.ChunkEmbeddings) > maxNodeSize` skips the measuring
encode (`badger_helpers.go:1036-1054`) and a body that measures larger is dropped into the
same separate-embedding branch. The reported fixture, 60 chunks × 1024 values = 61440
values > 51200, takes that branch, so lines 1066-1071 run.

Runtime-record ownership is **not** part of the defect: `SetRuntimeMetadata`
(`pkg/storage/node_runtime.go:43-64`, `setRuntimeState` 71-113) replaces the record on
`bodyCopy` only — it either interns a metadata-only record or allocates a new `nodeRuntime`,
so the caller's `runtime` pointer is untouched (`node_runtime.go:5-8` states the rule).

## 2. Ownership inventory

| Behaviour | Canonical owner | Note |
|---|---|---|
| Node body encoding (incl. separate-embedding decision) | `encodeNodeInTxn` `badger_helpers.go:1014` | V2 hot path; `encodeNode` (`:1133-1139`) is a thin wrapper with test-only callers today |
| The injected `chunk_count` value | the same branch, `badger_helpers.go:1059-1071` | single writer of that key on the body image; guard at 1069 is the existing "explicit count wins" rule |
| Copying caller-supplied `EmbedMeta` into a storage-owned image | `ownedEntries(dst, src)` `property_ownership.go:26-34` over `CopyValue`/caller-copy protocol `property_copy.go:15-57` | already the owner used for `EmbedMeta` at `transaction.go:190-192` (`copyNodeWithoutEmbeddings`) and `badger_transaction.go:1288` (`reconcileManagedEmbeddingsLocked`) |
| Namespace qualification / caller input detachment on wrappers | `(n *NamespacedEngine).NodeWriteInput` `namespaced.go:1311-1321` → `copyNode` `transaction.go:127` | this is why namespaced routes are already safe |
| Stored-image publication | `writtenNodeImage` `badger_transaction_images.go:102-116`, `keepSourceEmbeddings` `:72-90` | consumes the writer's node as publication source |
| Chunk-vector sidecar persistence | caller after encode: `embeddingsToPersist = node.ChunkEmbeddings`, `cw.replaceEmbeddingRange(...)` e.g. `badger_nodes.go:143-148`, `:764-771` | vectors are not detached by the encoder by design |

## 3. Call sites and caller-map alias status

| Call site | Ingress owner | Caller map aliased on the oversized path? |
|---|---|---|
| `badger_helpers.go:196` (`writeCreatedNode`) ← `badger_nodes.go:112` | direct `CreateNode` | yes |
| `badger_helpers.go:196` ← `badger_transaction.go:994` | native tx `CreateNode`, and `BulkCreateNodes` (`badger_bulk.go:20-29` → `tx.CreateNode`) | yes |
| `badger_nodes.go:579` | `UpdateNode`/upsert insert branch | yes |
| `badger_nodes.go:671` | `UpdateNode`/upsert update branch | yes |
| `badger_transaction.go:1148` | native tx update (`body` may be the caller's `node`) | yes when `body == node` |
| `badger_deindex_enqueue.go:165` | suppression re-encode of the write's own image | yes (second, idempotent write of an already-counted map) |
| `commit_admission.go:185` | `replayedBodyLimitError`, measurement of commit images | yes |
| `badger_transaction_images.go:27` | `StoredNodeImage` | no — `copyNode(node)` at `:25` first |
| `migration_v1_to_v2.go:446`, `migration_v3_to_v4.go:67` | migrations | no — decoded/migration-owned images |
| `serialization_bench_test.go:65,75,122,144`, `write_behind_stored_image_test.go:161`, other fixtures | benchmarks/tests | n/a |

## 4. Why the namespaced controls are already safe

`NamespacedEngine.CreateNode` / `UpdateNode` (`namespaced.go:229-241`, `:301-309`) route the
caller node through `NodeWriteInput`, which returns `copyNode(node)`
(`namespaced.go:1315`). `copyNodeWithoutEmbeddings` copies `EmbedMeta` with
`ownedEntries(nil, node.EmbedMeta)` (`transaction.go:190-192`), so the image that reaches
`encodeNodeInTxn` owns its metadata map. The recorded RED log shows both namespaced
controls passing while the three reported routes fail
(`evidence/testdata/macos.log.txt`).

## 5. What the stored body must keep (unchanged contract)

- The body must still record `chunk_count` when the caller supplied none: hydration reads it
  with `chunkCountFromMeta` inside `loadNodeEmbeddingState`
  (`badger_helpers.go:1190-1230`; the reads are at 1196 and 1214).
- An explicitly supplied `chunk_count` still wins (guard at 1069), including `0` or a value
  that disagrees with the vector count; that behaviour predates this change and is asserted
  for the V1 sibling by `badger_helpers_extra_test.go:155-171`.
- `separate == true` and the caller-side chunk persistence are unaffected (vectors are nilled
  on `bodyCopy` only, `:1060`).
- The encoded bytes are unchanged: same keys, same values. Only the map identity the encoder
  writes into changes.

## 6. Duplicate candidate (neighbour encode path)

`encodeNodeV1Body` — `badger_helpers.go:959-990` — repeats the same three steps at
`:972-986` (`nodeCopy := *n` at 972, `make(map[string]any)` at 981-983, guarded write at
984-986) with the identical alias. `encodeNodeV1` has **no production caller**: every
reference outside `badger_helpers.go` is a test
(`migration_v1_to_v2_test.go:21`, `property_codec_test.go:463`,
`byte_property_roundtrip_test.go:58`, `badger_helpers_extra_test.go:139,162`); the migration
tool reads V1 bodies via `decodeNodeV1`. So it is a genuine duplicate of the behaviour and a
candidate for convergence onto one shared owner, but it cannot produce the ticket's reported
routes. Parent decides whether convergence is in scope; the acceptance criteria do not
require it.

## 7. Source-visible effects of the leak (why it is consequential, and its limits)

Within the same write, after `encodeNodeInTxn`, the caller's mutated map is read back:

- `badger_nodes.go:691` — `sameStoredValue(meta, node.EmbedMeta)` decides whether the
  retained sidecar is reused or rewritten, using a map the encoder just changed.
- `writtenNodeImage(written, node)` (`badger_transaction_images.go:102-116`) treats `node` as
  the publication source; `keepSourceEmbeddings` (`:72-90`) adopts `source.EmbedMeta` by
  reference in its `{chunk_count: 0}`-only case.
- `badger_transaction.go:1153` — `invalidated` tests `node.EmbedMeta == nil`.
- `Node.HasChunkEmbeddings` (`types.go:1340-1348`) answers from `chunkCountFromMeta`, so the
  caller's own image changes meaning after the write.
- A caller that writes the same `*Node` again (retry, worker pool, second engine) now stores
  a `chunk_count` it never supplied — the class of carried-codec-metadata bug documented in
  `stored_image_embedding_state_test.go:20-40`.

Limits: I did not exhaustively trace every reader of a caller image after encode, and no
behaviour here was executed. The accepted product statement remains the ticket's: storage must
not write into the metadata map the caller supplied.

## 8. Explicitly out of scope

Per the ticket disposition (`evidence/ticket.json`: "Out of scope: timestamps, vector
ownership, every wrapper variant and production correction") and the delegated task: caller
timestamps (already mutated by `initializeCreationTimes`, `badger_nodes.go:178`), vector
ownership, wrapper variants, `keepSourceEmbeddings` reference adoption of
`source.EmbedMeta`, and any change outside the embedding-metadata map ownership described
above.

## 9. Concurrent working-tree state (not produced by this worker)

`git status` was clean at the start of this investigation and no longer is, so another agent
is editing this checkout:

```
 M pkg/storage/badger_helpers.go
 M pkg/storage/embedding_chunk_decodes_test.go
?? pkg/storage/embedding_encode_metadata_ownership_test.go
```

`git diff` on `badger_helpers.go` shows the injection at HEAD lines 1066-1071 replaced by a
`maps.Clone` variant inside the same guard, plus a `maps` import; the reproduction test and
`embedding_chunk_decodes_test.go` (the matched-encoding test of `worker-proposal.md` §2.6)
are being edited too. That is the implementation owner's work, not mine; this document's
references describe HEAD (`f77ec77`). The landed variant is compared with this proposal in
`worker-proposal.md` §7.
