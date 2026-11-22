# N152-128 (NOR-330) worker proposal: one canonical embedding-metadata map owner

Bounded proposal only. This worker wrote no source and no tests, ran no Go command, and
claims no runtime verification. Implementation and test edits belong to the Codex agent that
owns them; architecture, performance consequence and final acceptance belong to the parent.
Source-owner evidence, line references and limits: `owner-map.md` in this change folder.
All line references describe HEAD `f77ec7706de099588ac232b32b6908860511d3da`; §7 records the
implementation that another agent landed in the working tree during this investigation.

## 1. Narrow canonical correction

One write in one owner: `(b *BadgerEngine).encodeNodeInTxn`, `pkg/storage/badger_helpers.go`,
lines 1059-1071. Replace the shared-map injection with an injection into a body image the
encoder owns, using the existing map-copy owner `ownedEntries`
(`property_ownership.go:26-34`) — the same owner that already copies `EmbedMeta` for
storage-owned node images in `copyNodeWithoutEmbeddings` (`transaction.go:190-192`) and
`reconcileManagedEmbeddingsLocked` (`badger_transaction.go:1288`).

```go
	chunkCount := len(bodyCopy.ChunkEmbeddings)
	bodyCopy.ChunkEmbeddings = nil
	{
		metadata := bodyCopy.RuntimeMetadata()
		metadata.EmbeddingsStoredSeparately = true
		bodyCopy.SetRuntimeMetadata(metadata)
	}
	if _, hasChunkCount := bodyCopy.EmbedMeta["chunk_count"]; !hasChunkCount {
		// The count describes the body this encoder writes, so it is written
		// into an image the encoder owns: bodyCopy shared n's metadata map, and
		// the caller's map is not this body's to change — as n's properties,
		// vectors and runtime record are already not (ownedEntries is the
		// embedding-metadata copy owner, and it allocates the map n lacked).
		bodyCopy.EmbedMeta = ownedEntries(nil, n.EmbedMeta)
		bodyCopy.EmbedMeta["chunk_count"] = chunkCount
	}
```

Why this shape:

- It corrects the root cause (shared map ownership) in the single owner of that write, so all
  ten call sites in `owner-map.md` §3 are corrected at once. No call site gains a
  copy-before-encode step; a per-caller fix would duplicate the behaviour and leave the next
  caller broken.
- The guard is unchanged, so the accepted "explicit count wins" behaviour is preserved
  verbatim. The copy is made only in the branch that writes, so the already-safe case (caller
  supplied `chunk_count`) pays nothing: no allocation, no map identity change.
- `ownedEntries(nil, nil)` returns a non-nil empty map allocated with room for the source, so
  the existing `if bodyCopy.EmbedMeta == nil { make(...) }` at 1066-1068 is subsumed. The
  caller's nil map stays nil (nothing is assigned back to `n`).
- The encoded bytes are unchanged: same keys, same values, only the map the encoder mutates
  differs. `separate == true`, `ChunkEmbeddings` dropping on the copy, the error paths, the
  runtime-record handling and the caller-side chunk persistence
  (`embeddingsToPersist = node.ChunkEmbeddings`) are untouched.

Rejected alternatives (recorded so the parent can re-weigh them):

| Alternative | Why not |
|---|---|
| `maps.Clone` (as `snapshot_vector_metadata.go:23`) | shallow: nested `EmbedMeta` values (e.g. the range-manifest `[]byte`, the embedding-source record) would still be shared with the caller map, and it is not the owner storage already uses for node `EmbedMeta`. Cheaper than `ownedEntries`; see §7 — the implementation owner chose this one, and its residual exposure is a nested value only if some encoder-side code ever mutates one |
| `copyCallerMetadata` (`property_copy.go:55`) alone | returns nil for a nil input, so the nil case would still need its own allocation at the site — two owners for one behaviour |
| Copy the whole node (`copyNode`) inside the encoder | copies chunk vectors (60 × 1024 values on the reported fixture) and properties only to discard them, and detaches state the encoder does not change |
| Detach in each ingress (`writeCreatedNode`, `updateNode`, tx staging, deindex, admission) | the injecting behaviour would still own a caller map; N copies of one rule |

Cost boundary for the parent: one small map copy (entry count, plus nested values if
`ownedEntries`) per separate-embedding encode whose caller metadata lacks `chunk_count`.
Expected to be negligible beside the separate branch's own work, but it is an allocation on a
write path and must be measured, not assumed — see §4.

Scope discipline: `encodeNodeV1Body` (`badger_helpers.go:959-990`) duplicates the same three
steps and has no production caller (`owner-map.md` §6). Leave it or converge it — parent
decision. Correcting it adds no ticket route and is not required by the acceptance criteria.

## 2. Targeted controls

The reproduction already exists in-repo and is preserved byte-identically from the ticket
fixture: `pkg/storage/embedding_encode_metadata_ownership_test.go`
(`TestNOR330EmbeddingEncodeKeepsCallerEmbedMetaOwned`, sha256 `b843d45c…`, matching
`evidence/manifest.json`). Its recorded RED log (`evidence/testdata/macos.log.txt`,
`linux.log.txt`) shows `direct CreateNode`, `BulkCreateNodes` and
`explicit transaction CreateNode` failing with `chunk_count:60` added to the caller map, and
the three controls passing. That evidence is recorded, not produced by this worker.

Required coverage after the fix, each mapped to a behaviour:

1. **Ticket RED routes must go green** — direct `CreateNode`, `BulkCreateNodes`,
   `tx.CreateNode` + commit, oversized 60 × 1024 fixture, caller map without `chunk_count`:
   caller map unchanged; stored body `chunk_count == 60`; 60 chunks hydrate with first/last
   vector bits equal to the input. (Already asserted by `nor330RequireChunksStoredApart` —
   this is the "stored count/chunks preserved" half.)
2. **Gap: native transaction update.** The ticket's routes cover `tx.CreateNode`. The
   defect's `native tx` owner also includes `tx.UpdateGraphNode` → `badger_transaction.go:1148`,
   where `body` may *be* the caller's node; and `reconcileManagedEmbeddingsLocked` can
   substitute its own body. One subtest on the oversized fixture through
   `tx.UpdateGraphNode` with a caller image whose metadata lacks `chunk_count` should assert
   the caller map is unchanged and the stored count/chunks are right.
3. **Gap: direct `UpdateNode`.** `badger_nodes.go:671` (update branch) and `:579` (insert
   branch of the upsert) reach the encoder with the caller's image and are not in the ticket
   matrix. Same assertions; the insert branch needs a first write on a fresh ID.
4. **Explicit count behaviour must be pinned, not assumed.** Two cases on the oversized
   fixture: caller metadata with `chunk_count: 60` (values equal — unchanged, no copy taken),
   and with a disagreeing value such as `chunk_count: 99` (still stored as supplied; the
   current contract, cf. `badger_helpers_extra_test.go:155-171`). Both must also show the
   caller map unchanged and the vectors still persisted separately. A separate case with
   `EmbedMeta == nil` must show the caller's map stays nil while the body carries the count.
5. **Namespaced and inline controls must keep passing** (they already do), as the
   non-vacuity proof: only the four aliasing ingresses change.
6. **Byte-equality regression must be reconciled by the test owner — this is a real
   conflict, not a formality.** `TestEncodeNodeInTxnMatchesMeasuredEncoding`
   (`embedding_chunk_decodes_test.go:284-368`) uses `referenceEncodeNodeInTxn`
   (`:240-282`), a copy of the pre-NOR-412 encoder that shares the caller map, and asserts
   "the same change to the caller's metadata map" (`:289`, `:350-357`:
   `require.Equal(t, want.EmbedMeta, got.EmbedMeta)`). After the correction the reference
   still mutates `want.EmbedMeta` while production leaves `got.EmbedMeta` alone, so the
   `far over the limit, metadata without chunk_count` and `metadata that cannot be encoded,
   far over the limit` cases fail on that assertion. The reference exists to prove byte /
   branch / error equality of the measured encoding, which the fix does not change, so the
   cursor should move to the caller-copy contract: keep the reference for bytes, decoded
   bodies, branch and error identity, and replace the caller-map equality with "production
   left the caller map as supplied" (the `len(want.EmbedMeta) <= 1` byte-equality shortcut
   stays valid). Whoever edits this must confirm no other test depends on the post-encode
   caller mutation; I found none besides this file. This applies unchanged to the
   `maps.Clone` variant in §7.

Suggested new-file/placement is the test owner's call; the constraints above are what matter.

## 3. Verification the implementer must record (not run by this worker)

Per `AGENTS.md`/`CONTRIBUTING.md`: focused package normal and race runs for `pkg/storage`
covering the controls above, `go vet`/`gofmt` for the touched package, and the recorded RED
evidence kept as the before-state. A helper-only pass does not establish the four public
ingress routes. Report actual command, head SHA and any skips.

## 4. Matched benchmark options

The ticket's fixture class (`EmbedMeta` present without `chunk_count`, separate-embedding
branch) has **no** existing benchmark, so no current benchmark exercises the new copy:

| Benchmark | Relation to the change |
|---|---|
| `BenchmarkSerializeNode` (`serialization_bench_test.go:59-67`) | the encode owner's own benchmark; its fixture (`benchmarkNode`, `:213-231`) has nil `EmbedMeta` and stays inline, so it must show no change — the no-regression gate for the untouched path |
| `BenchmarkTransactionNodeUpdateSeparateEmbeddings` (`badger_transaction_node_chunks_test.go:143-…`) | the public write-behind update that ends in the separate-embedding branch; all three fixtures (`small`, `large`, `large text`) supply `chunk_count`, so the guarded injection never runs — it measures the branch, not the copy |
| `BenchmarkBadgerEngine_BulkCreateNodes` / `BenchmarkBadgerEngine_CreateNode` (`badger_test.go:1981-2040`) | the bulk and direct ingress owners, with ordinary nodes — unaffected path, useful only as a broad no-regression check |

To size the added allocation, the test owner should extend
`BenchmarkTransactionNodeUpdateSeparateEmbeddings` with a fixture matching the defect (60 ×
1024 chunks, `EmbedMeta` without `chunk_count`) or add an encode-only benchmark beside
`BenchmarkSerializeNode` for that fixture, and compare identical fixtures across revisions
with `-benchmem`. Precedent for "cost evidence recorded on unchanged source, asserting
nothing" is `nor279_metadata_ownership_test.go:700-703` (the metadata-ownership change that
introduced these detach rules). Exact commands are the implementer's to run; this worker ran
none.

## 5. Limits of this proposal

- No Go test, build, vet, race or benchmark run; no profile; no runtime claim. The RED
  outcomes quoted are the recorded artifacts in `evidence/`.
- The defect path is established statically from source: shallow copy at
  `badger_helpers.go:1024`, branch entry at `1036-1054`, shared-map write at `1069-1070`.
- I did not exhaustively trace every consumer of a caller image after encode; §7 of
  `owner-map.md` lists the ones found. Final acceptance of consequences is the parent's.
- One in-path behaviour change beyond the fix's purpose, flagged for the parent: at
  `badger_nodes.go:691` the sidecar-reuse decision (`sameStoredValue(meta, node.EmbedMeta)`)
  currently compares against a map the encoder has already modified; after the correction it
  compares the caller's real metadata, which can turn a forced sidecar rewrite into a reuse.
  Source-visible, bounded, and in the direction of the intended contract — but it is a write
  effect the parent should accept explicitly, and the test owner should cover it if accepted.
- Out of scope per ticket and task: caller timestamps, vector ownership, wrapper variants,
  `keepSourceEmbeddings` reference adoption, `encodeNodeV1Body` convergence (decision
  pending), and any production correction this proposal does not name.

## 6. Decisions requested from the parent

1. Approve the correction as the canonical owner — encoder-side copy, guard ordering as
   sketched — and choose the copy owner: `ownedEntries` (deep, one owner for node
   `EmbedMeta`; §1) or `maps.Clone` (shallow, cheaper, already landed; §7).
2. Decide `encodeNodeV1Body` convergence: fold both sites onto one shared owner now, or
   leave the test-only V1 duplicate documented in `owner-map.md` §6.
3. Accept or reject the `badger_nodes.go:691` sidecar-reuse effect described in §5.

## 7. Concurrent implementation state (another agent's work, reviewed only)

`git status` was clean when this investigation began and was not clean when it ended
(`owner-map.md` §9). The working tree now contains, uncommitted:

```
 M pkg/storage/badger_helpers.go
?? pkg/storage/embedding_encode_metadata_ownership_test.go
```

`git diff` replaces HEAD lines 1066-1071 in `encodeNodeInTxn` with:

```go
	if _, hasChunkCount := bodyCopy.EmbedMeta["chunk_count"]; !hasChunkCount {
		// The shallow body copy still shares caller metadata. Only this new
		// entry belongs to the encoded body; existing values remain read-only.
		bodyCopy.EmbedMeta = maps.Clone(bodyCopy.EmbedMeta)
		if bodyCopy.EmbedMeta == nil {
			bodyCopy.EmbedMeta = make(map[string]any)
		}
		bodyCopy.EmbedMeta["chunk_count"] = chunkCount
	}
```

Assessment against the ticket (`seen in source only; nothing executed`):

- It satisfies the accepted defect statement and all three acceptance criteria as written:
  the injection no longer reaches the caller's map, the guard is unchanged (explicit count
  still wins), the nil case still produces a body with the count, and the encoded bytes are
  unchanged. All four aliasing ingresses in `owner-map.md` §3 are corrected by the single
  owner because the copy is inside the encoder.
- Difference from §1, for the parent to weigh: `maps.Clone` is shallow, so nested `EmbedMeta`
  values stay shared with the caller map. On the traced paths that is not observable — the
  encoder only passes the map to `encodeValue`, never returns or retains it, and
  `writtenNodeImage` decodes rather than adopting it whenever `EmbedMeta != nil`
  (`codecNodeProperties`, `badger_transaction_images.go:229-235`). It leaves two copy rules
  for the same map in the package (deep `ownedEntries` for storage-owned images, shallow
  clone here), which is the principle-14 question the parent owns. It allocates one map fewer
  than `ownedEntries` and copies no values.
- It does not resolve §2.6: the matched-encoding test's caller-map equality assertion still
  conflicts with the new behaviour and must be updated by the test owner.
- Note for whoever reviews the final patch: the same shallow-copy-plus-shared-map pattern
  remains in `encodeNodeV1Body` (`badger_helpers.go:972-986`, test-only caller), unchanged.
