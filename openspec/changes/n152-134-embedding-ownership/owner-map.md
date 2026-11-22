# Owner map: cached embedding vector ownership

Checkout `f77ec7706de099588ac232b32b6908860511d3da`. Line numbers are that
revision. Source of truth: `pkg/embed/cached_embedder.go` (411 lines), read in
full; `pkg/embed/embed.go`, `pkg/embed/provider_error.go`,
`pkg/embed/provider_batch.go`, `pkg/embed/voyage.go`, `pkg/embed/local_gguf.go`,
`pkg/embed/spans.go` read at the cited lines.

## Retention owner

| State | Field | Single writer | Notes |
|---|---|---|---|
| Retained vector | `cacheEntry.embedding` (`:45-48`) | `CachedEmbedder` under `c.mu` write lock | written only at `:157` (single) and `:246` (batch); never mutated in place afterwards; entries are replaced wholesale (`Clear` `:389-394`) or dropped (`evictOldest` `:398-405`) |
| Retained key | `cacheEntry.key` (`:46`) | same | used by `evictOldest` to delete the map entry |
| Map/list index | `c.cache`, `c.lru` (`:43-45`) | same | promoted at `:120-123`, `:213-215`; capacity loop `:152-154`, `:243-245` |
| Counters | `c.hits`, `c.misses` (`:48-49`) | `atomic` at `:117`, `:129`, `:209`, `:218` | `Stats` `:343-374` reads them |

Today the retained vector has a second, unwritten-but-reachable owner: the
caller, through the slice header copied at `:125`, `:148`, `:161`, `:208`,
`:238`. That shared ownership is the defect.

## Key owners (unchanged by the correction)

| Owner | Function | Property preserved |
|---|---|---|
| Plain key | `hashText` `:88-92` (FNV-1a, base-36) | `Embed` (`:111`) and `EmbedBatch` (`:169`, `inputType ""`) share one namespace via `hashTextForInputType` `:93-96` |
| Typed key | `hashTextForInputType` `:93-101` | a non-empty `inputType` is prefixed with a `0x00` separator, so query and document embeddings cannot collide; `EmbedBatchWithInputType` falls back to plain keys when the base is not a `TypedEmbedder` (`:187-190`) |

## Return paths that must become caller-owned copies

| Site | Line | Path | Directed by |
|---|---|---|---|
| R1 | `:125` | `Embed` hit | `TestCachedEmbedder_CacheHit`, `..._Concurrent` |
| R2 | `:161` | `Embed` miss, insertion winner | `TestCachedEmbedder_CacheHit`, `_LRUEviction` |
| R3 | `:148` | `Embed` miss, double-check loser | `TestCachedEmbeddingOwnershipConcurrentMiss` (untracked) |
| R4 | `:208` | batch hit (plain and typed) | `TestCachedEmbedder_BatchCaching`, `_TypedCacheSeparatesQueryAndDocument` |
| R5 | `:238` | batch miss result (also the duplicate and concurrent-loser position) | same, plus `TestCachedEmbeddingOwnershipBatchMissResults` (untracked) |
| R6 | `:181` | `EmbedWithInputType` returns `vecs[0]` | inherits R4/R5 |
| R7 | `:265-269` | `EmbedDocumentChunks` fallback embeds chunks | inherits R4/R5 |
| R8 | `:305-308` | `EmbedDocumentBatchChunks` fallback | inherits R4/R5 |

Not owners, no vectors retained by the cache: `EmbedDocumentChunks` provider
branch `:271`, `EmbedDocumentPropertyChunks` `:282-287`,
`EmbedDocumentPropertyBatchChunks` `:292-297` (structured payloads deliberately
bypass the text cache), `ChunkText` `:312-314`, `Dimensions`/`Model`/`Backend`/
`EmbeddingSpace` `:317-341`, `Close` `:377-383`, `Unwrap` `:386`.

## Reusable clone owners

| Candidate | Reusable from `pkg/embed`? | Finding |
|---|---|---|
| `cloneFloat32Slice` `pkg/cypher/vector_query_embed_cache.go:22-30` | no (package-private, different cache) | established repository pattern: that cache stores a clone (`:79`) and returns a clone (`:49`, `:60`, `:87`) |
| `append([]float32(nil), vec...)` idiom, `pkg/search/vector_index.go:243`, `:249`; `pkg/search/hnsw_index.go:309` | no (consumer-local copies) | shows the hazard is already treated per consumer |
| `normalizeVectorCopy` `pkg/search/relationship_search.go:515-533` | no | normalize-plus-copy, not a plain clone |
| existing helper inside `pkg/embed` | — | none: `cached_embedder.go` contains no `slices.Clone`, no `copy(`, no `make([]float32`; the package has no clone helper |

Conclusion: exactly one new clone owner is required in package `embed`
(`cloneVector`), mirroring the cypher pattern, used at the retention and return
sites listed above. Moving or exporting the cypher copy would be a
cross-package refactor outside this change.

## Focused existing controls (must keep passing, unchanged semantics)

| Control | File | Constraint it pins |
|---|---|---|
| `TestCachedEmbedder_CacheHit` | `pkg/embed/cached_embedder_test.go:84` | provider call counts; hits=1/misses=2/size=2 after warm-up |
| `TestCachedEmbedder_TypedCacheSeparatesQueryAndDocument` | `pkg/embed/cached_embedder_test.go:126` | typed key separation, 2 typed misses + 1 hit, per-input-type vector contents |
| `TestCachedEmbedder_BatchCaching` | `pkg/embed/cached_embedder_test.go:160` | only misses reach the provider, forwarded batch size 2, 1 hit |
| `TestCachedEmbedder_LRUEviction` | `pkg/embed/cached_embedder_test.go:191` | capacity 3 keeps size 3, oldest evicted, re-fetch misses |
| `TestCachedEmbedder_Concurrent` | `pkg/embed/cached_embedder_test.go:220` | 100 concurrent hits after warm-up, exactly 2 provider calls |
| `TestCachedEmbedder_Dimensions` / `_Model` / `_Clear` / `_Stats_HitRate` | `pkg/embed/cached_embedder_extra_test.go:12,18,24,48` | delegation, `Clear` empties, hit rate 75% |
| `TestCachedEmbedderCloseClosesWrapped` | `pkg/embed/cached_embedder_close_test.go:19` | `Close` still reaches the wrapped embedder |
| `TestCachedEmbedderBatchesChunksAcrossDocuments` | `pkg/embed/cached_embedder_test.go:19` | document batching still groups 3 chunks into one provider batch |
| provider error paths | `pkg/embed/provider_error_test.go:34,43`, `pkg/embed/provider_error_finite_test.go:20`, `pkg/embed/provider_error_contract_test.go` | provider errors and `CheckEmbeddings` rejections propagate and are not cached |
| `CheckEmbeddings` | `pkg/embed/provider_error.go:73-96` | count, empty, dimension and finiteness validation, and its order relative to caching |
| provider batch tests | `pkg/embed/provider_batch_compatibility_test.go:40,54`, `provider_batch_http_test.go:134,232`, `provider_partition_usage_test.go:14,31,67,98`, `structured_batch_wrappers_test.go:47` | wrappers sit in both orders (`Traced(Cached(...))`, `Cached(Traced(...))`) and must keep producer/partition behaviour |
| `BenchmarkCachedEmbedder_CacheHit` / `_CacheMiss` | `pkg/embed/cached_embedder_test.go:270`, `:283` | legacy benchmarks use `mockEmbedder` with `Dimensions() == 3` and no dimension axis |
| `BenchmarkCachedEmbeddingOwnership` | `pkg/embed/cached_embedder_ownership_bench_test.go:32` (untracked) | already carries `dims{1024,4096}` × `{single_hit, batch16_hit, batch16_duplicate, batch16_half_miss}` through `ownershipBenchEmbedder` (`:11-30`, one fresh allocation per provider call, `ReportAllocs`) — the matrix in `tasks.md` extends this owner rather than adding a second one |

## Fail-first controls already in the worktree (untracked, read only)

| File | sha256 | Covers | Gaps |
|---|---|---|---|
| `pkg/embed/cached_embedder_ownership_test.go` | `1c57078fc6924854cfca95b85459133c325f21418cfdab013c36942864bcf7a7` | routes `single`, `batch`, `typed_single`, `typed_batch`, `typed_fallback_single`, `typed_fallback_batch` × `{miss, hit}` mutation; duplicate cached batch (plain, typed) | no cold duplicate-miss assertion, no concurrent case, no eviction interaction, no error-path assertion, no identity check against `cached.cache`, mock is 3-dimensional |
| `pkg/embed/cached_embedder_ownership_boundaries_test.go` | `9528960eb6e8e903f39151e697443a68616e53e9a01f1ad185961af2ddcb2189` | R3 race: deterministic two-goroutine double-check via `ownershipRacingEmbedder` (`:11-33`, `:35`); A-distinct violation via `ownershipAliasedBatchEmbedder` (`:72`) returning one shared slice for `["same","same","other"]` (`:83`) | plain key only (no typed parity), no eviction or error interaction |
| `pkg/embed/cached_embedder_ownership_bench_test.go` | `35f4daa3faca56d18e973b7e08c746e75f04f4234e516d70abd6261017918972` | 1024/4096 dimensions for single/batch hit, duplicate batch and half-miss batch, with `ReportAllocs` | no cold single miss, no typed routes, provider never reuses a buffer, no recorded pre-fix baseline |

Recorded red evidence: `openspec/changes/n152-134-embedding-ownership/evidence/current-release-red.log.txt`
(sha256 `3891303a3e6bb8bc0690cdb0f4b0732336a7f95b9193d2cebc839e5e535d4cec`,
produced by another process — this investigation did not run it) shows all 12
`TestNOR381…` route/phase subtests and both duplicate-batch subtests failing
while `TestCachedEmbedder_CacheHit`, `_TypedCacheSeparatesQueryAndDocument`,
`_BatchCaching`, `_LRUEviction` and `_Clear` pass.

## Interfaces that carry the vectors beyond the cache

| Interface | Line | Ownership effect |
|---|---|---|
| `Embedder` (`Embed`, `EmbedBatch`) | `pkg/embed/embed.go:80-113` | no ownership or immutability statement — the contract is undocumented, which is why A-provider and A-distinct cannot be assumed |
| `TypedEmbedder` | `pkg/embed/embed.go:123-126` | same |
| `DocumentChunkEmbedder` / `DocumentBatchChunkEmbedder` | `pkg/embed/embed.go:143-145`, `:172-174` | provider-owned results; the cached wrapper's provider branches bypass the text cache |
| `mergeProviderBatch` | `pkg/embed/provider_batch.go:12-23` | passes provider slices through when a response was not partitioned, so provider memory reaches cache retention today |
| `TracedEmbedder` | `pkg/embed/spans.go:22-159` | pass-through wrapper (no clone, no `copy`, no `slices.*`), so callers receive whatever `CachedEmbedder` returned |

## Invariant to enforce after the correction

1. `cacheEntry.embedding` is reachable only from `CachedEmbedder` internals.
2. No returned slice shares a backing array with a retained entry, with a
   sibling position of the same call, or with provider memory.
3. Same key ⇒ same values, independently owned per result position (including
   duplicates, double-check losers, and concurrent batches).
4. Keys, validation, LRU order, eviction, counters and `Stats` are unchanged.
