> Parent review: the source alias map is accepted. Implementation will use standard-library `slices.Clone`, avoiding a redundant package helper. Batch miss outputs continue to represent their provider answer; they will not be replaced by another winner's cached value. Existing response arbitration, counters and LRU behavior remain unchanged. Sequential aliasing is reproduced; worker discussion of concurrent caller mutation is a potential consequence, not an executed race reproduction. Copying assumes provider outputs remain stable while consumed and cannot protect against a provider concurrently mutating its answer. Forwarding decoded provider slices does not itself prove provider buffer reuse.

# Give each cached embedding result its own vector

N152-134, bounded source investigation. Checkout `f77ec7706de099588ac232b32b6908860511d3da`.
All citations are `pkg/embed/cached_embedder.go` unless another path is named.

## Established defect (source-level)

`CachedEmbedder` keeps exactly one retained copy of each vector in
`cacheEntry.embedding` (`:46-49`) and hands that same slice, or the provider's
slice that became it, to every caller. Nothing in the wrapper copies a vector.

| # | Site | Route | Alias produced |
|---|---|---|---|
| A1 | `:125` `return entry.embedding, nil` | `Embed` single hit | caller == retained entry |
| A2 | `:157`/`:161` `&cacheEntry{…embedding: embedding}` + `return embedding, nil` | `Embed` single miss (insertion winner) | caller == retained entry |
| A3 | `:148` `return elem.Value.(*cacheEntry).embedding, nil` | `Embed` miss double-check loser | caller == retained entry written by the winner |
| A4 | `:208` `results[i] = entry.embedding` | `embedBatchWithKeyFunc` hit (plain `EmbedBatch` and typed `EmbedBatchWithInputType`) | each hit position == retained entry |
| A5 | `:238`/`:246` `results[i] = embedding` + `&cacheEntry{…embedding: embedding}` | batch miss, newly inserted key | result position == retained entry |
| A6 | `:208` twice for the same key | duplicate texts already cached | both result positions are the *same* slice as each other and as the entry |
| A7 | `:238` for both duplicate positions | duplicate texts, one batch, cold | both positions come from two provider positions; the second insertion is skipped at `:242`, so position 2 holds a provider slice that is *not* what later hits return |

`EmbedWithInputType` (`:176-182`) returns `vecs[0]` from the batch owner, so the
typed single path inherits A4/A5. The document-chunk fallbacks (`:265-269` and
`:305-308`) also consume `EmbedBatchWithInputType` results and therefore inherit
the same aliased vectors inside `DocumentChunkResult.Embeddings`; they need no
separate edit once the batch owner is corrected.

A6 is unconditional: for two cached duplicates, `results[i1]`, `results[i2]`
and the retained entry are one array, so one caller's in-place mutation changes
its sibling result and every future hit. A1–A5 and A7 make caller mutation
permanently corrupt the retained cache for that key. A3 is a data race, not only
stale data: two goroutines hold the same array, one writes and one reads.

In-place vector mutation is ordinary in this repository, not an exotic caller
behaviour: `pkg/simd.NormalizeInPlace` (`pkg/simd/simd.go:113`),
`vector.NormalizeInPlace` (`pkg/math/vector/similarity.go:222`) and
`pkg/localllm/llama.go:888` all rewrite a `[]float32` in place. Three consumers
already defend themselves individually — `pkg/search/vector_index.go:243`
(`private := append([]float32(nil), vec...)`),
`pkg/cypher/vector_query_embed_cache.go:49,60,69,79,87`
(`cloneFloat32Slice`) and `pkg/search/relationship_search.go:515`
(`normalizeVectorCopy`) — which is the same ownership decision duplicated per
consumer instead of being owned once where the vectors are retained.

## Provider-output alias assumption (stated, not relied on)

The current code implicitly relies on two provider properties that the
`Embedder` contract does not state (`pkg/embed/embed.go:80-113` documents
concurrency safety and `Backend()` only):

- **A-provider**: a returned vector is exclusively the caller's and is never
  mutated or re-used by the provider afterwards. Counter-evidence that this is
  not contractual: `mergeProviderBatch` (`pkg/embed/provider_batch.go:12-23`)
  forwards provider slices unchanged when a request is not partitioned, and
  `orderedVoyageEmbeddings` (`pkg/embed/voyage.go:263-274`) assigns decoded
  `item.Embedding` slices directly.
- **A-distinct**: a batch returns a distinct backing array per position. Not
  guaranteed; a provider may return one array, or views of one flat buffer, for
  repeated input. The worktree already contains an untracked control for this
  (`ownershipAliasedBatchEmbedder`, `pkg/embed/cached_embedder_ownership_boundaries_test.go`).

The proposed correction depends on neither assumption: the cache retains only
its own copies, returns only its own copies, and clones each result position
independently (never deduplicated by cache key).

## Explicit double-check of the miss race

`Embed` releases the read lock at `:127`, calls the provider at `:133`, then
re-acquires the write lock at `:142` and re-checks `:146-149`.
Goroutine A wins and stores its slice at `:157-159`, returning that array at
`:161`. Goroutine B, whose own miss was recorded at `:129`, takes the
double-check branch and returns the *same* array at `:148`. B's provider vector
is discarded. So the double-check does not remove the alias; it converts one
alias into two callers sharing the retained array — and the loser is the call
that never wrote the entry. Any fix that only changes `:125`/`:161` leaves A3
open. The untracked `TestCachedEmbeddingOwnershipConcurrentMiss` exercises
exactly this ordering; it is a required control for the correction.

The batch owner has a different race outcome: a concurrent batch for the same
key skips insertion at `:242` but still assigns its own provider slice at
`:238`. That path therefore does not alias the cache, but two callers receive
two different arrays for one key, and only one of them can corrupt the cache.
The same-key divergence also makes results order-dependent when the provider is
not deterministic.

## Minimum ownership correction

One invariant, one owner: **the cache never hands out the slice it retains, and
never retains a slice a caller can reach.** Every vector crossing the
`CachedEmbedder` boundary in either direction is a private copy.

1. Add one unexported clone owner in package `embed` (no existing clone helper
   exists in `pkg/embed`; `pkg/cypher.cloneFloat32Slice` is package-private and
   belongs to a different cache):

   ```go
   // cloneVector returns a copy the recipient owns. The cache retains and
   // returns only copies, so no caller can mutate retained state and duplicate
   // inputs never share a backing array.
   func cloneVector(v []float32) []float32 {
       if len(v) == 0 {
           return nil
       }
       out := make([]float32, len(v))
       copy(out, v)
       return out
   }
   ```

2. Retention: `:157` and `:246` store `cloneVector(embedding)` in
   `cacheEntry.embedding`. After this, `cacheEntry.embedding` is cache-private
   and immutable: the cache only replaces whole entries or drops them.

3. Returns, all through the same clone owner when the value leaves the cache:
   `:125`, `:148`, `:161`, `:208`, and the miss result assignment at `:238`
   (which becomes the retained entry's copy, so a same-key duplicate or a
   concurrent loser returns the value future hits will return, then gets its own
   array).
4. No change to key construction (`hashText` `:88`, `hashTextForInputType`
   `:93-101`), validation (`CheckEmbeddings` at `:137`/`:230`, always before any
   copy or retention), LRU promotion (`:120-123`, `:213-215`), eviction
   (`:152-154`, `:243-245`, `evictOldest` `:398-405`), hit/miss counters, or
   `Stats`/`Clear`. `EmbedWithInputType` and both document-chunk fallbacks
   inherit the correction through the batch owner and need no edit.
5. Document the contract on `Embed`, `EmbedBatch`, `EmbedWithInputType`,
   `EmbedBatchWithInputType` and on `cacheEntry`: each returned vector is owned
   by the receiver; retained vectors are never exposed.

Cost, stated plainly: one `dim*4`-byte copy per returned vector (4 KiB at 1024
dimensions, 16 KiB at 4096) plus one per insertion. Ownership correctness is the
value; there is no throughput improvement to claim, and the benchmark matrix
exists to publish that cost.

Alternative rejected as the primary correction: documenting "do not mutate" and
leaving consumers to copy. The defect is the cache corrupting itself when a
caller mutates a returned vector; the copy obligation already exists three times
downstream, and adding a fourth documented obligation preserves the shared
ownership instead of correcting it (standing principles 3 and 14).

## Out of scope

Hash-collision behaviour of the FNV-1a key, key namespacing changes, cache
policy/size changes, singleflight or request coalescing, provider-side clone
requirements, `TracedEmbedder`, and any unrelated cache redesign. The parent
owns concurrency design, the clone-under-lock vs clone-after-unlock choice,
performance acceptance, and final integration.

## Evidence limits

Kinds of evidence used: static source reading at the commit above, repository
guidance (`AGENTS.md`, `CONTRIBUTING.md`, `.agents/standing-design-principles.md`),
and artifacts another process added to this worktree during the session:

- untracked `pkg/embed/cached_embedder_ownership_test.go`
  (sha256 `1c57078fc6924854cfca95b85459133c325f21418cfdab013c36942864bcf7a7`),
- untracked `pkg/embed/cached_embedder_ownership_boundaries_test.go`
  (sha256 `9528960eb6e8e903f39151e697443a68616e53e9a01f1ad185961af2ddcb2189`),
- untracked `pkg/embed/cached_embedder_ownership_bench_test.go`
  (sha256 `35f4daa3faca56d18e973b7e08c746e75f04f4234e516d70abd6261017918972`),
- `openspec/changes/n152-134-embedding-ownership/evidence/{original-ownership-test.go.txt,original-command.sh.txt,current-release-red.log.txt,manifest.json}`
  (none written by this investigation).

The recorded red log `evidence/current-release-red.log.txt` (sha256
`3891303a3e6bb8bc0690cdb0f4b0732336a7f95b9193d2cebc839e5e535d4cec`) was
produced by that other process with

```bash
go test ./pkg/embed -run '^(TestNOR381.*|TestCachedEmbedder_CacheHit|TestCachedEmbedder_BatchCaching|TestCachedEmbedder_TypedCacheSeparatesQueryAndDocument|TestCachedEmbedder_LRUEviction|TestCachedEmbedder_Clear)$' -count=1 -p=1 -tags=noui,nolocalllm -v -timeout=180s
```

It shows all 12 `TestNOR381CachedVectorsBelongToEachResult` route/phase subtests
failing, both `TestNOR381DuplicateCachedBatchResultsAreIndependent` subtests
failing, and `TestCachedEmbedder_CacheHit`, `_TypedCacheSeparatesQueryAndDocument`,
`_BatchCaching`, `_LRUEviction`, `_Clear` passing. That recorded result agrees
with the source reading; this investigation did not produce it.

Not done here, and not claimed: no `go test`, build, benchmark, `-race` run, or
`make verify` by this investigation; no Linear/Plane read; no production or test
file edited; no edits outside this change directory; no commit or push. No
readiness claim is made, and the three untracked test files were read only.
