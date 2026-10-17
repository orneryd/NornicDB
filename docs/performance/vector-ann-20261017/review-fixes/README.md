# Review corrections — October 17, 2026

Addresses both P2 findings and the measurement clarification in the
[final source review](https://github.com/orneryd/NornicDB/pull/1372#issuecomment-6234131718).
Frozen checked source: `975da2776c729bf72b93a7bcbbafb1e685cbc176`.
Checked release base: `f2ddd27cfae3788076778a0a0d48c827df046a3b`.
Later edits are report documentation only; runtime source is unchanged.

## Corrected behavior

- Cached `Service.Search` responses are separated by HNSW beam and effective
  quantization ignore/rescore/oversampling. Cache identity and pipeline scoring
  share one default resolver. Unset rescore means true, remains distinct from
  false, and zero oversampling resolves to one.
- Changing a service compression policy invalidates cached results and advances
  the existing index revision. Reapplying the same override retains the existing
  no-op behavior. The pipeline is still rebuilt for a changed policy.
- `ClearIndexes` resets the collection compression override with the retired
  collection state. Reused providers inherit the configured service default
  after recreation. `ClearVectorIndex` preserves the collection policy during
  embedding regeneration. Other collections retain their own policies.
- The report now states that sequential queries supply recall and latency;
  the eight-client pass supplies throughput without separate recall validation.
  This benchmark's vector-query path applies 1.5× initial overfetch: top-10
  requests reach up to 15/30/120 candidates at 1×/2×/8× oversampling. Matching
  request parameters does not establish identical cross-engine candidate work.

## Proof and results

Both public reproductions failed before the fix:

- [`before-search.log`](before-search.log): cached approximate/exact scores leaked
  across rescore and ignore settings in both orders. Different oversampling and
  beam requests returned the same cached response. Policy changes retained a
  populated result cache; collection clearing retained the binary override.
- [`before-qdrant.log`](before-qdrant.log): after dropping/recreating a collection
  through a reused provider, the gRPC API returned binary score 0.5 instead of
  approximately 0.7071 under both global `none` and scalar defaults.

The same tests pass after the correction ([transcript](after.log)). Tests
exercise the actual result cache, including repeated-query cache hits and
returning to the first request after the second. They test both request orders
for unset/explicit rescore, ignore, oversampling and beam; policy transitions in
both directions; embedding-only versus collection retirement; reused-service
identity, fresh collection metadata and a second binary collection as an
isolation control. Global float and scalar defaults are both exercised.

```sh
go test ./pkg/search ./pkg/qdrantgrpc \
  -run 'TestSearchCacheSeparatesQuantization|TestSearchCacheRetiresChangedQuantization|TestClearIndexesRetiresOnlyCollectionCompression|TestQuantizedDropRecreateUsesDefaultAndPreservesOtherCollection' \
  -count=1
go test -race ./pkg/search ./pkg/qdrantgrpc \
  -run 'TestSearchCache.*Quantization|TestClearIndexesRetiresOnlyCollectionCompression|TestQuantizedDropRecreate|TestSharedIndexDropRecreateRetires|TestSearchSharedIndexDefaultDropRecreate|TestConfigureHNSWQuantization' \
  -count=1 -coverprofile=/tmp/nornic-monster-review-focused.cover
make test
```

- Fresh focused public tests: search **0.569s**, Qdrant **0.941s**.
- [Focused races](focused.log): search **1.955s**, Qdrant **2.357s**, passed.
  [Function coverage](focused-functions.txt): shared resolver **100%**,
  policy configuration **96.7%**. Whole-package coverage in this focused run was
  only 16.0% / 22.6%; it is not a full coverage run. Existing filter/file-error
  branches lie outside these specific reproductions.
- [One complete `make test`](make-test.log): **passed, 94 packages, 25 fresh and
  69 cached**. Fresh complete packages: search **201.731s**, Qdrant **10.930s**,
  server **202.724s**, storage **123.234s**. Logging/cardinality gates passed.
  There were no failures, retries or cancellations in this full run. These are
  package-test durations, not request latencies. Make filters individual skips;
  unavailable external fixtures are not claimed as executed.
- `make verify` remains skipped per the maintainer's explicit instruction.
  No GitHub Actions were dispatched; no merge was performed.

## Benchmark and hardware scope

No ANN algorithm, construction budget, SIMD kernel or native backend changed in
this correction. Existing numerical reports retain their recorded source:
`7c201376d...` for the 15-mode sweep and `c5ef90e3...` for the latest IVF-HNSW
run. The timed direct vector-query path bypasses `Service.Search`'s result cache
and uses fresh services rather than the drop/recreate reuse scenario. The
request-default helper extraction preserves the existing scoring semantics,
covered by the public package tests. No new-head latency/recall measurement or
competitor execution is claimed here. CUDA/Vulkan/native AMD64 and BEIR SciFact
limits remain as recorded in the [earlier verification](../verification.md).
