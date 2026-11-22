# N152-134 verification

Base f77ec7706de099588ac232b32b6908860511d3da. Unless noted, Go commands use
`env GOMAXPROCS=2 GOMEMLIMIT=1500MiB` and `-p=1 -tags=noui,nolocalllm`.

- Original exact reproduction plus five existing controls: `go test ./pkg/embed -run '^(TestNOR381.*|TestCachedEmbedder_CacheHit|TestCachedEmbedder_BatchCaching|TestCachedEmbedder_TypedCacheSeparatesQueryAndDocument|TestCachedEmbedder_LRUEviction|TestCachedEmbedder_Clear)$' -count=1 -timeout=180s -v` with GOGC50: expected FAIL0.595s;12 route/phase and2 duplicate-hit cases fail, five controls pass.
- Additional baseline binary run `-test.run='^TestCachedEmbeddingOwnership' -test.count=1 -test.timeout=2m -test.v`: expected FAIL for deterministic concurrent miss double-check and shared-buffer batch-miss ownership. Mutations occur after the two concurrent calls finish, so the test introduces no shared-vector write race.
- Fixed focused normal: `go test ./pkg/embed -run 'TestCachedEmbedder|TestCachedEmbeddingOwnership|TestNOR381|TestCheckEmbeddingsAndCacheRefusesUnusable' -count=1 -timeout=5m -v`: PASS0.504s.
- Whole package: `go test ./pkg/embed -count=1 -timeout=5m`: PASS0.556s with the stated tags. Native local-model and separately integration-tagged provider tests were not executed.
- Same focused selection with `go test -race`: PASS1.477s; no race warnings or selected test skips.
- `go vet ./pkg/embed`: exit0, empty output.
- `make verify-list BASE=origin/release/v1.5.2`: exit0, plan only.
- `env GOMAXPROCS=2 GOMEMLIMIT=1500MiB make verify BASE=origin/release/v1.5.2 ONLY=localization`: exit0. UI, llama-libs, build, full tests, allocations, conformance, differential and docs unselected. Full release/default-tag/platform qualification remains pending.
- Matched benchmarks and CPU profiles: see benchmark.md. Explicit correctness cost, no performance-qualified claim.
- `scripts/code-intelligence-hook.sh check-staged`: exit1, analysis/ingestion failure at configured localhost7474. Current-source manual review below completed; indexed discovery remains incomplete. CONTRIBUTING permits advisory duplicate analysis for review candidates; no hook disabled.

Manual reuse review: cache storage/return owners remain Embed and
embedBatchWithKeyFunc. Typed single/batch and untyped fallback continue through
those owners; document chunk fallbacks inherit them. Standard slices.Clone
avoids adding a duplicate local clone helper or importing a query-layer helper.
Retained slices are immutable and never exposed; entry promotion/eviction does
not mutate their elements. Only copies are returned or retained. No changes to
provider validation/error branches, key hashing, response arbitration, LRU,
counters, cache size or public API. DeepSeek source proposal was reviewed;
its suggested local helper and batch value-convergence change were rejected.

Original fixture/logs remain verbatim, including transcript whitespace; Go and
Markdown whitespace checks pass. Open PR resources and benchmark binaries are
retained; shared Go cache is actively used by other release work. No remote
containers or images were created for this fix.
