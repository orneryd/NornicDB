# Retained-result ownership qualification

The candidate isolates embedded APOC and query-cache results for NOR-306 and
NOR-307. NOR-310 has bounded consumer/lifecycle regression coverage; no
additional statement-view production copies are justified by those routes.

## Workload and measurement

Apple M2 Max, darwin/arm64, Go 1.27.3; GOMAXPROCS=4, GOGC=100. Test binaries
were built before timing. ABBA order, three 200 ms samples per invocation,
six samples per arm; medians below. No local Go tests or builds overlapped.
Short microbenchmarks are workload-specific, not application throughput claims.

The cache and adjacency baselines are the **first correct ownership candidate**
539f78c7d, not the unsafe release implementation. Detaching returned values
necessarily costs more than returning a shared reference; these measurements
qualify removing avoidable work from the correction, not zero-cost isolation.

| Workload | First correct ns/op | Tuned ns/op | First correct B/op | Tuned B/op | First correct allocs/op | Tuned allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 100-row scalar/list smart-cache hit | 42,515 | 22,295 | 56,408 | 23,172 | 825 | 712 |
| 100-row entity smart-cache hit | 51,975 | 41,052 | 86,392 | 71,180 | 1,123 | 1,112 |
| 64-edge degree count | 81,263 | 194 | 75,256 | 0 | 1,671 | 0 |
| 64-edge star, one-hop shortest path | 5,377,711 | 16,142 | 5,005,446 | 20,456 | 111,131 | 194 |

The cache walker reuses a cleared overflow identity map (at most 1,024 recorded
references), retaining no caller values between copies. Typed map/list dispatch
uses the same existing traversal and alias/cycle memo. Degree counts visit
metadata without materializing properties; paths detach only returned members.

The native storage copier was already correct on the release baseline
bea145d518c9ae2c9df7ced456b245321cad9565. Its separate controls compare that
baseline against the tuned shared owner:

| Native node copy | Release ns/op | Tuned ns/op | B/op both | allocs/op both |
| --- | ---: | ---: | ---: | ---: |
| 8 flat properties | 257 | 243 | 496 | 4 |
| 64 flat properties | 1,948 | 1,775 | 5,112 | 6 |
| 8 nested properties | 1,756 | 1,798 | 3,824 | 44 |
| 64 nested properties | 14,063 | 14,370 | 31,736 | 326 |

Nested controls show about 2.2–2.4% higher median time in this sample with
unchanged bytes and allocations; this is not proof of identical latency.

## Profiles and reproduction

The allocation profile attributed 57.7% of initial scalar-cache copy bytes to
identity-index growth. The degree allocation profile attributed 98.4% to copying
relationships that a count never returns. Darwin CPU sampling was dominated
by runtime.kevent, so it does not establish precise CPU attribution.

```sh
GOMAXPROCS=4 GOGC=100 go test -tags=noui,nolocalllm ./pkg/cypher \
  -run '^$' -bench '^BenchmarkNOR307QueryResultCache$' -benchmem -count=6
GOMAXPROCS=4 GOGC=100 go test -tags=noui,nolocalllm ./pkg/storage \
  -run '^$' -bench '^BenchmarkNativeEntityCallerCopy$' -benchmem -count=6
GOMAXPROCS=4 GOGC=100 go test -tags=noui,nolocalllm ./apoc/storage \
  -run '^$' -bench '^BenchmarkInMemoryStorage(DegreeMetadata|ShortestPathOwnership)$' \
  -benchmem -count=6
```

Use matched fixtures on both revisions, build before timing and interleave runs
for comparisons. Full integrated readiness still requires passing explicit-write
integration and exact-head make-test CI. These benchmark results alone are not
a merge-readiness claim.
