# Retained-result ownership qualification

The candidate isolates embedded APOC and query-cache results for NOR-306 and
NOR-307. NOR-310 additionally corrects live streamed property-list ownership:
projections reuse the detached scoring snapshot, and no-memo streams detach
at delivery through the existing structural copy owner. The additional
publication workloads below qualify that correction.

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

## Live statement publication (NOR-310)

The public continuation regression uses valid string-list properties, nodes and
relationships, result caching disabled, and decay on/off. The consumer changes a
list in the first delivered row before pulling the remaining rows. Release
bea145d51 exposes that changed value in later rows. Scoring snapshots already
owned their property values; projecting those snapshots protects the memo path
without another property copy. Other live streams detach at delivery, preserving
map-order metadata and refusing cancellation during copying.

The following six-sample medians use the same machine and ABBA controls above.
The outer result cache is explicitly disabled. `repeated_property` reads one
hidden property 100 times; `node_rows` returns one node across 100 rows with the
memo active; `plain_stream_rows` pulls 100 rows with decay off. Compared with the
unsafe release, the latter must now allocate independent mutable node images.

| Workload | Release ns/op | Candidate ns/op | Release B/op | Candidate B/op | Release allocs/op | Candidate allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| repeated_property | 258,341 | 257,955 | 28,189 | 28,188 | 262 | 262 |
| node_rows | 615,924 | 628,380 | 570,315 | 570,319 | 9,353 | 9,353 |
| plain_stream_rows | 78,733 | 118,141 | 23,360 | 80,161 | 346 | 946 |

The final candidate column comes from the follow-up ABBA pair against the first
correct copying implementation; these are separate paired samples, so small
cross-pair latency differences are not precise causal attribution. Default memo
workloads show unchanged allocations and effectively unchanged bytes; the
node-row median is about 2% higher across these samples. The plain stream's
additional retained output images are a documented correctness cost, not a
claimed performance win over shared mutable references.

An allocation profile of correct plain streaming attributed 14.7% of allocation
objects to the root list copier. Keeping the producer-owned single-column row
buffer and detaching only its value removes that unnecessary root copy;
multi-column rows still use one graph copy to preserve container aliases. In a
separate direct ABBA comparison this reduced correct plain streaming from
123,623 to 118,141 ns/op, 88,961 to 80,161 B/op, and 1,346 to 946 allocs/op.
No benchmark ran alongside local Go tests/builds; incomplete setup runs are
excluded. CPU and allocation profiles were captured for the live publication
workloads; no end-to-end ingest speed claim follows from these microbenchmarks.

```sh
GOMAXPROCS=4 GOGC=100 go test -tags=noui,nolocalllm ./pkg/cypher \
  -run '^$' -bench '^BenchmarkStatementPropertyPublication$' -benchmem -count=6
```

The over-budget projection is covered by ownership component tests using an
already constructed large read image. It is not a large persisted-payload
benchmark: the in-memory Badger fixture rejects values over its 64 KiB inline
limit, so the attempted oversized public fixture is excluded from results.
