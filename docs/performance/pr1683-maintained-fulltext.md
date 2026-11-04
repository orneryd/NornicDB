# Maintained full-text performance (#1683 / NOR-148)

## Workload and reproducibility

Measured on an Apple M2 Max, macOS/arm64, Go 1.27.3. The baseline is release
`31208b17fe7df60cfab1a6062e91d95711abc9f3` (#1682 merged). The candidate ports
#1683 onto that release. Both arms use identical fixture and benchmark files;
recorded SHA-256 hashes establish this. Query-result caching is disabled, so every
execution reaches the public Cypher procedure. No private application data or
external database service is used.

The principal fixture uses native Badger in memory and generated correspondence
bodies of roughly 2 KiB. Every fourth node belongs to the named full-text index;
every fortieth matches the query, and every eightieth also satisfies the required
eligibility predicate. At 4,000 nodes this means 1,000 indexed documents, 100
matching documents and 50 eligible documents. Top-k and eligibility queries return
at most ten rows. Count queries assert the exact count; warmup asserts complete
rows and scores. Fixture stores and services close at the package boundary.

`MaintainedPublic` grows both indexed and unrelated data. `UnrelatedCorpusGrowth`
holds the index at 100 documents, ten matches and five eligible matches while
storage grows from 400 to 16,000 nodes. That experiment distinguishes repeated
whole-database reconstruction from necessary work on a growing index/result.
`CorpusReadState` measures the first query and first reuse separately, with a warm
parser and fresh search service for every operation. Setup/closure and, for first
reuse, its preceding query are outside timing. `DistinctTerms` cycles four term
sets, preventing a repeated statistics-cache hit while preserving exact counts.

```sh
go test -tags noui,nolocalllm ./pkg/cypher -run '^$' \
  -bench '^BenchmarkNOR148(MaintainedPublic|UnrelatedCorpusGrowth|CorpusReadState|DistinctTerms)$' \
  -benchtime=5x -count=5 -benchmem \
  -cpuprofile /path/to/output.cpu -memprofile /path/to/output.mem \
  -o /path/to/cypher.test
```

The tags exclude optional UI/model bindings from query-only qualification. Badger
and public execution are real. The separate readiness gate uses untagged
`make test`, an actually built UI, and a native library matching the checked-in
revision and headers.

## Results

Medians of five samples of five measured operations, baseline → candidate. Other
verification processes were active during parts of the paired runs; the machine
was not globally isolated. These latencies are descriptive workload evidence, not
a production SLO or a statistical-significance claim. Timed B/op and allocations
do not represent retained heap or RSS.

| Workload | ms/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Growing indexed corpus / nodes=400/top10 | 2.164 → 0.401 | 2,616,958 → 121,097 | 15,001 → 797 |
| Growing indexed corpus / nodes=400/count | 2.115 → 0.377 | 2,608,902 → 112,497 | 14,944 → 739 |
| Growing indexed corpus / nodes=400/eligible | 2.196 → 0.398 | 2,615,275 → 119,145 | 14,989 → 784 |
| Growing indexed corpus / nodes=4000/top10 | 25.236 → 3.216 | 25,966,331 → 1,077,891 | 148,220 → 6,111 |
| Growing indexed corpus / nodes=4000/count | 21.159 → 3.147 | 25,886,032 → 1,001,275 | 147,714 → 5,603 |
| Growing indexed corpus / nodes=4000/eligible | 20.945 → 3.217 | 25,947,153 → 1,060,068 | 148,126 → 6,014 |
| Growing indexed corpus / nodes=16000/top10 | 114.927 → 13.604 | 138,954,065 → 5,234,768 | 748,831 → 27,633 |
| Growing indexed corpus / nodes=16000/count | 114.282 → 13.434 | 138,372,188 → 4,931,820 | 746,167 → 25,627 |
| Growing indexed corpus / nodes=16000/eligible | 115.638 → 13.637 | 138,751,427 → 5,163,948 | 748,230 → 27,236 |
| Fixed indexed corpus / nodes=400/top10 | 2.110 → 0.400 | 2,616,888 → 120,435 | 15,001 → 794 |
| Fixed indexed corpus / nodes=400/count | 2.036 → 0.377 | 2,608,822 → 112,504 | 14,944 → 739 |
| Fixed indexed corpus / nodes=400/eligible | 2.026 → 0.403 | 2,615,204 → 118,752 | 14,989 → 782 |
| Fixed indexed corpus / nodes=4000/top10 | 9.132 → 0.397 | 14,803,932 → 120,486 | 123,007 → 794 |
| Fixed indexed corpus / nodes=4000/count | 9.063 → 0.378 | 14,796,188 → 112,291 | 122,950 → 737 |
| Fixed indexed corpus / nodes=4000/eligible | 9.059 → 0.405 | 14,801,961 → 119,136 | 122,995 → 784 |
| Fixed indexed corpus / nodes=16000/top10 | 56.304 → 0.416 | 87,102,515 → 146,921 | 641,679 → 879 |
| Fixed indexed corpus / nodes=16000/count | 58.148 → 0.393 | 87,047,708 → 138,852 | 642,014 → 823 |
| Fixed indexed corpus / nodes=16000/eligible | 58.506 → 0.420 | 87,119,065 → 145,238 | 642,277 → 868 |
| Read state / nodes=400/cold | 1.818 → 1.795 | 2,609,609 → 2,079,004 | 14,922 → 14,458 |
| Read state / nodes=400/first-reuse | 1.942 → 0.194 | 2,610,220 → 111,041 | 14,922 → 712 |
| Read state / nodes=4000/cold | 18.236 → 16.218 | 25,886,670 → 20,712,113 | 147,686 → 142,212 |
| Read state / nodes=4000/first-reuse | 17.716 → 1.246 | 25,894,689 → 1,002,219 | 147,694 → 5,580 |
| Changing term sets / nodes=400 | 2.097 → 0.378 | 2,615,070 → 112,244 | 14,942 → 736 |
| Changing term sets / nodes=4000 | 18.970 → 3.149 | 25,889,579 → 1,001,092 | 147,705 → 5,602 |
| Changing term sets / nodes=16000 | 110.656 → 13.366 | 141,571,019 → 4,935,564 | 758,915 → 25,603 |

At 4,000 nodes, repeated top-ten queries allocate about 95.8% fewer bytes and
95.9% fewer objects. The fixed-index experiment stays around 0.40 ms as unrelated
storage grows fortyfold. Its small residual allocation growth belongs to
backend/request bookkeeping; the result is not claimed to be zero-cost. Growing
the index or result population still requires more matching and hydration work.
First reuse and changing-term results show that repeated-query gains do not hide
an expensive secondary-index build or depend on query-result caching.

## Rejected representation and final statistics owner

A preceding candidate lazily built trigram postings on the second query. At 4,000
nodes, a separate three-sample qualification measured approximately 64.7 ms for
that call versus 17.8 ms on release. Its CPU profile identified `postBody` map
operations. This representation was removed entirely, not hidden by more warmup.

The retained view now keeps immutable bodies and only the most recent unique term
set's counts. One cancellable body pass computes all requested frequencies with
the existing literal `strings.Contains` predicate. Matching/ranking still belongs
to the existing evaluator. Term strings are cloned so they cannot pin a large
request buffer, the returned map is caller-owned, and every document replacement
or reset invalidates the counts. Request-local views retain no term cache.

The final first-reuse benchmark above uses the same public query shape and a fresh
service per operation. At 4,000 nodes it measures 1.25 ms, with no postings build.

## Profiles and retained memory

Both principal runs have CPU/allocation/post-collection in-use profiles. Their
aggregate profiles include fixture construction and all subbenchmarks: use the
per-operation table for query allocation comparisons. End-of-process in-use
profiles follow cleanup and do not establish live-corpus footprint or peak RSS.

The old PR's disk-backed large-document retention benchmark was ported to current
Go APIs and fixture registration. It keeps the executor/service alive, performs
twelve public count queries per operation, and measures heap before population and
after collection. Three samples cover 32 KiB, 256 KiB and 9 MiB documents, with
lowercase and mixed-case bodies. Every result must remain exactly one match.

```sh
go test -tags noui,nolocalllm ./pkg/cypher -run '^$' \
  -bench '^BenchmarkNOR148FulltextLargeDocumentRetention$' \
  -benchtime=1x -count=3 -benchmem
```

The unchanged one-GC measurement includes transient pooled decoder buffers and
parser state. Candidate median heap deltas were approximately 102/103 KiB for the
32 KiB bodies, 502/501 KiB for 256 KiB, and 29.27/29.27 MB for 9 MiB
(lowercase/mixed). Release's corresponding 9 MiB medians were near zero; it does
not retain the corpus. These measurements are disclosed as observed process-heap
cost, not all attributed to retained documents.

A separate diagnostic overlay wrote live heap profiles before and after one
lowercase 9 MiB case at full sampling. One GC left about 20.9 MB of additional
heap: approximately 9.45 MB in the normalized scoring body, 9.92 MB in
MessagePack `readNGrow` scratch buffers, plus parser/setup state. The decoder pool
and its implementation are unchanged by this PR. A second diagnostic with two
collections removed those transient pooled buffers: about 11.0 MB remained,
including the same 9.45 MB body and about 1.36 MB of ANTLR parser state. This
isolates one maintained normalized body rather than several retained entity
copies. It does **not** replace the one-GC figures or qualify peak heap/RSS.

Optional-corpus lifetime is separately covered by real application/runtime
pressure and refill tests, bounded change journals, concurrent readers/writers,
service retirement and public row hydration. Status includes `estimated_bytes`;
it is an estimate rather than an RSS guarantee. Admission uses current physical
usage without charging an already-live corpus twice. No new memory-limit setting
or operator environment variable is introduced. No storage format changes.

## Public behavior and evidence limits

A qualification-only common driver compared 21 public query shapes after initial
population, update, create and delete. All 84 comparisons have identical ordered
rows and printed floating-point scores across release and the final candidate.
The intended exact-field and cross-property phrase corrections have their own
node/relationship public regressions; they are not described as unchanged defects.
Both parsers, read failures, cancellation and service-reader release have focused
checks; the integrated full local gate is recorded separately.

The private R14 30-question sample is unavailable. Generated workloads reproduce
the reported top-k/count/eligibility shapes and scan mechanism; this is not a
replay of that application suite. NOR-351/352/353 vector-checkpoint changes are
separate and excluded. Existing persisted BM25/vector owners remain in place.

Raw logs/profiles/fixture hashes are retained under
`/Users/timothysweet/.local/share/nornicdb-pr-readiness/pr1683-release/`:
`{baseline,candidate}-statistics-bench.log`, `statistics-paired-medians.json`,
`statistics-paired-fixture-hashes.json`, `{baseline,candidate}-statistics.{cpu,mem}`,
`{baseline,candidate}-large-document.log`, `large-retention-{1gc,2gc}-top.txt`,
and `crosshead-public/statistics-comparison.json`. Historical failed and
superseded runs are preserved and do not establish final readiness.

The operator approved the generated R14-shaped workload as release acceptance on
4 November 2026, with private replay explicitly unavailable. The final untagged
`make test` passes all 147 packages (8 fresh, 139 cached) in 1,375.58 s; 8,317 recorded inputs stayed
unchanged. The reviewed follow-up changes only documentation and the constructor
reconciliation record, so the measured runtime and benchmark source is unchanged.
