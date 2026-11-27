# FGAC search scale evidence

Base: `9a03bf90a616368b865690467936da481ab1c859` (`release/v1.5.3`).
Hardware: Apple M2 Max, Darwin arm64. Fixtures use Badger in memory,
2,000 or 5,000 indexed nodes, one restricted reader, warmed search paths,
and two fixed-iteration benchmark repetitions. The vector fixture forces CPU
exact search and disables GPU to hold the candidate strategy constant.

Commands: `go test ./pkg/search -run '^$' -bench '^BenchmarkFineGrainedKeywordSearch$' -benchtime=30x -count=2 -benchmem` and the same command with
`-bench '^BenchmarkFineGrainedVectorSearch(FourEntries)?$'`.

| Workload | Base time | Current time | Base B/op | Current B/op | Base allocs/op | Current allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Keyword, 5,000 nodes, unrestricted | 30.2-30.9 us | 30.3-30.5 us | 20,168 | 20,168 | 218 | 218 |
| Keyword, 5,000 nodes, restricted | 8.48-8.65 ms | 0.826-0.836 ms | 11.9 MB | 177 KB | 110,804 | 347 |
| Vector, 2,000 nodes, one entry, unrestricted | 85.4-85.7 us | 84.7-85.1 us | 26,096 | 26,096 | 168 | 168 |
| Vector, 2,000 nodes, one entry, restricted | 271-273 us | 194-196 us | 141 KB | 62-64 KB | 1,883-1,885 | 417-450 |
| Vector, 2,000 nodes, four entries, unrestricted | 409-410 us | 412-423 us | 712 KB | 712 KB | 275 | 275-276 |
| Vector, 2,000 nodes, four entries, restricted | 1.26-1.27 ms | 1.16-1.17 ms | 1.96 MB | 1.87 MB | 2,645-2,646 | 908-909 |

The keyword result measures steady-state, cacheable queries. Cold policy or
graph revisions still scan the index population once. Custom analyzer live
writes without composable field provenance use the authorized projection
fallback until a full rebuild. Exact vector completion and relationship
vector ranking still scan their indexed population when needed for complete
pages. These numbers do not establish P99 latency or RAM usage for other
population sizes, backends, policy cardinalities, or query distributions.

Final integrated checks: `go test ./pkg/search -count=1` passed in 264.002s;
`go vet ./pkg/search` passed; focused FGAC cache and provenance tests passed;
focused `go test -race ./pkg/search -run 'TestFineGrainedVectorVisibility|TestFineGrainedRestrictedKeywordSearchReusesVisibleCorpus|TestFineGrainedRestrictedKeywordCorpus(InvalidatesOnGraphChange|DoesNotCacheDecayReads)' -count=1`
passed in 2.099s of test execution. `git diff --check` passed. The
maintainer-directed PR gate does not include full release verification or a
local duplicate-check run; CI duplicate review and combined release
verification remain pending.
The final integrated benchmark rerun used the same fixture and reported
5,000-node restricted keyword at 0.825 ms/177 KB, unrestricted keyword at
29 us/218 allocs, one-entry restricted vector at 201 us/64 KB, and four-entry
restricted vector at 1.20 ms/1.87 MB.
