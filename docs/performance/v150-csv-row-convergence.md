# CSV row construction

Node and relationship exports now share one row-construction loop. Each row is
allocated once, with its identity fields followed by the inferred properties.
The exporter no longer builds a separate property-cell matrix and then copies
it into a second set of rows.

## Measurement

Both sides implement the corrected CSV value contract. The baseline restores
only `neo4j_csv.go` from candidate `bc8496b53` through a Go build overlay; all
other sources and the benchmark fixture are identical. This compares the row
consolidation, not an entire release against its predecessor.

The workload exports 1,000 prebuilt nodes or relationships, each with a string,
an empty string, a boolean and a two-value float64 list. Each iteration replaces
one temporary CSV file. On Apple M2 Max, Go 1.27.3, GOMAXPROCS=4, an ABBA run
collected four samples per side, 20 iterations per sample, with no concurrent
local Go tests.

| Export | Before median | After median | Allocated bytes | Allocations |
| --- | --- | --- | --- | --- |
| Nodes | 0.914 ms | 0.899 ms | 578,490 → 409,902 (-29.1%) | 20,015 → 18,014 (-10.0%) |
| Relationships | 0.955 ms | 0.914 ms | 674,624 → 458,007 (-32.1%) | 20,015 → 18,014 (-10.0%) |

Node timings ranged 0.878–0.997 ms before and 0.827–1.166 ms after. Relationship
timings ranged 0.946–0.972 ms before and 0.895–0.948 ms after. All samples are
included. Timing ranges overlap; the demonstrated gain is lower allocation,
without a statistically qualified latency-speedup claim. CPU and allocation
profiles were captured separately on both binaries; their totals include fixture
setup and are not query-only percentages.

Reproduce the workload with:

```sh
go test -tags nolocalllm ./pkg/adminimport -run '^$' \
  -bench '^BenchmarkCSVEntityExport$' -benchmem -benchtime=20x -count=4
```

A separate writer-only benchmark repeatedly encodes a prebuilt, unquoted 1MiB
field and reports 0 B/op and 0 allocs/op in three 100-iteration samples. It excludes
row construction and file I/O. Large quoted-field correctness is covered by the
Badger-backed round-trip test; no zero-allocation claim is made for that workload.
