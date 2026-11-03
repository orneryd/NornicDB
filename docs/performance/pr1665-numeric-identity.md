# Numeric identity: PR #1665 qualification measurements

These measurements qualify the numeric JSON correction for NOR-68 against
release `df07c94fb7df5bb05a0c854cdaa0b8b6eae8182a`. The measured implementation
is `fdbf6568c76e9602972230828e6020e5ba00963f`, on Go 1.27.3, darwin/arm64,
Apple M2 Max. Local timing is nonblocking because this host is contended;
allocation counts, allocated bytes and retained heap were examined separately.
This report does not claim full release `make verify` qualification.

## Public export: matched release control

The workload is the public `apoc/export.Json` API: one node with 60,000 dynamic
fractional FLOAT properties, represented as a `values` array of `1.25`. Its output
is exactly 960,174 bytes in both arms. The baseline function is the release
implementation (`encoding/json.MarshalIndent` followed by `string` conversion),
run inside the same executable/toolchain as the candidate. This isolates the
API change rather than dependency/toolchain differences. It is not an ingest
or full query benchmark.

Each arm runs in an independent subprocess after both paths are warmed and two
GCs. Normal automatic GC remains enabled. Samples are balanced
baseline/current/current/baseline, 50 calls each. Mixed runs alternate a large
export and a one-value export; their per-iteration metrics include both calls.

| Workload | GOMAXPROCS | Release B/op | Candidate B/op | Release allocs/op | Candidate allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| Large | 1 | 2,342,123 | 1,975,736 | 22 | 6 |
| Large then small | 1 | 2,343,192 | 1,976,480 | 40 | 11 |
| Large | 12 | 6,119,570–7,168,529 | 3,111,156–3,195,393 | 51–60 | 16–17 |
| Large then small | 12 | 6,540,308–6,750,171 | 3,238,005–3,532,247 | 73–75 | 22–25 |

The multi-P figures include normal GC/pool turnover and show the measured range,
not an invariant allocation promise. The single-P large case reduces allocated
bytes about 15.6% and allocation count about 72.7%.

Retained heap is measured separately in balanced independent subprocesses:
warm both paths, GC twice, record heap, export once, discard output, GC once,
record heap while keeping the fixture live. At 60,000 values the release delta
is 1,114,664 bytes; the candidate delta is 1,048,888 bytes, about 5.9% lower.
At 1/8/32/64/128 values the candidate also retains no more than the matched
release. These are post-GC working-buffer measurements, not process RSS or an
unbounded-cache guarantee. `sync.Pool` can discard buffers on subsequent GCs.

## Canonical owner and rejected experiments

One recursive token writer handles compact and formatted JSON. Ordinary
containers write indentation directly; only opaque custom-marshaler fragments
use the standard indenter. Returned byte slices remain caller-owned. Formatting
strings are cleared before pooling. Small documents retain bytes.Buffer growth; large documents use geometric
size classes to avoid inheriting an oversized capacity sequence from an early
append. The 4 KiB boundary preserves the measured small-buffer behavior.

The intermediate compact-then-indent design retained an extra compact document.
Dropping its buffer above 64 KiB removed retention but added about 55% allocation
bytes and 16 allocations in repeated large exports, so that experiment was
rejected. A last-document-length hint was rejected after mixed-size controls.
The final implementation uses neither that hint nor a separate JSON capacity
scanner.

A 500-export CPU profile of the public path was captured separately. Its largest product frames were FLOAT formatting and token/indent
layout, with substantial GC/write-barrier work. There is no second ordinary
whole-document indentation pass. Profiled runs are separate from the allocation
measurements above, and no latency claim is based on the contended profile.

## Smaller output and required correctness costs

`BenchmarkPropertyJSONIndent` covers widths 1/8/32/64/128 against both the
intermediate two-buffer implementation and the standard encoder. Direct formatting used 2 allocations at all five widths versus 3–5 for the
intermediate design and 9 for the standard control. The final size-class owner
is measured separately by the large/mixed allocation and retention controls
above.

`BenchmarkToStringCost` compares the old `fmt.Sprint` owner with canonical number
formatting. For whole FLOAT widths 32 and 64, allocations fall from 23 and 55 to
zero; at width 128 they fall from 119 to 29 and allocated bytes from 272 to 154.
For `1000000.0`, correct Neo4j-compatible output is `1000000.0`, whereas the old
output was `1e+06`. Both paths allocate one returned string; the larger necessary
output/allocation class is accepted for compatibility, rather than claimed as
avoidable overhead. Existing larger-value controls prevent a small-token table
from hiding other values' costs.

The pinned maintained JSON decoder correction in
[go-json PR #2](https://github.com/orneryd/go-json/pull/2) fixes FLOAT32 maximum
rejection and destination-precision double rounding. Both ordinary fractions
and midpoint controls retain zero B/op and zero allocs/op. The previously
rejected maximum changes from 216 B/op and 3 error allocations to a successful
zero-allocation bind. Correct destination parsing has measured CPU cost:
`1.1` approximately 24–28 ns to 103–104 ns; the midpoint approximately 49–52 ns
to 102–108 ns. Exactly representable `1.25` remains approximately 24–26 ns.
This is documented correctness cost, not a speed improvement. Float64 binding
and persisted formats remain unchanged.

## Reproduction and evidence

Small-width and value-format controls:

```sh
go test ./pkg/convert -run '^$' -bench '^BenchmarkPropertyJSONIndent$' -benchmem -benchtime=2000x -count=2
go test ./apoc/convert -run '^$' -bench '^BenchmarkToStringCost' -benchmem -benchtime=2000x -count=2
```

The matched public baseline used for the large-export and retention comparisons:

```go
func baseline(nodes []*apocexport.Node) (string, error) {
    data := map[string]interface{}{
        "nodes": nodes,
        "relationships": []*apocexport.Relationship(nil),
    }
    encoded, err := json.MarshalIndent(data, "", "  ")
    return string(encoded), err
}
```

Construct `nodes` as one `apocexport.Node{ID: 1, Labels: []string{"Probe"},
Properties: map[string]any{"values": values}}`, with 60,000 `any` values each
holding `float64(1.25)`. Warm with the same shape containing one value. Measure
`runtime.MemStats.TotalAlloc`/`Mallocs` before and after 50 calls, divide deltas
by 50, and keep the fixture and last result alive. Compare baseline with
`apocexport.Json(nodes, nil)` in separate subprocesses as described above.

The complete harnesses, raw JSON samples, CPU profile, original REDs and gate
receipts are retained in the task evidence directory
`~/.local/share/nornicdb-pr-readiness/pr1665-current-release/`:
`export-allocation-comparison.go`, `final-buffer-export-allocations.json`,
`final-retained-export-memory.go`, `final-buffer-retained.json`,
`final-buffer-export-cpu.pprof`, and the final `make-test-release-final-*` receipts.
The PR qualification comment identifies the exact tested head/base and actual
local gate result. Older canceled/superseded runs are retained separately and
are not passes. Duplicate-checker analysis hit its deadline and is incomplete;
its advisory exit zero is not a clean analysis result.
