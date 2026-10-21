# Focused performance evidence (draft)

Source: `a6a23bfcf`, Apple M2 Max, macOS arm64, Go package benchmarks with
`-tags noui,nolocalllm`. These runs compare transaction accounting disabled
(`off`) and enabled with a permissive limit (`on`) on the same source head;
they do not replace base-versus-head whole-server benchmarks. Each query was
run in ten adjacent off/on rounds, alternating order, at 200 ms per sample:

```sh
go test -tags noui,nolocalllm ./pkg/cypher -run '^$' \
  -bench '^BenchmarkTransactionMemoryTracking$' -benchtime=200ms \
  -count=1 -benchmem -args -txmem.rounds=10
```

| Query | Median enabled latency change | Largest single-round slowdown | Median B/op change | Median allocations/op change |
|---|---:|---:|---:|---:|
| `collect` | +0.61% | +10.79% | +106 B | 0 |
| grouped collect | +1.22% | +2.72% | +38 B | 0 |
| topic-groups shape | +0.78% | +14.84% | +227 B | +0.5 |
| point lookup | -0.41% | +0.34% | +2 B | 0 |

The two isolated slow rounds do not establish a repeatable regression;
median changes are under the draft 5% latency gate. The separate admission
microbenchmark (`-benchtime=500ms -count=3`) measured 2.108–2.150 ns/op,
0 B/op, 0 allocs/op when disabled; enabled reservation and release measured
75.54–76.40 ns/op, 32 B/op, 1 alloc/op. CPU and heap profiles were also
captured for point lookup off/on at two seconds per mode: 46,614 ns/op off,
46,115 ns/op on, both 475 allocs/op. Neither profile showed a new dominant
memory-accounting frame; parsing and Go runtime allocation remained the
largest sampled costs. The profiling binaries and raw logs are local test
artifacts, not checked into this PR.

This is a focused check only. The OpenSpec gate still requires paired
base/head end-to-end throughput, latency, heap and CPU measurements across
workload sizes and container configurations before readiness.

## Bolt result-stream state after admission wiring

The pre-Bolt-admission integrated source (`a6a23bfcf`) and the current
database-owned Bolt admission implementation were each measured with five
300 ms samples on the same M2 Max using `BenchmarkBoltResultStreamState` and
`-benchmem`. CPU profiles were captured for both. This benchmark exercises
stream selection and partial-PULL state updates, including the no-limit path;
it does not include network round trips or query execution.

| Stream | Pre-admission median | Current median | Change | Allocations |
|---|---:|---:|---:|---:|
| Autocommit | 6.279 ns/op | 6.336 ns/op | +0.9% | 0 → 0 |
| Explicit transaction | 8.177 ns/op | 8.225 ns/op | +0.6% | 0 → 0 |

Result leases are looked up only at terminal PULL/DISCARD or cleanup. The
shared `QueryResult` and partial-PULL `resultStream` layouts are unchanged;
the enabled policy stores explicit-result leases in session-owned state.
The measured changes are within the draft 5% latency gate for this narrow
state-update workload. Whole-server base/head and varied result-size tests
remain required.

The same pre-admission source and code head `70a7760c2` were also measured
through an actual local Bolt TCP connection with `BenchmarkBolt_StreamRecords_EndToEnd_SmallRow`:
2,000 small rows per RUN/PULL, five 200 ms samples for each write-buffer size,
`-benchmem`, and a CPU profile on each source. Median latency and allocation
results were:

| Bolt write buffer | Pre-admission | `70a7760c2` | Change | Allocations/op |
|---|---:|---:|---:|---:|
| 8 KiB | 167,390 ns/op | 168,846 ns/op | +0.9% | 2,065 → 2,065 |
| 64 KiB | 197,615 ns/op | 198,061 ns/op | +0.2% | 2,065 → 2,065 |
| 256 KiB | 202,252 ns/op | 199,534 ns/op | −1.3% | 2,065 → 2,065 |

The benchmark runs with application admission disabled, so it checks the
compatibility-default overhead of the changed Bolt path. It is one small-row
workload on one host, not the full protected-mode or varied-size gate.

## Bounded buffered-WAL recovery

Apple M2 Max, macOS arm64, three `-benchtime=1x` samples per workload with
CPU profiles. `BenchmarkBufferedRecovery` creates 16 or 1,024 acknowledged
commits with an 8 KiB string property each, simulates loss of the buffer, and
measures replay only. Fixture setup, engine open and count assertions are
outside the timer. Every replay validates commit and label counts.
The baseline uses `825089c00` recovery source via Go's file overlay; all
other source and the benchmark fixture are identical.

| Commits | Baseline median | Bounded batches median | Change | Median allocs/op |
|---|---:|---:|---:|---:|
| 16 | 5.310 ms | 5.384 ms | +1.4% | 5,641 → 5,645 |
| 1,024 | 237.581 ms | 236.395 ms | −0.5% | 322,832 → 318,543 |

Command: `go test -tags noui,nolocalllm ./pkg/storage -run '^$' -bench
'^BenchmarkBufferedRecovery$' -benchtime=1x -count=3 -cpuprofile=<profile>`.
The baseline adds `-overlay=<mapping to the prior recovery file>`.
These are small samples, including one 338.854 ms current large-case outlier;
they do not establish whole-server performance. The bounded algorithm retains
an estimated 8 MiB batch plus the current decoded record, with an oversized
single commit applied alone. Neither peak RSS nor a hard allocation bound is
inferred from these allocation totals.

### Transaction accounting overflow guard

On the Apple M2 Max, `BenchmarkTransactionMemoryTracking` was run sequentially
against the preceding source through a Go overlay and the corrected source
(`-tags noui,nolocalllm`, 200 iterations, five samples, `-benchmem`). Median
latencies in microseconds/op:

| Mode / query | Prior | Corrected | Change |
| --- | ---: | ---: | ---: |
| disabled / group | 2,502 | 2,430 | -2.9% |
| disabled / point | 50.0 | 47.7 | -4.5% |
| enabled / group | 2,489 | 2,456 | -1.3% |
| enabled / point | 48.0 | 47.6 | -0.9% |

Allocation counts were unchanged in these cases. This establishes the scoped
accounting-overflow guard's performance, not whole-server memory protection.
The prior overlapping benchmark runs were discarded as noisy.

### Limit configured only on another database

`BenchmarkTransactionMemoryOtherDatabaseLimited` executes `RETURN 1 AS n`
against the unlimited `nornic` database while `limited` has a 1 MiB
transaction cap. The baseline uses a Go overlay of the preceding
`transaction_memory.go`; the current run uses the corrected source. Five
sequential 300 ms samples on the same Apple M2 Max, with CPU profiles:

| Source | Mean latency | Allocations |
| --- | ---: | ---: |
| Prior | 4,118 ns/op | 1,721 B/op, 28 allocs/op |
| Corrected | 4,066 ns/op | 1,721 B/op, 28 allocs/op |

The 1.3% lower mean is within measurement noise. The functional regression
test establishes that the unlimited database no longer attaches a tracker;
this microbenchmark does not establish aggregate throughput or the cost of a
database with its own limit enabled.
