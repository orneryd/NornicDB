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
