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
