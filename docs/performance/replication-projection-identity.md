# Database-scoped replication projection cost

Measured on Apple M2 Max, macOS arm64, with the Go toolchain selected by go.mod.
Release baseline: `9dcf1bba10f0709bec9472628dd9f3f0fa3b32f6`. Candidate: the
namespace-scoped projection implementation in this PR. The same existing
`BenchmarkStorageAdapterAcceptedProjection` ran sequentially on both versions,
with no concurrent local tests: five samples of 1,000 accepted one-node commands,
including the final WAL flush. Both runs had CPU profiling enabled.

| Version | Median ns/op | Range ns/op | B/op range | Allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Release | 57,000 | 56,850–66,111 | 20,921–20,981 | 336 |
| Candidate | 56,843 | 56,708–66,646 | 21,040–21,090 | 336 |

Timing ranges overlap; these samples do not establish a speedup. Allocation count
is unchanged. Roughly 0.5% additional bytes per operation retain database identity
in the journal's comparable key and publication request. The local accepted handle
also retains its immutable namespace after prepared-frame retirement. No
concatenated identity string is allocated. Wire commands reuse their existing
validated envelope; the FIFO writer does not decode transaction images again.

The short candidate CPU profile contains 460 ms of samples, dominated by runtime
condition-variable signaling/waiting and syscalls. It includes fixture/background
work, so it cannot attribute a precise per-operation CPU cost to identity hashing.
The benchmark does not qualify large retained-journal startup, multi-node network
throughput, or peak retained memory. Startup reconstructs identities from existing
transaction envelopes without changing the stored format.

```sh
go test -tags 'noui nolocalllm' ./pkg/replication -run '^$' \
  -bench '^BenchmarkStorageAdapterAcceptedProjection$' -benchtime=1000x \
  -count=5 -benchmem -cpuprofile projection.cpu
```

Correctness qualification separately covers public prepared commits into two
databases with one logical ID, fresh standby application, queued/published modes,
request-ID/envelope disagreement, concurrent failed applications, prune/reopen,
accepted-frame retirement, malformed history and original publication positions.
