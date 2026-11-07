# Vector checkpoint validation costs

Measured on Apple M2 Max, macOS arm64, Go toolchain selected by `go.mod`.
Release baseline: `9dcf1bba10f0709bec9472628dd9f3f0fa3b32f6`.
Candidate: `b5bcceef7362fab206606f2693b17d55360bb659`.
Local test processes were stopped before each benchmark; baseline and candidate
were run sequentially. Remote CI uses separate runners.

## Normal vector mutations

`BenchmarkVectorCheckpointMutation` appends/updates 128-dimensional vectors over
64 IDs. Five samples, 1,000 operations per sample, CPU profiling enabled on both
sides. This measures mutation, not Save/compaction or end-to-end ingestion.

| Version | Median ns/op | Sample range ns/op | B/op | Allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Release | 2,358 | 2,189–2,480 | 1,030 | 2 |
| Candidate | 2,274 | 2,168–3,444 | 1,030 | 2 |

Allocation cost is unchanged. Timing ranges overlap; these short samples do not
establish a speedup or a statistically qualified latency difference. The candidate
uses an epoch/value counter for vector generations instead of generating a UUID
on every mutation. No source marker or acknowledgement write is added to storage
commit paths.

```sh
go test -tags 'noui nolocalllm' ./pkg/search -run '^$' \
  -bench '^BenchmarkVectorCheckpointMutation$' -benchtime=1000x \
  -count=5 -benchmem -cpuprofile vector-mutation.cpu
```

## Source validation

`BenchmarkIndexSourceStamp` uses a temporary native memory engine populated with
Doc nodes carrying a text property. Three 100 ms samples at each population size.
The benchmark includes read-snapshot acquisition and metadata validation, not
hydrating vectors or building search indexes.

| Nodes | Median ns/op | B/op range | Allocs/op |
| --- | ---: | ---: | ---: |
| 100 | 44,473 | 7,664–7,670 | 177 |
| 1,000 | 384,758 | 8,528–8,535 | 181 |
| 10,000 | 3,755,531 | 8,560–8,640 | 183 |

CPU samples within source validation primarily show Badger iterator/value work
and SHA-256 updates. The full profile also contains fixture setup and runtime
background activity; it is not an isolated profile of only the timed loop.

Validation time scales with physical source metadata across the backing store,
including other namespaces. Working allocation in this fixture stays near 8 KB,
but this is not a production-scale disk or retained-memory qualification. It
must not be described as constant-time startup or zero validation work.

```sh
go test -tags 'noui nolocalllm' ./pkg/storage -run '^$' \
  -bench '^BenchmarkIndexSourceStamp$' -benchtime=100ms \
  -count=3 -benchmem -cpuprofile source-scan.cpu
```

## Restart tradeoff

A complete, unchanged certified population avoids a new population scan. Live
updates have no complete durable delivery acknowledgement, so an uncertified
checkpoint rebuilds after restart rather than trusting possibly missing updates.
The rebuilt checkpoint can be reused on subsequent unchanged restarts. This
moves the additional correctness cost to restart instead of adding a normal-write
acknowledgement ledger. Source validation still runs when checking reuse.

Checkpoint Save and compaction additionally hash payloads/metadata and synchronize
publication files. The mutation benchmark above does not measure that I/O cost.
