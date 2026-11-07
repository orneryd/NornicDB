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

## Checkpoint Save cost

A separate paired fixture saves 1,000 or 10,000 128-dimensional vectors, five
operations per sample and three samples. Baseline and candidate ran sequentially
with local tests stopped and CPU profiling enabled. Candidate includes the final
publication-error and schema-source review corrections after `c588e1328`.

| Vectors | Version | Median ms/op | Range ms/op | B/op range | Allocs/op range |
| --- | --- | ---: | ---: | ---: | ---: |
| 1,000 | Release | 9.81 | 9.63–10.42 | 81,123–81,196 | 2,061 |
| 1,000 | Candidate | 39.00 | 36.21–39.36 | 338,688–549,505 | 7,299–7,300 |
| 10,000 | Release | 10.61 | 10.42–11.01 | 679,443–889,534 | 20,089–20,090 |
| 10,000 | Candidate | 41.79 | 40.39–43.99 | 3,490,665–4,462,032 | 70,395–70,421 |

This is a substantial checkpoint-path cost, not a speedup: payload/association
integrity checks, metadata decoding and the durable publication decision add work
that the old unsafe publication omitted. CPU samples were dominated by syscalls
(72% flat), with fixture setup and runtime work also present; the short profile
does not isolate every individual synchronization cost. Temporary allocation is
reported, not peak retained memory. Population relocation additionally copies the
payload and is not included in this Save-only fixture.

The runtime-first choice avoids per-commit source markers or acknowledgements;
it does not make checkpoint persistence free. These results must accompany the
unchanged mutation allocation result when assessing the release tradeoff.

```sh
go test -tags 'noui nolocalllm' ./pkg/search -run '^$' \
  -bench '^BenchmarkVectorCheckpointSave$' -benchtime=5x -count=3 \
  -benchmem -cpuprofile checkpoint-save.cpu
```
