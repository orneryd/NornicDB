# Property-only updates with managed vector sidecars

Apple M2 Max, darwin/arm64, Go1.27.3, `noui,nolocalllm`. The generated fixture stores
1,024 chunks of256 float32 values (1MiB total). Each timed operation reads the full
node through the public storage API, changes an unrelated flag and commits through
WALEngine. WAL encoding remains included and unchanged. Setup precedes timing.

Baseline: release7f565720f. Candidate: f3aed4641. Four16-operation samples per arm,
ordered baseline/candidate/candidate/baseline (two samples per process), medians:

| Metric | Baseline | Sidecar reuse |
| --- | ---: | ---: |
| ns/op | 20,685,989 | 13,642,069 |
| ops/sec | 48.34 | 73.30 |
| B/op | 16,073,233 | 12,032,233 |
| allocations/op | 44,454 | 20,853 |

Latency decreases34%, allocated bytes25%, and allocations53% for this workload.
Process monitoring observed no competing Go test/build command during these samples.
An earlier overlapping trial was excluded. These are local bounded measurements,
not production-size, transport or concurrent-load qualification.

CPU and allocation profiles were also captured. Whole-process allocation profiles
include fixture construction and Badger startup; their percentages do not describe
only the timed updates. The physical-record regression proves the avoided work:
previously an unchanged chunk was rewritten and its metadata record removed; the
candidate retains both physical versions. Replacement vectors, changed metadata
and changed embedding source controls pass.

The existing embedding-source decision, fresh-sidecar reader and stored-value
comparator supply the reuse proof. Mere sidecar-key presence cannot establish that
the sidecar belongs to the current source. Canonical vector equality compares bits,
preserving signed zero and NaN payload distinctions.

## Remaining scope

This removes physical sidecar replacement only. Property updates still encode full
vectors into the WAL; the17.4MiB standard-record refusal and journal amplification
remain open release blockers. No WAL-byte reduction, storage migration or full
release-performance qualification is claimed.

```sh
go test -tags=noui,nolocalllm ./pkg/storage -run '^$' \
  -bench '^BenchmarkPropertyUpdateVectorJournal$/FlagUpdate$' \
  -benchmem -benchtime=16x -count=2 \
  -cpuprofile=sidecar.cpu -memprofile=sidecar.mem
```
