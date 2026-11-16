# Automatic snapshot compaction and foreground writes

Automatic compaction previously held the WAL engine's mutation barrier while
encoding, syncing and renaming the entire snapshot. Slow file output therefore
prevented foreground writes from reaching storage.

Native Badger compaction now flushes acknowledged buffered writes and acquires
one stored-inclusive read point under that barrier. It releases the barrier
before encoding the snapshot. A separate compaction lock serializes snapshot
files, truncation and cleanup; WAL records after the captured cut remain for
recovery. Custom engines without this native pinned reader retain their existing
exclusive snapshot acquisition.

The pin includes separately stored vectors, suppressed entities and physical
dictionary bindings. Namespace deletion/recreation cannot change the node IDs
or property names emitted for the captured relationship bodies. It reuses the
ordinary scan and binding readers rather than copying the graph or dictionaries.
Badger retains versions needed by this read point until the file operation
finishes and closes it; a slow snapshot can therefore retain older disk versions
for longer.

## Verification and limits

A deterministic test parks actual snapshot-file output, completes foreground
update/create operations, then resumes the file. The snapshot retains the old
image, while snapshot plus remaining WAL recovers both subsequent writes. A
second compaction includes those writes, and an idle compaction does no new work.
The old held-barrier implementation fails the same test.

A separate test pins relationships, deletes and recreates their namespace, then
checks both the strict decoder and actual snapshot file for the original
endpoints and properties. The old live-dictionary transcoder fails this test.

Integrated parent race checks for pins, native source reads, transcoding and
foreground overlap pass. These deterministic checks establish barrier ownership
and recovery correctness; they are not production-size latency or peak-memory
measurements. The production-size cost measurement is in
[Production-size cost qualification](#production-size-cost-qualification).

## Production-size cost qualification

Measured on release `a492f2acd` with a test-only harness, Apple M2 Max (12 cores,
32 GiB), Darwin arm64 and Go 1.27.3. Each case seeds its own temporary Badger/WAL
store and performs one untimed warmup snapshot. The measured call is the same
`createSnapshotAndCompact` / `saveCompactionSnapshot` pair used by automatic
compaction. Production code and defaults are unchanged.

`BenchmarkSnapshotCompactionQualification` uses two generated populations:

- 40,000 nodes with one 20,480-byte property each and 40,000 relationships.
  Properties have distinct ID prefixes and repetitive filler: 781 MiB of logical
  property data, compressed in Badger but uncompressed in the snapshot.
- Initially 4,000 nodes with eight 256-dimensional managed vectors each, plus
  1,000 relationships. Foreground writes use ordinary
  property-update requests; modified nodes are not re-embedded. This is not a
  named-vector workload.

Four foreground writers are paced at 20 ms each. A 200-write baseline runs
without compaction before each case. During overlap, the same writers update
properties and occasionally create nodes. This measures write latency at that
bounded rate, not maximum throughput or HTTP/Bolt latency.

### Observed costs

One profiled sample per case; these are diagnostic measurements, not a latency
SLA or an old-versus-new production comparison.

| Population | Foreground during snapshot | Snapshot | File | Allocated during operation | Sampled peak Go heap |
| --- | --- | ---: | ---: | ---: | ---: |
| 40,000 × 20 KB | idle | 1.276 s | 798.8 MiB | 1,625 MiB | 1,573 MiB |
| 40,000 × 20 KB | 240 writes | 1.223 s | 798.8 MiB | 1,717 MiB | 1,552 MiB |
| Managed-vector fixture | idle | 518 ms | 39.57 MiB | 204.1 MiB | 180.5 MiB |
| Managed-vector fixture | 100 writes | 520 ms | 39.57 MiB | 207.7 MiB | 183.4 MiB |

| Population | Overlap p50 / p95 / p99 / maximum | No-compaction baseline p99 / maximum |
| --- | --- | --- |
| 40,000 × 20 KB | 167 µs / 655 µs / 23.31 ms / 23.50 ms | 225.01 ms / 225.07 ms |
| Managed-vector fixture | 89 µs / 4.00 ms / 7.63 ms / 7.67 ms | 4.22 ms / 5.70 ms |

The property baseline also had a 239.75 ms maximum in the idle case. Those
outliers occur without compaction and are not attributed to the snapshot.
The observed overlap writes did not wait for the full 1.223-second snapshot.
Small sample counts and shared-host variation do not establish a general
percentile guarantee.

Allocation and RSS counters are captured before snapshot read-back validation.
They include concurrent foreground work and sampling overhead. Allocation is
cumulative work, not retained memory. Go-heap peaks are sampled every 100 ms.
Process RSS high-water marks span setup and earlier cases: 4,246–4,693 MiB here,
so they are not isolated per-operation peaks. Post-GC heap changes were about
−252 MiB for the property cases and +0.11/+0.14 MiB for the vector cases; pool
turnover makes the negative values unsuitable as a memory-saving claim.

### Recovery and disk observations

After the writers join, recovery into a fresh store compares every fixture
node's ID, labels, properties and managed-vector bits, and every relationship's
ID, endpoints, type and properties with the live store. These comparisons pass:

| Population / mode | Recovery | Nodes / relationships | Remaining WAL records |
| --- | ---: | ---: | ---: |
| Properties / idle | 5.595 s | 40,004 / 40,000 | 1 |
| Properties / overlap | 7.217 s | 40,012 / 40,000 | 237 |
| Vectors / idle | 795 ms | 4,004 / 1,000 | 1 |
| Vectors / overlap | 1.081 s | 4,004 / 1,000 | 97 |

Follow-up vector controls compared 30,432 populated managed-vector chunks
bit-for-bit in each mode and reject a vacuous zero-vector comparison. The seed
has 32,000 chunks before the ordinary foreground baseline writes.

The extra nodes are foreground creates. Recovery reads a quiescent, flushed
journal; it is not a killed-process test. Timestamps, non-populated named vectors
and internal storage-layout keys are outside this comparison.

Two property snapshots occupy about 1,598 MiB: warmup plus measured output.
The property store directory stayed at 436.5 MiB, with about 51.5 MiB in
compressed LSM tables. The vector store remained at 385 MiB, mostly preallocated
files and unflushed data. No extra store/table bytes were resolved by the
100 ms samples. This does **not** prove zero retained-version cost: the update
window is short, and preallocation/memtables hide smaller changes. Larger WAL
truncation intervals, sustained churn and production disks remain unqualified.

### Reproduce and profile

```sh
go test -tags noui,nolocalllm ./pkg/storage -run '^$' \
  -bench '^BenchmarkSnapshotCompactionQualification$' -benchtime=1x \
  -timeout=90m -cpuprofile=compaction.cpu -memprofile=compaction.mem

go tool pprof -top -focus=runOperation -ignore=newSnapshot compaction.cpu
go tool pprof -sample_index=alloc_space -top \
  -focus=runOperation -ignore=newSnapshot compaction.mem
```

The filters exclude fixture setup, warmup, recovery and diagnostic snapshot
read-back. Across the four measured operations, the allocation profile is led
by Badger block decompression (about 1.53 GiB) and decoded property strings
(about 1.51 GiB). CPU samples are mostly file-write syscall frames. These are
aggregate measured-operation profiles, not per-query costs.

The benchmark must run without other local tests or inference work. Normal
package tests compile it but do not execute its large fixtures. It uses only
throwaway data and joins writers/samplers before their stores are closed.

## Existing stream allocation check

An isolated ABBA comparison of the unchanged 1,000-node stream benchmarks used
release `7f565720f` and candidate `e8bf08f98` on an Apple M2 Max, Go 1.27.3,
GOMAXPROCS=4. Each side has four samples of 20 iterations; no local Go test ran
alongside them.

| Existing workload | Release median | Candidate median | Allocations / scan |
| --- | --- | --- | --- |
| Full node stream | 393.9 µs; 690,978 B | 412.5 µs; 691,234 B | 7,216 → 7,222 |
| Projected options stream | 939.6 µs; 703,282 B | 943.0 µs; 703,545 B | 17,779 → 17,785 |

Full-stream samples ranged 376.9–443.4 µs on release and 399.3–734.5 µs on the
candidate; the upper candidate sample remains in the comparison. The extracted
read-view path adds six allocations and about 256 bytes per scan, under 0.1%
for these workloads. This checks extraction cost only; the production-size
measurement above covers the 40,000-node property population and a bounded
vector-heavy population. No production-size timing target is claimed from
either.
