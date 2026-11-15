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
measurements. Workload cost qualification remains pending.

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
for these workloads. This checks extraction cost only: it does not qualify
40,000-node snapshot write latency, vector-heavy streams or peak disk retention.
No production-size timing target is claimed from it.
