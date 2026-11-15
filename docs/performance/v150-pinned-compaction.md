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
