# HNSW construction coordinate ownership

The deterministic navigation fixture fails before the correction: while a live
replacement has temporarily removed a navigation seed from the canonical index,
an unpublished catch-up insertion obtains coordinates from that live source,
returns success, but has no reachable edges. Membership and current vector-code
assertions pass before the public ANN assertion fails. The 515-node restored
fixture deliberately controls topology; it does not claim a naturally generated
6K graph or uniquely identify the captured historical CI failure.

The construction owner now retains the frozen coordinate source until catch-up
publication. Applied graph changes advance only their own coordinates, under the
graph mutex. Catch-up reuses immutable normalized canonical records instead of
retaining another vector-sized insertion array. Removal retires its applied
entry. Publication switches to the live source only after quiescence, while the
existing admission lock excludes canonical replacement; reference retirement is
constant-time under the publication locks. Closing a discarded graph first waits
for current coordinate consumers and removes its lookup, then releases its file
lease outside the graph lock. Published query lookup behavior is unchanged.

The mapping-unavailable ReadAt owner pins the same ordinal table and independently
owned file generation, rather than consulting live ordinals. Updates, deletion,
compaction and subsequent store closure cannot change that construction source.
Mapped little-endian reads retain their borrowed zero-copy contract. The copied
ReadAt/portable decoder is shared with ordinary file-row reads, preserving one
copying decoder. No stored bytes, key layout or storage version changes.

The unused buildHNSWForTransition forwarding method is retired. Its tests and
benchmark now use the production snapshot construction owner and explicitly
release the coordinate source; memory and file controls check their actual
source coordinates rather than an unrelated service lookup.

## Qualification

- The public-navigation fail-first is preserved in
  `/tmp/v150-hnsw-public-ann-failfirst.log` (0.685 s).
- Focused construction/catch-up/file-lease race checks pass in 5.617 s. The separately named initial/rebuild catch-up, publication-tail, owner-stop and
  snapshot-source fixtures also pass when selected explicitly.
- The original public `TestHNSWBuildEndDoesNotHoldCommits` passes both its first
  build and tombstone rebuild at 6,000 nodes / 128 dimensions (68.419 s).
- Active-lookup closure uses bounded existing fixture waits. Both mapped and
  forced-ReadAt sources retain old coordinates through canonical updates and
  compaction. Churn releases dead applied references, publication retires the
  baseline, and close completes once.

`BenchmarkHNSWReplayCoordinateRetention` compares two correct replay owners:
retaining each temporary normalized insertion array, versus replacing it with
its canonical immutable record. Both retain the same frozen baseline. Each sample
replays 512 vectors; three one-generation samples use CPU and heap profiles.
Assertions verify the actual pointer ownership of both arms. Per-generation
failure cleanup is registered immediately and runs outside measured work.

| Dimensions | Temporary-copy retained heap | Canonical-record retained heap | Replay ns/vector ranges |
| --- | --- | --- | --- |
| 128 | 513,040–513,480 B | 250,024–250,784 B | 96,084–102,299 vs 96,347–98,037 |
| 1,536 | 7,302,944–7,514,728 B | 4,368,104–4,376,824 B | 220,805–222,855 vs 221,718–224,524 |

Heap values are process-level post-GC deltas with graph, canonical source and
changed inputs kept alive. They establish the avoided retained-copy cost, not
peak process memory, file-backed retention, queue contention or public commit
latency. Allocation/timing includes canonical replacement plus graph replay;
setup, snapshot build, GC and cleanup are excluded. Final ordinary ready-query
and build baselines and integrated CI remain pending.


The 512-node / 64-dimensional ordinary lifecycle comparison uses three samples:
ready queries (1,000 operations/sample) retain exactly 154 allocations/op and
25,263–25,279 B/op. Median latency is 50.909→50.169 microseconds. Full build
(three builds/sample) measures 13.193→13.559 ms (+2.8%), with overlapping
5.505–5.523 MB/op before and 5.509–5.514 MB/op after; allocation ranges overlap
(61,718–61,735 before, 61,637–61,752 after). CPU profiles accompany each arm.
These qualify the named scalar in-memory workload, not all corpus sizes or file
formats. Source integration with statistics/PROFILE changes passes race controls
(search 5.654 s, Cypher 18.011 s). Final exact-head CI remains required.
