# Projected-read tuning evidence (Mac M2 Max, 2026-11-27)

## Immutable baseline

The four fresh-store Northwind and mixed-corpus baseline reports were published
at source `b0332923ff6112ebde1a94959a50ea400c25f805` before tuning. See
[`baseline/summary.md`](../../../testing/benchmarks/projected_reads/results/matrix-2026-11-27/baseline/summary.md).
The baseline ran serially, with 30 measured and five warmup iterations per
Northwind query, then 4,000 calls and 64 warmups per mixed phase.

## Profiled bottleneck and accepted change

The baseline projected low-stock query completed 67 calls in 50 seconds on
standard layout and 71 on experimental layout, versus 135 and 144 for ordinary
stores. A projected-store CPU profile attributed 9.84 of 30 sampled seconds
to `readProjectedNodeItemInTxn`, 9.81 to `readPhysicalProjectedNode`, 5.20 to
`readStoredPropertyToken`, and 3.67 to Badger `Item.ValueRange`. The public
Cypher path uses the unfiltered projected label reader for this query.

The retained correction memoizes property-name bindings only within a single
Badger read view and reads a small inline value once. It retains bounded
`ValueRange` reads for large records. A cold 4,096-Product label-scan benchmark
with node caches cleared between iterations measured projected modes at
17.2–19.3 ms/op and 16.2 MB/op before tuning, versus 10.5–11.8 ms/op and
12.2 MB/op after both changes. Ordinary modes remained near 7 ms/op and
6.26 MB/op. In an end-to-end 25-second low-stock driver on copied stores,
standard projected completed 41 before and 69 after; experimental projected
completed 44 before and 87 after. The standard ordinary-store control was
80 before and 78 after. These paired drivers are separate from the full
Northwind report and do not establish every query's performance.

## Rejected and unresolved alternatives

- A first small-body experiment did two `ValueRange` calls before a whole
  `Value` read and added a helper closure. It increased cold-scan allocation
  to 17.3 MB/op and 327,737 allocations/op, without a latency gain. It was
  reverted before the accepted direct-single-read version.
- Borrowing the inline directory instead of copying it removed 4,096
  allocations and about 262 KiB per 4,096-node scan, but alternating
  before/after runs measured median projected scan time 1–2% slower in both
  layouts. It added a second directory path and was reverted.
- The baseline mixed-corpus random-wide p95 gap (experimental projected
  1.942 ms versus ordinary 0.567 ms) did not reproduce as a stable ranking
  in a separate 20,000-call copied-store run: the projected baseline measured
  1.008 ms and the ordinary baseline 1.769 ms. The candidate projected
  measured 0.920 ms. Short phases and environmental variation preclude a
  causal claim; the final four-mode matrix and result hashes remain the gate.
- Repeated dictionary lookups on a single point read remain snapshot-bound.
  Using the global current dictionary without a read-view proof risks assigning
  a dropped and recreated namespace's new meaning to an old record. The
  scan-local memo is the measured safe scope.

The final profiled small-node pass found one safe additional improvement:
reuse one Msgpack reader and decoder for all selected property regions of
one node. Alternating A/B cold scans measured standard projected median
10.95 to 10.45 ms and experimental projected median 10.96 to 10.43 ms;
each avoided 8,192 allocations and about 0.39 MB per scan. The 2 MiB
encrypted narrow-read control stayed at 6.56 versus 6.61 µs median, 457
logical value-log bytes, 9.8 KB/op and 99 allocations. Focused encrypted,
frame, cache and decoder tests passed. The final four-mode matrix was run on
this accepted source; see the separate report below.

## Intermediate full matrix and paired regression audit

The first tuned four fresh-store reports used the same Mac and workload at source
`538e66d1f413aac8de05440f3aa1c486b1475f67`. Their raw local receipts
were retained separately from the published baseline and final report; this
intermediate source precedes the decoder-reuse change.
All 14 Northwind query fingerprints and all eight mixed-phase fingerprints
matched across every cell, and each cell verified all 4,352 document bodies.
Standard projected low-stock fell from 738 to 460 ms mean; experimental
projected fell from 750 to 318 ms. Ordinary-store low-stock was 240 to 243 ms
on standard layout and 311 to 236 ms on experimental layout; the latter
control shift demonstrates run-to-run variation.

The one-pass matrix showed standard projected category revenue and supplier
revenue 26% and 16% slower than its earlier baseline. A separate alternating
copied-store Bolt check ran eight measured calls per query per version in each
of two orders. Category revenue was 5.43 s baseline versus 4.36 s candidate
in round one, and 4.09 s baseline versus 4.16 s candidate in the reverse
round. Supplier revenue was 5.05 versus 4.40 s, then 4.69 versus 4.06 s.
The two flagged short queries changed direction between rounds. These
regressions did not reproduce. The experiment supports keeping the measured
scan improvement while treating small cross-run differences as uncertain.

## Final four-mode report and control audit

The [final report](../../../testing/benchmarks/projected_reads/results/matrix-2026-11-27/final/summary.md)
measures source `b5666cd157caa2dfd7fd8e4345b12e9b2e0f9387` on four
fresh isolated stores. One brief verifier-plan self-test overlapped the first
standard nonprojected cell; that cell was discarded and rerun alone. The
published cells are serial. All 14 Northwind fingerprints and eight mixed
fingerprints matched across modes, and 4,352 complete document bodies were
verified in each mode. The per-mode [comparison](../../../testing/benchmarks/projected_reads/results/matrix-2026-11-27/final/comparison.md)
links each query, phase, p95/p99, and result check.

| Layout | Projected | Northwind mean ms baseline -> final | Random narrow p95 ms baseline -> final | Random wide p95 ms baseline -> final |
|---|:---:|---:|---:|---:|
| standard | false | 2601.1 -> 2298.6 | 0.845 -> 0.731 | 1.277 -> 1.606 |
| experimental | false | 2726.6 -> 2247.9 | 0.344 -> 0.370 | 0.567 -> 0.556 |
| standard | true | 2873.9 -> 2343.1 | 0.502 -> 0.377 | 0.808 -> 0.515 |
| experimental | true | 3073.7 -> 2577.1 | 0.606 -> 0.344 | 1.942 -> 0.498 |

The tuned low-stock public query measured 246 ms in the standard projected
mode versus 738 ms baseline, and 257 ms in the experimental projected mode
versus 750 ms baseline. Nonprojected controls measured 324 and 265 ms in the
final pass. This removes the earlier projected scan penalty on this workload.

The standard nonprojected one-pass control showed category revenue, low-stock,
and random-wide regressions despite no changed path for `b.projected == false`.
An [alternating Bolt audit](../../../testing/benchmarks/projected_reads/results/matrix-2026-11-27/final/paired-control.md)
on copied stores did not reproduce a stable category or low-stock penalty.
In eight additional 20,000-call random-wide starts, six p95 results were
0.589-0.649 ms; an adjacent final/baseline pair both spiked above 1.8 ms.
All hashes matched. No further default-path edit cleared the causal or
measurement bar. The isolated spikes' cause remains unknown.

The final run keeps the default format and existing stores untouched. The
experimental nonprojected mode was the one-pass Northwind aggregate and disk
winner, while projected modes improved the large-unrelated-payload case and
finished well in the random-read phases. The one-pass aggregate cannot prove
a universal configuration winner. Logical value-log byte counts do not prove
physical device I/O savings; that measurement remains open.

## Correctness and verification state

The encrypted 2 MiB public projected-read tests and new pinned-snapshot token
resolver test pass on the tuning source. The staged duplicate checker passed
against the Radeon code graph, with nine changed functions analyzed and no
duplicate implementation reported. Its index was at release head `ed456ccd7`.
An unsharded `go test ./pkg/storage -count=1` hit Go's default 10-minute
package timeout while the WAL rollover concurrent-appender test was running;
that test passed separately in 167 seconds. This is not a package-suite pass.
The repository's grouped full release verifier remains required on the final
head. The final four-mode reports are complete and linked above.
