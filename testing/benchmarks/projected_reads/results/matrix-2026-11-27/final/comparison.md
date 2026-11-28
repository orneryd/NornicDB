# Final versus baseline comparison

These are matched workloads on the same Mac and frozen corpus, with fresh stores for each final cell. Negative latency change means faster. The four baseline cells and four final cells each ran serially once, so small changes are descriptive, not a significance claim.

| Mode | Northwind mean ms before → after | Change | Random narrow p95 before → after | Random wide p95 before → after |
|---|---:|---:|---:|---:|
| [standard-projected-false](standard-projected-false/comparison.md) | 2601.1 → 2298.6 | -11.6% | 0.845 → 0.731 | 1.277 → 1.606 |
| [experimental-projected-false](experimental-projected-false/comparison.md) | 2726.6 → 2247.9 | -17.6% | 0.344 → 0.370 | 0.567 → 0.556 |
| [standard-projected-true](standard-projected-true/comparison.md) | 2873.9 → 2343.1 | -18.5% | 0.502 → 0.377 | 0.808 → 0.515 |
| [experimental-projected-true](experimental-projected-true/comparison.md) | 3073.7 → 2577.1 | -16.2% | 0.606 → 0.344 | 1.942 → 0.498 |

Per-query and per-phase detail, including p95/p99 and result checks, is in each linked mode report. A final performance conclusion must account for the measured spread and any regression larger than noise.

The [alternating default-path audit](paired-control.md) repeats the apparent
standard nonprojected slowdowns on copied stores. Its public Bolt query
timings did not reproduce a stable final-source penalty. A random-wide tail
spike appeared in adjacent final and baseline starts; the other six starts
clustered closely.
