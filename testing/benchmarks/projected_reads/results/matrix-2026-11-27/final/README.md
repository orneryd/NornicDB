# Final projected-read matrix

This report measures source `b5666cd157caa2dfd7fd8e4345b12e9b2e0f9387` after the
scan-local token resolver and per-node decoder reuse were retained. The
[baseline](../baseline/summary.md) was published before either change.

- [Four-mode Northwind and mixed-corpus report](summary.md)
- [Final versus baseline comparisons](comparison.md), with per-query and
  per-phase detail linked from each mode
- [Alternating control audit](paired-control.md) and its
  [Northwind/Bolt](paired-control.log) and [random-wide](paired-wide.log) receipts

Each mode contains its Northwind report, mixed-corpus report, raw result JSON,
configuration receipt, full-corpus verification, and completion receipt. The
four fresh stores were measured serially. A brief verifier-plan self-test
overlapped the first control pass, so that cell was discarded and rerun alone
after the other three cells; only the clean rerun appears in this report.

The four configurations are distinct creation-time choices. `projected: false`
is the default and does not convert existing stores. `layout: 'experimental'`
is independent of the projected option.
