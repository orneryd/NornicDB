# Alternating default-path audit

The final one-pass matrix showed slower category revenue, low-stock, short
category lookup, and random-wide p95 for the standard nonprojected control
than its earlier baseline. The retained source edits are guarded by
`b.projected`, so we repeated those same public Bolt queries on copied stores
in baseline/final then final/baseline order. Each query had two warmups and
12 measured calls per version per round. The random-wide phase used 12,000
calls and 256 warmups per round.

| Round/order | Category revenue mean s | Low-stock mean ms | Category lookup mean ms | Customer detail mean ms | Random-wide p95 ms |
|---|---:|---:|---:|---:|---:|
| 1 baseline | 4.078 | 245.7 | 2.721 | 1.122 | 0.568 |
| 1 final | 3.797 | 247.5 | 2.680 | 1.127 | 0.589 |
| 2 final | 3.639 | 256.5 | 3.203 | 1.356 | 1.872 |
| 2 baseline | 3.881 | 380.0 | 3.515 | 1.451 | 1.066 |

The category-revenue slowdown did not reproduce, and low-stock and short
lookups did not show a stable final-source penalty. Random-wide tails varied
within both builds. A further eight-start alternating check ran 20,000
random-wide calls and 512 warmups per start. Six starts clustered at
0.589–0.649 ms p95; an adjacent final/baseline pair reached 2.278/1.877 ms.
All eight result hashes matched. The isolated spikes do not establish a
source regression, and this evidence does not identify their environmental
cause. See [raw Bolt receipt](paired-control.log) and
[raw random-wide receipt](paired-wide.log).

These checks use the production Bolt endpoint and frozen corpus but copied,
already seeded stores. The one-pass matrix remains the source for the full
Northwind and mixed reports, with its uncertainty stated in the
[summary](summary.md).
