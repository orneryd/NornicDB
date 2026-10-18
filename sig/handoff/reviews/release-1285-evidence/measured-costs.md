## Measured costs

Paired A/B `a20261018112328-105884-3ed8` completed successfully: release `24d08776c` (identical benchmark fixture commit `7d8b27490`) versus `c26247cfc`, 10 ABBA rounds per side, 100ms per benchmark, Go 1.26.9, AMD EPYC 7713P. Slot 6 used the same cores 40–43 for both sides. All 20 side-rounds exited 0. Statistics below use the runner's own analyser (two-sided Mann–Whitney U).

Base → head medians, n=10+10:

- `StreamNodes_Full`: **4,041,613.5 → 3,747,280.5 ns/op** (−7.28%, p=0.089); **1,133,648 → 1,133,625 B/op** (−0.002%, p=0.364); **20,764 → 20,764 allocs/op** (unchanged, p=0.368). Time spread: 48.1% / 49.3%.
- `BadgerEngine_UpdateNode_MVCC`: **44,144 → 46,918 ns/op** (+6.28%, p=1.000); **10,472 → 10,472 B/op** (unchanged, p=0.425); **200 → 200 allocs/op** (unchanged, p=1.000). Time spread: 21.6% / 57.8%.
- `DeleteNode_SeparateEmbeddings`: **171,109 → 161,747.5 ns/op** (−5.47%, p=0.734); **235,816.5 → 256,087.5 B/op** (+8.60%, p=0.241); **508 → 508 allocs/op** (unchanged, p=1.000). Time spread: 45.4% / 49.5%; byte spread: 43.2% / 81.3%; allocation spread: 6.5% / 4.1%.
- `UpdateNodeEmbeddingSidecar`: **27,423.5 → 25,349.5 ns/op** (−7.56%, p=0.162); **10,930.5 → 10,509.5 B/op** (−3.85%, p=0.076); **132 → 129.5 allocs/op** (−1.89%, p=0.0000473). Time spread: 47.3% / 50.9%; byte spread: 13.2% / 7.6%; allocation spread: 0% / 0.8%.

The new-layout read penalty is absent: whole-node hydration and key encoding are identical to the release base, with no extra key-family probe or per-node allocation. Sidecar replacement now computes its scan prefix once through the shared owner instead of rebuilding it on every iterator-bound check. Stable per-key copies remain necessary because Badger retains deletion keys until commit; this cost already exists on the release base.

**Measurement limit and remaining uncertainty:** the runner recorded foreign CPU activity on the assigned cores: median **19.59%**, worst **29.31%**. The higher ordinary-update latency median (+6.28%) and delete byte median (+8.60%) are reported above, not hidden. With this interference and the wide spreads, this run cannot establish a latency improvement or regression, or attribute those increases to a removable code cost. No extra whole-node read work remains to remove, and no additional retained allocation has been identified. A more isolated run would be needed for a firm timing decision; that decision remains with the maintainer. The PR is ready and no percentage threshold is being applied.

