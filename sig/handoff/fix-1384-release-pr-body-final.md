Cherry-pick of #1384 (`159cfe273` → `f418585d4`), with the follow-up fix from #1468. Current head `4bc1858580e9c30c383af4ac8763226be1ea5163`.

The single pattern-predicate owner compiles syntax once and retains the bound-hop path through the shared EXISTS expansion. Compiled binding filters and row evaluation call that owner. Complete-row matching retains bound relationships, row-dependent property maps, rows without node bindings and null behavior. Existential checks avoid preliminary Badger degree scans; COUNT retains degree-based side selection. Pattern dispatch stays outside the scalar predicate switch to preserve its hot layout.

Neo4j 5.26.30 and TCK Pattern1 [10] reject unbound named pattern variables with `Neo.ClientError.Statement.SyntaxError`. Explicit EXISTS bodies can introduce locals, and anonymous endpoints remain valid. The helper now asserts the recorded error instead of binding local `c`.

Release adaptation: backport the shared EXISTS expansion once because this branch predates it, and use it from both EXISTS and the compiled predicate owner. Adapt the release row plan's bool result and direct-map scope API. No second pattern matcher or boolean parser is added.

TrueNAS validation at this head, Go 1.26.9 (`noui,nolocalllm`), both parsers:
- Focused owner/helper/compiler and row-oracle tests: `i20261018045856-23648-9fef`, pass. Red on untouched develop: `i20261018034726-96532-5686`.
- Full `pkg/cypher`: list `i20261018050023-3924-2403`; shards `j20261018051229-3924-e549`, `j20261018051230-3924-20de`, `j20261018051230-3924-81de`, `j20261018051231-3924-3bad`. Default 4,989 passed / 1 failed / 22 skipped; ANTLR 4,988 / 1 / 23; all 5,012 tests accounted for per parser. Sole failure on each parser is the standing 15K throughput-floor waiver (`TestBenchmarkMatchCreateDelete_LargeDataset_Direct`, 9,583.26 / 11,526.52 ops/sec). No other failures or command-line skips.
- Live generated pattern families: `i20261018050033-98840-d2bc` (both Bolt routes), `i20261018050033-100076-58d6` (HTTP). All 3,263 cases per route/parser match Neo4j, zero differences.
- Formatting clean; final dispatch `i20261018051245-103012-0345`, shared helper `i20261018041825-46872-d787`.
- Paired A/B for the same fix on develop, against develop immediately before #1384 (`524b71ba3`), 10 alternating rounds, 250ms: default **3,511 → 2,957 ns/op (−15.79%, p=.004)**; ANTLR **3,331 → 2,764 (−17.01%, p<.001)**. Both **36 → 26 allocs/op**, **2,320 → 1,888 B/op**. Native jobs `a00000000000001-1384-default`, `a00000000000004-1384-antlr`, both DONE/exit 0. Timing spread base/head: default 32.7%/58.8%, ANTLR 8.7%/6.9%; recorded foreign CPU median/worst 12.05%/20.86% and 5.70%/10.71%, respectively. These are the final-head measurements; both satisfy the requested bound-hop gate.

The added pattern dispatch initially slowed WITH scalar controls, so it was moved outside the scalar switch and retested. Paired controls against the task's actual develop base `e13815677` (`a00000000000002-1384-control-default`, `a00000000000003-1384-control-antlr`) show unchanged allocations. Default deltas span −1.56% to +6.01%; the +6.01% WITH/1024 median is not significant (p=.345, spreads 29.0%/27.2%). ANTLR deltas span −2.49% to +2.96%. The older-baseline binding controls remain about +5–8%, while the actual task-base binding comparisons span −2.49% to +1.45%; all values are retained in the handoff rather than filtered out.

Raw rounds and benchstat inputs are retained under the native execution IDs in `sig/tnrun-results/`.

The focused scope covers the owner, adapters, #1384's regressions and both row oracle corpora. Full package runs are the requested gate; generated families cover all three routes. Full make verify is background information under the current testing standard and was not rerun per fix. No local workloads or extra CI.

- [x] Base release/v1.4.4; bug label; milestone v1.4.4.
- [x] Focused/full package tests (standing throughput waiver only), compatibility and paired performance gates complete.

Ready for Codex review; do not merge before that review.
