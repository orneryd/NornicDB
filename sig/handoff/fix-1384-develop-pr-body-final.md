Follow-up to #1384. Restore the compiled bound-hop path inside the single pattern-predicate owner, avoiding the per-row generic EXISTS wrapper while preserving complete-row matching for bound relationships, property maps and rows without node bindings.

The owner caches graph-independent syntax and is called by row evaluation and compiled binding filters. It reuses the shared EXISTS expansion. Existential checks read adjacency directly; COUNT retains degree-based side selection, avoiding two Badger index scans before a possible first match. Pattern dispatch stays outside the scalar predicate switch to preserve its hot dispatch layout. No node-only matcher or executor-local boolean parser is restored.

The runtime owner now enforces the static validator's existing named-variable rule. Neo4j 5.26.30 and TCK Pattern1 [10] reject unbound named nodes/relationships with `Neo.ClientError.Statement.SyntaxError`; anonymous endpoints and local variables inside an explicit `EXISTS { ... }` remain valid. The formerly failing helper asserts the recorded error, and bound nulls remain false.

Validation at `fc375efa4ba55253cc8a775172fd4933856f00ea`, entirely on TrueNAS, Go 1.26.9 (`noui,nolocalllm`), both parsers:
- Red on untouched develop: `i20261018034726-96532-5686`. Green focused owner/helper/compiler and row-oracle tests: `i20261018045846-97544-7d9a`.
- Full `pkg/cypher`: list `i20261018050023-103204-c88c`; shards `j20261018051239-103204-dd6c`, `j20261018051240-103204-0e2c`, `j20261018051240-103204-ccb4`, `j20261018051241-103204-d620`. Default 5,260 passed / 0 failed / 25 skipped; ANTLR 5,259 / 0 / 26; all 5,285 tests accounted for per parser. No command-line skips.
- Live generated `pattern-predicate`, `pattern`, `pattern-comprehension` families: `i20261018050033-91736-8f6e` (Bolt auto-commit + explicit transaction), `i20261018050033-64728-1bbe` (HTTP). Each route/parser checks 3,263 cases with zero differences.
- Neo4j probes: `j20261018034414-39652-9196`, `j20261018034742-91116-11c6`, `j20261018034528-58736-ba5a`. Formatting clean; final dispatch `i20261018050137-96632-a7eb`, shared helper `i20261018041801-42716-4fb9`.
- Paired A/B against develop immediately before #1384 (`524b71ba3`), 10 alternating rounds on the same dedicated CPUs, 250ms: default **3,511 → 2,957 ns/op (−15.79%, p=.004)**; ANTLR **3,331 → 2,764 (−17.01%, p<.001)**. Both **36 → 26 allocs/op**, **2,320 → 1,888 B/op**. Native jobs `a00000000000001-1384-default`, `a00000000000004-1384-antlr`, both DONE/exit 0. Timing spread base/head: default 32.7%/58.8%, ANTLR 8.7%/6.9%; recorded foreign CPU median/worst 12.05%/20.86% and 5.70%/10.71%, respectively. These are the final-head measurements; both satisfy the requested bound-hop gate.

The added pattern dispatch initially slowed WITH scalar controls, so it was moved outside the scalar switch and retested. Paired controls against the task's actual develop base `e13815677` (`a00000000000002-1384-control-default`, `a00000000000003-1384-control-antlr`) show unchanged allocations. Default deltas span −1.56% to +6.01%; the +6.01% WITH/1024 median is not significant (p=.345, spreads 29.0%/27.2%). ANTLR deltas span −2.49% to +2.96%. The older-baseline binding controls remain about +5–8%, while the actual task-base binding comparisons span −2.49% to +1.45%; all values are retained in the handoff rather than filtered out.

Raw rounds and benchstat inputs are retained under the native execution IDs in `sig/tnrun-results/`.

The focused scope covers the changed owner and adapters plus #1384's cases. Full package runs are the requested regression gate; generated families verify all three routes. Full make verify is background information under the current testing standard, and was not run per fix. No local workloads or extra CI.

- [x] Base develop; bug label; milestone v1.4.4.
- [x] Red/green, full package, Neo4j compatibility and paired performance gates complete.

Release backport: #1459. Ready for Codex review; do not merge before that review.
