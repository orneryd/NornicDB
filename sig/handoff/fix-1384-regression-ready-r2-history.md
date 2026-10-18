# #1384 regression fix — implementation handoff

Implementation, correctness validation and the final paired A/B are complete. Both PRs are pushed, mergeable and **ready for Codex review**, with label `bug` and milestone `v1.4.4`. Bound hops are **15.79% faster on default and 17.01% faster on ANTLR** than develop immediately before #1384; allocations fall **36 → 26**. All owned jobs finished, all clients were collected, the result monitor was removed and the held folder is empty. **Do not merge: Codex review comes first.**

All queries, tests and benchmarks execute on TrueNAS. Local work is file edits, git/gh and administrative TrueNAS clients only; no local Go workload ran. Full chronological evidence through the first compiled-owner/degree-probe iteration is preserved in `fix-1384-regression-iteration-history.md`; provisional heads/statuses in that archive are superseded here. Exact scripts/logs beside this file use prefix `fix-1384-`.

## Branches and commits

| Target | PR / branch | Pushed and validated head |
| --- | --- | --- |
| develop | [#1468](https://github.com/orneryd/NornicDB/pull/1468), `fix/pattern-predicate-compiled-owner` | `fc375efa4ba55253cc8a775172fd4933856f00ea` |
| release/v1.4.4 | [#1459](https://github.com/orneryd/NornicDB/pull/1459), `cherry/1.4.4-1384-pattern-predicates` | `4bc1858580e9c30c383af4ac8763226be1ea5163` |

Both have label `bug`, milestone `v1.4.4`, and verified links to this implementation thread and parent `075cf8e3-9632-45e4-a74b-ecbe4e778f84`. Both are OPEN / MERGEABLE / not draft on GitHub, with no pending CI checks. Current-thread PR watches are off. All pushes were fast-forward, no force push. Worktrees are `wt-fix-1384-dev` and `wt-fix-1384-release`; release local branch is `fix/1384-release-validation`. Both are clean after removing task-only test overlays. Final PR bodies are `fix-1384-develop-pr-body-final.md` and `fix-1384-release-pr-body-final.md`, uploaded successfully. Metadata receipt: `fix-1384-final-prs.json`.

Task develop base: `e138156772abea4912fdea1c3f151ef8065e386b`. Required pre-#1384 baseline: `524b71ba3365be6f453a107d77c0816b7396b41a`, first parent of squash `159cfe273`, in `wt-fix-1384-base`. Original #1459 head: `f418585d4f0ff8486aab0f08f15e0e03b64ec434`.

Develop commits:

- `928494086f8e31d6b3249be371694ea42db66fa3`: compiled owner, row/binding integration, scope validation, tests.
- `7df810ec67013c4f5d5cc179ab3369dd86919bf5`: bound nulls and shared syntax reader.
- `4d406da5fc616f40371c16566b834b82e47690f2`: compiler-admission regression assertion.
- `065bb25826e01f31d3965de628126108a8e6892d`: shared EXISTS degree-probe correction.
- `fc375efa4ba55253cc8a775172fd4933856f00ea`: preserve scalar dispatch beside the pattern owner.

Release equivalents/adaptations: `9198ff18e`, `2b23502e5`, `568465c8a`, `faba378cd`, `c37a11cb4`, `aabaefc8b`, `47ebf05d6`, `4bc185858`. Equivalent final commits include their source trailers. The release-specific shared helper and bool/map adapters are described below.

## Neo4j semantics and red/green

Neo4j **5.26.30** rejects an unbound named node `c` or relationship `r` in a WHERE pattern at compile time with **`Neo.ClientError.Statement.SyntaxError`**. These are not existential locals. Anonymous endpoints are valid. Explicit `EXISTS { (a)-[:KNOWS]->(c) }` can introduce local `c` and is true on the probe graph. TCK `testing/cypher/tck/testdata/opencypher/features/expressions/pattern/Pattern1.feature` scenario [10] also requires UndefinedVariable.

Oracle jobs (`fix-1384-oracle*.cypher` and corresponding logs):

- `j20261018034414-39652-9196`: Neo4j version and missing node, expected exit 1.
- `j20261018034742-91116-11c6`: missing relationship, expected exit 1.
- `j20261018034528-58736-ba5a`: version, anonymous endpoint count 1, explicit EXISTS-local count 1.

Error jobs exit 1 because cypher-shell is fail-fast. Red **`i20261018034726-96532-5686`** overlays only the new scope test on untouched develop `e13815677`, both parsers: it fails the new missing-variable error test and inherited `TestEvaluateWhereForContext_Branches`.

The helper keeps its false assertion and additionally checks the recorded localized UndefinedVariable SyntaxError; its assertion was not simply inverted. Static and runtime checks share one named-variable validator. Bound nulls, including typed nil nodes/edges, remain false.

## One owner and fast path

`pkg/cypher/pattern_predicate.go` owns compiled pattern syntax, scope and evaluation. Its bounded global cache contains graph-independent syntax only: no executor, graph or statement values. All row callers go through thin `rowPatternPredicate`; compiled binding filters and row plans hold the same owner and supply the current executor and complete row.

Simple one-hop shapes compile once and call shared EXISTS `boundExpandCount`. Complex shapes call `rowSubqueryValue` with the full correlated row. This preserves bound relationship identity, row-dependent property maps, rows without node bindings, null behavior and chains. Independent pipeline comparisons cover 17 shapes including direction, type, labels, properties/scalars, relationship identity, nulls and chains.

The node-only matcher and executor-local boolean compiler deleted by #1384 stay deleted. Pattern recognition uses the shared reader. #547/#1055 were read before implementation; snapshots are `fix-1384-issue547.txt` and `fix-1384-issue1055.txt`.

The first complete compiled owner still cost +56% because shared EXISTS performed two degree probes before matching, each an index iteration in Badger. The fix is inside the same shared helper: existential checks (`first=true`) read adjacency directly; COUNT retains smaller-side degree selection. No second matcher was added.

The auxiliary actual-base comparison then found a WITH scalar-filter slowdown caused by the new predicate dispatch. The final adjustment moves pattern handling outside the scalar switch while retaining the same owner. The previous uniform WITH slowdown is removed; all final comparisons are reported below, including the noisy default WITH/1024 result.

Release predates the shared expansion helper, so `subquery_bound_expand.go` backports it once for both EXISTS and the owner. It uses release's traversal/node-match primitives, bool-valued row results and direct-map scope. Newer develop retention/endpoint memo types and unrelated row-fold metadata were not imported. `aabaefc8b` removes one formatting-only blank line. No release-specific duplicate predicate implementation.

## Exact-final-head tests — both parsers

Go 1.26.9, tags `noui,nolocalllm`, `bash sig/tnrun.sh go <worktree> --parser both -- ...`. No command-line skips. All jobs below finished and all test clients were collected.

| Check | Develop `fc375efa4` | Release `4bc185858` |
| --- | --- | --- |
| Focused owner/helper/compiler + row oracles | `i20261018045846-97544-7d9a`, PASS | `i20261018045856-23648-9fef`, PASS |
| Full pkg/cypher list | `i20261018050023-103204-c88c` | `i20261018050023-3924-2403` |
| Full default | 5,260 passed / 0 failed / 25 skipped | 4,989 passed / 1 waived failure / 22 skipped |
| Full ANTLR | 5,259 passed / 0 failed / 26 skipped | 4,988 passed / 1 waived failure / 23 skipped |
| Bolt auto-commit + explicit transaction families | `i20261018050033-91736-8f6e`, PASS | `i20261018050033-98840-d2bc`, PASS |
| HTTP families | `i20261018050033-64728-1bbe`, PASS | `i20261018050033-100076-58d6`, PASS |
| Final dispatch formatting | `i20261018050137-96632-a7eb`, clean | `i20261018051245-103012-0345`, clean |

Develop full shards: **`j20261018051239-103204-dd6c`**, **`j20261018051240-103204-0e2c`**, **`j20261018051240-103204-ccb4`**, **`j20261018051241-103204-d620`**. All 5,285 listed tests accounted for per parser; all four shards exit 0.

Release full shards: **`j20261018051229-3924-e549`**, **`j20261018051230-3924-20de`**, **`j20261018051230-3924-81de`**, **`j20261018051231-3924-3bad`**. All 5,012 tests accounted for per parser. Sole failure is the standing 15K throughput-floor waiver, `TestBenchmarkMatchCreateDelete_LargeDataset_Direct`: **9,583.26 / 11,526.52 ops/sec**, default / ANTLR. Aggregate exit 1 because of that waiver; no other failure.

Focused filter: PatternPredicate, PatternOperand, Exists, PatternComprehension, Where, CompiledBindingWhere, both row oracle corpora, Gh728, EvaluateWhereForContext, RelationshipPattern and DisjunctiveRelationshipPatterns. This covers #1384's correctness cases and the originally failing helper.

Generated `pattern-predicate` (113), `pattern` (2,272), `pattern-comprehension` (878) families check **3,263 cases per route per parser per branch**, all three routes: **zero known/unstable/new differences** against Neo4j. Task-only harnesses are preserved as `fix-1384-families_test.go` and `fix-1384-http-families_test.go`; overlays were removed after jobs finished. Committed ratchet unchanged.

Shared-helper formatting passed at `i20261018041801-42716-4fb9` (develop) and `i20261018041825-46872-d787` (release); other changed-file formatting also passed, recorded in the archive. Full make verify is background information under the current testing standard and was not rerun per fix. No extra CI launched.

## Final paired A/B — gate passes

Ten paired ABBA rounds, same dedicated CPUs 48–51, 250ms, both parsers. Required baseline `524b71ba3` (immediately before #1384) vs exact final develop `fc375efa4`. Both native jobs are **DONE / exit 0 / ten rounds**:

| Parser / native execution ID | Bound-hop ns/op | Change | Allocs/op | B/op |
| --- | --- | --- | --- | --- |
| default `a00000000000001-1384-default` | **3,511 → 2,957** | **−15.79%, p=.004** | **36 → 26** | **2,320 → 1,888** |
| ANTLR `a00000000000004-1384-antlr` | **3,331 → 2,764** | **−17.01%, p<.001** | **36 → 26** | **2,320 → 1,888** |

Both comfortably satisfy the requested within-5% bound-hop gate. Allocation reduction 27.78%; bytes 18.62%. Timing spread base/head: default **32.7%/58.8%**, ANTLR **8.7%/6.9%**. Recorded foreign CPU median/worst: default **12.05%/20.86%**, ANTLR **5.70%/10.71%**. Results are paired laboratory medians under that recorded load, not a topology-wide assurance. Default incoming/COUNT control timings −4.31% to −0.22%; ANTLR −0.07% to +3.10%, no significant time difference. Incoming/COUNT allocation counts unchanged; the ANTLR optional control's four-byte median difference is retained in the raw output.

```sh
bash sig/tnrun.sh ab wt-fix-1384-base wt-fix-1384-dev --rounds 10 --parser default -- ./pkg/cypher -bench '^Benchmark(BoundOneHopRelationshipPatternPredicate|BoundEndIncomingPatternExpressions|BindingWherePipelineHandlers)$' -benchtime 250ms
```

Actual task-base `e13815677` vs final `fc375efa4`, `BenchmarkBindingWherePipelineHandlers` only: default **`a00000000000002-1384-control-default`**, ANTLR **`a00000000000003-1384-control-antlr`**, both DONE / exit 0 / ten rounds. All bytes and allocation counts unchanged.

| Control | Default ns/op, delta, p | ANTLR ns/op, delta, p |
| --- | --- | --- |
| binding / 1 | 291 → 287, −1.56%, .307 | 293 → 286, −2.49%, .016 |
| binding / 32 | 4,503 → 4,469, −0.76%, .970 | 4,426 → 4,485, +1.32%, .623 |
| binding / 1024 | 142,451 → 142,553, +0.07%, .678 | 140,030 → 142,057, +1.45%, .521 |
| WITH / 1 | 191 → 191, −0.44%, .910 | 188 → 188, +0.08%, .623 |
| WITH / 32 | 5,951 → 5,927, −0.40%, 1.000 | 5,893 → 6,067, +2.96%, .045 |
| WITH / 1024 | 194,832 → 206,547, **+6.01%, .345** | 195,717 → 191,337, −2.24%, .186 |

The default WITH/1024 median exceeds 5% but is not significant, with base/head spreads **29.0%/27.2%**; it is reported rather than called within 5%. The other default controls stay near unchanged. ANTLR controls are all within 3%. Foreign CPU median/worst default **7.79%/17.37%**, ANTLR **8.57%/12.45%**. The final older-baseline binding controls remain +5–8%, while the actual task-base binding comparisons span −2.49% to +1.45%; both sets are retained for attribution. The previous uniform 8–12% WITH slowdown is absent.

`wt-fix-1384-red` supplies the committed actual base; its untracked red-only test is ignored by A/B. Raw rounds and benchstat inputs are saved in `sig/tnrun-results/ab-<job-id>/` under both original request and native execution IDs. The mapping is recorded below. Native terminal states, exact source SHAs and genuine result aliases were independently verified in **`fix-1384-final-jobs-receipt.json`**; all four use the exact final develop head. Original client logs have suffix `-dispatch.log`.

## Performance iterations (not final-head acceptance)

At prior develop head `065bb2582`, required pre-#1384 comparisons passed:

| Parser / job | Bound-hop ns/op | Change | Allocs/op | B/op |
| --- | --- | --- | --- | --- |
| default `a20261018041540-652-4067` | 3,648 → 2,943 | −19.33%, p<0.001 | 36 → 26 | 2,320 → 1,888 |
| ANTLR `a20261018041540-94980-7563` | 3,567 → 2,809 | −21.24%, p=0.001 | 36 → 26 | 2,320 → 1,888 |

Both DONE exit 0, 10 paired rounds. Timing spread base/head default 15.5%/7.9%, ANTLR 28.8%/27.1%. Foreign CPU median/worst default 2.87%/17.81%, ANTLR 5.85%/12.60%. Allocation reduction 27.78%; bytes 18.62%. Other incoming/COUNT controls were approximately 0–1% on default.

Scalar controls against the older pre-#1384 base were +6–18%. The task-base comparison confirmed an introduced WITH slowdown: default `a20261018043103-39980-bd08`, +8.39% / +12.20% / +5.65% (1/32/1024 rows); ANTLR `a20261018043115-68584-6652`, +11.32% / +11.15% / +10.38%. Binding controls were within 5% of the task base; allocations unchanged. Both jobs DONE exit 0. This prompted the final dispatch adjustment; it was not dismissed as preexisting.

Initial compiled-owner jobs at `4d406da5f` failed: default `a20261018035659-106084-a21f`, 3,683 → 5,769 ns/op (+56.65%); ANTLR `a20261018035659-55204-f3be`, 3,463 → 5,398 (+55.86%); 36 → 56 allocs, 2,320 → 3,424 B/op. Both ten-round runs completed and led to the shared degree-probe correction.

Initial queued A/B `a20261018034947-79736-cd94` and `a20261018034947-21120-a647` at `928494086` were exact-ID/head/label verified, archived and retired SUPERSEDED without measurements; clients closed exit 1. Earlier red/green, compile-error and formatting iterations are recorded in the chronological archive; none is represented as final-head evidence.

## Queue handling and explicit decisions

The parent release-priority decision (18 Oct 02:15:37 UTC: 1.4.4 PRs before queued develop work, leave running jobs alone) is implemented by temporary queue-only controllers. Non-target queued definitions are held with SHA-256 guards and restored unchanged. No worker or benchmark App restart, and no recurring scheduling policy.

- Window 1: 19 restored unchanged, zero errors; controller removed. `fix-1384-bench-priority-receipt.json`.
- Window 2: 18 restored unchanged, zero errors; controller removed. `fix-1384-bench-priority-r2-receipt.json`.
- Window 3: 19 restored unchanged, four unique jobs concurrently cancelled by their owners (terminal CANCELLED/exit125/seconds0/not-started). Two duplicate hold records preserved in original receipt; all 23 unique definitions accounted for, held directory empty, controller removed. Original restore warnings retained for audit, reconciliation has no errors. `fix-1384-bench-priority-r3-receipt.json`, detailed `fix-1384-priority-r3-audit.json`. No cancelled job resurrected.
- Window 4: the holding method was corrected at the user's instruction below. All 20 foreign definitions were restored unchanged and in original order, held folder empty. Only our four requests were promoted. All four completed and the temporary own-result monitor was removed with r4 `-Cleanup`. Receipt `fix-1384-bench-priority-r4-receipt.json`.

The user explicitly approved stopping **only** superseded #1391 job **`a20261018011000-25632-758b`**, head `cc453b3862902611f18d658e0d3acd53bea94d63`. Exact ID/head/base/label/arguments were verified before TERM was sent to its round timeout groups (20 groups until worker advanced). No worker restart/signal and no other job signal. Stop controller removed; `fix-1384-stop-1391-receipt.json`.

The user/orchestrator then explicitly chose **let current valid develop benchmark `a20261018045718-87272-de63` finish**, latest head `6ac2637a1498d7028998c72463c59a8f02b3539a`, base `6757809a657ce9d2d47bb7dab5456e7213d4c7d5`. Its 20 paired rounds take about 100s per side; an hour is acceptable and 1.4.4 has no deadline. The four short comparisons must run next, and this turn must keep waiting. The locally prepared `fix-1384-stop-de63.*` helper **was never launched and was removed after that decision**; no signal/stop sent to this job. Prior #1391 approval was not generalized.

## Delivery complete

Both final heads pushed fast-forward; PR bodies updated; both PRs ready and mergeable with `bug` / `v1.4.4`; current and parent links verified; current-thread watches off. All owned jobs and clients complete, no temporary App or held work remains. No merge performed. **Next action belongs to the orchestrator: independent Codex review before merge.**

## Queue correction — user instruction supersedes the holding method

The orchestrator identified that r4's holding mechanism made 20 other agents' jobs unavailable in the ready queue. This was the wrong implementation of “our four next.” The user explicitly required immediate restoration, only our four ahead, and **never hold or remove other agents' jobs again**. The original holding controller was stopped (only that temporary App, never the benchmark worker), replaced with `fix-1384-restore-ready.py`, and its default creation path disabled.

All **20 definitions restored to `queue/ab` under their original filenames**, SHA-256 verified against the original receipt. Their relative FIFO order is unchanged. The held directory is **empty**. `bash sig/tnrun.sh ab-status <id>` independently reports all four #1378 jobs as **QUEUED**: `a20261018053326-91572-f25f`, `a20261018053326-70840-1903`, `a20261018060544-39716-2217`, `a20261018060544-48896-8e7d`. Evidence: `fix-1384-ready-queue-receipt.json`. The running valid develop job remains untouched.

Only this task's own four queued requests were promoted to priority execution IDs (the worker orders files by name). The payloads retain the original request IDs, source SHAs and all benchmark arguments; original definitions are archived. Other ready job files were not edited. Mapping:

| Request / existing client ID | Priority execution ID |
| --- | --- |
| `a20261018045918-48476-6ade` | `a00000000000001-1384-default` |
| `a20261018045918-61784-2d57` | `a00000000000002-1384-control-default` |
| `a20261018045918-84716-05d2` | `a00000000000003-1384-control-antlr` |
| `a20261018045919-67144-d619` | `a00000000000004-1384-antlr` |

The replacement temporary App **did not hold or modify foreign jobs**. It waited only for these four genuine execution results, copied their artifacts to the original request IDs for the existing clients, preserved all failure codes and published each aliased result last. Every aliased result records `originalRequestId` and `priorityExecutionId`; raw output identifies the actual execution ID. All four source states/SHAs/aliases were verified, all clients collected, and the result monitor was removed. No result was fabricated and no job rerun. The earlier holding method is historical and forbidden going forward.

The valid current-head develop benchmark finished naturally: **DONE / exit 0 / 20 rounds / 4,242 seconds**, receipt `fix-1384-valid-job-finished.json`. No signal or stop was sent to it. Final independent checks confirm zero files in `held` and no temporary r4 App. All other agents' work remains available to the normal worker; no other job was held or removed after the correction.
