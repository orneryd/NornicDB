# Fix #1384 regression — implementation handoff

Work is in progress; this file is updated before final delivery. All tests, queries and benchmarks execute on TrueNAS; local operations are git/gh, file edits and TrueNAS clients only. No merge is authorized: Codex review comes first.

## Branches and ownership

- Develop PR **#1468**, https://github.com/orneryd/NornicDB/pull/1468, base `develop`, branch `fix/pattern-predicate-compiled-owner`, head `4d406da5fc616f40371c16566b834b82e47690f2`, worktree `wt-fix-1384-dev`.
- Release PR **#1459**, base `release/v1.4.4`, branch `cherry/1.4.4-1384-pattern-predicates`, head `aabaefc8becc648b858ba267a35e96c96f9d5131`, local worktree `wt-fix-1384-release` / branch `fix/1384-release-validation`. Updated by a fast-forward push; no force push.
- Both PRs have label `bug` and milestone `v1.4.4`; both remain draft pending final live differentials and paired A/B.
- #1468 is linked to parent thread `075cf8e3-9632-45e4-a74b-ecbe4e778f84` and this implementation thread. #1459 is linked to both too.
- Paired baseline: `wt-fix-1384-base`, **524b71ba3**, the first parent of #1384's develop squash `159cfe273`. Develop task base: `e138156772abea4912fdea1c3f151ef8065e386b`. Release original: `f418585d4f0ff8486aab0f08f15e0e03b64ec434`.

## Semantics established

Neo4j **5.26.30** rejects `(a)-[:KNOWS]->(c)` when `c` is absent from the outer scope. It also rejects `(a)-[r:KNOWS]->(b)` when `r` is absent. These are compile-time `Neo.ClientError.Statement.SyntaxError`, not local existential variables. Anonymous endpoints are allowed. An explicit `EXISTS { (a)-[:KNOWS]->(c) }` introduces local `c` and is true for the probe graph.

Oracle files: `fix-1384-oracle.cypher`, `fix-1384-oracle-rel.cypher`, `fix-1384-oracle-valid.cypher` beside this file. Jobs: `j20261018034414-39652-9196` (version and missing node), `j20261018034742-91116-11c6` (missing relationship), `j20261018034528-58736-ba5a` (version, anonymous endpoint = 1, explicit EXISTS local = 1). Error jobs exit 1 as expected from cypher-shell --fail-fast. The same rule is required by the openCypher TCK, `testing/cypher/tck/testdata/opencypher/features/expressions/pattern/Pattern1.feature`, scenario [10].

The helper test retains false and now verifies the recorded `UndefinedVariable` SyntaxError. The runtime owner shares named-variable validation with static semantic validation. It does not reinterpret missing names as locals. Bound nulls, including typed nil node/relationship pointers, cannot fall through into local matching.

## Implementation

`pattern_predicate.go` is the sole compiled owner of a relationship pattern used as a predicate. It caches graph-independent syntax only. All three row call sites still enter through `rowPatternPredicate`; the binding filter and compiled row plan hold that same owner. The graph/executor is supplied at evaluation, never retained by a global cache. A simple one-hop pattern reuses the EXISTS owner's `boundExpandCount`; complex shapes retain the complete row in `rowSubqueryValue`. There is no restored node-only matcher or executor-local boolean parser.

Source commits:
- `928494086f8e31d6b3249be371694ea42db66fa3`: compiled owner and binding/row plan integration, shared scope validation, regression tests.
- `7df810ec67013c4f5d5cc179ab3369dd86919bf5`: keep bound nulls out of existential locals; use the shared pattern recognizer for compilation.
- `4d406da5fc616f40371c16566b834b82e47690f2`: update compiler admission test to require a compiled pattern owner and no row scratch.

Release cherry-picks: `9198ff18e`, `2b23502e5`, `faba378cd`. Release lacks current develop's shared EXISTS expansion and uses a different row-plan API, so two supporting backport commits are necessary: `568465c8a` adds `subquery_bound_expand.go` and routes both EXISTS and the predicate owner through it; `c37a11cb4` adapts the row-plan result to the release API's bool value. Release scope uses a map directly; develop uses a pointer to the map. The cherry-pick's row-plan conflict was resolved with its existing shared pattern recognizer, without importing unrelated newer row-fold metadata. The release expansion uses the existing traversal context and node-match primitives; memory-retention accounting and endpoint memo types only present in newer develop are not imported. All these adjustments are implementation-version adapters, not a second predicate matcher.

The old duplicates were `binding_where_compile.go`'s executor-local compiler and `match_multi.go`'s bound relationship matcher, already deleted by #1384. The replacement keeps those deleted. Existing #547 and #1055 issue bodies were read and saved as `fix-1384-issue547.txt` / `fix-1384-issue1055.txt`.

## Validation and jobs

Exact command scripts and complete logs use the `fix-1384-` prefix beside this file. Go 1.26.9, tags `noui,nolocalllm`, both parsers. No command-line test skips. Only the standing 15K throughput-floor waiver is permitted; any other full-suite failure must be fixed or shown unchanged on the relevant base.

- RED: untouched develop `wt-fix-1384-red@e13815677`, only the new scope test overlaid, both parsers: **i20261018034726-96532-5686**, fails both the new error test and inherited `TestEvaluateWhereForContext_Branches`.
- GREEN focused develop final head: **i20261018035455-53504-03bb**, both parsers pass. Filter includes all #1384 tests, pattern recognition, EXISTS, pattern comprehensions, WHERE, compiled binding filters, both row oracle corpora, Gh728 parameter ownership and the formerly failing helper.
- GREEN focused release final head: **i20261018035506-92340-20b0**, both parsers pass, same filter.
- Full develop: list **i20261018035631-85372-765b**; 5,285 tests; shards **j20261018035643-85372-0457**, **j20261018035644-85372-4816**, **j20261018035645-85372-27f6**, **j20261018035645-85372-8869**. Completed: default **5,260 passed / 0 failed / 25 skipped**; ANTLR **5,259 passed / 0 failed / 26 skipped**. All 5,285 listed tests accounted for; all four shards exit 0.
- Full release: list **i20261018035641-66228-73d3**; 5,012 tests; shards **j20261018035713-66228-94ed**, **j20261018035713-66228-ba44**, **j20261018035714-66228-26ce**, **j20261018035714-66228-3c8b**. Completed: default **4,989 passed / 1 failed / 22 skipped**; ANTLR **4,988 passed / 1 failed / 23 skipped**. All 5,012 tests accounted for. The throughput was 10,317.99 default / 10,638.20 ANTLR ops/sec. The sole failure on each parser is the explicitly permitted `TestBenchmarkMatchCreateDelete_LargeDataset_Direct` 15K throughput floor; no other failure.
- Live generated `pattern-predicate`, `pattern`, `pattern-comprehension` families, both Bolt auto-commit and explicit transactions, both parsers: develop **i20261018035534-30616-4ba4**; release **i20261018035545-31396-5d8b**. Release completed: all 3,263 cases per parser per Bolt route match Neo4j, known=0 / unstable=0 / new=0. Develop also completed: 3,263 cases per parser per route, zero known/unstable/new differences.
- HTTP family harness: `fix-1384-http-families_test.go`; both parsers on each branch, develop **i20261018035827-48940-ed60**, release **i20261018035833-101708-1680**, running. Both original HTTP jobs completed: 3,263 cases on each parser and branch, zero known/unstable/new differences. The task-only harnesses are untracked in the worktrees and preserved beside this file. They generate their corpus on TrueNAS and check the committed mismatch ratchet without updating it.
- Formatting final committed owner/tests/changed files: develop **i20261018035608-62440-cd83**, release **i20261018035610-53324-506b**, both clean. Release helper and modified admission test will also be checked.
- Final-head paired A/B: default **a20261018035659-106084-a21f**; ANTLR **a20261018035659-55204-f3be**; 10 rounds ABBA, same dedicated CPUs, `-benchtime 250ms`, bound-hop/incoming/pipeline-handler benchmarks. QUEUED. Baseline immediately before #1384. Final values pending.

## Iteration history (excluded from final acceptance)

- Initial red job **i20261018034609-98884-9a07** ran the new scope test on both parsers and failed as expected. A direct PowerShell-to-bash pipe character was misquoted locally; the proper red script above then ran both intended tests. The original job was collected to completion with `tnrun result`; no job is orphaned.
- Early focused green **i20261018034733-37404-faab** passed both parsers before adding the broader pipeline comparison.
- Early formatting **i20261018034844-99732-17bf** showed three alignment changes; fixed. Initial committed-format jobs **i20261018035042-103948-8237**, **i20261018035045-58344-7c93** passed.
- Early develop focus **i20261018034932-27928-fafd** found the typed nil and old admission assertion. **i20261018035319-100688-c907** verified the null correction, leaving only the admission assertion. Final focus passes.
- Early release focus **i20261018035027-99408-2f0c** / log `fix-1384-release-focused.log` failed to compile because newer develop helpers and the pointer-map row API were absent. **i20261018035329-95940-22c3** compiled after backporting, exposed the bool/truth-value API adjustment and old admission assertion; fixed. Final focus passes.
- Early full develop list **i20261018034948-25852-6038**, shards **j20261018035039-25852-cd19**, **j20261018035040-25852-e16a**, **j20261018035040-25852-b3d6**, **j20261018035041-25852-b783**: invalidated by those corrections; tnrun superseded two still-running shards. It is not cited as a full pass.
- Early full release list **i20261018035043-15400-7d0a** could not compile the initial version; excluded. All its jobs finished before the final submission.
- Initial A/B jobs **a20261018034947-79736-cd94** (default) and **a20261018034947-21120-a647** (ANTLR) remain queued for the initial implementation head `928494086`. They are diagnostic only, excluded from final-head acceptance; wait for or retire them before ending.

## Remaining work

Wait for all submitted jobs, resolve any new failures, collect final A/B within 5%, rewrite both PR bodies with exact evidence, mark both ready, verify parent-thread PR links and update this handoff. Do not merge. Never end the implementation turn with owned jobs running or queued.

## Final release head and last checks

`aabaefc8becc648b858ba267a35e96c96f9d5131` removes one trailing blank line from the backported helper; no executable change. All acceptance tests are rerun on this exact head:
- Focused: **i20261018040746-99308-cd9d**, both parsers pass.
- Full list: **i20261018040756-65992-b0fc**, shards **j20261018040857-65992-2c59**, **j20261018040858-65992-80b4**, **j20261018040858-65992-f27d**, **j20261018040859-65992-83a7**. Default 4,989 passed / 1 failed / 22 skipped; ANTLR 4,988 passed / 1 failed / 23 skipped; 5,012 tests accounted for on each parser. Only the explicitly permitted 15K throughput-floor test fails. No other failures.
- Final-head Bolt families: **i20261018040820-83420-2198**, both parsers and both transaction routes pass all 3,263 cases with zero differences.
- Final-head HTTP families: **i20261018040820-25908-6363**, running.
- Final formatting: release helper/admission test/subquery integration **i20261018040820-51164-cad4**, clean. Develop admission test **i20261018040823-54436-8e97**, clean. Earlier final owner/row-plan formatting jobs are also clean. The first helper formatting check **i20261018040036-74892-5ac8** caught the trailing blank line; this final commit fixes it.

## Benchmark queue handling

The parent thread's release-priority decision (18 Oct 02:15:37 UTC: release 1.4.4 PRs before queued develop work, no stopping the running job) is applied to the two final-head A/B jobs. The temporary task-specific operator uses the same queue-only method as the earlier release review. It is **not** a new recurring scheduling policy. No worker or benchmark App is restarted, and the running benchmark is untouched. Queued non-target definitions are held with SHA-256 checks and restored unchanged once both final jobs are claimed. Exact controller/client: `fix-1384-bench-priority.py`, `fix-1384-bench-priority-client.mjs`, `fix-1384-bench-priority.ps1` beside this report.

The initial-head A/B jobs **a20261018034947-79736-cd94**, **a20261018034947-21120-a647** were still queued. Their head/labels/IDs were verified, their definitions archived, and they were retired as `SUPERSEDED` with no measurements. Both attached clients finished exit 1; those empty tables are not performance evidence. Final-head jobs **a20261018035659-106084-a21f** and **a20261018035659-55204-f3be** remain the only acceptance A/B jobs. The first is now running; the second is next. The receipt and operator removal are pending completion.

## Performance correction — current heads supersede the earlier heads above

Current develop head: **065bb25826e01f31d3965de628126108a8e6892d** (#1468).
Current release head: **47ebf05d60ad7fd38c670fccaf7ddecd3c8f0bd9** (#1459).
Both were pushed by fast-forward; both PRs remain draft.

The first complete compiled-owner A/B still failed the timing gate: default **3,683 → 5,769 ns/op (+56.65%, p<0.001)**; ANTLR **3,463 → 5,398 ns/op (+55.86%, p<0.001)**, **36 → 56 allocations**, **2,320 → 3,424 B/op**. Jobs `a20261018035659-106084-a21f`, `a20261018035659-55204-f3be` both completed 10 rounds. These are failed intermediate results, not final acceptance. The code inspection found the shared EXISTS expansion makes two degree probes before matching a bound hop. In Badger, GetInDegree/GetOutDegree each iterate the index; those preliminary scans dominate the simple existential check.

The correction is inside that same shared EXISTS helper, on both branches: for EXISTS (`first=true`), read adjacency directly; COUNT retains its smaller-side degree selection. No second matcher is added. Develop commit `065bb2582`; release equivalent `47ebf05d6` carries the full source-commit trailer and adjusts only the helper's release file location.

New exact-head validation:
- Develop focused **i20261018041541-4248-3ac3**, both parsers PASS.
- Release focused **i20261018041541-94796-579b**, both parsers PASS.
- Develop full list **i20261018041610-94496-6bd6**; shards **j20261018041633-94496-bbbe**, **j20261018041634-94496-5440**, **j20261018041634-94496-4ae3**, **j20261018041635-94496-558d**; running.
- Release full list **i20261018041610-35288-9e70**; shards **j20261018041703-35288-8632**, **j20261018041703-35288-3cb0**, **j20261018041705-35288-7f3e**, **j20261018041706-35288-8491**; running.
- Develop Bolt families **i20261018041610-54860-3feb**, release **i20261018041610-4256-5163**, running.
- Develop HTTP families **i20261018041654-5484-663a**, release **i20261018041654-104988-0239**, running.
- Corrected-head A/B default **a20261018041540-652-4067**, ANTLR **a20261018041540-94980-7563**, 10 rounds each, queued.
- Final changed-helper formatting: `fix-1384-perf-fmt.log`, both branches clean.

First priority window: **19 queued definitions restored unchanged**, no restore errors, both obsolete initial jobs retired, both intermediate A/B jobs finished, and the temporary App was removed. Receipt: `fix-1384-bench-priority-receipt.json`.

Second priority window: same release-priority rule, exact corrected-head pair only, scripts/receipt prefix `fix-1384-bench-priority-r2`. At startup, the existing benchmark `a20261018011000-25632-758b` had already claimed the lane. It is left untouched; the new pair runs next. Eighteen other queued definitions are temporarily held and must be restored unchanged. Remove the second temporary App using `pwsh -NoProfile -File sig/handoff/fix-1384-bench-priority-r2.ps1 -Cleanup` only once both target jobs finish and the restoration receipt is complete.

All correctness checks on the preceding release formatting head `aabaefc8b` did finish: same sole 15K-floor waiver in the full package, zero differences across all three routes and both parsers. These are superseded by the current exact-head runs for acceptance.

All current-head correctness jobs have finished. Develop full: 5,260 / 5,259 passed, 0 failures, 25 / 26 skipped on default / ANTLR respectively (5,285 listed each). Release full: 4,989 / 4,988 passed, one waived 15K-floor failure each, 22 / 23 skipped (5,012 listed each); throughput 9,267.54 / 10,540.60 ops/sec. Both branches' current-head generated families pass 3,263 cases per route per parser across Bolt auto-commit, Bolt explicit transaction, and HTTP, with zero known/unstable/new differences. All focused and formatting jobs pass.

The user explicitly approved stopping the superseded #1391 benchmark **a20261018011000-25632-758b**, head `cc453b3862902611f18d658e0d3acd53bea94d63`, after it remained in its first round. A task-specific controller joined the benchmark container's PID namespace and verified the exact ID/head/base/label/argument vector before each signal. It sent TERM only to that job's round timeout groups until the worker advanced (20 groups), did not restart or signal the worker, and was removed. Receipt: `fix-1384-stop-1391-receipt.json`; no other job was signalled. The corrected-head A/B pair is now running.

## Bound-hop acceptance measurements complete

Corrected-head default A/B `a20261018041540-652-4067`: **3,648 → 2,943 ns/op (−19.33%, p<0.001)**, **36 → 26 allocs/op (−27.78%)**, **2,320 → 1,888 B/op (−18.62%)**. ANTLR `a20261018041540-94980-7563`: **3,567 → 2,809 ns/op (−21.24%, p=0.001)**, same allocation/byte reductions. Both finish DONE exit 0, 10 paired ABBA rounds on CPUs 48–51, 250ms. The required bound-hop gate passes comfortably on both parsers. Default bound-hop timing spreads: base 15.5%, head 7.9%; ANTLR 28.8% / 27.1%. Foreign CPU share: default median 2.87%, worst 17.81%; ANTLR full figures in its log. These are ten-round measurements under lab load, not an assurance for every topology.

The included `BindingWherePipelineHandlers` controls show +6–18% vs the older pre-#1384 develop baseline on default. Before declaring the PR ready, run a focused paired control comparison against the task's actual develop base `e13815677` to establish attribution. This does not change the bound-hop gate result:
- Default control `a20261018043103-39980-bd08`, ANTLR `a20261018043115-68584-6652`.
- Baseline worktree `wt-fix-1384-red@e13815677` (benchmark sends the committed tree, ignoring the untracked red-only scope test); head `wt-fix-1384-dev@065bb258`.
- Ten paired rounds, same dedicated CPUs, only `BenchmarkBindingWherePipelineHandlers`, 250ms. Queued.
- Third task-specific queue-only release-priority window uses prefix `fix-1384-bench-priority-r3`; 19 queued definitions held. Current running benchmark `a20261018011038-97756-129f` is untouched. The control pair is next. Once complete, restore/verify/delete this App with `pwsh -NoProfile -File sig/handoff/fix-1384-bench-priority-r3.ps1 -Cleanup`.
- Second priority window completed and restored all 18 definitions unchanged with no errors; its App was removed. Receipt `fix-1384-bench-priority-r2-receipt.json`.

All test harness overlays were removed from the develop/release worktrees after their jobs finished. Both worktrees are clean and both PR heads match the SHAs above. Final PR-body drafts are `fix-1384-develop-pr-body-final.md` / `fix-1384-release-pr-body-final.md`, with one `__BENCH__` placeholder to fill when the control comparison finishes. Then update bodies, mark both ready, verify/link PRs to parent/current thread, unwatch current-thread PRs, rewrite this handoff with a clear final result, and deliver the final reply. Do not merge. Two auxiliary A/B clients and the r3 control App are the only owned jobs still active.
