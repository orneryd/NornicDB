# #1468 parked for 1.5.0 — 18 October 2026

The user explicitly stopped 1.5.0 implementation so the lab serves 1.4.4. The parking protocol was read and followed. **No new test or benchmark jobs were submitted after that instruction. No owned queued/running job remains, and no job needed cancellation during parking.** The already-running release-priority measurement was completed and collected before the parking instruction arrived. Existing local worktrees and graphify roots are retained.

## PRs and saved work

| Item | State / head |
| --- | --- |
| [Develop #1468](https://github.com/orneryd/NornicDB/pull/1468) | Draft, milestone **v1.5.0**; PR head stays `fc375efa4ba55253cc8a775172fd4933856f00ea` on `fix/pattern-predicate-compiled-owner`. Do not move it to unfinished code. |
| [Release #1459](https://github.com/orneryd/NornicDB/pull/1459) | Closed by the user; last head `4bc1858580e9c30c383af4ac8763226be1ea5163`. No release implementation or acceptance obligation remains. |
| Saved WIP branch | `wip-1.5.0-1468-graph-expression-owner`, on the PR's `origin` remote (`orneryd/NornicDB`). |
| Code/evidence snapshot commit | `a590653545c39e417ab583aa46847ca055d57233` — `wip(cypher): park graph-expression runtime owner and Rule 73 evidence`. The following parking-record commit adds this handoff and the final parking status; it does not continue implementation. |
| Active WIP worktree | `C:/Users/mm0ns/claude/nornic-pr387-test/wt-fix-1384-integrated-dev`; renamed its implementation branch to the WIP branch. |

The WIP commit preserves 361 files: integrated production scaffolding, executable gate probes at their real test locations, the generator, matrix CSV/JSON reports, raw logs, graphify/AST inventories, review evidence, instructions, priority comparison, and full snapshots/patches of the earlier provisional work. Its working tree is clean after the parking record is committed. **This code is unfinished, unformatted and untested; it is not buildable until the external static API is integrated, and it is not a verified correction.** No green result from an earlier head applies to it.

The provisional edits in `wt-fix-1384-dev` are all saved under `sig/handoff/park-1.5.0-1468/provisional-dev/`: production source snapshots, `tracked-changes.patch`, the `gen_sweep.py` edits, `pattern_predicate_imports_test.go`, and the memory probe. The superseded release provisional edits are also preserved under `provisional-release/` solely as history. Neither set was overlaid onto the integrated implementation. Original worktrees and their dirty files remain untouched. Archived Go sources use `.go.txt` so ordinary Go package discovery cannot run historical packages; remove the final `.txt` only when restoring a chosen snapshot. The archive README explains this.

**Branch-name exception:** the protocol names `wip/1.5.0-1468-<slug>`, but origin already has branch `wip` at `bd089c10b99576cd197687020c26ea644f82a66b`. Git rejected the slash-prefixed branch with a directory/file conflict. The saved branch uses `wip-1.5.0-1468-graph-expression-owner`, matching the other parked 1.5.0 branches. The existing `wip` branch was not changed. An earlier upload also hit a transient remote unpack error; the final push and remote-head verification determine preservation, not those failed attempts.

## Approved gate and plan

The **Rule 73 gate was approved** after the mechanically extracted inventory and generated self-attack, not after another pair of leaf fixes. Preserve the approach: one compiled graph-expression plan captures the original form before normalisation; one runtime frame resolves values/presence and persistent imports before choosing strategy; all consumers converge; producers derive requirements from the plan; one expansion/accounting policy. The family includes WHERE patterns, exists(pattern), pattern comprehensions, EXISTS/COUNT/COLLECT, internal WITH/UNION/CALL, compiled/generic/prepared/context adapters, producer property reads, physical sparse/dense/variable-hop strategies, transaction/view/error context, and stored graph effects.

- Full analysis and exact inventory commands: [fix-1384-regression.md](fix-1384-regression.md), under “Rule 73 family analysis”, and [fix-1384-rule73-analysis.md](fix-1384-rule73-analysis.md).
- Mechanically extracted graph/AST/function/call inventories and runtime coverage artifacts are `fix-1384-rule73-*` beside this file. The old review and gate-stop/ready statements are historical and superseded by explicit decisions; the integrated correction has not passed verification/review.
- Prior r1/r2 review evidence is saved in `reviews/sol-1458-1459-1460-r1.md` and `reviews/daybreak-1468-1459r2.md`.
- Develop-only scope for 1.5.0 is binding. The parity rows 21–34 question and every release frame/prerequisite/acceptance plan were dropped. Historical ledger mappings were marked withdrawn; do not revive them as a release commitment.
- There is **no 5% threshold**. Optimise, measure with paired `ab --no-wait` in normal queue order, include a dense-end case and explain any remaining cost for the maintainer. Never hold/move/remove another agent's jobs. Historical operator scripts in the evidence are not a restart procedure and must not be run.

## Static owner dependency — #1385

The single static owner is `semanticScopeWalk`, maintained by the #1385 agent, including Fabric through its injected reader. This task must consume its metadata and must not build a second walk.

```go
func (e *StorageExecutor) bodyScopeOf(body string, visible []string, cypher25 bool) (bodyScope, error)
type bodyScope struct {
    Reads []string
    Branches []branchScope
}
type branchScope struct {
    Reads, Declares, Importing []string
}
```

Reads are actual free outer reads; comprehension/reduce/quantifier locals are excluded. Pattern names are reads if visible at that point and declarations otherwise. Each UNION branch is independent. Nested expression-subquery reads propagate from the enclosing scope at their position; named CALL imports read their exact names. WITH does not remove enclosing-name visibility. Null versus absent is the runtime frame's responsibility. The cache belongs to the statement clause plan, keyed by body, language version and sorted visible names (`nil` = implicit CALL). **Landing order: the small static owner commit lands on #1385 first, then consumers switch.** No owner implementation commit has been integrated into this WIP snapshot.

**Required API extension accepted by the orchestrator:** a form-tagged entry into that same walker for the original pattern-comprehension form. In `[p=(a)-->(b) | p]`, p is a local path shadowing any outer p: `Declares` must contain p and `Reads` only real outer reads. Do not translate the comprehension into `MATCH p=… RETURN p` for scope. The orchestrator is obtaining the exact signature from the #1385 owner; it has **not yet been forwarded here**. The WIP code's temporary comprehension scope call must be switched to that entry before any verification. Full forwarded note/analysis is archived as `park-1.5.0-1468/static-owner-analysis-and-api.md` (source: “Family A owner: API note” in `sig/handoff/fix145-1385-1408.md`).

The runtime side remains this family's responsibility: presence independent of null, active locals versus persistent imports across horizons, exact CALL boundary replacement/masking, and choosing physical strategy only after the frame is resolved.

## Half-done implementation — exact places to resume

These are scaffolding changes, not claims of correct behavior:

- `pkg/cypher/graph_expression_frame.go:32`: resolves row/entity/context/parameter/persistent-import values with presence preserved. `:84` calls `bodyScopeOf` and selects its Reads. The owner API is absent from the current base; per-branch declarations/frames, static clause visibility and filtering of stale context locals still need integration. `:77` installs imports across the whole body. No per-row/static-cache performance proof exists.
- `pkg/cypher/graph_expression_plan.go:28`: immutable form-tagged syntax plan and fixed-hop capture. `:61` centralises frame-first strategy admission and expression results. `:52` retains the original comprehension syntax, but its scope consumer still awaits the form-tagged API. `:108` normalises a body through a statement analysis slot. No compiled-plan/strategy caching or allocation optimisation has been measured.
- `pkg/cypher/pattern_predicate.go:18` is a thin adapter onto that graph plan; `pattern_expression.go:229` contains the comprehension body. Its existing named-path deletion remains until the static owner supplies correct local declarations. Compiled WHERE, row/context and exists(pattern) adapters have begun convergence. Other legacy helper paths still need the inventory-driven convergence pass.
- `pkg/cypher/label_expression_rewrite.go:1606` now leaves plain predicates intact so normalisation cannot admit an undefined named endpoint as an EXISTS local. The owner normalises the MATCH body later. Syntax checks, anonymous generated names, quantified patterns, existing rewrite assertions and the original-form tests still need integrated verification; do not claim the normalisation problem fixed yet.
- `pkg/cypher/subquery_value_expression.go:306` is a thin owner call. `:343` removes textual MATCH/id/WITH reconstruction in favour of the frame on the current executor/view. `entityRow`/`entityBindings` retain null presence. Generic CALL/procedure, UNION branch frame isolation and all public transaction routes remain unverified.
- `pipeline_imports.go`, `pipeline_executor.go` and `value_bindings.go` begin consuming the shared runtime import context, including an explicitly empty CALL scope. No second static scope policy should be added there.
- **Not implemented:** producer-property requirements (`pipelineNodePredicateProperties` remains unchanged), the unified expansion/accounting policy, sparse-end EXISTS restoration across every form, `*1..1` equivalence with correct relationship-list semantics, dense-case memory probes/benchmarks, durable oracle expectations/remaining-difference attribution, and complete consumer convergence. The old `!first` sparse-selection gate remains; the resource defect is still present.

## Probes, tests and job results

The executable develop probes are saved at normal paths:

- `scripts/cypher-tck/differential/rule73_pattern_matrix.py` — generated form × scope × strategy × consumer population.
- `testing/cypher/tck/rule73_pattern_attack_test.go` — live Neo4j 5.26.30 Bolt auto/explicit routes, including real parameters and statement sequences.
- `pkg/server/rule73_pattern_attack_test.go` — HTTP route.
- `pkg/cypher/rule73_pattern_{resource,memory,context,fabric}_attack_test.go`, plus `rule73_pattern_coverage_test.go` and `scratch_rule73_coverage/main_test.go` — resources, native contexts/Fabric and coverage replay.
- Full original gate and priority overlays are also saved in `park-1.5.0-1468/gate-{dev,release}/` and `priority-{release,v143}/`. Coverage is reachability evidence, not semantic proof. Probes have not been turned into passing durable regression tests for the integrated WIP.

Every acknowledged historical test/oracle job found in the task logs is indexed with its terminal result and source logs in [fix-1384-park-job-log-index.json](fix-1384-park-job-log-index.json) (97 IDs; includes merged shard dispatches). Earlier A/B iterations, including two superseded unmeasured jobs, are in [fix-1384-park-benchmark-index.json](fix-1384-park-benchmark-index.json); the four prior final request/execution aliases and terminal receipts are in [fix-1384-final-jobs-receipt.json](fix-1384-final-jobs-receipt.json). These older green/benchmark results were invalidated by r2 review and do not validate this WIP. Reviewer-owned/other-agent IDs in historical queue/audit evidence are not owned jobs to cancel.

The complete Rule 73 gate receipt table in `fix-1384-regression.md` contains all 43 gate jobs: 28 matrix jobs, 14 native/resource/coverage jobs and the resource oracle. `fix-1384-rule73-jobs.json` indexes the 28 matrix jobs. All are terminal. Per head/parser/route: gate develop **864 match / 390 differ**, release-copy **838 / 416**, out of 1,254 comparisons. This found ten concrete failures beyond the reviews, including actual wrong stored values. The develop budget probe passed 13 / failed 7; the final adjacency matrix passed 7 / failed 13 on each parser/head. Those are red evidence, not acceptance waivers. No integrated green, new A/B or fresh review ran before parking.

Current parking status is in `fix-1384-park-status.log`: no queued Go/cypher jobs, and no owned job in the running lists. Existing benchmark slots/queues belong to other agents and were untouched. All four prior owned final comparisons have terminal receipts. During parking, the one early red run whose original client log stopped at “running” was collected by ID (`i20261018034609-98884-9a07`, both parsers exit 1); raw output is `fix-1384-park-collect-red.log`. It was already terminal. **Cancelled during parking: none. Jobs to resubmit because of parking: none.** When work resumes, new integrated tests/benchmarks are required for new source, not automatic duplication of the historical IDs.

## Completed release-base / v1.4.3 measurement

The user's exception is fulfilled; no measurement was cancelled. [Full report](fix-1384-priority-measurement.md), [ten-failure CSV](fix-1384-priority-ten-failures.csv), [raw stored effects](fix-1384-priority-stored-effects.json), and [job index](fix-1384-priority-jobs.json).

| Pinned baseline, one route (Bolt autocommit), all seven phases | Default | ANTLR |
| --- | --- | --- |
| Release base without #1459, `24d08776cdb9cb624da1073007da437306518426` | **816 match / 438 differ** | **816 / 438** |
| v1.4.3 commit `f2ddd27cfae3788076778a0a0d48c827df046a3b` | **816 / 438** | **816 / 438** |

Each cell has 1,254 comparisons (1,221 cases + 33 graph effects). Failing ID sets and outcomes are identical (error-code equivalence, graph snapshot ordering ignored): **no worsening in this measured family**. Nine concrete failures occur in both baselines; #3 (WITH-* endpoint property map) occurs in neither. Wrong stored writes from G02 SET and U02 CREATE/MERGE exist without #1459, but were already identical in v1.4.3. This is evidence scoped to the generated population and pinned base, not a general claim about all of 1.4.4.

All jobs terminal: release both parsers `i20261018124351-39084-d1d6`; v1.4.3 default `i20261018124351-67092-4bbb`; v1.4.3 ANTLR `i20261018124802-31396-6263`. Exit 1 is the deliberate mismatch assertion after complete counts, not build failure. A first local ANTLR submission failed the shared self-check and created no job; the successful retry is recorded. The summariser checks each cell's seven phases and all differing outcomes.

## Next, when the user resumes 1.5.0

1. Confirm the new user scope, inspect #1468/base movement and this WIP branch, and obtain #1385's landed owner commit plus exact comprehension-form API. Keep the owner split above; no second static walk. Do not reopen #1459 or resume parity/release work.
2. Integrate the metadata into the runtime frame (static clause visibility, branch declarations, persistent imports/local horizons) and all inventoried consumers. Complete producer requirements and the single physical expansion/accounting decision. Format on TrueNAS and test compilation there. No claim that saved scaffolding already works.
3. Turn the matrix and resource/transaction probes into durable tests. Re-run focused correctness plus the accepted complete generated population on **develop**, both parsers/all three routes and stored graph effects. Attribute every remaining difference per query to a documented extension or a specific outside-family issue/PR. Existing composite issues #1478/#1479/#1480 are documented outside this family; do not hide their differences.
4. Run the paired A/B in normal FIFO order (`ab --no-wait`), including bound-hop and dense-end cases, and report any cost plainly. Do not advance ahead of 1.4.4 work or hold others' jobs.
5. Only after integrated acceptance, update #1468, arrange the requested fresh Codex review on another account (account-quota skill first), respond to findings and mark ready. Never merge. The Rule 73 integration criterion remains active throughout.

## Retained local analysis assets and limits

Local graphify roots `/graphs/nornic-fix1384-rule73-{dev,release}` and exact mirrors under `/tmp/nornic-fix1384-rule73-{dev,release}` in the shared `graphify` container are retained; visible repository copies live under `C:/Users/mm0ns/Desktop/codex/builds/repos/nornic-fix1384-rule73-{dev,release}`. Their large raw graph caches are reproducible from the retained exact heads and scripts; extracted inventories/queries/coverage are pushed. Do not delete the local worktrees or shared roots. One writer per own root; no modifications to `/repos/NornicDB`. The later announced service may supersede graphify on resumption.

No usable source/probe/result is intentionally left only in an uncommitted worktree. The known limitation is unfinished implementation and pending external API, not lost work. The previous provisional patch sets, the current integrated edits and their exact bases are all preserved independently.
