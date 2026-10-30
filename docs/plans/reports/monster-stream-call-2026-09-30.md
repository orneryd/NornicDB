# Stream / CALL / MERGE Integration Evidence

Branch: `fix/monster-stream-call-0930`.
Dependency: `ca2da7a907cabac4d4dcf0d7ac01fd423a49de2d`, fast-forwarded from
`1125684d` before cluster edits. Integrate this cluster into existing PR #771
only. No push or separate PR was performed.

## Reproductions and Changes

Latest issue comments were fetched with `gh issue view` on 2026-09-30:
#648 comment 5918579304, #640 comment 5918578724, and #728 comment 5918576853.
Raw responses are `/tmp/monster-stream-{648,640,728}-0930.json`.

- #648: `MATCH (t:T) CALL (t) { CREATE (:X {i: t.id}) } IN TRANSACTIONS
  RETURN count(*) AS c` returned 0 with one seeded T; now returns 1 and stores
  `X.i = 1`. Empty MATCH and empty UNWIND explicit-transaction inputs formerly
  raised `TransactionStartFailed`; now succeed without writes. Nonempty explicit
  transaction calls still reject. Admission follows seed evaluation; scoped
  batches execute canonical CALL to preserve unit-subquery input bindings.
  Two-row batches preserve both outer variables and subquery return columns,
  and counters count each write once.
- #640: the three latest directed, undirected, and one-labeled bare-endpoint
  MERGE statements failed with `start node variable 'a' not in context` in
  autocommit and explicit transactions. Removing the early bare-endpoint guard
  admits them to canonical whole-pattern MERGE. Each creates two fresh nodes
  and one relationship; repeated MERGE creates neither nodes nor relationships.
- #728: a 256-by-256 Doc product returned the right count but allocated
  57,040,960 bytes in autocommit and 57,470,120 in an explicit transaction,
  violating the 8 MiB correctness cap. The shared typed node-product producer
  now feeds the existing incremental aggregate collector, with no count-only
  calculation or new expression evaluator. Initial pass-after allocations were
  359,744 and 799,312 bytes respectively. Grouped WITH/RETURN, sum, average,
  min/max, DISTINCT count, null count, float sum, three-leg and empty products
  pass. Materializing row consumers copy transient rows. Typed parameter,
  bound/null binding, repeated-variable, early-stop, and cancellation checks pass.

## Changed APIs

No exported APIs or signatures changed. New internal method:

```go
func (e *StorageExecutor) pipelineNodeProductSource(
    ctx context.Context, rows []pipelineRow, clause string,
) (pipelineRowSource, bool, error)
```

The source reuses `pipelineNodeMatchTemplate.node`,
`collectPipelineInitialNodeCandidates`, and `pipelineNodeMatchesPattern`.
`runPipelineClauseRows` supplies it to existing `pipelineApplyWithSource` and
`pipelineApplyReturnSource`; `pipelineApplyMatchWithHint` uses the same source
with `materializePipelineSource` for retained row results. Aggregate state and
typed expression semantics remain owned by `pipelineAggregateGroups`.

Existing CALL admission and scoped batch helpers, and canonical relationship
MERGE admission, changed behavior without changing their signatures.

## Observed Validation

Every Go command used an absolute worktree `go -C` path and
`-tags noui,nolocalllm`, `-count=1`, and a bounded test timeout. Commands were
synchronous. No benchmark, timing comparison, Docker runner, or detached
terminal workaround was run.

| Log under `/tmp/` | Observed Outcome |
| --- | --- |
| `monster-stream-648-before-0930.log` | FAIL: outer count 0; zero-input explicit rejection |
| `monster-stream-648-after-0930.log` | PASS: exact regressions and existing MATCH rejection |
| `monster-stream-648-batches-0930.log` | PASS: unit/returning multi-batch bindings and counters |
| `monster-stream-640-before-0930.log` | FAIL: all six query/mode cases |
| `monster-stream-640-after-0930.log` | PASS: all six, graph effects and idempotency |
| `monster-stream-728-before-0930.log` | FAIL: both allocation caps; aggregate semantics passed |
| `monster-stream-728-after-0930.log` | PASS: allocation caps and initial aggregate matrix |
| `monster-stream-728-contracts-0930.log` | PASS: expanded typed/source contract matrix |
| `monster-stream-related-0930.log` | PASS: CALL, MERGE, WHERE/product, collector regressions |
| `monster-stream-final-0930.log` | PASS: expanded focused pipeline correctness and coverage |
| `monster-stream-race-0930.log` | PASS: scoped race-enabled correctness suite |

The first broader related run caught product admission accepting relationship
patterns; a shared top-level comma scanner and balanced whole-node check fixed
that regression, and the same suite subsequently passed. One intermediate
compile failed while selecting the scanner helper; no failed intermediate result
is reported as passing evidence.

The final focused command used this test-name expression:

```text
^(TestGh648|TestGh640|TestGh728|TestMonster|TestCartesian|TestCallInTransactions|TestPipeline|TestUnwindRangeAggregate)
```

The race command used the same expression except for its final alternatives:
`TestPipelineAggregateSource` instead of `TestPipeline|TestUnwindRangeAggregate`.
The new product-source function has 91.1% focused statement coverage. The
package-wide focused-run coverage is 25.8%; this is not a full-package coverage
claim. Profile: `/tmp/monster-stream-focused-0930.cover`. Touched files were
gofmt-formatted; `git diff --check` passed. The macOS linker still emits its
existing duplicate `-lobjc` warning.

VS Code's diagnostic service cannot resolve this sibling worktree's module
outside the open workspace and reports unresolved package symbols. No workspace
configuration was changed; the worktree-specific Go builds/tests above provide
the executable compile validation.

## Residuals and Coordinator Gates

- Pinned Neo4j three-route verification remains coordinator-owned after PR771
  integration. These local results do not establish Bolt/HTTP serialization or
  independently rerun the pinned reference. The 3,000/5,000-node server OOM
  workloads were not run; allocation correctness used a bounded 256-node fixture.
- #728 remains a WHERE convergence family. Filtered products stay on the
  existing pushdown/join planner. Relationship products, dependent sibling
  pattern properties, unnamed patterns, and intervening nonaggregate WITH
  horizons do not acquire a streaming guarantee from this change. Row results
  materialize; collect, DISTINCT, and percentiles retain necessary state.
- #648's existing general/chained transactional CALL and text batching paths
  remain architectural residuals. The precise latest scoped MATCH and empty
  input regressions are fixed; this is not a full transactional-CALL migration.
- #640's canonical self-variable and one-bound endpoint resolution still use
  `resolveMergeRelationshipEndpoint`; no general rewrite of these paths was
  attempted. The three latest whole-pattern unbound endpoint cases are covered.
- #713 latest comment (32/32 behavior cases matching; 37 full projection
  implementations on main) is not a closure gate passed by this cluster.
  Shared dependency `ca2da7a9` already made `processCallSubqueryReturn` and
  `pipelineApplyReturn` adapters to the shared projection operator. Credit that
  to PR771's dependency, not this patch. Remaining creation, mutation, legacy
  Cartesian, ordering, naming, and parsing implementations require separate
  convergence evidence; no exhaustive inventory/refactor was performed here.
- #547 remains open: shared preparation, typed bindings instead of textual
  replay, lexical scanner consolidation, dispatch migration beyond the shared
  pipeline, storage contract/repair/iterator coverage, forced ANTLR, typed
  evaluation, oversized legacy files, and CI/reporting gates are not completed
  by these focused repairs.

No full repository suite, full Neo4j compatibility suite, or package-wide 90%
coverage gate was run or claimed. Do not close the broader families from this
report. Commit identity is returned to the coordinator after this report is
included in the cluster commit.