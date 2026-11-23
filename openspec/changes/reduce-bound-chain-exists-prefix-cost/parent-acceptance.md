# Parent acceptance and verification

Base `18b36a85d10e775b9d5d3500ecf3482682754a8d`. This file supersedes the
initial investigation-only status in evidence.md; historical numbers there remain
historical and are not represented as current qualification.

## Causal correction

The bound-chain entry now supplies its WHERE to the existing `chainPrefixPrune`
owner, retaining only structural prefix metadata with the statement's existing
`boundChainReadPlan`. Parameter-bearing maps still parse per evaluation. Each
row gets a shallow match copy; cached structures, parameters, entities, verdicts
and executor contexts are not mutated or retained. `preparedRowPredicate.holds`
lazily mutates plan/program selection, so each prefix evaluation uses a local
value copy rather than sharing that mutation through the retained plan.

An independent review identified a concrete error boundary. The public probe
with an R prefix but no S suffix and `m.value / 0 > 0` returns COUNT = 0 / EXISTS = false
on pristine release, but the first candidate raised division by zero. Complete
suffix controls raise the error both before and after. Logs:
`/tmp/n152-90-prefix-error-{baseline,current}.log`.

The shared speculative prefix owner now uses existing `deferredExpressionValue`:
a failed speculative predicate retains its path and leaves the final complete-path
WHERE responsible for the error. It does not classify expressions with another
parser or suppress the final error. This also corrects the same speculative error
boundary for ordinary MATCH consumers of that owner. Permanent COUNT/EXISTS/MATCH
missing/complete suffix tests cover both parsers. The corrected public probe
matches the release's COUNT/EXISTS behavior (`...-fixed.log`).

## Completed checks before final error-boundary rerun

- Current-release original cost fixture passed, both parsers, 1k/2k populations:
  all-admitted adjacency/body reads 3000/6000; 1k first-page 1107, 2k 6000.
- New public EXISTS/COUNT suffix-read regression failed on unmodified production
  (expected 1 S read, actual 2) in both parsers. Other new semantic controls passed.
- First correction focused tests passed (2.293s). Race run (43.138s) included shared-context
  concurrent preparation, parameters/entities, outer/subquery/volatile predicates,
  zero hops, read-count ledger and generated continuation/depth controls.
- Counts were exactly 1712/2712 all-admitted; first-page 1081/2712. Candidate and
  result identity assertions held, with no label or whole-store scan.
- Search public ranked/global-top-N, provider stages, independent readers,
  capacity, cancellation/retry/shutdown and ID/grouping controls passed (1.338s).
- Those race results precede the final speculative-error correction; final rerun
  remains necessary and is recorded below once complete.

## Duplicate and owner review

The traversal filter, stable-conjunct admission, predicate evaluation, source-plan
cache, source storage reads and logical error isolation are all existing owners.
The entry's missing link to prefix pruning was corrected without a new planner,
result/verdict cache, graph scan or storage API. Search candidate ranking/grouping,
continuations, per-invocation filter verdicts and external providers are unchanged.
Independent source review verified structural parsing and mutable predicate-copy
ownership. The staged duplicate checker was invoked; graph discovery timed out.
Graph-backed discovery remains incomplete, not a clean pass.

## Limits

No private provider-included 3.9–6.9s report, 16-way stress, whole-repository suite,
Linux/default-tag release qualification or full verifier pass is claimed. Named
controlled-provider benchmarks and their exact current results are recorded
separately. Full release qualification remains pending.

## Final semantic checks

After the speculative-error correction, the corrected public probe returned
COUNT=0/EXISTS=false for missing suffixes and ArithmeticError for complete suffixes,
in both parsers. Final focused `-race` run passed (42.799s), including permanent
COUNT/EXISTS/MATCH missing/complete controls, the same read-count reductions,
concurrent shared-plan use, parameter/correlation/volatile/zero-hop boundaries,
and generated continuation/depth controls. Command:

```
GOMAXPROCS=2 GOMEMLIMIT=1500MiB GOGC=60 go test -race -p=1 -tags=noui,nolocalllm ./pkg/cypher -run '^TestBoundChainPrefix|^TestNOR406RetrieveExistsWhereCostOwner$|^TestRetrievalWhereReusesChainSyntaxWithFreshStatementBindings$|^TestBoundChainSyntaxPlanDoesNotRetainParameterValues$|TestDbRetrieveContinuationGeneratedExecutionMatrix|TestDbRetrieveContinuationIssuedDepthCompletion|ChainedPatternVolatilePredicatesAreNotEvaluatedOnPrefixes|ChainedPatternEvaluatesInjectedCounterOncePerPath' -count=1 -v -timeout=8m
```

The final staged checker was rerun after this correction and again reported a
code-graph timeout. No graph-backed clean pass is claimed.

Small-population verification is included explicitly: the retained public benchmark
ladder adds 1000/2000 cells to the existing 12.5K/25K/49K workloads. Baseline small
binaries use a Go overlay replacing only the two changed production files with
exact release-base files, retaining identical benchmark/test fixtures. Only selected
benchmarks run in those binaries; compiling test files is not a test pass.

The sibling `match_multi.go` callers were inspected directly: they pass a bound
relationship pattern and correlate nodes/edges, not a subquery body with its own
WHERE. They share the same traversal implementation; there is no body WHERE to
forward in those entries. Their contract is retained rather than introducing an
unrelated predicate-admission interface.

## Small-population regression and further correction

The first final-error candidate was not accepted as a performance improvement:
1K first-page rretrieve/EXISTS median regressed 12.1% in 20x5 and 7.55% in
100x5 samples, despite larger-population gains. The 49K plain control's initial
+5.99% median fell to +1.60% with overlapping ranges in a 100x5 follow-up;
2K plain retrieve likewise fell from +9.35% to +2.18%. All original logs remain.

CPU profiling of the small repeat attributed 0.79s of prepared predicate's
0.99s to the initial evaluator entry: the local predicate copy starts from an
expression-only retained structure on every bound-chain invocation. The corrected
constructor retains the existing canonical complete immutable row-predicate plan.
For complete plans outside PROFILE, prefix evaluation consumes the existing
rowPredicatePartValue/cypherPredicateTruth explicit error contract, retaining paths
on errors without recording a speculative failure on the statement. PROFILE and
incomplete plans retain existing deferred evaluation. No new parser, cache,
workload threshold or predicate language is introduced.

Focused tests passed after this further correction (3.122s), including asserted
complete-plan division errors with missing/live suffixes, PROFILE, program-disable
controls and concurrent reuse. Earlier race/benchmark results predate this
correction; final-source race and performance acceptance remain pending.

## Final source verification and acceptance

Final measured candidate passes the named performance gate; exact numbers, all
noise checks and allocation increases are retained in benchmark-results.md. The
existing prefix owner was then extracted byte-for-byte to traversal_prefix.go to
keep traversal.go within the 2500-line limit. Body identity was checked directly.
Final race on that integrated source passed (42.759s), including the earlier
command plus all new complete-plan, disabled-program and PROFILE controls.
No full repository, combined release or private-workload qualification is claimed.

Dedicated bounded `go vet -p=1 -tags=noui,nolocalllm ./pkg/cypher` passed.
Final `make verify-list BASE=origin/release/v1.5.2` completed and scoped
`make verify BASE=origin/release/v1.5.2 ONLY=localization` passed (1s; localization
and inventory package results cached). UI, llama-libs, build, tests, allocations,
conformance, differential and docs verifier steps were skipped by ONLY.
The final staged checker after owner extraction again timed out against the code
graph. Manual review includes the exact moved owner and both shared consumers;
indexed discovery is incomplete.
