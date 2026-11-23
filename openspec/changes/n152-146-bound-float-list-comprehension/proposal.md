# N152-146 / NOR-79: bound Float static admission

The retained defect includes false rejection of a bound Float comprehension on empty input and the recovered258-case parameter-constructor family. The ticket's historical title does not authorize blanket parameter relaxation. The original120 generated parameter cases remain unrecovered.
DeepSeek investigation job7bd706c3a799491e95cf64a83d4bbef5 failed with
JSONDecodeError after121s; it recovered fixture artifacts only. Parent-owned
fallback investigation and source evidence follow.

## Reproduction and reference

The exact recovered original harness was copied into pkg/cypher without changes.
`GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/cypher -run '^TestNor79To82StaticClaims/NOR-79' -count=1 -timeout=5m -v`
failed, package1.159s, on base3aa1f1dce; log
`/tmp/n152-146-original-red.log`. All12 comprehension cells fail; all12 arithmetic
controls pass. The temporary complete harness was removed after preserving this
result; the byte-exact source remains in fixtures. No production edits occurred before this red run.
Tracked source then fast-forwarded cleanly to f77ec7706de099588ac232b32b6908860511d3da,
including the N148/N149 corrections. The same original24-cell reproduction failed on this integrated base before edits (1.222s, `/tmp/n152-146-integrated-red.log`), with the same12 false rejections and12 matching controls.

Fresh Neo4j5.26.30 RUN probes (46) contrast7 bound parameter kinds in6 list
positions, both original queries, a WITH-bound Float variable, and literalFloat.
The Float parameter accepts in comprehension, indexed comprehension, filtered
comprehension, quantifier, reduce and IN membership. Integer/string/boolean/map
parameters reject those positions; null/list accept. WITH-boundFloat accepts;
literalFloat rejects. The original accepted comprehension and rejected arithmetic
control agree. Exact queries, CLI parameter expressions, output and exits:
`/tmp/n152-146-reference-results.json`; lifecycle log/script share that prefix.
A further10 nested-list/map and arithmetic-consumer probes completed: bound Float list/member/map extraction and1.0 accept; literal Float list element rejects; literal map field accepts. Direct Float-parameter toUpper accepts, multiplication by2 followed by toUpper rejects, addition by2 followed by toUpper accepts, and literal Float toUpper rejects. Results are in `/tmp/n152-146-reference-nested-results.json`. All56 records are retained byte-for-byte as JSON values in `pkg/cypher/testdata/nor79_reference_admission.json`, independently compared to both raw result files.
These targeted probes are newly constructed boundary evidence, not recovered120.

## Canonical cause and approved correction

Neo4j exact-version source:
- [ParameterValueTypeHelper](https://github.com/neo4j/neo4j/blob/5.26.30/community/cypher/cypher-planner/src/main/scala/org/neo4j/cypher/internal/compiler/helpers/ParameterValueTypeHelper.scala)
  maps FloatingPointValue to ParameterTypeInfo.FLOAT.
- [ParameterTypeInfo](https://github.com/neo4j/neo4j/blob/5.26.30/community/cypher/front-end/util/src/main/scala/org/neo4j/cypher/internal/util/symbols/ParameterTypeInfo.scala)
  defines FLOAT as CTAny for JavaScript numeric compatibility. IntegralValue
  remains INT, map remains MAP, nonstring lists remain List<Any>.
- parameterValueTypeReplacement installs that type in ExplicitParameter;
  SemanticExpressionCheck checks parameterType, and list comprehensions and
  quantifiers share FilteringExpressions' List<Any> expectation. No list exemption.

Before the correction, NornicDB staticParameterOperand asserted Float. The parent approved this narrow correction:
return unknown for Float parameter kind through the existing ReadNumber owner;
preserve integer typing, literalFloat and arithmetic signature inference.
staticListOperand bypassed this owner via cypherTypeName. Route only its numeric bound-parameter domain through staticParameterOperand.display. Preserve its existing nonnumeric diagnostic policy, including temporal, Point, Node, Relationship and Path kinds that the shared parameter helper does not classify. Broadly replacing that branch would silently weaken existing rejections; broadening the helper would strengthen other consumers beyond this defect. Preserve parameter diagnostic names and map union display. The owner-pin hash changes to the actual function hash `4c15e9f79bdc0fb9c0fb19693089cd10ad8390666fc13d051acb67fc39d38bd2`, with this convergence justification. Existing function-argument checks and property
access already exempt Float locally: remove those redundant exemptions once the
shared owner supplies the uncertainty. Preserve property access's separate Map
presentation. No new parser, cache, list-position rule or runtime conversion.

Manual duplicate review compares staticListOperand, staticParameterOperand,
checkAtom, validateStaticFunctionParameters and parameterPropertyAccessType.
Their overlap is compile-time bound-value knowledge; runtime type names remain
correct for runtime error messages and must not be globally weakened.

## Limits

The approved correction is implemented. The interrupted first green run produced an empty log and is not a pass. Final current-source normal98.531s/race515.868s, package vet and scoped localization verifier7s pass; results and limits are recorded in verification.md. No full-suite or original120 claim. Matched benchmark costs are recorded in benchmark-results.md; parent owns integration and PR.
Reference tasks use one slot per lifecycle, contention/memory/disk inspection,
1536MiB/2CPU no-port containers, and task-owned cleanup. Both lifecycle logs end in CLEANUP_COMPLETE after removing task-owned containers, volumes and task-pulled images. No other task's resources were touched.

## Recovered constructor-family gap and complete causal correction

The first post-Float-fix normal run failed52.427s exclusively in the recovered
constructor population:257 of258 inherited cases were accepted where the
historical Neo4j oracle rejects, across all12 parser/mode/transaction lanes.
The original24 cells, fresh112 boundary cells and numeric/function/property
controls passed. No oracle expectation was changed.

A detached f77ec7706 baseline with the exact recovered test overlay independently
failed258/258 in nornic/RUN/auto (3.320s): the same257 missing rejections plus the
one bound-Float false rejection. Thus the Float correction resolves one existing
defect, while the remaining257 expose an existing per-request scope omission.
The canonical population hash, inherited indices and parameter values remain
unchanged; original120 generated cases are still missing.

`validateBoundParameterExpressions` checks direct parameters but never carries
parameter facts across WITH bindings. The cached semantic pass intentionally has
no parameter facts. `staticTypeScope.params` and `staticOperatorChecker` already
model those facts, but the request entry did not supply them to the canonical
scope walk. An integer parameter projected as a variable consequently became
unknown before its consumer.

The parent approved reusing the same semantic walker from the per-request
admission gate with explicit params. Compile admission passes nil and retains
its existing cache policy. The uncached request walk carries params through the
existing type-scope view constructor, WITH/LET/UNWIND projections, nested CALL,
expression subqueries and UNION branches. No new walker, textual classifier or
retained parameter cache is introduced. Existing direct-parameter checks stay
in their original order; the request scope pass follows them. Each request uses
its own scope views and checker, and the retained compiled program is read-only.
Public repeated-query/map-reuse tests exercise alternating accepted String,
rejected Integer and statically unknown Float values, including nested CALL and
UNION. Rebinding/comprehension/CALL shadowing guards keep outer parameter proofs
from leaking into local bindings.

The first integrated propagation run exited1 with only FAIL in its log during
host disk exhaustion; it produced no runtime test evidence. No pass is claimed.
The nil-parameter UNION path was then restored to its original cached-branch
policy before the next verification. The final normal selection now passes98.531s; race515.868s and vet also pass. Matched
after measurements are complete with explicit correctness overhead, not a
performance-qualified result.

## Integrated feedback and shared-owner corrections

The recovered broad run after disk cleanup failed90.565s: all257 inherited
invalid cases now reject, but32 per lane have the wrong diagnostic class; seven
historically accepted Map MATCH cases falsely reject; nested checks duplicate
work. Exact baseline profiling confirms the allocation regression (3681/9972
allocations at depth5/10 versus15850/496084). The shared `checkScoped` memo restores
linear work (4467/11759) without relaxing the guard or changing nil compile facts.

Fresh49 exact Neo4j5.26.30 MATCH probes preserve the population hash and verdicts:
all32 inherited cases reject with Type mismatch and all9 accepted Map variants
accept. Eight literal/entity/subquery controls confirm parameter Map may stand
for a graph entity while literal Map remains concrete. The canonical Map owner
now uses the existing union constructor; the binding-conflict owner prefixes its
existing message with Type mismatch, preserving status/detail and existing
wording. This establishes diagnostic-class parity, not exact text parity. No
historical verdict or test threshold changed. Full integrated results remain in
verification.md; original120 remains missing.

## Boolean phase boundary

The full constructor oracle also exposed nine list-parameter NOT false rejections.
Fresh34 probes confirm parameter lists remain admissible on empty input while
literal lists reject across NOT, AND, CASE and WHERE. Exact Neo4j5.26.30
[FrontEndCompilationPhases](https://github.com/neo4j/neo4j/blob/5.26.30/community/cypher/front-end/frontend/src/main/scala/org/neo4j/cypher/internal/frontend/phases/FrontEndCompilationPhases.scala)
runs SemanticTypeCheck before parameter metadata replacement;
[SemanticTypeCheck](https://github.com/neo4j/neo4j/blob/5.26.30/community/cypher/front-end/frontend/src/main/scala/org/neo4j/cypher/internal/frontend/phases/SemanticTypeCheck.scala)
owns the separate list-coercion prohibition. Ordinary Boolean signatures still
run after parameter metadata. Reusing the whole walker requires preserving that
phase distinction, rather than weakening types or individual oracle cases.

The parent approved the existing cached semantic owner before request-local
validation, so opaque internal bodies establish the same compile phase as public
Execute. The Boolean owner keeps the list-coercion prohibition in that phase and
normal Boolean signatures in both. No new walker, cache or type classifier.
