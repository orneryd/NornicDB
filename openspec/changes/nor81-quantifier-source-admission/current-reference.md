# Current parent review and fresh reference

The initial DeepSeek proposal is preserved as historical investigation. Its
blanket non-list quantifier exemption is rejected by fresh Neo4j 5.26.30 results.
The reference investigation made no production edits or local builds; subsequent
approved implementation and verification are recorded below.

## Reference boundary

The first RUN-only empty-input population has 62 probes: any/all/none/single
across 15 source forms, plus the two original negative controls. All four accept
null, list and timestamp(). All four reject integer, float, string, boolean,
date(), duration(), map, bound scalar parameter, WITH-bound integer, and properly
scoped Node/Relationship/Path variables. Both original controls reject.

The second population has 26 RUN-only probes. any/all reject toInteger('1'),
size([]), abs(1), 1+1, coalesce(1,2), head([1]), rand(), toFloat('1') and
properties({a:1}) as non-list sources. length('a') rejects its own argument
(expected Path), so that cell cannot independently establish list-source typing.
Both accept timestamp()+0 and datetime().epochMillis. reduce and comprehension
over timestamp() also accept. This is neither a general function exemption nor
a quantifier-position exception.

Exact query text, parameter bindings, output and exit status are preserved in
`/tmp/n152-148-reference-results.json` and
`/tmp/n152-148-reference-category-results.json`; corresponding `-run.py` scripts
and `.log` files preserve commands, version, contention checks and cleanup.
The third population accepts all four direct consumers: toUpper(timestamp()),
toUpper(datetime().epochMillis), size(timestamp()), and
size(datetime().epochMillis). Exact results are in
`/tmp/n152-148-reference-consumer-results.json`.

## Causal owner and tentative correction

Neo4j's exact version source has a preparatory
[timestampRewriter](https://github.com/neo4j/neo4j/blob/5.26.30/community/cypher/front-end/rewriting/src/main/scala/org/neo4j/cypher/internal/rewriting/rewriters/timestampRewriter.scala)
that replaces unqualified timestamp function invocation with a datetime call's
epochMillis property before semantic analysis. Its
[Timestamp signature](https://github.com/neo4j/neo4j/blob/5.26.30/community/cypher/front-end/expressions/src/main/scala/org/neo4j/cypher/internal/expressions/functions/Timestamp.scala)
still declares Integer. The runtime result signature therefore does not establish
a concrete static admission proof after the preparatory rewrite.

The parent approved removing timestamp's incorrect concrete proof from
`staticFunctionResultTypes`, retaining its runtime catalog/signature/evaluation.
The direct-consumer reference supports this route.
No new query-text rewrite, quantifier exception, parser, or admission owner is
proposed. Both current rejectors already consume the same metadata:
`staticListOperand -> scope.operandOf -> checker.check` and
`checkListComprehension -> checker.check -> staticFunctionResultType`.
`staticPatternExpressionType` is the other metadata consumer. Existing list
rules and staticListOperand's owner pin should remain unchanged.

## Scope and provenance

These are static empty-input RUN observations, not nonempty runtime, transaction,
EXPLAIN/PROFILE or full-family qualification. Each remote lifecycle acquires one
mesh slot, checks contention/memory/disk, uses a unique task-owned container and
volume, publishes no ports, and caps Neo4j at 1536MiB/2CPU. The first two runs
removed their container, volume and newly pulled image after checking consumers.
Logs preserve actual stop/inspect results; cleanup is not a clean JVM exit claim.
Third lifecycle also completed cleanup of its owned container, volume and newly
pulled image. All 92 probes completed; no local builds/tests were run.

The recovered harness remains unmodified; fresh sha256:
`ced90f8c9729737ab2b8d81416426a418540bb990143fe852d176f8ee53918c5`.
The staged/advisory checker and implementation verification remain parent-owned.

## Approved implementation and fail-first evidence

Tracked source was clean and locally fast-forwarded from 6f52ed118 to current
release 3aa1f1dceb758155f249d6c984c546e088ae40f9 before production edits.
No other task's worktree, branch or PR was modified.

The exact recovered harness was copied byte-for-byte into pkg/cypher for the
original 36-cell NOR81 run:
`GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/cypher -run '^TestNor79To82StaticClaims/NOR-81' -count=1 -timeout=5m -v`
It failed as expected, package 1.232s, log `/tmp/n152-148-original-red.log`.
Production edits followed this run. The committed test candidate retains only
the NOR81 queries, expectations, helpers and 36 execution cells from that harness;
unrelated sibling-ticket assertions are not introduced. The original full
harness remains unchanged in fixtures.

The sole production correction removes timestamp's Integer entry from the
canonical static function-result map and documents upstream rewrite provenance.
Both existing list-source readers continue their current admission rules.
Runtime catalog, evaluator, volatility and 0..1 arity policy are unchanged.
Manual duplicate review confirms both rejectors converge through this metadata;
no new parser, exemption, helper or owner-pin update is introduced.

The exact 92 fresh reference queries are retained as
`pkg/cypher/testdata/nor81_reference_admission.json`, exercised under both parsers
for empty-input RUN. Original36 and existing arity/runtime controls are included
in the focused green run recorded below. No full-suite or performance
claim is made; parent owns canonical verification and advisory checker.

## Current-source focused results

`GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/cypher -run 'TestNOR81|TestFunctionArities|TestFunctionArityThroughExecute|TestTimestampFunction' -count=1 -timeout=5m -v`
passed, package 2.066s; `/tmp/n152-148-focused-green.log`. This includes all36
original cells, all184 RUN boundary cells (92 queries x2 parsers), existing
0..1 arity assertions and timestamp runtime numerical controls.

The unchanged-source owner/metadata selection also passed:
`GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/cypher -run '^(TestStaticOperandCategoryOwner|TestStaticTypeNameDecisionOwner|TestStaticPredicateTypesMatchNeo4j|TestStaticFunctionResultType)$' -count=1 -timeout=5m -v`
Log `/tmp/n152-148-owner-green.log`. Owner-pin assertions were not modified.

Changed Go files were formatted; git diff --check passed. No broad package suite,
race, benchmark or canonical make verification was run by this implementation
agent. Parent owns integration and these remaining gates.

## Parent acceptance

The parent independently reviewed both list-admission consumers, the shared
metadata reader, upstream rewrite evidence, all 92 reference verdicts and the
scoped tests. The initial quantifier-exemption proposal was rejected, not
implemented. Only timestamp's static Integer proof was removed.

Parent command:
`GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -race -p=1 -tags=noui,nolocalllm ./pkg/cypher -run 'TestNOR81|TestFunctionArities|TestFunctionArityThroughExecute|TestTimestampFunction|TestStaticFunctionResultType|TestStaticPredicateTypesMatchNeo4j|TestStaticOperand' -count=1 -timeout=5m`
passed (9.817s). `make verify-list BASE=origin/release/v1.5.2` completed;
`make verify BASE=origin/release/v1.5.2 ONLY=localization` passed (7s).
UI, llama-libs, build, tests, allocations, conformance, differential and docs
verifier steps were skipped by ONLY; this is not full release qualification.
The staged checker was invoked and failed code-graph hostname resolution;
manual review is not represented as a clean indexed result.

Dedicated bounded package vet passed. Parent independently compared every one of
92 shipped fixture queries, parameter-presence flags, acceptance verdicts and
rejection classes with the three raw reference-result JSON files; all match.
The exact upstream rewrite was also inspected directly. `git diff --check`
reports six trailing spaces only in the verbatim reference transcript's execution
mode headings; these are preserved as source evidence, not introduced Go-code
whitespace. Go formatting and source whitespace checks are clean.
