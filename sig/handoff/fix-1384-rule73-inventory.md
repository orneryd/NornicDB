> Superseded by the Rule 73 family analysis in [fix-1384-regression.md](fix-1384-regression.md). This preliminary inventory predates graphify, the expanded attack and the release-carrier analysis; its conclusions are historical.

# Pattern expressions: Rule 73 boundary and causal inventory

Status: integrated rework in progress; neither PR is ready. The r1/r2 findings
are preserved in the review handoffs. This inventory supersedes the earlier
per-finding implementation plan, not the recorded failing evidence.

## Forms and scope contracts to compare with Neo4j 5.26.30

| Form | Names introduced locally | Outer scope and horizons | Result |
| --- | --- | --- | --- |
| WHERE relationship pattern; NOT/AND/OR/XOR operands; `exists(pattern)` | Anonymous endpoints only; named node/relationship must exist | Current row wins, including null; retained expression imports remain visible after WITH | Boolean; bound null has no path |
| Pattern comprehension, including named path, WHERE and nested expressions | Node/relationship/path locals; path assignment shadows an outer path | Current iterator row plus expression imports; locals do not escape | Projected list |
| EXISTS, COUNT, COLLECT braces, pattern shorthand or MATCH body | Pattern and clause locals | All expression imports survive internal WITH; newly introduced locals follow ordinary WITH projection; renaming does not erase the original imported binding | Existence, row count, one-column list |
| Expression body with WITH *, renaming, omission, dropping/re-adding, DISTINCT/aggregation | Projection aliases, subject to imported-name redeclaration rules | Expression imports stay available; local names can be dropped; Cypher 5 aggregation grouping uses surviving imports | Same rows under every wrapper |
| Expression body with UNION / UNION ALL | Branch locals | Every branch receives the same resolved imports; existing UNION combiner owns deduplication | Rows combined before wrapper result |
| Expression body with scoped, wildcard, empty or legacy CALL | CALL imports and returned aliases | CALL is a genuine lexical boundary; explicit imports survive WITH; legacy importing WITH and dropped locals are tested separately | Existing CALL clause owner executes the selected scope |
| Nested quantifier, list comprehension, reduce, CASE and graph expression | Iterator-local bindings | Resolve at the expression's evaluation point; explicit null shadows outer values | Normal deferred-expression/error semantics |

Each form is exercised with outgoing/incoming/undirected hops, bound/unbound
endpoints, bound relationships, labels, property maps, row-dependent maps,
chains, variable length and null/type-error inputs where applicable. The
generated family is combined with the existing pattern, pattern-comprehension,
pattern-predicate and correlated-body families. Public routes: Bolt auto-commit,
Bolt explicit transaction and HTTP, with both parsers.

## Extracted execution inventory

Source searches: `rg` for `rowSubqueryValue`, `runCorrelatedSubquery`,
`correlatedSubqueryExecutor`, `evaluatePatternComprehensionFromRow`,
`patternPredicatePlan`, `boundPatternRows`, `fabricRecordBindings`,
`carrySubqueryImports`, `pipelineImports`, `boundExpandCount` and all callers.

| Entry / consumer | Current execution and scope decision | Integrated destination |
| --- | --- | --- |
| row_expression.go: direct pattern and exists(pattern) | patternPredicatePlan independently validates/looks up row, then hop or rowSubqueryValue | Shared graph-expression plan |
| row_predicate_plan.go; binding_where_compile.go; single-node/relationship WHERE adapters | Cached pattern plan over heterogeneous compiledRowScope | Same shared plan, compiled once |
| row_value_program.go: rowSubqueryNode; row/context expression and deferred operands | rowSubqueryValue chooses degree/chain/node/kernel/pipeline | Shared graph-expression plan before any choice |
| pattern_expression.go: direct/compiled/context comprehension adapters | evaluatePatternComprehensionFromRow builds entities and chooses boundPatternRows | Shared graph-expression plan; projection remains a row kernel |
| subquery_value_expression.go: degree, chain, node, traversal, generic body | Fast helpers receive only current values; generic executor merges imports later | All kernels receive one already-resolved scope |
| correlatedSubqueryExecutor, runCorrelatedSubquery, UNION branches, full-executor fallback | Generic-only fabricRecordBindings merge; fallback can reconstruct bindings independently | Prepared scope passed unchanged through strategy and body rewrites |
| pipeline_imports.go, pipeline_call_operator.go, pipeline_projection.go | Canonical carrySubqueryImports rule; genuine CALL import boundary | Reuse retention rule; preserve lexical CALL ownership |
| pipelineLabelScanProjection / pipelineNodePredicateProperties | May omit properties required by an inline node pattern | Supply the complete binding when the existing node-pattern reader reports its use |
| boundExpandCount; traversalEdges; trackedRetention (develop only) | One shared hop kernel, but `first` disables sparse-end choice | One sparse-end/resource policy for existence and count |
| legacy evaluateRelationshipPatternInWhere | Only test callers; thin adapter | Remains thin; no second matcher |

## Causal mechanism and change of approach

The generic executor resolved persistent imports after the fast strategy had
already been admitted from the current row. Pattern predicates additionally
validated an import in one map and read its value from another. Comprehensions
had their own entity partition and matching entry. The prior performance fix
also made resource strategy depend on the result wrapper (`first`) rather than
the expansion's working set. Existing differential families did not cross
internal horizons with these strategies.

The replacement boundary is one graph-expression execution plan. It resolves
the effective scope using the canonical import-retention rule **before**
validation or strategy admission; it owns predicate-local restrictions,
comprehension path shadowing, compiled-hop admission and generic subquery
selection. Kernels receive that resolved scope and do not reconstruct a
different one. Body rewriting/UNION/CALL execution retain it. Sparse-end
selection and memory charging remain in the sole shared expansion kernel.

The generated matrix must verify this entire boundary before the next review.
The earlier narrow green run is evidence only; it does not satisfy Rule 73.

