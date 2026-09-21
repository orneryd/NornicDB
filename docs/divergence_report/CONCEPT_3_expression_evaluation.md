# Concept: converge expression evaluation (HARD_CONVERGENCE item 3)

Basis: upstream main at a427a468. Read-only investigation plus two local experiments with temporary, env-gated probes in a scratch worktree; the probes are reverted, nothing was pushed or posted. Raw output: `results5/concepts-2-3/fallthrough.tsv`, `strict.log`.

## 1. What actually exists today

HARD_CONVERGENCE named the four `evaluateExpression…` variants. Reading the code corrects that: **those four are already one evaluator.** `evaluateExpression`, `…WithContext` and `…WithPathContext` are one-line wrappers around `evaluateExpressionWithContextFull`. The real divergence is elsewhere: there are **six evaluator families, separated by how they represent "what variables are in scope"**, and each re-implements literals, property access, operators, CASE and functions to a different degree.

| family | scope representation | entry functions (call sites) | own copies of |
| --- | --- | --- | --- |
| **A. main evaluator** | `nodes map[string]*Node`, `rels map[string]*Edge`, optional paths | `evaluateExpressionWithContext` (408), `…WithPathContext` (28), `evaluateExpression` (8), `…Full` (7) | everything: literals, operators, math, ~all functions, CASE, comprehensions |
| **B. computed-row evaluators** | `values map[string]interface{}` (after WITH / aggregation / YIELD) | `evaluateExpressionFromValues` (31), `evaluateRowExpression` (26), `evaluateRowPredicate` (7), `evaluateWithWhereCondition` (7), `evaluateConditionFromValues` (6), `evaluateWhereOnComputedRow` (5), `evaluateReturnExprInContext` (2), `evaluateWithWhere`, `evaluateYieldWhere`, `resolveReturnExprFromVarMap` | property access, arithmetic (`evaluateArithmeticExprFromValues`), CASE (`evaluateCaseExpressionFromValues`), map literals (`evaluateMapLiteralFromValues`), COALESCE, list literals; falls back to A by rebuilding node/rel maps out of the values |
| **C. single-node WHERE** | one `*Node` + its variable name | `evaluateWhere` (13), `evaluateInnerWhere`, `evaluateStringOp`, `evaluateInOp`, `evaluateIsNull`, `evaluateExistsSubquery`, `evaluateCountSubqueryComparison`, `parseValue` (36) for the right-hand side | comparison, string predicates, IN, IS NULL, regex, literal parsing |
| **D. multi-variable WHERE** | `map[string]*Node` (no rels) or a `binding` struct | `evaluateWhereForContext` (8), `evaluateBindingWhere` (5) + compiled form `tryCompileBindingWhere`, `evaluateWhereForNodeMap` (3), `evaluateWhereForMergeContext`, `evaluateCondition` (6) | AND/OR splitting, comparison |
| **E. SET values** | **none** — `evaluateSetExpression(expr string)` has no scope parameter at all | `evaluateSetExpression` (11 call sites inside set_helpers.go) vs `evaluateSetExpressionWithContext` (4, delegates to A) | literals, lists, maps, a few functions |
| **F. traversal / OPTIONAL MATCH compiled expressions** | compiled closures | `tryCompileTraversalExpr`, `tryCompileTraversalFunctionCall`, `evaluateScalarPropertyExpression(Fast)` | property access, arithmetic, a function subset |

Assignment twins on top of that: `applySetToNode` (set_helpers.go, uses family E) vs `applySetToNodeWithContext` and `applySetToRelationshipWithContext` (merge.go, use family A), plus `applySetMergeToCreated` (create.go).

**The common failure mechanism.** Every family ends the same way: if nothing matched, *return the expression text as the value*. Four such sites produce values: family A (`functions_eval_props_literals.go`, "Unknown - return as string"), family B (`match_with_rel.go`, "Return as literal if not found"), family C (`parseValue`, `return s`), family E (`set_helpers.go`, "return as-is"). That is why a gap in any family is never an error: it becomes a plausible-looking string in the result or, worse, in the stored data.

How the open bugs map onto this:
- **#462** (`SET r.n = r.n + 1` stores `'<nil>1'`) and **#474** (`SET n += {k: r.x}` stores the text): family E has no scope, so a variable reference can only fall through to text or nil; the identical expression in family A evaluates correctly.
- **#460, #468** (`reduce()` and list slices return null when the RETURN follows a WITH): consistent with the expression going to family B after a WITH (family B has no `reduce` or slice handling, family A has); I have not traced these two queries step by step.
- **#465, #466, #467** (`startNode(h).id`, map projection `n {.a}`, nested map access `m.b.c` come back as the expression text): the text fallthrough, reached because no family implements that form.
- **#463** (`round(avg(x) * 100) / 100` returns one row's value) and **#455** (`ON CREATE SET r.w = 1` not applied to a merged relationship) are in the aggregation and SET-assignment code next to the evaluators; I have not established which family each one goes through.
- **#475** (`head(a.tags)` in WHERE, 2 rows auto-commit vs 0 in a transaction): family C/A receives a differently typed list from the transactional storage view — a storage-contract issue surfacing in the evaluator.

## 2. Experiment 1: how often does the text fallthrough fire in the *passing* test suite?

Probe: log every hit of the four value-producing fallthrough sites, run `go test ./pkg/cypher` (3,185 top-level tests, all pass).

**1,231 hits, 150 distinct expressions.** By site: A 1,039 (92 distinct) · B 74 (29) · E 70 (2) · C 48 (27).

What falls through is mostly not "an unquoted word". A sample of what the evaluators were handed and returned as a value, while the tests passed:

| returned as a "value" | hits | what it is |
| --- | --- | --- |
| `node CALL db.create.setNodeVectorProperty(n, "name_embedding", node.name_embedding)` | 621 | a mis-split query fragment (two clauses glued together) |
| `'caller-` · `callee'` · `callee' = ''` | 132 · 67 · 67 | a string literal cut in half by a splitter that ignored the quotes |
| `now\nCALL {\n  WITH o, existing, targetLang, …` | 4 | the rest of the query |
| `n\nCREAT (m:ImplicitRollback {id: 2})` · `o MATCH (c:OMClass {uid:"cls:ServiceDog"})` · `v\n\t\tORDER BY id DESC\n\t\tSKIP 1\n\t\tLIMIT 2` | 1–2 each | clause boundaries not found |
| `CASE WHEN null IS NULL THEN null ELSE datetime(null) END` (family E) | 69 | a CASE expression family E cannot evaluate; stored as text |
| `1 \| x * 2`, `[label IN labels(f)` | 3–4 each | half of a list comprehension |
| `existing.isRefetch`, `n.group_id` | 2–4 each | property access on a variable that is not in the scope that family was given |
| `datetime()`, `datetime('2024-02-01T00:00:00Z')`, `tostring(tointeger(tostring(val)))` | 1–3 each | function calls a family does not implement (family A does) |
| `$age`, `$name` | 2 each | unresolved parameters |

So the fallthrough does two jobs at once: it hides evaluator gaps **and** it hides splitter/router mistakes further up (the halves of string literals and the glued clauses). Both kinds are invisible today.

## 3. Experiment 2: how much depends on it?

Switch: at family A's fallthrough, return `nil` instead of the text. Result: 3,178 pass, **7 fail** —
3 tests that assert the fallthrough itself (`…PropsLiterals_EveryBranch`, `…Operators_Branches`, `MatchRowsAndTransactionProjection`), and 4 behavioural ones (`TestMCPBug3_PostYieldWhere_AggregatingReturnAlwaysOneRow`, `TestExactShape_CreateOrUpdateTranslation_MergeOptionalCallUnion_ReturnsRow`, `TestExecuteSetTrailingUnwind_ErrorAndProjectionBranches`, `TestExplicitTransaction_MatchMergeOnCreateRoutesToMerge`).

That is the key number for planning: **1,039 fallthrough hits in family A, but only 4 behavioural tests actually rely on them.** Making family A strict is a small change with a short, known fix list — not the large behavioural break one would fear. (Only family A was switched; B, C, E need the same measurement, expected smaller because they have 48–74 hits each.)

## 4. Proposed target

**One evaluator, one scope type, no text fallthrough.**

Step 1 — **one scope type.** `type evalScope struct { nodes, rels, paths, values, params }` with a single lookup `resolve(name) (value, found)`. Family A's signature takes it instead of three maps + three path arguments. Pure mechanical refactor (the 408 call sites go through the existing wrappers, which build the scope). No behaviour change.

Step 2 — **make the fallthrough observable, then strict.** (a) Replace the four `return expr` sites by one helper `unresolved(site, expr)` that counts and, under a test flag, fails the test. Check in the current 150 expressions as the known list; CI fails if a new one appears. (b) Fix the list top-down — the first four rows of the table above are splitter bugs, 887 of the 1,231 hits. (c) When the list is empty, the helper returns an error: the evaluator signature becomes `(value, error)` and "unknown expression" reaches the client as an error instead of as data. This is §1.3 of #482, now with a measured size.

Step 3 — **fold family E into A first.** `evaluateSetExpression(expr)` → `evaluate(scope, expr)` with the node/relationship being SET in scope; `applySetToNode` and `applySetToNodeWithContext`/`applySetToRelationshipWithContext` become one `applySet(scope, target, assignments)`. This is the smallest family (11 internal call sites), it fixes #462 and #474 at the root, and it is where wrong values get *persisted*, so it has the highest payoff per line.

Step 4 — **fold family B into A.** With `values` inside the scope type, `evaluateExpressionFromValues`, `evaluateRowExpression`, the `…FromValues` CASE/arithmetic/map copies and the four WITH/YIELD WHERE evaluators become calls to the one evaluator. Fixes the "different answer before and after WITH" group (#460, #468) and gives #465–#467 one place to be implemented instead of six. Differential test: for every expression in the existing tests, evaluate through the old family-B entry and through A with the same scope, assert equal, then delete B.

Step 5 — **fold the WHERE families C and D into A**: a predicate is an expression that yields a boolean/null. `evaluateWhere(node, variable, clause)` becomes `truthy(evaluate(scope, clause))`. `parseValue` (36 call sites) becomes `evaluate` with an empty scope. Keep the *compiled* forms (`tryCompileBindingWhere`, family F) as optimisations under the same rule as the fast paths in concept 2: a test that they are taken, and a differential test that they equal the evaluator.

Order: 1 → 2a → 3 → 2b → 4 → 5 → 2c. Steps 1, 2a change no behaviour. Step 3 is the first visible fix.

## 5. Relation to the single-router work (#488) and to concept 2

- **No overlap with #488.** The router decides *which handler* runs and on *which storage view*; the evaluators sit below the handlers and are the same code in every transaction mode. None of the steps here touches routing, transactions or the storage view, and nothing in the remaining router-concept steps touches the evaluators.
- **One shared edge: #475.** It shows up in an evaluator, but the cause is that the transactional storage view returns list properties in a different Go type than the live engine. It belongs to the router concept's storage contract tests (its step 3), not here. A strict evaluator would turn it from "0 rows" into a visible error, which helps, but does not fix it.
- **Concept 2 depends on this one, not the other way round.** The pipeline executor evaluates through families A and B; several of the 18 pipeline-vs-handler differences in concept 2 (`map.prop` stringified, `DISTINCT {…}` returned as text) are fallthroughs. Doing step 2a–3 here first makes concept 2's differential list shorter and its failures readable.
- Shared tooling: the known-list + CI gate of step 2a is the same mechanism as concept 2's known-divergence list.

## 6. Acceptance data

- `go test ./pkg/cypher` green at every step; the fallthrough list only shrinks; the two experiments above are repeatable with a 30-line probe.
- Benchmarks: the evaluator is the hottest code in the executor. `evaluateExpressionFastLeaf` (the allocation-free path for `n.prop`, literals, parameters) stays in front unchanged; run the repo's executor benchmarks and `BenchmarkStatementRouting` before/after steps 1, 4, 5. The scope struct must be passed by pointer and built once per row, not per expression.
- Bug repros #462, #474 (step 3) and #460, #468 (step 4) as regression tests; #465–#467 once the missing forms are implemented in the one evaluator.

## 7. Risks

- Queries that "work" today because an unquoted word or an unknown function silently became a string will start to fail after step 2c. The measured size in the test suite is small (4 behavioural tests for family A), but production queries are not in the test suite. Mitigation: ship 2c first as a logged warning with a counter (`cypher_unresolved_expression_total`), switch to an error one release later.
- Family B rebuilds node/rel maps from `values` on every fallback to A; folding it in removes that cost but changes evaluation order for COALESCE/CASE over computed rows — covered by the differential test in step 4.
- 408 call sites of family A make step 2c (the `(value, error)` signature) a wide mechanical change. It can be staged: keep the old signature as a wrapper that records the error on the scope, and convert callers handler by handler.
