# Concept: converge the execution entry points (HARD_CONVERGENCE item 2)

Basis: upstream main at a427a468 (after #488, the single router, was merged). Read-only investigation plus one local experiment with a temporary, env-gated switch in a scratch worktree; the switch is reverted, nothing was pushed or posted. Raw output: `results5/concepts-2-3/pipeline-first.log`.

## 1. What actually exists today

HARD_CONVERGENCE listed "five entry points". After reading the code the picture is more precise, and partly better than that list suggested:

| layer | what | size | who calls it |
| --- | --- | --- | --- |
| **Public entry** | `Execute` — normalisation, `:use`/`:param` shell commands, Fabric, `USE`, authorisation, transaction scripts, `BEGIN/COMMIT/ROLLBACK`, syntax validation, query limits, analysis, result cache, EXPLAIN/PROFILE, then the choice of transaction mode, then result limit and cache invalidation | ~450 lines | Bolt, HTTP, gRPC, PROFILE (re-enters `Execute`) |
| **Internal re-entry** | `executeInternal` — a second, shorter preamble (normalise, `USE`, authorise, validate, merge params) and then the router | 82 lines | **42 call sites**: subqueries, `CALL {}`, FOREACH, UNION branches, CALL-tail, `apoc.periodic.*`, `apoc.cypher.*`, procedure bodies, Fabric |
| **Router** | `executeWithoutTransaction` — since #488 the only router, for all three transaction modes | ~450 lines, ~75 dispatch rules | 9 call sites |
| **Executor A: monolithic handlers** | one hand-written function per query shape: `executeMatch`, `executeMatchWithClause`, `executeCompoundMatchCreate`, `executeCompoundMatchMerge`, `executeMultipleCreates`, `executeMultipleMerges`, `executeCreateSet`, `executeMatchWithOptionalMatch`, … | the bulk of the 97k lines | router |
| **Executor B: the clause pipeline** | `executePipeline` — splits the text into clauses (MATCH, OPTIONAL MATCH, CREATE, MERGE, SET, WITH, UNWIND, RETURN) and threads binding rows through them; returns "not handled" for anything else (FOREACH, CALL, DELETE, REMOVE, UNION, nested MERGE …). The source calls it "the semantic pipeline" and calls the handlers "specialised physical plans". | 1,776 lines | **7 call sites**: 5 different positions inside the router, 2 inside `executeMatch`, 1 inside `executeMatchWithClause` |
| **Executor C: pattern executors** | `DetectQueryPattern` + `ExecuteOptimized`: 5 recognised shapes (mutual relationship, incoming/outgoing count, edge property aggregate, large result set) | 462 + 669 lines | 1 position in the router |
| **Fast paths** | 57 `try…` functions. 3 run before any routing (`tryFastPathSimpleMatchReturnLimit`, `tryFastPathAnyMatchVectorCosine` → 4 vector-cosine variants, `tryFastPathCompoundQuery`); the rest are called from inside handlers (14 index-seek variants in `match_index_seek.go`, 7 CALL-tail, 6 traversal, 4 traversal aggregates, …) | — | router and handlers |

So the divergence is no longer "several routers". It is: **three executors (A handlers, B pipeline, C patterns) plus 57 fast paths can each answer the same query, and which one does is decided by text position rules.** The same query shape is answered by B when the router reaches one of its five pipeline positions, but by A if an earlier rule matched first; inside a subquery it re-enters through `executeInternal` and may take a different one again.

Two smaller points:
- `Execute` and `executeInternal` each carry their own copy of the preamble (normalise, `USE` handling, authorise, validate). They differ in what they skip on purpose (limits, cache, implicit transaction) but the shared part is duplicated text.
- Everything below the router communicates by **query text**. `executeInternal` has 42 callers because a handler that meets a nested query re-serialises a fragment to a string and routes it again; bindings from the outer query are passed by *substituting values into that string*.

## 2. Experiment: make the pipeline go first

If the pipeline really is "the semantic reference" and the handlers are "optimised plans for the same semantics", then trying the pipeline *first* for every query should change speed, not results. Switch: at the top of the router, `if pipelineFirst { if r, ok, err := e.executePipeline(…); ok || err != nil { return r, err } }`. Then `go test ./pkg/cypher` (3,185 top-level tests).

Result: 3,153 pass, **32 fail**.

| group | count | examples | meaning |
| --- | --- | --- | --- |
| Tests that assert *which route* ran (trace flags, "must hit batch path", "must not label-scan") | 14 | `TestNorthwindSeeder_*HitsUnwindMultiMatchCreate` ×6, `TestSeeder_*HitsBatchPath` ×2, `TestUnwindMultiMatchCreateBatch*UsesIndex` ×2, `TestMatchVectorCosineFastPath_*` ×4 | Expected. Not a semantic difference. |
| **Different result from the two executors** | 18 | `TestAggregation_MultipleAggregates`: pipeline returns 4 ungrouped rows, handler returns 2 grouped rows. `TestFailingQuery_VariableLengthWithClauseAggregation`: pipeline returns the text `DISTINCT { node: connected …` as a value. `TestMCPBug1_MapParamPropertyAccess…`: `map.prop` evaluated by the handler, stringified by the pipeline. `TestMatchWhereSetUnwind_*`, `TestBug_UnwindMatchCreate_CorrelatedJoinKeepsCardinality`, `TestSetPlusMergeWithMapVariable`, `TestParamMapPropertyAccess_WithAliasProjection`, 2 with errors in the pipeline where the handler succeeds | **The two executors do not implement the same semantics.** |

The honest reading: the pipeline is closer to a real executor in *structure* (clauses, binding rows), but today it is **not** a usable reference — it does not group aggregates, and it inherits the text-fallthrough of the expression evaluator (concept 3). And the handlers are not a reference either (#449–#481 are handler bugs). Neither side can be the oracle for the other; the oracle has to come from outside (openCypher TCK, #482).

## 3. Proposed target

One statement of the goal: **one preamble, one router, one reference executor; everything else is an optimisation that must prove it returns what the reference returns.**

Step 1 — **one preamble.** `Execute` = `prepare()` + public-only concerns (limits, cache, transaction mode, result limit); `executeInternal` = the same `prepare()` and nothing else. Pure refactor, no behaviour change, removes the duplicated `USE`/authorise/validate text. Small.

Step 2 — **a differential harness, as a test helper.** For every query in the existing table-driven tests: run it normally, and, when `canExecuteAsPipeline` accepts it, also run it through the pipeline only; compare columns and rows (order-insensitive unless ORDER BY). Differences become a checked-in list of known divergences (the 18 above are the starting content). This is the same idea as the both-modes harness from the router concept, on a second axis. It costs nothing at runtime and stops new divergences immediately.

Step 3 — **fix the pipeline until the list is empty.** Aggregation grouping, map property access, DISTINCT/collect handling. Each fix is small and local because the pipeline is 1,776 lines with real structure, not 97k lines of shapes.

Step 4 — **widen the pipeline's clause set** (DELETE, REMOVE, FOREACH, CALL, UNION), each behind the harness. Every clause kind added lets the router rule and the monolithic handler for that shape become "optimisation only".

Step 5 — **demote handlers and fast paths to optimisations with a contract.** A fast path keeps its place only with (a) a test that asserts it is taken for its target shape (14 such tests already exist — that convention is good and should be kept) and (b) a differential test that its result equals the pipeline's. A handler whose shape the pipeline covers and which has no measurable speed advantage is deleted. Benchmarks decide, per the maintainer's rule "at least as fast".

Step 6 — **stop re-entering by text.** `executeInternal(ctx, "<query text with values substituted in>")` becomes `runClauses(ctx, clauses, bindingRow)` for the callers that already hold parsed clauses (subqueries, FOREACH bodies, UNION branches). This is where the recursive-descent parser the maintainer wants plugs in: parse once, hand the clause list down. It removes the class of bugs where a bound string value is re-parsed as Cypher (judging by its name, `TestCypherHelpers_WithRemainderEscapesBoundStringValues` guards one instance of it; I did not read that test).

Order: 1 → 2 → 3 → (4, 5 interleaved, one clause kind at a time) → 6.

## 4. Relation to the single-router work (#488)

**It builds directly on it, and does not conflict.** #488 made `executeWithoutTransaction` the only router for all transaction modes. Without that, every step above would have had to be done twice. Concretely:
- Step 2's harness and the both-modes harness from the router concept are the same helper with two switches (mode: auto-commit/explicit; executor: normal/pipeline-only). Build them together.
- Steps 3–5 change *which executor answers inside the router*; they do not touch transaction handling, the storage view, or the remaining router-concept steps (pure-CREATE write route R4, storage contract tests for #461/#475).
- One real interaction: the pure-CREATE no-transaction route (`isEventualAsyncEligible` / `tryAsyncCreateNodeBatch`) is decided in `Execute` *before* the router. It is both a router-concept item (R4, the #461 lead) and one of the "fast paths" here. Treat it once, under the router concept, and list it here only as "already covered".

## 5. Acceptance data

- All existing `pkg/cypher` tests pass at every step (steps 1, 2 change no behaviour; steps 3–4 only turn known-divergence entries into passing comparisons).
- `BenchmarkStatementRouting` (from #488) plus the repo's existing executor benchmarks before/after each handler removal.
- The known-divergence list only shrinks.

## 6. Risks

- Parameters are substituted into the query text (`substituteParams`): by the pipeline up front, by the handlers after routing (the comment in `Execute` says so). Until step 6 a string parameter that contains Cypher keywords or quotes is therefore re-scanned as text by whatever runs next; the harness should include such values.
- Some handlers are fast because they skip work the pipeline cannot skip (batch MERGE, index-backed UNWIND lookups). Those stay; the contract in step 5 is what keeps them honest.
- Step 4 is the long part. It is the same work as "#482 stage 2", just started from code that already exists in the repository instead of from zero.
