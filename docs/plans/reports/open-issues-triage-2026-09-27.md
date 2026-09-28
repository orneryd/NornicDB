# Open-issue triage: mm0nst3r bug set (2026-09-27)

Captured from `gh issue list --repo orneryd/NornicDB --state open` on 2026-09-27.
28 open issues total; 24 authored by mm0nst3r (20 `[BUG]`, 1 `[TECH DEBT]`,
1 `[PERF]`, 2 `[TEST]`). Non-mm0nst3r open issues (#31, #33, #294, #340) are
feature/I18n workstreams and are out of scope here.

Each bug was re-run against current `main` (in-process executor on a namespaced
memory engine, matching the issue methodology) to separate still-broken from
already-fixed.

## Already fixed on main (no new work)

| Issue | Evidence on main |
| --- | --- |
| #514 invalid expressions `1 +* 2` etc. | all nine original shapes now raise SyntaxError; `1/0` raises ArithmeticError. The **core remains**: see clusters below. |
| #698 missing Neo4j 5 functions | `radians`, `isNaN`, `char_length`, `character_length`, `upper`, `btrim`, `trim(x FROM y)`, `valueType`, `nullIf`, `toIntegerList`, `left` all return Neo4j values. |
| #530 CALL … YIELD aggregation | `CALL db.labels() YIELD label RETURN count(*)` aggregates; WHERE-filtered count returns 0-row aggregate correctly. |
| #713 projection family headline shapes | `RETURN DISTINCT`, `WITH … ORDER BY`, MATCH…MERGE…SET with ORDER BY/SKIP/LIMIT all behave. |
| #657 (in-process class) | duplicate key now classifies `Neo.ClientError.Schema.ConstraintValidationFailed`. Bolt connection-close route parts remain open. |
| #648 (label part) | `CALL (n) { SET n:B }` works. Stale outer row after `SET n.y = 2` remains open. |
| #514 same-pattern node reference | `CREATE (a:T …), (b:U {name: a.name})` evaluates to the value; the relationship form is rejected like Neo4j; `datetime('x')` in a CREATE map fails; `MERGE {id: i.id + x}` keys evaluate per row. |

## Still broken — shared root-cause clusters

| Cluster | Issues | Shared root cause | Files |
| --- | --- | --- | --- |
| A. Property-map expression values | #514 core + #656 | `parseValue` in `pkg/cypher/pattern_parser.go` evaluates `+`/`/` in CREATE/MERGE property maps but stores `*`, `-`, `^`, `%`, unary minus and list items as their own text (silent wrong data; Neo4j values expected) | `pkg/cypher/pattern_parser.go` |
| B. Validator leniency | #514 (`NOT IN`, trailing comma, doubled quotes, dangling `UNWIND [1] AS x`), #744 (`EXPLAIN PROFILE` accepted) | `validateSyntaxNornic` + pipeline list-literal parser accept forms Neo4j rejects | `pkg/cypher/executor_query_routing.go`, `pkg/cypher/pipeline_executor.go` |
| C. Statement framing | #743 FINISH, #744 CYPHER preamble | Prologue/terminator scanning only accepts Nornic clause starts; Neo4j 5 statement frames rejected | `executor.go`, `keyword_scan.go`, `clauses.go` |
| D. Non-boolean WHERE | #514 (WHERE 42 keeps rows), #728 (remaining) | WHERE predicates keep rows instead of raising a type error | WHERE evaluators |
| E. EXPLAIN/PROFILE plan delivery | #744 §2 | plans never reach Bolt/HTTP clients | `pkg/bolt`, `pkg/server` |
| F. MERGE whole-pattern creation | #640 | Unbound node patterns in a merged relationship pattern reuse existing nodes instead of creating the whole pattern | `merge.go` / pipeline MERGE |
| G. Typed list properties | #643 | No Neo4j list-property type rule (homogeneous, null-free) and no int→float coercion on storage | property value conversion |
| H. OPTIONAL MATCH shortestPath | #581 | shortestPath pattern binding not supported in OPTIONAL MATCH position | `match`/traversal planning |
| I. CALL (n) {} row staleness | #648 (remaining) | Outer row not refreshed after a subquery writes the bound node | CALL subquery row plumbing |
| J. Explicit-tx create+remove | #741 | Staged state keeps removed edges/nodes inconsistent at COMMIT; label counters one high | `pkg/storage` transaction staging |
| K. Route parity | #657 (Bolt conn close/error code), #745 element ids across routes, #668 HTTP result entry | Protocol-layer divergences | `pkg/bolt`, `pkg/server` |
| L. Infra | #726 race, #445 stats, #446 ANN floor, #715 tests, #591 perf, #739 antlr, #547 debt, #754 CI | Infrastructure-specific root causes | various |

## Branch policy

One branch off `main` per cluster (shared root cause = shared branch).
Unrelated root causes stay on separate branches unless they directly overlap
files, per instruction. PR per branch with fix description + benchmark evidence.

## Progress

- [x] Shared-executor transaction follow-up (lane-3 finding 1) — PR #755 (`fix/shared-executor-transaction-control`).
- [x] Cluster A (property-map expression values, #514 core + #656) — PR #757 (`fix/cypher-property-expression-values`).
- [x] Cluster C (statement framing, #743 FINISH + #744 CYPHER preamble / EXPLAIN PROFILE) — `fix/cypher-statement-framing`: preamble stripping, FINISH terminator (incl. UNION branches and CALL bodies), EXPLAIN/PROFILE exclusivity. PR pending.
- [ ] Cluster E (#744 §2 plan delivery to clients) and clusters B, D, F–L pending.
