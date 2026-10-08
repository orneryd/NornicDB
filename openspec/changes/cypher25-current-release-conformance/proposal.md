## Why

NornicDB is **not Cypher 25 compliant**: it explicitly rejects `CYPHER 25`,
many current constructs fail execution, and at least one probed predicate
silently produces a wrong result. Catch up to **Neo4j 2026.09.0**, the latest
stable release verified on 2026-10-08 UTC, without creating a parallel executor.

## What Changes

- Add current syntax and semantics **inline in the main pipeline**: LET, FILTER,
  FOR, NEXT, conditional/braced composition, GROUP BY, match/path modes, SEARCH,
  expressions/functions, typed VECTOR/UUID values and open graph types.
- Keep fast paths and the parser adapter on those same semantic helpers.
  No separate Cypher 25 interpreter, duplicated router or reconstructed-query
  fallback is introduced.
- Support explicit CYPHER 5 and CYPHER 25 in that one execution path. Use a
  small immutable language context only for real differences in admission,
  semantics, catalogs and response values; do not fork the execution engine.
- Select language with CYPHER 5 / CYPHER 25 in query text, overriding the
  process default `NORNICDB_CYPHER_VERSION=5|25`. Unprefixed queries use that
  configured version; unset retains 5. Reject invalid configured values.
  No automatic installation default cutover, persisted database-language
  migration or default-language DDL.
- Preserve existing queries and APIs. Add current features without upstream
  removals becoming NornicDB deletions or new rejection rules for existing
  supported input. Record retained extensions as explicit compatibility
  differences; never silently retry as 5. Correctness fixes require failing
  reproductions and regression coverage, not unrelated behavior changes.
- Extend the [Enterprise plan](../enterprise-cdc-impersonation-query-api/README.md)
  to current-release CDC, security, Query API and protocol contracts. Its
  5.26.30 tests remain historical evidence, not the current-release oracle.
- Add an exact release inventory and recurring upstream-drift workflow so
  "current" has a dated, tested meaning rather than a blanket compatibility claim.

## Capabilities

### New Capabilities

- `cypher-language-selection`: query override and configured process default in one pipeline.
- `cypher-current-query-semantics`: current clauses, composition, expressions,
  path behavior and batch transactions.
- `cypher-current-values-schema-search`: persistent value types, graph types
  and indexed SEARCH.
- `cypher-release-conformance`: release-scoped catalogs, protocol evidence,
  compatibility reporting and upstream tracking.

### Modified Capabilities

None in the active spec inventory (`openspec list --specs`: no specs).
These contracts complement, not duplicate, the in-flight Enterprise
impersonation/CDC/Query API capability specs.

## Impact

Baseline: `15644e54cf20e0e5f82896c5f9db1e1f7de9c2b8`.
Existing work includes shared row execution, transaction boundaries, simple
relationship quantifiers, native schema enforcement, temporal values and many
Neo4j 5 functions. Closed #743/#744 are prior framing work, not proof of 25
support. Remaining work is mapped in [audit](audit.md), [design](design.md) and
[tasks](tasks.md).

Related open dependencies: #935 (effective graph/security), #936
(impersonation), #937 (CDC), #938 (authorized search pagination).
Follow [convergence Step 1](../../../docs/plans/cypher-convergence-plan.md#step-1--install-the-correctness-baseline-before-parser-changes):
independent evidence before changing semantics, shared execution, no replay
after effects. Only planning/evidence/documentation is delivered here.
