# Release Notes — v1.4.1 (Draft)

> **Status:** Draft for review. Not for publication yet.
> **Scope:** All changes between tag `v1.4.0` (`22b90d2d`) and the upcoming `v1.4.1` — 97 commits on `main` plus the Cypher 25 shared-grammar branch (assumed merged). Figures below are from commit inspection, not a packaged build.

---

## Highlights

- **Cypher 25 shared grammar (foundation slice).** `LET`, `FILTER`, and `FOR` reading clauses execute in the existing shared pipeline, with an optional `CYPHER 5` / `CYPHER 25` header. No separate executor, router, or text-rewrite fallback.
- **Correctly rounded math.** `^` / `power()` (and `exp` / `log` / `log2` / `exp2` / hyperbolic functions) now return Neo4j's correctly rounded results, fixing the 1-ulp `^` divergence (#981).
- **A large Neo4j 5.26.30 compatibility sweep (#907)** across operators, literals, functions, UNION, DELETE, temporal values, and MERGE.
- **Lazy Bolt auto-commit reads** stream rows as the driver `PULL`s them (#939).
- **Storage and concurrency correctness** for label scans, record errors, and concurrent unique-key MERGE (#961).

---

## 1. Cypher 25 — Shared Grammar Foundation

NornicDB previously rejected `CYPHER 25`. This release adds the first implemented slice of the Cypher 25 conformance plan.

### What is included

- **`LET`** — bindings with retained scope, including chained `LET` clauses. Aggregate expressions are rejected.
- **`FILTER`** — a standalone reading clause filtering rows, with optional `WHERE`.
- **`FOR x IN expr`** — iteration over a list, equivalent to `UNWIND expr AS x` (including scalar coercion: `FOR x IN 1` returns one row).
- **Optional language header** — `CYPHER 5` / `CYPHER 25` are accepted and discarded by both parsers; callers pass statements as written.

```cypher
FOR x IN [1, 2, 3]
LET scaled = x * 10
FILTER scaled > 10
RETURN x, scaled ORDER BY x
```

### One pipeline, two front ends

- The **SRD parser** (default) accepts `LET` / `FILTER` / `FOR` and correlated unscoped `CALL` without a header — a permissive, deterministic extension.
- The **ANTLR parser** keeps the strict Cypher 5.26 contract: those additive clauses require `CYPHER 25`, and implicit unscoped `CALL` imports remain rejected. The ANTLR grammar now also parses the `CYPHER 5` / `CYPHER 25` preamble directly.

Both front ends feed the same pipeline clause kinds and operators; ANTLR is a syntax front end, not a second execution path.

### Rule enforcement

`LET`/`FOR` redeclaration is rejected (`VariableAlreadyBound`), non-boolean `FILTER` predicates fail as in Neo4j (including the list-to-boolean coercion error), aggregates are rejected in `FILTER`, and conflicting `CYPHER 5 CYPHER 25` is an `ArgumentError`.

> This is the **foundation slice** of the Cypher 25 conformance plan — not full 2026.09 conformance. The remaining inventory (SEARCH, graph types, typed VECTOR/UUID values, standalone ORDER BY after LET, and more) is tracked in the plan and stays open.

---

## 2. Correctly Rounded Math (#981)

Go's `math` package (fdlibm) is up to 1 ulp away from Java's `Math` for some inputs, so `3 ^ 2.5` returned `15.588457268119894` instead of Neo4j's `15.588457268119896`.

- Ported musl's double-precision `exp`, `exp2`, `log`, `log2`, and `pow` (Arm optimized-routines, MIT) and the six hyperbolic functions into `pkg/math/libm`.
- The package mirrors the standard library `math` API, so every `math` import became an import alias to `libm` with no call-site changes.
- `Sin`/`Cos`/`Tan`/inverse-trig/`Log10`/`Log1p`/`Expm1` are fdlibm-derived identically in musl and Go, so they are re-exported unchanged.

`3 ^ 2.5` and `9007199254740993 ^ 2.5` now match Neo4j exactly.

---

## 3. Neo4j 5.26.30 Compatibility Sweep (#907)

Dozens of correctness fixes from a differential sweep against `neo4j:5.26.30-community`. Notable groups:

- **Operators** — `AND` / `OR` / `XOR` / `NOT` follow Neo4j's rules through one implementation; unary plus; `^` static types.
- **Literals** — `true`, `false`, `null`, `NaN`, and `Infinity` are literals even where a variable has that name; numeric literals group digits with underscores; `0X` / `0O` prefixes kept as a NornicDB extension; a NaN MERGE property fails.
- **UNION** — matches columns by name; `UNION` works inside `EXISTS` / `COUNT` / `COLLECT`; result-column checks.
- **DELETE & properties()** — deleted entities read as Neo4j's empty entity; `properties()` on a deleted relationship fails; map keys stay DELETE targets; a non-DETACH DELETE of a connected node is checked at commit.
- **Functions** — argument counts and compile-time types from catalog signatures; `trim` specifier forms; temporal functions' run-time argument errors; `isEmpty(null)`; `point.withinBBox` on non-points; `reduce` with a literal accumulator.
- **CALL subqueries** — `OPTIONAL CALL` keeps rows the call produces nothing for; an unscoped body's implicit imports are kept as a NornicDB extension; branch-local declarations.
- **SET / MERGE** — SET applies items in order; MERGE takes repeated `ON CREATE` / `ON MATCH` and multi-relationship paths through one action parser.
- **Aggregates** — duration `sum`/`avg`; `sum` switches to float on overflow; `avg` is a running mean; equal-length durations order by months; aggregate value types.
- **Framing** — `FINISH` after `WITH` and `YIELD`; `finish` as a valid variable name; a variable read anywhere in a `RETURN` / `WITH` / `UNWIND` expression must be bound.
- **Ordering** — maps, nodes, relationships, and paths order as Neo4j; tied rows and `keys()` / `labels()` compared the way Neo4j defines them (#931).

### Documented intentional differences

- `keys()` returns property names in **alphabetical order** on every route, where Neo4j's order is unspecified and unstable (property-store layout / map hash order). Cypher defines no order for `keys()`, and the openCypher TCK grades it ignoring list order (#994).
- Retained extensions from the sweep remain documented rather than removed.

---

## 4. Bolt & Protocol

- **Lazy auto-commit reads (#939):** the server produces an auto-commit read's rows as the driver `PULL`s them, instead of materializing the whole result up front.
- An error on a row after the first 1,000 fails the `PULL` that reaches it (#939).

---

## 5. Storage & Concurrency

- Label scans and bounded scan tails fail on unreadable records; one-pass record errors are reported in visitation order.
- Concurrent `MERGE` of a uniquely constrained key matches the creator's node (#961); a multi-MERGE UNIQUE race is retryable like single-node MERGE.
- Embeddings follow the embedded text; storage hands callers their own node copies (#963, #965).
- Search results are keyed by a search-index revision so a rebuilt index invalidates cached results.

---

## 6. Search

- `db.index.vector.queryNodes` returns when the HNSW search reaches fewer than `k` nodes (#974).
- A filtered search fills its page; an empty filter list matches nothing.

---

## 7. MCP

- Pin tools to a database through the URL path (`/mcp/{database}/…`); the pinned database is injected and cannot be overridden by the payload.
- The standalone `task` tool is folded into `tasks` (management arguments create/update/complete/delete; filter arguments list tasks).

---

## 8. Performance

- OPTIONAL MATCH / OPTIONAL CALL keyword checks without allocating.
- ORDER BY … LIMIT over a property index starts at the bound WHERE puts on the sort property.
- Reduced shared WITH LIMIT source overhead; plain RETURN keeps its captured result local to its row-source branch.
- Equivalence keys of single values are built in one allocation.
- A new allocation-ratchet gate compares fixed-count benchmark workloads against the baseline, so parsing stays allocation-free.

---

## 9. Infrastructure & Build

- Upgrade Debian runtime packages during Docker builds.
- Differential conformance now runs a fixed corpus against the pinned Neo4j reference and refreshes known differences as fixes land.

---

## Upgrade Notes

- **No breaking changes.** Existing Cypher 5 statements and NornicDB extensions continue to work unchanged.
- `CYPHER 5` / `CYPHER 25` headers are optional. The SRD parser accepts the new clauses without a header; the ANTLR parser requires `CYPHER 25` for them.
- `^` / `power()` results may change in the last ulp, matching Neo4j instead of Go's `math.Pow`.
- `keys()` remains alphabetical (see the documented difference above).
