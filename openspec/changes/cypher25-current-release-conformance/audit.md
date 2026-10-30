# Cypher 25 audit and release inventory

## Verdict and scope of evidence

**Not compliant.** This is a code-backed, executable gap assessment, not a
complete differential certification or a percentage of language coverage.
Repository: `15644e54cf20e0e5f82896c5f9db1e1f7de9c2b8`.
Research/test date: 2026-10-08 UTC.

- `validateCypherPreamble` in
  [statement_framing.go](../../../pkg/cypher/statement_framing.go) accepts only
  version 5. [executor_entry.go](../../../pkg/cypher/executor_entry.go) validates
  and then strips the preamble without preserving a language semantic context.
- Existing `TestResidualCypherPreambleAdmission` explicitly expects 25 to fail.
  `go test -tags 'noui,nolocalllm' ./pkg/cypher -run '^TestResidualCypherPreambleAdmission$' -count=1`
  **passed**; this proves intentional rejection, not 25 support.
- An embedded probe ran 43 statements through each of the `nornic` and `antlr`
  configurations, using a fresh namespaced MemoryEngine and two-node graph per
  case: **86 executions, 70 errors, 16 result-returning executions**.
  Two of those result-returning executions are the wrong PROPERTY_EXISTS result.
  Do not translate these deliberately gap-focused probes into a coverage score.
- The existing [reference manifest](../../../testing/cypher/tck/testdata/neo4j-reference.json)
  pins **5.26.30 Community**, which cannot validate Cypher 25 or Enterprise
  features. The openCypher TCK is valuable but not a Cypher 25 certification.
- No current Enterprise server was launched in this audit. Exact edge-case
  errors and full cross-protocol/durability semantics remain phase-0 oracle work.

### Reproduce the local evidence

The [probe source](evidence/probe.go.txt) is stored as text so it is not added
to the repository's Go package graph. [Raw results](evidence/probe-results.jsonl)
include queries, parser configuration, columns/rows or errors. These are
synthetic fixtures, not application data.

```sh
cp openspec/changes/cypher25-current-release-conformance/evidence/probe.go.txt /tmp/nornic-cypher25-probe.go
go run -tags 'noui,nolocalllm' /tmp/nornic-cypher25-probe.go
rm /tmp/nornic-cypher25-probe.go
```

The executed audit used the same source in session storage, with output
redirected to the persisted JSONL. Both probe and targeted test commands exited
0; the linker printed its existing duplicate `-lobjc` warning.
Feature errors are recorded outcomes, not a failed probe process.

## Probe findings

| Cases | Observed result | Meaning |
| --- | --- | --- |
| P01 / P02 | CYPHER 25 ArgumentError / CYPHER 5 returns 1 | Confirmed language admission gap |
| P03-P11 | LET/FILTER/FOR/NEXT/WHEN/braced UNION/RETURN ALL/WITH ALL/GROUP BY rejected | Missing current composition surface |
| P12-P13 | Map comprehension/string interpolation rejected | Expression parsing/evaluation gaps |
| P14-P21 | cardinality, coll.distinct, string.join, format, patterned date, uuid, vector, allReduce rejected | Function/value surface gaps |
| P22 | stDev(null) returns null | This one new-style behavior already works; test broader empty/nonempty cases, not blanket replacement |
| P23-P24 | toString(MAP) and replace limit rejected | Confirmed updated-signature/semantics gaps |
| P25-P26 | ceiling and ln rejected | Registration alone is insufficient: ceiling is registered in functions_neo5.go yet rejected through public Execute |
| P27-P30 | Explicit match modes, ANY SHORTEST, ACYCLIC rejected | Existing variable-length traversal does not establish current path semantics |
| P31 / P43 | PROPERTY_EXISTS counts 0 / IS NOT NULL counts 2 | **Silent wrong result**, with both stored nodes carrying x; add failing semantic regression first |
| P32-P36 | IS LABELED, SEARCH, SHOW GRAPH TYPE, composed SHOW rejected; SHOW TRANSACTIONS null is TypeError | Current predicate/search/schema/introspection gaps |
| P37 | `$0hello` rejected as numeric literal | Cypher 25 parameter lexical rule missing |
| P38-P40 / P42 | sinh(0)=0, char_length=3, simple relationship quantifier length=1, valueType integer works | Reuse existing helpers, not wholesale rewrites |
| P41 | Tested dynamic-label spelling rejected | Investigate exact spelling/route; not proof that every dynamic label form is unsupported |

Most feature probes omit the version prefix to get past the known global
rejection and examine current execution. They do **not** demonstrate supported
Cypher 25 mode. Both parser configurations share substantial execution, so
agreement is not independent oracle evidence. Some error messages differ.

## Verified upstream target and sources

The [official current versions](https://neo4j.com/current-neo4j-versions/) and
[2026.09.0 release notes](https://neo4j.com/release-notes/database/neo4j-2026-09-0/)
identify **2026.09.0, released 21 September 2026**, as current stable. The
current LTS patch is 5.26.31; neither it nor the existing 5.26.30 baseline is a
substitute for a 25 oracle. Resolve exact Community/Enterprise image digests
before differential work; do not invent a digest or use a floating image.

Primary inventory:
[additions/removals](https://neo4j.com/docs/cypher-manual/current/deprecations-additions-removals-compatibility/),
[language selection](https://neo4j.com/docs/cypher-manual/current/queries/select-version/),
[GQL conformance](https://neo4j.com/docs/cypher-manual/current/appendix/gql-conformance/).
The GQL page is dated 2026.06: Cypher 25 compatibility is not equivalent to full
ISO GQL conformance. Do not claim the latter.

## Stable release catch-up matrix

IDs below are implementation fixture groups, not passing tests. Each group
must expand into exact statements/expected results/errors/side effects. Reuse
existing good behavior after oracle checks. "Gap" includes partial coverage.

| ID / release | Stable public surface | Local evidence / implementation owner |
| --- | --- | --- |
| V01 / 2025.06 | CYPHER 25/5 overrides and native NORNICDB_CYPHER_VERSION=5\|25 default; context, caches and routing | Hardcoded 5. Config, executor construction/framing/entry, caches and multidb routing. Persisted database defaults and default-language DDL remain out of scope |
| Q01 / 2025.06 | LET, FILTER, RETURN/WITH ALL, read-after-write without WITH | P03/04/09/10 gaps; pipeline clause kinds and projection parser |
| Q02 / 2025.06, 2025.08 | NEXT whole-table composition, WHEN/ELSE, braces mixing UNION variants; aggregation fixes across NEXT/CALL/UNION | P06-08; pipeline scope/composition, not per-row CALL substitution |
| Q03 / 2026.04, 2026.07 | FOR and explicit GROUP BY | P05/P11; unwind iteration plus shared aggregation grouping |
| P01 / 2025.06 onward | DIFFERENT RELATIONSHIPS, REPEATABLE ELEMENTS, quantified path/group variables, ANY/SHORTEST/GROUPS parameter counts | P27-29; simple quantifier control P40 works; shared traversal/planner |
| P02 / 2026.03, 2026.05 | ACYCLIC and combinations with restrictive selectors | P30; node uniqueness differs from relationship uniqueness |
| E01 / 2025.06 | Imported variables as constants in expression subquery aggregation; numeric-leading parameters; replace limit; hyperbolic functions | P37/P24 gaps; P38 works; lexical/scope/registry convergence |
| E02 / 2025.07-.11 | Dynamic label/type predicates; allReduce; temporal format/pattern constructors; coll.distinct/flatten/indexOf/insert/max/min/remove/sort | P15/P17/18/21/41; row expressions, quantifiers, temporal and function catalog |
| E03 / 2026.02-.07 | Full GQL alias set; PROPERTY_EXISTS; IS [NOT] LABELED; string.indexOf/join/regexReplace; null stDev behavior; cardinality | P14/16/25/26/31/32 gaps; P22/39 controls; shared evaluators/catalog/static typing |
| E04 / 2026.08-.09 | Interpolated strings, map comprehensions, broadened toString/toStringList/toStringOrNull | P12/13/23; lexer/canonicalizer and native value rendering |
| T01 / 2025.10 | Native VECTOR constructors, dimensions/coordinate types, norm/distance/conversions/similarity, VECTOR type constraints | P20; existing slices/float32 search are not the native type |
| T02 / 2026.08 | Native UUID, constructors/bits, equality/order/string conversion/property persistence | P19; randomUUID string and UUID property names are not typed UUID |
| S01 / 2026.01-.09 | VECTOR/FULLTEXT SEARCH in MATCH/OPTIONAL MATCH, SCORE, index target/filter properties, IN filter, analyzer/SKIP/OFFSET, quantization/search expansion options | P33; vector/fulltext procedures exist, not the clause; schema.VectorIndex has one label/property |
| G01 / 2026.02-.07 | Open graph types SET/ADD/ALTER/DROP/SHOW/AS GRAPH, implied labels/relationship endpoints, classification/enforcedLabel metadata | P34; native constraint contracts are reusable but not equivalent |
| B01 / current stable | Concurrent CALL IN TRANSACTIONS, status/error/retry modes, DISJOINT BY expressions/AUTO/NONE | Current call parser has sequential inTransactions/batchSize, no full modifier model |
| A01 / 2025.06-.09 | Current SHOW column/type/null changes, constraint names, propertyTypes, composable SHOW/TERMINATE, currentQueryProgress | P35/36; show_admin emits 5-style timestamps/empty strings |
| A02 / 2026.03-.09 | AUTH RULE administration, OIDC attributes/native tags, role assignment, SHOW USERS/ROLES AS COMMANDS, auth-rule privilege filters/new columns/SHOW USER CREDENTIALS | Extend #935 canonical authorization; no second policy engine |
| R01 / 2025.06 | Upstream removals: indexProvider, entity RHS SET, same-MERGE references, graph quoting/identifiers, procedures/options; REVOKE errors | Inventory differences and preserve existing supported behavior/APIs, including explicit 25. Test real effects, not ignored options; do not add breaking rejections for upstream parity |
| I01 / 2026.04-.09 | Current CDC/current timestamp, Query API tx configuration/timings/notification filters, protocol type and diagnostic changes, procedure/catalog changes | Explicit current-release delta over earlier Enterprise plan; distinguish server API version from Cypher language |
| O01 / current stable | Database seed option evolution and current public operational procedures, including reloadProcedures | Map to native restore/plugin capabilities; never report fake success or execute foreign store formats |

Special notes:

- Scope correction: query prefixes override `NORNICDB_CYPHER_VERSION=5|25`;
  unprefixed queries use that configured version, unset retains 5 and invalid
  settings fail validation. No database migration or breaking deletions.
  Existing supported syntax/APIs remain available. Retained upstream-removed constructs
  are documented extensions, so exact rejection parity is not claimed.
  [Source research](language-selection-research.md) explains per-query selection
  and equivalent 5/25 examples. Neo4j's persisted database-language defaults/DDL
  are an acknowledged upstream surface outside this delivery.
- [Graph types](https://neo4j.com/docs/cypher-manual/current/schema/graph-types/)
  became **GA in 2026.06**. Quantization options became GA in 2026.07. They are
  not left on a preview backlog merely because their initial introduction was
  preview.
- [VECTOR](https://neo4j.com/docs/cypher-manual/current/values-and-types/vector/)
  and [UUID](https://neo4j.com/docs/cypher-manual/current/values-and-types/uuid/)
  require Enterprise oracle coverage; Nornic stores native equivalents, not
  Neo4j block format.
- [SEARCH](https://neo4j.com/docs/cypher-manual/current/clauses/search/) is a
  MATCH/OPTIONAL MATCH subclause. FULLTEXT is not an alias for vector search.
  Approximate result sets and scores need declared algorithm/tolerance evidence,
  not blindly normalized equality or a claim of identical index internals.
- [Batch transactions](https://neo4j.com/docs/cypher-manual/current/subqueries/subqueries-in-transactions/)
  can preserve already-committed children after later failure; don't impose
  whole-statement rollback on independent commits.
- [ABAC](https://neo4j.com/docs/operations-manual/current/authentication-authorization/attribute-based-access-control/)
  is authorization based on validated attributes, not a query function that
  trusts arbitrary supplied claims.
- Inventory optimizer operators separately: native plan topology, JVM runtimes,
  Infinigraph shards, Neo4j store format and optional plugins are not promised
  internal replicas. Public EXPLAIN/PROFILE structure and truthful native
  statistics remain required. External plugin features are not silently counted
  as core Cypher conformance.

## Continuing to track development

Pin dated source revisions/URLs and classify each upstream delta as stable
language, server API, Enterprise, preview, plugin, or internal-only. Track
post-2026.LTS announcements and unreleased branches on a separate watchlist;
never silently increase the stable target from moving `/current/` pages.
Every new stable item needs owner module, fixture IDs, implementation status
and acceptance evidence. Publish scoped support, partial, missing and untested
states rather than the old unqualified "Complete" claim.
