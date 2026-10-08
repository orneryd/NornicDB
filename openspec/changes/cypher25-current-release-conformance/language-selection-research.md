# Query-expressed language selection: source research

## Source and evidence boundary

The requested checkout at `/Users/timothysweet/src/neo4j` was not present;
a search under `/Users/timothysweet/src` found no Neo4j checkout. This research
therefore used the public Neo4j source, not a local checkout.

Repository: `neo4j/neo4j`, branch `2026.09`, revision
`54a7dcf7c2501b31866199143364c5332da8936f`.
Source was inspected through GitHub's contents API. Examples below are derived
from grammar, semantic tests and documentation; they have not been executed
against a live Neo4j instance.

## It is a language selector, not a separate runtime

`CYPHER 5` and `CYPHER 25` select the query language for that statement.
Neo4j also has default selection, but that configuration is outside this
NornicDB delivery. The language selector is independent of runtime options.

- [Preparser tests, lines 213 onward](https://github.com/neo4j/neo4j/blob/54a7dcf7c2501b31866199143364c5332da8936f/community/cypher/cypher-tests/src/test/scala/org/neo4j/cypher/internal/preparser/CypherPreParserTest.scala#L213)
  exercise each explicit version with each database default. The same tests
  parse `runtime=slotted` and other options separately.
- [Official version selection](https://neo4j.com/docs/cypher-manual/current/queries/select-version/)
  documents the query prefix and default precedence.

Most established query syntax needs no rewrite:

```cypher
CYPHER 5 MATCH (p:Person) RETURN p.name AS name ORDER BY name
```

```cypher
CYPHER 25 MATCH (p:Person) RETURN p.name AS name ORDER BY name
```

This example asserts shared syntax, not that every possible expression has
identical cross-version semantics.

## Equivalent query using new syntax

Cypher 5:

```cypher
CYPHER 5
UNWIND [1, 2, 3] AS x
WITH x, x * 10 AS scaled
WHERE scaled > 10
RETURN x, scaled
ORDER BY x
```

Cypher 25:

```cypher
CYPHER 25
FOR x IN [1, 2, 3]
LET scaled = x * 10
FILTER scaled > 10
RETURN x, scaled
ORDER BY x
```

Expected rows for both:

| x | scaled |
| --- | --- |
| 2 | 20 |
| 3 | 30 |

The important scope distinction is that `LET` retains `x`. The 5 equivalent
must carry `x` through `WITH`; `WITH x * 10 AS scaled` alone would drop it.
`FILTER` operates on existing rows. It must not be implemented as a blanket
replacement of `WHERE`, especially for OPTIONAL MATCH where placement changes
row preservation.

Source:

- [25 grammar, FOR/LET/FILTER](https://github.com/neo4j/neo4j/blob/54a7dcf7c2501b31866199143364c5332da8936f/community/cypher/front-end/parser/v25/parser/src/main/antlr4/org/neo4j/cypher/internal/parser/v25/Cypher25Parser.g4#L276)
  defines standalone FILTER, UNWIND, FOR variable IN expression, and LET.
- [5 grammar, clause alternatives](https://github.com/neo4j/neo4j/blob/54a7dcf7c2501b31866199143364c5332da8936f/community/cypher/front-end/parser/v5/parser/src/main/antlr4/org/neo4j/cypher/internal/parser/v5/Cypher5Parser.g4#L41)
  includes WITH/UNWIND, not those new clauses.
- [LET semantic tests](https://github.com/neo4j/neo4j/blob/54a7dcf7c2501b31866199143364c5332da8936f/community/cypher/front-end/frontend-tests/src/test/scala/org/neo4j/cypher/internal/frontend/LetClauseSemanticAnalysisTest.scala#L35)
  check retained UNWIND variables and chained bindings; lines 86 onward expect
  these queries to fail in 5 and pass in the newer language.
- [LET documentation](https://neo4j.com/docs/cypher-manual/current/clauses/let/)
  explains scope retention and excludes aggregation/DISTINCT.
- [FILTER documentation](https://neo4j.com/docs/cypher-manual/current/clauses/filter/)
  explains standalone filtering and differences from WHERE.

## NornicDB implementation decision

Add these constructs to the existing shared pipeline. Retain the prefix as
statement context instead of validating and discarding it. FOR can reuse bound
list iteration; LET retains scope; FILTER uses the shared row predicate
evaluator. There is no separate executor, text-rewrite replay or retry as 5.

Existing queries and APIs remain supported, including constructs removed by
upstream 25. Record retained constructs as explicit NornicDB extensions.
That intentionally prevents a claim of identical upstream rejection behavior.
The default for unprefixed queries is configured as
`NORNICDB_CYPHER_VERSION=5|25`; explicit prefixes override it. Unset retains 5,
while present-empty/invalid settings fail startup validation. With default 25,
the second example also works without its prefix. Configuration is resolved
once outside query execution and passed to every executor/transport.
No persisted database-language migration, default-language DDL or automatic
cutover to 25 is introduced.
