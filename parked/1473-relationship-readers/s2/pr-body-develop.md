Every place in the Cypher package that reads the inside of a relationship pattern (`-[ … ]->`) now goes through one reader, `relationshipDeclarationOf` / `relationshipDeclarationHead`. Before this change, valid statements could panic, silently return wrong rows, write null, fail MERGE, or be rejected, depending on which clause or fast path read the pattern.

Part of #1055. Supersedes #1473 (its correct parts are kept here as its four commits). Label `bug`, milestone v1.4.4. Release copy: see the linked release/v1.4.4 PR.

## What was wrong (Neo4j 5.26.30 is the reference)

- **Go panic:** a quoted string containing `)` inside a pattern, as in `MATCH ()-[r:$('D):Z|Q{*..')]->()`, panicked the executor (slice bounds out of range).
- **Silently wrong rows:** in `MATCH (:A)-[r:R {k: {outer:{xs:[11,17]}}.outer.xs[i]}]->()`, the property filter was dropped, so rows with k = 999 came back next to k = 17. Under the same nested-map access, CREATE wrote null and MERGE failed.
- **Backtick-quoted types holding quotes or arrows:** for types like `` `D]'Z` ``, `` `D"Z]-[` `` and `` `U]-[:z` ``:
  - MATCH, OPTIONAL MATCH, SET and pattern comprehensions failed with "Variable `b` not defined";
  - MERGE failed with "invalid traversal pattern";
  - `MATCH ()-[r:`U]-[:z`]->()` returned 0 rows.

  Escaped quotes in dynamic types (`$('D]\'Z')`, `$("D\"Z]-[")`) failed the same way.
- **Rejections and type errors:**
  - Inline `WHERE` with a `*` inside a dynamic type, as in `[r:$(['X','DZ'][i * 1]) WHERE r.w = 17]`, was rejected as a variable-length relationship.
  - A CASE returning a list as a dynamic type was a type error.
  - A slice as a dynamic type was a TypeError.
- **PROFILE:** PROFILE of a pattern with a dynamic type failed on an internal function. PROFILE also replaced data values that matched a rewritten part of the statement: `type(r)` came back as `:$('D]\'Z')`.
- **Dynamic types on variable-length ranges (`*`)** returned different rows from Neo4j (see the note below).

## Changes

- **One reader for the inside.**
  - `relationshipDeclarationHead` now also reads the element's own `WHERE` predicate (new `predicate` field), so nothing in a predicate is taken as the type, length or map.
  - Quotes, backticks, escapes and parenthesised expressions are masked once, by the reader.
- **Routed through the reader.** Each of these readers used to split on `] : { * |` on its own:
  - **UNWIND batch plans:** UNWIND…MERGE chain, fixed chain link, relationship MERGE batch, multi-match CREATE. They now share one `(a)-[…]->(b)` shape reader, `boundOutgoingRelationship`. A backtick-quoted type no longer keeps its backticks.
  - **CALL-tail relationship plan** and **write-stretch analysis** (a relationship's own WHERE now counts as a read).
  - **MERGE retry planner** (`appendPatternPropertyExpressions`).
  - **Label-expression rewriter:** element, same-pattern map reads, relationship element, inline-WHERE length check, quantified relationships, path-selector repetition checks, repeated relationship variables, declared variables.
  - **Validators:** CREATE map validation, static operator checks, parameter-map check, pattern-variable check, relationship variable collection.
  - **Others:** bound-chain constant check, EXPLAIN variable-length detection, shortestPath message, schema FOR patterns, AST builder.
- **Scanners that cross relationship brackets** now skip a relationship's inside, and skip quoted names (backticks included), through the shared bracket finder:
  - traversal pattern splitting: one splitter, `traversalPatternParts`, for single and chained patterns;
  - node-variable collection: `extractNodeVariables`, `extractVariableNamesFromPattern`;
  - pattern comprehension split;
  - CREATE node splitting;
  - the delete-cache heuristic and the pattern-shape count.
- **PROFILE.**
  - From #1473: PROFILE re-runs the source statement, so internal predicates are never validated as user calls.
  - New here: the client-text restore applies to EXPLAIN rows (plan text) only. PROFILE rows are data.
- **Deleted**, because no statement reaches them: `parseOptionalClauseEndpoints`, `parseOptionalRelPattern`, `optionalRelationshipTypeMatches`, `evaluateRelationshipPatternInWhere` and its hop/chain/direction helpers (`rel_existence_pattern.go`), `extractRelationshipType`, `callTailHasRelationshipTypeConstraint`.
- **From #1473, kept:** the quote-aware matcher fixes the panic, `rowSubscriptReceiverStart` reads the nested-map receiver, and CASE dynamic types use the signature owner's union admission.

### Note for the maintainer: dynamic types on `*` ranges

Neo4j 5.26.30 does not filter a variable-length relationship by a dynamic type at all. `MATCH (:A)-[r:$('NOPE')*1..2]->()` returns every path of any type, and so do the values 7 and null. Single-hop dynamic types, static types and property maps still filter. This looks like a Neo4j bug. Following the project rule, NornicDB now does the same. The rule is applied in one place (`relationshipElement` in label_expression_rewrite.go, with a comment saying so), so it is a one-line choice if you prefer to keep filtering. The openCypher TCK has no dynamic types, so it does not decide this.

## Testing

All runs are on TrueNAS (`sig/tnrun.sh`).

- **New `TestRelationshipPatternReaderMatchesNeo4j`.** It runs a 415-statement stream: one database, statements in order, both auto-commit and one explicit transaction per statement. Neo4j's output for each statement is recorded in `testdata/relationship_pattern_oracle.json`.
  - Neo4j job: j20261018122655-75088-eaef.
  - Coverage:
    - dynamic types with braces, stars, ranges, quotes and escapes;
    - CASE (scalar and list);
    - backtick types holding quotes and arrows;
    - nested map access in node and relationship maps;
    - inline WHERE;
    - mixed-type ranges.
  - Clauses: MATCH, OPTIONAL MATCH, CREATE, MERGE (repeated, with ON CREATE / ON MATCH), UNWIND…MERGE, write stretches, schema statements, EXPLAIN and PROFILE.
- **Red** on develop `16923aef`, both parsers: i20261018123110-13160-01e6 (fails in every group above). On #1473's head: 69 of 415 statements still differed (i20261018123114-28052-80c2).
- **Green**, both parsers: oracle and the changed tests i20261018132504-88952-65f8 (only `TestEvaluateWhereForContext_Branches` fails, the develop baseline fixed by #1468).
- **Full `pkg/cypher`**, both parsers: running (results will be added here).
- `vet`: i20261018130044-50888-dce1. gofmt clean on the final head.

## Performance

Paired A/B, 10 rounds, `a20261018132748-28960-3638` (base develop `16923aef` vs head): StatementRouting, MatchSemanticValidationCached, MergeUniqueKey, ChainedTraversal, the UNWIND batch plans and OPTIONAL MATCH traversal. Queued; results will be added here.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
