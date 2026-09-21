# Cypher convergence issue matrix

Snapshot: 2026-09-21; repository `orneryd/NornicDB`; local base
`a427a46815c607d0801331f4975e26cc941d125a`.

GitHub returned 45 open issues. The 35 below are in the Cypher program: 33
correctness/storage bugs, performance issue #487 and proposal #482.

Implementation and acceptance rules are in the
[main plan](cypher-convergence-plan.md). Initial status for every row is
**open / reproduction pending**. Phase mappings require confirmation by failing
tests. The linked issue body is the source for exact setup/query/expected-result
fixtures; retain every variant and follow-up comment.

The TCK column names candidate directories under `tck/features/clauses` or
`tck/features/expressions`. It does **not** claim a verified exact upstream
scenario exists. Step 1 must attach exact upstream IDs and permanent
`gh-<issue>-<case>` regression IDs. Supplementary local tests remain required
where the TCK does not cover a Neo4j extension, storage invariant or performance.

## Scoped issues

| Issue | Fix step | Candidate TCK family / supplemental suite | Concrete closure evidence |
| --- | --- | --- | --- |
| [#447](https://github.com/orneryd/NornicDB/issues/447) | 6 | unwind; match; null; with-where | UNWIND retains OPTIONAL MATCH columns/cardinality; absent optional node IS NULL is true; filters and grouping execute. |
| [#448](https://github.com/orneryd/NornicDB/issues/448) | 3B | Local storage concurrency suite | Committed label membership/count remains complete during embedding writeback; deterministic barriers exercise overlay publication. |
| [#449](https://github.com/orneryd/NornicDB/issues/449) | 6 | return-orderby | All sort keys apply, including hidden properties, ASC/DESC null order and LIMIT; issue fixture returns Dee then Cid. |
| [#450](https://github.com/orneryd/NornicDB/issues/450) | 6 | return; with | RETURN c.name AS c reads input c before publishing alias; returns company string, not node. |
| [#451](https://github.com/orneryd/NornicDB/issues/451) | 6 | with-orderBy; aggregation | WITH ordering feeds collect; one group per company, actual person names in specified order. |
| [#452](https://github.com/orneryd/NornicDB/issues/452) | 6 | unwind; return-orderby; with-where; aggregation | All three original cases plus comment: ORDER BY sorts and WITH i,i*i AS sq WHERE sq>1 RETURN collect(sq) returns [4,9]. |
| [#453](https://github.com/orneryd/NornicDB/issues/453) | 5 | list; quantifier; mathematical | Comprehension applies WHERE then projection: odd range(1,5) times ten is [10,30,50]. |
| [#454](https://github.com/orneryd/NornicDB/issues/454) | 5 | mathematical; precedence | Integer 7/2 is 3, float 7.0/2 is 3.5 and 2^3 is float 8.0; repeat after WITH. |
| [#455](https://github.com/orneryd/NornicDB/issues/455) | 3A | merge; set | Relationship ON CREATE SET persists w=1; repeat MERGE and test ON MATCH without duplicate edges. |
| [#456](https://github.com/orneryd/NornicDB/issues/456) | 3A,6 | remove; set; match | REMOVE property then SET label executes only on matched Dee; fresh read confirms city absent and exactly one VIP. |
| [#457](https://github.com/orneryd/NornicDB/issues/457) | 6 | call; with; aggregation | Correlated CALL preserves outer company and per-company counts in both modes; exercise nested imports and empty inner matches. |
| [#458](https://github.com/orneryd/NornicDB/issues/458) | 5 | temporal; graph | Date/duration component access and values carried through WITH retain types and return year/month/days; supplement pinned TCK where needed. |
| [#459](https://github.com/orneryd/NornicDB/issues/459) | 6 | return-orderby; string | ORDER BY toLower(p.name) DESC evaluates hidden sort expression and returns correct LIMIT row in both modes. |
| [#460](https://github.com/orneryd/NornicDB/issues/460) | 5 | list; with | reduce after WITH evaluates its scoped accumulator; literal [1,2,3] sum is 6 and collected input is also covered. |
| [#461](https://github.com/orneryd/NornicDB/issues/461) | 3B | create; match; Local MVCC/reopen suite | Pure autocommit CREATE of nodes plus edge is visible to later explicit pattern reads and MATCH SET, including after restart; test repair needs for preexisting records. |
| [#462](https://github.com/orneryd/NornicDB/issues/462) | 3A | set; mathematical | Relationship SET reads bound relationship; increment/multiply stores numeric result and readback agrees in opposite mode. |
| [#463](https://github.com/orneryd/NornicDB/issues/463) | 6 | aggregation; mathematical | Evaluate avg over all matching rows before round/multiply/divide; original issue fixture yields 11.92. |
| [#464](https://github.com/orneryd/NornicDB/issues/464) | 6 | with-orderBy; aggregation | Aggregate alias total remains its numeric value across WITH ORDER BY and RETURN; never literal 'total'. |
| [#465](https://github.com/orneryd/NornicDB/issues/465) | 5 | graph; path | Property access on startNode/endNode function results returns endpoint properties, including null/missing cases. |
| [#466](https://github.com/orneryd/NornicDB/issues/466) | 5 | map | Map projections support selected properties, all properties and computed entries; projected values are evaluated. |
| [#467](https://github.com/orneryd/NornicDB/issues/467) | 5 | map | Nested m.b.c resolves recursively; keys(map) returns the map keys under defined ordering expectations. |
| [#468](https://github.com/orneryd/NornicDB/issues/468) | 5,6 | list; aggregation; with | Subscript/slice works on collect results and preserves aggregation cardinality; empty/negative/out-of-range bounds covered. |
| [#469](https://github.com/orneryd/NornicDB/issues/469) | 6 | aggregation; list; match | size(skus) agrees with nonempty collected skus after OPTIONAL MATCH; absent matches obey null-collection rules. |
| [#470](https://github.com/orneryd/NornicDB/issues/470) | 3A | set; null; graph | SET n.prop=null removes property; keys/properties, index lookup and fresh-session read agree. |
| [#471](https://github.com/orneryd/NornicDB/issues/471) | 5 | mathematical; typeConversion; Neo4j extension suite | round(x,precision) evaluates per pinned Neo4j contract; ceil/floor/round preserve expected floating types. Record upstream TCK coverage gaps. |
| [#474](https://github.com/orneryd/NornicDB/issues/474) | 3A | set; map | SET += recursively evaluates variable/arithmetic map entries; persists values rather than expression source; failure rolls back. |
| [#475](https://github.com/orneryd/NornicDB/issues/475) | 3B,5 | list; match-where | Stored-list indexing and head work on live and snapshot views; literal/parameter/collected forms agree without erasing value types. |
| [#476](https://github.com/orneryd/NornicDB/issues/476) | 5 | temporal; comparison | Stored datetime comparisons with datetime constructors preserve temporal type and timezone semantics across persistence. |
| [#477](https://github.com/orneryd/NornicDB/issues/477) | 5,6 | aggregation; comparison | min/max over strings return lexical extrema; cover null, empty and mixed-type rules against the oracle. |
| [#478](https://github.com/orneryd/NornicDB/issues/478) | 5,6 | unwind; match; merge; list | Bind p before MATCH inline p[0]/p[1] evaluation; relationship MERGE produces expected edges and is idempotent. |
| [#479](https://github.com/orneryd/NornicDB/issues/479) | 6 | set; aggregation | RETURN sum(a.hits) after SET aggregates all updated rows; persisted hits and returned total both verified. |
| [#480](https://github.com/orneryd/NornicDB/issues/480) | 3A | set | SET n:LabelA:LabelB adds both labels, equivalent to comma form; repeated assignment is idempotent. |
| [#481](https://github.com/orneryd/NornicDB/issues/481) | 6 | match; aggregation | OPTIONAL MATCH count(r) groups per x, including a zero-count row for unmatched x. |
| [#482](https://github.com/orneryd/NornicDB/issues/482) | 1–9 | All core TCK families and local suites | Conformance CI, strict evaluation, common semantics, fast-path-first execution with ANTLR fallback, differential regression and completed deletion evidence. |
| [#487](https://github.com/orneryd/NornicDB/issues/487) | 7 | Local performance and SI suites | Snapshot-visible streaming/projection avoids full-label materialization for eligible LIMIT; reproduce and improve latency/allocations without SI regression. |

## Other open issues

These remain visible but do not become parser-refactor deliverables. They must
not be closed by this effort merely because a nearby shared helper changed.

| Issue | Current title | Disposition |
| --- | --- | --- |
| [#31](https://github.com/orneryd/NornicDB/issues/31) | [FEATURE] UI Enhancements | Separate product workstream. |
| [#33](https://github.com/orneryd/NornicDB/issues/33) | [FEATURE] Full Duplex Streaming ORM | Separate product workstream. |
| [#294](https://github.com/orneryd/NornicDB/issues/294) | [I18n] Docs and labels | Separate product workstream. |
| [#340](https://github.com/orneryd/NornicDB/issues/340) | [FEATURE] - graphQL auto-schema | Separate product workstream. |
| [#439](https://github.com/orneryd/NornicDB/issues/439) | [FEATURE] Stage-2 rerank: send title + matched chunk expanded with neighbouring chunks (the reranker currently sees one small chunk; top-1 relevance 52/80 → 74/80 on the same candidates with richer text) | Separate product workstream. |
| [#443](https://github.com/orneryd/NornicDB/issues/443) | [FEATURE] Label include/exclude filter for managed embeddings (today every node of any label with a matching property is sent to the provider) | Separate product workstream. |
| [#446](https://github.com/orneryd/NornicDB/issues/446) | [BUG] Compressed ANN rescoring floor from 5becc8c5 is still clamped by the request-derived maxLimit: recall and hybrid agreement unchanged on the server (follow-up to #440) | Separate search correctness/performance change; retain existing retrieval regressions. |
| [#472](https://github.com/orneryd/NornicDB/issues/472) | [DOCS] backup-restore.md documents `{"output": …}` for POST /admin/backup but the handler reads `path`; the request fails only after the full export | Separate backup/restore change; #473 is related to wrapper capabilities, but requires independent backup/restore acceptance. |
| [#473](https://github.com/orneryd/NornicDB/issues/473) | [BUG] POST /admin/backup on a Badger server falls back to the in-memory JSON export: no embeddings, no schema, whole database loaded into RAM, and no server-side restore | Separate backup/restore change; #473 is related to wrapper capabilities, but requires independent backup/restore acceptance. |
| [#489](https://github.com/orneryd/NornicDB/issues/489) | [BUG] TestHNSWSearchUsesLexicalEntryPointsToReachDisconnectedRegions fails on some CI runners: it asserts on a candidate whose score sits exactly on the minSimilarity = -1 threshold | Separate search correctness/performance change; retain existing retrieval regressions. |

## Evidence fields to fill during implementation

Maintain a machine-readable ledger in `testing/cypher/tck/testdata/issues.json`
as part of Step 1. It must carry: issue number/URL; source snapshot; reproduction
IDs; exact TCK scenario IDs or explicit no-upstream-match reason; failure
signature; affected route/mode/storage combinations; change ID; failing baseline
commit; fixing commit/PR; passing artifact links; write/readback and recovery
proof; benchmark evidence where applicable; current status and reviewer.

Statuses: `unreproduced`, `reproduced`, `fix-in-progress`, `verified`,
`closure-ready`, `closed`. A routing change, improved failure message, or a
new expected-failure entry alone cannot advance a bug to `verified`.
Use typed unsupported errors as an interim safety improvement; valid queries
in the scoped issues still require their specified results to close.

Refresh the inventory with:

```bash
gh issue list --repo orneryd/NornicDB --state open --limit 200 \
  --json number,title,author,labels,body,comments,url
```

Record any new issue or TCK counterexample and its disposition at each milestone.
Do not overwrite verified evidence when refreshing GitHub titles or state.
