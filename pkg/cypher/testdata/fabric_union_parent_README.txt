These fixtures replay the reviewed UNION and scope cases through the public
executor on both parsers. Their original observations came from #1484/#1483
at 66bb5780d / e5274c180. Original-base observations and parent differences
remain in the implementation handoff; the JSON is an assertion, not a dump
of whatever a parent happens to return.

The composition parents a01a49911 / 071769253 supply EXPLAIN columns from
shared native admission facts. Successful empty unit-CALL EXPLAIN cases
therefore expect [seed], and WITH/procedure YIELD/RETURN * EXPLAIN cases
expect [label,value]. The original-correct [seed] header regression is
rejected by these fixtures. Unchanged grammar/control failures retain their
recorded semantic identity; they are not claimed fixed by this child lane.

TestUnionNarrowRequired separately asserts the corrected original row loss,
column-name alignment, deduplication, and typed mismatch contract through
local composite and real HTTP participant execution.

Current release reconciliation (f91dac08cf13d5eabcda2a3ea53d9a60ff0df419):
The original JSON files above are retained unchanged. The 84 historical r4
unit/export and metadata controls additionally compare against the exact release
observations in fabric_union_release_f91_{default,antlr}.json. Every name, query
and family is checked; no input was removed or skipped.

The release now rejects a nested non-unit CALL that concludes a body, fixing
an old accepted-invalid extension. Existing Cypher 25 conditional/braced/NEXT
scope errors and unresolved wildcard metadata are recorded as inherited controls,
not certified correct or claimed fixed by this UNION lane. This follows the
maintainer's instruction to defer existing release failures. No production
framing/conditional scope code was changed; PR 1612 is being worked separately.

The original public lost-row cases already pass on this release. The remaining
new fail-first proof is missing participant metadata: the release returns reversed
columns and misses mismatched names despite the branch's declared schema; the
ported merge owner restores and validates those declarations. The original
iterator and DISTINCT semantics are retained.
