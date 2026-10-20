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
