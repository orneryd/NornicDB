# N152-123 / NOR-319: map projection in pattern-property predicates

Original RED base f77ec7706de099588ac232b32b6908860511d3da.
Current integrated base f0f5a2f4fd2bf8cb8a9344af9923cb0f4efb5210 (N147/N145 merged). Preserve the canonical
map-projection and pattern-property owners; no stored-map feature or general
projection-value expansion. N146 is a separate parent-owned candidate.

The ticket supplies two correlated MATCH forms across RUN/PROFILE/EXPLAIN,
both parsers and auto/explicit transactions. Each case seeds scalar properties
and two edges, verifies scalar correlation returns exactly b, then expects the
projected map comparison to produce zero rows. Undefined receiver controls reject.
The original test and source blocks are preserved in fixtures. Historical macOS
1.248s/Linux0.434s RED and Neo4j5.26.30 output are historical, not current checks.

Fresh exact original24 RED on f77: FAIL1.182s, same invalid receiver b.k = a;
scalar controls and four undefined-receiver controls pass. Log
/tmp/n152-123-original-red.log. No production edits preceded this run.

Cause: labelExpressionRewriter.samePatternMapReads correctly rewrites correlated
property values as variable.key = value. staticOperatorChecker.derive checks the
whole b.k = a{.k} comparison as a projection before operator decomposition.
mapProjectionSplit correctly rejects the non-variable prefix; its shared runtime
boundary must remain unchanged.

Parent-approved correction moves the sole invalid-receiver guard into checkAtom,
after operator decomposition. The existing rejected-expression gate delegates
projection-shaped expressions there as well, preserving invalid receivers that
cannot be decomposed. The guard passes the current scope to mapProjectionSplit,
preserving bound keyword names. No new scanner or runtime projection owner.
checkAtom only descends into proper child expressions; it never calls derive on
its unchanged input. Pattern/subquery exclusions remain in the existing scanner.

Rejected alternatives: blanket masked-text inequality can hide an invalid outer
receiver containing a valid inner projection; suffix masking also mishandles
whitespace-qualified property receivers. Existing canonical operand decomposition
provides the correct ownership boundary without adding a second parser.

DeepSeek read-only job 4ade309c3e844fa5ab275cda2cb72eaa completed (166.1s).
It corroborated the initial guard failure; its blanket masking suggestion was
independently rejected for the above counterexample. Automatic graph retrieval
was incomplete (DNS failures; no code baseline). No worker code was accepted.
Static inspection establishes reuse of mapProjectionSplit, existing operator
splitting and staticMapProjectionReceiver; indexed duplicate analysis remains
incomplete until the staged checker is run and evaluated.

Fresh exact original24 on integrated f0f5: FAIL 1.192s before production edits,
same receiver error; /tmp/n152-123-integrated-red.log. Original f77 log retained.

Fresh Neo4j 5.26.30 reference: 14 exact probes, queries and metadata independently
compared with their requested input. Both original queries accept under RUN,
PROFILE and EXPLAIN; RUN returns zero rows and PROFILE reports Rows 0. Scalar
correlation returns b. Undefined and scalar receivers reject. Both comparison
sides and nested NOT return true; malformed outer selector/keyed receivers reject.
Raw records: fixtures/fresh-neo4j-5.26.30.json. The mesh task used one slot and
owned bounded container/volume; log ended CLEANUP_COMPLETE and newly pulled image
was removed. This is fresh reference evidence, separate from ticket historical
output. Whitespace and keyword controls are canonical regression boundaries,
not claimed as fresh reference probes.
