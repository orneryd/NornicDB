# Preserve timestamp static admission (N152-148 / NOR-81)

## Problem and contract

`UNWIND [] AS s RETURN any(x IN timestamp() WHERE true) AS a` is accepted by
Neo4j 5.26.30 but rejected by NornicDB before execution as an Integer in a list
position. The recovered original population is 36 cells: 12 required acceptances
and 24 rejection controls across both parsers, transaction modes and
RUN/EXPLAIN/PROFILE. This change covers static admission on empty input;
nonempty scalar iteration semantics and an unrecovered historical family are
not claimed.

## Established cause

The runtime type and the reference compiler's proof are different contracts.
Neo4j's exact-version `timestampRewriter.scala` rewrites unqualified timestamp()
to `datetime().epochMillis` before semantic analysis. That property carries no
concrete Integer proof, although timestamp's runtime signature remains Integer.
NornicDB incorrectly listed timestamp in `staticFunctionResultTypes` as a proven
Integer. Both rejecting list-admission consumers already reach that same metadata
owner; neither needs a new exemption or parser.

Fresh reference checks distinguish this from permissive quantifier validation:
scalar literals, bound scalar parameters, other numeric functions, temporal
values and graph variables reject. Timestamp, its arithmetic result and the
exact lowered property accept; timestamp also accepts in comprehensions, reduce,
size and toUpper on empty input. The complete 92-probe record and pinned upstream
source are in current-reference.md and the retained fixture.

## Correction

Remove the incorrect timestamp entry from the existing compile-time proof map
and document the reference rewrite. Preserve runtime evaluation, the function
catalog, arity, volatility, list-source rules and source-owner pins. There is no
new type checker, expression parser or consumer-specific condition.

The initial worker proposal to exempt quantifier sources was rejected by fresh
reference evidence. Its source inventory was useful; its architecture was not
accepted. The superseded proposal is retained with local task artifacts, while
this file describes the implemented correction.

## Acceptance and verification

- Original 36 cells: fail-first 12 false rejections, then all pass.
- Fresh 92 reference probes in both parser lanes: all pass (184 cells).
- Existing timestamp runtime, arity, static predicate/result and owner guards pass.
- Parent focused race over those public/owner controls passes (9.817s).
- Full repository/platform verification and indexed duplicate discovery remain
  pending. The code graph could not resolve; no clean indexed verdict is claimed.

Source provenance, exact commands, results and cleanup are recorded in
current-reference.md. Historical investigation evidence is retained separately
in evidence.md and the original fixture text. New checks do not substitute for
an unavailable original whole-family population.
