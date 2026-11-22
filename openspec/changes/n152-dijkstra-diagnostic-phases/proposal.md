# Dijkstra diagnostic phase regression test

Test-only follow-up to merged #1926 (a301c8ff618b998d9869c977814292f86e13f65c).
Base and fetched origin/release/v1.5.2 both equal
a5b7980e89171764091a6d47d0b8a8ee3ca19c88 before edits. The original test blob
is d62b697616242619c350a6af1699f8b525025b68 on both. Main checkout and other
agents' worktrees remain untouched. No production or unrelated test changes.

The supplied unmodified-release witness FAIL1.030s is preserved with exact
original test source in original-evidence.json. Successful fallback weights pass;
the public String error fails the old defaultWeight substring assertion before
the independent handler assertion is reached. That test originated in4205ea5ae,
before request parameter facts reached static procedure checks.

Canonical ownership: validateBoundParameterExpressions now invokes the existing
semantic walk with request-local params. Its CALL branch passes those facts to
staticProcedureArgumentTypeError. The registry declares defaultWeight FLOAT;
proved String arguments reject via typeNameMismatchError as SyntaxError before
runtime. Existing TestProcedureArgumentStaticTypes asserts this same signature
contract for literals/WITH bindings. The evaluated Dijkstra handler independently
uses procedureArgumentTypeError to report named defaultWeight TypeError.

Parent approved preserving both contracts in separate subtests on both parsers:
public literal/bound/WITH String inputs assert static SyntaxError and the Float
versus String mismatch; evaluated handler input asserts named TypeError and its
detail. All five original successful weights remain unchanged. A valid bound
Float call after rejected String confirms per-request/cache rebinding still works.
No exception is added to production diagnostics or parameter admission.

Pinned official source review (not an exact Neo4j/APOC runtime replay):
- Neo4j5.26.30 [ResolvedCall.argumentCheck](https://github.com/neo4j/neo4j/blob/5.26.30/community/cypher/front-end/frontend/src/main/scala/org/neo4j/cypher/internal/frontend/phases/ResolvedCall.scala#L154)
  checks argument expressions and expects each input signature's type before
  execution. This supports the static signature boundary.
- [ParameterTypeInfo](https://github.com/neo4j/neo4j/blob/5.26.30/community/cypher/front-end/util/src/main/scala/org/neo4j/cypher/internal/util/symbols/ParameterTypeInfo.scala#L27)
  preserves String parameter typing; Float uses Any. This supports the relevant
  request type distinction, not a fresh observed APOC error message.

Plane searches for Dijkstra and defaultWeight returned no match; N152-146 is
related NOR-79 context, not reopened or edited. No new external item requested.
DeepSeek read-only contract review job e82aec790818459fb4cc6ea4419fd861 completed read-only in135.859s;
initial invalid-base submissions created no jobs. Graph retrieval incomplete.

Scoped normal/race/vet and localization verification passed; see verification.md.
Full release qualification and exact Neo4j/APOC execution
are not claimed. Test-only correction requires no performance claim or production
benchmark. The parent owns final publication after scoped evidence review.

Worker review corroborated distinct static/coercion/handler owners and the masked handler assertion. Its restricted files omit request-scope propagation; parent independently traced that owner. Suggested either/or assertions, unrelated suite changes and a bound-Float static rejection example were not accepted: the approved contract pins one static outcome and #1926 deliberately leaves bound Float unknown. No worker code was integrated.
