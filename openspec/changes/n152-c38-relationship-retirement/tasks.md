# C38: unanchored relationship retirement

- [x] Reproduce no-match, sparse, dense, all-match and anchored controls on the integrated release head using the report's generated stand-in; production input is unavailable.
- [x] Trace the canonical typed relationship scan and the Cypher planner's unanchored relationship pattern path, including transaction snapshot semantics.
- [x] Implement a narrow type-scan endpoint prefilter for stable mandatory relationship equalities; retain the complete WHERE, updates, row multiplicity and errors, and bypass the prefilter for historical views.
- [ ] Verify focused matrix, package checks and the exact PR base; open a release-targeted fix PR with explicit limitations.

## Scope and limits

The original production query, parameters, and dataset were not retained. The
20,000-node/100,000-relationship test is a generated stand-in. This change
narrows endpoint candidates only for stable, mandatory relationship-property
equalities on a one-hop typed match. Range predicates and all-match updates
still use the existing traversal and may take seconds at this size. Historical
viewports retain the existing path because the current type index may not
contain historical property values. Both parsers share this execution path;
there is no ANTLR-specific optimization.

## Duplicate review

- The existing C68 `relationshipPropertySeedEdge` on release head `0e524433`
  parsed exactly the mandatory stable WHERE equalities needed here, but used
  them only with a declared relationship equality index. The parser loop is
  now `relationshipWhereEqualityValues`, shared by that index path and this
  type-index endpoint filter. C68's unique/ordinary/WAL tests still exercise
  the same public path and predicate boundaries.
- `planPipelineTraversalScan` can stream a type cursor for read-only tails,
  but a `SET` remains on the materialized traversal path. Reusing the cursor
  for mutation would need update-effect and transaction planning; child
  N15-222 retains that broader design.
- The local duplicate checker is intentionally left to CI under the user's
  current PR gate. Graphify retrieval was unavailable; this is a source-level
  comparison, not a clean graph/checker result.
