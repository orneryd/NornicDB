This is the release/v1.4.4 copy of #1485: every place in the Cypher package that reads the inside of a relationship pattern now goes through one reader. Its priority fixes:

- **No more Go panic** on a quoted string inside a pattern (`MATCH ()-[r:$('D):Z|Q{*..')]->()` panicked with slice bounds out of range).
- **No more silently wrong rows**: a nested map read in a relationship or node property map (`{k: {outer:{xs:[11,17]}}.outer.xs[i]}`) was dropped, so MATCH and OPTIONAL MATCH also returned the k = 999 rows. CREATE wrote null and MERGE failed.
- **Dynamic types on `*` ranges** follow Neo4j 5.26.30. The type does not filter at all, which looks like a Neo4j bug. The comment in `relationshipElement` and #1485's description explain it, so the maintainer can choose otherwise in that one place.

The copy also carries the rest of #1485: backtick types holding quotes or arrows, escaped quotes in dynamic types, inline WHERE with a `*` in a dynamic type, CASE and slice dynamic types, and PROFILE.

## Base

- Stacked on #1472 (`cherry/1407-1.4.4`, the release copy of #1407, already in the 1.4.4 candidate) because it needs #1407's reader. Retarget to `release/v1.4.4` once #1472 merges.
- Label `bug`, milestone v1.4.4. Part of #1055.

## Commits

- #1473's four commits.
- #1485's two commits. One conflict, resolved by deleting the dead `evaluateRelationshipPatternInWhere` block exactly as on develop.
- **One release-only commit.** `{outer:…}['outer'].xs[i]` in a relationship map still matched both rows on release, because release lacks develop's #1214 receiver rule (a `[` after a map literal subscripts the map). That one-character rule is added here.

## Testing (TrueNAS, `sig/tnrun.sh`)

- **The 415-statement Neo4j oracle** (`TestRelationshipPatternReaderMatchesNeo4j`), auto-commit and explicit transactions:
  - red on #1472's head, both parsers, including 26 panics: i20261018132002-62416-81b1;
  - green, both parsers: i20261018131806-69468-cb61, before the edge-contract commit. The full suite below includes the oracle at the final head.
- **Full `pkg/cypher`**, both parsers: running (results will be added here).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
