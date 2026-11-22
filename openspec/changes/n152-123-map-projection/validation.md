# Validation

Current base: f0f5a2f4fd2bf8cb8a9344af9923cb0f4efb5210. Worktree:
/tmp/n152-123-map-projection, branch fix/n152-123-map-projection.

All Go checks use GOMAXPROCS=2 GOMEMLIMIT=1500MiB, -p=1,
-tags=noui,nolocalllm, and -count=1 for tests. No other worktree was modified.

- Exact original24 RED on f77: FAIL 1.182s, original scalar correlation and
  undefined receiver controls retained. /tmp/n152-123-original-red.log.
- Exact original24 integrated RED on f0f5: FAIL 1.192s, same invalid receiver
  b.k = a. /tmp/n152-123-integrated-red.log.
- Focused `-run '^TestNOR319'`: PASS 1.232s. This includes the original matrix,
  direct operand Boolean/type/malformed receiver assertions and public both-side,
  nested, undefined/scalar receiver and whitespace-qualified receiver boundaries.
  /tmp/n152-123-focused-green.log.
- Broad owner normal: PASS 36.065s; /tmp/n152-123-broad-normal.log.
  Selector (also used for race):
  `^Test(NOR319|Static|CheckOperator|CompileTimeTypeErrors|DeleteCount|MapProjection|PatternProperty|MatchPatternProperty|ComparisonOperatorsMatchNeo4j|TemporalFieldsAndUnaryPlusMatchNeo4j|ValueSemanticsParsingForms|GH466_MapProjection|LiteralMapsRemainDistinct|QuotedExpressionMapProjection|PropertyAccessOnMapProjection|Cypher25TemporalMapProjection)`.
  Includes merged N147 DELETE/COUNT admission and subquery exclusions.
- Same-selector race: PASS 287.603s; /tmp/n152-123-broad-race.log.
- Package vet: PASS exit0; /tmp/n152-123-vet.log.
- `GOMAXPROCS=2 GOMEMLIMIT=1500MiB GOFLAGS='-p=1 -tags=noui,nolocalllm' make verify BASE=origin/release/v1.5.2 FETCH=0 ONLY=localization`: PASS;
  /tmp/n152-123-localization.log. All other verifier steps explicitly unselected;
  this is scoped verification, not a full verify pass.
- `make verify-list BASE=origin/release/v1.5.2 FETCH=0`: PASS. Selected localization,
  UI, llama libraries, build, all test groups, allocation ratchet, conformance and
  differential; docs path filter skips. /tmp/n152-123-verify-list.log.
- Unchanged staged duplicate checker `scripts/code-intelligence-hook.sh check-staged`:
  FAIL exit1, AnalysisError analysis/ingestion failed at localhost:7474 database
  nornicdbcode. No candidate verdict was returned. /tmp/n152-123-duplicate.log.
  Its failure is not a clean duplicate result or a waiver. Worker graph retrieval
  was separately incomplete because of DNS failures.

Canonical source review: reuse the existing operand/operator decomposition and
mapProjectionSplit boundary. samePatternMapReads remains the correlation producer;
row map projection remains the runtime consumer. No new helper/scanner, storage
feature, per-query exception or alternative runtime path was added.

Fresh reference records are checked into fixtures/fresh-neo4j-5.26.30.json.
Fourteen exact query probes are fresh; historical ticket logs remain separately
labelled in fixtures. Mesh execution and cleanup: /tmp/n152-123-reference.log.

Full release verification, full-suite coverage, allocation/performance qualification,
conformance and differential remain unexecuted for this candidate. This correction
makes no performance-improvement or merge-ready claim.

Original ticket source blocks are losslessly JSON-encoded in fixtures/original-source-blocks.json; the original test also remains a separate source fixture. Staged diff whitespace check passes.

Parent reviewed and approved the narrow source/test correction and authorized draft publication after the above checks. Open PR worktree/head branch and shared Go cache remain preserved for active review and concurrent work. Task temporary evidence will be archived before redundant task-only temporary files are removed.

Draft PR: https://github.com/orneryd/NornicDB/pull/1931, release/v1.5.2, milestone v1.5.2. Raw temporary files cited above now reside under /Users/timothysweet/.codex/task-evidence/n152-123 with a SHA-256 manifest; identical copies were verified before removing task-only temporary files. Verifier logs were also archived. Open worktree and branch retained; shared /Users/timothysweet/Library/Caches/go-build retained because other release tasks are active. No task subagent worktree exists; remote reference resources were already cleaned.
