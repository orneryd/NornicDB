# NOR-300 candidate verification

This PR qualifies its statement-effect, permission, cache and SHOW-header fixes.
Release-wide failures are owned by PR #1742. They are not evidence that these
focused fixes pass or fail. No throughput or allocation improvement is claimed.

## Correctness

- [x] Preserve the original focused regression populations in the PR.
- [x] Integrate current release scope/import and storage fixes by rebasing.
- [x] Final-head focused Cypher controls pass (3.210 s; final schema/cache/admission replay 3.099 s), full Fabric package passes (2.327 s), storage routing controls pass (0.438 s), and lexical inventory passes (4.311 s).
- [ ] Final-head CI, with release failures distinguished from candidate failures.

## Duplicate review

- [x] Replace the repeated schema-mode fixture constructor with the existing
  `newCypherTestExecutor` owner, retaining its namespace and assertions.
- [x] `ASTBuilder.Build` calls `build`; the latter is the single structural
  constructor. The historical Build body is not a second current implementation.
- [x] `planQuery` owns UNION/branch decomposition and calls `planSingleQuery`,
  which owns USE/CALL fragment preparation. These are separate stages.
- [x] Correlated-call filtering versus empty-outer-header tests assert different
  outcomes; top-level USE versus CALL/USE tests assert different routing scopes.
  Existing gateway and executor helpers own their shared setup.
- [x] The two TTL tests cover separate cache types; cancellation of a warm cached
  analysis and cache consistency are separate contracts.
- [x] Admission-boundary rejection and poisoned-decision rejection exercise
  different inputs and state; neither assertion population was removed.
- [x] Native query analysis consumes canonical live procedure effects; the ANTLR
  adapter retains parser-tree metadata. Storage AnalyzeQuery consumes an already
  resolved writes flag for routing and does not reclassify effects.
- [x] The advisory's AST-resolved owner-call and extracted-owner entries retain
  calls to the named canonical owners rather than copied implementations.

The advisory displayed ten reuse candidates and abbreviated two more. These
are dispositions of the displayed candidates and named owner-call evidence,
not a claim that an abbreviated report exposes every finding. Final advisory
output remains visible on the PR for review.
