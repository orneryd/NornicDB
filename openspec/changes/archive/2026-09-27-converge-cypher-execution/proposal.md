## Why

Overlapping Cypher execution and expression paths produce silent wrong results,
missed mutations and inconsistent transaction visibility. Establish independent
conformance tests first, then converge shared behavior while retaining measured
streaming fast paths.

## What Changes

- Add the pinned official openCypher TCK over Bolt in both transaction modes,
  an exact known-failure baseline, issue regressions and differential testing.
- Share preparation, lexical context, bound scopes and expression semantics.
- Keep fast-path-first execution and surface uncovered shapes as explicit
  unsupported or syntax errors.
- **BREAKING**: reject previously silent expression-text fallthrough and ignored
  clause tails; malformed or unsupported queries no longer report success.
- Unify mutation/visibility contracts across live and transactional storage
  views; preserve snapshot isolation and atomic rollback.
- Stream snapshot reads, retain verified optimizations, and remove redundant
  semantic implementations after conformance and benchmark evidence.
- Introduce informational rejection reports to guide subsequent optimizations.

## Capabilities

### New Capabilities

- `cypher-conformance`: reproducible two-mode conformance and regression evidence.
- `cypher-execution`: complete statement handling and bound composition.
- `cypher-values`: consistent typed expression, projection and aggregation behavior.
- `cypher-transactions`: atomic mutation, snapshot and wrapper visibility contracts.
- `cypher-performance`: measured streaming behavior and informational rejection reporting.

### Modified Capabilities

None. These are the first OpenSpec contracts for existing Cypher behavior.

## Impact

Implementation sequence and evidence gates:
[convergence plan](../../../docs/plans/cypher-convergence-plan.md).
Issue mapping: [35 scoped issues](../../../docs/plans/cypher-convergence-issues.md).

Affected areas: `pkg/cypher`, `pkg/storage`, Bolt/integration tests, test
tooling, CI and compatibility docs.
New test dependencies include a pinned Gherkin runner and TCK corpus; use the
existing Neo4j Go driver.

Decay/promotion/knowledge-policy behavior, search/GPU consolidation, backup
implementation and unrelated product issues remain separate work.
