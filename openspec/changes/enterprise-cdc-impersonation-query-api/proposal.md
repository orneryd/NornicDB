## Why

[#936](https://github.com/orneryd/NornicDB/issues/936) and
[#937](https://github.com/orneryd/NornicDB/issues/937) require applications to
delegate authorization and consume trustworthy committed changes without
reimplementing either in application code. Implement the observable contract of
**Neo4j Enterprise 5.26.30**, using NornicDB's authentication, transaction,
namespace, and managed Badger architecture rather than imitating Neo4j internals.

## What Changes

- Add `GRANT`, `DENY`, and `REVOKE [GRANT | DENY] IMPERSONATE`, privilege
  introspection, and Bolt 4.4+ `imp_user` on BEGIN, autocommit RUN, and ROUTE.
- Execute with the target user's complete security context, including home
  database, graph privileges, procedure permissions, and cache isolation.
  Keep the authenticated identity separately for ownership and attribution.
- Add **Query API v2**, including plain/typed JSON, autocommit and explicit
  transactions, bookmarks, and `impersonatedUser`. This scope was explicitly
  selected during planning; it is not merely an extra field on the old HTTP API.
- Persist `txLogEnrichment` (`OFF`, `DIFF`, `FULL`); implement database DDL,
  SHOW options, `db.cdc.earliest/current/query`, events, selectors, cursor
  invalidation, and retention.
- Record events and transaction attribution in the same logical atomic commit
  as graph changes, including managed multi-batch commits and cascading deletes.
- Add an authenticated, digest-pinned Enterprise differential suite with strict
  wire-shape, ordering, error, lifecycle, and persistent-side-effect checks.
- **BREAKING**: replace conflicting coarse authorization fallbacks, ignored
  impersonation, synthetic/process-local bookmarks, missing-capability direct
  execution, and non-Neo4j responses in affected APIs. No compatibility switch,
  dual authorization engine, or old-token acceptance branch remains.
- Preserve the old HTTP transaction endpoints as independent supported 5.26
  endpoints, not as Query API fallback. Preserve non-conflicting Nornic extensions.

## Capabilities

### New Capabilities

- `user-impersonation`: privilege administration and execution under a target
  identity across Bolt and Query API.
- `transaction-attribution`: trusted dual identity, metadata, commit identity,
  and consistent logging/introspection.
- `change-data-capture`: database options, durable events, procedures, selectors,
  retention, and lifecycle.
- `query-api-v2`: Neo4j 5.26 HTTP request/response and transaction contracts.

### Modified Capabilities

None in the active capability inventory (`openspec list --specs` reports none).
The archived Cypher convergence contracts remain background requirements;
this proposal explicitly supersedes any conflicting behavior in the surfaces
above, rather than changing unrelated execution semantics.

## Impact

Baseline inspected: `7a2ae51ac6ab929ad6dfef8288602a889c4b88b2`.
Existing building blocks include authenticated principals, request identities,
session-owned explicit transactions, `tx.setMetaData`, persisted database/server
UUIDs, net before/after commit states, and atomic multi-batch publication.
**None of the four new capabilities is implemented by this planning change.**

Affected packages: `pkg/auth`, `pkg/bolt`, `pkg/cypher`, `pkg/storage`,
`pkg/multidb`, `pkg/server`, `pkg/txsession`, configuration/startup wiring,
and differential tooling. Existing UI/access-management callers must migrate
with the canonical privilege store; no second read/write matrix remains.

Hard dependency: [#935](https://github.com/orneryd/NornicDB/issues/935).
An impersonation resolver backed only by the current read/write switches is
not acceptance-complete. Its version-5.26 privilege/effective-graph contract
must land before impersonation is declared complete; Cypher 25-only rules
are not silently mixed into the 5.26 oracle.

This extends [Cypher convergence Step 1](../../../docs/plans/cypher-convergence-plan.md#step-1--install-the-correctness-baseline-before-parser-changes)
with a separate Enterprise oracle and preserves its shared execution and atomic
write boundaries. See [design](design.md), [contract research and oracle gates](reference-contract.md),
and the dependency-ordered [implementation tasks](tasks.md).
