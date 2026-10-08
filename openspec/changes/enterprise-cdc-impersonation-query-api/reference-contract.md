# Reference contract and evidence gates

## Authority and research status

Target: `neo4j:5.26.30-enterprise`, Cypher 5, negotiated Bolt versions supported
by that server, and its Query API v2. Issue comments settle the Enterprise
evaluation image; they are design input, not a substitute for observations.
Research date: 2026-10-08 UTC. Repository baseline:
`7a2ae51ac6ab929ad6dfef8288602a889c4b88b2`.

The issue bodies/comments and sources below were read. **An Enterprise instance
was not run during drafting.** Exact error text, ambiguous edge cases, and
version-sensitive result shapes below are mandatory phase-0 oracle work, not
claimed verified facts. Capture the image digest, server version, driver
version, protocol version, auth fixture, locale, configuration, commands, and
raw responses before implementing these cases.

| Source | Contract used and important correction |
| --- | --- |
| [#936](https://github.com/orneryd/NornicDB/issues/936) | IMPERSONATE, Bolt fields, dual attribution, dependency on #935 |
| [#937 and design comments](https://github.com/orneryd/NornicDB/issues/937) | CDC scope; dedicated storage family rather than WAL; comments describe an older code revision |
| [#935](https://github.com/orneryd/NornicDB/issues/935) | Full effective graph and procedure privileges, DENY precedence, cache isolation |
| [5.26 IMPERSONATE privileges](https://neo4j.com/docs/operations-manual/5/authentication-authorization/dbms-administration/dbms-impersonate-privileges/) | Named users, wildcard/omitted target, multiple roles, IMMUTABLE syntax, and prohibition on updating administration commands while impersonating, even an admin |
| [Bolt messages](https://neo4j.com/docs/bolt/current/bolt/message/) | `imp_user` is introduced in 4.4; null means no impersonation. ROUTE 4.3 has a different third-field shape |
| [CDC configuration/security](https://neo4j.com/docs/cdc/current/get-started/self-managed/) | REMOVE OPTION disables; OFF invalidates old cursors permanently; query needs EXECUTE plus EXECUTE BOOSTED plus ACCESS, current/earliest do not need BOOSTED |
| [CDC procedures](https://neo4j.com/docs/cdc/current/procedures/) | Default `from=""` means current; selectors default to []; earliest is a boundary usable to include the oldest available event; event-ID resume must not skip the rest of its transaction |
| [CDC schema and order](https://neo4j.com/docs/cdc/current/procedures/output-schema/) | Node create, relationship create, node update, relationship update, node delete, relationship delete; not statement order |
| [CDC selectors](https://neo4j.com/docs/cdc/current/procedures/selectors/) | OR between selectors; AND within one; all `changesTo` properties; endpoint selectors cannot contain operation/changesTo |
| [CDC key properties](https://neo4j.com/docs/cdc/current/procedures/elementids-key-properties/) | Key constraints OR equivalent uniqueness plus existence constraints; capture keys at commit, never retrofit history |
| [5.26 transaction logs](https://neo4j.com/docs/operations-manual/5/database-internals/transaction-logs/) | Default since 5.13 is **`2 days 2G`**, not `2 days`; policies include size/count/time/combined and keep_all/keep_none; retain newest nonempty segment and prune at safe checkpoints |
| [CDC restore](https://neo4j.com/docs/cdc/current/backup-restore/) | Restore makes old cursors unusable; downstream resnapshot/reconciliation required |
| [Query API introduction](https://neo4j.com/docs/query-api/current/) | v2 enabled by default since 5.25; separate from old transaction HTTP API |
| [Query API query](https://neo4j.com/docs/query-api/current/query/) and [transactions](https://neo4j.com/docs/query-api/current/transactions/) | Autocommit, explicit lifecycle, fields/values, transaction ID/expiry, errors and bookmarks |
| [Query API impersonation](https://neo4j.com/docs/query-api/current/impersonation/) | `impersonatedUser` selects complete target context |
| [Query API authentication](https://neo4j.com/docs/query-api/current/authentication-authorization/) | Per-request authorization, Basic/Bearer, authentication error envelopes |
| [Plain JSON](https://neo4j.com/docs/query-api/current/plain-json/) and [typed JSON](https://neo4j.com/docs/query-api/current/typed-json/) | Independent input/output negotiation; typed media type `application/vnd.neo4j.query.v1.2`, `$type`/`_value` |
| [Query API bookmarks](https://neo4j.com/docs/query-api/current/bookmarks/) and [routing](https://neo4j.com/docs/query-api/current/routing/) | Commit-state tokens and `accessMode` Read/Write, not a timer-derived success token |

Current documentation is not a version lock. In particular:

- `db.cdc.current`'s `txCommitTime` column is documented as a 2026.06/Cypher 25
  addition: do not add it to the 5.26 one-column `id` response.
- Query API `txMetadata` and `maxExecutionTime` are documented as 2026.04
  additions, response `queryType` and result timings as 2026.07 additions, and
  `notificationsFilter` as a 2026.08 addition. Do not assume they exist in 5.26.
  Use Bolt `tx_metadata` and `tx.setMetaData` for the pinned attribution tests.
- The current CDC examples disagree on `databaseId` versus `databaseName`.
  Freeze the exact 5.26 metadata key set and types, rather than returning both
  speculatively. The issue-requested `databaseId` is the working contract.
- `SHOW TRANSACTIONS` does not necessarily have two separately named user
  columns. Inspect the actual 5.26 `username`/other projections during
  impersonation; do not invent `authenticatedUser` and `executingUser` columns.
- The Query API transactions page has a rollback URL typo and a surprising
  authentication-failure lifecycle statement. Verify the actual
  `/db/{database}/query/v2/tx/{id}` DELETE endpoint and lifecycle.

## Phase-0 fixture matrix

These are planned test IDs. Record the initial NornicDB failures and then
commit fixtures/test cases; a documentation entry is not a passing test.

| IDs | Exact observation and implementation gate |
| --- | --- |
| `IMP-01` | GRANT/DENY/REVOKE variants, omitted/wildcard/named targets, multi-role expansion, escaped names/parameters, IMMUTABLE; exact SHOW rows, commands, columns and update counters |
| `IMP-02` | Authorized/unauthorized target, nonexistent/suspended target, expired credentials, impersonating self, empty/wrong-typed/null target, auth-disabled mode; exact code/message and error precedence |
| `IMP-03` | BEGIN/RUN/ROUTE on Bolt 4.4 and supported 5.x; pre-4.4 fields; RUN extras inside explicit tx; failed-state/RESET transitions; ROUTE database response and target home resolution |
| `IMP-04` | Target-only roles, DENY over GRANT, updating-admin prohibition, SHOW CURRENT USER, SHOW TRANSACTIONS and query log attribution |
| `IMP-05` | Same pooled connection alternates Alice/Bob/no target; revoked privilege, changed roles, disabled user, user rename/drop; existing transaction versus next request timing |
| `IMP-06` | #935 effective graph and caches via patterns, paths, aggregates, subqueries and procedures; local aliases/USE/composites and remote delegation |
| `ATTR-01` | Bolt BEGIN/autocommit tx_metadata, tx.setMetaData replacement/merge and size/type limits, metadata visibility while idle, immutable user attribution |
| `ATTR-02` | Existing HTTP and Query API autocommit/explicit metadata and connection fields; batch subtransactions, retries, rollback, disconnect, background writes |
| `CDC-01` | CREATE/ALTER/REMOVE options, parameters/case/type/errors, IF NOT EXISTS/IF EXISTS/OR REPLACE/WAIT where valid, SHOW projections, system/composite/alias rules, admin transaction mode |
| `CDC-02` | Procedure signatures/defaults, nullable versus omitted args, empty database/enabled-with-no-history/OFF states, SHOW PROCEDURES metadata, YIELD/WHERE/RETURN/LIMIT |
| `CDC-03` | Exact FULL/DIFF c/u/d shapes and native value types; property and label additions/removals, keys and endpoint snapshots |
| `CDC-04` | Net-zero/update-revert/create-delete/delete-recreate, repeated writes, detach/self-loop/shared-edge deletion, ordering within event categories and seq allocation |
| `CDC-05` | Current boundary versus event cursor; resume halfway through a transaction; earliest inclusion; invalid/foreign/future/expired/old-epoch IDs; concurrent commits and failures |
| `CDC-06` | Every selector field, before/after label/key matching, nested endpoints, unknown fields, wrong types, nulls, duplicate selectors, empty arrays/maps and missing select |
| `CDC-07` | ACCESS/EXECUTE/BOOSTED independently granted/denied, procedure globs, target-only rights during impersonation, full unfiltered event visibility once authorized |
| `CDC-08` | Retention grammar/default/settings introspection, rotation/checkpoint, whole-transaction expiry, active reader overlap, restart, OFF/re-enable, drop/recreate and restore |
| `CDC-09` | Crash/fault at every managed commit batch, direct writes, all wrappers, one tx per statement/batch and no capture side effect on query/rollback |
| `QAPI-01` | All five endpoints/methods, content negotiation, valid/malformed/empty bodies, field defaults/types, unknown fields, accessMode, includeCounters, EXPLAIN/PROFILE, notifications |
| `QAPI-02` | Plain and typed primitives, large integers, special floats, bytes, nested values, temporal, spatial, node/relationship/path; exact null/absence and input rejection |
| `QAPI-03` | Basic/Bearer/no-auth/error envelopes and status/headers; begin/continue/keepalive/commit/rollback, timeout, concurrent requests, failure cleanup and wrong database/owner |
| `QAPI-04` | Impersonation pinned at open; omitted/repeated/changed target on continuation; wrong authenticated owner, privilege revocation and suspended target |
| `QAPI-05` | Commit/bookmark exchange with Bolt and old HTTP, restart, future-token wait/timeout/cancellation, failed write, CDC procedures under both encodings |

## Differential comparison rules

Extend the existing harness, but **do not use its default unordered-row
comparison for CDC**. Preserve event order even without ORDER BY. Check every
column, field, type, null/omission, status/header, error code, message template,
transaction lifecycle and committed effect. Do not compare just row counts.

Opaque IDs, local addresses, server/database UUIDs, timestamps, expiry and timing
values differ across systems. Normalize with stable per-run bijections and
validate consistency, scope, ordering, native type and validity separately;
never strip them wholesale. Carry each server's own cursor/bookmark through
resumption. Do not require cross-product CDC tokens to be interchangeable.
Do not normalize operation order, `seq`, labels/properties, usernames, metadata,
codes, or response field names. Match within-category ordering as actually
guaranteed by the pinned oracle, not accidental internal IDs.

Run the oracle locally under explicit evaluation-license acceptance with
synthetic data only. CI must be separately authorized for that license; no
Community fallback, silent skip, or documentation-only substitute can satisfy
the Enterprise acceptance gate.
