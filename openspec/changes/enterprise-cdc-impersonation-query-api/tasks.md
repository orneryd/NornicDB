## Execution rules and dependency graph

All checkboxes are implementation work, not completed by creating this plan.
For each behavioral fix: add a failing test, record the failure, implement,
then record the passing command and persistent-state evidence. IDs refer to
[reference-contract.md](reference-contract.md); new test names below are planned.

| Phase | Depends on | Deliverable |
| --- | --- | --- |
| 0 | None | Enterprise observations and failing baseline |
| 1a | 0 and #935 canonical persisted privilege/evaluator subset | Database administration and ACCESS/EXECUTE/BOOSTED checks (1.1, 1.8) |
| 1b | 0, 1a and #935 effective-graph foundation | Full security/impersonation cutover (1.2-1.7) |
| 2a | 0 | Ordinary/system transaction attributes and metadata (2.1-2.4, 2.6) |
| 2b | 0, 1b, 2a | Impersonated identity, SHOW and lifecycle integration (2.5, 2.7) |
| 3 | 0, 1b, 2b | Bolt impersonation |
| 4 | 0 | Durable commit positions and bookmarks |
| 5 | 0, 1b, 2b, 4 | Query API v2 including impersonation |
| 6 | 0, 2a, 4 | CDC storage and complete write-path coverage |
| 7 | 0, 6 for 7.1-7.4; public enablement 7.5 also 1a | Database enrichment options |
| 8a | 0, 2a, 6, 7.1-7.4 | Event/cursor/selector/stream implementation (8.1-8.5, 8.7); no public registration |
| 8b | 0, 1a, 8a; impersonation acceptance also 2b | Authorization, public registration and examples (8.6, 8.8) |
| 9 | 0, 4, 6, 7.1-7.4, 8a; privileged settings/checkpoint exposure also 1a | Retention, recovery and restore |
| 10 | All phases and #935 effective-graph acceptance | Integrated acceptance and cutover |

Phases 2a, 4 and security work can proceed independently after phase 0. Phase 6
uses internal configuration fixtures before public DDL is enabled. Do not
admit DIFF/FULL before phase 6 is safe or claim impersonation acceptance with
only current database read/write booleans.

Storage, ordinary attribution, option parsing/transitions, events, selectors,
scans and retention do not wait on full RBAC or impersonation. Implement them
with internal test fixtures before public enablement. Public option DDL needs
canonical administration checks (7.5); current/earliest need ACCESS/EXECUTE,
and query additionally needs BOOSTED (8.6). These checks use 1a's canonical
subset, not a second evaluator or an admin-role fallback. Each CDC procedure
stays unregistered until its own authorization tests pass. Impersonated cases
and full release acceptance still require 1b/2b. Core 7/8a/9 tasks do not
depend on the public gates or 2b, avoiding an indirect #935 dependency.

## 0. Freeze the Enterprise contract

- [ ] 0.1 Add `scripts/cypher-tck/run-enterprise-differential.sh` (new): pin `neo4j:5.26.30-enterprise` plus resolved digest, require explicit evaluation-license opt-in, enable auth and use isolated dynamic ports/synthetic data. Verify readiness/version and explicit failure without license/reference availability; never substitute Community.
- [ ] 0.2 Add `testing/cypher/enterprise/` (new), reusing existing driver facilities for authenticated raw-Bolt and HTTP probes. Verify it records digest, version, protocol, locale, configuration and raw results; document the runner and license mechanism alongside the script.
- [ ] 0.3 Capture `IMP-01..06` and `ATTR-01..02`, including exact errors, SHOW user representation, revocation timing and metadata setter behavior. Verify fixtures have both reference outcomes and failing NornicDB results.
- [ ] 0.4 Capture `CDC-01..09` (CDC-01 option DDL, values, errors and SHOW projections recorded in [evidence/cdc-01-database-options.txt](evidence/cdc-01-database-options.txt); parameters, system/composite/alias targets and admin transaction modes remain), resolving metadata key/type ambiguity, defaults/nulls, event order/net-zero cases, cursor boundaries, retention and admin transaction modes. Verify every matrix case has raw reference evidence.
- [ ] 0.5 Capture `QAPI-01..05`, including all five endpoints, encodings, statuses, continuation ownership/target fields and version-specific fields. Verify fixtures target 5.26 rather than the evolving current docs.
- [ ] 0.6 Add strict comparison tests: preserve CDC order without ORDER BY; compare fields/types/nulls/errors/lifecycle and fresh-session graph effects. Verify deliberately reordered events, missing same-tx resume rows and invented fields fail.
- [ ] 0.7 Record existing Community/openCypher results separately and document all Enterprise baseline failures. Verify missing promised capabilities are failures, not informational unsupported cases.
- [ ] 0.8 Promote the recorded CDC-01 option responses into executable reference fixtures with exact columns/types/errors and NornicDB failing reproductions. Preserve the supplied digest/config/driver provenance; add successful parameter values, mixed-case values, database-kind/admin-transaction cases and authenticated grant/deny observations. The auth-disabled capture is partial DDL evidence, not authorization acceptance; do not repeat completed observations unnecessarily or mark CDC-01 complete from the text file alone.

## 1. Integrate canonical privileges

- [ ] 1.1 Pin #935's canonical persisted grant/deny, principal/revision, database-administration and ACCESS/EXECUTE/BOOSTED interfaces as subset 1a. Verify named/wildcard procedures, DENY/missing grants and restart; use the same canonical records/evaluator later consumed by full effective-graph work, not parallel temporary permission strings (`CDC-01/07`).
- [ ] 1.2 Implement IMPERSONATE scopes and syntax through #935's administration parser/renderer and `pkg/auth` canonical records. Add `TestEnterpriseImpersonatePrivileges` for named/wildcard/multi-role/immutable/revoke/SHOW AS COMMANDS and restart (`IMP-01`).
- [ ] 1.3 Implement immutable security context and target resolver in `pkg/auth`; test lookup/error precedence, no-auth/self/disabled/unknown target and updating-admin restriction in `TestEnterpriseImpersonationResolution` (`IMP-02/04/05`).
- [ ] 1.4 Replace coarse prechecks on affected routes, including `/db/` middleware and procedure checks. Verify caller-without-graph-READ can impersonate a permitted target, while target cannot borrow caller rights (`IMP-04`, `QAPI-04`, `CDC-07`).
- [ ] 1.5 Implement one-time migration of role/allowlist/privilege records with a dry-run mapping and actionable ambiguity errors. Verify idempotence, credential/ID preservation and failed-migration recovery before deleting dual reads/global fallbacks.
- [ ] 1.6 Migrate native access-management API/UI consumers to canonical records or explicitly retire lossy matrix mutation. Verify native updates cannot erase fine-grained grants/denies and no old evaluator remains; document breaking migration examples.
- [ ] 1.7 Wire effective policy identity/revision to #935 caches and local/remote USE/composite execution. Verify `IMP-06`, including native search/path/procedure surfaces and no service-account-only remote forwarding.
- [ ] 1.8 Verify the ordinary-user canonical subset independently of full graph filtering and impersonation, including database option administration, ACCESS/EXECUTE for current/earliest, and BOOSTED for query. Add `TestEnterpriseCDCPrivilegeFoundation` with positive/negative/glob/DENY/reopen cases; these results unlock only the corresponding public gates, not #935 or impersonation acceptance (`CDC-01/07`).

## 2. Carry attribution into actual transactions

Tasks 2.1-2.4/2.6 form 2a and can land without phase 1. Tasks 2.5/2.7
form 2b and integrate the canonical impersonation context after 1b.

- [ ] 2.1 Add ordinary-session `TestEnterpriseTransactionAttribution` reproducing metadata loss through Bolt `BeginTransaction`; record failing `ATTR-01/02` cases before changes. Use authenticated=executing for ordinary callers and an explicit system origin for internal writers; impersonated RequestIdentity cases belong to 2.7.
- [ ] 2.2 Add storage transaction-attribute values and extend Cypher request/transaction context; wire ordinary Bolt BEGIN/autocommit and implicit transaction creation. Verify trusted users/connections reach storage without storage importing auth/protocol packages. The transport-neutral dual-user value type does not depend on resolving impersonation.
- [ ] 2.3 Wire old HTTP sessions, txsession and CALL IN TRANSACTIONS child creation. Verify each child has its own commit identity and inherited attribution and rollback emits nothing (`ATTR-02`).
- [ ] 2.4 Use one deep-copying metadata setter for protocol metadata and tx.setMetaData. Verify oracle semantics/limits, final same-commit snapshot and resistance to spoofing trusted fields (`ATTR-01`).
- [ ] 2.5 Update SHOW CURRENT USER, SHOW TRANSACTIONS and structured query/audit attribution. Verify exact projection/user formatting and existing redaction; document ordinary/impersonated output (`IMP-04`, `ATTR-01/02`).
- [ ] 2.6 Add ordinary/system race/cleanup tests for metadata and identity reuse across retries, rollback, timeout, HTTP continuation, child batches and internal writers; verify snapshot/deep-copy ownership without canonical target resolution (`ATTR-02`).
- [ ] 2.7 After 1b and 2a, attach resolved authenticated/executing identities from the canonical security context to all transaction creation paths. Add the failing single-user RequestIdentity reproduction, then verify impersonated retries, child batches, continuation, rollback and pooled-session cleanup without caller/target role union (`ATTR-01/02`, `IMP-05`).

## 3. Implement Bolt impersonation

- [ ] 3.1 Add `TestEnterpriseBoltImpersonation` using raw messages and driver sessions across 4.4/supported 5.x and pre-4.4 field/layout cases. Record current failures (`IMP-02/03`).
- [ ] 3.2 Parse/validate imp_user in RUN, BEGIN and ROUTE in `pkg/bolt/session_messages.go`; resolve before database selection/authorization. Verify target home database and exact routing/error metadata (`IMP-03`).
- [ ] 3.3 Pin effective context in explicit lifecycle and autocommit result streams without mutating authResult. Verify explicit RUN extras, PULL/DISCARD and deferred completion retain correct identity (`IMP-03/05`).
- [ ] 3.4 Extend existing lifecycle tests for failed/nested BEGIN, pipelined FAILURE/IGNORED, RESET, timeout, disconnect, LOGOFF/LOGON and Alice/Bob/no-target pool reuse. Verify no leaked identity or storage transaction (`IMP-03/05`).
- [ ] 3.5 Document Bolt version support, target privilege requirements and examples; verify examples with official-driver sessions against both servers.

## 4. Build durable commit positions and bookmarks

- [ ] 4.1 Add `TestEnterpriseCommitPosition` for delayed/failed reservations, restart and multi-batch publication. Record why current process-counter/wall-clock/MVCC-reservation tokens fail (`CDC-05/09`, `QAPI-05`).
- [ ] 4.2 Implement per-database logical commit positions with `badger_commit_writer.go`/`badger_managed.go`, durable head and recovery seeding. Verify logical commits are distinct from physical batches.
- [ ] 4.3 Establish an acquisition/release ledger for write barriers, unique-key locks (#961/#964), constraint validation, count locks, CDC ordering, physical large-commit gates, close, retention and option transitions before wiring the gate. Add `TestCDCCommitLockOrder` with controlled schedules reproducing A waiting for B's key while B commits; take the CDC gate only after key locks/validation and never reacquire keys while holding it. Verify progress, abort/recovery lock release and unrelated-database independence under race tests, for small and multi-batch commits (`CDC-09`).
- [ ] 4.4 Extract shared committed-position/bookmark capability for Bolt/HTTP, with database scope, waiting, cancellation/deadline and reference errors. Verify cross-transport/restart/future-token cases (`QAPI-05`).
- [ ] 4.5 Replace wall-clock HTTP bookmarks and Bolt process-counter/placeholder fallback. Integrate receipts only where they prove durable state; verify old placeholder acceptance is gone and document client token reacquisition.
- [ ] 4.6 Verify OFF-mode positions and benchmark same-database versus independent-database commit contention before phase 6. Reuse the existing publication/durability boundary; avoid a separately synced head write merely for position tracking. Compare pre-phase-4 and position-only OFF commits for latency percentiles, throughput, allocations, RSS, head writes and fsync count; report rather than assume zero cost, with the design's 5% performance/10% memory budgets and reviewed justification for exceptions.

## 5. Implement Query API v2

- [ ] 5.1 Add `TestEnterpriseQueryAPIRequests` for endpoints/methods/bodies/media/auth and record missing-route failures (`QAPI-01/03`).
- [ ] 5.2 Add focused `pkg/server/query_api.go` routing/decoding/auth/response adapter (new), extracting reusable helpers from server_db.go rather than rewriting to old HTTP requests. Verify 5.26 fields/defaults/statuses/headers/unknown-field behavior.
- [ ] 5.3 Add plain/typed scalar and collection codecs in `query_api_values.go` (new), reusing native converters. Verify int64, special float, bytes, nested values and malformed typed parameters with `TestEnterpriseQueryAPIValues` (`QAPI-02`).
- [ ] 5.4 Complete temporal/spatial and graph/path codecs, independently negotiating input/output types. Verify all oracle value fixtures and no fmt.Sprint/old row-meta envelope fallback (`QAPI-02`).
- [ ] 5.5 Wire counters, notifications, EXPLAIN/PROFILE, bookmarks and streaming response metadata. Verify exact 5.26 omissions, late errors and cancellation versus durable-commit delivery failures (`QAPI-01/03/05`).
- [ ] 5.6 Extend txsession with protocol-scoped IDs/ownership and pinned security context; implement begin/continue/keepalive/final-statement commit/DELETE rollback in `query_api_transactions.go` (new). Verify TTL, expiry shape and cleanup (`QAPI-03`).
- [ ] 5.7 Add lifecycle concurrency tests for overlapping continue/commit/rollback, wrong database/owner, authentication failure and expiry. Verify exactly-once cleanup and fresh-session/reopen persistence (`QAPI-03`).
- [ ] 5.8 Add `TestEnterpriseQueryAPIImpersonation` with shared resolver and continuation target rules. Verify omitted/repeated/changed target, revocation and no-auth behavior (`QAPI-04`).
- [ ] 5.9 Document all five endpoints, plain/typed examples, authentication and lifecycle; run examples against both servers. Verify old HTTP remains independently supported and cross-transport bookmark cases pass (`QAPI-05`).

## 6. Add atomic CDC journal and write-path coverage

- [ ] 6.1 Add `TestCDCAtomicCommit` and `TestCDCLargeCommitRecovery`, reusing existing large-commit/failure hooks. Record missing-capture failures and assert no graph/event/head mismatch at any batch (`CDC-09`).
- [ ] 6.2 Recheck prefix allocation, reserve 0x26 and implement versioned event/control records with namespace/epoch identity and native values. Verify namespace collision/isolation and register accounting/backup/drop ownership.
- [ ] 6.3 Implement namespace CDC finalization over commitWriter and phase-4 logical positions. Verify events/head participate in publication and large-commit undo/recovery, not a post-commit append.
- [ ] 6.4 Share net-state reduction with MVCC; retain cascaded relationship bodies and endpoint states. Verify deferred edges, operation compaction, repeated writes, create-delete, update-revert and self-loop/shared-edge DETACH (`CDC-04/09`).
- [ ] 6.5 Gate both async CREATE shortcuts in `pkg/cypher/executor.go` for capture-enabled databases. Verify one transaction per statement/child batch and explicit failure before effects for incapable engines (`CDC-09`).
- [ ] 6.6 Route six direct CRUD methods and bulk methods through shared capture finalization. Verify each public mutation entry with a checked path matrix and no nested/double capture (`CDC-09`).
- [ ] 6.7 Forward capture through namespace/WAL/async/size-tracking wrappers and native API writers. Verify internal embedding/index maintenance is distinguished from captured system-origin graph changes (`CDC-09`).
- [ ] 6.8 Verify implicit Sync/transport flush durability with subprocess crash/reopen and rollback tests, not only in-process counts. Document native journal format, lock order, system-origin rules and upgrade boundary.

## 7. Add enrichment option DDL

- [ ] 7.1 Add `TestEnterpriseCDCOptions` for CREATE OPTIONS, ALTER SET/REMOVE, SHOW projections, parameters, IF/WAIT variants, counters/rows and invalid cases; record current failures (`CDC-01`).
- [ ] 7.2 Extend DatabaseInfo, multidb persistence and Cypher manager interface; implement fenced durable transition between system metadata and namespace mode/epoch. Verify crash recovery at every transition failure point.
- [ ] 7.3 Extract shared option parsing from executor_show.go; fully consume statements and replace conflicting name-row/placeholder-options responses. Verify invalid syntax makes no effects and native LIMIT/policy extensions remain intact.
- [ ] 7.4 Wire oracle-defined in-flight transaction mode semantics, OFF invalidation and DIFF/FULL continuity. Verify fresh-session/reopen state and document configuration/DDL examples (`CDC-01/08`).
- [ ] 7.5 Gate public enrichment DDL on phase 6 safety and 1a's canonical database-administration checks. Add authenticated CDC-01 allowed/denied tests with unchanged option state after denial; internal parser/transition tests need neither this gate nor full phase 1b.

## 8. Complete events, procedures and selectors

- [ ] 8.1 Add `TestEnterpriseCDCEvents` for FULL/DIFF schema, typed values, labels, keys, endpoints and net-change order. Verify exact rows/types rather than event counts (`CDC-03/04`).
- [ ] 8.2 Implement materialization from net states/committed schema/attributes, including equivalent uniqueness-plus-existence keys. Verify captured top-level label/key/endpoint rules, category ordering and stable seq.
- [ ] 8.3 Add narrow storage CDC read capability and wrapper forwarding. Implement versioned cursor kinds and validation in `TestEnterpriseCDCCursors`, including same-tx resume and earliest/current boundaries (`CDC-02/05`).
- [ ] 8.4 Implement procedure handlers and metadata in new `pkg/cypher/call_cdc.go` with exact signatures/defaults and typed args; exercise YIELD/WHERE/RETURN/LIMIT through internal test registration. Production registration is separately gated by each procedure's 8.6 authorization tests, not merely by handler completion (`CDC-02`).
- [ ] 8.5 Implement compiled selector validation/matching with `TestEnterpriseCDCSelectors`; verify every field, OR/AND, deduplication, nested endpoints, historic matching and null/type/unknown-field errors (`CDC-06`).
- [ ] 8.6 Using 1a, implement canonical ACCESS/EXECUTE for current/earliest and ACCESS/EXECUTE/BOOSTED for query with `TestEnterpriseCDCAuthorization`; verify grants/denies/globs before enabling each production registration. Ordinary-user acceptance can proceed independently; target-only impersonation and graph-restricted all-events cases additionally require 1b/2b. No coarse interim guard (`CDC-07`).
- [ ] 8.7 Implement a streaming procedure result through the registry, CALL/YIELD/WHERE/RETURN and consuming transports, with bounded iterator/producer buffering, cancellation and safe snapshot ownership. Reuse #939/#968's Bolt infrastructure only once available; #968 was OPEN at review on 2026-10-08 and does not itself stream CALL sources. Add `TestCDCProcedureStreaming` measuring events visited, rows/bytes buffered and iterator/snapshot release: no event visits before the cursor; unfiltered LIMIT 1 visits at most 1+B events for a fixed documented total prefetch budget B independent of history; a first match at position k visits at most k+B; no-match may scan the window with bounded buffering. ORDER BY/aggregation may consume the window and must retain correct results with separately reported materialization cost. Verify LIMIT, late errors, resume, concurrent commits, PULL/DISCARD/RESET, cancellation and HTTP disconnect (`CDC-02/06/08`).
- [ ] 8.8 Document procedure signatures, event schemas, selectors and ordinary/impersonated consumers; execute examples in both HTTP encodings and Bolt. Include the privileged all-events visibility warning.

## 9. Implement retention and storage lifecycle

- [ ] 9.1 Add `TestEnterpriseCDCRetention` for `2 days 2G`, all pinned policy forms, invalid forms, rotation/checkpoint and floor errors. Verify native segment byte accounting explicitly instead of comparing Neo4j physical bytes (`CDC-08`).
- [ ] 9.2 Wire config validation/startup/dynamic settings/SHOW SETTINGS and the checkpoint trigger via existing `pkg/config`, `pkg/config/dbconfig` and procedure facilities. Verify exact reference setting values/errors; document defaults and grammar.
- [ ] 9.3 Implement complete-transaction segment metadata, safe pruning and atomic floor changes, retaining newest nonempty segment. Verify time/size/count boundaries, active-reader overlap and cancellation (`CDC-08`).
- [ ] 9.4 Test backup/restore/drop/recreate/restart and namespace accounting/cleanup. Verify restored logical identity rejects old cursors and other databases remain unaffected (`CDC-08/09`).
- [ ] 9.5 Implement storage-format/downgrade rejection and interrupted cleanup diagnostics. Verify no missing-event reconstruction from WAL/MVCC and document restore/resnapshot/operator recovery procedures.

## 10. Integrated acceptance and cutover

- [ ] 10.1 Run every Enterprise fixture across supported Bolt versions, old HTTP and Query API plain/typed, implicit/explicit and ordinary/impersonated modes. Record exact commands/results; verify no skipped oracle gate remains.
- [ ] 10.2 Run focused race/coverage and persistent-state checks, then existing compatibility suites/full build. Verify >90% added-code coverage, target 95% core capture/auth, and fresh-session/reopen/rollback evidence.
- [ ] 10.3 Record workload/hardware-scoped OFF/DIFF/FULL before/after ops/sec, p50/p95/p99, allocations, RSS, bytes/event, fsync/recovery and namespace contention. Investigate OFF regressions >5% performance or >10% memory before approval.
- [ ] 10.4 Add an explicitly licensed Enterprise workflow alongside existing conformance workflows. Verify unavailable oracle/license yields blocked/failed acceptance rather than pass; keep Community TCK mandatory.
- [ ] 10.5 Execute documented migration/cutover on an existing-store fixture: backup, async drain, canonical privilege migration, version boundary and new bookmark/cursor acquisition. Verify all deletion targets are gone and non-conflicting native behavior remains.
- [ ] 10.6 Integrate phase-specific docs into API/admin guides and CHANGELOG. Close #936/#937 only with #935 effective-graph integration, all API surfaces working and differential/crash/performance evidence linked.

## Planned validation commands

These commands are for implementation, **not results of this planning change**.
New script/package/test names are deliverables above. A no-tests-matched result
is not evidence. Add exact selectors for existing regression suites as each
phase lands.

```sh
go test -tags 'noui,nolocalllm' ./pkg/auth ./pkg/bolt ./pkg/cypher ./pkg/server ./pkg/txsession ./pkg/multidb ./pkg/storage -run 'Test(Enterprise|CDC)' -count=1
go test -tags 'noui,nolocalllm' ./testing/cypher/enterprise -count=1
go test -tags 'noui,nolocalllm' -race ./pkg/auth ./pkg/bolt ./pkg/server ./pkg/txsession ./pkg/storage -run 'Test(Enterprise|CDC)' -count=1
go test -tags 'noui,nolocalllm' ./pkg/auth ./pkg/bolt ./pkg/cypher ./pkg/server ./pkg/txsession ./pkg/multidb ./pkg/storage -run 'Test(Enterprise|CDC)' -coverprofile=/tmp/nornic-enterprise-coverage.out
go tool cover -func=/tmp/nornic-enterprise-coverage.out

# New runner: explicit license opt-in is required as documented in phase 0.
bash scripts/cypher-tck/run-enterprise-differential.sh
# Existing baseline, not a replacement for Enterprise:
bash scripts/cypher-tck/run-differential.sh fixed
go build -tags 'noui,nolocalllm' ./cmd/nornicdb
```

Calculate coverage for added/changed code separately; a targeted package-wide
percentage is not a proxy. Clean up the named coverage artifact after collecting
evidence.
