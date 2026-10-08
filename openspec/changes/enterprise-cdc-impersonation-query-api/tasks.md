## Execution rules and dependency graph

All checkboxes are implementation work, not completed by creating this plan.
For each behavioral fix: add a failing test, record the failure, implement,
then record the passing command and persistent-state evidence. IDs refer to
[reference-contract.md](reference-contract.md); new test names below are planned.

| Phase | Depends on | Deliverable |
| --- | --- | --- |
| 0 | None | Enterprise observations and failing baseline |
| 1 | 0 and #935 canonical privilege/effective-graph foundation | Canonical security and privilege cutover |
| 2 | 0, 1 | Shared identity, metadata and attribution |
| 3 | 0, 1, 2 | Bolt impersonation |
| 4 | 0 | Durable commit positions and bookmarks |
| 5 | 0, 1, 2, 4 | Query API v2 |
| 6 | 0, 4 | CDC storage and complete write-path coverage |
| 7 | 0, 6 | Database enrichment options |
| 8 | 0, 2, 6, 7; 8.6 also 1 | Events, procedures and selectors |
| 9 | 0, 4, 6, 7, 8 | Retention, recovery and restore |
| 10 | All phases and #935 effective-graph acceptance | Integrated acceptance and cutover |

Phase 4 and security work can proceed independently after phase 0. Phase 6
uses internal configuration fixtures before public DDL is enabled. Do not
admit DIFF/FULL before phase 6 is safe or claim impersonation acceptance with
only current database read/write booleans.

Only `db.cdc.query`'s authorization (8.6) needs #935's canonical privileges:
options DDL is ordinary database administration, and current/earliest need
ACCESS and EXECUTE. So phases 7 and 8 (except 8.6) do not wait on phase 1.
`db.cdc.query` stays unregistered until 8.6 passes; there is no interim coarse
guard such as an admin-role check.

## 0. Freeze the Enterprise contract

- [ ] 0.1 Add `scripts/cypher-tck/run-enterprise-differential.sh` (new): pin `neo4j:5.26.30-enterprise` plus resolved digest, require explicit evaluation-license opt-in, enable auth and use isolated dynamic ports/synthetic data. Verify readiness/version and explicit failure without license/reference availability; never substitute Community.
- [ ] 0.2 Add `testing/cypher/enterprise/` (new), reusing existing driver facilities for authenticated raw-Bolt and HTTP probes. Verify it records digest, version, protocol, locale, configuration and raw results; document the runner and license mechanism alongside the script.
- [ ] 0.3 Capture `IMP-01..06` and `ATTR-01..02`, including exact errors, SHOW user representation, revocation timing and metadata setter behavior. Verify fixtures have both reference outcomes and failing NornicDB results.
- [ ] 0.4 Capture `CDC-01..09` (CDC-01 option DDL, values, errors and SHOW projections recorded in [evidence/cdc-01-database-options.txt](evidence/cdc-01-database-options.txt); parameters, system/composite/alias targets and admin transaction modes remain), resolving metadata key/type ambiguity, defaults/nulls, event order/net-zero cases, cursor boundaries, retention and admin transaction modes. Verify every matrix case has raw reference evidence.
- [ ] 0.5 Capture `QAPI-01..05`, including all five endpoints, encodings, statuses, continuation ownership/target fields and version-specific fields. Verify fixtures target 5.26 rather than the evolving current docs.
- [ ] 0.6 Add strict comparison tests: preserve CDC order without ORDER BY; compare fields/types/nulls/errors/lifecycle and fresh-session graph effects. Verify deliberately reordered events, missing same-tx resume rows and invented fields fail.
- [ ] 0.7 Record existing Community/openCypher results separately and document all Enterprise baseline failures. Verify missing promised capabilities are failures, not informational unsupported cases.

## 1. Integrate canonical privileges

- [ ] 1.1 Pin #935's persisted grant/deny, effective graph, principal/revision, home database and EXECUTE/BOOSTED interfaces. Verify dependency tests distinguish TRAVERSE/READ, DENY/missing grant and target/caller roles (`IMP-04/06`, `CDC-07`).
- [ ] 1.2 Implement IMPERSONATE scopes and syntax through #935's administration parser/renderer and `pkg/auth` canonical records. Add `TestEnterpriseImpersonatePrivileges` for named/wildcard/multi-role/immutable/revoke/SHOW AS COMMANDS and restart (`IMP-01`).
- [ ] 1.3 Implement immutable security context and target resolver in `pkg/auth`; test lookup/error precedence, no-auth/self/disabled/unknown target and updating-admin restriction in `TestEnterpriseImpersonationResolution` (`IMP-02/04/05`).
- [ ] 1.4 Replace coarse prechecks on affected routes, including `/db/` middleware and procedure checks. Verify caller-without-graph-READ can impersonate a permitted target, while target cannot borrow caller rights (`IMP-04`, `QAPI-04`, `CDC-07`).
- [ ] 1.5 Implement one-time migration of role/allowlist/privilege records with a dry-run mapping and actionable ambiguity errors. Verify idempotence, credential/ID preservation and failed-migration recovery before deleting dual reads/global fallbacks.
- [ ] 1.6 Migrate native access-management API/UI consumers to canonical records or explicitly retire lossy matrix mutation. Verify native updates cannot erase fine-grained grants/denies and no old evaluator remains; document breaking migration examples.
- [ ] 1.7 Wire effective policy identity/revision to #935 caches and local/remote USE/composite execution. Verify `IMP-06`, including native search/path/procedure surfaces and no service-account-only remote forwarding.

## 2. Carry attribution into actual transactions

- [ ] 2.1 Add `TestEnterpriseTransactionAttribution` reproducing metadata loss through Bolt `BeginTransaction` and single-user RequestIdentity. Record failures for `ATTR-01/02` before changing these paths.
- [ ] 2.2 Add storage transaction-attribute values and extend Cypher request/transaction context; wire Bolt BEGIN/autocommit and implicit transaction creation. Verify trusted users/connections reach storage without storage importing auth/protocol packages.
- [ ] 2.3 Wire old HTTP sessions, txsession and CALL IN TRANSACTIONS child creation. Verify each child has its own commit identity and inherited attribution and rollback emits nothing (`ATTR-02`).
- [ ] 2.4 Use one deep-copying metadata setter for protocol metadata and tx.setMetaData. Verify oracle semantics/limits, final same-commit snapshot and resistance to spoofing trusted fields (`ATTR-01`).
- [ ] 2.5 Update SHOW CURRENT USER, SHOW TRANSACTIONS and structured query/audit attribution. Verify exact projection/user formatting and existing redaction; document ordinary/impersonated output (`IMP-04`, `ATTR-01/02`).
- [ ] 2.6 Add race/cleanup tests for metadata and identity reuse across retries, rollback, timeout, HTTP continuation, child batches and internal system writers (`ATTR-02`, `IMP-05`).

## 3. Implement Bolt impersonation

- [ ] 3.1 Add `TestEnterpriseBoltImpersonation` using raw messages and driver sessions across 4.4/supported 5.x and pre-4.4 field/layout cases. Record current failures (`IMP-02/03`).
- [ ] 3.2 Parse/validate imp_user in RUN, BEGIN and ROUTE in `pkg/bolt/session_messages.go`; resolve before database selection/authorization. Verify target home database and exact routing/error metadata (`IMP-03`).
- [ ] 3.3 Pin effective context in explicit lifecycle and autocommit result streams without mutating authResult. Verify explicit RUN extras, PULL/DISCARD and deferred completion retain correct identity (`IMP-03/05`).
- [ ] 3.4 Extend existing lifecycle tests for failed/nested BEGIN, pipelined FAILURE/IGNORED, RESET, timeout, disconnect, LOGOFF/LOGON and Alice/Bob/no-target pool reuse. Verify no leaked identity or storage transaction (`IMP-03/05`).
- [ ] 3.5 Document Bolt version support, target privilege requirements and examples; verify examples with official-driver sessions against both servers.

## 4. Build durable commit positions and bookmarks

- [ ] 4.1 Add `TestEnterpriseCommitPosition` for delayed/failed reservations, restart and multi-batch publication. Record why current process-counter/wall-clock/MVCC-reservation tokens fail (`CDC-05/09`, `QAPI-05`).
- [ ] 4.2 Implement per-database logical commit positions with `badger_commit_writer.go`/`badger_managed.go`, durable head and recovery seeding. Verify logical commits are distinct from physical batches.
- [ ] 4.3 Establish and test lock order against write barriers, unique-key commit locks (#961/#964), constraints/counts/schema, close and large-commit gates; the ordering gate is taken after the unique-key locks. Verify no deadlock under race tests, including concurrent MERGE on one unique key with capture on, and unrelated databases do not share a new global serialization gate.
- [ ] 4.4 Extract shared committed-position/bookmark capability for Bolt/HTTP, with database scope, waiting, cancellation/deadline and reference errors. Verify cross-transport/restart/future-token cases (`QAPI-05`).
- [ ] 4.5 Replace wall-clock HTTP bookmarks and Bolt process-counter/placeholder fallback. Integrate receipts only where they prove durable state; verify old placeholder acceptance is gone and document client token reacquisition.
- [ ] 4.6 Verify OFF-mode positions and benchmark same-database versus independent-database commit contention before CDC is layered on; record results and durability semantics. The position service runs on every commit, including OFF databases: compare OFF-mode commit latency, throughput and allocations against the pre-phase-4 baseline with the design's 5% budget before phase 6 starts.

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

## 8. Complete events, procedures and selectors

- [ ] 8.1 Add `TestEnterpriseCDCEvents` for FULL/DIFF schema, typed values, labels, keys, endpoints and net-change order. Verify exact rows/types rather than event counts (`CDC-03/04`).
- [ ] 8.2 Implement materialization from net states/committed schema/attributes, including equivalent uniqueness-plus-existence keys. Verify captured top-level label/key/endpoint rules, category ordering and stable seq.
- [ ] 8.3 Add narrow storage CDC read capability and wrapper forwarding. Implement versioned cursor kinds and validation in `TestEnterpriseCDCCursors`, including same-tx resume and earliest/current boundaries (`CDC-02/05`).
- [ ] 8.4 Register procedures and handlers in new `pkg/cypher/call_cdc.go` with exact signatures/defaults/SHOW PROCEDURES metadata and typed args. Verify YIELD/WHERE/RETURN/LIMIT composition (`CDC-02`).
- [ ] 8.5 Implement compiled selector validation/matching with `TestEnterpriseCDCSelectors`; verify every field, OR/AND, deduplication, nested endpoints, historic matching and null/type/unknown-field errors (`CDC-06`).
- [ ] 8.6 Implement canonical ACCESS/EXECUTE/BOOSTED checks and `TestEnterpriseCDCAuthorization`. Verify grants/denies/globs, impersonated target rights and all-events visibility despite graph restrictions (`CDC-07`).
- [ ] 8.7 Implement bounded published-window scans with cancellation and safe snapshot ownership. Add a streaming procedure result (rows yielded on demand through CALL … YIELD/WHERE/RETURN into #939's Bolt result stream) so LIMIT ends the scan. Verify LIMIT 1, resume, concurrent commits, PULL/DISCARD/HTTP disconnect and memory independent of total history size.
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
