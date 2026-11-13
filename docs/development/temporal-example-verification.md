# Verifying temporal examples

The ownership, fact-ledger, historical-read and epoch-block examples are executable
specifications. `internal/testfixture/temporalguide.Example` reads each named Cypher
fence from its guide; tests execute those bytes instead of a separately maintained
query. The downloadable ownership script is executed separately.

## Product behavior and its proof

| Contract | Unit/native proof | Public transport proof |
| --- | --- | --- |
| Ownership setup, transfer and shared-boundary selection | `TestTemporalGuideOwnershipExamples` | `TestTemporalGuideHTTPEndToEnd`, `TestTemporalGuideBoltEndToEnd` |
| Open past, open future, both-null rejection and finite-bound ordering | `TestSymmetricTemporalEpochs`, `TestTemporalGuideEpochBounds` | Both guide transport suites |
| Failed transfer rolls back its earlier changes; repeated transfer has no matching old owner | Ownership example suite | Both guide transport suites |
| Same asset rejects overlapping claims across owners; another asset has an independent timeline | Ownership example suite | Both guide transport suites |
| Full employment contract: every primitive and boolean entry remains active | `TestTemporalGuideConstraintBlockExamples`, `TestEmploymentContractWithEpoch` | Both guide transport suites |
| Composite groups and independent grouping per epoch entry | `TestConstraintContractEpochGroupingIsLocalToEntry`, `TestConstraintContractMultipleEpochEntriesKeepSeparateGroups` | Employment and price scenarios in both transport suites |
| Explicit fact versions, current-pointer replacement and event linkage | `TestTemporalGuideGenericLedgerExamples` | Both guide transport suites |
| Historical result and current result differ after a committed update | `TestTemporalGuideHistoryExample` | Both guide transport suites |
| Published cost declarations | `TestTemporalGuideCostExamples` | Both guide transport suites |
| Standalone and deprecated constraint declarations | `TestTemporalGuideStandaloneDeclarations` | Existing procedure/declaration transport suites |
| Downloadable ownership script | `TestTemporalGuideDownloadableScript` | Both guide transport suites |
| Indexed and scan validation agree; existing finite index-key bytes stay unchanged | `TestSymmetricTemporalEpochs`, `TestTemporalEpochSortEncodingKeepsFiniteKeys` | Public epoch scenarios |
| Restart and write-behind preserve open-start lookup; deprecated reads accept it | `TestSymmetricEpochIndexSurvivesRestart` | Deprecated signature transport tests also cover existing finite-bound calls |
| Concurrent open-bound claims cannot both succeed, for nodes/edges and standalone/block declarations | `TestSymmetricEpochConcurrentClaims` | Public executor concurrency; transport suites check ordinary conflict responses |
| Deletion/recreation, creation after cutoff and unavailable history | `TestHistoricalQueryDoesNotResurrectDeletedOrRecreatedEntities`, `TestHistoricalQueryReportsPrunedHistory`, native historical-read suite | Ownership transports confirm deleted relationships are not resurrected |
| Cancellation, pinned admission and reader release | `TestHistoricalRead_Cancellation`, `TestHistoricalRead_ReadOnlyAndCloseLifetime`, `TestHistoricalReadPruneMarkerUsesPinnedAdmission` | Native/component cancellation proof; no new transport cancellation claim |

The executor and transport suites run both parsers. HTTP tests use an authenticated
loopback server; Bolt tests use the Neo4j driver and a loopback Bolt server. Fixtures
own their temporary databases and listeners. They never connect to dogfood storage.

## Reproduce

```sh
go test ./pkg/storage ./pkg/cypher ./pkg/bolt ./pkg/server \
  -run 'TestSymmetric|TestTemporalEpochSortEncoding|TestTemporalGuide|TestConstraintContractEpoch|TestEmploymentContract' \
  -count=1 -timeout=3m
```

Run `npm run build` and `npm test` in `docs-site` for publishing, navigation and
search checks. Its maintenance-scope check must use the actual scope/base of the
change; a mixed feature branch is not a documentation-only maintenance PR.
`npx playwright test browser-tests/temporal.spec.mjs` exercises the rendered
learning path and mobile/desktop layout.

A docs build or browser pass does not prove Cypher behavior. Focused tests do not
replace full CI, a backup/restore qualification or workload-specific benchmarking.
The [cost report](../performance/temporal-read-and-constraint-costs.md) distinguishes
measured validation kernels from end-to-end latency.
