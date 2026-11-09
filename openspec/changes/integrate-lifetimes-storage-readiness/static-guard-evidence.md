# PR #1693 focused static-guard correction — 2 November 2026

## Result and boundary

All three assigned static guards pass on the integrated dirty shared workspace
at HEAD `6b546791283db0e8b2f1951df1ac530a1829a26f`. This is focused evidence,
not PR readiness or a full-suite result. No production implementation, guard
assertion, census selector, exception activation, or non-egress allowlist changed.
The parent owns `tasks.md`; it was not edited by this lane.

Changed files:

- `internal/cyphertext/testdata/lexical_population.json`
- `pkg/observability/testdata/url_sink_inventory.json`
- `pkg/nornicdb/apoc_storage_adapter_test.go`
- `pkg/cypher/admin_cancellation_projection_test.go` (new)
- this evidence record

The APOC fixture change was announced to the helper-contracts lane before editing.
No commit, push, broad verify, independent full-suite run, or full workcensus
`Extract` was performed. No runtime/performance change was made; benchmarks and
profiles are not claimed.

## Original focused RED

Each command was run unfiltered with `-count=1`; each failed:

```sh
go test -count=1 -v ./internal/cyphertext -run '^TestFoundationLexicalPopulation$'
go test -count=1 -v ./internal/testfixture -run '^TestRepositoryStorageFixtureConstructorOwnership$'
go test -count=1 -v ./pkg/observability -run '^TestCredentialURLSinkStructure$'
```

Original tool output remains at
`/var/folders/72/0lxlwnts2_x8f823b1lbz9800000gn/T/1793673114102-copilot-tool-output-98733-a627068b-2c95-4d2f-95f3-5eb4e490b283.txt`.
This is a tool-output location, not a durable repository log or the parent's
filtered `make test` log.

### Lexical population owner

The explicit existing `NORNICDB_LEXICAL_POPULATION_DISCOVERY=1` mode was run,
its JSON compared against every reviewed row, and every delta source reviewed.
There were **12 added / 4 removed owner keys**, with **no mechanism/shared change
among retained keys**. Final population: **4,000 declaration rows / 3,999 owner
keys** (two existing `functions_text_locate.go:init` declarations share a key).
The prior population was 3,992 rows / 3,991 keys.

Reviewed additions:

| Source | Members | Disposition |
|---|---|---|
| `internal/workcensus/nil_capability.go` | `*initializerCapabilities.node`, `*initializerCapabilities.validate` | Go SSA initializer regions and capability verdicts, not credential/query readers |
| `internal/workcensus/nil_flow.go` | `*nilFlow.globalInitializer`, `*nilFlow.nonNilGlobalErrorAtCall`, `*nilFlow.sourcePackage` | Go package source loading and initializer/callsite metadata |
| `internal/workcensus/select_receive.go` | `*Program.channelFamilyOf`, `channelCallSeal`, `channelClosureSeal`, `channelUse`, `hiddenChannelUse`, `selectCaseBlock` | Closed SSA channel-population proof; numeric conversion reads compiler case-index constants |
| `pkg/cypher/subquery_value_expression.go` | `*StorageExecutor.evaluateRowSubqueryBody` | Shared lexical path through the row's statement-context adapter |

Removed candidate keys were individually reviewed:

- `*nilFlow.externalBody` still exists, but its source-loading implementation
  moved into `sourcePackage`; the wrapper no longer matches the conservative
  credential-name candidate selector.
- `*StorageExecutor.prepareFabricExecution`, `*StorageExecutor.planFabricQuery`,
  and `fabricSingleGraphStatement` still exist. Planning now passes the operation
  context and `cyphertext.AcceptedSource` directly to a local Fabric planner.
  The frozen extractor does not resolve local-object method dispatch; their
  former statically linked callback path no longer makes them candidates.
  This is an extractor limit, not a claim that Fabric no longer reads queries.
  Existing public preparation/cache cancellation and healthy-retry tests were run.

Every new row has an exact reason and named behavioral/adversarial tests.
The frozen extractor and its assertions were not broadened or weakened.

### Fixture lifetime owner

The complete module constructor scan reported exactly three gaps in
`pkg/nornicdb/apoc_storage_adapter_test.go`, original lines **807, 832, 849**:

- `TestAPOCStorageAdapterNativeDeleteCascadesIncidentRelationships`
- `TestAPOCStorageAdapterPreservesNativePointFailure`
- `TestAPOCStorageAdapterNativeTraversalVisitorCancellation`

Manual `t.Cleanup(Close)` disposed the stores but did not register the actual
constructor results with the runtime lifetime auditor. All three now use the
existing exact pattern:

```go
base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
```

The redundant manual cleanup was removed. The explicit `base.Close()` proving
native closed-store error propagation remains. Existing constructor-lint
negatives and runtime actual-state/leak/invalid-owner controls also pass.

### URL sink owner

The complete guard reported **10 new fingerprints / 8 stale entries**.
Every new hash was attributed to its exact formatted AST node before approval:

| Current owner / original reviewed source line | Actual change |
|---|---|
| `cmd/nornicdb/main.go:runServe:779` | Admin bootstrap failure returns the localized command-error owner instead of continuing startup |
| `pkg/cypher/admin_error_redaction.go:redactAdminCommandError:60-77` | Early return only for nil or exact immutable context errors |
| `pkg/cypher/admin_error_redaction.go:redactAdminCommandErrorSource:79-90` | Same exact-error predicate; wrapped errors still project credentials |
| `pkg/cypher/row_expression.go:evaluateRowValueText:644` | `endpoint.eval(operationCtx, e, values, nil)`; relationship graph endpoint, not configured URL serialization |
| `pkg/cypher/executor_show.go:executeDropAlias:1370` | Tokenizer cancellation returns `ctx.Err()` under the installed statement redaction defer |
| `pkg/config/dbconfig/store.go:SetOverrides:174,182` | Existing config-node create/update now receive `ctx`; payload unchanged |
| `pkg/multidb/catalog_transition.go:transitionCatalog:93,102` | Existing qualified catalog create/update now receive `ctx`; credential sealing/receipt publication retained |
| `pkg/storage/composite_engine.go:CreateEdge:531` | Existing constituent dispatch now receives `ctx`; edge and routing unchanged |

Exact full fingerprints and individual dispositions are in the changed manifest.
No unknown owner was accepted function-wide. Non-egress classifications remain
unchanged. Existing namespace-name private triage mentioned by the catalog
inventory is not cleared or reclassified by this work.

`TestAdminCredentialCancellationProjection` additionally checks **both** error
projection entry points, PASSWORD and URL-userinfo credential sources, nil,
exact cancellation/deadline identity, credential-bearing `fmt.Errorf` wrappers
and `errors.Join` wrappers. A switch to an `errors.Is` bypass would fail these
negative controls. Wrappers must retain `errors.Is` cause classification and
must not display the synthetic secret.

## Focused proof

All focused normal runs passed freshly (`-count=1`):

- Three original guards: cyphertext 2.670s; testfixture 3.097s;
  observability 2.219s.
- Cypher public/adversarial packet: 1.503s; normalization output: 1.403s.
- APOC three-fixture packet: 0.794s.
- Config store create/update/reload/clear: 0.439s.
- Catalog credential persistence/input/every-writer: 0.772s.
- Composite CreateEdge/validation: 1.249s.
- Real CLI bootstrap rejected/canceled/existing-account controls: 2.908s.
- Bounded compiler-model positive/negative controls: 36.914s.
- Constructor/runtime lifetime negative controls: 0.438s.

The **final integrated focused race packet** was run with the following exact
command (JSON output inspected for skipped and failed tests):

```sh
go test -race -count=1 -json ./internal/cyphertext ./internal/testfixture ./pkg/observability ./pkg/cypher ./pkg/nornicdb ./pkg/config/dbconfig ./pkg/multidb ./pkg/storage ./cmd/nornicdb ./internal/workcensus -run '^(TestFoundationLexicalPopulation|TestRepositoryStorageFixtureConstructorOwnership|TestCredentialURLSinkStructure|TestModuleConstructorRegistrationPopulation|TestConstructorEntryLint|TestLifetime.*|TestAdminCredentialCancellationProjection|TestUsePreparationCancellationDiagnosticsPreserveIdentityAndRedaction|TestAdminCredentialErrorOutput|TestPublicAliasTokenizationCancellation|TestRowEndpointPublicReadErrorAndRetry|TestRowEndpointOperationCancellationWithMetadata|TestRowEndpointPublicCancellationAndRetry|TestRowSubqueryAdapterStatementContract|TestRowSubqueryAdapterBodyErrorIdentity|TestRowSubqueryAdapterPublicParserModes|TestUsePreparationAPIErrorsAndHealthyRetry|TestUsePreparationFabricPlanCacheRetriesCanceledNameRecognition|TestCredentialNormalizationClassesOutput|TestAPOCStorageAdapterNativeDeleteCascadesIncidentRelationships|TestAPOCStorageAdapterPreservesNativePointFailure|TestAPOCStorageAdapterNativeTraversalVisitorCancellation|TestStore_.*|TestSolReview1513PersistedCredentials|TestRemoteCredentialPersistenceEveryWriter|TestRemoteCredentialPersistenceInput|TestCompositeEngine_CreateEdge.*|TestServeAdminBootstrap.*|TestSelectReceive.*|TestChannelExposureCyclicContainmentKeepsReachableTarget|TestNilFlowCapabilityPrimitive|TestNilFlowCapabilityFrontier|TestNilFlowCapabilityPhaseIsolation|TestNilFlowCapabilityUnseededSSA|TestNilFlowCapabilityCompleteUsesWithoutReferrers|TestMayBeNilPlants)$'
```

Result: **10 packages passed, 929 passing test/subtest events, zero failures,
zero skips, exit 0**. Package elapsed times: cyphertext 23.008s; testfixture
14.079s; observability 13.224s; cypher 11.200s; nornicdb 3.181s; dbconfig
4.057s; multidb 4.230s; storage 5.136s; CLI 19.528s; workcensus 59.257s.
These are verification timings, not performance benchmarks.

The final guards inventoried 18,770 production functions, 889 raw-egress
expressions, 37 string helpers and 54 exact non-egress classifications.
The repository fixture-constructor scan had zero remaining gaps.
`git diff --check` passed. Darwin linking emitted the existing duplicate
`-lobjc` warning; it did not block any test.

After writing this record, the original three-guard normal command was repeated:
cyphertext 2.208s, testfixture 2.029s, observability 1.598s, all passed.
Markdown fence balance and `git diff --check` passed. Full docs build was not run.
The seven URL-owner source hashes below were identical before and after the
final packet, confirming no intervening edits to these reviewed implementations:

```text
555dab6cd8139f40288c8c938a11b90272c3b04d290bf706e9e1c9a324fd477b  pkg/cypher/admin_error_redaction.go
2fc112adfaa2f6b71118c74c191072515430c7ccb1661852f5705c46366a6608  pkg/cypher/row_expression.go
9a8d26a4b581d1a96cda4e83c1f7ac4059f90660212667afcdc3491235d687b1  pkg/cypher/executor_show.go
b51bef1ce9a181ca31862565ca89078f11ac160e8c806edf1f82536ec5c713b8  pkg/config/dbconfig/store.go
e19c65b5236f0f8433a4011947559fd0160ef39013a8c81fe78ebbd00bc999c8  pkg/multidb/catalog_transition.go
eb98e478cfbeca3f7aebcb65127b967c93c74f393264711f370e834c4f428130  pkg/storage/composite_engine.go
a956ef5f5b628c82c30d1fe4b2a3335d7d6fac42943e4e3ed6c68dcb1fd63186  cmd/nornicdb/main.go
```

## Limits

Static candidates are not a provenance proof. Actual parser/program row paths,
cancellation, healthy retries, storage persistence and command bootstrap were
therefore exercised separately. No proof is claimed for arbitrary dynamic
dispatch, every credential sink, the entire workcensus model, unrelated dirty
changes or the interrupted full-suite run. Broad verify/full suite, benchmarks,
CI and readiness remain with the parent/operator. No operator decision is needed
for these three corrected guards.

Suggested scoped commit message (not committed):

```text
test: reconcile reviewed static ownership guards

- register APOC native fixtures with the runtime lifetime owner
- review lexical population and exact context-migration sink fingerprints
- prove immutable cancellation identity without bypassing wrapper redaction
```
