# Native count failure evidence

Base: f0f5a2f4fd2bf8cb8a9344af9923cb0f4efb5210, release/v1.5.2. No rebase of another agent's work.

All Go checks used GOMAXPROCS=2, GOMEMLIMIT=1500MiB, -p=1 and -tags=noui,nolocalllm. These tags exclude UI and local model compilation; no model inference was tested.

## Outcome

The five public acceptance owners now propagate native node/edge count errors: admin statistics, native database information, status, authorized metrics and the Heimdall database router. Healthy empty/nonempty and recovery controls pass. HTTP uses existing localized generic500 envelopes; the router preserves the underlying error. Composite statistics provenance behavior is unchanged.

## Checks

- Original five-owner selector before implementation: FAIL2.396s, ten injected node/edge fault cells. Healthy and recovery controls passed. Full raw log retained.
- Same selector after implementation: PASS2.504s.
- Broader server statistics, authorization, composite provenance, cancellation and Heimdall controls: PASS10.157s, 30 top-level tests.
- Same broader selector under -race: PASS93.738s.
- `go vet -p=1 -tags=noui,nolocalllm ./pkg/server`: PASS.
- `make verify-list BASE=origin/release/v1.5.2`: PASS; code changes select the broader release checks.
- `make verify BASE=origin/release/v1.5.2 ONLY=localization FETCH=0`: PASS. Other selected release checks were not executed by this scoped invocation.
- `scripts/code-intelligence-hook.sh check-staged`: FAILED before candidate reporting with AnalysisError: analysis/ingestion failed at localhost:7474. No indexed duplicate verdict; no hook configuration changed or bypassed.

The complete focused selector is `^TestNOR287(AdminStatsCountErrorAcceptance|DatabaseInfoCountErrorAcceptance|StatusFallbackCountErrorAcceptance|MetricsScopedCountErrorAcceptance|HeimdallStatsMustPropagateNativeCountFailure)$`. Use -count=1 -v -timeout=5m. Broader normal/race used `TestNOR287|TestHandleAdminStats|TestHandleStatus$|TestHandleDatabaseInfo$|TestStatusAggregatesOnlyReadableDatabases|TestMetricsDatabaseReadAdmission|TestMetricsCatalogUsesExistingAuthenticationAndLivePermissions|TestDatabaseInfoRequiresDatabaseAndConstituentRead|TestDatabaseInfoCompositeStatsProvenanceDeterministic|TestPublicHandleStatus_CanceledContextReturnsEarly|TestHeimdallRouter.*|TestStatusEndpoint.*|TestMetricsEndpoint.*|Test_HandleMetrics.*`, -count=1 -v -timeout=10m.

## Reuse and practical limits

One complete-pair helper removes repeated error swallowing. `DB.Stats` owns additional database state and cannot supply counts for arbitrary manager-selected engines; no new cross-package helper/API is necessary. NamespacedEngine already owns context-aware prefix counting. The removed legacy status path asserted context-free methods that no built-in prefix owner implements, rather than the actual storage.PrefixStatsEngine contract.

Bundled Heimdall repo-map/status/db-stats actions and conversation context were source-traced: they already observe router errors. Their error callback paths were not invoked end to end in these checks. Runtime proof covers the actual router and HTTP consumers. Storage-acquisition skips, composite admission and the broader errorless-API umbrella remain outside this native count correction. No full suite, benchmark, coverage threshold, provider matrix or full release qualification is claimed.

Four runtime fixtures were reused from the previous isolated proposal and ported there to current manager/auth seams. Original ticket blocks are separately preserved verbatim; the executable fixtures are not represented as byte-identical historical tests. No prior checkout was changed.
