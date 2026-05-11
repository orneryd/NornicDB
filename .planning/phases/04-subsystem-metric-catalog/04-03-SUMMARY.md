---
phase: 04-subsystem-metric-catalog
plan: 04-03
subsystem: observability
tags: [observability, metrics, cypher, prometheus, op-type-classifier, phase-4, met-08, met-09, met-26, met-25, risk-1]

requires:
  - phase: 04-subsystem-metric-catalog
    plan: 04-01
    provides:
      - CacheMetrics typed bag (cross-cutting cache_hits_total{cache="query_result"})
      - AllowedCacheNames / AllowedEvictionReasons closed enum precedent
      - GaugeFunc panic-recover template (RISK-8 / Pitfall 1)
      - cmd/nornicdb D-02c init-order chokepoint already populated
  - phase: 04-subsystem-metric-catalog
    plan: 04-02
    provides:
      - catalog_redstubs.go scaffolding (CypherMetrics stub now removed)
      - cmd/nornicdb HTTP+Bolt bag wiring pattern (extended for Cypher here)
      - per-subsystem BenchmarkObserve_HotXxx naming cadence
  - phase: 03-metrics-infrastructure-discipline
    provides:
      - NewLatencyHistogramVec / NewRowCountHistogramVec / NewCounterVec / NewGaugeVec typed constructors (D-01)
      - LatencyHistogram + RowCountHistogram + Bind() typed wrappers + observeWithExemplar chokepoint (D-02)
      - ForbiddenLabels panic-at-registration (D-03a)
      - TestEnv.AssertCardinalityCeiling (TEST-02)
      - allowedSubsystems closed enum (cypher prefix in scope)

provides:
  - CypherMetrics typed bag (11 families per MET-08) with D-08 tenant-flag forward-compat
  - AllowedCypherOpTypes closed-enum {read, write, schema, admin, fabric, parse_error} (RISK-1 corrected)
  - classifyOpType(info *QueryInfo, isFabric, isAdmin bool) string — pure function in pkg/cypher
  - StorageExecutor SetCypherMetrics(m, database) + SetCacheMetrics(m) DI setters
  - Three observation chokepoints in pkg/cypher/executor.go (admin-dispatch / parse-error / normal-path-after-Analyze)
  - D-16 transaction_conflicts wiring via observeTransactionConflict(err) helper
  - active_transactions gauge + observeTransactionBegin/End helper pair
  - slow_queries threshold gate via observeSlowQueryIfThresholded(duration)
  - QueryPlanCache.SetCypherMetrics + planner_cache_{hits,misses,size} emission (D-12a)
  - SmartQueryCache.SetCacheMetrics + cache_hits_total{cache="query_result"} bridge
  - SmartQueryCache LRU/TTL eviction emission (cache_evictions_total{reason="lru"|"ttl"})
  - cmd/nornicdb startup integration (CypherMetrics + CacheMetrics injection)
  - BenchmarkObserve_HotCypher — 5 sub-benches all 0 allocs/op evidence

affects: [04-04, 04-05, 04-06, 04-07, 05-tenant-flag, 06-sampler-flip, 08-trace-emission, 09-helm-chart, 10-recording-rules]

tech-stack:
  added:
    - pkg/observability/catalog_cypher.go (CypherMetrics typed bag — 359 LOC)
    - pkg/observability/catalog_cypher_bench_test.go (BenchmarkObserve_HotCypher — 96 LOC)
    - pkg/cypher/op_type.go (RISK-1 corrected classifier — 77 LOC)
    - pkg/cypher/op_type_test.go (table-driven D-04c — 176 LOC)
    - pkg/cypher/executor_metrics_test.go (3-chokepoint + D-16 + gauge balance + slow-query gate — 211 LOC)
    - pkg/cypher/cache_metrics_test.go (planner cache + bridge + nil-bag — 134 LOC)
    - cmd/nornicdb/cypher_metrics_test.go (startup smoke — 76 LOC)
  patterns:
    - "RISK-1 corrected classifier: classifyOpType(info *QueryInfo, isFabric, isAdmin bool) string reads QueryAnalyzer flags (IsSchemaQuery / IsWriteQuery / IsReadOnly) — the actual normal-path classifier surface — NOT a non-existent plan.Root.Op field. Three observation sites (admin dispatch / parse-error / normal-path-after-Analyze) cover all six closed-enum values."
    - "SetCypherMetrics propagates the bag into the executor's owned planCache so D-12a planner-cache emission fires automatically without callers reaching into private fields. SetCacheMetrics shape mirrors for the cross-cutting query-result cache bridge."
    - "MET-26 slow_query_threshold_seconds via prometheus.NewGaugeFunc reads cfg.Logging.SlowQueryThreshold().Seconds() on every scrape — config reload auto-reflects without event wiring (D-15b). RISK-8 / Pitfall 1: callback wraps defer-recover that returns 0 on panic so a buggy callback cannot poison /metrics scrape during the shutdown race window."
    - "D-08 forward-compat via constructor bool: NewCypherMetrics(reg, tenantLabelsEnabled, slowQueryThresholdFn) decides label-set shape ONCE at construction. Subsystems pass database unconditionally through bool-agnostic Bind helpers (BindQueryDuration / BindQueries / BindTransactionConflicts / BindSlowQueries); the helper drops the arg internally when the bag was constructed with the flag off."
    - "D-12a separation preserved: planner_cache_{hits,misses,size} live under nornicdb_cypher_planner_* (Cypher subsystem; planner cache is Cypher-specific). The Cypher query-result cache (SmartQueryCache) bridges into the cross-cutting Cache subsystem via cache_hits_total{cache=\"query_result\"} so the Phase 10 K8S-11 hit-ratio recording rule sees the workload."
    - "D-16 transaction_conflicts cross-layer wiring: storage layer detects ErrConflict and returns it; the Cypher transaction wrapper observes via observeTransactionConflict(err) using errors.Is(err, storage.ErrConflict) — wrapped errors still classify correctly. Storage layer never imports observability — preserves AGENTS.md §8 separation."

key-files:
  created:
    - pkg/observability/catalog_cypher.go (CypherMetrics: 11 families incl. SlowQueryThreshold GaugeFunc; tenant-flag-aware Bind helpers)
    - pkg/observability/catalog_cypher_bench_test.go (5 sub-benches at 0 allocs/op)
    - pkg/cypher/op_type.go (classifyOpType pure function over QueryInfo + dispatch flags)
    - pkg/cypher/op_type_test.go (table-driven D-04c TestOpType_AllClauseShapes + nil-info + precedence + closed-enum coverage)
    - pkg/cypher/executor_metrics_test.go (TestObserveQuery_NormalPath/_Write/_AdminDispatch/_ParseError/_NilBag_NoOp + TestTransactionConflicts_Increment + TestActiveTransactions_GaugeBalance + TestSlowQueries_ThresholdedIncrement)
    - pkg/cypher/cache_metrics_test.go (TestPlannerCache_HitsMisses + TestPlannerCacheSize_Gauge + TestQueryResultCache_BridgeIntoCacheBag + TestCacheEvictions_ClosedReasonEnum + nil-bag tests)
    - cmd/nornicdb/cypher_metrics_test.go (TestServerStartup_CypherMetricsRegistered)
    - .planning/phases/04-subsystem-metric-catalog/bench-cypher-04-03.txt (BenchmarkObserve_HotCypher evidence — 0 allocs/op everywhere)
  modified:
    - pkg/observability/catalog_cypher_test.go (RED skips removed; eight GREEN tests cover registration + GaugeFunc panic safety + nil callback safety + D-08 tenant-flag + Bind helper bool-agnostic shape + forbidden-label rejection + closed-enum contract + cardinality ceiling on tenant flag axis)
    - pkg/observability/catalog_redstubs.go (Cypher stub block removed; header updated to mark Plan 04-03 as SHIPPED)
    - pkg/cypher/executor.go (metrics + database StorageExecutor fields; SetCypherMetrics + SetCacheMetrics setters; cloneWithStorage propagation; observeQuery / observeTransactionConflict / observeTransactionBegin / observeTransactionEnd / observeSlowQueryIfThresholded helpers; observation wired at fabric branch + isSystemCommandNoGraph + validateSyntax err + post-Analyze chokepoints)
    - pkg/cypher/cache.go (QueryPlanCache.metrics field + SetCypherMetrics + observePlannerHit/Miss + setPlannerSize emission at Get/Put; SmartQueryCache.metrics field + SetCacheMetrics + observeHit/Miss/Eviction emission at Get/PutWithLabels/evictOldestLRU + TTL-expired path emits both miss and ttl eviction)
    - cmd/nornicdb/main.go (cypherMetrics constructed at D-02c init-order chokepoint between obs.New and httpServer.Start; injected into db.GetCypherExecutor() via SetCypherMetrics + SetCacheMetrics; cacheMetrics no longer dangling)

key-decisions:
  - "RISK-1 correction is the keystone of this plan. The original CONTEXT D-04 named a `plan.Root.Op` chokepoint that does NOT exist — *ExecutionPlan is built only on EXPLAIN/PROFILE; normal Execute uses QueryAnalyzer.Analyze() *QueryInfo. Implementation reads QueryInfo.IsSchemaQuery / IsWriteQuery / IsReadOnly. Three observation sites cover {read, write, schema, admin, fabric, parse_error} via classifyOpType + isAdmin/isFabric overrides at the dispatch sites. Reviewers: this is the only material deviation from the literal CONTEXT D-04 wording, and it is the deviation the plan was explicitly written to land."
  - "Single observation chokepoint post-Analyze covers cache-hit early-return AND every downstream execution path. Earlier draft considered observing inside each handler (executeWithoutTransaction / executeImplicitAsync / cache hit return); rejected because the cache-hit path bypasses both handlers and would silently drop observations for the highest-volume read workload. The single classifyOpType + observeQuery call AFTER Analyze and BEFORE any further branching catches every Execute return path uniformly."
  - "SetCypherMetrics propagates internally into the owned planCache (and SetCacheMetrics into the owned SmartQueryCache) so cmd/nornicdb only has to call ONE setter per concern instead of reaching through StorageExecutor.planCache and StorageExecutor.cache (which are private). Trade-off: marginally more coupling inside the setter; significantly cleaner public API surface and one fewer accidental-omission risk at startup."
  - "slow_query_threshold_seconds GaugeFunc reads cfg.Logging.SlowQueryThreshold (NOT cfg.Cypher.SlowQueryThresholdSeconds() as plan named). The latter does not exist on the config struct; the canonical config field is cfg.Logging.SlowQueryThreshold (time.Duration), shipped in Phase 2 D-04d as the single source of truth. The callback wraps `cfg.Logging.SlowQueryThreshold.Seconds()` to produce the float64 the GaugeFunc expects."
  - "TTL-expired entries on SmartQueryCache.Get emit BOTH a miss AND a ttl eviction. Rationale: from the caller's perspective the result was unavailable (miss); from the cache's perspective the entry was evicted (ttl). Emitting both observations gives dashboards the cause-and-effect signal — high TTL eviction rate explains a high miss rate without ambiguity."
  - "TransactionConflicts uses errors.Is rather than ==. Storage layer surface returns storage.ErrConflict directly today, but future code may wrap it via fmt.Errorf(\"...: %w\", storage.ErrConflict). errors.Is bridges both shapes without restructuring the storage error contract — D-16 separation preserved while the Cypher counter still classifies correctly."

requirements-completed: [MET-08, MET-09, MET-25, MET-26, TEST-02]

duration: ~85min
completed: 2026-05-03
---

# Phase 4 Plan 04-03: Cypher Subsystem Metric Catalog Summary

**Cypher catalog GREEN with the RISK-1 corrected `op_type` classifier: 11 families (MET-08) registered through Phase-3 typed constructors; `classifyOpType(*QueryInfo, isFabric, isAdmin)` reads the actual normal-path classifier surface (NOT the non-existent `plan.Root.Op`); three observation chokepoints (admin-dispatch / parse-error / normal-path-after-Analyze) cover the closed `{read, write, schema, admin, fabric, parse_error}` enum; D-15b GaugeFunc-backed `slow_query_threshold_seconds` reads `cfg.Logging.SlowQueryThreshold` on every scrape; D-12a planner cache stays under cypher subsystem with cross-cutting `cache_hits_total{cache="query_result"}` bridge; D-16 transaction_conflicts wired via the Cypher transaction wrapper preserving AGENTS.md §8 separation; BenchmarkObserve_HotCypher 0 allocs/op across all five sub-benches (well below ≤ 2 budget).**

## Performance

- **Duration:** ~85 min (single-agent worktree)
- **Tasks:** 6/6 (04-03-01 through 04-03-06; each TDD task split into RED test commit + GREEN impl commit)
- **Files created:** 8
- **Files modified:** 5
- **Per-file LOC (PERF-06 sub-cap = 800 in pkg/observability; global cap = 2,500):**
  - catalog_cypher.go — 359
  - catalog_cypher_test.go — 302
  - catalog_cypher_bench_test.go — 96
  - op_type.go — 77
  - op_type_test.go — 176
  - executor_metrics_test.go — 211
  - cache_metrics_test.go — 134
  - cypher_metrics_test.go — 76
  - All under the 800-LOC pkg/observability sub-cap.

## Accomplishments

- **MET-08 GREEN — 11 Cypher families** registered through Phase-3 typed constructors: `queries_total`, `query_duration_seconds`, `planner_duration_seconds`, `planner_cache_hits_total`, `planner_cache_misses_total`, `planner_cache_size`, `rows_returned_rows`, `active_transactions`, `transaction_conflicts_total`, `slow_queries_total`, `slow_query_threshold_seconds`. All families surface from a single `Gather()` call when the bag is materialized at startup.
- **RISK-1 corrected `op_type` classifier** — `classifyOpType(*QueryInfo, isFabric, isAdmin) string` is the closed-enum producer over the actual normal-path classifier surface (`QueryAnalyzer.Analyze()` flags). Precedence: `admin > fabric > parse_error (nil-info defensive) > schema > write > read > read (safe default)`. The `TestOpType_AllClauseShapes` table-driven test (D-04c) covers every clause shape the analyzer recognizes; `TestClassifyOpType_AllReturnsAreClosedEnum` belt-and-suspenders confirms the function NEVER returns outside the closed enum across all flag combinations.
- **Three observation chokepoints in `pkg/cypher/executor.go`** (per RISK-1 corrected mapping):
  - **Site 1 admin dispatch** (`isSystemCommandNoGraph` branch): `classifyOpType(info, false, true)` overrides the QueryInfo-derived classification (e.g., `SHOW DATABASES` would otherwise classify as `schema` via `HasShow → IsSchemaQuery`; D-04a says it buckets as `admin` instead). The override happens at the unified post-Analyze chokepoint so cache-hit early-returns AND every downstream path observe uniformly.
  - **Site 2 parse-error** (validateSyntax err return): emits `op_type="parse_error"` (D-04b sixth enum value) with `observeDuration=false` because parse cost is sub-microsecond and not meaningful to bucket.
  - **Site 3 normal-path-after-Analyze**: single `classifyOpType(info, false, isSystemCommandNoGraph(cypher))` call covers the read/write/schema buckets. The fabric branch has its own parallel observation (`isFabric=true → fabric`) at line ~972 because it returns earlier without falling through to the main path.
- **MET-26 GREEN — `slow_query_threshold_seconds`** registered as `prometheus.NewGaugeFunc` with callback `func() float64 { return cfg.Logging.SlowQueryThreshold.Seconds() }` (D-15b). Auto-reflects config reload; `defer recover` returns 0 on callback panic per RESEARCH RISK-8 / Pitfall 1 — buggy callback cannot poison `/metrics` scrape during shutdown race window. `TestSlowQueryThresholdGauge_PanicSafe` and `TestSlowQueryThresholdGauge_NilCallback` falsifiability gates pin the safety.
- **D-12a planner cache locality preserved**: `planner_cache_{hits,misses,size}` live under `nornicdb_cypher_planner_*` (Cypher subsystem owns these — planner cache is Cypher-specific, NOT cross-cutting). `QueryPlanCache.SetCypherMetrics(m)` propagates from the executor's `SetCypherMetrics` setter so callers don't reach into private fields.
- **D-12a cross-cutting cache bridge**: `SmartQueryCache.SetCacheMetrics(m)` routes the cross-cutting bag into the query-result cache; `cache_hits_total{cache="query_result"}` + `cache_misses_total` + `cache_evictions_total{reason="lru"|"ttl"}` emit on every Get/Put/Evict so the Phase 10 K8S-11 hit-ratio recording rule sees the workload. Closed-enum reason values (`lru`, `ttl`) — never free-form (Phase 3 D-03a forbidden-label discipline catches if anyone passes a free-form string).
- **D-16 transaction_conflicts cross-layer**: `observeTransactionConflict(err)` helper uses `errors.Is(err, storage.ErrConflict)` so wrapped storage errors still classify correctly. Storage layer never imports observability — preserves AGENTS.md §8 separation.
- **Active transactions gauge**: `observeTransactionBegin` / `observeTransactionEnd` helpers exposed for the transaction wrapper sites; gauge balance verified by `TestActiveTransactions_GaugeBalance` (Inc/Dec pairs balance to 0).
- **Slow queries counter**: wired into the existing `slowStart` deferred closure in `Execute()` alongside `emitSlowQueryLog` — single threshold gate (matches Phase 2 D-04c LOG-07 semantics; threshold≤0 disables emission).
- **D-08 forward-compat**: `NewCypherMetrics(reg, tenantLabelsEnabled, slowQueryThresholdFn)` decides label-set shape ONCE at construction; subsystem callers pass `database` unconditionally through bool-agnostic helpers (`BindQueryDuration` / `BindQueries` / `BindTransactionConflicts` / `BindSlowQueries`). Tests parameterized on both flag values per MET-21.
- **MET-25 hot path**: BenchmarkObserve_HotCypher measured **0 allocs/op** across all 5 sub-benches (Apple M1 Max, count=3) — far below the ≤ 2 alloc budget. Per-op latency 7.0ns (counter Inc) to 25.97ns (histogram Observe with exemplar guard short-circuit on Phase 1's NeverSample default).
- **`cmd/nornicdb/main.go` integration**: `cypherMetrics` constructed at the same Phase 4 D-02c init-order chokepoint (between `obs.New` and `httpServer.Start`) Plan 04-01 used for `cacheMetrics` and Plan 04-02 used for HTTP+Bolt bags; injected into `db.GetCypherExecutor()` via `SetCypherMetrics` (which propagates into the owned planCache for D-12a) + `SetCacheMetrics` (which routes into the owned SmartQueryCache for the bridge).

## Task Commits

Each task committed atomically (worktree mode, --no-verify; orchestrator validates hooks). TDD tasks split into RED test commit + GREEN impl commit:

1. **04-03-01: catalog_cypher.go GREEN — 11 families + slow_query_threshold GaugeFunc** — `37ca88e` (feat)
2. **04-03-02 RED: failing tests for classifyOpType (RISK-1 corrected)** — `ad817b9` (test)
3. **04-03-02 GREEN: pkg/cypher/op_type.go classifyOpType over QueryInfo** — `fae0a7e` (feat)
4. **04-03-03 RED: failing tests for three Cypher observation chokepoints** — `92be613` (test)
5. **04-03-03 GREEN: executor.go observation chokepoints (RISK-1 corrected)** — `f130851` (feat)
6. **04-03-04 RED: failing tests for planner cache + cross-cutting bridge** — `963d43d` (test)
7. **04-03-04 GREEN: planner cache + cross-cutting cache_hits_total wiring** — `6f2fd51` (feat)
8. **04-03-05: cmd/nornicdb wires CypherMetrics + CacheMetrics into executor** — `31cbb9a` (feat)
9. **04-03-06: BenchmarkObserve_HotCypher 0 allocs/op (MET-25 hot path)** — `8b17f3a` (test)

## Decisions Made

1. **RISK-1 corrected classifier — the keystone of this plan.** Original CONTEXT D-04 wording named a `plan.Root.Op` chokepoint that does NOT exist on the normal-path executor. `*ExecutionPlan` is built only on EXPLAIN/PROFILE; normal Execute uses `QueryAnalyzer.Analyze() *QueryInfo`. Implementation reads `QueryInfo.IsSchemaQuery / IsWriteQuery / IsReadOnly`. Three observation sites cover all six closed-enum values. This is the only material deviation from the literal CONTEXT D-04 wording, and it is the deviation the plan was explicitly written to land per RESEARCH RISK-1.
2. **Single observation chokepoint post-Analyze** covers cache-hit early-return AND every downstream execution path. An earlier draft considered observing inside each handler (executeWithoutTransaction / executeImplicitAsync / cache hit return); rejected because the cache-hit path bypasses both handlers and would silently drop observations for the highest-volume read workload. The single `classifyOpType` + `observeQuery` call AFTER `Analyze` and BEFORE any further branching catches every Execute return path uniformly. The fabric branch has its own parallel observation because it returns earlier.
3. **`SetCypherMetrics` propagates internally into the owned planCache** (and `SetCacheMetrics` into the owned SmartQueryCache) so `cmd/nornicdb` only has to call ONE setter per concern instead of reaching through `StorageExecutor.planCache` and `StorageExecutor.cache` (which are private). Trade-off: marginally more coupling inside the setter; significantly cleaner public API surface and one fewer accidental-omission risk at startup.
4. **`slow_query_threshold_seconds` GaugeFunc reads `cfg.Logging.SlowQueryThreshold`** (NOT `cfg.Cypher.SlowQueryThresholdSeconds()` as the plan literally named). The latter does not exist on the config struct; the canonical config field is `cfg.Logging.SlowQueryThreshold` (time.Duration), shipped in Phase 2 D-04d as the single source of truth. The callback wraps `cfg.Logging.SlowQueryThreshold.Seconds()` to produce the float64 the GaugeFunc expects. Reviewers: same `time.Duration` value used by `emitSlowQueryLog` and `observeSlowQueryIfThresholded` — single source.
5. **TTL-expired entries on `SmartQueryCache.Get` emit BOTH a miss AND a ttl eviction.** From the caller's perspective the result was unavailable (miss); from the cache's perspective the entry was evicted (ttl). Emitting both observations gives dashboards the cause-and-effect signal — high TTL eviction rate explains a high miss rate without ambiguity. Closed-enum reason="ttl" stays inside the AllowedEvictionReasons set per D-12b.
6. **`TransactionConflicts` uses `errors.Is` rather than `==`.** Storage layer surface returns `storage.ErrConflict` directly today, but future code may wrap it via `fmt.Errorf("...: %w", storage.ErrConflict)`. `errors.Is` bridges both shapes without restructuring the storage error contract — D-16 separation preserved while the Cypher counter still classifies correctly.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] cmd/nornicdb integration target file is main.go, not serve.go**

- **Found during:** Task 04-03-05
- **Issue:** Plan files_modified listed `cmd/nornicdb/serve.go` but the runServe function lives in `cmd/nornicdb/main.go` (same finding as Plan 04-01 deviation #3 and Plan 04-02 deviation #2). Plans 04-01 and 04-02 already documented this; we mirror the same pattern.
- **Fix:** Inserted the CypherMetrics construction + executor injection block into `cmd/nornicdb/main.go` at the same Phase 4 D-02c init-order chokepoint Plan 04-01 used for cacheMetrics and Plan 04-02 used for HTTP+Bolt bags — between `obs := observability.New(...)` and `httpServer.Start()`.
- **Files modified:** cmd/nornicdb/main.go.
- **Verification:** `go build -tags "nolocalllm noui" ./cmd/nornicdb/...` clean; `TestServerStartup_CypherMetricsRegistered` passes.
- **Committed in:** 31cbb9a (Task 04-03-05 commit).

**2. [Rule 3 - Blocking] Config field name correction (cfg.Cypher.SlowQueryThresholdSeconds vs cfg.Logging.SlowQueryThreshold)**

- **Found during:** Task 04-03-05
- **Issue:** Plan task body referenced `cfg.Cypher.SlowQueryThresholdSeconds()` for the GaugeFunc callback. That field does NOT exist on the loaded `cfg` struct; the canonical slow-query threshold is `cfg.Logging.SlowQueryThreshold` (time.Duration), shipped in Phase 2 D-04d as the single source of truth. Same value already drives `emitSlowQueryLog` (`exec.SetSlowQueryThreshold(cfg.Logging.SlowQueryThreshold)` at main.go:605).
- **Fix:** Callback wraps `cfg.Logging.SlowQueryThreshold.Seconds()` to produce the float64 the GaugeFunc expects. Single source preserved.
- **Files modified:** cmd/nornicdb/main.go (callback closure).
- **Verification:** `TestServerStartup_CypherMetricsRegistered` passes; the GaugeFunc `TestSlowQueryThresholdGauge_GaugeFunc` test in catalog_cypher_test.go also exercises the callback shape (using a stand-in `current` variable).
- **Committed in:** 31cbb9a (Task 04-03-05 commit).

**3. [Rule 1 - Bug] Plan named `rows_returned` metric; Phase-3 NewRowCountHistogramVec validates `_rows` suffix**

- **Found during:** Task 04-03-01 verification
- **Issue:** Plan named the family `rows_returned` (no `_rows` suffix). Phase 3 D-01a `validateNameSuffix` rejects rowcount histograms whose names don't end in `_rows` — panic at registration. The full registered name is therefore `nornicdb_cypher_rows_returned_rows`.
- **Fix:** Used `Name: "rows_returned_rows"` so the validator passes; tests assert the full `nornicdb_cypher_rows_returned_rows` name. The double-rows reads slightly awkward but is the intentional naming consequence of the MET-02 suffix discipline shipped in Phase 3.
- **Files modified:** pkg/observability/catalog_cypher.go (Name: "rows_returned_rows"); pkg/observability/catalog_cypher_test.go (asserts the full name).
- **Verification:** `TestCypherMetrics_RegistersElevenFamilies` passes; the registered name surfaces in `Gather()`.
- **Committed in:** 37ca88e (Task 04-03-01 commit).

**4. [Rule 3 - Blocking] QueryType constant naming**

- **Found during:** Task 04-03-04 RED test verification
- **Issue:** Test referenced `QueryTypeRead` but pkg/cypher's QueryType enum uses `QueryMatch` / `QueryCreate` / etc. (no `QueryType` prefix on the constants).
- **Fix:** Updated test to use `QueryMatch`. Documented the actual constants here for any future maintainer who reads the test.
- **Files modified:** pkg/cypher/cache_metrics_test.go.
- **Verification:** Tests build & pass.
- **Committed in:** 963d43d (Task 04-03-04 RED commit).

---

**Total deviations:** 4 (1 bug, 3 blocking).
**Impact on plan:** No scope creep. All four deviations are mechanical alignment with codebase reality (file path, config field name, suffix discipline already shipped in Phase 3, and a minor naming inconsistency in a test reference).

## Issues Encountered

- **Pre-existing macOS link warning** — `ld: warning: ignoring duplicate libraries: '-lobjc'` surfaces on every `go test` invocation in pkg/cypher (CGO localllm linkage). Pre-existing; not introduced by Plan 04-03. Tests pass cleanly.
- **Pre-existing `go vet` notices** — `pkg/cypher/antlr/cypher_parser.go` "unreachable code" warnings (ANTLR-generated) and `pkg/cypher/cypher_helpers_extra_test.go` "assignment copies lock value" warnings (pre-existing test file using sync.Once swap pattern). Both pre-existing; out of scope per Plan 04-03 boundary.
- **`make bench-cypher` Makefile target not yet wired** — Plan 04-07 (or later) will add it. Per-subsystem `BenchmarkObserve_HotCypher` evidence in `bench-cypher-04-03.txt` is the early-warning system this plan ships; the cumulative regression gate (`make bench-cypher` Δ ≤ 2%) is owned by KD-12 / Phase 12.

## Self-Check

Verifying claims:

**Files created (existence check):**

```
$ for f in pkg/observability/catalog_cypher.go \
          pkg/observability/catalog_cypher_bench_test.go \
          pkg/cypher/op_type.go \
          pkg/cypher/op_type_test.go \
          pkg/cypher/executor_metrics_test.go \
          pkg/cypher/cache_metrics_test.go \
          cmd/nornicdb/cypher_metrics_test.go \
          .planning/phases/04-subsystem-metric-catalog/bench-cypher-04-03.txt; do
  [ -f "$f" ] && echo "FOUND: $f" || echo "MISSING: $f"
done
FOUND: pkg/observability/catalog_cypher.go
FOUND: pkg/observability/catalog_cypher_bench_test.go
FOUND: pkg/cypher/op_type.go
FOUND: pkg/cypher/op_type_test.go
FOUND: pkg/cypher/executor_metrics_test.go
FOUND: pkg/cypher/cache_metrics_test.go
FOUND: cmd/nornicdb/cypher_metrics_test.go
FOUND: .planning/phases/04-subsystem-metric-catalog/bench-cypher-04-03.txt
```

**Commits exist (git log check):**

```
37ca88e feat(04-03-01): catalog_cypher.go GREEN — 11 families + slow_query_threshold GaugeFunc
ad817b9 test(04-03-02): add RED tests for classifyOpType (RISK-1 corrected)
fae0a7e feat(04-03-02): pkg/cypher/op_type.go GREEN — classifyOpType over QueryInfo
92be613 test(04-03-03): RED tests for three Cypher observation chokepoints
f130851 feat(04-03-03): executor.go observation chokepoints (RISK-1 corrected)
963d43d test(04-03-04): RED tests for planner cache + cross-cutting cache_hits_total
6f2fd51 feat(04-03-04): planner cache + cross-cutting cache_hits_total wiring (D-12a)
31cbb9a feat(04-03-05): cmd/nornicdb wires CypherMetrics + CacheMetrics into executor
8b17f3a test(04-03-06): BenchmarkObserve_HotCypher 0 allocs/op (MET-25 hot path)
```

**audit-untouched gate:**

```
$ git diff --name-only 154bbdbc50ce731a99aae796c629da691960fdf2..HEAD pkg/audit/
(empty)
```

No modifications to pkg/audit/audit.go.

**lint-cardinality gate:**

```
$ make lint-cardinality
lint-cardinality: PASS (MET-04 helper-only registration enforced)
```

**Build clean:**

```
$ go build -tags "nolocalllm noui" ./...
(success)
$ go vet -tags nolocalllm ./pkg/observability/...
(success)
```

(Pre-existing pkg/cypher/antlr unreachable-code and test-file lock-copy notices documented under Issues Encountered — not introduced by this plan.)

**BenchmarkObserve_HotCypher budgets:**

| Sub-bench | allocs/op | budget |
|-----------|-----------|--------|
| cypher_query_duration_bound_read | 0 | ≤ 2 |
| cypher_query_duration_bound_write_tenant | 0 | ≤ 2 |
| cypher_planner_cache_hits_inc | 0 | ≤ 2 |
| cypher_rows_returned_bound_read | 0 | ≤ 2 |
| cypher_queries_counter_bound | 0 | ≤ 2 |

All sub-benches: **0 allocs/op** (Apple M1 Max, count=3) — well below the ≤ 2 alloc budget.

**Test suite (race, scoped to plan-introduced packages):**

```
$ go test -tags nolocalllm -race -count=1 ./pkg/observability/... ./pkg/cypher/
ok      github.com/orneryd/nornicdb/pkg/observability   3.65s
ok      github.com/orneryd/nornicdb/pkg/cypher         44.20s

$ go test -tags "nolocalllm noui" -race -count=1 ./cmd/nornicdb/...
ok      github.com/orneryd/nornicdb/cmd/nornicdb        1.70s
```

**File-LOC discipline (PERF-06 800-LOC sub-cap in pkg/observability):**

```
catalog_cypher.go              — 359 LOC ✓ (well under 800)
catalog_cypher_test.go         — 302 LOC ✓
catalog_cypher_bench_test.go   —  96 LOC ✓
op_type.go                     —  77 LOC ✓
op_type_test.go                — 176 LOC ✓
executor_metrics_test.go       — 211 LOC ✓
cache_metrics_test.go          — 134 LOC ✓
cypher_metrics_test.go         —  76 LOC ✓
```

**Self-Check: PASSED**

## Next Phase Readiness

- **Plans 04-04..04-06 unblocked.** `pkg/observability` compiles via the remaining `catalog_redstubs.go` entries (Storage / MVCC / Embed / Search / Replication / Auth); each downstream plan replaces one stub block with the real GREEN bag. The `catalog_redstubs.go` Cypher entry is removed (Plan 04-03 SHIPPED).
- **Plan 04-07 inputs ready.** `bench-cypher-04-03.txt` evidence file captured with 5 sub-benches at 0 allocs/op; per-subsystem bench naming (BenchmarkObserve_HotCypher) cumulates cleanly with cache (HotCache), HTTP (HotHTTP), Bolt (HotBolt) and downstream subsystems' future benches.
- **Phase 5 forward-compat preserved.** D-08 `tenantLabelsEnabled` bool plumbed through `NewCypherMetrics(reg, tenantLabelsEnabled, slowQueryThresholdFn)`; reads from `cfg.Observability.Metrics.TenantLabelsEnabled` at startup. Phase 5 K8s autodetect will set the bool's default value with no re-registration.
- **Phase 6 forward-compat preserved.** `BoundLatencyObserver` + `BoundRowCountObserver` route through `observeWithExemplar` chokepoint — Phase 1's `NeverSample()` default means zero exemplars emitted today; Phase 6's `TraceIDRatioBased(0.01)` flip lights up exemplars at every observation site automatically with no second-pass migration.
- **Phase 8 (TRC-15, TRC-16) forward-compat preserved.** The three observation chokepoints in `pkg/cypher/executor.go` are the natural span-emission chokepoints. When Phase 8 ships, span emission piggy-backs on the same chokepoints (now metric + span) without restructuring the instrumentation. `classifyOpType` output becomes a span attribute.
- **Phase 9 (Helm/Grafana) catalog freeze contributor.** 11 Cypher metric names enumerated here are now canonical for the M1 release cycle — Phase 9 dashboard hard-codes them per ROADMAP.md hard ordering #3.
- **Phase 10 (K8S-11 cache hit-ratio recording rule) consumer ready.** `cache_hits_total{cache="query_result"}` ÷ `cache_misses_total{cache="query_result"}` is the underlying input — Plan 04-03 ships both counters via the `SmartQueryCache.SetCacheMetrics` bridge (D-12a).

---
*Phase: 04-subsystem-metric-catalog*
*Plan: 04-03*
*Completed: 2026-05-03*
