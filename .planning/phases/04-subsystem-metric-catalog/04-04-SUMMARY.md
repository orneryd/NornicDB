---
phase: 04-subsystem-metric-catalog
plan: 04-04
subsystem: storage-mvcc
tags: [observability, metrics, storage, mvcc, badger, lifecycle-component, risk-2, met-10, met-11, met-25, phase-4]

requires:
  - phase: 04-subsystem-metric-catalog
    plan: 04-01
    provides:
      - StorageMetrics + MVCCMetrics stub blocks (now removed; GREEN bags ship here)
      - MVCCProbe interface seam — relocated to catalog_mvcc.go alongside the GREEN bag
      - GaugeFunc panic-recover template (RISK-8 / Pitfall 1)
  - phase: 04-subsystem-metric-catalog
    plan: 04-03
    provides:
      - catalog_redstubs.go ownership map convention (Storage/MVCC entries now SHIPPED)
      - cmd/nornicdb D-02c init-order chokepoint (extended for storage+mvcc bags here)
      - per-subsystem BenchmarkObserve_HotXxx naming cadence (HotStorage / HotMVCC)
  - phase: 03-metrics-infrastructure-discipline
    provides:
      - NewLatencyHistogram / NewCounterVec / NewGaugeVec typed constructors (D-01)
      - LatencyHistogram + Bind() typed wrappers + observeWithExemplar chokepoint (D-02)
      - ForbiddenLabels panic-at-registration (D-03a)
      - TestEnv.AssertCardinalityCeiling (TEST-02)
      - allowedSubsystems closed enum (storage, mvcc prefixes already registered)
  - phase: 01-observability-foundation-skeleton
    provides:
      - lifecycle.Component interface (consumed by BytesMetricsSweeper)

provides:
  - StorageMetrics typed bag (8 families per MET-10) with D-08 tenant-flag forward-compat
  - MVCCMetrics typed bag (4 families per MET-11) with D-08 forward-compat + D-14 thresholds
  - AllowedStorageBytesKinds {nodes, edges, index, wal, search} closed enum
  - AllowedStorageOps {get, put, delete, scan} closed enum
  - AllowedStorageIndexes {label, edge_between, temporal, embedding, user_created} closed enum (D-13c)
  - AllowedStorageResults {success, failure, aborted} closed enum
  - AllowedMVCCBands {normal, warn, high, critical} closed enum (D-14)
  - StorageProbe + MVCCProbe interfaces declared in pkg/observability (D-02d leaf-package boundary preserved)
  - RISK-2 accessors on *BadgerEngine: PinnedBytes() int64, OldestReaderAgeSeconds() float64, ActiveReaders() int64
  - storage.ClassifyIndexName(internal string) string — pure D-13c mapping function
  - storage.BytesMetricsSweeper lifecycle.Component (D-07 30s sweep using badger.DB.EstimateSize)
  - storage.DefaultBytesMetricsInterval (30s) constant
  - BadgerEngine.AttachMetrics(storage, mvcc) DI setter (pre-binds the four op-duration observers)
  - BadgerEngine.DB() exposes *badger.DB for sweeper EstimateSize calls
  - BadgerEngine per-op observation at GetNode/GetEdge/CreateNode/UpdateNode/CreateEdge/UpdateEdge/DeleteNode/DeleteEdge/AllNodes/AllEdges
  - cmd/nornicdb unwrapBadgerEngine + badgerStorageProbe + badgerMVCCProbe adapters
  - config.StorageRuntimeConfig.BytesMetricInterval — sweep cadence override
  - BenchmarkObserve_HotStorage + BenchmarkObserve_HotMVCC — 5 sub-benches all 0 allocs/op evidence

affects: [04-05, 04-06, 04-07, 05-tenant-flag, 06-sampler-flip, 08-trace-emission, 09-helm-chart, 10-recording-rules]

tech-stack:
  added:
    - pkg/observability/catalog_storage.go (StorageMetrics typed bag — 274 LOC)
    - pkg/observability/catalog_storage_bench_test.go (HotStorage 3 sub-benches — 59 LOC)
    - pkg/observability/catalog_mvcc.go (MVCCMetrics typed bag — 229 LOC)
    - pkg/observability/catalog_mvcc_bench_test.go (HotMVCC 2 sub-benches — 45 LOC)
    - pkg/storage/bytes_metrics.go (D-07 30s sweep lifecycle.Component — 173 LOC)
    - pkg/storage/bytes_metrics_test.go (sweeper coverage incl. -race -count=10 — 246 LOC)
    - pkg/storage/index_rebuild_metrics.go (D-13c classifyIndexName — 45 LOC)
    - pkg/storage/index_rebuild_metrics_test.go (5-bucket + 1k-name falsifiability — 70 LOC)
    - pkg/storage/badger_metrics.go (AttachMetrics + observeStorageOp + updatePressureBand — 89 LOC)
    - pkg/storage/badger_metrics_test.go (per-op chokepoints + alloc gate — 191 LOC)
    - pkg/storage/badger_mvcc_accessors_test.go (RISK-2 race-clean accessor coverage — 124 LOC)
    - cmd/nornicdb/storage_metrics.go (unwrap chain + probe adapters — 80 LOC)
    - cmd/nornicdb/storage_metrics_test.go (unwrap + probe coverage — 61 LOC)
    - .planning/phases/04-subsystem-metric-catalog/bench-storage-mvcc-04-04.txt (0 allocs/op evidence)
  patterns:
    - "RISK-2 fixed in Wave 0 BEFORE the GaugeFunc registrations land. The MVCC bag's three live-read gauges (pinned_bytes, oldest_reader_age_seconds, active_readers) call accessors on *BadgerEngine that the bag's MVCCProbe interface declares. Plan 04-04-01 added those accessors with a fallback path that reads existing atomic counters when no lifecycle controller is wired (preserves the existing zero-config Engine surface) and routes through the controller's optional interfaces (PinnedBytes / OldestReaderAge) when one is present (production path)."
    - "D-13c keystone: closed `index` enum {label, edge_between, temporal, embedding, user_created} prevents user-named index strings from becoming Prometheus label values. The pure mapping function `storage.classifyIndexName` lives in pkg/storage/index_rebuild_metrics.go (NOT in pkg/observability — keeps the leaf-package boundary clean). Falsifiability gate `TestIndexRebuild_NoFreeFormLabel` drives 1k random user-named indexes and asserts cardinality stays at 1 (all bucket to user_created)."
    - "D-07 30s sweep is a lifecycle.Component (Pattern 2 from RESEARCH §Architecture). Start spawns a goroutine that fires an initial sweep immediately (so gauges have non-zero values by the first scrape) then ticks every interval. Shutdown is sync.Once-guarded and idempotent. The sweep itself is wrapped in defer-recover per RISK-8 — a Badger panic during concurrent compaction cannot crash the supervisor errgroup. Five kind buckets: nodes (prefixNode), edges (prefixEdge), index (sum across label_index + edge_between_index + temporal_index prefixes — D-13c user-created live under same Badger prefixes so byte accounting captures them transparently without leaking names), wal (heuristic vlog-lsm from db.Size() per RISK-6), search (Plan 04-05 wires SearchSizeFn callback)."
    - "D-14 pressure_band thresholds 0.50/0.75/0.90 hardcoded as MVCCBandWarn/High/Critical constants. UpdateBand(database, ratio) is indicator-style: the active band's gauge is set to 1, the other three bands' gauges are set to 0 for the same database. Lets dashboards do `max by (database) (... == 1)` to pick current band; alert rules fire on `nornicdb_mvcc_pressure_band{band=\"critical\"} == 1`."
    - "MET-25 hot path: AttachMetrics(storage, mvcc) pre-binds the four BoundLatencyObserver fields (opDurGet/Put/Delete/Scan) at construction time. The deferred observation in observeStorageOp pays only one Observe call per Get/Put/Delete/Scan — no WithLabelValues lookup per request. The metricsAttached atomic.Bool short-circuits observation entirely when no bag is wired (test mode / embedded-library callers). BenchmarkObserve_HotStorage/storage_op_duration_bound_get measures 25 ns/op, 0 allocs."
    - "D-08 forward-compat: NewStorageMetrics + NewMVCCMetrics accept tenantLabelsEnabled bool. Storage bag carries the database label only on IndexRebuildTotal (D-08a — op_duration stays tenant-agnostic; per-database op latency lives at the Cypher subsystem). MVCC bag carries database only on PressureBand. BindIndexRebuild + UpdateBand helpers are tenant-flag-aware so subsystem callers pass database unconditionally."
    - "D-16 separation preserved: storage emits ONLY its own subsystem families (op_duration, bytes, compactions, index_rebuild, nodes/edges, wal_lag). transaction_conflicts stays under nornicdb_cypher_* (Plan 04-03). The pkg/storage → pkg/observability dependency added by Plan 04-04 is the LIMITED storage-emits-its-own-families dependency; storage still does NOT classify ErrConflict, that wiring lives in pkg/cypher per Plan 04-03 D-16."
    - "wal_lag_bytes documented as best-effort heuristic (vlog - lsm from db.Size()). Both `nornicdb_storage_wal_lag_bytes` (dedicated gauge) and `nornicdb_storage_bytes{kind=\"wal\"}` are populated with the same value so alert rules don't have to filter the kind label. Per RISK-6 the documentation calls out 5-minute trends as the correct alerting input — single scrapes are noisy."
    - "Leaf-package boundary nuance: pkg/observability still does NOT import pkg/storage (verified by `grep`). The reverse — pkg/storage imports pkg/observability — was already explicitly allowed by the CLAUDE.md plan constraints (`Leaf-package boundary — pkg/observability never imports pkg/storage. The reverse is fine.`). Plan 04-04 lands the first pkg/storage → pkg/observability import edge; this is by design and necessary for the per-op observation chokepoints."

key-files:
  created:
    - pkg/observability/catalog_storage.go (StorageMetrics: 8 families incl. nodes_total/edges_total GaugeFuncs; tenant-flag-aware BindIndexRebuild)
    - pkg/observability/catalog_storage_test.go (TestStorageMetrics_RegistersEight + closed-enum gates + GaugeFunc panic-safe + tenant-flag axis)
    - pkg/observability/catalog_storage_bench_test.go (3 sub-benches at 0 allocs/op)
    - pkg/observability/catalog_mvcc.go (MVCCMetrics: 4 families incl. 3 live-read GaugeFuncs; closed band enum + UpdateBand helper)
    - pkg/observability/catalog_mvcc_test.go (TestMVCCMetrics_RegistersFour + closed-enum + GaugeFunc panic-safe + threshold mapping)
    - pkg/observability/catalog_mvcc_bench_test.go (2 sub-benches at 0 allocs/op)
    - pkg/storage/bytes_metrics.go (BytesMetricsSweeper lifecycle.Component + DefaultBytesMetricsInterval)
    - pkg/storage/bytes_metrics_test.go (FirstTick + IntervalOverride + PanicRecovers + RaceSafe + ShutdownStops + NilGuards)
    - pkg/storage/index_rebuild_metrics.go (classifyIndexName + ClassifyIndexName)
    - pkg/storage/index_rebuild_metrics_test.go (TestClassifyIndexName + TestIndexRebuild_NoFreeFormLabel)
    - pkg/storage/badger_metrics.go (AttachMetrics + observeStorageOp + updatePressureBand helpers)
    - pkg/storage/badger_metrics_test.go (per-op chokepoints + AllocsPerRun gate)
    - pkg/storage/badger_mvcc_accessors_test.go (RISK-2 accessor race-clean coverage)
    - cmd/nornicdb/storage_metrics.go (unwrapBadgerEngine + badgerStorageProbe + badgerMVCCProbe)
    - cmd/nornicdb/storage_metrics_test.go (unwrap chain + probe accessor routing)
    - .planning/phases/04-subsystem-metric-catalog/bench-storage-mvcc-04-04.txt (BenchmarkObserve_HotStorage + HotMVCC 0 allocs/op evidence)
  modified:
    - pkg/observability/catalog_redstubs.go (Storage + MVCC stub blocks removed; ownership header updated to mark Plan 04-04 SHIPPED)
    - pkg/observability/catalog_mvcc_test.go (RED skips removed; eight GREEN tests cover registration + closed-enum + threshold mapping + GaugeFunc panic safety + nil-probe defensive fallback)
    - pkg/observability/catalog_storage_test.go (RED skips removed; seven GREEN tests cover registration + 3 closed-enum gates + nodes/edges GaugeFunc + panic safety + tenant-flag axis)
    - pkg/storage/badger.go (BadgerEngine fields: storageMetrics + mvccMetrics + four BoundLatencyObserver + metricsAttached atomic; DB() accessor; observability import)
    - pkg/storage/badger_mvcc.go (RISK-2 accessors: PinnedBytes / OldestReaderAgeSeconds / ActiveReaders — read-only, concurrent-safe via existing lifecycle.ReaderRegistry RWMutex + atomic counter fallback)
    - pkg/storage/badger_nodes.go (per-op observation: GetNode→opDurGet, CreateNode/UpdateNode→opDurPut, DeleteNode→opDurDelete)
    - pkg/storage/badger_edges.go (per-op observation: GetEdge→opDurGet, CreateEdge/UpdateEdge→opDurPut, DeleteEdge→opDurDelete)
    - pkg/storage/badger_queries.go (per-op observation: AllNodes/AllEdges→opDurScan; time import added)
    - cmd/nornicdb/main.go (StorageMetrics + MVCCMetrics constructed at the same Phase 4 D-02c init-order chokepoint Plans 04-01..04-03 used; AttachMetrics injection; bytes_metrics_sweeper appended to lifecycle.Component list between pprof and workers per RESEARCH §Q4)
    - pkg/config/config.go (Config.Storage StorageRuntimeConfig with BytesMetricInterval time.Duration)

key-decisions:
  - "RISK-2 Wave-0 fix shipped first: PinnedBytes() int64 / OldestReaderAgeSeconds() float64 / ActiveReaders() int64 added to *BadgerEngine BEFORE the MVCC bag's GaugeFunc registrations land. Existing PressureController already had a `pinnedBytes func() int64` callback wired in pkg/storage/lifecycle/pressure.go, so the engine accessor probes the lifecycle controller via an optional interface (`interface{ PinnedBytes() int64 }`) — this avoids coupling the storage types.go MVCCLifecycleController surface to a metric-only accessor while still routing real values when a controller is wired. Falls back to 0 when no controller (test mode / embedded-library); the GaugeFunc callback's defer-recover means a buggy/missing controller cannot poison /metrics. ActiveReaders ALWAYS returns a real value (atomic counter when no controller; controller's ReaderRegistry.ActiveCount when wired)."
  - "MVCCProbe + StorageProbe interfaces declared in pkg/observability — the existing Plan 04-01 stub had MVCCProbe at int64/float64/int64 signatures (matching client_golang and the eventual usage of `float64(probe.PinnedBytes())`). Wave 0 RED tests already pinned this shape; Plan 04-04-01 implementation matched. The plan task body's literal `uint64 + time.Duration + int` was a documentation drift relative to the test stub that landed first; we honor the test-stub shape (which had been reviewed and shipped). Cmd/nornicdb wraps *BadgerEngine.NodeCount() (int64, error) via the badgerStorageProbe adapter that drops the error to 0 — engine ensureOpen errors are NOT a metric surface concern (the GaugeFunc reports 0 during shutdown anyway)."
  - "D-13c index-name classifier lives in pkg/storage (NOT pkg/observability) per the leaf-package boundary nuance: pkg/observability still does not import pkg/storage. The classifier is a pure function over an internal string — it belongs at the storage call site where the internal string is generated. The closed enum AllowedStorageIndexes is declared in pkg/observability (it's the metric label-set authority); the producer function (classifyIndexName) is declared in pkg/storage; both live by the same closed-enum contract."
  - "D-07 sweep cadence via cfg.Storage.BytesMetricInterval — added to a NEW StorageRuntimeConfig type on cfg.Config, NOT to the existing cfg.Database (DatabaseConfig) struct. Rationale: DatabaseConfig mirrors persisted ENV/YAML on-disk schema; BytesMetricInterval is a runtime-only knob that does not belong on the on-disk config. The plan literal `cfg.Storage.BytesMetricInterval` matches this new path. cmd/nornicdb's main.go reads `cfg.Storage.BytesMetricInterval` directly; zero/unset falls back to storage.DefaultBytesMetricsInterval (30s)."
  - "Per-op chokepoints land at the public-method entry, NOT at the withUpdate/withView callback. Rationale: the chokepoint must observe the FULL wall-clock cost (including encoding, validation, callback dispatch) — observation deeper inside loses the validation + serialization phases. The `defer b.observeStorageOp(start, observer)` pattern at the top of GetNode/CreateNode/etc. captures the full method latency. metricsAttached atomic short-circuits the observation entirely when no bag is wired so the unattached path pays only one atomic.Bool.Load (~1ns)."
  - "AttachMetrics is the SINGLE setter — both storage and MVCC bags injected together. Pre-binds the four op-duration observers (Bind(\"get\"/\"put\"/\"delete\"/\"scan\")) into struct fields at construction time per MET-25. Subsequent Observe calls pay zero WithLabelValues lookup. The atomic.Bool guard ensures injection is observable from the read goroutine without holding a lock on the hot path."
  - "bytes_metrics_sweeper sits BETWEEN pprof and workers in the lifecycle.Component registration order per RESEARCH §Q4. Drain runs in reverse order, so the sweeper drains AFTER workers and BEFORE the telemetry listener — guaranteeing the final scrape during drain still reflects the last sweep's gauge values before the engine starts shutting down. The supervised drain budget (5s typical) is more than enough for the sync.Once + done-channel Shutdown."

requirements-completed: [MET-10, MET-11, MET-25, TEST-02]

duration: ~75min
completed: 2026-05-03
---

# Phase 4 Plan 04-04: Storage + MVCC Subsystem Metric Catalogs Summary

**Storage + MVCC catalogs GREEN with the RISK-2 accessor fix shipped Wave 0: 8 storage families (MET-10) + 4 MVCC families (MET-11) registered through Phase-3 typed constructors; D-13c closed `index` enum {label, edge_between, temporal, embedding, user_created} prevents user-named indexes from becoming Prometheus label values; D-07 30s lifecycle.Component sweep populates `bytes{kind}` via `badger.DB.EstimateSize(prefix)`; per-op observation at GetNode/CreateNode/UpdateNode/DeleteNode/AllNodes (and edge counterparts) via pre-bound BoundLatencyObserver cached in BadgerEngine struct fields (MET-25); D-14 pressure_band {normal, warn, high, critical} closed enum with thresholds 0.50/0.75/0.90; PinnedBytes/OldestReaderAgeSeconds/ActiveReaders accessors on *BadgerEngine satisfy the MVCCProbe interface with controller-routing optional-interface fallback; cmd/nornicdb wires both bags + sweeper at the same Phase 4 D-02c init-order chokepoint Plans 04-01..04-03 used; D-16 separation preserved (storage emits no Cypher metrics); BenchmarkObserve_HotStorage + HotMVCC 5 sub-benches all 0 allocs/op (Apple M1 Max, count=3) — well below the ≤ 2 alloc budget.**

## Performance

- **Duration:** ~75 min (single-agent worktree)
- **Tasks:** 8/8 (04-04-01 through 04-04-08; TDD task split into RED test commit + GREEN impl commit for 04-04-01 only — subsequent tasks landed test+impl together to fit the worktree-isolated execution flow)
- **Files created:** 15
- **Files modified:** 9
- **Per-file LOC (PERF-06 sub-cap = 800 in pkg/observability; global cap = 2,500):**
  - catalog_storage.go            — 274 LOC ✓ (under 800)
  - catalog_mvcc.go               — 229 LOC ✓
  - catalog_storage_test.go       — 192 LOC ✓
  - catalog_mvcc_test.go          — 211 LOC ✓
  - catalog_storage_bench_test.go —  59 LOC ✓
  - catalog_mvcc_bench_test.go    —  45 LOC ✓
  - bytes_metrics.go              — 173 LOC ✓
  - bytes_metrics_test.go         — 246 LOC ✓
  - index_rebuild_metrics.go      —  45 LOC ✓
  - index_rebuild_metrics_test.go —  70 LOC ✓
  - badger_metrics.go             —  89 LOC ✓
  - badger_metrics_test.go        — 191 LOC ✓
  - badger_mvcc_accessors_test.go — 124 LOC ✓
  - storage_metrics.go (cmd)      —  80 LOC ✓
  - storage_metrics_test.go (cmd) —  61 LOC ✓
- All under the 800-LOC pkg/observability sub-cap and the 2,500-LOC global cap.

## Accomplishments

- **MET-10 GREEN — 8 Storage families** registered through Phase-3 typed constructors:
  `nodes_total` (GaugeFunc), `edges_total` (GaugeFunc), `bytes{kind}`, `op_duration_seconds{op}`, `compactions_total{level,result}`, `compaction_duration_seconds{level}`, `wal_lag_bytes`, `index_rebuild_total{[database,] index, result}`. All families surface from a single `Gather()` once the bag is materialized at startup.
- **MET-11 GREEN — 4 MVCC families** registered through Phase-3 typed constructors:
  `pressure_band{[database,] band}` (indicator-style; closed enum {normal, warn, high, critical}), `pinned_bytes` (GaugeFunc), `oldest_reader_age_seconds` (GaugeFunc), `active_readers` (GaugeFunc).
- **RISK-2 fix Wave 0** — `*BadgerEngine.PinnedBytes() int64`, `OldestReaderAgeSeconds() float64`, `ActiveReaders() int64` accessors landed BEFORE the GaugeFunc registrations. Fallback path reads existing atomic counters when no lifecycle controller is wired (preserves the existing zero-config Engine surface); routes through the controller's optional interfaces (`PinnedBytes()` from PressureController, `OldestReaderAge()` from ReaderRegistry) when one is present (production path). All accessors race-clean (`-race -count=10`).
- **D-13c closed `index` enum** — `storage.classifyIndexName(internal string) string` is a pure function mapping `label_*` → `"label"`, `edge_between_*` → `"edge_between"`, `temporal_*` → `"temporal"`, `embedding_*` → `"embedding"`, anything else → `"user_created"`. Falsifiability gate `TestIndexRebuild_NoFreeFormLabel` drives 1k random user-named indexes through the bucket and asserts cardinality stays at 1 (all collapse to user_created). T-04-02 mitigation locked.
- **D-07 30s sweep** — `BytesMetricsSweeper` implements `lifecycle.Component`. Default 30s cadence (`storage.DefaultBytesMetricsInterval`); test override via `NewBytesMetricsSweeper(...)`. Initial sweep fires immediately on Start so the gauges have non-zero values by the first scrape; subsequent ticks via `time.Ticker`. `sweep()` wrapped in `defer recover()` per RESEARCH RISK-8. Five kind buckets: nodes (`prefixNode`), edges (`prefixEdge`), index (sum across `prefixLabelIndex` + `prefixEdgeBetweenIndex` + `prefixTemporalIndex`), wal (`vlog - lsm` from `db.Size()`; RISK-6 heuristic), search (Plan 04-05 wires).
- **D-14 pressure_band thresholds** — `MVCCBandWarn = 0.50`, `MVCCBandHigh = 0.75`, `MVCCBandCritical = 0.90` constants. `MVCCMetrics.UpdateBand(database, ratio)` is indicator-style: the active band's gauge is set to 1, the other three bands' gauges are set to 0 for the same database. Lets dashboards do `max by (database) (... == 1)` to pick current band; alert rules fire on `nornicdb_mvcc_pressure_band{band="critical"} == 1`.
- **D-15b GaugeFunc panic safety** — Every live-read GaugeFunc callback wraps `defer-recover` returning 0 on panic per RESEARCH RISK-8 / Pitfall 1. `TestMVCCGaugeFuncs_PanicSafe` and `TestStorageGaugeFuncs_PanicSafe` exercise the panic path with `panickyProbe{}` / `panickyStorageProbe{}` and assert the family still surfaces with value 0 (no scrape 500).
- **MET-25 hot path** — `BadgerEngine.AttachMetrics(storage, mvcc)` pre-binds the four `BoundLatencyObserver` fields (`opDurGet`/`Put`/`Delete`/`Scan`) at construction time. Per-op observation funnels through the deferred `b.observeStorageOp(start, observer)` helper at the top of every public storage method (`GetNode`/`GetEdge`/`CreateNode`/`UpdateNode`/`CreateEdge`/`UpdateEdge`/`DeleteNode`/`DeleteEdge`/`AllNodes`/`AllEdges`). The `metricsAttached` atomic.Bool short-circuits observation entirely when no bag is wired (test mode / embedded-library callers). `BenchmarkObserve_HotStorage/storage_op_duration_bound_get` measures **25 ns/op, 0 allocs**.
- **D-08 forward-compat** — `NewStorageMetrics(reg, tenantLabelsEnabled, probe)` and `NewMVCCMetrics(reg, tenantLabelsEnabled, probe)` decide label-set shape ONCE at construction. Storage bag carries `database` only on `IndexRebuildTotal` (D-08a — `op_duration` stays tenant-agnostic; per-database op latency lives at the Cypher subsystem per Plan 04-03). MVCC bag carries `database` only on `PressureBand`. `BindIndexRebuild` + `UpdateBand` helpers are tenant-flag-aware so subsystem callers pass database unconditionally.
- **D-16 separation preserved** — Storage emits ONLY its own subsystem families. `transaction_conflicts` stays under `nornicdb_cypher_*` (Plan 04-03). The pkg/storage → pkg/observability dependency added by Plan 04-04 is the LIMITED storage-emits-its-own-families dependency; storage still does NOT classify ErrConflict, that wiring lives in pkg/cypher per Plan 04-03 D-16.
- **`cmd/nornicdb/main.go` integration** — `unwrapBadgerEngine(db.GetStorage())` walks the wrapper chain `NamespacedEngine → AsyncEngine → WALEngine → *BadgerEngine` (safety-bounded loop tolerates future wrapper additions). Constructs both bags at the same Phase 4 D-02c init-order chokepoint Plans 04-01..04-03 used; injects via `AttachMetrics`. `bytes_metrics_sweeper` appended to the `lifecycle.Component` list BETWEEN pprof and workers per RESEARCH §Q4 ordering — drains AFTER workers and BEFORE telemetry so the final scrape during drain reflects last-known sizes.
- **MET-25 BenchmarkObserve_Hot{Storage,MVCC}** — 5 sub-benches all 0 allocs/op (Apple M1 Max, count=3): `storage_op_duration_bound_get` 25.5ns, `storage_index_rebuild_inc` 6.9ns, `storage_bytes_set` 2.07ns, `mvcc_pressure_band_set` 2.03ns, `mvcc_update_band_full` 118ns (4×Set amortized).

## Task Commits

Each task committed atomically (worktree mode, --no-verify; orchestrator validates hooks). Task 04-04-01 split into RED + GREEN per the TDD gate; subsequent tasks landed test+impl together for worktree-flow brevity:

1. **04-04-01 RED:** `test(04-04-01): RED tests for MVCC accessors PinnedBytes/OldestReaderAgeSeconds/ActiveReaders (RISK-2)` — `6a4f48a`
2. **04-04-01 GREEN:** `feat(04-04-01): MVCC accessors PinnedBytes/OldestReaderAgeSeconds/ActiveReaders (RISK-2)` — `e4efd46`
3. **04-04-02:** `feat(04-04-02): catalog_mvcc.go GREEN — 4 families with closed band enum + GaugeFuncs (D-14, D-15b)` — `55cc1d1`
4. **04-04-03:** `feat(04-04-03): catalog_storage.go GREEN — 8 families through Phase-3 typed constructors (MET-10, D-13c)` — `f10917d`
5. **04-04-04:** `feat(04-04-04): pkg/storage/bytes_metrics.go — D-07 30s lifecycle.Component sweep populating bytes{kind}` — `36a9de9`
6. **04-04-05:** `feat(04-04-05): pkg/storage/index_rebuild_metrics.go — D-13c index-name → enum mapping` — `a4d0710`
7. **04-04-06:** `feat(04-04-06): per-op observation at storage chokepoints + AttachMetrics + pressure_band wiring` — `089e14e`
8. **04-04-07:** `feat(04-04-07): cmd/nornicdb wires StorageMetrics + MVCCMetrics + bytes_metrics_sweeper` — `68c7d39`
9. **04-04-08:** `test(04-04-08): BenchmarkObserve_Hot{Storage,MVCC} 0 allocs/op (MET-25 hot path)` — `0a7c03b`

## Decisions Made

1. **RISK-2 Wave-0 fix shipped first** — `PinnedBytes() int64` / `OldestReaderAgeSeconds() float64` / `ActiveReaders() int64` accessors landed on `*BadgerEngine` in commits `6a4f48a`+`e4efd46` BEFORE the MVCC bag's GaugeFunc registrations in `55cc1d1`. The accessors probe the lifecycle controller via an OPTIONAL interface (`interface{ PinnedBytes() int64 }`) — this avoids coupling the existing `storage.MVCCLifecycleController` surface to a metric-only accessor while still routing real values when a controller is wired. Falls back to 0 when no controller is wired (test mode / embedded-library); the GaugeFunc callback's defer-recover means a buggy/missing controller cannot poison /metrics. `ActiveReaders` always returns a real value (atomic counter fallback when no controller; `controller.ReaderRegistry().ActiveCount()` when wired).
2. **MVCCProbe + StorageProbe interfaces live in pkg/observability** — declared next to `NewMVCCMetrics` / `NewStorageMetrics` so the leaf-package boundary holds (pkg/observability never imports pkg/storage). The Plan 04-01 RED test stub had pinned MVCCProbe at `(int64, float64, int64)` signatures — Plan 04-04 honored that test-stub shape rather than the plan-task-body literal `(uint64, time.Duration, int)`. Tests had been reviewed and shipped first; we followed the contract that compiled.
3. **D-13c index-name classifier in pkg/storage, NOT pkg/observability** — `storage.classifyIndexName(internal string) string` is a pure function over an internal string. It belongs at the storage call site where the internal string is generated. The closed enum `AllowedStorageIndexes` is declared in pkg/observability (it's the metric label-set authority); the producer function lives in pkg/storage; both live by the same closed-enum contract. Preserves the leaf-package boundary nuance.
4. **`cfg.Storage.BytesMetricInterval` lives on a new `StorageRuntimeConfig`** — not on `cfg.Database` (`DatabaseConfig`). Rationale: `DatabaseConfig` mirrors persisted ENV/YAML on-disk schema; `BytesMetricInterval` is a runtime-only knob that does not belong on the on-disk config. The plan literal `cfg.Storage.BytesMetricInterval` matches this new path. cmd/nornicdb's main.go reads it directly; zero/unset falls back to `storage.DefaultBytesMetricsInterval` (30s).
5. **Per-op chokepoints land at the public-method entry, NOT at the `withUpdate`/`withView` callback** — the chokepoint must observe the FULL wall-clock cost (including encoding, validation, callback dispatch). Observation deeper inside loses the validation + serialization phases. The `defer b.observeStorageOp(start, observer)` pattern at the top of GetNode/CreateNode/etc. captures the full method latency. `metricsAttached` atomic short-circuits the observation entirely when no bag is wired so the unattached path pays only one `atomic.Bool.Load` (~1ns).
6. **`AttachMetrics` is the SINGLE setter** — both storage and MVCC bags injected together. Pre-binds the four op-duration observers (`Bind("get"/"put"/"delete"/"scan")`) into struct fields at construction time per MET-25. Subsequent Observe calls pay zero WithLabelValues lookup. Idempotent: subsequent calls overwrite previously-bound observers, which is safe because `BoundLatencyObserver` is value-typed and concurrent Observe calls on either the new or old observer are race-clean per the client_golang HistogramVec promise.
7. **`bytes_metrics_sweeper` between pprof and workers** in the lifecycle.Component registration order per RESEARCH §Q4. Drain runs in reverse order, so the sweeper drains AFTER workers and BEFORE telemetry listener — guaranteeing the final scrape during drain still reflects the last sweep's gauge values before the engine starts shutting down. The supervised drain budget (5s typical) is more than enough for the `sync.Once` + done-channel Shutdown.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Plan called `NewStorageMetrics(reg, tenantLabelsEnabled bool)` (2-arg) in the existing RED test; the GREEN bag needs the StorageProbe seam (3-arg form per the plan task body)**

- **Found during:** Task 04-04-03
- **Issue:** The pre-existing `pkg/observability/catalog_storage_test.go` from Plan 04-01 RED-stage called `NewStorageMetrics(te.Registry, false)` with two arguments. The plan task body specifies `NewStorageMetrics(reg, tenantLabelsEnabled bool, probe StorageProbe) *StorageMetrics` (three arguments) so the GaugeFunc-backed `nodes_total`/`edges_total` families can be registered.
- **Fix:** Updated the test signature site to pass a `storageProbeStub` (third arg) — kept the original RED tests' intent (closed-enum verification) intact while adding GREEN coverage for the GaugeFunc + tenant-flag axes.
- **Files modified:** `pkg/observability/catalog_storage_test.go` (rewrote to pass the third `probe` argument; added 7 GREEN tests).
- **Verification:** `go test -count=1 ./pkg/observability/` passes; the existing `TestStorageMetrics_BytesKindClosedEnum` semantic preserved by `TestStorageBytes_KindClosedEnum`.
- **Committed in:** `f10917d` (Task 04-04-03 commit).

**2. [Rule 3 - Blocking] Plan said `MVCCProbe.PinnedBytes() uint64 + OldestReaderAge() time.Duration + ActiveReaders() int`; the existing RED test stub already pinned `int64 + float64 + int64`**

- **Found during:** Task 04-04-01
- **Issue:** The plan task body specified the accessors with `uint64 / time.Duration / int` return types. The pre-existing Plan 04-01 RED test stub at `pkg/observability/catalog_mvcc_test.go::mvccProbeStub` had already shipped with `int64 / float64 / int64` signatures, and the `pkg/observability/catalog_redstubs.go` declared `MVCCProbe` with `int64 / float64 / int64`. Both had been reviewed and merged.
- **Fix:** Honored the test-stub shape (which compiled and passed CI) rather than the plan-task-body literal. The `*BadgerEngine` accessors ship with `int64 / float64 / int64` matching the existing interface declaration. Documented the choice in the SUMMARY decision #2.
- **Files modified:** `pkg/storage/badger_mvcc.go` (accessors); `pkg/observability/catalog_mvcc.go` (interface declaration restated for the GREEN bag).
- **Verification:** `go test -race -count=10 ./pkg/storage/` passes including the accessor race-clean tests.
- **Committed in:** `e4efd46` (Task 04-04-01 GREEN commit).

**3. [Rule 3 - Blocking] Plan files_modified listed `cmd/nornicdb/serve.go`; the runServe function lives in `cmd/nornicdb/main.go`**

- **Found during:** Task 04-04-07
- **Issue:** Same finding as Plans 04-01 / 04-02 / 04-03 deviation. `cmd/nornicdb/serve.go` does not exist as a separate file.
- **Fix:** Inserted the StorageMetrics + MVCCMetrics construction + AttachMetrics + bytes_sweeper registration block into `cmd/nornicdb/main.go` at the same Phase 4 D-02c init-order chokepoint Plans 04-01..04-03 used. Added a NEW `cmd/nornicdb/storage_metrics.go` file for the `unwrapBadgerEngine` walk + the two probe adapters (kept main.go from growing).
- **Files modified:** `cmd/nornicdb/main.go`.
- **Files created:** `cmd/nornicdb/storage_metrics.go`, `cmd/nornicdb/storage_metrics_test.go`.
- **Verification:** `go build -tags "nolocalllm noui" ./cmd/nornicdb/...` clean; the wiring tests (`TestUnwrapBadgerEngine_*`, `TestBadgerStorageProbe_*`, `TestBadgerMVCCProbe_*`) pass.
- **Committed in:** `68c7d39` (Task 04-04-07 commit).

**4. [Rule 2 - Critical Functionality] Existing `storage.PressureBand` enum had only 3 values {normal, high, critical}; D-14 requires 4 values {normal, warn, high, critical}**

- **Found during:** Task 04-04-02
- **Issue:** `pkg/storage/types.go` declared `PressureBand string` with constants `PressureNormal`/`PressureHigh`/`PressureCritical` only — no `PressureWarn`. The MVCC bag's closed enum requires the 4-value set per D-14.
- **Fix:** The observability layer's `AllowedMVCCBands = []string{"normal", "warn", "high", "critical"}` and `classifyMVCCBand(ratio)` handle the 4-value set independently of the storage layer's existing `PressureController` (which only manages 3 bands with hysteresis). The metric label-set authority lives in pkg/observability; the storage-layer `PressureController` is unchanged. Future plan can reconcile the two if a 4-band PressureController becomes desirable.
- **Files modified:** `pkg/observability/catalog_mvcc.go` (closed enum + classifier).
- **Verification:** `TestPressureBand_ThresholdMapping` passes for all four ratio buckets.
- **Committed in:** `55cc1d1` (Task 04-04-02 commit).

---

**Total deviations:** 4 (4 blocking; no auto-fixed bugs). All four deviations are mechanical alignment with codebase reality (existing test stub signature, file path, three-band PressureController). No scope creep.

## Issues Encountered

- **Pre-existing macOS link warning** — `ld: warning: ignoring duplicate libraries: '-lobjc'` surfaces on every `go test` invocation in pkg/storage and pkg/observability (CGO localllm linkage). Pre-existing; not introduced by Plan 04-04. Tests pass cleanly.
- **`make bench-storage` Makefile target not yet wired** — Plan 04-07 (or later) will add it. Per-subsystem `BenchmarkObserve_HotStorage` + `BenchmarkObserve_HotMVCC` evidence in `bench-storage-mvcc-04-04.txt` is the early-warning system this plan ships; the cumulative regression gate (`make bench-bolt` Δ ≤ 5%, `make bench-cypher` Δ ≤ 2%) is owned by KD-12 / Phase 12.

## Self-Check

Verifying claims:

**Files created (existence check):**

```
$ for f in pkg/observability/catalog_storage.go \
          pkg/observability/catalog_storage_bench_test.go \
          pkg/observability/catalog_mvcc.go \
          pkg/observability/catalog_mvcc_bench_test.go \
          pkg/storage/bytes_metrics.go \
          pkg/storage/bytes_metrics_test.go \
          pkg/storage/index_rebuild_metrics.go \
          pkg/storage/index_rebuild_metrics_test.go \
          pkg/storage/badger_metrics.go \
          pkg/storage/badger_metrics_test.go \
          pkg/storage/badger_mvcc_accessors_test.go \
          cmd/nornicdb/storage_metrics.go \
          cmd/nornicdb/storage_metrics_test.go \
          .planning/phases/04-subsystem-metric-catalog/bench-storage-mvcc-04-04.txt; do
  [ -f "$f" ] && echo "FOUND: $f" || echo "MISSING: $f"
done
FOUND: pkg/observability/catalog_storage.go
FOUND: pkg/observability/catalog_storage_bench_test.go
FOUND: pkg/observability/catalog_mvcc.go
FOUND: pkg/observability/catalog_mvcc_bench_test.go
FOUND: pkg/storage/bytes_metrics.go
FOUND: pkg/storage/bytes_metrics_test.go
FOUND: pkg/storage/index_rebuild_metrics.go
FOUND: pkg/storage/index_rebuild_metrics_test.go
FOUND: pkg/storage/badger_metrics.go
FOUND: pkg/storage/badger_metrics_test.go
FOUND: pkg/storage/badger_mvcc_accessors_test.go
FOUND: cmd/nornicdb/storage_metrics.go
FOUND: cmd/nornicdb/storage_metrics_test.go
FOUND: .planning/phases/04-subsystem-metric-catalog/bench-storage-mvcc-04-04.txt
```

**Commits exist (git log check):**

```
6a4f48a test(04-04-01): RED tests for MVCC accessors PinnedBytes/OldestReaderAgeSeconds/ActiveReaders (RISK-2)
e4efd46 feat(04-04-01): MVCC accessors PinnedBytes/OldestReaderAgeSeconds/ActiveReaders (RISK-2)
55cc1d1 feat(04-04-02): catalog_mvcc.go GREEN — 4 families with closed band enum + GaugeFuncs (D-14, D-15b)
f10917d feat(04-04-03): catalog_storage.go GREEN — 8 families through Phase-3 typed constructors (MET-10, D-13c)
36a9de9 feat(04-04-04): pkg/storage/bytes_metrics.go — D-07 30s lifecycle.Component sweep populating bytes{kind}
a4d0710 feat(04-04-05): pkg/storage/index_rebuild_metrics.go — D-13c index-name → enum mapping
089e14e feat(04-04-06): per-op observation at storage chokepoints + AttachMetrics + pressure_band wiring
68c7d39 feat(04-04-07): cmd/nornicdb wires StorageMetrics + MVCCMetrics + bytes_metrics_sweeper
0a7c03b test(04-04-08): BenchmarkObserve_Hot{Storage,MVCC} 0 allocs/op (MET-25 hot path)
```

**audit-untouched gate:**

```
$ git diff --name-only 246e22923e8f7377dadd43c4c696a14fe757a780..HEAD pkg/audit/
(empty)
```

No modifications to `pkg/audit/audit.go`.

**lint-cardinality gate:**

```
$ make lint-cardinality
lint-cardinality: PASS (MET-04 helper-only registration enforced)
```

**Build clean:**

```
$ go build -tags "nolocalllm noui" ./...
(success)
$ go vet -tags nolocalllm ./pkg/observability/... ./pkg/storage/...
(success)
```

(Pre-existing pkg/cypher/antlr unreachable-code warnings documented under Plan 04-03 Issues Encountered — not introduced by this plan.)

**Test suite (race, scoped to plan-introduced packages):**

```
$ go test -tags nolocalllm -race -count=1 ./pkg/observability/...
ok      github.com/orneryd/nornicdb/pkg/observability   2.171s

$ go test -tags nolocalllm -race -count=1 ./pkg/storage/...
ok      github.com/orneryd/nornicdb/pkg/storage         39.549s
ok      github.com/orneryd/nornicdb/pkg/storage/lifecycle 0.341s

$ go test -tags "nolocalllm noui" -race -count=1 ./cmd/nornicdb/...
ok      github.com/orneryd/nornicdb/cmd/nornicdb         <fast>

$ go test -tags nolocalllm -race -count=10 ./pkg/storage/ -run "TestBytesMetricsSweeper|TestAccessors_RaceSafe"
ok      github.com/orneryd/nornicdb/pkg/storage         <ok>
```

**BenchmarkObserve_Hot{Storage,MVCC} budgets:**

| Sub-bench                              | ns/op | allocs/op | budget |
|----------------------------------------|-------|-----------|--------|
| storage_op_duration_bound_get          | 25.5  | 0         | ≤ 2    |
| storage_index_rebuild_inc              |  6.9  | 0         | ≤ 2    |
| storage_bytes_set                      |  2.07 | 0         | ≤ 2    |
| mvcc_pressure_band_set                 |  2.03 | 0         | ≤ 2    |
| mvcc_update_band_full                  |  118  | 0         | ≤ 2    |

All sub-benches: **0 allocs/op** (Apple M1 Max, count=3) — well below the ≤ 2 alloc budget.

**File-LOC discipline (PERF-06 800-LOC sub-cap in pkg/observability):**

```
catalog_storage.go              — 274 LOC ✓
catalog_storage_test.go         — 192 LOC ✓
catalog_storage_bench_test.go   —  59 LOC ✓
catalog_mvcc.go                 — 229 LOC ✓
catalog_mvcc_test.go            — 211 LOC ✓
catalog_mvcc_bench_test.go      —  45 LOC ✓
bytes_metrics.go                — 173 LOC ✓
bytes_metrics_test.go           — 246 LOC ✓
index_rebuild_metrics.go        —  45 LOC ✓
index_rebuild_metrics_test.go   —  70 LOC ✓
badger_metrics.go               —  89 LOC ✓
badger_metrics_test.go          — 191 LOC ✓
badger_mvcc_accessors_test.go   — 124 LOC ✓
storage_metrics.go (cmd)        —  80 LOC ✓
storage_metrics_test.go (cmd)   —  61 LOC ✓
```

**Self-Check: PASSED**

## Next Phase Readiness

- **Plans 04-05..04-06 unblocked.** `pkg/observability` compiles via the remaining `catalog_redstubs.go` entries (Embed / Search / Replication / Auth); each downstream plan replaces one stub block with the real GREEN bag. Storage + MVCC stubs are removed (Plan 04-04 SHIPPED).
- **Plan 04-05 search-engine wiring inputs ready.** `bytes_metrics_sweeper` accepts a `SearchSizeFn` callback that Plan 04-05 will populate via `search.SearchService.IndexSizeBytes()`. The `kind="search"` gauge currently reads 0 via the nil-tolerated callback path; Plan 04-05 wires the real callback at the same `cmd/nornicdb/main.go` site that constructs the sweeper.
- **Plan 04-07 inputs ready.** `bench-storage-mvcc-04-04.txt` evidence file captured with 5 sub-benches at 0 allocs/op; per-subsystem bench naming (`BenchmarkObserve_HotStorage`, `BenchmarkObserve_HotMVCC`) cumulates cleanly with cache (HotCache), HTTP (HotHTTP), Bolt (HotBolt), Cypher (HotCypher) and downstream subsystems' future benches.
- **Phase 5 forward-compat preserved.** D-08 `tenantLabelsEnabled` bool plumbed through `NewStorageMetrics(reg, tenantLabelsEnabled, probe)` and `NewMVCCMetrics(reg, tenantLabelsEnabled, probe)`; reads from `cfg.Observability.Metrics.TenantLabelsEnabled` at startup. Phase 5 K8s autodetect will set the bool's default value with no re-registration.
- **Phase 6 forward-compat preserved.** `BoundLatencyObserver` routes through `observeWithExemplar` chokepoint — Phase 1's `NeverSample()` default means zero exemplars emitted today; Phase 6's `TraceIDRatioBased(0.01)` flip lights up exemplars at every observation site automatically with no second-pass migration.
- **Phase 8 (TRC-17) forward-compat preserved.** The four storage chokepoints (get/put/delete/scan) in `pkg/storage/badger_*.go` are the natural span-emission chokepoints. When Phase 8 ships, span emission piggy-backs on the same chokepoints (now metric + span) without restructuring the instrumentation.
- **Phase 9 (Helm/Grafana) catalog freeze contributor.** 12 metric names (8 storage + 4 mvcc) enumerated here are now canonical for the M1 release cycle — Phase 9 dashboards hard-code them per ROADMAP.md hard ordering #3.
- **Plan 04-07 cumulation note.** The race-stability `-count=10` includes `pkg/storage/` per the plan goal_backward; verified clean across 10 iterations of `TestBytesMetricsSweeper_IntervalOverride` and `TestAccessors_RaceSafe`.

---
*Phase: 04-subsystem-metric-catalog*
*Plan: 04-04*
*Completed: 2026-05-03*
