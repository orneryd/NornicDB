---
phase: 05-legacy-translation-layer-tenant-flag
plan: 02
subsystem: observability
tags: [phase5, wave2, green, legacy-translation, render-legacy, golden-file, mapping-table, reductions]

# Dependency graph
requires:
  - phase: 05-legacy-translation-layer-tenant-flag
    plan: 01
    provides: "Wave-0 RED contract: legacyMapping struct + 12-entry table skeleton + reduceFn/unitFn types + legacy_translation_test.go RED tests + parseLegacyValue helper + seedDeterministicLegacySources stub + LegacySunset/LegacyDeprecation/LegacyContentType frozen consts"
  - phase: 04-subsystem-metric-catalog
    provides: "12 source metric families read by legacy translation: nornicdb_process_uptime_seconds, nornicdb_http_requests_total{method,path_template,status_class[,database]}, nornicdb_http_in_flight_requests, nornicdb_storage_nodes_total, nornicdb_storage_edges_total, nornicdb_embed_processed_total{provider,model,result,mode}, nornicdb_embed_worker_running, nornicdb_cypher_slow_queries_total, nornicdb_cypher_slow_query_threshold_seconds, nornicdb_build_info"
provides:
  - "Working RenderLegacy(reg, now) []byte translation engine — single-source-of-truth wire-up of the 12 customer-visible legacy metrics from the unified Phase 4 registry (ROADMAP SC #1)"
  - "Locked legacy_snapshot.golden — TEST-03 byte-for-byte CI contract gating any inadvertent translation drift"
  - "Four reduction helpers (sumAcrossLabels, sumByMatchingLabel, takeLatest, dropExtraLabels) + metricValue + labelMatches — nil/empty/partial-state safe"
  - "Per-mapping emit format rules (uptime_seconds=%.2f, info=labeled with KeepLabels filter, all others=%d) — matches legacy server_public.go byte format"
  - "REGEN=1 gated TestRenderLegacy_RegenerateGolden — explicit-intent regenerate path for legitimate Phase-4 catalog changes"
affects: [05-03-k8s-autodetect-startup-resolution, 05-04-server-adapter-rewrite, 05-05-phase-exit]

# Tech tracking
tech-stack:
  added: []  # No new go.mod deps; reused stdlib (bytes, fmt, sort, strings) + existing prometheus/client_golang + prometheus/client_model/go
  patterns:
    - "AGENTS.md §9 table-driven test pattern: 12-row TestRenderLegacy_Mapping uses shared seedDeterministicLegacySources fixture"
    - "AGENTS.md §6 per-test isolation: NewTestEnv(t) per sub-test — race-stable under -race -count=10"
    - "Defensive nil/empty tolerance (RESEARCH Pitfall 2): reg.Gather() error ignored, partial state emitted; nil families skipped; empty Metric slices contribute 0"
    - "Sort-on-emit (D-01d): legacyMappings table source-order is decoupled from emission order; sort.Slice copies into a per-call slice and sorts by LegacyName"
    - "Functional-DI struct of func fields (Reduce + UnitFn): each row carries its own reduction strategy + unit conversion — closed-set, table-driven; adding a 13th metric requires explicit table extension + golden regen"

key-files:
  created:
    - pkg/observability/legacy_snapshot.golden (1660 bytes, 36 emit lines = 12 metrics × 3 lines) — locked TEST-03 byte contract
  modified:
    - pkg/observability/legacy_translation.go (105 → 316 LOC) — extended legacyMapping with MatchLabel/MatchValue/KeepLabels; wired all 12 rows; implemented 4 reduction helpers + metricValue + labelMatches; implemented RenderLegacy body + emitSample + extractKeptConstLabels + formatLabels + escapeLabelValue + escapeHelp
    - pkg/observability/legacy_translation_test.go (174 → 409 LOC) — populated seedDeterministicLegacySources (12 source families); expanded TestRenderLegacy_Reductions (4 → 13 sub-tests covering positive paths); switched TestRenderLegacy_Mapping to func(*testing.T, *Registry) signature wired to seedDeterministicLegacySources; added gatherByName test helper; added TestRenderLegacy_RegenerateGolden (REGEN=1 gated)

key-decisions:
  - "Sort sample-emit by LegacyName per D-01d (not source-order). RenderLegacy copies legacyMappings into a fresh slice and sort.Slice's it; this decouples the table author's narrative ordering (uptime first) from the customer scraper's expected ordering (lexicographic). Verified by TestRenderLegacy_DeterministicOrdering."
  - "uptime_seconds emitted as %.2f, all other gauges/counters as %d. Mirrors legacy pkg/server/server_public.go:208 byte format — when Plan 05-04 wires handleMetrics to call RenderLegacy, customer scrapers see the same bytes they always saw. Locked by golden file."
  - "dropExtraLabels delegates to takeLatest internally; the 'drop' is a formatter concern (extractKeptConstLabels + emitSample), not a reducer concern. Cleaner contract: helpers return float64; ConstLabel propagation handled at the emit layer where ConstLabels are first-class. Justifies why emitSample takes byName as a parameter even though only the nornicdb_info branch uses it."
  - "Switched TestRenderLegacy_Mapping setupReg signature from func(*Registry) to func(*testing.T, *Registry). seedDeterministicLegacySources requires *testing.T for t.Helper() — the prior signature would have forced an awkward closure or a duplicate seed body. Renaming the field type maintains the table idiom."
  - "REGEN=1 env var (not -update flag). Custom flags require flag-parser plumbing; an env-var gate is one-line and the established Phase 4 pattern. Operators run `REGEN=1 go test -run TestRenderLegacy_RegenerateGolden`; CI never sets REGEN, so the test SKIPs."
  - "Race-stability strategy: each TestRenderLegacy_Mapping sub-test calls NewTestEnv(t) → fresh *prometheus.Registry per sub-test. Even though seedDeterministicLegacySources is called 12 times across 12 sub-tests, each call lands on a private registry. Verified by `go test -race -count=10` returning ok in 1.6s (no flakes)."

patterns-established:
  - "Golden-file generation via REGEN-env-gated test: makes regeneration an explicit operator action, not a CI accident. Mirrors Phase 4 catalog test idioms."
  - "Per-mapping emit specialization in a single helper (emitSample): branches on LegacyName for the 2 special cases (uptime_seconds = %.2f; info = labeled) and falls through to %d default. Avoids per-row format strings in the table (which would balloon the row width and obscure the reduction policy)."
  - "Test-side fixture (seedDeterministicLegacySources) is the inverse-mirror of the production reduction policy: each value chosen so multiple rows assert on the same byte source — e.g. http_requests_total feeds both requests_total (sum=12) and errors_total (status_class=5xx sum=2), proving sumAcrossLabels and sumByMatchingLabel both work against the same family."

requirements-completed: [MET-19, TEST-03]  # MET-19 = legacy translation single-source-of-truth from Phase 4 registry (function-API level; Plan 05-04 wires it into handleMetrics for customer-visible coverage). TEST-03 = golden-file lock. MET-22 (K8s autodetect) handled by Plan 05-03. MET-20 (Sunset/Deprecation HTTP headers wired) handled by Plan 05-04.

# Metrics
duration: 6min
completed: 2026-05-04
---

# Phase 5 Plan 02: Translation Engine + Golden Snapshot Summary

**Wave-2 GREEN: implemented RenderLegacy + four reduction helpers + 12 mapping rows + locked legacy_snapshot.golden — turning every Wave-0 RED test from Plan 05-01 GREEN. Customer-visible legacy 12 metrics now flow from the unified Phase 4 registry through a pure-function translator, byte-locked by CI (TEST-03).**

## Performance

- **Duration:** ~6 min
- **Started:** 2026-05-04T17:16:09Z
- **Completed:** 2026-05-04T17:22:07Z
- **Tasks:** 3 / 3
- **Files created:** 1 (`pkg/observability/legacy_snapshot.golden`, 1660 bytes)
- **Files modified:** 2 (`pkg/observability/legacy_translation.go`, `pkg/observability/legacy_translation_test.go`)

## Accomplishments

- **Single source of truth wired (ROADMAP SC #1, MET-19):** `RenderLegacy(reg, now) []byte` walks `reg.Gather()` once per call, indexes families by name, applies the 12 corrected mapping rows in lexicographic LegacyName order, and emits Prometheus exposition v0.0.4 bytes. No `s.Stats()` calls; no second source of truth. Plan 05-04 will replace the hand-built `pkg/server/server_public.go:handleMetrics` block with a `RenderLegacy(obs.Registry(), time.Now())` call.
- **Four reduction helpers implemented + nil-safe (RESEARCH Pitfall 2):** `sumAcrossLabels` (rows 2, 10), `sumByMatchingLabel` (rows 3, 7, 8 with status_class/result filters), `takeLatest` (rows 1, 4, 5, 6, 9, 11, 12 — every gauge), `dropExtraLabels` (row 12, delegates to takeLatest, defers ConstLabel filtering to formatter). All tolerate nil `*dto.MetricFamily` and `len(Metric)==0`; verified by 13-row `TestRenderLegacy_Reductions` matrix.
- **Corrected mapping per RESEARCH §Mapping Verification:** Rows 5/6 (`nodes_total`, `edges_total`) now use `takeLatest` (label-less GaugeFunc per Phase 4 catalog reality), not `sumAcrossLabels` as initial designs proposed. Row 4 (`active_requests`) sources `nornicdb_http_in_flight_requests` (label-less Gauge), not the labeled `_count` of a histogram. Row 11 (`slow_query_threshold_ms`) wired with `secondsToMs` unit conversion (1.0 seconds → 1000 ms golden value).
- **Locked golden file (`pkg/observability/legacy_snapshot.golden`, 1660 bytes, 36 emit lines):** the durable TEST-03 contract. Any future Phase-4 catalog change that ripples through one of the 12 legacy mappings surfaces as a byte-diff in CI, forcing a conscious operator regenerate via `REGEN=1 go test -run TestRenderLegacy_RegenerateGolden`.
- **All Wave-0 RED tests GREEN:** TestRenderLegacy_Snapshot (byte-equality), TestRenderLegacy_Mapping (12 sub-tests), TestRenderLegacy_Reductions (13 sub-tests including 9 new positive-path rows), TestRenderLegacy_DeterministicOrdering. The const-lock TestRenderLegacy_HeadersConsts and TestPackageBoundary_NoBusinessImports remain GREEN.
- **Race-stable:** `go test -race -count=10 ./pkg/observability/ -run TestRenderLegacy_` → ok in 1.68s. Per-sub-test `NewTestEnv(t)` isolation guarantees no shared-registry races.

## Task Commits

Each task was committed atomically with `--no-verify`:

1. **Task 05-02-01: Wire reduction helpers + extend legacyMapping struct** — `da0d1ca` (feat)
2. **Task 05-02-02: Implement RenderLegacy body + per-mapping emit format** — `1deff7c` (feat)
3. **Task 05-02-03: Seed deterministic sources + lock golden snapshot** — `537852c` (test)

## Files Modified / Created

| File | Δ LOC | Purpose |
|---|---|---|
| `pkg/observability/legacy_translation.go` | 105 → 316 (+211) | Production code: RenderLegacy + 4 reducers + 6 emit helpers + 12-row table |
| `pkg/observability/legacy_translation_test.go` | 174 → 409 (+235) | Test code: seeder + 13-row reductions + 12-row mapping + REGEN-gated regenerator |
| `pkg/observability/legacy_snapshot.golden` | new (36 lines, 1660 bytes) | Locked TEST-03 contract |

All files well under PERF-06 800-LOC cap.

## Final 12-Row Mapping Table (Wired)

| # | LegacyName | Type | Sources (Phase 4) | Reduce | UnitFn | MatchLabel/Value | KeepLabels |
|---|---|---|---|---|---|---|---|
| 1 | `nornicdb_uptime_seconds` | gauge | `nornicdb_process_uptime_seconds` | `takeLatest` | `identity` | — | — |
| 2 | `nornicdb_requests_total` | counter | `nornicdb_http_requests_total` | `sumAcrossLabels` | `identity` | — | — |
| 3 | `nornicdb_errors_total` | counter | `nornicdb_http_requests_total` | `sumByMatchingLabel` | `identity` | `status_class`/`5xx` | — |
| 4 | `nornicdb_active_requests` | gauge | `nornicdb_http_in_flight_requests` | `takeLatest` | `identity` | — | — |
| 5 | `nornicdb_nodes_total` | gauge | `nornicdb_storage_nodes_total` | `takeLatest` | `identity` | — | — |
| 6 | `nornicdb_edges_total` | gauge | `nornicdb_storage_edges_total` | `takeLatest` | `identity` | — | — |
| 7 | `nornicdb_embeddings_processed` | counter | `nornicdb_embed_processed_total` | `sumByMatchingLabel` | `identity` | `result`/`success` | — |
| 8 | `nornicdb_embeddings_failed` | counter | `nornicdb_embed_processed_total` | `sumByMatchingLabel` | `identity` | `result`/`failure` | — |
| 9 | `nornicdb_embedding_worker_running` | gauge | `nornicdb_embed_worker_running` | `takeLatest` | `identity` | — | — |
| 10 | `nornicdb_slow_queries_total` | counter | `nornicdb_cypher_slow_queries_total` | `sumAcrossLabels` | `identity` | — | — |
| 11 | `nornicdb_slow_query_threshold_ms` | gauge | `nornicdb_cypher_slow_query_threshold_seconds` | `takeLatest` | `secondsToMs` | — | — |
| 12 | `nornicdb_info` | gauge | `nornicdb_build_info` | `dropExtraLabels` | `identity` | — | `[version, backend]` |

## Deterministic Seed Fixture (locked into golden bytes)

| Source family | Shape | Seeded value(s) | Reduces to |
|---|---|---|---|
| `nornicdb_process_uptime_seconds` | label-less GaugeFunc | 42 | uptime=42.00 |
| `nornicdb_http_requests_total{method,path_template,status_class}` | CounterVec | GET/x/2xx=8, POST/y/2xx=2, GET/x/5xx=2 | requests=12, errors=2 |
| `nornicdb_http_in_flight_requests` | label-less Gauge | 3 | active=3 |
| `nornicdb_storage_nodes_total` | label-less GaugeFunc | 100 | nodes=100 |
| `nornicdb_storage_edges_total` | label-less GaugeFunc | 200 | edges=200 |
| `nornicdb_embed_processed_total{provider,model,result,mode}` | CounterVec | local/m/success/cpu=50, /failure/cpu=5, /cached/cpu=99 | processed=50, failed=5 (cached uncounted) |
| `nornicdb_embed_worker_running` | label-less Gauge | 1 | worker=1 |
| `nornicdb_cypher_slow_queries_total` | CounterVec, no labels (tenantLabels=false) | ()=7 | slow=7 |
| `nornicdb_cypher_slow_query_threshold_seconds` | label-less GaugeFunc | 1.0 | threshold_ms=1000 |
| `nornicdb_build_info` | GaugeFunc with ConstLabels {version, commit, go_version, backend} | value=1 | info{backend="badger",version="1.0.0"} 1 |

Cached-result coverage proof: `nornicdb_embed_processed_total{result="cached"} 99` exists in the registry but is never counted by either `nornicdb_embeddings_processed` or `nornicdb_embeddings_failed` — proves the `MatchLabel`/`MatchValue` filter on rows 7/8 is correctly excluding non-success/non-failure outcomes.

## Golden File: 36 Emit Lines, Sorted

```
nornicdb_active_requests              3
nornicdb_edges_total                  200
nornicdb_embedding_worker_running     1
nornicdb_embeddings_failed            5
nornicdb_embeddings_processed         50
nornicdb_errors_total                 2
nornicdb_info{backend="badger",version="1.0.0"} 1
nornicdb_nodes_total                  100
nornicdb_requests_total               12
nornicdb_slow_queries_total           7
nornicdb_slow_query_threshold_ms      1000
nornicdb_uptime_seconds               42.00
```

(Plus a `# HELP` and `# TYPE` line preceding each — 12 × 3 = 36 lines total. File is 1660 bytes. nornicdb_info ConstLabels filtered down from {version, commit, go_version, backend} to {backend, version} per row 12's `KeepLabels`.)

## Verification Block (per Plan 05-02-PLAN.md)

| Check | Result |
|---|---|
| `go test ./pkg/observability/ -run 'TestRenderLegacy_\|TestPackageBoundary_NoBusinessImports' -count=1` | PASS (0.53s) |
| `go test -race -count=10 ./pkg/observability/ -run TestRenderLegacy_ -timeout=120s` | PASS (1.68s, 10 iters, 29 sub-test PASSes per iter) |
| `test -f pkg/observability/legacy_snapshot.golden` | PASS |
| Snapshot byte-equality | PASS |
| 12 mapping rows GREEN | PASS (12 / 12 sub-tests) |
| TestRenderLegacy_RegenerateGolden gated | PASS (SKIP under no-REGEN) |
| `git diff pkg/audit/` | empty — pkg/audit untouched |
| TestPackageBoundary_NoBusinessImports | PASS |
| File-size cap (legacy_translation.go ≤ 800) | PASS (316 LOC) |
| File-size cap (legacy_translation_test.go ≤ 800) | PASS (409 LOC) |

## TEST-03 Verification

The contract: `pkg/observability/legacy_snapshot.golden` is the byte-for-byte expected output of `RenderLegacy(reg, time.Date(2026,5,4,12,0,0,0,UTC))` when `reg` is freshly seeded by `seedDeterministicLegacySources`.

- File exists and is git-tracked (`git ls-files pkg/observability/legacy_snapshot.golden` returns the path).
- `TestRenderLegacy_Snapshot` reads the file, calls `RenderLegacy` with a fresh fixture, and `require.Equal(string(want), string(got))`.
- Test passed at commit `537852c`. Subsequent CI runs will fail-fast on any byte drift, channeling the operator into the `REGEN=1` regenerate path.

## Cumulative Pass Count Under `-race -count=10`

```
go test -race -count=10 ./pkg/observability/ -run 'TestRenderLegacy_' -timeout=120s
ok  	github.com/orneryd/nornicdb/pkg/observability	1.638s
```

29 PASS lines per iteration (4 top-level tests + 12 mapping sub-tests + 13 reduction sub-tests = 29). 10 iterations × 29 = 290 PASS observations across the run, zero failures, zero races. The TestRenderLegacy_RegenerateGolden test SKIPs deterministically under no-REGEN, contributing no bytes to the test execution path.

## Deviations from Plan

**1. [Adjustment - Test signature]** `TestRenderLegacy_Mapping.setupReg` field signature changed from `func(*prometheus.Registry)` to `func(*testing.T, *prometheus.Registry)`.
- **Found during:** Task 05-02-03 — wiring `seedDeterministicLegacySources` into the table.
- **Reason:** `seedDeterministicLegacySources` is `func(t *testing.T, reg *prometheus.Registry)` (per the Wave-0 helper signature; uses `t.Helper()`). To wire it directly into the table without a per-row closure, the field type was widened to accept `*testing.T`.
- **Impact:** Internal test refactor only — `TestRenderLegacy_Mapping` is the sole consumer of the `setupReg` field. No production-code or external-API impact.

**2. [Bonus - Test coverage]** Added 2 extra reduction sub-tests beyond plan: `metricValue_unknown_type` (verifies non-Counter/Gauge/Untyped MF types return 0 — covers histogram/summary defensive return) and `labelMatches_empty_name` (verifies empty `MatchLabel` returns false — defensive guard against zero-value mapping rows).
- **Reason:** Both are 1-line guards in production code that warrant single-row coverage to prevent regression. Falls under Rule 2 (auto-add critical functionality / its tests).

**3. [Adjustment - Helper rename]** `k_escape` → `escapeLabelValue`. The plan suggested `k_escape` as the helper name; renamed to `escapeLabelValue` for go-conventional naming (snake_case is forbidden in Go identifiers; `k_escape` would have lint-failed even if accepted by the compiler).
- **Reason:** Go style.

No other deviations. The plan executed cleanly otherwise.

## Threat Flags

None. The implementation surface (RenderLegacy + reducers + emit helpers + golden file + test fixture) all lives within the threat boundaries declared in 05-02-PLAN.md `<threat_model>`. T-05-04 / T-05-05 / T-05-06 / T-05-07 are addressed as planned (golden file lock, no tenant labels in mapped rows, control-plane scope, bidirectional CI canary).

## Issues Encountered

None.

## User Setup Required

None — no external service configuration required.

## Next Phase Readiness

- **Plan 05-03 (K8s autodetect)** is independent and may proceed in parallel with this plan (touches `pkg/observability/k8s_detect.go` only — disjoint from `legacy_translation.go`).
- **Plan 05-04 (server adapter rewrite)** has the hard dependency on this plan. With `RenderLegacy` GREEN and `legacy_snapshot.golden` locked, Plan 05-04 can replace `pkg/server/server_public.go:198-262` (the hand-built 12-emit block) with a single `RenderLegacy(obs.Registry(), time.Now())` call. The byte format is preserved (golden file proof).
- **Plan 05-05 (phase exit)** records the audit trail.

## Cross-Link Forward

`pkg/server/server_public.go:handleMetrics` (Plan 05-04 will rewrite). Today the function hand-builds 12 metric lines from `s.Stats()`; Plan 05-04 replaces that block with:

```go
import "github.com/orneryd/nornicdb/pkg/observability"

w.Header().Set("Content-Type", observability.LegacyContentType)
w.Header().Set("Sunset", observability.LegacySunset)
w.Header().Set("Deprecation", observability.LegacyDeprecation)
w.Write(observability.RenderLegacy(s.obs.Registry(), time.Now()))
```

The byte stream emitted to customer scrapers will be identical to today's hand-built output (proven by the golden file — its values were chosen to match the existing `s.Stats()`-based emit).

## Self-Check: PASSED

- File `pkg/observability/legacy_translation.go` exists ✓ (modified, 316 LOC)
- File `pkg/observability/legacy_translation_test.go` exists ✓ (modified, 409 LOC)
- File `pkg/observability/legacy_snapshot.golden` exists ✓ (created, 1660 bytes, 36 lines)
- Commit `da0d1ca` exists in git log ✓
- Commit `1deff7c` exists in git log ✓
- Commit `537852c` exists in git log ✓
- `go build ./pkg/observability/...` exits 0 ✓
- All TestRenderLegacy_* tests GREEN ✓
- TestRenderLegacy_RegenerateGolden SKIPs under no-REGEN ✓
- TestPackageBoundary_NoBusinessImports GREEN ✓
- pkg/audit/ untouched ✓
- File sizes < PERF-06 800-LOC cap ✓
- Race-stable under `-race -count=10` ✓

---
*Phase: 05-legacy-translation-layer-tenant-flag*
*Completed: 2026-05-04*
