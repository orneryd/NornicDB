---
phase: 05-legacy-translation-layer-tenant-flag
plan: 01
subsystem: observability
tags: [phase5, wave0, red, legacy-translation, k8s-detect, scaffolding, prometheus, golden-file, autodetect]

# Dependency graph
requires:
  - phase: 04-subsystem-metric-catalog
    provides: "Phase 4 catalog freeze (12 source metric families); D-08 forward-compat plumbing of cfg.Observability.Metrics.TenantLabelsEnabled through every tenant-tagged bag constructor"
  - phase: 01-observability-foundation-skeleton
    provides: "pkg/observability leaf-package boundary (boundary_test.go), TestEnv per-test isolated registry, AGENTS.md §4 functional-DI patterns (CheckFunc analog), config-block style"
provides:
  - "Locked public-API surface for legacy translation layer: LegacySunset, LegacyDeprecation, LegacyContentType consts + RenderLegacy signature + 12-entry legacyMappings table (Name+Help+Type+Sources only)"
  - "Locked public-API surface for K8s autodetect: k8sProbe functional-DI struct + 6 Reason* consts + DefaultK8sProbe / Detect / ResolveTenantLabels signatures"
  - "Wave-0 RED tests holding the contract for Plans 05-02 / 05-03 to fill"
affects: [05-02-translation-engine, 05-03-k8s-autodetect-startup-resolution, 05-04-server-adapter-rewrite, 05-05-phase-exit]

# Tech tracking
tech-stack:
  added: []  # No new go.mod deps; reused stdlib + prometheus/client_golang + prometheus/client_model/go
  patterns:
    - "AGENTS.md §4 functional-DI struct of func fields applied to k8sProbe + legacyMapping (Reduce/UnitFn typed fields)"
    - "AGENTS.md §9 table-driven test pattern applied to TestRenderLegacy_Mapping (12 rows) + TestDetectK8s_Signals (6-row matrix)"
    - "Nyquist Wave-0 RED contract: locked public-API surface via test references before implementation bodies exist"
    - "In-package test pattern (package observability) for unexported-symbol references — mirrors Phase 4 catalog tests"

key-files:
  created:
    - pkg/observability/legacy_translation.go (105 LOC) — Wave-0 skeleton with consts, types, 12-entry table, helper stubs
    - pkg/observability/k8s_detect.go (44 LOC) — Wave-0 skeleton with k8sProbe + Detect + ResolveTenantLabels stubs
    - pkg/observability/legacy_translation_test.go (174 LOC) — 5 test functions + parseLegacyValue + seedDeterministicLegacySources helpers
    - pkg/observability/k8s_detect_test.go (118 LOC) — 3 test functions + fakeFI + stubProbe helpers
  modified: []

key-decisions:
  - "Wave-0 RED first (D-05a): mirrors Phase 1/2/3/4 cadence — failing tests + minimal skeletons before any production logic. Plans 05-02 / 05-03 implement against locked test contracts with no design freedom on signatures."
  - "Skeleton imports minimized: legacy_translation.go imports only time + dto + prometheus (Plan 05-02 adds bytes/sort/fmt when needed). k8s_detect.go imports only stdlib os (Plan 05-03 adds strings for TrimSpace). Keeps the Wave-0 diff small + boundary_test.go GREEN."
  - "identity + secondsToMs unitFn helpers landed final (not stubbed): trivial pure functions; deferring would gain nothing. Reduction helpers (sumAcrossLabels, sumByMatchingLabel, takeLatest, dropExtraLabels) remain stubs returning 0 since their full implementations need the registry-walk shape from Plan 05-02."
  - "Tests in package observability (in-package, NOT _test package): enables reference to unexported symbols (legacyMappings, k8sProbe, sumAcrossLabels) per the established Phase 4 catalog test pattern. Required for the Wave-0 contract surface."
  - "parseLegacyValue test helper landed in this plan (Claude's Discretion per Plan 05-01 output spec): Plan 05-02 will reuse it from the same _test.go file — colocating it now avoids a redundant TODO marker."

patterns-established:
  - "Wave-0 RED skeleton: production file declares types + signatures + consts, function bodies return zero values; test files reference all symbols Plans 05-N will implement; one const-lock test (TestRenderLegacy_HeadersConsts) is GREEN immediately to enforce customer-API contract bytes."
  - "12-entry static legacyMappings table: closed-set enumeration mirrors Phase 4 AllowedStorageBytesKinds = []string{...} pattern. Adding a 13th entry requires explicit review + golden-file regenerate."
  - "Closed-set Reason* string consts: enumeration of every value the resolution log line may emit, allowing operators to grep deterministically (D-02b)."

requirements-completed: []  # MET-19, MET-20, MET-22, TEST-03 are SCAFFOLDED (RED) — turned GREEN by Plans 05-02/05-03/05-04. Per validation plan, this Wave-0 plan does not COMPLETE any requirement.

# Metrics
duration: 18min
completed: 2026-05-04
---

# Phase 5 Plan 01: Wave-0 RED Scaffolding Summary

**Locked public-API surface for legacy translation layer (12-entry mapping table, Sunset/Deprecation/ContentType consts, RenderLegacy signature) and K8s autodetect (k8sProbe functional-DI, 6 Reason consts) via failing tests + minimal skeletons — Plans 05-02 / 05-03 implement against frozen contracts.**

## Performance

- **Duration:** ~18 min
- **Started:** 2026-05-04T16:55:00Z (approx)
- **Completed:** 2026-05-04T17:11:00Z
- **Tasks:** 2 / 2
- **Files created:** 4 (2 production skeletons + 2 test files)
- **Files modified:** 0

## Accomplishments

- **Public-API surface frozen for Phase 5 deliverable #1 (legacy translation):** LegacySunset = "Fri, 31 Dec 2027 23:59:59 GMT", LegacyDeprecation = "true", LegacyContentType = "text/plain; version=0.0.4; charset=utf-8" — locked via TestRenderLegacy_HeadersConsts (GREEN immediately, will fail CI on any edit).
- **12-entry legacyMappings table declared** with LegacyName + LegacyHelp + LegacyType + Sources columns (Reduce + UnitFn left nil for Plan 05-02 to fill). Closed set per D-01a.
- **Public-API surface frozen for Phase 5 deliverable #2 (K8s autodetect):** k8sProbe{Getenv, StatFile} functional-DI struct + DefaultK8sProbe / Detect / ResolveTenantLabels signatures + 6 Reason* enum consts.
- **Nyquist Wave-0 RED state achieved:** TestRenderLegacy_HeadersConsts + TestPackageBoundary_NoBusinessImports GREEN; TestRenderLegacy_Snapshot / TestRenderLegacy_Mapping (12 sub-tests) / TestRenderLegacy_DeterministicOrdering / TestDetectK8s_Signals (6 sub-tests) / TestResolveTenantLabels_Precedence (4 sub-tests) / TestK8sProbe_DefaultProbeIsLive all FAIL — Plans 05-02 / 05-03 turn them GREEN.
- **Leaf-package boundary preserved (OBS-01):** boundary_test.go still passes — no forbidden imports added.

## Task Commits

Each task was committed atomically:

1. **Task 05-01-W0a: Wave-0 production skeletons** — `65f8771` (feat)
2. **Task 05-01-W0b: Wave-0 RED tests** — `894a9d5` (test)

## Files Created

- `pkg/observability/legacy_translation.go` (105 LOC) — Phase 5 legacy translation skeleton: 3 const literals, reduceFn / unitFn types, legacyMapping struct, 12-entry legacyMappings table, 4 reduction-helper stubs (sumAcrossLabels, sumByMatchingLabel, takeLatest, dropExtraLabels), 2 unitFn final implementations (identity, secondsToMs), RenderLegacy stub.
- `pkg/observability/k8s_detect.go` (44 LOC) — Phase 5 K8s autodetect skeleton: 6 Reason* string consts, k8sProbe struct (Getenv + StatFile fields), DefaultK8sProbe / Detect / ResolveTenantLabels stubs.
- `pkg/observability/legacy_translation_test.go` (174 LOC) — 5 test functions: TestRenderLegacy_HeadersConsts (GREEN const-lock), TestRenderLegacy_Snapshot (RED, golden missing + nil body), TestRenderLegacy_Mapping (RED, 12-row table), TestRenderLegacy_Reductions (4 nil-family rows accidentally GREEN), TestRenderLegacy_DeterministicOrdering (RED, sort check). Plus parseLegacyValue + seedDeterministicLegacySources test helpers.
- `pkg/observability/k8s_detect_test.go` (118 LOC) — 3 test functions: TestDetectK8s_Signals (RED, 6-row matrix), TestResolveTenantLabels_Precedence (RED, 4-row precedence), TestK8sProbe_DefaultProbeIsLive (RED, zero-value stub). Plus fakeFI os.FileInfo stub + stubProbe helper.

## The 12 Legacy Mappings Declared

| # | LegacyName | Type | Sources |
|---|---|---|---|
| 1 | `nornicdb_uptime_seconds` | gauge | `nornicdb_process_uptime_seconds` |
| 2 | `nornicdb_requests_total` | counter | `nornicdb_http_requests_total` |
| 3 | `nornicdb_errors_total` | counter | `nornicdb_http_requests_total` |
| 4 | `nornicdb_active_requests` | gauge | `nornicdb_http_in_flight_requests` |
| 5 | `nornicdb_nodes_total` | gauge | `nornicdb_storage_nodes_total` |
| 6 | `nornicdb_edges_total` | gauge | `nornicdb_storage_edges_total` |
| 7 | `nornicdb_embeddings_processed` | counter | `nornicdb_embed_processed_total` |
| 8 | `nornicdb_embeddings_failed` | counter | `nornicdb_embed_processed_total` |
| 9 | `nornicdb_embedding_worker_running` | gauge | `nornicdb_embed_worker_running` |
| 10 | `nornicdb_slow_queries_total` | counter | `nornicdb_cypher_slow_queries_total` |
| 11 | `nornicdb_slow_query_threshold_ms` | gauge | `nornicdb_cypher_slow_query_threshold_seconds` |
| 12 | `nornicdb_info` | gauge | `nornicdb_build_info` |

Reduce / UnitFn columns intentionally NOT set in Wave-0 (left nil) — Plan 05-02 wires:
- Rows 1, 4, 5, 6, 9, 11 → `takeLatest` / `identity` (or `secondsToMs` for row 11)
- Rows 2, 7, 10 → `sumAcrossLabels` / `identity`
- Rows 3, 8 → `sumByMatchingLabel` / `identity` (status_class="5xx" / result="failure")
- Row 12 → `dropExtraLabels` / `identity` with ConstLabels = {version, backend}

## Reason* Const Values Declared (k8s_detect.go)

| Const | Value |
|---|---|
| `ReasonExplicitYAML` | `"explicit_yaml"` |
| `ReasonK8sDetected` | `"k8s_detected"` |
| `ReasonServiceHostAbsent` | `"not_k8s_service_host_absent"` |
| `ReasonTokenFileAbsent` | `"not_k8s_token_file_absent"` |
| `ReasonTokenFileEmpty` | `"not_k8s_token_file_empty"` |
| `ReasonTokenStatError` | `"not_k8s_token_stat_error"` |

## Test Counts

| Test | State | Reason |
|---|---|---|
| `TestRenderLegacy_HeadersConsts` | GREEN | Const-lock — already correct |
| `TestPackageBoundary_NoBusinessImports` | GREEN | Boundary preserved |
| `TestRenderLegacy_Snapshot` | RED | Golden file missing + RenderLegacy returns nil |
| `TestRenderLegacy_Mapping` (12 sub-tests) | RED | RenderLegacy returns nil → parseLegacyValue can't find metrics |
| `TestRenderLegacy_Reductions` (4 nil-family sub-tests) | accidentally GREEN | Helpers return 0 for empty input — Plan 05-02 adds positive-value rows that go RED until helpers implemented |
| `TestRenderLegacy_DeterministicOrdering` | RED | RenderLegacy returns nil → no `# TYPE` lines to sort |
| `TestDetectK8s_Signals` (6 sub-tests) | RED | Detect returns (false, "") |
| `TestResolveTenantLabels_Precedence` (4 sub-tests) | RED | ResolveTenantLabels returns (false, "") |
| `TestK8sProbe_DefaultProbeIsLive` | RED | DefaultK8sProbe returns zero-value struct |

Plan 05-02 turns the 4 + 12 + 1 + 4 legacy-translation tests GREEN by implementing reduction helpers, the legacyMappings table function fields, RenderLegacy body, and generating `legacy_snapshot.golden`.

Plan 05-03 turns the 6 + 4 + 1 K8s tests GREEN by implementing Detect, ResolveTenantLabels, and DefaultK8sProbe live wiring.

## Boundary / Coverage / File-size Confirmation

- `go test ./pkg/observability/ -run TestPackageBoundary_NoBusinessImports` → PASS (no forbidden subsystem imports)
- File sizes well under 800-LOC PERF-06 cap:
  - `legacy_translation.go`: 105 LOC
  - `k8s_detect.go`: 44 LOC
  - `legacy_translation_test.go`: 174 LOC
  - `k8s_detect_test.go`: 118 LOC
- `pkg/audit/audit.go` untouched (carry-forward gate from Phase 1+).

## VALIDATION.md Task-Row IDs (RED → GREEN handoff)

| Task ID | Plan delivers | Wave-0 state | Plan that turns GREEN |
|---|---|---|---|
| T-05-01-W0a | Production skeletons compile | DONE (GREEN — compiles) | n/a |
| T-05-01-W0b | RED test contract | DONE (RED tests run) | 05-02 + 05-03 |
| MET-19 (legacy translation single-source-of-truth) | RenderLegacy reads from registry | scaffolded (RED) | 05-02 |
| MET-20 (Sunset/Deprecation headers) | Const literals declared | scaffolded (consts GREEN, handler not wired) | 05-04 (handler rewrite) |
| MET-22 (K8s autodetect) | k8sProbe + Detect signature | scaffolded (RED) | 05-03 |
| TEST-03 (golden-file lock) | Test references golden file | scaffolded (RED — file missing) | 05-02 |

## Decisions Made

- **Skeleton imports minimized.** legacy_translation.go imports only `time` + `dto` + `prometheus` (defers `bytes`/`sort` to Plan 05-02 when RenderLegacy needs them). k8s_detect.go imports only stdlib `os` (defers `strings` to Plan 05-03 for TrimSpace). Keeps the diff small and avoids unused-import compile errors at the Wave-0 boundary.
- **identity + secondsToMs unitFn helpers landed final.** Trivial pure functions; deferring would force a dummy edit in Plan 05-02. Left as final implementations now.
- **`parseLegacyValue` test helper landed in this plan** (Claude's Discretion per Plan 05-01 output spec). It compiles cleanly and Plan 05-02 reuses it directly without redundant TODO markers.
- **Tests are in `package observability`** (in-package, not `_test`): enables reference to unexported symbols (`legacyMappings`, `k8sProbe`, `sumAcrossLabels`, etc.) per Phase 4 catalog test convention. Required for Wave-0 contract locking.

## Deviations from Plan

None — plan executed exactly as written. The plan's `<action>` block specified the exact file contents, and the actual files match those specifications byte-for-byte (with the addition that the import order in `k8s_detect_test.go` was sorted by gofmt convention; no semantic change). One minor: `stubProbe` helper accepts a `reason` parameter that it currently ignores (per the helper's documented Wave-0 strategy of using env-empty for the disabled cases) — Plan 05-03 may evolve the helper, no action needed in Wave-0.

## Issues Encountered

None.

## User Setup Required

None — no external service configuration required.

## Next Phase Readiness

- **Plans 05-02 and 05-03 may parallelize** (D-05a). They touch disjoint files (`legacy_translation.go` vs `k8s_detect.go`) and have independent test suites. Only Plan 05-04 has a hard dependency on Plan 05-02's golden file output.
- **Plan 05-02** picks up: implement RenderLegacy body, fill Reduce/UnitFn function fields in 12-entry legacyMappings table, implement 4 reduction helpers, implement seedDeterministicLegacySources test helper, generate `pkg/observability/legacy_snapshot.golden` (single one-shot test invocation, then commit). All RED legacy-translation tests GREEN at Plan 05-02 exit.
- **Plan 05-03** picks up: implement Detect (env + filesystem AND-signal probe), implement ResolveTenantLabels (precedence chain), wire DefaultK8sProbe to live os.Getenv + os.Stat, add `cmd/nornicdb/main.go` startup hook (per R-02 sentinel pattern), add `cfg.Observability.Metrics.TenantLabelsExplicit *bool` field. All RED k8s tests GREEN at Plan 05-03 exit.
- **No blockers.** Wave-0 RED contract is fully locked.

## Self-Check: PASSED

- File `pkg/observability/legacy_translation.go` exists ✓
- File `pkg/observability/k8s_detect.go` exists ✓
- File `pkg/observability/legacy_translation_test.go` exists ✓
- File `pkg/observability/k8s_detect_test.go` exists ✓
- Commit `65f8771` exists in `git log` ✓
- Commit `894a9d5` exists in `git log` ✓
- `go build ./pkg/observability/...` exits 0 ✓
- `TestRenderLegacy_HeadersConsts` GREEN ✓
- `TestPackageBoundary_NoBusinessImports` GREEN ✓
- All Wave-0 RED tests FAIL as designed ✓

---
*Phase: 05-legacy-translation-layer-tenant-flag*
*Completed: 2026-05-04*
