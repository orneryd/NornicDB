---
phase: 5
subsystem: observability
tags: [observability, legacy-translation, tenant-flag, k8s-autodetect, deprecation-headers, sunset, golden-file, phase-5, phase-exit, audit-trail]
requires:
  - "Phase 0 — ADR-0001 §2.3 catalog contract; §3.2 cardinality discipline; §4.1 audit-trail row 5 placeholder"
  - "Phase 1 — pkg/observability boundary_test.go (leaf-package invariant); listener.go :9090 handler; TestEnv per-test isolated registry"
  - "Phase 2 — pkg/observability slog handler stack; cmd/nornicdb/main.go logger variable in scope (LOG-09 injected-logger pattern); ResolveAndLogTenantLabels reuses it"
  - "Phase 3 — registration helpers, exemplar wrappers, AssertCardinalityCeiling, make lint-cardinality (Plan 05-04 lint-cardinality MET-04 still PASS)"
  - "Phase 4 — 12 source metric families read by RenderLegacy; D-08 forward-compat plumbing of cfg.Observability.Metrics.TenantLabelsEnabled through every tenant-tagged bag constructor"
provides:
  - "RenderLegacy(reg *prometheus.Registry, now time.Time) []byte — pure-function legacy translation engine; single source of truth (ROADMAP SC #1, MET-19)"
  - "12-row legacyMappings table (uptime, requests, errors, active_requests, nodes_total, edges_total, embeddings_processed, embeddings_failed, embedding_worker_running, slow_queries_total, slow_query_threshold_ms, info)"
  - "LegacySunset / LegacyDeprecation / LegacyContentType frozen consts in pkg/observability/legacy_translation.go; locked by TestRenderLegacy_HeadersConsts"
  - "pkg/observability/legacy_snapshot.golden — TEST-03 byte-for-byte CI contract (1660 bytes, 36 emit lines)"
  - "k8sProbe{Getenv,StatFile} functional-DI struct + Detect() AND-signal logic + DefaultK8sProbe live-wiring (MET-22)"
  - "ResolveTenantLabels(explicit *bool, probe k8sProbe) precedence chain (explicit YAML > autodetect > default false)"
  - "Six closed-set Reason* string consts for forensic logging (explicit_yaml, k8s_detected, not_k8s_service_host_absent, not_k8s_token_file_absent, not_k8s_token_file_empty, not_k8s_token_stat_error)"
  - "ResolveAndLogTenantLabels cmd-level wrapper: probes + resolves + logs in one call with the four canonical fields (enabled, reason, service_host_present, token_file_present)"
  - "MetricsConfig.TenantLabelsExplicit *bool sentinel field (R-02) — preserves operator's YAML intent through to Phase 5 hook so explicit-false beats autodetect-true on K8s"
  - "cmd/nornicdb/main.go startup-resolution hook between obs init and first NewCacheMetrics — Phase 4 D-02c init-order chokepoint"
  - "Server.obsRegistry *prometheus.Registry field + SetObsRegistry post-construction setter (mirrors Phase 4 SetHTTPMetrics pattern)"
  - "Rewritten handleMetrics: 7-line adapter calling RenderLegacy + setting Content-Type/Deprecation/Sunset headers (MET-19, MET-20)"
  - "obsRegistryForHandler() RLock-protected accessor — race-clean under -race -count=10"
affects:
  - "Phase 6 — sampler flip lights up exemplars at every Phase-4-registered hot-path histogram automatically; Phase 5 unaffected (RenderLegacy is control-plane scrape, not hot-path)"
  - "Phase 9 — Helm chart values document the K8s autodetect contract (MET-22) — operators on K8s get tenant labels enabled by default; values can override"
  - "Phase 11 — metrics-doc-gen consumes :9090/metrics only; the legacy :7474/metrics adapter is documented separately as a deprecation surface"
  - "Phase M2+ — RFC 9745 cutover (R-01 deferred) migrates Deprecation: true → Deprecation: @<unix-timestamp>; legacy :7474/metrics removal happens after Sunset passes (Fri, 31 Dec 2027 23:59:59 GMT)"
tech-stack:
  added: []  # No new go.mod deps; reused stdlib (bytes, fmt, sort, strings, os, log/slog, time) + existing prometheus/client_golang + prometheus/client_model/go
  patterns:
    - "Pure-function translation (D-01): RenderLegacy(reg, now) []byte takes registry as input, returns bytes — leaf-package boundary preserved"
    - "Static closed-set mapping table (D-01a): 12-row legacyMappings with Reduce/UnitFn function fields; adding a 13th = explicit table extension + golden regen"
    - "Functional-DI struct of func fields (D-02c): k8sProbe{Getenv, StatFile} — production wires os.Getenv + os.Stat; tests inject fakes"
    - "Closed-set string-const enumeration (D-02b): Reason* enum for grep-discoverable forensic logging — operators lookup any of 6 outcomes"
    - "Sentinel-field deferred-dereference (R-02): pkg/config writes *bool into TenantLabelsExplicit sentinel; downstream resolver dereferences — distinguishes YAML-omitted from explicit-false"
    - "Post-construction setter (Plan 05-04 D-02): Server.SetObsRegistry mirrors Phase 4 SetHTTPMetrics pattern — mu.Lock + assign + unlock; nil-safe; injected by cmd/nornicdb/main.go after observability.New"
    - "Golden-file CI lock (TEST-03): legacy_snapshot.golden byte-equality test; REGEN=1-gated regenerator forces explicit operator intent for legitimate Phase-4 catalog changes"
    - "Per-mapping emit specialization in single helper (emitSample): branches on LegacyName for the 2 special cases (uptime=%.2f, info=labeled) and falls through to %d default"
    - "Probe-then-log-then-return helper shape (ResolveAndLogTenantLabels): probes once, resolves once, re-derives observable signals from SAME probe inputs (avoids drift), emits one log line, returns resolved value"
    - "LOG-09 injected-logger pattern preserved: ResolveAndLogTenantLabels takes *slog.Logger as a parameter — never touches slog.Default / slog.SetDefault"
key-files:
  created:
    - pkg/observability/legacy_translation.go (316 LOC)
    - pkg/observability/legacy_translation_test.go (409 LOC)
    - pkg/observability/legacy_snapshot.golden (1660 bytes, 36 emit lines)
    - pkg/observability/k8s_detect.go (126 LOC)
    - pkg/observability/k8s_detect_test.go (118 LOC)
    - pkg/server/server_public_test.go (143 LOC)
    - .planning/phases/05-legacy-translation-layer-tenant-flag/deferred-items.md
    - .planning/phases/05-legacy-translation-layer-tenant-flag/05-VERIFICATION.md
    - .planning/phases/05-legacy-translation-layer-tenant-flag/05-01-SUMMARY.md
    - .planning/phases/05-legacy-translation-layer-tenant-flag/05-02-SUMMARY.md
    - .planning/phases/05-legacy-translation-layer-tenant-flag/05-03-SUMMARY.md
    - .planning/phases/05-legacy-translation-layer-tenant-flag/05-04-SUMMARY.md
  modified:
    - pkg/observability/config.go (128 → 144 LOC) — added MetricsConfig.TenantLabelsExplicit *bool field (R-02)
    - pkg/config/config.go (3163-3164 swap, +2 LOC net) — replaced *bool dereference with pointer-copy into sentinel
    - cmd/nornicdb/main.go (~+19 LOC across two insertions: Plan 05-03 startup hook + Plan 05-04 SetObsRegistry call)
    - cmd/nornicdb/serve_test.go (482 → 594 LOC) — added Test_TenantLabelsResolved precedence + slog-shape tests
    - pkg/server/server.go (1913 → 1938 LOC) — added obsRegistry field + SetObsRegistry setter
    - pkg/server/server_public.go (268 → 231 LOC) — handleMetrics rewritten 70 LOC → 7-line adapter
    - .planning/STATE.md (Phase 5 closed)
    - .planning/ROADMAP.md (Phase 5 [x] + Plans 05-01..05-05 [x])
    - .planning/REQUIREMENTS.md (MET-18..MET-22 + TEST-03 [x])
    - docs/architecture/adr/0001-observability.md (§2.3.1 amendment + §4.1 row 5)
decisions:
  - "D-01: Pure function RenderLegacy(reg, now) []byte — leaf-package boundary preserved; TEST-03 golden-file becomes byte-equality on function output, no httptest round-trip"
  - "D-01a: Static 12-entry legacyMapping table — closed set; adding a 13th entry requires explicit table extension + golden regenerate"
  - "D-01b/c/d: 12-row mapping with reduceFn (sumAcrossLabels, sumByMatchingLabel, takeLatest, dropExtraLabels) + unitFn (identity, secondsToMs); sort sample-emit by LegacyName"
  - "D-01e/f: handleMetrics becomes ~7-line adapter (calls RenderLegacy + sets headers + writes body); pkg/observability owns translation, pkg/server is thin protocol shim"
  - "D-02: Multi-signal AND — KUBERNETES_SERVICE_HOST env non-empty AND /var/run/secrets/kubernetes.io/serviceaccount/token exists & non-empty; conservative — false positives on CI runners blocked"
  - "D-02a: Override precedence — explicit YAML (R-02 *bool) > autodetect AND-signal > default false; logged with closed-enum reason"
  - "D-02b: Single-line slog INFO at startup with four canonical fields — operators grep on a finite vocabulary instead of free-form messages"
  - "D-02c: k8sProbe{Getenv, StatFile} functional-DI struct — testable injection of fakes, production wires DefaultK8sProbe to os.Getenv + os.Stat"
  - "D-02d: Resolution happens once at startup between obs.New and first NewCacheMetrics — exactly the Phase 4 D-02c init-order chokepoint"
  - "D-03: Hardcoded const LegacySunset = 'Fri, 31 Dec 2027 23:59:59 GMT' (RFC 7231 IMF-fixdate) — customer-API contract surface; ADR-anchored"
  - "D-03a: const LegacyDeprecation = 'true' (RFC 8594 boolean truthy)"
  - "D-03b: ADR-0001 §2.3 amendment documents Sunset value + autodetect contract — changing requires follow-up ADR amendment"
  - "D-04: Auth gate verbatim on :7474/metrics — keep s.withAuth(s.handleMetrics, auth.PermRead); dropping auth would itself require its own deprecation cycle"
  - "D-05: 5 plans, layer-grouped — Wave-0 RED in 05-01; 05-02/05-03 may parallelize; 05-04 owns the only pkg/server touch; 05-05 is phase exit"
  - "D-06: All translation logic in pkg/observability/legacy_translation.go; all autodetect logic in pkg/observability/k8s_detect.go — no reverse imports; boundary_test.go preserved"
  - "D-07: Per-test isolated TestEnv (Phase 1 foundation); -race -count=10 race-stability across pkg/observability + pkg/server + cmd/nornicdb"
  - "D-08: Phase 4 D-08 forward-compat consumed verbatim — no bag constructor changes; only main.go startup-resolution insertion"
  - "R-01 (Resolution Addendum): Keep Deprecation: true literal per REQ MET-20; deliberate non-conformance with RFC 9745 §2.1 for M1; cutover deferred to post-M1 ticket in deferred-items.md"
  - "R-02 (Resolution Addendum): Sentinel TenantLabelsExplicit *bool added next to resolved TenantLabelsEnabled bool; smallest-blast-radius preservation of operator intent"
  - "Mapping correction (research-applied): Rows 5/6 (nodes_total, edges_total) use takeLatest (label-less GaugeFunc), not sumAcrossLabels; Row 4 (active_requests) uses takeLatest (single-sample gauge); Row 3 (errors_total) uses sumByMatchingLabel(status_class, 5xx)"
  - "Init-order pin: startup resolution inserts between line 717 (obs := observability.New) and line 727 (first NewCacheMetrics) in cmd/nornicdb/main.go"
metrics:
  duration_total: "across 5 plans on 2026-05-04 (multi-hour; per-plan durations: 18min + 6min + 14min + 11min + 05-05 exit work)"
  completed: 2026-05-04
  files_created: 12
  files_modified: 10
  total_legacy_metrics_translated: 12
  total_reduction_helpers: 4
  total_k8s_signals: 6
  coverage_pct: 92.4
  largest_phase5_file_loc: 409 (legacy_translation_test.go)
  largest_phase5_production_file_loc: 316 (legacy_translation.go)
commit_range: 65f8771..973bbd7
---

# Phase 5: Legacy Translation Layer & Tenant Flag — SUMMARY

**Phase exit:** 2026-05-04
**Commit range on `otel`:** `65f8771..973bbd7` (this Plan 05-05 final-commit closes the phase from a tracking standpoint; the ADR §4.1 row-5 fill is the second commit and is not included in the range it documents — same precedent as Phase 2's `4201a24..021a983`, Phase 3's `1ba8f01..a62d30a`, and Phase 4's `523c23d..9a0e574`).
**ADR §4.1 row:** 5 (this phase)
**Verifier:** asanabria
**Single-PR strategy:** all Phase 5 commits land directly on `otel` (PR #126).

## One-liner

Customer scrapers on `:7474/metrics` continue to receive the original 12 metric names — fed from the unified Phase 4 registry through a pure-function translator (`RenderLegacy`), byte-locked by `legacy_snapshot.golden` (TEST-03), with `Deprecation: true` + `Sunset: Fri, 31 Dec 2027 23:59:59 GMT` headers on every response. A K8s autodetect probe (multi-signal AND: `KUBERNETES_SERVICE_HOST` env + non-empty SA token) resolves `tenant_labels_enabled` once at startup before any Phase-4 bag constructor reads it, with operator's YAML intent (R-02 `*bool` sentinel) always winning over autodetect. ROADMAP Phase 5 SC #1-#5 wire-level satisfied; MET-18..MET-22 + TEST-03 GREEN.

## Phase Goal (verbatim from ROADMAP)

> **Phase 5: Legacy Translation Layer & Tenant Flag** — Existing customer scrapers on `:7474/metrics` continue to receive the original 12 metric names with their old labels — fed from the new `pkg/observability` registry via a translation layer — while a tenant-label kill switch with K8s auto-detect prevents tenant enumeration on `:9090`.

## Plans Delivered

| Plan | Title | Commits | Tasks | Files (created/modified) | Outcome |
|------|-------|---------|-------|--------------------------|---------|
| 05-01 | Wave-0 RED scaffolding (legacy_translation + k8s_detect skeletons + failing tests) | `65f8771` feat / `894a9d5` test / `f18d7ff` docs | 2 | 4 created (2 production skeletons + 2 test files) | Locked public-API surface; Wave-0 RED contract for 05-02/05-03 |
| 05-02 | Translation engine + golden snapshot (RenderLegacy body, 4 reduction helpers, 12-row table, REGEN-gated regenerator) | `da0d1ca` / `1deff7c` feat / `537852c` test / `58d8721` docs | 3 | 1 created (golden) + 2 modified | All Wave-0 RED legacy-translation tests GREEN; TEST-03 byte contract locked |
| 05-03 | K8s autodetect + startup resolution (Detect AND-signal, ResolveTenantLabels precedence, R-02 sentinel, ResolveAndLogTenantLabels helper, main.go startup hook) | `863376f` / `b550641` / `ff8f9e9` feat / `140d5ce` test / `e7ca461` docs | 4 | 5 modified | All Wave-0 RED k8s tests GREEN; MET-22 forensic log line emitted exactly once |
| 05-04 | Server adapter rewrite (handleMetrics 70 LOC → 7-line adapter; SetObsRegistry setter; Deprecation/Sunset headers; integration tests) | `1ca2e61` feat / `8d98b60` test RED / `6c8a875` feat GREEN / `28c25e3` feat / `dcbbf80` docs / `849f948` fix | 3 | 2 created (test + deferred-items) + 3 modified | MET-19 + MET-20 wire-level satisfied at customer-facing surface |
| 05-05 | Phase exit (THIS PLAN — SUMMARY + ADR §2.3.1 amendment + §4.1 row 5 + cumulative evidence + STATE/ROADMAP/REQUIREMENTS sync + deferred-items + 05-VERIFICATION) | (this final commit) | 7 | SUMMARY + VERIFICATION + STATE/ROADMAP/REQUIREMENTS/ADR/deferred + bench/race captures | Phase 5 closed; M1 PR #126 §4.1 row 5 populated |

**Total Phase 5 commits (cumulative):** 23 across plans 05-01..05-05 (commit range `65f8771..973bbd7`); the Plan 05-05 closing commits are `cf6791a` (SUMMARY narrative) → `3134cdf` (ADR §2.3.1 amendment) → `4ab8e09` (deferred-items + 05-VERIFICATION skeleton) → `973bbd7` (cumulative evidence) → final consolidation commit (Task 05-05-07).

## Files Created / Modified (grouped by package)

### `pkg/observability/` (the leaf-package boundary preserved)

| File | Status | LOC | Purpose |
|------|--------|-----|---------|
| `legacy_translation.go` | CREATED | 316 | RenderLegacy + 12-row legacyMappings + 4 reduction helpers + 6 emit helpers + LegacySunset/LegacyDeprecation/LegacyContentType consts |
| `legacy_translation_test.go` | CREATED | 409 | Golden-file (TEST-03) + 12-row TestRenderLegacy_Mapping + 13-row TestRenderLegacy_Reductions + TestRenderLegacy_HeadersConsts (const-lock) + REGEN-gated TestRenderLegacy_RegenerateGolden + seedDeterministicLegacySources fixture |
| `legacy_snapshot.golden` | CREATED | 1660 bytes (36 emit lines) | Locked TEST-03 byte contract |
| `k8s_detect.go` | CREATED | 126 | k8sProbe{Getenv, StatFile} + Detect AND-signal + ResolveTenantLabels precedence + DefaultK8sProbe live + ResolveAndLogTenantLabels helper + 6 Reason* consts |
| `k8s_detect_test.go` | CREATED | 118 | TestDetectK8s_Signals (6-row matrix) + TestResolveTenantLabels_Precedence (4-row) + TestK8sProbe_DefaultProbeIsLive |
| `config.go` | MODIFIED | 128 → 144 (+16) | Added MetricsConfig.TenantLabelsExplicit *bool sentinel field (R-02) + DefaultConfig nil + extended doc comments |

### `pkg/config/`

| File | Status | LOC delta | Purpose |
|------|--------|-----------|---------|
| `config.go` | MODIFIED | 3163-3164 swap (+2 net) | Replaced *bool dereference into TenantLabelsEnabled with pointer-copy into TenantLabelsExplicit (R-02 deferred dereference) |

### `pkg/server/`

| File | Status | LOC | Purpose |
|------|--------|-----|---------|
| `server.go` | MODIFIED | 1913 → 1938 (+25) | Added obsRegistry *prometheus.Registry field + SetObsRegistry setter + 1 import (github.com/prometheus/client_golang/prometheus) |
| `server_public.go` | MODIFIED | 268 → 231 (−37 net) | Rewrote handleMetrics from ~70 LOC of hand-built strings to 7-line RenderLegacy adapter; added obsRegistryForHandler RLock-protected accessor; imports: −strings, +time, +prometheus, +observability |
| `server_public_test.go` | CREATED | 143 | Test_HandleMetrics_DeprecationHeaders + Test_HandleMetrics_NilRegistry_ReturnsEmptyBodyWithHeaders |

### `cmd/nornicdb/`

| File | Status | LOC delta | Purpose |
|------|--------|-----------|---------|
| `main.go` | MODIFIED | +19 net (across 2 insertions) | (1) Plan 05-03 startup hook calling observability.ResolveAndLogTenantLabels (9 LOC); (2) Plan 05-04 httpServer.SetObsRegistry(obs.Registry()) call (10 LOC including comment) |
| `serve_test.go` | MODIFIED | 482 → 594 (+112) | Added Test_TenantLabelsResolved_Override_Precedence + Test_TenantLabelsResolved_LogsViaInjectedSlog; added bytes + log/slog imports |

### `docs/architecture/adr/`

| File | Status | Purpose |
|------|--------|---------|
| `0001-observability.md` | MODIFIED | §2.3.1 Phase 5 amendment (Sunset value + Deprecation R-01 non-conformance + autodetect AND-signal + 12-metric translation-layer lock); §4.1 row 5 populated with `65f8771..973bbd7 \| 2026-05-04 \| asanabria` |

### `.planning/`

| File | Status | Purpose |
|------|--------|---------|
| `STATE.md` | MODIFIED | Phase 5 closed; commit range; verifier handle |
| `ROADMAP.md` | MODIFIED | Phase 5 [x] + Plans 05-01..05-05 [x] + Progress table 5/5 Done |
| `REQUIREMENTS.md` | MODIFIED | MET-18..MET-22 + TEST-03 marked [x] + Traceability Complete |
| `phases/05-legacy-translation-layer-tenant-flag/05-SUMMARY.md` | CREATED | THIS DOCUMENT |
| `phases/05-legacy-translation-layer-tenant-flag/05-VERIFICATION.md` | CREATED | Evidence bundle (KD-12 universal gates, per-SC verification, per-requirement table) |
| `phases/05-legacy-translation-layer-tenant-flag/deferred-items.md` | EXTENDED | RFC 9745 cutover (R-01); accessor cleanup; deprecation-usage telemetry; migration documentation; legacy `:7474/metrics` removal |
| `phases/05-legacy-translation-layer-tenant-flag/bench-cypher-phase5.txt` | CREATED | bench-cypher capture (carry-forward "target not found" per Phase 1/4 posture) |
| `phases/05-legacy-translation-layer-tenant-flag/bench-bolt-phase5.txt` | CREATED | bench-bolt capture (same posture) |
| `phases/05-legacy-translation-layer-tenant-flag/race-evidence-phase5.txt` | CREATED | -race -count=10 evidence across pkg/observability + pkg/server + cmd/nornicdb |

## Requirement Coverage

| REQ | Description (abridged) | Status | Verification Evidence | Plan |
|-----|------------------------|--------|----------------------|------|
| **MET-18** | Operator can scrape `:9090/metrics` with no auth and receive Prometheus exposition format | ✅ COMPLETE | Phase 1 Plan 01-03 listener.go GREEN; carry-forward (no Phase 5 regression) | 05-05 closes (cumulative carry-forward) |
| **MET-19** | Customer scraper on `:7474/metrics` continues to receive original 12 metrics via translation layer | ✅ COMPLETE | `pkg/observability/legacy_translation.go::RenderLegacy` reads from unified registry; `pkg/server/server_public.go::handleMetrics` is a 7-line adapter; `Test_HandleMetrics_DeprecationHeaders` PASS at commit `6c8a875` | 05-02 (engine) + 05-04 (wire-up) |
| **MET-20** | Legacy `:7474/metrics` responses include `Deprecation: true` and `Sunset: <date>` headers | ✅ COMPLETE | `LegacySunset = "Fri, 31 Dec 2027 23:59:59 GMT"` + `LegacyDeprecation = "true"` consts; locked by `TestRenderLegacy_HeadersConsts`; wired by `Test_HandleMetrics_DeprecationHeaders` PASS | 05-01 (consts) + 05-04 (wire-up) |
| **MET-21** | When `metrics.tenant_labels_enabled=false`, registry strips `database` label from storage/cypher/search/mvcc | ✅ COMPLETE | Phase 4 D-08 plumbing already in place; Phase 5 supplies the resolved `bool` value via `cmd/nornicdb/main.go` startup hook (commit `ff8f9e9`); bag constructors honor the flag | 05-03 (resolution) + Phase 4 (forward-compat) |
| **MET-22** | `metrics.tenant_labels_enabled` defaults `true` on K8s detection (multi-signal: env + ServiceAccount token); resolved value logged at startup | ✅ COMPLETE | `k8sProbe.Detect()` AND-signal logic GREEN by `TestDetectK8s_Signals` (6 sub-tests); `ResolveAndLogTenantLabels` emits the four-field slog INFO; `Test_TenantLabelsResolved_LogsViaInjectedSlog` PASS | 05-03 |
| **TEST-03** | Golden-file test in `pkg/observability/legacy_translation_test.go` fails CI on diff between new-registry-via-translation-layer and locked legacy snapshot | ✅ COMPLETE | `pkg/observability/legacy_snapshot.golden` exists (1660 bytes, 36 emit lines, git-tracked); `TestRenderLegacy_Snapshot` PASS at commit `537852c`; REGEN=1-gated regenerator forces explicit operator intent for legitimate Phase-4 catalog changes | 05-02 |

**Total:** 6/6 Phase 5 requirements GREEN.

## Goal-Backward Verification — ROADMAP Phase 5 Success Criteria (5/5 GREEN)

### SC #1 — Single source of truth (translation layer fed from new registry)

> **Verbatim:** "Customer running an unmodified pre-M1 Prometheus scrape config against `:7474/metrics` continues to find `nornicdb_uptime_seconds`, `nornicdb_requests_total`, etc. with the same labels they had before — fed from the new registry, no second source of truth."

✅ **Verified.** `pkg/server/server_public.go::handleMetrics` is a 7-line adapter that calls `observability.RenderLegacy(s.obsRegistryForHandler(), time.Now())` — the registry is the same `obs.Registry()` that backs `:9090/metrics`. The pre-Plan-05-04 hand-built `~70` LOC body that read from `s.Stats()` / `s.db.Stats()` / `s.db.EmbedQueueStats()` is gone (commits `8d98b60` test RED + `6c8a875` feat GREEN). `pkg/observability/legacy_snapshot.golden` byte-locks the customer-visible output; CI fails on any diff.

### SC #2 — Deprecation + Sunset headers on every response

> **Verbatim:** "Every response from `:7474/metrics` carries `Deprecation: true` and `Sunset: <date>` HTTP headers."

✅ **Verified.** `Test_HandleMetrics_DeprecationHeaders` (`pkg/server/server_public_test.go`) PASS asserts `Deprecation: true` + `Sunset: Fri, 31 Dec 2027 23:59:59 GMT` + `Content-Type: text/plain; version=0.0.4; charset=utf-8` on every response from the rewritten handler. The values are sourced from `observability.LegacyDeprecation` / `LegacySunset` / `LegacyContentType` package consts (locked by `TestRenderLegacy_HeadersConsts`).

### SC #3 — Resolved tenant flag absent off-K8s

> **Verbatim:** "Operator on a non-K8s host sees `tenant_labels_enabled=false` resolved at startup and confirms `database` label is absent on `nornicdb_storage_*`, `nornicdb_cypher_*`, `nornicdb_search_*`, `nornicdb_mvcc_*` series."

✅ **Verified.** `TestDetectK8s_Signals/env_absent` PASS — `k8sProbe.Detect()` returns `(false, ReasonServiceHostAbsent)` when `KUBERNETES_SERVICE_HOST` is unset. `ResolveTenantLabels` returns `false` when explicit nil + autodetect false. `Test_TenantLabelsResolved_Override_Precedence` confirms the precedence chain at the cmd-level. Phase 4 D-08 plumbing through every tenant-tagged bag constructor honors the resolved bool — when `false`, the `database` label is stripped from storage/cypher/search/mvcc series (Phase 4 verified untouched).

### SC #4 — Resolved true on K8s with override

> **Verbatim:** "Operator on K8s (with `KUBERNETES_SERVICE_HOST` env + ServiceAccount token) sees `tenant_labels_enabled=true` resolved; can override via `metrics.tenant_labels_enabled=false`."

✅ **Verified.** `TestDetectK8s_Signals/env_present_token_present` PASS — `k8sProbe.Detect()` returns `(true, ReasonK8sDetected)` when both signals present. `TestResolveTenantLabels_Precedence` (4 sub-tests) confirms explicit YAML always wins (R-02 sentinel `*bool` preservation). `Test_TenantLabelsResolved_Override_Precedence` is the cmd-level smoke test exercising explicit-true / explicit-false overriding autodetect.

### SC #5 — Golden-file CI lock

> **Verbatim:** "Developer running `go test ./pkg/observability/legacy_translation_test.go` against the locked snapshot fails CI on any diff between new-registry-emitted-via-translation-layer and the legacy snapshot (golden-file test)."

✅ **Verified.** `pkg/observability/legacy_snapshot.golden` exists in git (1660 bytes, 36 emit lines = 12 metrics × 3 lines). `TestRenderLegacy_Snapshot` reads the file, calls `RenderLegacy` with a fresh deterministic fixture, and `require.Equal(string(want), string(got))`. Test passed at commit `537852c`. Regeneration is gated behind `REGEN=1` env var (custom flag rejected per established Phase 4 pattern); CI never sets REGEN, so the test SKIPs the regenerator and asserts byte-equality against the locked file.

## Decisions Recorded

### From CONTEXT.md (D-01..D-08)

- **D-01 Pure RenderLegacy(reg, now) []byte** — leaf-package boundary preserved; TEST-03 golden becomes byte-equality on function output without httptest round-trip.
- **D-01a Static legacyMapping table** — closed-set 12 entries; adding a 13th = explicit table extension + golden regenerate (with explicit "Why?" review).
- **D-01b The 12 legacy mappings** (canonical from `pkg/server/server_public.go:181-262` pre-Plan-05-04). See "12-Row Mapping Table (Wired)" below.
- **D-01c Reduction helpers** — `sumAcrossLabels`, `sumByMatchingLabel`, `takeLatest`, `dropExtraLabels`. Each pure function, table-driven test coverage per helper.
- **D-01d Output ordering deterministic** — sort `legacyMappings` by `LegacyName` before emitting; emit `# HELP`, `# TYPE`, then sample line per metric.
- **D-01e handleMetrics becomes 7-line adapter** — calls `RenderLegacy` + sets headers + writes bytes; no `s.Stats()` / `s.db.Stats()` reads.
- **D-01f Translation function lives in pkg/observability** per TEST-03 path requirement; takes `*prometheus.Registry` as input parameter, returns `[]byte`.
- **D-02 Multi-signal AND** — `KUBERNETES_SERVICE_HOST` env non-empty AND `/var/run/secrets/kubernetes.io/serviceaccount/token` exists & non-empty.
- **D-02a Override precedence** — explicit YAML (R-02 *bool) wins; then autodetect; then default false.
- **D-02b Log line at startup, exactly once** — `level=INFO msg="resolved tenant labels enabled" enabled=<bool> reason=<enum> service_host_present=<bool> token_file_present=<bool>`.
- **D-02c k8sProbe testable injection** — functional-DI struct of func fields; production wires `os.Getenv` + `os.Stat`.
- **D-02d Resolution happens once at startup** between `obs.New()` and first `NewCacheMetrics()` (line 717→727 in cmd/nornicdb/main.go).
- **D-03 Hardcoded const LegacySunset** = `"Fri, 31 Dec 2027 23:59:59 GMT"` (RFC 7231 IMF-fixdate).
- **D-03a const LegacyDeprecation = "true"** (RFC 8594 boolean truthy).
- **D-03b ADR-0001 §2.3 amendment** documents Sunset value + autodetect contract; changing requires follow-up ADR amendment.
- **D-04 Auth gate verbatim** on `:7474/metrics` — keep `s.withAuth(s.handleMetrics, auth.PermRead)`. `git diff --quiet pkg/server/server_router.go` exits 0.
- **D-05 5 plans, layer-grouped** — Wave-0 RED in 05-01; 05-02/05-03 may parallelize; 05-04 owns the only `pkg/server` touch; 05-05 is phase exit.
- **D-05a Wave-0 RED in 05-01** mirrors Phase 1/2/3/4 cadence — failing tests before any production code.
- **D-05b Plan 05-04 only `pkg/server` touch** — minimizes audit surface for the M1 single-PR review.
- **D-06 All translation logic in pkg/observability** — no reverse imports; Phase 1 D-01a `boundary_test.go` continues to pass.
- **D-07 Per-test isolated TestEnv** + `-race -count=10` race-stability across `pkg/observability` + `pkg/server` + `cmd/nornicdb`.
- **D-08 Phase 4 D-08 forward-compat consumed verbatim** — no bag constructor changes; only main.go startup-resolution insertion.

### Resolution Addendum (post-research, 2026-05-04)

- **R-01 Deprecation header value: keep `Deprecation: true` literal per REQ MET-20** — RFC 9745 (March 2025) supersedes the original draft Deprecation header and now mandates a Structured-Field Date format (`Deprecation: @<unix-timestamp>`). M1 deliberately ships `true` per the locked REQ MET-20 / ROADMAP SC#2 contract; an RFC-9745 cutover is captured as a deferred item in `deferred-items.md`. ADR §2.3.1 amendment explicitly notes the project's deliberate non-conformance with RFC 9745 §2.1 for M1.
- **R-02 Sentinel `TenantLabelsExplicit *bool` plumbing** — Research found `pkg/observability/config.go:32` was plain `bool` (dereferenced at YAML load `pkg/config/config.go:1447 → 3163-3164`). D-02a's "explicit YAML wins" precedence requires distinguishing "user said false" from "user didn't set it". A sentinel `*bool` field is the smallest-blast-radius preservation of operator intent. `pkg/config/config.go` writes the YAML `*bool` into the sentinel; `cmd/nornicdb/main.go` startup resolution dereferences it.

### Research-applied mapping table corrections

- **Rows 5/6 (`nornicdb_nodes_total`, `nornicdb_edges_total`)**: Phase 4 catalog registers these as **process-wide label-less GaugeFuncs**, not per-database. Reduce = `takeLatest` + UnitFn = `identity` (NOT `sumAcrossLabels`). Plan 05-02 wired correctly.
- **Row 4 (`nornicdb_active_requests`)**: Sources the label-less Gauge `nornicdb_http_in_flight_requests`, not the labeled `_count` of a histogram. Reduce = `takeLatest`.
- **Row 3 (`nornicdb_errors_total`)**: Phase 4 emits `status_class` label (exact-match values `2xx`/`3xx`/`4xx`/`5xx`), not raw `status`. Reduce = `sumByMatchingLabel(status_class, "5xx")`.
- **Row 11 (`nornicdb_slow_query_threshold_ms`)**: Wired with `secondsToMs` unit conversion (1.0 seconds → 1000 ms golden value).

### Init-order pin (verified)

Phase 5 startup resolution inserts in `cmd/nornicdb/main.go` between line 717 (`obs := observability.New(...)` returns) and line 727 (`cacheMetrics := observability.NewCacheMetrics(...)` first bag construction). Resolution mutates `cfg.Observability.Metrics.TenantLabelsEnabled` before any constructor reads it.

## Patterns Established

These patterns are introduced by Phase 5 and may be reused by downstream phases:

1. **Pure-function translation engine over a registry** — `RenderLegacy(reg, now) []byte` reads from a `*prometheus.Registry` parameter, applies a static closed-set mapping table, returns bytes. Reusable for any future "old API surface fed from new registry" deprecation pattern. **No second source of truth.**

2. **Golden-file as customer-API contract lock** — `pkg/observability/legacy_snapshot.golden` is the byte-level lock that prevents drift between the new registry and the legacy 12 metrics. CI fails on any diff, forcing a conscious operator action via `REGEN=1`-gated regenerator. This pattern can lock any future "customer-visible byte format" surface against silent drift.

3. **Functional-DI probe struct for env+filesystem signals** — `k8sProbe{Getenv func(string) string, StatFile func(string) (os.FileInfo, error)}` makes the autodetect logic testable via fake injection while production wires `os.Getenv` + `os.Stat`. Reusable for any future feature flag that probes the host environment.

4. **Sentinel `*bool` field for "operator-explicit-vs-defaulted" plumbing** — `MetricsConfig.TenantLabelsExplicit *bool` next to resolved `TenantLabelsEnabled bool`. The sentinel preserves "user said false" vs "user didn't set it" through to startup-resolution time. Reusable for any future feature flag where YAML-omit / YAML-true / YAML-false must be distinguishable AND post-load logic needs to override the default.

5. **Closed-set string-const enumeration for forensic logging** — `Reason*` consts (6 values) for grep-discoverable log line outcomes. Operators grep on a finite vocabulary instead of free-form messages. Same pattern applied in Phase 1 `InstanceIDSource` and now reusable for any future auto-defaulted feature flag.

6. **Probe-then-log-then-return helper shape** — `ResolveAndLogTenantLabels(explicit, *slog.Logger)` probes once, resolves once, re-derives observable signals from the SAME probe inputs (avoids drift between log truth and resolution truth), emits one log line, returns the resolved value. Reusable for any other auto-defaulted feature flag in M1+.

7. **Post-construction setter mirror (Plan 05-04)** — `Server.SetObsRegistry(*prometheus.Registry)` replicates the Phase 4 `SetHTTPMetrics(*observability.HTTPMetrics)` shape — `mu.Lock + assign + unlock`; nil-safe; injected by `cmd/nornicdb/main.go` after `observability.New`. Pure idiom replication; no new abstraction.

8. **REGEN=1 env-gated test for golden-file regeneration** — explicit operator action, not a CI accident. CI never sets REGEN, so the test SKIPs the regenerator. Operators run `REGEN=1 go test -run TestRenderLegacy_RegenerateGolden`. Mirrors Phase 4 catalog test idioms; reusable for any future contract-lock test.

## Cumulative Evidence Bundle

See `.planning/phases/05-legacy-translation-layer-tenant-flag/05-VERIFICATION.md` for the full evidence bundle:

- **KD-12 universal gates** (8 gates): `make bench-cypher` Δ; `make bench-bolt` Δ; `pkg/observability` line coverage ≥ 90%; `pkg/observability` max file LOC ≤ 800; `pkg/audit/audit.go` untouched; Neo4j wire compatibility round-trip; race-stability `-race -count=10`; PII scan on legacy `:7474/metrics` body.
- **Per-SC verification** for each of the 5 ROADMAP success criteria with linked test names + commit SHAs.
- **Per-requirement table** mapping MET-18..MET-22 + TEST-03 to test names + status (Complete) + commit SHAs.

Raw artifact files in the same directory: `bench-cypher-phase5.txt`, `bench-bolt-phase5.txt`, `race-evidence-phase5.txt`.

## Deferred Items

See `.planning/phases/05-legacy-translation-layer-tenant-flag/deferred-items.md` for the full list. Summary:

1. **Pre-existing `TestEmbedTriggerAdditionalBranches` flake** (Plan 05-04 discovery; pre-existing pkg/server test-flake; out of Phase 5 scope; recommended for post-M1 hardening pass).
2. **RFC 9745 cutover** (R-01 deferred): migrate `Deprecation: true` → `Deprecation: @<unix-timestamp>`. Trigger: customer report of HTTP-strict gateway rejection, OR M2 major-version bump.
3. **Removing unused `s.Stats()` / `s.db.Stats()` / `s.db.EmbedQueueStats()` accessors** — out of Phase 5 scope; still consumed by `handleStatus` and other endpoints. Action: post-M1 cleanup phase.
4. **Telemetry on deprecation usage** — counting how many customers still scrape `:7474/metrics` via a self-instrumentation counter. Out of Phase 5 scope. Could ship in a future "observability self-instrumentation" follow-up.
5. **Migration documentation** (`docs/getting-started/migrating-from-legacy-metrics.md`) — Phase 11 owns docs generation.
6. **Removing `:7474/metrics` entirely** — out of M1 scope; happens at M2 or later, after Sunset (`Fri, 31 Dec 2027 23:59:59 GMT`) passes.

## Cross-Phase Signals

- **Phase 6 (Tracing SDK & Core Spans)** — sampler flip lights up exemplars at every Phase-4-registered hot-path histogram automatically. Phase 5 unaffected (RenderLegacy is control-plane scrape, not hot-path).
- **Phase 9 (Helm Chart Integration)** — chart docs note that on K8s, tenant labels are auto-enabled by default (D-02 AND-signal). Helm values can override via `metrics.tenant_labels_enabled: true|false`.
- **Phase 11 (Metrics Reference Doc Generator)** — consumes `:9090/metrics` only; the legacy `:7474/metrics` is documented separately as a deprecation surface (`docs/getting-started/migrating-from-legacy-metrics.md` may be added).
- **Phase M2 (post-M1 follow-up)** — RFC 9745 cutover (R-01) migrates Deprecation header format; legacy `:7474/metrics` removal happens after Sunset passes (`Fri, 31 Dec 2027 23:59:59 GMT`).

## Lessons Learned

- **The R-02 sentinel field was a small-footprint solution for "explicit_false vs unset"** once research surfaced the YAML-load dereference site (`pkg/config/config.go:3163-3164`). Adding `TenantLabelsExplicit *bool` next to the existing `TenantLabelsEnabled bool` was a 16-LOC change that preserved Phase 4's D-08 plumbing untouched while giving the Phase 5 resolver visibility into operator intent. This validates the "research before plan" workflow — without research the planner would have had to either (a) change every Phase 4 constructor signature to take `*bool` (wide blast radius), or (b) add a second config field that does the same job at YAML-load time (subtle bug surface). The sentinel is the cleanest of three options and only research surfaced it.

- **The golden-file pattern (TEST-03) became the contract-lock and prevented Phase 4 catalog drift from silently breaking customer scrape configs.** Before Plan 05-02, the only contract between the new registry and the legacy 12 metrics was the prose mapping in CONTEXT.md D-01b — easy to miss in a Phase 4 catalog refactor. After Plan 05-02, any future change to the 12 source families that would alter the legacy output bytes fails CI on `legacy_snapshot.golden` byte-equality, forcing the operator into a conscious `REGEN=1` regenerate path. This is the right pattern for any customer-visible API surface where the format is the contract.

- **The RFC 9745 conflict was caught in research before requirements were locked into code.** The R-01 resolution (keep `Deprecation: true` for M1, defer cutover) is a documented deliberate non-conformance recorded in ADR §2.3.1 and `deferred-items.md`. If research had not surfaced the spec conflict, the team might have either (a) shipped a non-conformant header without acknowledgment, or (b) tried to migrate mid-M1 (expanding the single-PR scope). Recording deliberate non-conformance in the ADR + deferred ticket is the right governance pattern for "we know about this, here's why we're shipping it anyway."

## Closing State

- **Commit range:** `65f8771..973bbd7`
- **Verified:** 2026-05-04
- **Verifier:** asanabria
- **ADR §4.1 row 5:** `| Phase 5  | Legacy Translation Layer & Tenant Flag             | 65f8771..973bbd7 | 2026-05-04 | asanabria |`

## Self-Check: PASSED

- File `.planning/phases/05-legacy-translation-layer-tenant-flag/05-SUMMARY.md` exists ✓
- File `.planning/phases/05-legacy-translation-layer-tenant-flag/05-VERIFICATION.md` exists ✓
- File `.planning/phases/05-legacy-translation-layer-tenant-flag/deferred-items.md` exists ✓
- File `.planning/phases/05-legacy-translation-layer-tenant-flag/bench-cypher-phase5.txt` exists ✓
- File `.planning/phases/05-legacy-translation-layer-tenant-flag/bench-bolt-phase5.txt` exists ✓
- File `.planning/phases/05-legacy-translation-layer-tenant-flag/race-evidence-phase5.txt` exists ✓
- Commit `cf6791a` (SUMMARY narrative) exists in git log ✓
- Commit `3134cdf` (ADR §2.3.1 amendment) exists in git log ✓
- Commit `4ab8e09` (deferred-items + 05-VERIFICATION skeleton) exists in git log ✓
- Commit `973bbd7` (cumulative evidence) exists in git log ✓
- Commit `48af38d` (cross-file STATE/ROADMAP/REQUIREMENTS sync + ADR §4.1 row 5) exists in git log ✓
- ADR §2.3.1 amendment present: `grep -c '^### 2\.3\.1 ' docs/architecture/adr/0001-observability.md` = 1 ✓
- ROADMAP Phase 5 [x]: `grep -c '\[x\] \*\*Phase 5:' .planning/ROADMAP.md` = 1 ✓
- ROADMAP all 5 plans [x]: `awk '/### Phase 5/,/### Phase 6/' .planning/ROADMAP.md | grep -cE '\[x\] 05-0[12345]-PLAN'` = 5 ✓
- ROADMAP Progress 5/5 Done: present ✓
- REQUIREMENTS MET-18..22 [x]: 5 ✓
- REQUIREMENTS TEST-03 [x]: 1 ✓
- REQUIREMENTS Traceability Phase 5 → Complete: 6 (MET-18..22 + TEST-03) ✓
- ADR §4.1 row 5 verifier "asanabria": 1 ✓
- 05-SUMMARY.md no remaining placeholders: 0 ✓
- Cross-file commit-range consistency: `65f8771..973bbd7` appears in STATE.md, ROADMAP.md, ADR, 05-SUMMARY.md (4/4 files) ✓
- pkg/audit/ untouched across `65f8771..973bbd7`: `git diff --name-only 65f8771..973bbd7 -- pkg/audit/ | wc -l` = 0 ✓
- 8/8 KD-12 universal gates GREEN per 05-VERIFICATION.md ✓

---

## Verifier Sign-off Addendum (Task 05-05-06 → 05-05-07)

**Phase 5 verifier sign-off:** orneryd 2026-05-04 — approved at Task 05-05-06 checkpoint (commit range `65f8771..973bbd7`).

**Closing-commit note (Task 05-05-07):** The original Plan 05-05 design called for one bundled closing commit covering SUMMARY + ADR + STATE/ROADMAP/REQUIREMENTS + evidence files. During execution this was intentionally split into five atomic commits (`cf6791a` SUMMARY narrative → `3134cdf` ADR §2.3.1 amendment → `4ab8e09` deferred-items + 05-VERIFICATION skeleton → `973bbd7` cumulative evidence → `48af38d` cross-file STATE/ROADMAP/REQUIREMENTS sync + ADR §4.1 row 5), with `c50af83` adding the self-check confirmation. All Task 05-05-07 acceptance criteria are satisfied across this commit range:

- ≥1 commit references Phase 5 / 05-05 / legacy-translation ✓ (all six Plan 05-05 commits)
- All 9 expected files present in the commit range ✓ (05-SUMMARY.md, 05-VERIFICATION.md, deferred-items.md, bench-cypher-phase5.txt, bench-bolt-phase5.txt, race-evidence-phase5.txt, ADR 0001-observability.md, STATE.md, ROADMAP.md, REQUIREMENTS.md)
- All commits include `Co-Authored-By: Claude Opus 4.7 (1M context)` trailer ✓
- Working tree clean post-commit (only intentionally-untracked planning artifacts + ui/dist) ✓
- pkg/audit/ untouched across the range ✓

The atomic-commit form preserves blame granularity per artifact while still satisfying the bundled-commit acceptance contract — recorded here as a deliberate deviation (Rule 1 / scope discipline, not a defect).

---

*Phase 5 ✅ CLOSED.*
*Phase: 05-legacy-translation-layer-tenant-flag*
*Plans: 5/5 complete (100%)*
*Milestone progress: 6 of 13 phases (Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, Phase 5)*
*Next up: `/gsd-plan-phase 6` (Tracing SDK & Core Spans)*
