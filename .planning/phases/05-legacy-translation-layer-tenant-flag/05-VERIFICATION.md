---
phase: 05-legacy-translation-layer-tenant-flag
verified: <TO_BE_CAPTURED in Task 05-05-04>
status: <TO_BE_CAPTURED in Task 05-05-04>
score: <TO_BE_CAPTURED>/8 universal gates verified
overrides_applied: 0
---

# Phase 5: Legacy Translation Layer & Tenant Flag — Verification Report

**Phase Goal:** Translate the legacy `:7474/metrics` 12-metric surface from the
unified Phase 4 registry via `pkg/observability.RenderLegacy`; resolve the
tenant-labels-enabled flag at startup via K8s autodetect (multi-signal AND);
emit `Deprecation: true` + `Sunset: Fri, 31 Dec 2027 23:59:59 GMT` headers on
every legacy response. Satisfies MET-18..MET-22 + TEST-03.

**Verified:** <TO_BE_CAPTURED in Task 05-05-04>
**Verifier:** asanabria
**Re-verification:** No — initial post-execution verification (this is the
post-execution counterpart of the per-plan VALIDATION contracts).
**Commit range under review:** `<TO_BE_CAPTURED>..<TO_BE_CAPTURED>` on `otel`

---

## Universal Gates (KD-12)

The hard gates enforced at every phase exit:

| Gate | Threshold | Measurement | Result | Captured-In |
|------|-----------|-------------|--------|-------------|
| 1. `make bench-cypher` Δ | ≤ 2% (tracing OFF and ON) | <TO_BE_CAPTURED> | <TO_BE_CAPTURED> | bench-cypher-phase5.txt |
| 2. `make bench-bolt` Δ | ≤ 5% | <TO_BE_CAPTURED> | <TO_BE_CAPTURED> | bench-bolt-phase5.txt |
| 3. `pkg/observability` line coverage | ≥ 90% (PERF-05) | <TO_BE_CAPTURED> | <TO_BE_CAPTURED> | inline below |
| 4. `pkg/observability` max file LOC | ≤ 800 (PERF-06) | <TO_BE_CAPTURED> | <TO_BE_CAPTURED> | inline below |
| 5. `pkg/audit/audit.go` untouched | byte-equal to Phase 4 close | <TO_BE_CAPTURED> | <TO_BE_CAPTURED> | inline below |
| 6. Race-stability `-race -count=10` | clean across pkg/observability + pkg/server + cmd/nornicdb | <TO_BE_CAPTURED> | <TO_BE_CAPTURED> | race-evidence-phase5.txt |
| 7. Neo4j wire compatibility round-trip | preserved | <TO_BE_CAPTURED> | <TO_BE_CAPTURED> | inline below |
| 8. PII scan on legacy `/metrics` body | zero email/token/CC-shaped strings | <TO_BE_CAPTURED> | <TO_BE_CAPTURED> | inline below |

---

## Goal-Backward Verification — ROADMAP Phase 5 Success Criteria

For each of the 5 ROADMAP Phase 5 success criteria, the linked test name +
commit SHA proves wire-level satisfaction:

### SC #1 — Single source of truth (translation layer fed from new registry)

> "Customer running an unmodified pre-M1 Prometheus scrape config against
> `:7474/metrics` continues to find `nornicdb_uptime_seconds`,
> `nornicdb_requests_total`, etc. with the same labels they had before — fed
> from the new registry, no second source of truth."

- **Implementation:** `pkg/observability/legacy_translation.go::RenderLegacy(reg, now) []byte`
  walks `reg.Gather()` once, applies the 12-row `legacyMappings` table, emits
  Prometheus exposition v0.0.4 bytes.
- **Server wiring:** `pkg/server/server_public.go::handleMetrics` calls
  `observability.RenderLegacy(s.obsRegistryForHandler(), time.Now())` (commit
  `6c8a875`).
- **Falsifiable lock:** `pkg/observability/legacy_snapshot.golden` byte-equality
  test `TestRenderLegacy_Snapshot` PASS at commit `537852c`.
- **Status:** ✅ VERIFIED.

### SC #2 — Deprecation + Sunset headers on every response

> "Every response from `:7474/metrics` carries `Deprecation: true` and
> `Sunset: <date>` HTTP headers."

- **Const-lock:** `LegacyDeprecation = "true"`, `LegacySunset = "Fri, 31 Dec 2027 23:59:59 GMT"`,
  `LegacyContentType = "text/plain; version=0.0.4; charset=utf-8"` —
  locked by `TestRenderLegacy_HeadersConsts` (commit `65f8771` GREEN immediately).
- **Wire-level:** `Test_HandleMetrics_DeprecationHeaders`
  (`pkg/server/server_public_test.go`) PASS at commit `6c8a875` asserts the
  three headers come from the consts AND the body matches the expected
  byte format.
- **Status:** ✅ VERIFIED.

### SC #3 — Resolved tenant flag absent off-K8s

> "Operator on a non-K8s host sees `tenant_labels_enabled=false` resolved
> at startup and confirms `database` label is absent on `nornicdb_storage_*`,
> `nornicdb_cypher_*`, `nornicdb_search_*`, `nornicdb_mvcc_*` series."

- **Detection:** `pkg/observability/k8s_detect.go::k8sProbe.Detect()` returns
  `(false, ReasonServiceHostAbsent)` when `KUBERNETES_SERVICE_HOST` is unset;
  `TestDetectK8s_Signals/env_absent` PASS at commit `863376f`.
- **Resolution:** `ResolveTenantLabels` returns `false` when explicit nil +
  autodetect false (default).
- **Phase 4 D-08 plumbing:** every tenant-tagged bag constructor reads the
  resolved bool and strips the `database` label when false. Verified untouched
  in Phase 5.
- **Cmd-level smoke test:** `Test_TenantLabelsResolved_Override_Precedence`
  PASS at commit `140d5ce`.
- **Status:** ✅ VERIFIED.

### SC #4 — Resolved true on K8s with override

> "Operator on K8s (with `KUBERNETES_SERVICE_HOST` env + ServiceAccount token)
> sees `tenant_labels_enabled=true` resolved; can override via
> `metrics.tenant_labels_enabled=false`."

- **Detection:** `TestDetectK8s_Signals/env_present_token_present` PASS at
  commit `863376f` — both signals present → `(true, ReasonK8sDetected)`.
- **Override precedence:** `TestResolveTenantLabels_Precedence` (4 sub-tests)
  PASS at commit `863376f` — explicit YAML always wins (R-02 sentinel `*bool`
  preservation).
- **Status:** ✅ VERIFIED.

### SC #5 — Golden-file CI lock

> "Developer running `go test ./pkg/observability/legacy_translation_test.go`
> against the locked snapshot fails CI on any diff between
> new-registry-emitted-via-translation-layer and the legacy snapshot
> (golden-file test)."

- **Golden file exists:** `pkg/observability/legacy_snapshot.golden` —
  1660 bytes, 36 emit lines (12 metrics × 3 lines = `# HELP` + `# TYPE` +
  sample), git-tracked (committed at `537852c`).
- **Byte-equality test:** `TestRenderLegacy_Snapshot` reads the file, calls
  `RenderLegacy` with a fresh deterministic fixture seeded by
  `seedDeterministicLegacySources`, asserts byte-equality.
- **REGEN gate:** `TestRenderLegacy_RegenerateGolden` SKIPs unless `REGEN=1`
  is set in the env — CI never sets it; explicit operator action required for
  legitimate regeneration.
- **Status:** ✅ VERIFIED.

---

## Per-Requirement Verification

| REQ | Description (abridged) | Status | Test/Evidence | Commit SHA |
|-----|------------------------|--------|---------------|------------|
| MET-18 | `:9090/metrics` unauth Prometheus exposition | ✅ Complete | Phase 1 carry-forward (Plan 01-03 listener.go) — no Phase 5 regression | (Phase 1 commits) |
| MET-19 | `:7474/metrics` translation layer single source of truth | ✅ Complete | `Test_HandleMetrics_DeprecationHeaders` PASS; `TestRenderLegacy_Snapshot` PASS | `6c8a875` (handler) + `537852c` (golden) |
| MET-20 | `Deprecation: true` + `Sunset: <date>` headers | ✅ Complete | `TestRenderLegacy_HeadersConsts` GREEN; `Test_HandleMetrics_DeprecationHeaders` PASS | `65f8771` (consts) + `6c8a875` (wire-up) |
| MET-21 | `tenant_labels_enabled=false` strips `database` label | ✅ Complete | Phase 4 D-08 plumbing + Plan 05-03 startup-resolution hook | `ff8f9e9` (hook) |
| MET-22 | K8s autodetect AND-signal + startup log | ✅ Complete | `TestDetectK8s_Signals` (6 sub-tests) PASS; `TestResolveTenantLabels_Precedence` (4 sub-tests) PASS; `Test_TenantLabelsResolved_LogsViaInjectedSlog` PASS | `863376f` + `140d5ce` |
| TEST-03 | Golden-file test fails CI on diff | ✅ Complete | `legacy_snapshot.golden` (1660 bytes) git-tracked; `TestRenderLegacy_Snapshot` PASS | `537852c` |

**Total:** 6/6 Phase 5 requirements GREEN.

---

## Cumulative pkg/observability Coverage

(Captured by Task 05-05-04. Initial measurement after Plan 05-03 was 91.8%
per 05-03-SUMMARY; final post-Plan-05-04 measurement filled below.)

- **Coverage:** <TO_BE_CAPTURED>
- **PERF-05 floor:** ≥ 90%

---

## File-Size Discipline (PERF-06)

(Captured by Task 05-05-04. Phase 5 added 3 files; max known size from
Plan 05-02 was 409 LOC `legacy_translation_test.go`.)

- Largest production file in `pkg/observability/`: <TO_BE_CAPTURED>
- Largest test file in `pkg/observability/`: <TO_BE_CAPTURED>
- **PERF-06 cap:** ≤ 800 LOC

---

## Audit-Untouched Gate

(Captured by Task 05-05-04. Plan-by-plan SUMMARYs all confirm
`pkg/audit/audit.go` untouched.)

- `git diff --name-only $PHASE5_BASE..HEAD pkg/audit/`: <TO_BE_CAPTURED>
- **Expected:** empty.

---

## Race-Stability Matrix

(Captured by Task 05-05-04 in `race-evidence-phase5.txt`.)

| Package | -race -count=10 result | Notes |
|---------|------------------------|-------|
| `pkg/observability` | <TO_BE_CAPTURED> | Plan 05-02 reported clean at 1.68s; Plan 05-03 clean at 1.57s; Plan 05-04 clean at 1.78s |
| `pkg/server` | <TO_BE_CAPTURED> | Plan 05-04 specific tests clean under `-race -count=10` (1.76s); pre-existing pkg/server `TestEmbedTriggerAdditionalBranches` flake documented in deferred-items.md |
| `cmd/nornicdb` | <TO_BE_CAPTURED> | Plan 05-03 reported clean at 1.81s under `-race -count=5` |

**PASS criteria:** zero `FAIL` lines, zero `race detected`, non-zero `PASS` /
`ok` lines from `pkg/observability` test results.

---

## Neo4j Wire Compatibility

(Captured by Task 05-05-04. Phase 5 does NOT touch any pkg/bolt code; the
neo4j-go-driver round-trip is a carry-forward gate.)

- Test: <TO_BE_CAPTURED>
- Result: <TO_BE_CAPTURED>

---

## PII Scan on Legacy `/metrics` Body

(Captured by Task 05-05-04. The 12 legacy metrics are bounded enums + numeric
values; no email/token/credit-card-shaped strings expected.)

- Method: <TO_BE_CAPTURED>
- Result: <TO_BE_CAPTURED>

---

## Bench Captures

| Bench | Phase 1 baseline | Phase 5 capture | Δ% | Within budget? |
|-------|------------------|-----------------|-----|----------------|
| bench-cypher (tracing OFF) | <TO_BE_CAPTURED> | <TO_BE_CAPTURED> | <TO_BE_CAPTURED> | <TO_BE_CAPTURED> |
| bench-cypher (tracing ON) | <TO_BE_CAPTURED> | <TO_BE_CAPTURED> | <TO_BE_CAPTURED> | <TO_BE_CAPTURED> |
| bench-bolt | <TO_BE_CAPTURED> | <TO_BE_CAPTURED> | <TO_BE_CAPTURED> | <TO_BE_CAPTURED> |

Note: Phase 1 / Phase 4 baselines record `make bench-cypher: target not found`
and `make bench-bolt: target not found` — the formal Δ-vs-baseline regression
gate is owned by Phase 12 per CLAUDE.md "Performance — `make bench-cypher` Δ
≤2%, `make bench-bolt` Δ ≤5% measured against pre-M1 baseline." Phase 5
mirrors this carry-forward posture; Phase 12 will install the targets and
sign off the final regression gate.

---

## Verdict

**Status:** <TO_BE_CAPTURED in Task 05-05-04>

(All 8 KD-12 universal gates measured + recorded. Phase 5 ✅ CLOSED upon
verifier sign-off in Task 05-05-06.)
