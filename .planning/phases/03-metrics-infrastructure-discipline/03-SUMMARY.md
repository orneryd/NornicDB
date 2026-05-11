---
phase: 3
subsystem: observability
tags: [observability, metrics, prometheus, exemplars, cardinality, phase-3, phase-exit, audit-trail]
requires:
  - "Phase 0 — ADR-0001 §2.2/§2.3 contract (naming + bucket + label discipline)"
  - "Phase 1 — pkg/observability/{registry,listener,testenv,provider}.go (TEST-01 isolation foundation, OTel→Prom bridge, EnableOpenMetrics:true listener)"
  - "Phase 2 — Makefile lint-slog precedent (POSIX-portable grep with `(^|[^a-zA-Z_])` boundary; two-commit phase-exit pattern)"
provides:
  - "MetricOpts + 6 typed registration constructors (NewCounterVec / NewGaugeVec / NewLatencyHistogramVec / NewSizeHistogramVec / NewRowCountHistogramVec / NewEmbeddingLatencyHistogramVec) — MET-01..05"
  - "4 bucket-constant slices (LatencyBucketsSeconds / SizeBucketsBytes / RowCountBuckets / EmbeddingLatencyBucketsSeconds) — single source of truth for histogram buckets (MET-03 / MET-05)"
  - "ForbiddenLabels (10-entry) + validateSubsystem / validateNameSuffix / validateLabels — Layer 1 registration-time panic (MET-04 Layer 1 / D-03a)"
  - "make lint-cardinality POSIX-portable Makefile gate — Layer 2 CI-failable enforcement (MET-04 Layer 2 / D-03b)"
  - "4 typed exemplar wrappers (LatencyHistogram / SizeHistogram / RowCountHistogram / EmbeddingLatencyHistogram) + 4 BoundObserver hot-path types — single observeWithExemplar chokepoint (MET-24 / MET-25 / D-02a)"
  - "(*TestEnv).AssertCardinalityCeiling helper — TEST-02 cardinality-ceiling assertion driven by 1k synthetic UUIDs across 8 goroutines (D-04)"
  - "OpenMetrics negotiation regression gate (TestListener_OpenMetricsContentTypeRoundTrip / ExemplarPresent / EnableOpenMetricsLineExists)"
affects:
  - "Phase 4 — subsystems consume the typed constructors + exemplar wrappers + AssertCardinalityCeiling helper"
  - "Phase 6 — sampler flip (TraceIDRatioBased) lights up exemplars at every Phase-3-wrapped histogram with no second-pass migration (D-02a forward-compat)"
tech-stack:
  added: []
  patterns:
    - "Two-layer MET-04 defense (Layer 1 runtime panic + Layer 2 Makefile grep gate)"
    - "Single chokepoint exemplar emission gated on IsValid() && IsSampled() (stricter than Phase 2 D-05's IsValid() alone)"
    - "Bind(lvs...) returns value-typed BoundObserver (struct-field cacheable, MET-25 hot-path zero-alloc)"
    - "Dual-access (wrapper for emission, raw *Vec for cardinality assertions via Vec() accessor)"
    - "Closed allow-list for both subsystem prefixes (allowedSubsystems, 11 entries) and forbidden labels (ForbiddenLabels, 10 entries) — additions require ADR amendment"
    - "Two-commit phase-exit pattern (commit 6a SUMMARY/STATE/ROADMAP/REQUIREMENTS + commit 6b ADR §4.1 row 3) — mirrors Phase 2 commits 021a983 + be5ab8a"
key-files:
  created:
    - pkg/observability/metrics.go
    - pkg/observability/registration.go
    - pkg/observability/exemplar.go
    - pkg/observability/metrics_test.go
    - pkg/observability/registration_test.go
    - pkg/observability/exemplar_test.go
    - pkg/observability/exemplar_bench_test.go
    - pkg/observability/listener_openmetrics_test.go
    - pkg/observability/testenv_cardinality_test.go
  modified:
    - pkg/observability/testenv.go
    - Makefile
    - docs/architecture/adr/0001-observability.md
decisions:
  - "D-01..D-01d: typed constructors per bucket family; Namespace='nornicdb' helper-injected; subsystem closed allow-list"
  - "D-02..D-02e: typed wrapper structs with single observeWithExemplar chokepoint; Bind() pre-bound observers; counter exemplars deferred"
  - "D-02a-stricter: chokepoint guards on IsValid() AND IsSampled() (Plan 03-03 Rule-1 deviation — exemplars without traces in backend would be operator-confusing; logs use IsValid() alone because grep-fallback exists)"
  - "D-02b: hot_no_span ≤ 2 allocs/op budget MET (measured 0 allocs/op); D-02b1 sync.Pool escalation NOT triggered"
  - "D-03..D-03d: two-layer MET-04 defense; falsifiability proof via inject-and-revert in pkg/cypher (D-03d)"
  - "D-04..D-04c: AssertCardinalityCeiling with errgroup.SetLimit(8) bounded fan-out + 1k deterministic UUIDs"
  - "D-05..D-05b: 6-commit cadence (5 substantive + 2-commit phase-exit) on `otel`; ADR §4.1 row 3 commit-range entry"
metrics:
  duration_total: ~31 minutes (across 6 plans)
  completed: 2026-05-02
  loc_added: ~1925 (Plan 03-01 643 + Plan 03-02 329 + Plan 03-03 200 + Plan 03-04 273 + Plan 03-05 20 + this plan)
  files_created: 9
  files_modified: 3
  commits: 7 (1 RED scaffolding + 1 feat + 1 fix + 1 feat + 1 feat + 1 feat + 2 phase-exit)
  coverage_pct: 92.2
  largest_file_loc: 512 (logging.go, Phase 2)
  race_iterations: 5
  race_runs_total: 50
  race_failures: 0
  data_races: 0
commit_range: 1ba8f01..(this commit, 6a)
---

# Phase 3: Metrics Infrastructure & Discipline — SUMMARY

**Phase exit:** 2026-05-02
**Commit range on `otel`:** `1ba8f01..<commit-6a-SHA>` (this SUMMARY commit closes the phase from a tracking standpoint; commit 6b is the audit-row write itself, which is not included in the range it documents — same precedent as Phase 2's `4201a24..021a983`).
**ADR §4.1 row:** 3 (this phase)
**Verifier:** asanabria
**Single-PR strategy:** all Phase 3 commits land directly on `otel` (PR #126).

## One-liner

Two-layer MET-04 defense + four bucket-constant slices + six typed Vec constructors + four exemplar wrapper families + `AssertCardinalityCeiling` test helper + POSIX-portable `make lint-cardinality` gate — assembled into the helper layer Phase 4's ~60 metric families will consume, with hot-path zero-alloc Bind+Observe (MET-25 budget cleared with margin) and Phase-6-forward-compat exemplar emission via the `IsValid() && IsSampled()` chokepoint.

## Deliverables

| Plan | Commit subject (prefix) | Concrete output | Hash |
|------|-------------------------|-----------------|------|
| 03-01 | `test(03-01): wave-0 RED scaffolding ...` | 6 new `_test.go` files (compile-fail by design) | `1ba8f01` |
| 03-02 | `feat(03-02): bucket constants + typed metric constructors + label allow-list (MET-01..MET-05)` | `metrics.go` (226 LOC) + `registration.go` (94 LOC); MET-01..05 GREEN | `9c548fe` |
| 03-02 | `fix(03-02): correct TestBridgeNamespaceParity_NoCollision assertion` | Plan 03-01 RED scaffolding had wrong upstream-behavior assumption (Rule-1 deviation) | `e8beba7` |
| 03-03 | `feat(03-03): exemplar wrapper types + Bind() pre-bound observers + bench (MET-24, MET-25)` | `exemplar.go` (200 LOC); MET-24/25 GREEN; 0 allocs/op hot-path | `5ad7ad0` |
| 03-04 | `feat(03-04): /metrics OpenMetrics negotiation verify + AssertCardinalityCeiling helper (MET-24, TEST-01, TEST-02)` | `testenv.go` extended +94 LOC with `AssertCardinalityCeiling`; TEST-02 GREEN; listener.go verify-only | `e0a1298` |
| 03-05 | `feat(03-05): Makefile lint-cardinality + falsifiability proof (MET-04 belt-and-suspenders)` | `Makefile` `lint-cardinality` target wired into `make test` (sibling of `lint-slog`) | `5d6a243` |
| 03-06 | this SUMMARY commit (6a) + ADR §4.1 row 3 (6b) | this SUMMARY + ADR §4.1 row 3 | (this) + (next) |

## Universal performance/coverage gates (KD-12 / PERF-05 / PERF-06)

| Gate | Threshold | Phase 3 measurement | Status |
|------|-----------|---------------------|--------|
| `pkg/observability` line coverage (PERF-05) | ≥ 90% | **92.2%** (post-Phase-3 final, `go test -coverprofile=cover.out ./pkg/observability/`) | PASS |
| Largest `pkg/observability` production file (PERF-06) | < 800 LOC | **512** (`logging.go`, Phase 2 — Phase 3 max is `testenv.go` at 292 LOC) | PASS |
| `make bench-cypher` Δ | ≤ 2% | N/A — Phase 12 owns; Phase 3 doesn't touch Cypher hot paths | DEFERRED to Phase 12 |
| `make bench-bolt` Δ | ≤ 5% | N/A — Phase 12 owns; Phase 3 doesn't touch Bolt hot paths | DEFERRED to Phase 12 |
| Neo4j wire compatibility | preserved | unchanged in Phase 3 (no Bolt edits) | PASS by inspection |
| Resident memory floor | ≤ 25 MB | N/A — Phase 12 (`TestObservability_MemoryFloor`) | DEFERRED to Phase 12 |
| Race-stability — 5×10 PASS | green | **5/5 iterations × 10 runs = 50 total runs PASS**, 0 DATA RACE, 0 FAIL — see `race-phase3-final.txt` (in addition to per-plan evidence at `race-observe-phase3.txt` and `race-plan04.txt`) | PASS |

## BenchmarkObserve_Hot (Plan 03-03 — D-02b/D-02c)

5-iteration measurement on Apple M1 Max, raw output in `bench-observe-hot-phase3.txt`. Per-shape median across 5 iterations:

| Bench shape | ns/op | B/op | allocs/op | Budget | Verdict |
|-------------|------:|-----:|----------:|--------|---------|
| `BenchmarkObserve_Hot/cold_no_span` | 54.2 | 0 | 0 | informational | — |
| `BenchmarkObserve_Hot/hot_no_span` | 25.6 | 0 | **0** | ≤ 2 (MET-25 / D-02b) | **PASS** (margin: 2 allocs unused) |
| `BenchmarkObserve_Hot/hot_with_span` | 515.0 | 744 | 14 | informational; D-02b1 escalation gates on `hot_no_span` only | NOT TRIGGERED |

The `hot_with_span` cost is dominated by upstream `client_golang`'s `ObserveWithExemplar` internal label-clone path plus the `prometheus.Labels{}` map literal and two `String()` calls on `TraceID`/`SpanID`; this is intentional (it is what makes the exemplar payload visible in OpenMetrics exposition).

**D-02b1 sync.Pool escalation:** NOT TRIGGERED — `hot_no_span = 0 allocs/op` clears the ≤ 2 budget with margin; the escalation gate is on `hot_no_span > 2`, not on `hot_with_span`.

## Race-stability (Phase 1 lesson — 5/5 green is the bar)

```
=== race iteration 1 ===  ok  github.com/orneryd/nornicdb/pkg/observability  5.982s
=== race iteration 2 ===  ok  github.com/orneryd/nornicdb/pkg/observability  5.939s
=== race iteration 3 ===  ok  github.com/orneryd/nornicdb/pkg/observability  5.896s
=== race iteration 4 ===  ok  github.com/orneryd/nornicdb/pkg/observability  5.921s
=== race iteration 5 ===  ok  github.com/orneryd/nornicdb/pkg/observability  5.873s
```

50 race-detector test runs across the post-Plan-03-05 tree. **Zero `DATA RACE` reports; zero `--- FAIL`.** Mirrors Phase 1 race-stability bar (5/5 GREEN), exceeds Phase 2 by being measured against the helper-layer + exemplar-wrapper + `AssertCardinalityCeiling` 8-goroutine fan-out drive (the most concurrent test surface in `pkg/observability` to date).

Per-plan evidence retained: Plan 03-03 → `race-observe-phase3.txt` (5 iter PASS); Plan 03-04 → `race-plan04.txt` (5 iter PASS); Plan 03-06 → `race-phase3-final.txt` (5 iter PASS).

## MET-04 falsifiability proof (Plan 03-05 / D-03d)

From `lint-cardinality-falsifiability.txt` (gitignored evidence file):

- **Injection:** `_ = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "foo_total"}, []string{"path"})` appended to `pkg/cypher/helpers.go:335` under `//go:build never_compile_this_falsifiability_proof` (zero compile risk).
- **`make lint-cardinality` exit code with injection:** **2** (non-zero) — message:
  > `MET-04 violation: subsystems must register metrics via pkg/observability helpers (D-01 / Plan 03-02); see ADR §3.2 'Cardinality discipline' and pkg/observability/metrics.go`
- **`make lint-cardinality` exit code post-revert:** **0** — `lint-cardinality: PASS (MET-04 helper-only registration enforced)`.
- **`git status pkg/cypher/`:** clean post-revert; no injection committed.

## Audit-untouched proof (Pitfall 9 / SEC-05 carried forward from Phase 2)

```
$ git diff --name-only 935389c..5d6a243 pkg/audit/   # Phase 3 base = parent of 1ba8f01 = 935389c
(empty — 0 lines)
```

`pkg/audit/audit.go` retention/signing/serialization unchanged across the cumulative Phase 3 commit range. Pitfall 9 / SEC-05 boundary preserved through 5 substantive Phase 3 commits.

## Requirement traceability

| Req | Status | Test / proof |
|-----|--------|--------------|
| MET-01 | GREEN | `TestMetricNaming_NamespaceInjected` (`metrics_test.go`); `Namespace: "nornicdb"` injected by all 6 ctors; `TestSubsystemValidation_RejectsUnknownSubsystem` enforces closed `allowedSubsystems` list (D-01d) |
| MET-02 | GREEN | `TestNamingValidation_RejectsBadSuffix` (5 cases); `validateNameSuffix` per ctor — counter `_total`, latency `_seconds`, size `_bytes`, rowcount `_rows`, embedding `_seconds` |
| MET-03 | GREEN | `TestBucketConstants_AreSingleSourceOfTruth` (4 hist types); 4 bucket constants in `metrics.go` (`LatencyBucketsSeconds`, `SizeBucketsBytes`, `RowCountBuckets`, `EmbeddingLatencyBucketsSeconds`); subsystem authors literally have no API surface to override |
| MET-04 (Layer 1) | GREEN | `TestForbiddenLabels_PanicsAtRegistration` (60-case Cartesian: 10 ForbiddenLabels × 6 ctors); `TestForbiddenLabels_CaseInsensitive`; `TestForbiddenLabels_AllTenEntriesPresent`; `validateLabels` (`registration.go`) |
| MET-04 (Layer 2) | GREEN | `make lint-cardinality` (`Makefile:871-879`) wired into `make test` as sibling of `lint-slog`; falsifiability proven D-03d (inject → exit 2 → revert → exit 0) |
| MET-05 | GREEN | `TestEmbeddingLatency_UsesLongTailBuckets`; tail of `EmbeddingLatencyBucketsSeconds` = 600s (≥ 60s required) |
| MET-23 | GREEN | `TestBridgeNamespaceParity_NoCollision`; Phase 1 bridge wiring at `registry.go:60-64` (`WithoutUnits()` + `WithNamespace("nornicdb_otel")`) confirmed; `nornicdb_otel_*` family disjoint from hand-instrumented `nornicdb_<subsystem>_*` |
| MET-24 | GREEN | `TestExemplarEmission_OnlyWhenSpanValid` (4 cases: no-context, ctx-no-span, ctx-with-noop-span [unsampled], ctx-with-real-span [sampled]) + `TestListener_OpenMetricsContentTypeRoundTrip` + `TestListener_OpenMetricsExemplarPresent` + `TestListener_EnableOpenMetricsLineExists` (regression gate against Phase-1 line removal) |
| MET-25 | GREEN | `BenchmarkObserve_Hot/hot_no_span` = **0 allocs/op** (≤ 2 budget cleared with margin); `Bind()` returns value-typed `BoundObserver` cached as struct field at construction; D-02b1 sync.Pool escalation NOT triggered |
| TEST-01 | GREEN | All Phase 3 tests use `NewTestEnv(t)` (Phase 1's foundation); `TestNewTestEnv_DefaultRegistererUntouched` and `TestNewRegistry_NoDefaultRegistererPoison` already enforce no Default-registerer leakage |
| TEST-02 | GREEN | `(*TestEnv).AssertCardinalityCeiling` (`testenv.go:271`); `TestAssertCardinalityCeiling_Helper` (3 sub-tests: bounded passes; unbounded falsifiability via `captureT` runHelperCapturing; race-clean parallel) |

## ROADMAP Phase 3 success criteria

| SC | Status |
|----|--------|
| 1. Histogram with arbitrary buckets rejected, pointing to one of the four bucket constants | GREEN — typed-constructor enforcement; no `func NewHistogramVec(... buckets []float64)` exposed; bucket choice encoded into ctor function name |
| 2. Forbidden label rejected at registration with clear error | GREEN — Layer 1 panic (`validateLabels`) + Layer 2 lint (`make lint-cardinality`) |
| 3. `go test -race ./pkg/observability/... -count=10` stable | GREEN — 5×10 = 50 runs PASS across post-Plan-03-05 tree; zero races, zero failures |
| 4. Helper exists for `TestMetricCardinality_<name>` per `*Vec` (Phase 3 ships the helper; Phase 4 ships per-`*Vec` calls) | GREEN — `AssertCardinalityCeiling` helper shipped, self-tested with bounded + unbounded + race-clean fixtures |
| 5. `:9090/metrics` w/ `Accept: application/openmetrics-text` returns OpenMetrics + exemplars; OTel-bridge under `nornicdb_otel_*` (no collision) | GREEN — `TestListener_OpenMetricsContentTypeRoundTrip` + `TestListener_OpenMetricsExemplarPresent` + `TestBridgeNamespaceParity_NoCollision` |

## Phase 4 entry-point readiness

Phase 4 subsystems may now:
1. Inject `*observability.LatencyHistogram` (or other typed wrapper) at construction; cache `BoundLatencyObserver` as struct field for hot-path observation (MET-25 zero-alloc pattern).
2. Receive raw `*prometheus.HistogramVec` via `(*LatencyHistogram).Vec()` for cardinality assertions and edge cases (D-02 dual-access pattern — wrappers don't hide raw types).
3. Test cardinality ceilings via `te.AssertCardinalityCeiling(t, name, ceiling, drive)` — the 1k-UUID concurrent drive lives ONCE in `pkg/observability/testenv.go`, subsystem tests are one-liners.
4. The lint gate (`make lint-cardinality`) enforces helper-only registration in CI as a `make test` prereq.

Forward-compat properties preserved:
- **Phase 5:** `metrics.tenant_labels_enabled` K8s autodetect can suppress `database` label at scrape time without changes to Phase 3 helpers.
- **Phase 6:** sampler flip in `provider.go` (one-line change to `WithSampler(TraceIDRatioBased(0.01))`) lights up exemplars at every Phase-3-wrapped histogram automatically. The `IsValid() && IsSampled()` chokepoint guarantees no exemplars without a corresponding trace in the backend.

## Files changed across Phase 3 commits

| File | Plan | Status | Lines (post-Phase-3) |
|------|------|--------|----------------------|
| `pkg/observability/metrics_test.go` | 03-01 (+03-02 fix) | created (208 LOC) | 208 + 9 fix lines |
| `pkg/observability/registration_test.go` | 03-01 | created | 105 |
| `pkg/observability/exemplar_test.go` | 03-01 (+03-04 backfill) | created (103 LOC) → extended (+123 LOC) | 219 |
| `pkg/observability/exemplar_bench_test.go` | 03-01 | created | 62 |
| `pkg/observability/listener_openmetrics_test.go` | 03-01 | created | 82 |
| `pkg/observability/testenv_cardinality_test.go` | 03-01 (+03-04 fix) | created (83 LOC) → fixed (+56/-7) | 139 |
| `pkg/observability/registration.go` | 03-02 | created | 94 |
| `pkg/observability/metrics.go` | 03-02 | created | 226 |
| `pkg/observability/exemplar.go` | 03-03 | created | 200 |
| `pkg/observability/testenv.go` | 03-04 | extended (+94) | 292 (was 198 pre-phase) |
| `Makefile` | 03-05 | extended (+20/-1) | `lint-cardinality` target + `test:` prereq |
| `docs/architecture/adr/0001-observability.md` | 03-06 (commit 6b) | extended (+1 row) | §4.1 row 3 |
| `pkg/audit/*` | NONE | unchanged across `935389c..HEAD` | unchanged (Pitfall 9 / SEC-05 preserved) |

## Cumulative LOC delta

- pkg/observability production: +520 LOC across 3 new files (`metrics.go` 226 + `registration.go` 94 + `exemplar.go` 200) + 94 LOC extension to `testenv.go`. Total production = 614 LOC added.
- pkg/observability tests: +815 LOC across 6 new test files (~643 LOC RED scaffolding + Plan 03-04 backfill +123 + Plan 03-04 capture pattern +56-7).
- Makefile: +20/-1 lines (lint-cardinality target + test prereq).

## Deviations from plan (Phase 3 cumulative)

Three Rule-1/Rule-2 deviations across Plans 03-02, 03-03, and 03-04 — all auto-fixed (no Rule-4 architectural triggers):

1. **Plan 03-02 [Rule 1 — Bug]:** Plan 03-01 RED scaffolding had wrong upstream-behavior assumption in `TestBridgeNamespaceParity_NoCollision` (assumed `WithoutUnits()` strips `_total`, but that's `WithoutCounterSuffixes`). Fixed in commit `e8beba7`. Architecture invariant unchanged.
2. **Plan 03-03 [Rule 1 — Bug]:** CONTEXT D-02a's snippet guarded only on `IsValid()`. A `SpanContext` from `NeverSample()` returns `IsValid()=true` but `IsSampled()=false`. Plan 03-01 RED test pinned the stricter contract: exemplars MUST NOT attach when unsampled (Tempo would return "trace not found"). Fix: chokepoint guards `IsValid() && IsSampled()`. Documented divergence from Phase 2 D-05 (logs use `IsValid()` alone because grep-fallback exists; exemplars don't have that fallback).
3. **Plan 03-04 [Rule 1 — Bug + Rule 2 — Coverage backfill]:** (1) Go's `t.Run` propagates subtest failures to parent unconditionally — the literal D-04 helper signature `(*testing.T)` could not satisfy the negative-falsifiability sub-test contract. Fix: introduced `CardinalityT` interface + `captureT` fake + `runHelperCapturing` runner; production `*testing.T` callers see no change. (2) Plan 03-03 left 3 of 4 wrapper families at 0% coverage; `TestHistogramWrappers_AllFamilies` (8 sub-tests) brought coverage 89.6% → 92.2% to clear PERF-05.

No Rule-4 architectural changes triggered. Plan 03-05 executed exactly as written. Plan 03-01 had a minor pre-commit grep-gate fix (`CollectAndCount` literal token in comments).

## Authentication gates

None. Phase 3 is pure Go source + Makefile work; no external services, no auth, no CLI handoff.

## TDD Gate Compliance

Phase 3 plans follow the RED → GREEN cadence at the **plan** level (not per-task). Plan 03-01 ships RED scaffolding (`test(03-01): wave-0 RED ...`); Plans 03-02..03-04 turn the build green incrementally (`feat(03-XX): ...`). Compliant with Phase 1/2 wave cadence.

| Wave | Commit prefix | Status |
|------|---------------|--------|
| Wave 0 (RED) | `test(03-01): wave-0 RED scaffolding` (`1ba8f01`) | DONE |
| Wave 1 (GREEN — helpers) | `feat(03-02): bucket constants ...` (`9c548fe`) + `fix(03-02): ...` (`e8beba7`) | DONE |
| Wave 1 (GREEN — exemplar wrappers) | `feat(03-03): exemplar wrapper types ...` (`5ad7ad0`) | DONE |
| Wave 2 (GREEN — listener verify + cardinality helper) | `feat(03-04): /metrics OpenMetrics negotiation verify + AssertCardinalityCeiling ...` (`e0a1298`) | DONE |
| Wave 3 (lint-gate) | `feat(03-05): Makefile lint-cardinality + falsifiability proof ...` (`5d6a243`) | DONE |
| Wave 4 (phase exit) | `docs(03-06): complete Phase 3 — STATE / ROADMAP / REQUIREMENTS post-exit updates` (commit 6a) + `docs(adr-0001): record Phase 3 audit-trail entry in §4.1 row 3` (commit 6b) | this commit + next |

## Project Constraints — Compliance

- [x] **`pkg/audit/` untouched** (Pitfall 9 / SEC-05): `git diff --name-only 935389c..5d6a243 pkg/audit/` returns 0 lines.
- [x] **Single-PR M1 strategy** (memory `project_pr_strategy.md`): all 7 Phase 3 commits land directly on `otel`; no push, no new PR.
- [x] **File-size cap (PERF-06, ≤ 800 LOC per file)**: largest production file is `logging.go` at 512 LOC (Phase 2); largest Phase-3-touched file is `testenv.go` at 292 LOC.
- [x] **Coverage ≥ 90% (PERF-05)**: 92.2% post-Phase-3 final.
- [x] **Race-stability (Phase 1 lesson)**: 5×10 = 50 PASS, 0 races, 0 fails.
- [x] **Leaf-package boundary preserved (OBS-01 / D-01a)**: `pkg/observability/boundary_test.go` continues to enforce no business-package imports; Phase 3 changes only add stdlib + `prometheus`/`otel` deps.
- [x] **Two-commit phase-exit pattern (Phase 0/1/2 precedent)**: this is commit 6a (SUMMARY/STATE/ROADMAP/REQUIREMENTS); the audit-row commit follows as 6b.
- [x] **CLAUDE.md compliance** (cardinality bombs forbidden as labels): `ForbiddenLabels` enumerates all four CLAUDE.md categories (full path, raw query, user/IP/UUID) plus 6 additional defensive entries.

## Threat Surface Scan

No new security-relevant surface introduced beyond what Plan 03-02 documented. Threat register entries:
- **T-DOC-DRIFT (mitigate):** ADR §4.1 row 3 (`1ba8f01..<commit-6a-SHA> | 2026-05-02 | asanabria`) makes the Phase 3 commit range immutably auditable, mirroring Phase 1 row 1 / Phase 2 row 2 verbatim.
- **T-AUDIT-LEAK (mitigate):** `pkg/audit/` untouched proven across the cumulative Phase 3 range.
- **T-CARD (mitigate):** Two-layer MET-04 defense fully operational (Layer 1 runtime panic + Layer 2 CI lint gate).

## Self-Check: PASSED

- [x] `pkg/observability/metrics.go` — FOUND (226 LOC)
- [x] `pkg/observability/registration.go` — FOUND (94 LOC)
- [x] `pkg/observability/exemplar.go` — FOUND (200 LOC)
- [x] `pkg/observability/testenv.go` — FOUND (292 LOC, extended)
- [x] All 6 `_test.go` files from Plan 03-01 — FOUND (verified per Plan 03-01 SUMMARY)
- [x] `Makefile` `lint-cardinality:` target at line 871 — FOUND
- [x] All 5 Phase 3 substantive commits — FOUND on `otel` (`1ba8f01`, `9c548fe`, `e8beba7`, `5ad7ad0`, `e0a1298`, `5d6a243`)
- [x] `pkg/audit/` diff vs `935389c..5d6a243` — empty (0 lines)
- [x] `make lint-slog && make lint-cardinality` — exits 0 (PASS)
- [x] `go test -coverprofile=cover.out ./pkg/observability/` — 92.2% coverage (≥ 90% PERF-05)
- [x] `go test -race -count=10 ./pkg/observability/` × 5 iterations — 50/50 PASS, 0 races, 0 failures
- [x] Bench evidence files retained at `.planning/phases/03-metrics-infrastructure-discipline/bench-observe-hot-phase3.txt`, `race-observe-phase3.txt`, `race-plan04.txt`, `race-phase3-final.txt`, `coverage-phase3-plan04.txt`, `lint-cardinality-falsifiability.txt`
- [x] Phase 4 entry-point ready (helper layer, exemplar wrappers, AssertCardinalityCeiling helper, lint-cardinality gate)

## Phase 3 closes 2026-05-02.
