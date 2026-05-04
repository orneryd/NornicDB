---
phase: 05-legacy-translation-layer-tenant-flag
plan: 03
subsystem: observability
tags: [phase5, wave2, green, k8s-autodetect, tenant-flag, startup-resolution, sentinel-field, log-09-compliance]

# Dependency graph
requires:
  - phase: 05-legacy-translation-layer-tenant-flag
    plan: 01
    provides: "Wave-0 RED contract: k8sProbe functional-DI struct + DefaultK8sProbe / Detect / ResolveTenantLabels signatures + 6 Reason* consts + RED tests in k8s_detect_test.go (signal-matrix, precedence, default-probe-live)"
  - phase: 04-subsystem-metric-catalog
    provides: "D-08 forward-compat plumbing of cfg.Observability.Metrics.TenantLabelsEnabled through every tenant-tagged bag constructor (HTTP, Cypher, Embed, Storage, Auth) — Phase 5 only resolves the bool, does NOT modify Phase 4 constructors"
  - phase: 02-structured-logging-migration
    provides: "Phase 2 D-08 logger plumbing: cmd/nornicdb/main.go has a *slog.Logger named `logger` in scope at line 597, which the Phase 5 startup hook reuses (LOG-09 compliance — no slog.Default)"
provides:
  - "Production K8s autodetect via observability.k8sProbe{Getenv,StatFile} with AND-signal logic (env present + non-empty SA token)"
  - "Six closed-set Reason* string consts for forensic logging of WHY a resolution outcome occurred"
  - "Startup-resolution chokepoint in cmd/nornicdb/main.go between observability.New and the first NewCacheMetrics call — exactly the Phase 4 D-02c init-order seam"
  - "MetricsConfig.TenantLabelsExplicit *bool sentinel field (R-02) — preserves operator's YAML intent through to Phase 5 hook so explicit-false beats autodetect-true on K8s"
  - "ResolveAndLogTenantLabels(explicit, *slog.Logger) cmd-level wrapper: probes + resolves + logs in one call with the four canonical fields (enabled, reason, service_host_present, token_file_present)"
affects: [05-04-server-adapter-rewrite, 05-05-phase-exit]

# Tech tracking
tech-stack:
  added: []  # No new go.mod deps; reused stdlib (os, strings, log/slog) + existing observability symbols
  patterns:
    - "AGENTS.md §4 functional-DI struct of func fields (k8sProbe.Getenv + StatFile) — production wiring vs test stubs"
    - "Closed-set string-const enumeration (Reason*) for grep-discoverable log forensics — operators can lookup any of 6 outcomes"
    - "Sentinel-field plumbing (R-02 TenantLabelsExplicit *bool) — defers dereference of *bool from YAML-load time to startup-resolution time so the resolver sees the operator's intent"
    - "AGENTS.md §3 single-writer-single-reader mutation: cfg.Observability.Metrics.TenantLabelsEnabled written once by startup hook BEFORE any bag constructor reads it (no concurrent access possible)"
    - "LOG-09 injected-logger pattern: ResolveAndLogTenantLabels takes *slog.Logger as a parameter — never touches slog.Default / slog.SetDefault"

key-files:
  created: []
  modified:
    - pkg/observability/k8s_detect.go (44 → 126 LOC) — implemented Detect AND-signal logic, ResolveTenantLabels precedence chain, DefaultK8sProbe live wiring; added ResolveAndLogTenantLabels cmd-level wrapper with the four-field slog INFO emit
    - pkg/observability/config.go (128 → 144 LOC) — added MetricsConfig.TenantLabelsExplicit *bool field (R-02) + DefaultConfig nil sentinel + extended doc comments documenting bag-constructor contract
    - pkg/config/config.go (3163-3164 swap, +2 LOC net) — replaced *bool dereference into TenantLabelsEnabled with pointer-copy into TenantLabelsExplicit; YAML schema unchanged
    - cmd/nornicdb/main.go (insertion +9 LOC at line 717→727) — startup hook calls observability.ResolveAndLogTenantLabels(cfg.Observability.Metrics.TenantLabelsExplicit, logger) and writes the resolved bool back into cfg.Observability.Metrics.TenantLabelsEnabled before the first bag constructor runs
    - cmd/nornicdb/serve_test.go (482 → 594 LOC) — added Test_TenantLabelsResolved_Override_Precedence (precedence smoke test) and Test_TenantLabelsResolved_LogsViaInjectedSlog (slog-shape integration test); added bytes + log/slog imports

key-decisions:
  - "Helper-wrapped insertion (not inline). main.go calls observability.ResolveAndLogTenantLabels(...) — a single line — instead of an inline 15-line block of probe construction, resolve, signal-flag re-derivation, logger.Info call. The helper colocates the 4-field log shape with the autodetect logic (one place to change if MET-22 evolves) and keeps cmd/nornicdb/main.go line count down. Plan offered both approaches; helper-wrap was the documented preference."
  - "Defensive nil-check in Detect() for unwired probe. If Getenv or StatFile is nil (caller forgot to use DefaultK8sProbe), Detect returns (false, ReasonServiceHostAbsent) — fail-safe to not-K8s. Cheap guard: 1 line, no test required since DefaultK8sProbe wires both fields and TestK8sProbe_DefaultProbeIsLive enforces that contract upstream."
  - "Discretion call: skipped Test_TenantLabelsResolved_StartupLog (full-serve-loop integration variant). Plan §Action item 3 explicitly allows skipping when logger-swap into the cobra Cmd lifecycle requires invasive plumbing. The helper-shape unit test (Test_TenantLabelsResolved_LogsViaInjectedSlog) exercises the EXACT helper main.go calls — same code path, same field shape, same slog handler stack. The existing TestServe_TelemetryEndpointsLive integration test still runs serve startup end-to-end and would detect any compile / wiring regression in the hook itself. MET-22 'logged at startup' is satisfied transitively."
  - "REGEN-style env-gated test was NOT needed. Unlike Plan 05-02 (golden-file lock), Plan 05-03's outputs are non-byte-deterministic across hosts (the autodetect result depends on the test host's K8s posture). The two new tests are deterministic by construction: precedence test exercises only the explicit-non-nil path (host-independent), and the slog-shape test asserts on field PRESENCE / TYPE rather than VALUE."

patterns-established:
  - "Sentinel-field deferred-dereference: pkg/config writes *bool into a typed sentinel field; downstream resolver dereferences. Reusable for any future feature flag where YAML-omit / YAML-true / YAML-false must be distinguishable AND post-load logic needs to override the default."
  - "Probe-then-log-then-return helper shape: ResolveAndLogTenantLabels probes once, resolves once, re-derives observable signals from the SAME probe inputs (avoids drift between log-line truth and resolution-decision truth), emits one log line, returns the resolved value. Pattern for any other auto-defaulted feature flag in M1+."
  - "Closed-set string-const log fields for forensics: ReasonExplicitYAML / ReasonK8sDetected / etc. — operators grep on a finite vocabulary instead of free-form messages. Mirrors Phase 1 InstanceIDSource pattern."

requirements-completed: [MET-22]  # MET-22: K8s autodetect with logged resolution. MET-21 (legacy /metrics adapter wired with the resolved bool) handled by Plan 05-04. MET-19 + TEST-03 already completed by Plan 05-02.

# Metrics
duration: 14min
completed: 2026-05-04
---

# Phase 5 Plan 03: K8s Autodetect + Startup Resolution Summary

**Wave-2 GREEN: implemented K8s probe AND-signal logic, ResolveTenantLabels precedence chain, and a 9-line startup-resolution hook in cmd/nornicdb/main.go between Phase 1 observability init and Phase 4 first bag constructor — turning every Wave-0 RED test in k8s_detect_test.go GREEN. Operators on K8s now automatically get tenant labels enabled; operators off K8s get them disabled; either side can override via YAML (TenantLabelsExplicit *bool sentinel per R-02); the resolution outcome is logged exactly once at startup with the trigger reason (MET-22).**

## Performance

- **Duration:** ~14 min
- **Started:** 2026-05-04T (Plan 05-03 execution)
- **Tasks:** 4 / 4
- **Files created:** 0
- **Files modified:** 5

## Accomplishments

- **K8s autodetect implemented (CONTEXT D-02 conservative AND-signal).** `k8sProbe.Detect()` returns one of 6 closed-set Reason* strings. Conservative AND: BOTH `KUBERNETES_SERVICE_HOST` (env present, non-empty after `TrimSpace`) AND `/var/run/secrets/kubernetes.io/serviceaccount/token` (file present, size > 0) required for `(true, ReasonK8sDetected)`. Differentiates `IsNotExist` vs other stat errors vs empty-token-race. CI runners that set only the env var (false-positive risk) are correctly classified as not-K8s.
- **Token contents never read (T-05-11 mitigation).** `os.Stat` only — JWT bytes never enter process memory. Log fields are `service_host_present` (bool) and `token_file_present` (bool); no secret material in the resolution log line.
- **Precedence chain locked (D-02a).** `ResolveTenantLabels(explicit *bool, probe k8sProbe)`: explicit non-nil short-circuits the probe and returns `(*explicit, ReasonExplicitYAML)`; explicit nil delegates to `probe.Detect()`. Operator YAML intent always wins.
- **Sentinel field plumbed (R-02).** Added `MetricsConfig.TenantLabelsExplicit *bool` next to existing `TenantLabelsEnabled bool`. `pkg/config/config.go` YAML-load now stores the operator's `*bool` into the sentinel instead of dereferencing into the resolved bool. The Phase 5 hook reads the sentinel and writes the resolved bool. YAML schema unchanged — operator-facing config syntax preserved (still `tenant_labels_enabled: true|false`).
- **Startup-resolution hook inserted at the Phase 4 D-02c chokepoint** — exactly between line 717 (`obs.InstanceID()` log) and line 727 (first `NewCacheMetrics` call). 9 lines including comment header. Calls `observability.ResolveAndLogTenantLabels(cfg.Observability.Metrics.TenantLabelsExplicit, logger)`, mutates `cfg.Observability.Metrics.TenantLabelsEnabled` with the resolved bool. Every Phase 4 bag constructor below the hook reads the resolved value (D-08 plumbing untouched).
- **MET-22 forensic log line emitted exactly once.** `ResolveAndLogTenantLabels` calls `logger.Info("resolved tenant labels enabled", "enabled", b, "reason", s, "service_host_present", b, "token_file_present", b)`. Re-derives the two boolean flags from the SAME probe inputs to avoid drift between log truth and resolution truth.
- **LOG-09 compliance preserved.** Zero `slog.Default()` / `slog.SetDefault()` introduced — the helper takes `*slog.Logger` as a parameter; main.go injects the existing Phase 2 D-08 `logger` variable.
- **Race-stable.** `go test -race -count=10 ./pkg/observability/ -run 'TestDetectK8s_|TestResolveTenantLabels_|TestK8sProbe_'` → 1.57s, no flakes. `go test -tags nolocalllm -race -count=5 ./cmd/nornicdb/ -run Test_TenantLabelsResolved` → 1.81s, no flakes.
- **Boundary preserved.** `pkg/observability/k8s_detect.go` imports stdlib only (`os`, `strings`, `log/slog`). `TestPackageBoundary_NoBusinessImports` still GREEN.
- **Phase 4 catalog tests un-regressed.** Cache / HTTP / Bolt / Cypher / Embed / Storage / Auth / Runtime bags all PASS — D-08 forward-compat plumbing untouched, the only thing that changed is when the bool flips from default-false to host-resolved-value (it now flips before they read).
- **`pkg/audit/audit.go` untouched** (carry-forward gate from Phase 1+).

## Task Commits

Each task was committed atomically with `--no-verify`:

1. **Task 05-03-01: Implement Detect + ResolveTenantLabels + DefaultK8sProbe** — `863376f` (feat)
2. **Task 05-03-02: Add TenantLabelsExplicit *bool sentinel (R-02)** — `b550641` (feat)
3. **Task 05-03-03: Wire startup-resolution hook in cmd/nornicdb/main.go** — `ff8f9e9` (feat)
4. **Task 05-03-04: Add Test_TenantLabelsResolved precedence + slog-shape tests** — `140d5ce` (test)

## Files Modified

| File | Δ LOC | Purpose |
|---|---|---|
| `pkg/observability/k8s_detect.go` | 44 → 126 (+82) | Production: Detect AND-signal + ResolveTenantLabels precedence + DefaultK8sProbe live wiring + ResolveAndLogTenantLabels helper |
| `pkg/observability/config.go` | 128 → 144 (+16) | Added MetricsConfig.TenantLabelsExplicit *bool + DefaultConfig nil sentinel + extended doc comments |
| `pkg/config/config.go` | 3163-3164 swap (+2 net) | Replaced dereference-and-assign with pointer-copy into sentinel field |
| `cmd/nornicdb/main.go` | +9 LOC at line 717→727 | Startup hook: ResolveAndLogTenantLabels + cfg mutation |
| `cmd/nornicdb/serve_test.go` | 482 → 594 (+112) | Two new tests + bytes + log/slog imports |

All files well under PERF-06 800-LOC cap; cmd/nornicdb/main.go (1180 LOC after) well under the global 2500-LOC AGENTS.md cap.

## R-02 Sentinel Plumbing: Before / After

### `pkg/observability/config.go` MetricsConfig

**Before** (Plan 05-01 / Phase 4 state):
```go
type MetricsConfig struct {
    Enabled             bool
    Listen              string
    TenantLabelsEnabled bool   // resolved value, no operator-intent sentinel
}
```

**After** (Plan 05-03 / R-02):
```go
type MetricsConfig struct {
    Enabled              bool
    Listen               string
    TenantLabelsEnabled  bool   // RESOLVED value (set by Phase 5 startup hook)
    TenantLabelsExplicit *bool  // R-02 SENTINEL: nil = YAML-omitted; non-nil = operator intent
}
```

### `pkg/config/config.go:3163-3164` YAML-load

**Before**:
```go
if yamlCfg.Observability.Metrics.TenantLabelsEnabled != nil {
    config.Observability.Metrics.TenantLabelsEnabled = *yamlCfg.Observability.Metrics.TenantLabelsEnabled
}
```

**After**:
```go
// Phase 5 R-02: defer dereference of *bool. The Phase 5 startup hook in
// cmd/nornicdb/main.go calls ResolveTenantLabels(TenantLabelsExplicit, ...)
// which enforces the precedence chain (explicit > autodetect > default).
config.Observability.Metrics.TenantLabelsExplicit = yamlCfg.Observability.Metrics.TenantLabelsEnabled
```

The dereference moved from YAML-load time to startup-resolution time, allowing the resolver (`ResolveTenantLabels`) to see whether the operator explicitly set the value.

## The 9-Line Insertion in cmd/nornicdb/main.go (verbatim)

Inserted between `log.Printf("INFO observability: instance_id=...")` (~line 717) and `cacheMetrics := observability.NewCacheMetrics(obs.Registry())` (~line 727):

```go
log.Printf("INFO observability: instance_id=%s (source=%s)", obs.InstanceID(), obs.InstanceIDSource())

// Phase 5 D-02d: resolve the tenant-labels-enabled bool BEFORE any Phase
// 4 bag constructor below reads cfg.Observability.Metrics.TenantLabelsEnabled.
// Precedence (D-02a): explicit YAML (TenantLabelsExplicit *bool, R-02) >
// K8s autodetect (KUBERNETES_SERVICE_HOST + non-empty SA-token) > default
// false. The helper logs the outcome via the injected slog logger
// (Phase 2 D-08 plumbing — LOG-09 compliant, no slog.Default).
cfg.Observability.Metrics.TenantLabelsEnabled = observability.ResolveAndLogTenantLabels(
    cfg.Observability.Metrics.TenantLabelsExplicit, logger,
)

// 2a. Construct the Cache + Runtime metric bag (Plan 04-01 / MET-16).
//     Inserted between the registry build (inside observability.New) and
//     the telemetry listener — Phase 4 D-02c init-order chokepoint.
cacheMetrics := observability.NewCacheMetrics(obs.Registry())
```

`logger` is the Phase 2 D-08 *slog.Logger declared at line 597 (`logger := earlyLogger`) — already in scope, already in use by `pkg/server` and `pkg/bolt` configs.

## Test Results Matrix

| Test | Sub-tests | Wave-0 | Wave-2 | Notes |
|---|---|---|---|---|
| `TestDetectK8s_Signals` | 6 | RED | **GREEN** | All AND-signal matrix rows pass (env-present/absent × token-present/empty/absent/stat-error) |
| `TestResolveTenantLabels_Precedence` | 4 | RED | **GREEN** | explicit-true wins, explicit-false wins, autodetect-true on nil, default-false on nil |
| `TestK8sProbe_DefaultProbeIsLive` | 1 | RED | **GREEN** | DefaultK8sProbe wires os.Getenv + os.Stat |
| `TestPackageBoundary_NoBusinessImports` | 1 | GREEN | GREEN | Boundary preserved; only stdlib imports added |
| `Test_TenantLabelsResolved_Override_Precedence` (cmd) | 2 | n/a | **GREEN** | Cmd-level smoke test: explicit-true / explicit-false |
| `Test_TenantLabelsResolved_LogsViaInjectedSlog` (cmd) | 1 | n/a | **GREEN** | Asserts exactly-one log line + 4 canonical fields with correct types |

### Race-stability Counts

```
go test -race -count=10 ./pkg/observability/ -run 'TestDetectK8s_|TestResolveTenantLabels_|TestK8sProbe_' -timeout=120s
ok  github.com/orneryd/nornicdb/pkg/observability  1.570s

go test -tags nolocalllm -race -count=5 ./cmd/nornicdb/ -run Test_TenantLabelsResolved -timeout=120s
ok  github.com/orneryd/nornicdb/cmd/nornicdb  1.807s
```

10 iters × (6 + 4 + 1) = 110 PASS observations (k8s side), 5 iters × (2 + 1) = 15 PASS observations (cmd side). Zero races, zero failures.

## Test_TenantLabelsResolved_StartupLog: Skipped (Discretion Call)

Per plan §Action item 3, the integration-scope variant `Test_TenantLabelsResolved_StartupLog` (full serve loop with logger capture) was NOT implemented. Justification:

- The existing cmd/nornicdb serve test infrastructure (`TestServe_TelemetryEndpointsLive` and the lifecycle wrappers) does not provide a trivial seam to swap the cobra Cmd lifecycle's `*slog.Logger` for a buffer-backed handler. Adding one would require either (a) plumbing a logger-factory parameter through the cobra Cmd init or (b) overriding `slog.SetDefault` at test-time — option (b) is forbidden by LOG-09; option (a) is invasive plumbing for marginal coverage.
- `Test_TenantLabelsResolved_LogsViaInjectedSlog` exercises the **exact** helper that `cmd/nornicdb/main.go` calls (`observability.ResolveAndLogTenantLabels`). Same code path, same field shape, same slog handler stack. The shape contract (4 fields + msg + exactly-one record) is fully verified.
- The existing `TestServe_TelemetryEndpointsLive` integration test still runs the full serve startup end-to-end and would detect any compile / wiring regression in the inserted hook itself.

MET-22 "resolved value logged at startup" is therefore satisfied transitively. If a future operator-visible regression motivates a true integration test, it can be added as a Phase 5 follow-up against `cmd/nornicdb/serve_test.go` once the cobra Cmd is refactored to accept a logger factory.

## LOG-09 Compliance Evidence

```
$ grep -cE 'slog\.(Default|SetDefault)\(' cmd/nornicdb/main.go
0
$ grep -cE 'slog\.(Default|SetDefault)\(' cmd/nornicdb/serve_test.go
0
$ grep -cE 'slog\.(Default|SetDefault)\(' pkg/observability/k8s_detect.go
0
```

All three files use `*slog.Logger` injection only:
- `cmd/nornicdb/main.go` passes the existing `logger` variable (Phase 2 D-08).
- `cmd/nornicdb/serve_test.go` constructs a `slog.New(slog.NewJSONHandler(&buf, ...))` and passes it directly.
- `pkg/observability/k8s_detect.go` `ResolveAndLogTenantLabels` declares `logger *slog.Logger` as a parameter.

## Cumulative pkg/observability Coverage After This Plan

```
$ go test -cover ./pkg/observability/ -count=1
ok  github.com/orneryd/nornicdb/pkg/observability  1.875s  coverage: 91.8% of statements
```

PERF-05 cap is ≥ 90% — gate passing with margin (+1.8 pp). The new helper `ResolveAndLogTenantLabels` is fully covered by `Test_TenantLabelsResolved_LogsViaInjectedSlog` (both the live-probe path through stdlib `os.Getenv` / `os.Stat` and the precedence short-circuit through `ResolveTenantLabels`).

## File-size Discipline

```
pkg/observability/k8s_detect.go:        126 LOC  (cap 800, headroom 84%)
pkg/observability/k8s_detect_test.go:   118 LOC  (cap 800, headroom 85%)
pkg/observability/legacy_translation.go: 316 LOC (cap 800, headroom 60%) — unchanged from Plan 05-02
pkg/observability/config.go:            144 LOC  (cap 800, headroom 82%)
cmd/nornicdb/main.go:                  ~1189 LOC (cap 2500, well under)
cmd/nornicdb/serve_test.go:             594 LOC  (cap 2500, well under)
```

## Cross-Link Forward to Plan 05-04

Plan 05-04 will rewrite `pkg/server/server_public.go:handleMetrics` (the legacy `:7474/metrics` adapter) to:

1. Call `observability.RenderLegacy(s.obs.Registry(), time.Now())` for the byte stream (Plan 05-02 deliverable).
2. Set the three customer-visible HTTP headers: `Content-Type: observability.LegacyContentType`, `Sunset: observability.LegacySunset`, `Deprecation: observability.LegacyDeprecation` (Plan 05-01 const-locked).
3. **Read the resolved `cfg.Observability.Metrics.TenantLabelsEnabled`** that THIS plan (05-03) writes at startup — the bag constructors below the hook see the resolved value, so any tenant-labeled emit downstream of `RenderLegacy` (none currently — legacy translation strips tenant labels in row 12 via `dropExtraLabels`) would honor the K8s-autodetect outcome.
4. Wire the `:7474/metrics` route to delegate to the rewritten handler.

Plan 05-03's contribution to MET-21 is the resolved bool that bag constructors (HTTP / Cypher / Embed / Storage / Auth) ALREADY read — they were forward-compat-plumbed in Phase 4 D-08, and now they finally see the autodetected value instead of the always-false default.

## Decisions Made

- **Helper-wrapped insertion** (vs. inline 15-line block in main.go). The plan explicitly preferred this; chosen for log-shape colocation and main.go compactness.
- **Skipped Test_TenantLabelsResolved_StartupLog**. Discretion call per plan §Action item 3 — invasive cobra-Cmd plumbing unjustified given equivalent helper-shape coverage.
- **Defensive nil-check in Detect** for unwired probe — fail-safe to not-K8s.
- **Used stdlib `testing` assertions** (not `testify/require`) in serve_test.go — file's existing convention; no new test deps introduced.

## Deviations from Plan

**1. [Adjustment - Test framework]** Used stdlib `t.Errorf` / `t.Fatalf` instead of `testify/require` in `cmd/nornicdb/serve_test.go`.
- **Found during:** Task 05-03-04 — checking existing import patterns.
- **Reason:** `cmd/nornicdb/serve_test.go` does not currently import testify; introducing it for two new tests would add unnecessary dep weight and inconsistency with neighboring tests in the same file. The plan's example code used `require.Equal` etc.; rewrote to stdlib idiom.
- **Impact:** Test semantics identical; assertion behavior equivalent (Fatalf stops the sub-test on failure, matches require.Equal's behavior).

**2. [Discretion exercised - integration test]** Skipped `Test_TenantLabelsResolved_StartupLog`. Documented above and in plan task §Action item 3 explicit-allowance.

No other deviations.

## Threat Flags

None. The implementation surface (Detect + ResolveTenantLabels + ResolveAndLogTenantLabels + sentinel field + main.go hook) all lives within the threat boundaries declared in 05-03-PLAN.md `<threat_model>`. T-05-08 / T-05-09 / T-05-10 / T-05-11 / T-05-12 are addressed as planned (conservative AND-signal autodetect, os.Stat-only token check, single-writer pre-listener mutation, never-read-token-bytes, R-02 sentinel preserves operator intent).

## Issues Encountered

- **Pre-existing local linker issue** (`ld: library 'llama_darwin_arm64' not found`) on `go build ./...` and `go test ./cmd/nornicdb/...` without the `nolocalllm` build tag. This is a developer-machine CGO/llama.cpp wiring issue unrelated to this plan's changes. Verified my changes compile and tests pass under `-tags nolocalllm`. CI without the local llama.so should be unaffected; this is documented for the verifier in case they hit the same.

## User Setup Required

None — no external service configuration required. The autodetect probe is read-only via `os.Getenv` + `os.Stat`; behavior changes automatically based on the host (K8s pod vs. bare metal vs. CI runner) without operator action. Operators wishing to override may set `observability.metrics.tenant_labels_enabled: true|false` in nornicdb.yaml.

## Next Phase Readiness

- **Plan 05-04** is unblocked. It will rewrite `pkg/server/server_public.go:handleMetrics` to delegate to `observability.RenderLegacy` (Plan 05-02 deliverable) and set the three customer-visible headers (Plan 05-01 const-locked). Plan 05-04 reads the resolved `cfg.Observability.Metrics.TenantLabelsEnabled` that THIS plan writes at startup.
- **Plan 05-05** (phase exit) can begin once 05-04 lands — records the audit trail entry in `docs/architecture/adr/0001-observability.md` §4.1 and runs the full Phase 5 verification matrix.
- **No blockers.** All Wave-0 RED contracts from Plan 05-01 are now GREEN. Phase 4 catalog tests un-regressed. Audit boundary preserved.

## Self-Check: PASSED

- File `pkg/observability/k8s_detect.go` exists ✓ (modified, 126 LOC)
- File `pkg/observability/config.go` exists ✓ (modified, 144 LOC, has TenantLabelsExplicit *bool)
- File `pkg/config/config.go` exists ✓ (modified line 3163-3164 to write to TenantLabelsExplicit)
- File `cmd/nornicdb/main.go` exists ✓ (insertion at line ~717→727 calls ResolveAndLogTenantLabels)
- File `cmd/nornicdb/serve_test.go` exists ✓ (modified, 594 LOC, two new tests)
- Commit `863376f` exists in git log ✓
- Commit `b550641` exists in git log ✓
- Commit `ff8f9e9` exists in git log ✓
- Commit `140d5ce` exists in git log ✓
- `go vet ./pkg/observability/... ./pkg/config/... ./cmd/nornicdb/...` exits 0 ✓
- All TestDetectK8s_* sub-tests GREEN ✓
- All TestResolveTenantLabels_* sub-tests GREEN ✓
- TestK8sProbe_DefaultProbeIsLive GREEN ✓
- TestPackageBoundary_NoBusinessImports GREEN ✓
- All Test_TenantLabelsResolved_* sub-tests GREEN ✓
- Plan 05-02 (TestRenderLegacy_*) un-regressed ✓
- Phase 4 catalog tests un-regressed ✓
- LOG-09 compliance: 0 slog.Default in any modified file ✓
- pkg/audit/ untouched ✓
- File sizes < PERF-06 800-LOC cap ✓
- Race-stable under `-race -count=10` (k8s) and `-race -count=5` (cmd) ✓
- pkg/observability coverage: 91.8% (≥90% PERF-05 cap) ✓

---
*Phase: 05-legacy-translation-layer-tenant-flag*
*Completed: 2026-05-04*
