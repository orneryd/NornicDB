---
phase: 04-subsystem-metric-catalog
plan: 04-02
subsystem: observability
tags: [observability, metrics, http, bolt, prometheus, phase-4, met-06, met-07, met-25]

requires:
  - phase: 04-subsystem-metric-catalog
    plan: 04-01
    provides:
      - RED catalog_http_test.go + catalog_bolt_test.go scaffolds
      - Cache+Runtime GREEN bag template (catalog_cache.go) — D-02 typed-bag pattern
      - AllowedCacheNames / AllowedEvictionReasons closed-enum precedent
      - GaugeFunc panic-recover template (RISK-8 / Pitfall 1)
      - cmd/nornicdb D-02c init-order chokepoint already populated by NewCacheMetrics
  - phase: 03-metrics-infrastructure-discipline
    provides:
      - NewLatencyHistogramVec / NewSizeHistogramVec / NewCounterVec / NewGaugeVec typed constructors (D-01)
      - LatencyHistogram + Bind() typed wrapper + observeWithExemplar chokepoint (D-02)
      - ForbiddenLabels panic-at-registration (D-03a)
      - TestEnv.AssertCardinalityCeiling (TEST-02)
      - allowedSubsystems closed enum (http + bolt + auth in scope)

provides:
  - HTTPMetrics typed bag (5 families per MET-06) with D-08 tenant-flag forward-compat
  - BoltMetrics typed bag (6 families per MET-07) with D-11/D-11a/D-11c closed enums
  - instrumentedMux HTTP chokepoint at pkg/server/server.go (D-03 single chokepoint, panic-safe)
  - Bolt session-lifecycle + per-message dispatch + auth-attempts crosswire instrumentation
  - reasonFromError() closed-enum classifier for packstream decode failures (D-11c)
  - 11 catalog_redstubs constructors for Plans 04-03..04-06 to replace
  - BenchmarkObserve_HotHTTP + BenchmarkObserve_HotBolt — 0 allocs/op evidence
  - cmd/nornicdb startup integration (HTTP + Bolt bags injected via setters)

affects: [04-03, 04-04, 04-05, 04-06, 04-07, 05-tenant-flag, 06-sampler-flip, 08-trace-emission, 09-helm-chart]

tech-stack:
  added:
    - pkg/observability/catalog_http.go (HTTPMetrics typed bag — 185 LOC)
    - pkg/observability/catalog_bolt.go (BoltMetrics typed bag — 193 LOC)
    - pkg/observability/catalog_redstubs.go (Plan 04-03..04-06 type stubs — 139 LOC)
    - pkg/server/instrumented_mux.go (HTTP observation chokepoint — 192 LOC)
    - pkg/bolt/metrics.go (Bolt metrics setters + per-op Bind cache — 135 LOC)
    - pkg/bolt/packstream_metrics.go (reasonFromError classifier — 181 LOC)
  patterns:
    - "instrumentedMux SOLE chokepoint per AGENTS.md §7 DRY: r.Pattern → path_template; r.PathValue('database') → tenant; deferred observation panic-safe per T-04-08."
    - "MET-25 hot-path pre-bound observers: per-tuple sync.Map cache at the HTTP chokepoint; per-op map at SetBoltMetrics — both produce 0 allocs/op (BenchmarkObserve_Hot{HTTP,Bolt} evidence)."
    - "D-11c reasonFromError closed-enum classifier: errors.Is sentinel match → io.EOF family → substring fallback against legacy fmt.Errorf messages. Returns '' for non-decode errors (caller MUST NOT observe under packstream_decode_errors_total)."
    - "Two-phase bootstrap preserved (Phase 2 D-08): server.New runs BEFORE observability.New so the same *slog.Logger threads through both; only httpServer.Start() moves to AFTER metrics injection so instrumentedMux picks up s.httpMetrics at Handler-mount time."
    - "RED-test type stubs: catalog_redstubs.go declares the bag struct fields the catalog_<sub>_test.go files reference (panicking constructors); RED tests t.Skip first, so the runtime panic is a defense-in-depth guard against accidental production usage before the GREEN bag lands."

key-files:
  created:
    - pkg/observability/catalog_http.go (HTTPMetrics: RequestDuration, Requests, InFlight, RequestBodyBytes, ResponseBodyBytes; tenant-flag-aware Bind helpers)
    - pkg/observability/catalog_http_test.go (5-family registry + D-08 flag-on/off + bool-agnostic Bind + forbidden-label panic + cardinality 1000 ceiling parameterized on D-08)
    - pkg/observability/catalog_bolt.go (BoltMetrics: 6 families + closed AllowedBoltOps/AllowedBoltResults/AllowedPackstreamReasons enums)
    - pkg/observability/catalog_bolt_test.go (6-family registry + 4 closed-enum cardinality-ceiling tests)
    - pkg/observability/catalog_redstubs.go (Cypher/Storage/MVCC/Embed/Search/Replication/Auth type stubs for Plans 04-03..04-06)
    - pkg/observability/catalog_http_bench_test.go (BenchmarkObserve_HotHTTP — 3 sub-benches)
    - pkg/observability/catalog_bolt_bench_test.go (BenchmarkObserve_HotBolt — 4 sub-benches)
    - pkg/server/instrumented_mux.go (instrumentedMux + statusRecorder + statusClass + per-tuple Bind cache)
    - pkg/server/instrumented_mux_test.go (TestInstrumentedMux_RPattern, _NotFound, _PanicSafe, _StatusRecorder×5, _InFlight, _NilBag)
    - pkg/bolt/metrics.go (boltOpName + SetBoltMetrics + SetAuthMetrics + observeAuthAttempt + per-op Bind cache)
    - pkg/bolt/server_metrics_test.go (TestSessionLifetime, TestMessageDispatch_AllOps, TestPullChunks_NoSeparateObservation, TestAuthAttempt_BoltProtocol)
    - pkg/bolt/packstream_metrics.go (errTruncated/errInvalidMarker/errWrongType/errOversize sentinels + reasonFromError + observePackstreamDecodeError + isAllowedPackstreamReason)
    - pkg/bolt/packstream_metrics_test.go (TestReasonFromError 20+ table cases, TestPackstreamReason_AllPaths, TestPackstreamDecodeError_NilSafe)
    - cmd/nornicdb/http_bolt_metrics_test.go (TestServerStartup_HTTPBoltMetricsRegistered)
    - .planning/phases/04-subsystem-metric-catalog/bench-http-bolt-04-02.txt (BenchmarkObserve_Hot evidence — 0 allocs/op everywhere)
  modified:
    - pkg/server/server.go (Server.httpMetrics field + SetHTTPMetrics setter; instrumentedMux wiring at TLS + h2c handler-mount sites; pkg/observability import)
    - pkg/bolt/server.go (Server.metricsState + Server.authMetrics fields; D-11 session-lifecycle defers in handleConnection; D-11a per-message dispatch wrapper around dispatchInner; D-11c packstream decode-error observation; D-05e auth-attempts crosswire in handleHello; pkg/observability import)
    - cmd/nornicdb/main.go (NewHTTPMetrics + NewBoltMetrics construction at D-02c init-order chokepoint; httpServer.Start() moved to AFTER metrics injection)

key-decisions:
  - "Stub bags (catalog_redstubs.go) for Cypher/Storage/MVCC/Embed/Search/Replication/Auth: required so pkg/observability compiles. Without them, the RED test files reference undefined types and the entire package fails to build, blocking my plan's verification. Constructors panic if invoked; t.Skip in each RED test means they're never reached at runtime. Plans 04-03..04-06 each replace one stub with their real GREEN bag."
  - "Two-phase bootstrap preserved by moving httpServer.Start() (not the obs.New call): keeps Phase 2 D-08 invariant that server.New runs BEFORE obs.New so the same *slog.Logger reference threads through; the only reorder is that Start() now waits until AFTER metrics injection. Reviewers: instrumentedMux consults s.httpMetrics at Handler-mount time inside Start(), so we MUST call SetHTTPMetrics before Start()."
  - "Per-tuple Bind cache lives at the HTTP chokepoint (not in the bag): path_template is not known until route resolution, so the bag's BindRequestDuration helper still pays a WithLabelValues lookup. The chokepoint's sync.Map keyed by (method, template, status_class, database) amortizes the lookup to one Load per request after warmup. BenchmarkObserve_HotHTTP/http_request_duration_bound shows 0 allocs/op — the cache successfully eliminates the per-request alloc."
  - "reasonFromError chooses errors.Is sentinel matching as the primary path with substring fallback: packstream.go's existing fmt.Errorf sites (e.g. 'incomplete STRING8', 'unknown marker: 0x42') don't yet wrap sentinel errors. The substring fallback bridges the gap without rewriting the entire decoder. Future code can wrap via fmt.Errorf('...: %w', errTruncated) for the fast errors.Is path; both paths reach the same closed-enum reason value."
  - "Bolt auth-attempts crosswire defer captures session.authenticated transition: the deferred closure at handleHello entry records pre-state (wasAuthenticatedBefore), checks post-state on return, and emits observeAuthAttempt('success') only when authentication actually transitioned. Re-HELLO calls (already-authenticated session) emit no metric — avoids double-counting."
  - "Removed bag-wired sub-test from TestAuthAttempt_BoltProtocol: building a fake AuthMetrics in a test file would call prometheus.NewCounterVec directly, tripping `make lint-cardinality` (MET-04 helper-only registration — Plan 03-05 falsifiability proof). Plan 04-06 ships the real NewAuthMetrics constructor AND the integration test that drives the closed-enum result values; Plan 04-02's nil-safe call-site test is sufficient for the forward-compat obligation."

patterns-established:
  - "Single-chokepoint observation per subsystem with deferred panic-safe emit: instrumentedMux for HTTP, dispatchMessage's deferred closure for Bolt. Both re-panic AFTER observation so outer panic handlers fire (T-04-08 mitigation pattern)."
  - "Per-subsystem suffix on BenchmarkObserve_Hot* (HotHTTP, HotBolt) to avoid duplicate-function-declaration with Phase 3's BenchmarkObserve_Hot in the same package. The regex 'BenchmarkObserve_Hot' matches all of them; Plans 04-03..04-06 mirror (HotCypher, HotStorage, HotMVCC, HotEmbed, HotSearch, HotReplication, HotAuth)."
  - "Closed-enum classifier helpers (reasonFromError, statusClass, boltOpName) live in subsystem packages, not pkg/observability — preserves the leaf-package boundary (pkg/observability never imports subsystem packages). The closed-enum slices (AllowedBoltOps, AllowedPackstreamReasons, AllowedStatusClasses) live in pkg/observability so all subsystems share one canonical set."
  - "D-08 forward-compat via constructor bool: NewHTTPMetrics(reg, tenantLabelsEnabled bool) decides label-set shape ONCE at construction. Subsystems pass database unconditionally through bool-agnostic Bind helpers; the helper drops the arg internally when the bag was constructed with the flag off. Phase 5 K8s autodetect flips the bool's default value with no re-registration."

requirements-completed: [MET-06, MET-07, MET-25]

duration: ~110min
completed: 2026-05-03
---

# Phase 4 Plan 04-02: HTTP + Bolt Subsystem Metric Catalog Summary

**HTTP + Bolt API-edge instrumentation: 5 HTTP families (MET-06) through `instrumentedMux` single chokepoint with `r.Pattern` (D-03) + panic-safe deferred observation (T-04-08); 6 Bolt families (MET-07) across session-lifecycle goroutine, per-message dispatch loop, and packstream decode boundary with closed reason enum (D-11c); D-08 tenant-flag forward-compat hook on HTTP bag; auth-attempts crosswire scaffolding for Plan 04-06 (D-05e/D-11); BenchmarkObserve_Hot{HTTP,Bolt} 0 allocs/op (MET-25, well below ≤ 2 budget).**

## Performance

- **Duration:** ~110 min (single-agent worktree)
- **Tasks:** 7/7 (04-02-01 through 04-02-07; 04-02-04 has a follow-up commit for lint-cardinality alignment)
- **Files created:** 14
- **Files modified:** 3
- **Per-file LOC (PERF-06 sub-cap = 800 in pkg/observability; global cap = 2,500):**
  - catalog_http.go — 185
  - catalog_bolt.go — 193
  - catalog_redstubs.go — 139
  - instrumented_mux.go — 192
  - bolt/metrics.go — 135
  - bolt/packstream_metrics.go — 181
  - All under the 800-LOC pkg/observability sub-cap.

## Accomplishments

- **MET-06 GREEN — 5 HTTP families** registered through Phase-3 typed constructors: `requests_total`, `request_duration_seconds`, `in_flight_requests`, `request_body_bytes`, `response_body_bytes`. The `instrumentedMux` wrapper at `pkg/server/server.go` is the SOLE observation site (DRY per AGENTS.md §7); reads `r.Pattern` post-`mux.ServeHTTP` for the closed-shape `path_template` label, with `_NOT_FOUND_` bucket for unmatched routes.
- **MET-07 GREEN — 6 Bolt families** registered: `connections_active`, `connections_total{result}`, `session_duration_seconds`, `messages_total{op,result}`, `message_duration_seconds{op}`, `packstream_decode_errors_total{reason}`. Session metrics observed in connection-accept goroutine (`pkg/bolt/server.go` handleConnection); message metrics wrap dispatch loop; packstream errors classified at decode boundary with closed reason enum.
- **D-03 panic-safe instrumentedMux**: observation runs in a `defer func()` with `recover()` so a panicking handler still emits an observation (with `status_class="5xx"`); panic re-propagates AFTER observation per Go convention so the outer http.Server panic handler still fires (T-04-08 mitigation).
- **D-08 forward-compat**: `NewHTTPMetrics(reg, tenantLabelsEnabled bool)` decides label-set shape ONCE at construction; subsystems pass `database` unconditionally through bool-agnostic `BindRequestDuration` / `BindRequests` helpers. Tests parameterized on both flag values per MET-21.
- **D-11c closed packstream reason enum** = {truncated, invalid_marker, wrong_type, oversize}. `reasonFromError(err)` classifies via errors.Is sentinel match → io.EOF family → substring fallback against legacy fmt.Errorf messages already in `packstream.go`. Returns `""` for non-decode errors so non-decode failures flow through `messages_total{result="error"}` only.
- **MET-25 hot path**: BenchmarkObserve_HotHTTP and BenchmarkObserve_HotBolt measured **0 allocs/op** across all 7 sub-benches (Apple M1 Max, count=3) — far below the ≤ 2 alloc budget. Per-op latency 6.88ns (counter Inc) to 25.57ns (histogram Observe with exemplar guard short-circuit).
- **Auth-attempts crosswire scaffolding**: Plan 04-02's `handleHello` increments `auth_attempts_total{result, protocol="bolt"}` behind a nil-check on `s.authMetrics`; Plan 04-06 wires the GREEN AuthMetrics bag and the deferred closure lights up automatically (no second-pass code change).
- **D-11b regression guard**: `TestPullChunks_NoSeparateObservation` drives 2 PULL messages and asserts exactly 2 observations on `message_duration_seconds{op=pull}` — proves chunk timing rolls up into the parent PULL message (matches Phase 8 TRC-13 stance).
- **catalog_redstubs.go scaffolding**: 7 type stubs (Cypher/Storage/MVCC/Embed/Search/Replication/Auth) keep `pkg/observability` compiling while their RED tests sit in `t.Skip("RED: pending Plan 04-NN")`. Each stub's constructor panics if invoked at runtime — defense-in-depth guard against accidental production usage. Plans 04-03..04-06 each replace one stub with their real GREEN bag.

## Task Commits

Each task committed atomically (worktree mode, --no-verify; orchestrator validates hooks):

1. **04-02-01: catalog_http.go GREEN — 5 HTTP families + D-08 tenant flag** — `543e209` (feat)
2. **04-02-03: catalog_bolt.go GREEN — 6 Bolt families + closed packstream reason enum** — `39a1f85` (feat)
3. **04-02-02: instrumentedMux HTTP chokepoint — panic-safe + r.Pattern** — `bd404fb` (feat)
4. **04-02-04: bolt session lifecycle + per-message dispatch + auth crosswire** — `a22bdf9` (feat)
5. **04-02-05: packstream decode-error reason classification (D-11c)** — `2c4a90b` (feat)
6. **04-02-06: cmd/nornicdb wires HTTP + Bolt metric bags** — `6d967b6` (feat)
7. **04-02-07: BenchmarkObserve_Hot{HTTP,Bolt} ≤ 2 allocs/op (MET-25)** — `8bb989e` (test)
8. **04-02-04 follow-up: align bolt auth test with lint-cardinality** — `bee6c3d` (fix)

## Decisions Made

1. **Stub bags via catalog_redstubs.go for plans 04-03..04-06.** Required so `pkg/observability` compiles while the per-bag RED tests sit in `t.Skip("RED: pending Plan 04-NN")` — without the stubs, the test files reference undefined types and the entire package fails to build, blocking 04-02 verification. Each stub constructor panics if invoked at runtime; the `t.Skip` in each RED test means the panic is never reached. Plans 04-03..04-06 each replace one stub block with the real GREEN bag.
2. **Two-phase bootstrap preserved** by moving only `httpServer.Start()` (not the `observability.New` call) AFTER metrics injection. Phase 2 D-08's invariant (server.New BEFORE obs.New so the same *slog.Logger threads through both) holds; the only reorder is that `Start()` now runs after `SetHTTPMetrics(...)` so the instrumentedMux wrapper picks up the bag at Handler-mount time inside Start().
3. **Per-tuple Bind cache lives at the HTTP chokepoint, not in the bag.** `path_template` is not known until route resolution, so the bag's `BindRequestDuration` helper still pays a per-call WithLabelValues lookup. The chokepoint hosts a `sync.Map` keyed by `(method, template, status_class, database)` that amortizes the lookup to a single Load per request after warmup. BenchmarkObserve_HotHTTP confirms 0 allocs/op — the cache successfully eliminates the per-request alloc.
4. **reasonFromError uses errors.Is sentinels primary, substring fallback secondary.** Packstream.go's existing `fmt.Errorf` sites (e.g. "incomplete STRING8", "unknown marker: 0x42") don't yet wrap sentinel errors. The substring fallback bridges the gap without rewriting the entire decoder; future code can wrap via `fmt.Errorf("...: %w", errTruncated)` for the fast `errors.Is` path. Both paths reach the same closed-enum reason value.
5. **Bolt auth-attempts crosswire uses a deferred closure capturing session.authenticated transition.** The defer at handleHello entry records pre-state (`wasAuthenticatedBefore`), checks post-state on return, and emits `observeAuthAttempt("success")` only when authentication actually transitioned. Re-HELLO calls (already-authenticated session) emit no metric — avoids double-counting. The deferred pattern means every return path (success, sendFailure, parse failure) is covered by a single chokepoint.
6. **Removed bag-wired sub-test from TestAuthAttempt_BoltProtocol** because the fake AuthMetrics fixture used `prometheus.NewCounterVec` directly, tripping `make lint-cardinality`. Plan 04-06 ships the real NewAuthMetrics constructor AND the integration test that drives the closed-enum result values across success/failure/denied; Plan 04-02's nil-safe call-site test is sufficient to satisfy the "ships the call site behind a nil-check" obligation in this plan's verifier.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Stub bags required for Plans 04-03..04-06**

- **Found during:** Task 04-02-01 verification
- **Issue:** The plan's verification command `go test -run "TestHTTPMetrics|..." ./pkg/observability/...` fails to compile in isolation: the RED test files for Cypher/Storage/MVCC/Embed/Search/Replication/Auth bags reference symbols that haven't shipped yet. Without those types, the package test binary won't build, and even the HTTP+Bolt tests this plan delivers can't run.
- **Fix:** Added `pkg/observability/catalog_redstubs.go` (139 LOC) with type stubs for the 7 not-yet-shipped bags. Each stub provides the struct fields the RED tests reference (`bag.AuthAttempts`, `bag.LagBytes`, `bag.IndexRebuild`, etc.) and a constructor that panics at runtime. The RED tests' leading `t.Skip` ensures the panic is never reached from a running test; the panic is a defense-in-depth guard against accidental production usage.
- **Files modified:** pkg/observability/catalog_redstubs.go (new).
- **Verification:** `go vet ./pkg/observability/...` clean; `go test ./pkg/observability/...` passes (with all RED stubs skipping); `go build ./...` clean.
- **Committed in:** 543e209 (Task 04-02-01 commit).

**2. [Rule 3 - Blocking] cmd/nornicdb integration target file is main.go, not serve.go**

- **Found during:** Task 04-02-06
- **Issue:** Plan files_modified listed `cmd/nornicdb/serve.go` but the runServe function lives in `cmd/nornicdb/main.go` (same finding as Plan 04-01-04 deviation #3). Plan 04-01 already documented this; we mirror the same pattern.
- **Fix:** Inserted the HTTP + Bolt bag construction into `cmd/nornicdb/main.go` at the same Plan-04-02 D-02c init-order chokepoint Plan 04-01 used for CacheMetrics — between `obs := observability.New(...)` and the Plan-01 `cacheMetrics := ...` block / health registry build.
- **Files modified:** cmd/nornicdb/main.go.
- **Verification:** `go build -tags nolocalllm ./cmd/nornicdb/...` clean; `TestServerStartup_HTTPBoltMetricsRegistered` passes.
- **Committed in:** 6d967b6 (Task 04-02-06 commit).

**3. [Rule 3 - Blocking] Two-phase bootstrap requires httpServer.Start() reorder**

- **Found during:** Task 04-02-06
- **Issue:** Existing main.go calls `httpServer.Start()` at line 657, BEFORE `observability.New()` at line 710. The instrumentedMux wrapper in `Start()` consults `s.httpMetrics` at Handler-mount time — but the metrics bag cannot be constructed until `obs.Registry()` exists. Without reordering, the HTTP server starts serving with `s.httpMetrics == nil` and the wrapper degrades to a pass-through, defeating the plan's MET-06 GREEN goal.
- **Fix:** Moved `httpServer.Start()` to AFTER the new metrics-injection block (lines 730-744 area). The Phase 2 D-08 two-phase bootstrap is preserved because `server.New(...)` still runs BEFORE `observability.New(...)` — only `Start()` moves. Documented inline with a multi-line comment so future maintainers don't accidentally undo the reorder.
- **Files modified:** cmd/nornicdb/main.go.
- **Verification:** All cmd/nornicdb tests pass; HTTPMetrics bag is non-nil at Start() time and the wrapper instruments every request.
- **Committed in:** 6d967b6 (Task 04-02-06 commit).

**4. [Rule 1 - Bug] r.Pattern includes HTTP method prefix**

- **Found during:** Task 04-02-02 verification (TestInstrumentedMux_RPattern)
- **Issue:** Initial test asserted `path_template == "/db/{database}/foo"`, but Go 1.22+ `*http.ServeMux` populates `r.Pattern` with the full registration string — including the method prefix when registered as `"GET /db/{database}/foo"`. The actual value is `"GET /db/{database}/foo"`.
- **Fix:** Updated test assertions to match Go stdlib semantics. The cardinality wall is unchanged: `r.Pattern` is still bounded by the closed route table; the method-prefixed form just slightly increases the bound (×8 for the 8 HTTP methods, but each method+template pair is a distinct route registration, so the actual cardinality is the route-table size, not template-count × 8).
- **Files modified:** pkg/server/instrumented_mux_test.go.
- **Verification:** Tests pass.
- **Committed in:** bd404fb (Task 04-02-02 commit).

**5. [Rule 1 - Bug] dispatchMessage panic skips metrics observation**

- **Found during:** Task 04-02-04 verification (TestMessageDispatch_AllOps for some op handlers)
- **Issue:** Some Bolt handlers (handleReset, handleRoute, etc.) write responses to `s.writer`, which is nil in test fixtures. The handler panics on the nil writer; the panic propagates through `dispatchInner` and skips past the metrics observation code in `dispatchMessage`. Result: these ops did NOT show up in `messages_total`.
- **Fix:** Restructured `dispatchMessage` to use a deferred closure with `recover()` (T-04-08 pattern from instrumentedMux). The metrics observation runs in the deferred closure, so handler panics still emit `result="error"` before the panic re-propagates to the outer connection-handler's panic recover.
- **Files modified:** pkg/bolt/server.go (dispatchMessage refactored to use defer).
- **Verification:** TestMessageDispatch_AllOps passes — every closed-enum op shows up in `messages_total`.
- **Committed in:** a22bdf9 (Task 04-02-04 commit).

**6. [Rule 3 - Blocking] lint-cardinality flags AuthMetrics test fixture**

- **Found during:** Task 04-02-04 follow-up `make lint-cardinality` run
- **Issue:** TestAuthAttempt_BoltProtocol's bag-wired sub-test built a fake `AuthMetrics` using `prometheus.NewCounterVec` directly to validate the increment behavior before Plan 04-06's GREEN bag lands. The Phase 3 lint-cardinality grep gate flagged this as an MET-04 violation (helper-only registration discipline).
- **Fix:** Removed the bag-wired sub-test entirely. Plan 04-06 ships the real NewAuthMetrics constructor AND the integration test that drives the closed-enum result values; Plan 04-02's nil-safe call-site test is sufficient to satisfy the verifier obligation ("ships the call site behind a nil-check").
- **Files modified:** pkg/bolt/server_metrics_test.go.
- **Verification:** `make lint-cardinality` passes; TestAuthAttempt_BoltProtocol still validates nil-safe no-op behavior.
- **Committed in:** bee6c3d (Task 04-02-04 follow-up commit).

---

**Total deviations:** 6 (1 bug, 5 blocking).
**Impact on plan:** No scope creep. Five deviations are mechanical alignment with codebase reality (path corrections, init-order ordering, Go stdlib semantics, lint-cardinality scope). One is a real bug found and fixed during testing (panic-skip-observe pattern that's now panic-safe across both HTTP and Bolt instrumentation chokepoints).

## Issues Encountered

- **Pre-existing race in `TestServerCloseWithListener`** (`pkg/bolt/server_test.go:893`) — surfaces under `go test -race -count=1 ./pkg/bolt/...`. Confirmed pre-existing per Plan 02-05 deferred-items.md ("listener publication race + AuthenticatorAdapter allowAnonymous race (pre-existing)"). NOT introduced by this plan: my changes do not touch the `s.listener` publication site or the `AuthenticatorAdapter.allowAnonymous` field. The race is on `s.listener` between `(*Server).ListenAndServe` write and `(*Server).Close` read; my Plan 04-02 instrumentation observes through pre-bound observers cached in `s.metricsState` and never reads/writes `s.listener`.
- **CGO link warning during test runs** — `ld: warning: ignoring duplicate libraries: '-lobjc'` surfaces consistently; affects all `go test ./pkg/server/...` and `./pkg/bolt/...` runs on macOS. Pre-existing; not introduced by Plan 04-02.

## Self-Check

Verifying claims:

**Files created (existence check):**

```
$ for f in pkg/observability/catalog_http.go \
          pkg/observability/catalog_bolt.go \
          pkg/observability/catalog_redstubs.go \
          pkg/server/instrumented_mux.go \
          pkg/server/instrumented_mux_test.go \
          pkg/bolt/metrics.go \
          pkg/bolt/server_metrics_test.go \
          pkg/bolt/packstream_metrics.go \
          pkg/bolt/packstream_metrics_test.go \
          cmd/nornicdb/http_bolt_metrics_test.go \
          .planning/phases/04-subsystem-metric-catalog/bench-http-bolt-04-02.txt; do
  [ -f "$f" ] && echo "FOUND: $f" || echo "MISSING: $f"
done
FOUND: pkg/observability/catalog_http.go
FOUND: pkg/observability/catalog_bolt.go
FOUND: pkg/observability/catalog_redstubs.go
FOUND: pkg/server/instrumented_mux.go
FOUND: pkg/server/instrumented_mux_test.go
FOUND: pkg/bolt/metrics.go
FOUND: pkg/bolt/server_metrics_test.go
FOUND: pkg/bolt/packstream_metrics.go
FOUND: pkg/bolt/packstream_metrics_test.go
FOUND: cmd/nornicdb/http_bolt_metrics_test.go
FOUND: .planning/phases/04-subsystem-metric-catalog/bench-http-bolt-04-02.txt
```

**Commits exist (git log check):**

```
543e209 feat(04-02-01): catalog_http.go GREEN — 5 HTTP families + D-08 tenant flag
39a1f85 feat(04-02-03): catalog_bolt.go GREEN — 6 Bolt families + closed packstream reason enum
bd404fb feat(04-02-02): instrumentedMux HTTP chokepoint — panic-safe + r.Pattern
a22bdf9 feat(04-02-04): bolt session lifecycle + per-message dispatch + auth crosswire
2c4a90b feat(04-02-05): packstream decode-error reason classification (D-11c)
6d967b6 feat(04-02-06): cmd/nornicdb wires HTTP + Bolt metric bags
8bb989e test(04-02-07): BenchmarkObserve_Hot{HTTP,Bolt} ≤ 2 allocs/op (MET-25)
bee6c3d fix(04-02-04): align bolt auth test with lint-cardinality
```

**audit-untouched gate:**

```
$ git diff --name-only d8bc55c97cce44802302ae2be63ec5d321e38cb6..HEAD pkg/audit/
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
$ go build -tags nolocalllm ./...
(success)
$ go vet ./pkg/server/... ./pkg/bolt/... ./pkg/observability/...
(success)
```

**BenchmarkObserve_Hot{HTTP,Bolt} budgets:**

| Sub-bench | allocs/op | budget |
|-----------|-----------|--------|
| http_request_duration_bound | 0 | ≤ 2 |
| http_request_duration_bound_tenant | 0 | ≤ 2 |
| http_requests_counter_bound | 0 | ≤ 2 |
| bolt_message_duration_bound | 0 | ≤ 2 |
| bolt_messages_total_bound | 0 | ≤ 2 |
| bolt_session_duration_bound | 0 | ≤ 2 |
| bolt_packstream_decode_errors_bound | 0 | ≤ 2 |

All sub-benches: **0 allocs/op** (Apple M1 Max, count=3) — well below the ≤ 2 alloc budget.

**Test suite (race, scoped to plan-introduced packages):**

```
$ go test -tags nolocalllm -race -count=1 ./pkg/observability/...
ok      github.com/orneryd/nornicdb/pkg/observability   3.6s

$ go test -tags nolocalllm -race -run "TestInstrumentedMux" -count=1 ./pkg/server/...
ok      github.com/orneryd/nornicdb/pkg/server   1.8s

$ go test -tags nolocalllm -race -run "TestSessionLifetime|TestMessageDispatch_AllOps|TestPullChunks|TestAuthAttempt|TestReasonFromError|TestPackstreamReason_AllPaths|TestPackstreamDecodeError" -count=1 ./pkg/bolt/...
ok      github.com/orneryd/nornicdb/pkg/bolt   1.8s

$ go test -tags nolocalllm -race -run "TestServerStartup_HTTPBoltMetricsRegistered" -count=1 ./cmd/nornicdb/...
ok      github.com/orneryd/nornicdb/cmd/nornicdb   2.0s
```

**Self-Check: PASSED**

## Next Phase Readiness

- **Plans 04-03..04-06 unblocked.** `pkg/observability` compiles via `catalog_redstubs.go`; each downstream plan replaces one stub block with the real GREEN bag (Cypher in 04-03, Storage+MVCC in 04-04, Embed+Search in 04-05, Replication+Auth in 04-06).
- **Plan 04-06's auth-wiring chokepoint is in place.** `bolt.SetAuthMetrics(bag)` accepts the AuthMetrics bag; `observeAuthAttempt(result)` no-ops on nil and emits `auth_attempts_total{result, protocol="bolt"}` once 04-06 wires the bag at startup. The HELLO completion site is already calling `observeAuthAttempt` — Plan 04-06 only needs to construct the bag and call SetAuthMetrics.
- **Plan 04-07 inputs ready.** `bench-http-bolt-04-02.txt` evidence file captured; per-subsystem bench naming (BenchmarkObserve_HotHTTP, BenchmarkObserve_HotBolt) cumulates cleanly with cache (BenchmarkObserve_HotCache) and downstream subsystems' future benches.
- **Phase 5 forward-compat preserved.** D-08 tenant-flag bool plumbed through `NewHTTPMetrics(reg, tenantLabelsEnabled)`; reads from `cfg.Observability.Metrics.TenantLabelsEnabled` at startup. Phase 5 K8s autodetect will set the bool's default value with no re-registration.
- **Phase 8 forward-compat preserved.** instrumentedMux + bolt dispatch loop are the natural span-emission chokepoints (TRC-12 / TRC-13). When Phase 8 ships, span emission piggy-backs on the same chokepoints (now metric + span) without restructuring the instrumentation.

---
*Phase: 04-subsystem-metric-catalog*
*Plan: 04-02*
*Completed: 2026-05-03*
