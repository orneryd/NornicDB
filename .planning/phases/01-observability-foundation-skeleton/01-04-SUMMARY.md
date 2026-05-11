---
phase: 01-observability-foundation-skeleton
plan: 04
subsystem: observability
tags: [integration, main, lifecycle, adapter, bolt, http, embed-workers, healthcheck, drain-order, oq2, oq3]

# Dependency graph
requires:
  - phase 01-01 (lifecycle.Run + lifecycle.Component)
  - phase 01-02 (observability.New + ServiceInfo + ObservabilityConfig + OBS-11 noop fallback)
  - phase 01-03 (telemetryListener + pprofListener + Health + /livez /readyz /version)
  - pkg/nornicdb (HealthCheck added here; pkg/storage.Engine.NodeCount existing contract)
  - pkg/server.Server.Start/Stop (existing)
  - pkg/bolt.Server.ListenAndServe/Close (existing)
provides:
  - "cmd/nornicdb/main.go runServe wired to lifecycle.Run as the canonical supervisor (OBS-07): no signal.Notify, no manual sequential shutdown, single errgroup with reverse-iteration drain encoding OBS-08."
  - "cmd/nornicdb/adapter_http.go (47 LOC): httpAdapter wrapping *server.Server with the OQ2 <-ctx.Done() block pattern (server.Start is non-blocking)."
  - "cmd/nornicdb/adapter_bolt.go (57 LOC): boltAdapter wrapping *bolt.Server with the OQ3 net.ErrClosed filter on both Start (ListenAndServe is blocking) and Shutdown."
  - "cmd/nornicdb/adapter_workers.go (45 LOC): workersAdapter sentinel-blocking on ctx.Done so db.StopEmbedQueue lands at the correct ordinal in reverse drain — fixes the Plan 03 anti-pattern that stopped workers FIRST."
  - "(*nornicdb.DB).HealthCheck(ctx) error backed by a real db.storage.NodeCount() probe — closed engine returns errors.Is(err, storage.ErrStorageClosed). W-4 fix: NOT a vacuous nil-check."
  - "cmd/nornicdb/serve_test.go (482 LOC): four Phase-success integration tests proving OBS-07/08 + Phase-success-1/2/3/5 at the cmd composition root."
  - "Latent supervisor-deadlock bug fixed in pkg/observability/listener.go and pkg/observability/pprof.go: each Start now observes ctx.Done() and triggers OQ4-ordered Shutdown internally, so lifecycle.Run no longer hangs forever waiting for Serve to return."
affects:
  - 01-05 (the final phase quality gate now has integration evidence for OBS-07, OBS-08, Phase-success-1, Phase-success-2, Phase-success-3, Phase-success-5)
  - phase 02 (slog migration: the "INFO observability: instance_id=…" log line in main.go is on the migration list)
  - phase 6+ (TraceIDRatioBased sampler swap: provider construction is wired; only the sampler argument changes)

# Tech tracking
tech-stack:
  added:
    - "(no new direct deps — pkg/lifecycle and pkg/observability already pinned errgroup/otel/prometheus by Plans 01–03)"
  patterns:
    - "OQ2 (RESEARCH §"Open Question 2"): non-blocking Start + <-ctx.Done() — encoded literally in cmd/nornicdb/adapter_http.go."
    - "OQ3 (RESEARCH §"Open Question 3"): blocking ListenAndServe + Close + errors.Is(err, net.ErrClosed) filter — encoded literally in cmd/nornicdb/adapter_bolt.go on BOTH Start and Shutdown."
    - "OBS-08 reverse-drain via D-04a registration order: telemetry → (pprof?) → workers → bolt → http; lifecycle.Run iterates in reverse to drain (http first, telemetry last). Verified by TestServe_DrainOrder timestamps."
    - "OQ4 listener Shutdown ordering preserved: Provider.Shutdown FIRST (BSP flush while :9090 still serving) then srv.Shutdown — kept reachable longest possible during graceful drain."
    - "Pitfall 11 fix (W-4): HealthCheck calls Engine.NodeCount which surfaces storage.ErrStorageClosed sentinel; %w-wrapping lets callers errors.Is upstream."
    - "Composition-root adapter pattern: the cmd/nornicdb adapter_*.go files import server/bolt/nornicdb (which is fine — they ARE the integration glue); pkg/observability and pkg/lifecycle remain leaf packages (OBS-01 boundary_test.go still GREEN)."

key-files:
  created:
    - cmd/nornicdb/adapter_http.go (47 LOC) — httpAdapter; Start blocks on <-ctx.Done() after non-blocking srv.Start.
    - cmd/nornicdb/adapter_bolt.go (57 LOC) — boltAdapter; net.ErrClosed filter on Start and Shutdown.
    - cmd/nornicdb/adapter_workers.go (45 LOC) — workersAdapter; sentinel-block Start, db.StopEmbedQueue in Shutdown.
    - cmd/nornicdb/serve_test.go (482 LOC) — TestServe_TelemetryEndpointsLive, TestServe_PprofOptIn, TestServe_DrainOrder, TestServe_OTLPCollectorDownStaysUp.
    - pkg/nornicdb/db_health_test.go (61 LOC) — TestDB_HealthCheck_NilWhenOpen / FailsWhenClosed / RespectsContextCancellation.
    - pkg/observability/listener_ctx_test.go (91 LOC) — guards the new ctx-observing Start path against regression.
    - .planning/phases/01-observability-foundation-skeleton/deferred-items.md — out-of-scope discoveries (pre-existing race, env flakes).
  modified:
    - cmd/nornicdb/main.go — runServe lines ~600-695 replaced by ~70 LOC of lifecycle.Run wiring; imports updated (added lifecycle, observability, errors; removed os/signal, syscall).
    - pkg/nornicdb/db.go — +44 LOC: (*DB).HealthCheck method.
    - pkg/observability/listener.go — telemetryListener.Start now observes ctx.Done() and invokes Shutdown internally (OQ4 ordering preserved via idempotent inner Shutdown).
    - pkg/observability/pprof.go — pprofListener.Start observes ctx.Done() in the same shape.

key-decisions:
  - "lifecycle.Run is the SOLE concurrency primitive in runServe. Removed: signal.Notify channel, sequential shutdown sequence, the orphaned `go boltServer.ListenAndServe()` goroutine that swallowed errors via fmt.Printf. The lifecycle errgroup supervises HTTP, Bolt, embed-workers, telemetry, and (optional) pprof; reverse-iteration drains in OBS-08-mandated order."
  - "Adapters live in cmd/nornicdb/, NOT pkg/observability/. This preserves OBS-01: pkg/observability and pkg/lifecycle remain leaf packages with no business-package imports (boundary_test.go still GREEN). The composition root is the right home for integration glue."
  - "(*DB).HealthCheck calls db.storage.NodeCount() (the cheapest synchronous Engine accessor) NOT a Begin/Discard txn round-trip. NodeCount is guaranteed to surface storage.ErrStorageClosed on a closed engine (existing test contract in pkg/storage/badger_test.go); we inherit that contract for free, with no Badger txn allocation overhead in the hot kubelet-probe path."
  - "search_warm registration is LAZY (lookup in the closure body, not at registration time). Reason: GetOrCreateSearchService may not yet have an instance for the default DB when /readyz is first scraped during cold start; lazy lookup degrades gracefully to 'search service not yet available' rather than nil-panicking."
  - "search_warm uses CheckOpts{Required: false} per CONTEXT.md D-03c — operators see it in /readyz JSON during index rebuild but it does NOT gate readiness. Phase 9 (K8S-06) will add a Progress field to CheckStatus."
  - "Latent listener-supervisor deadlock bug fixed (Rule 1 auto-fix): pkg/observability/listener.go and pkg/observability/pprof.go each had a Start that ignored ctx, blocking forever on srv.Serve. Combined with lifecycle.Run (drain happens AFTER g.Wait()) this would deadlock production main.go on SIGTERM. Fix: Start observes ctx.Done(), invokes its OWN Shutdown internally (preserving OQ4 ordering), then waits for Serve to return. The supervisor's drain phase calls Shutdown again — both inner Shutdowns are idempotent (sync.Once on Provider.Shutdown; net/http Server.Shutdown is itself idempotent). Caught by TestServe_TelemetryEndpointsLive failing with the original code."

requirements-completed: [OBS-07, OBS-08]

# Metrics
duration_seconds: 952
duration_human: "~16 min"
completed: 2026-04-30
task_count: 4
file_count: 7
top_level_test_count: 7
test_subcase_count: 9
coverage_pct_observability: 92.1
---

# Phase 1 Plan 04: cmd/nornicdb integration with lifecycle.Run Summary

**One-liner:** Replace cmd/nornicdb/main.go's hand-rolled signal+sequential-shutdown loop with `lifecycle.Run`, wire HTTP/Bolt/embed-workers as `lifecycle.Component` adapters in the OBS-08-mandated order, add a real `Engine.NodeCount`-backed `(*DB).HealthCheck`, and prove OBS-07/OBS-08 + Phase-success-1/2/3/5 at the composition root via four integration tests.

## Performance

- **Duration:** ~16 min (952s)
- **Started:** 2026-04-30T21:01:40Z (HEAD: 06dbddf — Plan 01-03 end)
- **Completed:** 2026-04-30T21:17:32Z (HEAD: ce17375)
- **Tasks executed:** 4 (00 RED scaffolding, 01 GREEN HealthCheck, 02 adapters+main, 03 integration tests)
- **Commits:** 4 atomic commits (b513977, 962e90f, e21a281, ce17375)

## Tasks

| # | Description | Commit |
|---|-------------|--------|
| 00 | RED: failing tests for (*DB).HealthCheck (Pitfall 11 W-4 gate) | b513977 |
| 01 | GREEN: implement HealthCheck via Engine.NodeCount probe | 962e90f |
| 02 | Adapters (http/bolt/workers) + main.go lifecycle.Run refactor | e21a281 |
| 03 | Phase-success integration tests + fix latent listener Start ctx bug | ce17375 |

## Test Results

### Unit tests
- `go test -tags nolocalllm -race ./pkg/nornicdb/ -run TestDB_HealthCheck_ -count=1`: **PASS** (3 tests: NilWhenOpen, FailsWhenClosed, RespectsContextCancellation).
- `go test -race ./pkg/lifecycle/... -count=1`: **PASS** (no regressions).
- `go test -race ./pkg/observability/... -count=1`: **PASS** (boundary_test.go still GREEN; new listener_ctx_test.go covers the ctx-observe path).

### Integration tests (cmd/nornicdb/serve_test.go)
- `TestServe_TelemetryEndpointsLive` (Phase-success-1): **PASS** — /livez=200, /readyz=200 with storage check, /version JSON has all 5 D-03b keys with correct service_instance_id, /metrics serves go_/process_ collectors.
- `TestServe_PprofOptIn` (Phase-success-2): **PASS** — disabled config returns nil listener; enabled config binds 127.0.0.1 only and serves /debug/pprof/.
- `TestServe_DrainOrder` (Phase-success-3): **PASS** — instrumented stub adapters in registration order [telemetry, pprof, workers, bolt, http] are drained in reverse: http → bolt → workers → pprof → telemetry. Telemetry-LAST is double-asserted.
- `TestServe_OTLPCollectorDownStaysUp` (Phase-success-5): **PASS** — observability.New does NOT error on unreachable OTLP endpoint (200ms timeout); /livez=200 stays up.

### Race-detector stability
- All Plan 01-04 tests (cmd/nornicdb TestServe_*, pkg/nornicdb TestDB_HealthCheck_*, pkg/observability TestTelemetryListener_StartObservesContextCancellation, pkg/observability TestPprofListener_StartObservesContextCancellation) pass cleanly under `-race -count=1`.

### Coverage
- `pkg/observability` coverage: **92.1%** (Plan 03 finished at 92.3%; -0.2pp absorbed within the headroom buffer Plan 03 explicitly left).

## VALIDATION.md row updates

The four integration tests close the four owed Phase-success rows:

| Row | Requirement | Test | Status |
|-----|-------------|------|--------|
| 01-05-01 | Phase-success-1 (telemetry endpoints live) | `TestServe_TelemetryEndpointsLive` | ✅ |
| 01-05-02 | Phase-success-2 (pprof opt-in) | `TestServe_PprofOptIn` | ✅ |
| 01-05-03 | Phase-success-3 (SIGTERM drain order) | `TestServe_DrainOrder` | ✅ |
| 01-05-05 | Phase-success-5 (OTLP down stays up) | `TestServe_OTLPCollectorDownStaysUp` | ✅ |

(Row 01-05-04 is intentionally absent because Phase-success-4 is coverage/file-size, owned by Plan 05.)

## main.go runServe delta

- **Before (Plan 03 head 06dbddf):** lines 600-695 = 96 LOC of channel-based signal handling + sequential shutdown with workers stopped FIRST (anti-pattern violating OBS-08).
- **After (this plan):** ~70 LOC of lifecycle.Run wiring with three observability constructors, two health.Register calls, three adapters, and the components slice in D-04a order.
- **Net change:** runServe gained explicit observability wiring while LOSING the orphaned `go boltServer.ListenAndServe()` goroutine, the manual `signal.Notify(sigChan, ...)` + `<-sigChan` block, the manual `context.WithTimeout(context.Background(), 30s)` shutdown ctx (now owned by lifecycle.Run per OBS-09), and the workers-stop-first sequence.

Imports: added `pkg/lifecycle`, `pkg/observability`, `errors`. Removed `os/signal`, `syscall`.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed listener Start ctx-observation deadlock**
- **Found during:** Task 03 — TestServe_TelemetryEndpointsLive timed out at 5s waiting for lifecycle.Run to return after cancel.
- **Issue:** `pkg/observability/listener.go` and `pkg/observability/pprof.go` each had `Start(ctx)` that blocked on `srv.Serve(ln)` and IGNORED ctx. Combined with `lifecycle.Run` (which only invokes `Shutdown` AFTER `g.Wait()` returns), the supervisor would deadlock on SIGTERM in production: Start never returns until Shutdown is called, but Shutdown isn't called until Start returns. Plan 03's unit tests never caught this because they call `l.Shutdown` directly, bypassing `lifecycle.Run`.
- **Fix:** Each Start now spawns Serve in a goroutine and selects on `ctx.Done()`; on cancellation, Start invokes its own `Shutdown(shutdownCtx)` (preserving OQ4 ordering: Provider.Shutdown FIRST then srv.Shutdown), then waits for Serve to return. The supervisor's reverse-drain phase calls Shutdown a second time; both are idempotent (sync.Once on Provider.Shutdown; net/http Server.Shutdown is itself idempotent).
- **Files modified:** `pkg/observability/listener.go`, `pkg/observability/pprof.go`. Two new unit tests in `pkg/observability/listener_ctx_test.go` guard against regression.
- **Commit:** ce17375

### Out-of-scope discoveries (deferred)

Recorded in `deferred-items.md`:
- Pre-existing race in `pkg/nornicdb/embed_queue` tests (`TestEmbedWorkerConcurrency`/`TestEmbedQueueDebounceAndHelpers`) — confirmed against baseline 06dbddf; outside Phase 1 observability scope.
- Plugin/build-tag environmental flakes (`TestLoadPluginsFromDir`, `TestPluginLoadAndProcedureExtractionHelpers`) — fail locally without `lib/llama_darwin_arm64`; CI env handles this.
- `ui/dist/` build prerequisite — `go build ./cmd/nornicdb/...` requires the UI to be built first; not a code defect.

## Authentication gates

None. Plan 01-04 is fully autonomous infrastructure work; no external service auth required.

## (*DB).HealthCheck shape

The HealthCheck wrapper consumed by `obs.Health().Register("storage", db.HealthCheck)` is implemented as:

```go
func (db *DB) HealthCheck(ctx context.Context) error {
    if db == nil { return errors.New("nornicdb: nil DB") }
    if err := ctx.Err(); err != nil { return err }
    if db.storage == nil { return errors.New("nornicdb: storage engine not initialized") }
    if _, err := db.storage.NodeCount(); err != nil {
        return fmt.Errorf("nornicdb: storage probe: %w", err)
    }
    return nil
}
```

Forward note for Phase 6+: deeper liveness probes (replication peer reachability, MVCC scheduler health, ledger lag) can extend this method without changing the wrapper contract — the public observability.CheckFunc shape `(ctx) error` is stable. Adding probes in additional steps is a strict superset of the current behavior.

## W-4 closure

The vacuous-stub warning from plan-checker is resolved:

- Probe is real: `db.storage.NodeCount()` is called on every check.
- Closed-engine sentinel propagates: NodeCount returns `storage.ErrStorageClosed`; HealthCheck wraps via `%w`; callers `errors.Is(err, storage.ErrStorageClosed)` succeeds.
- Unit-level guard: `TestDB_HealthCheck_FailsWhenClosed` enforces the closed-engine path returns non-nil + `errors.Is(err, storage.ErrStorageClosed)`.

## Open Question 1 resolution

`search_warm` is registered via `searchSvc.IsReady()` (existing accessor in pkg/search/search.go:689) as **informational** (`Required: false`). Operators see it in `/readyz` JSON during search index rebuild but it does NOT gate readiness — kubelet rollout proceeds. The lookup is lazy in the closure (not captured at registration) so cold-start `/readyz` calls don't see a nil service if `GetOrCreateSearchService` hasn't yet been called for the default DB.

## Forward note

Plan 05 verifies coverage (PERF-05) and file-size (PERF-06) as the final phase gate; this plan does NOT measure those. PERF-05 status snapshot from this plan: pkg/observability=92.1% (≥90% threshold satisfied with 2.1pp headroom).

## Self-Check: PASSED

All claimed files exist and all claimed commits are in `git log`:
- `cmd/nornicdb/adapter_http.go` — FOUND.
- `cmd/nornicdb/adapter_bolt.go` — FOUND.
- `cmd/nornicdb/adapter_workers.go` — FOUND.
- `cmd/nornicdb/serve_test.go` — FOUND.
- `pkg/nornicdb/db_health_test.go` — FOUND.
- `pkg/observability/listener_ctx_test.go` — FOUND.
- `.planning/phases/01-observability-foundation-skeleton/deferred-items.md` — FOUND.
- Commits: b513977, 962e90f, e21a281, ce17375 — all in `git log`.
