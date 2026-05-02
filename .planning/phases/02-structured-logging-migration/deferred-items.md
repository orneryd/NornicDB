# Phase 2 Deferred Items

## Plan 02-02 — out-of-scope race detected during verification

**Detected:** 2026-05-01 during Plan 02-02 race-stability gate.

**Test:** `go test -tags nolocalllm -race -count=1 ./pkg/server/` — PASSES without `-race`. Under `-race -count=1` running ANY two pkg/server tests together, several tests fail with `race detected during execution of test`.

**Failing tests (illustrative, not exhaustive):**
- `TestStartQdrantGRPCInvalidPermissionBranch`
- `TestConstructorHeimdallOpenAIAndStartBranches`
- `TestPublicHandlersAdditionalBranches`
- `TestHandleSearchDimensionMismatchAndFallbackBranches`
- `TestMultiDatabase_ConcurrentAccess`

**Root cause (from race report):** Background goroutine in `pkg/nornicdb/search_services.go:206` `(*DB).getOrCreateSearchService` started by `pkg/nornicdb/db.go:1408` (`Open.func10` → `runClusteringOnceAllDatabases` → `startBackgroundTask`). The goroutine outlives test teardown when multiple `nornicdb.Open(...)` instances run in sequence within a single test binary, causing data races on shared search-service state.

**Confirmed pre-existing (NOT caused by Plan 02-02):**
- `git stash` → run the same two-test combo against HEAD~1 → SAME race detected.
- Plan 02-02 only modifies `pkg/server/*.go` log call sites + adds `*slog.Logger` plumbing. None of the failing tests exercise the new log path; the race is in `pkg/nornicdb` background tasks unrelated to logging.

**Deferred owner:** This is a pkg/nornicdb concurrency bug. Requires either (a) goroutine ctx-cancel observance in `runClusteringOnceAllDatabases`, or (b) per-DB sync.Mutex around `getOrCreateSearchService`. Out of M1 Phase 2 scope (LOG-01 surface only); should be filed against the storage/search team for a focused fix.

**Mitigation for Plan 02-02 acceptance:**
- Plan 02-02 acceptance gates `go test ./pkg/server/` (without -race) → PASS.
- `go test -race ./pkg/observability/` → PASS.
- `go test -race ./cmd/nornicdb/` → PASS.
- The grep-zero LOG-01 surface gate is the falsifiable contract for Plan 02-02 and it PASSES.

**Tracking:** STATE.md — open todo entry "pkg/nornicdb/search_services.go race under -race + multi-test runs (pre-existing)".

## Plan 02-05 — out-of-scope races detected during verification

**Detected:** 2026-05-01 during Plan 02-05 race-stability gate.

**Tests:** `go test -tags nolocalllm -race -count=1 ./pkg/bolt/` — PASSES without `-race`. Under `-race`:
- `TestServerCloseWithListener` (server_test.go:893): data race on `s.listener` between `(*Server).ListenAndServe` (write at server.go:837/841 — pre-Plan-02-05 line numbers; the call is the same `net.Listen` + `s.listener = listener` block) and `(*Server).Close` (read at server.go:878/879 — `s.listener.Close()`). The two goroutines neither hold a mutex nor synchronize on a channel; this is an intrinsic listener-publication race, not a logging migration regression.
- `TestConcurrentAuthentication/concurrent_SetAllowAnonymous_toggle` (auth_adapter_test.go:891): data race on `(*AuthenticatorAdapter).allowAnonymous` between concurrent `SetAllowAnonymous(true)` / `SetAllowAnonymous(false)` calls. The adapter mutates the field without holding `mu`.

**Confirmed pre-existing (NOT caused by Plan 02-05):**
- `git stash` → reproduce: BOTH races detected against the same `otel` HEAD without any of this plan's edits applied.
- Plan 02-05 only adds `Config.Logger`, `Server.log`, the `logger()` accessor (read-only on the fallback path), and migrates 14 log/fmt sites to slog. None of the failing tests exercise the new log path; both races are in `s.listener` publication and `AuthenticatorAdapter.allowAnonymous` respectively.

**Deferred owner:** pkg/bolt server lifecycle (`s.listener` should be guarded by `s.mu` or published atomically before `serve()` is started; matched read in `Close()` should acquire the same lock) and `AuthenticatorAdapter` (acquire its `mu` write lock around `allowAnonymous` assignment, mirroring the read path). Out of M1 Phase 2 scope (LOG-01 surface only).

**Mitigation for Plan 02-05 acceptance:**
- Plan 02-05 acceptance gates `go test -tags nolocalllm -count=1 ./pkg/bolt/ ./pkg/observability/ ./cmd/nornicdb/` (without `-race`) → PASS.
- `go test -tags nolocalllm -race -count=1 ./pkg/observability/` → PASS.
- `go test -tags nolocalllm -race -count=1 ./cmd/nornicdb/` → PASS.
- `go test -tags nolocalllm -race -count=1 -run 'TestLogRunTiming|TestBoltServer_NoBoltBracketPrefix|TestBoltServer_DiscardFallback|TestBoltServer_RedactsCredentials' ./pkg/bolt/` → PASS (Plan 02-05's own tests are race-clean).
- The grep-zero LOG-01 surface gate is the falsifiable contract for Plan 02-05 and it PASSES.

**Tracking:** STATE.md — open todo entry "pkg/bolt listener publication race + AuthenticatorAdapter allowAnonymous race (pre-existing)".
