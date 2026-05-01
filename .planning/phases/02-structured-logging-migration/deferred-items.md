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
