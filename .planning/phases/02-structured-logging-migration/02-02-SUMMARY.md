---
phase: 02-structured-logging-migration
plan: 02
subsystem: server-log-migration
tags: [observability, slog, migration, server, log-call-sites, log-01]
requires:
  - "Plan 02-01 (Provider.Logger() accessor + observability.NewLogger factory + 4-layer handler stack)"
provides:
  - "pkg/server.Config.Logger *slog.Logger field with discard-fallback"
  - "pkg/server.Server.log *slog.Logger field tagged component=server"
  - "cmd/nornicdb/main.go threads obs.Logger() into server config at construction"
  - "Zero log.Printf / fmt.Print* call sites across pkg/server/*.go (excluding 2 D-04d-owned lines)"
affects:
  - pkg/server/server.go (Config + Server + ~50 sites)
  - pkg/server/server_auth.go (7 sites)
  - pkg/server/server_db.go (1 site)
  - pkg/server/server_dbconfig.go (3 sites)
  - pkg/server/server_helpers.go (2 sites: logRequest + slow-query fallback)
  - pkg/server/server_middleware.go (5 sites: auth trace + recovery)
  - pkg/server/server_nornicdb.go (8 sites: regen goroutine + search timing)
  - pkg/server/server_qdrantgrpc.go (1 site)
  - pkg/server/server_retention.go (1 site)
  - pkg/server/server_router.go (5 sites)
  - cmd/nornicdb/main.go (relocate logger bootstrap; thread Logger field)
tech-stack:
  added: []
  patterns:
    - "Constructor injection of *slog.Logger via Config field (D-01) — non-breaking 1-return ctor preserved"
    - "Discard-fallback: slog.New(slog.NewTextHandler(io.Discard, nil)) when cfg.Logger == nil (D-01a)"
    - "Component attribution: s.log = config.Logger.With(\"component\", \"server\") at New()"
    - "Subsystem child loggers derived ONCE per goroutine entry: embedLog / heimdallLog / rerankLog / embedInitLog (D-10a)"
    - "Two-phase D-08 init: NewLogger BEFORE business-package construction so logger flows into pkg/server.Config.Logger"
key-files:
  created: []
  modified:
    - pkg/server/server.go
    - pkg/server/server_auth.go
    - pkg/server/server_db.go
    - pkg/server/server_dbconfig.go
    - pkg/server/server_helpers.go
    - pkg/server/server_middleware.go
    - pkg/server/server_nornicdb.go
    - pkg/server/server_qdrantgrpc.go
    - pkg/server/server_retention.go
    - pkg/server/server_router.go
    - cmd/nornicdb/main.go
decisions:
  - id: D-01-realized
    text: "pkg/server.Config gains Logger *slog.Logger; New ctor arity unchanged ((*Server, error)); s.log threaded via field injection."
  - id: D-01a-realized
    text: "discard-fallback installed at New() when config.Logger == nil so existing tests/callers compile unchanged."
  - id: D-08-realized
    text: "main.go relocates observability.NewLogger to BEFORE server.New so the *slog.Logger flows into Config.Logger at construction time."
  - id: D-10a-realized
    text: "All emoji prefixes stripped from msg strings; bracket prefixes ([EMBED]/[BOLT]/[Debug]/[AUTH]/[HTTP]/[GRAPHQL]/[SLOW QUERY]/[Transaction]) become subsystem slog attributes via .With(\"subsystem\", ...) child logger derived once at scope entry."
  - id: B3-exclude-honored
    text: "server.go lines holding config.SlowQueryThreshold (decl 411 + 413, default 538, log readers 1650 + 1654) untouched in this plan; Plan 02-03 D-04d collapses all four atomically."
  - id: deviation-relocate-logger-bootstrap
    text: "Auto-fixed (Rule 3): existing main.go ordering had observability.NewLogger AFTER server.New; relocated the NewLogger block to before serverConfig assembly so Config.Logger receives the threaded *slog.Logger. The downstream observability.New still consumes the same logger reference; no second NewLogger call exists."
metrics:
  duration: 0h41m
  completed_date: 2026-05-01T22:32:57Z
  pre_call_sites: 87
  post_call_sites: 0  # excluding 2 D-04d-owned lines
  excluded_sites: 2  # lines 1650 + 1654 (D-04d collapse owned by Plan 02-03)
  files_touched: 11
  commits:
    - "17dcda3 feat(02-02): add Logger field to pkg/server.Config (discard-fallback); thread observability *slog.Logger at server construction"
    - "c8028a8 feat(02-02): migrate pkg/server log call sites to slog (emoji-strip + bracket-prefix-to-attribute per D-10a; lines 1650/1654 excluded — owned by 02-03 D-04d)"
---

# Phase 2 Plan 02: pkg/server log-call-site migration to slog Summary

First — and largest — Phase 2 migration wave. All 87 operational `log.Printf`/`log.Println`/`fmt.Print*` call sites in `pkg/server/*.go` migrated to `*slog.Logger` emission via field injection through `Config.Logger`, except the two D-04d-owned slow-query toggle lines (server.go:1650 + 1654) which Plan 02-03 collapses atomically when it renames `config.SlowQueryThreshold` → `cfg.Logging.SlowQueryThreshold`. Server ctor arity unchanged.

## Outcome

- **Pre-plan call-site count** (re-grepped 2026-05-01 against the working tree at HEAD~2): **87** matches under `grep -RnE '(^|[^a-zA-Z_])log\.(Printf|Println)|fmt\.(Print|Println|Printf)\(' pkg/server --include='*.go' --exclude='*_test.go'`.
- **Post-plan call-site count**: **2 — both excluded by Plan 02-03 D-04d** (server.go:1650 + 1654).
- **Migrated**: **85 sites** across 10 files.
- `pkg/server.Config.Logger *slog.Logger` field + `Server.log *slog.Logger` field both present and exercised.
- `cmd/nornicdb/main.go` threads `Logger: logger` into the `serverConfig` struct literal at line 595 (the `logger` produced by Plan 02-01's `observability.NewLogger`, which is the same reference `Provider.Logger()` returns later).
- `pkg/audit/audit.go` and every `s.audit.*` call site **untouched** (LOG-10 / SEC-05 / Pitfall 9 boundary holds).
- Build green at every commit boundary; `go test -tags nolocalllm ./pkg/server/` PASS.
- The no-emoji falsifiable contract `TestRedactor_NoEmojiInJSONOutput` in `pkg/observability` still PASSES.

## File-by-file migration breakdown

| File | Sites migrated | Subsystems introduced | Notes |
|------|----------------|------------------------|-------|
| `pkg/server/server.go` | ~50 | `server` (default) + `heimdall` + `search_rerank` + `embed_init` + `rbac` + `dbconfig` + `slow_query` + `search` | Heimdall/reranker/embedding-init/embed-regen scope use child loggers derived once at goroutine entry per D-10a. Lines 1650 + 1654 EXCLUDED (D-04d). All `s.audit.*` calls preserved verbatim. |
| `pkg/server/server_auth.go` | 7 | `oauth` + `rbac` | OAuth redirect/callback + role entitlements set + allowlist save x2 + privileges PutMatrix. State prefix in oauth-redirect log truncated to first 16 chars (no full state token leaked). |
| `pkg/server/server_db.go` | 1 | `create_database` | CREATE DATABASE defensive-fix log. |
| `pkg/server/server_dbconfig.go` | 3 | `dbconfig` (implicit via component=server) | reload-after-PUT + storage rebuild + search rebuild. |
| `pkg/server/server_helpers.go` | 2 | `http` (D-13 group) + `slow_query` | `logRequest` becomes structured HTTP record; `logSlowQuery` fallback (when no file logger) emits `event=slow_query` per D-04c. |
| `pkg/server/server_middleware.go` | 5 | `auth` (3 traceAuth sites) + recovery | Recovery middleware: `s.log.Error("panic recovered ...", "panic", fmt.Sprintf(...))` keeps panic value as string for safe rendering. |
| `pkg/server/server_nornicdb.go` | 8 | `embed` (regen goroutine) + `search` (request, lookup, query embedding x2, timing x2) | `embedLog := s.log.With("subsystem", "embed")` derived once at goroutine entry; reused for all 6 inner calls. Search timing diagnostics carry full structured attribute set (db, total, svc_lookup, embed_total, embed_calls, embed_ok, search_total, search_calls, chunks, vector_chunks, chunk_loop, fallback_bm25, search_method, fallback, results). |
| `pkg/server/server_qdrantgrpc.go` | 1 | `server` (default) | Qdrant gRPC enabled banner. |
| `pkg/server/server_retention.go` | 1 | `server` (default) | Retention defaults add-policy failure. |
| `pkg/server/server_router.go` | 5 | `server` + `graphql` | Headless mode + UI init failed + UI enabled + GraphQL trace + GraphQL API enabled. Dead `// log.Println(...)` comment at L339 sanitized to non-grep-matching prose. |

Comment examples in package godoc that referenced literal `fmt.Printf(...)` syntax (lines 40, 588, 592, 1650, 1782-1785 originally) reformatted to plain prose so the grep gate is clean.

## Acceptance Criteria

| # | Check | Result |
|---|-------|--------|
| 1 | Grep-zero gate (modulo D-04d-owned excludes): `! grep -RnE '(^|[^a-zA-Z_])log\.(Printf\|Println)\|fmt\.(Print\|Println\|Printf)\(' pkg/server --include='*.go' --exclude='*_test.go' \| grep -vE 'pkg/server/server\.go:(1650\|1654):'` | **PASS** (zero remaining LOG-01 surface) |
| 2 | B3 exclude verification: `grep -nE 'config\.SlowQueryThreshold' pkg/server/server.go` returns 4 sites (decl 411, type 413, default 538, log readers 1650 + 1654) — same code SHA as before this plan | **PASS** |
| 3 | `go build -tags nolocalllm ./...` exits 0 | **PASS** (whole-tree build green at every commit boundary; only pre-existing `ld: warning: ignoring duplicate libraries '-lobjc'` from cgo binaries) |
| 4 | `go vet -tags nolocalllm ./pkg/server/` exits 0 | **PASS** |
| 5 | `go test -tags nolocalllm ./pkg/server/` exits 0 | **PASS** (76s) |
| 6 | `go test -tags nolocalllm -run TestRedactor_NoEmojiInJSONOutput ./pkg/observability/` exits 0 | **PASS** (the falsifiable no-emoji contract holds against the new pkg/server records) |
| 7 | `! grep -RnE 'slog\.Default\(' pkg/server` exits 0 (LOG-09 placeholder) | **PASS** (zero matches) |
| 8 | `grep -c "Logger \*slog.Logger" pkg/server/server.go` ≥ 1 | **PASS** (Config field) |
| 9 | `grep -c "log \*slog.Logger" pkg/server/server.go` ≥ 1 | **PASS** (Server struct field) |
| 10 | `grep -c "config.Logger == nil" pkg/server/server.go` ≥ 1 | **PASS** (discard fallback wired) |
| 11 | `grep -c "io.Discard" pkg/server/server.go` ≥ 1 | **PASS** (discard fallback handler target) |
| 12 | `grep -c "component.*server" pkg/server/server.go` ≥ 1 | **PASS** (component attribution wired) |
| 13 | `grep -cE 'Logger:\s+logger' cmd/nornicdb/main.go` ≥ 1 | **PASS** (main.go threads logger into config) |
| 14 | server.New arity preserved (B2): signature still `func New(db *nornicdb.DB, authenticator *auth.Authenticator, config *Config) (*Server, error)` | **PASS** (no 4th positional parameter added) |
| 15 | `git diff --name-only HEAD~2..HEAD pkg/audit/` produces no output (Pitfall 9) | **PASS** (audit boundary intact) |
| 16 | `grep -c 's\.audit\.' pkg/server/server.go` ≥ 1 (audit calls preserved) | **PASS** (all 5+ `s.audit.*` references intact across server.go + server_helpers.go) |
| 17 | `grep -c "subsystem.*embed" pkg/server/server_nornicdb.go` ≥ 1 (Pitfall 2 [EMBED] migration verified) | **PASS** |

## Deviations from Plan

### Rule 3 — Auto-fixed blocking issue: relocate observability.NewLogger BEFORE server.New

**Found during:** Task 1 build verification.

**Issue:** Plan 02-01 already wired `observability.NewLogger` BEFORE `observability.New` (D-08 two-phase bootstrap), but the existing `cmd/nornicdb/main.go` placed BOTH calls AFTER the `httpServer, err := server.New(...)` invocation at line 601. Threading `Logger: obs.Logger()` (or `Logger: logger`) into `serverConfig` at line 594 was not buildable — the variables didn't exist yet at that point in `runServe`.

**Fix:** Relocated the `loggerInfo` + `observability.NewLogger(...)` block (~16 lines) to BEFORE the `serverConfig := server.DefaultConfig()` setup. The downstream `observability.New(cmd.Context(), cfg.Observability, loggerInfo, logger, writerRef)` call at the lifecycle.Run section now consumes the same `logger` / `writerRef` / `loggerInfo` variables already in scope (no second `NewLogger` call exists in the file). The lifecycle-section comment block updated to reference the earlier construction.

**Why this is Rule 3, not Rule 4:** No architectural change — D-08's two-phase bootstrap already mandates "NewLogger BEFORE consumers". The fix is a pure ordering correction to honor an already-locked decision.

**Files modified:** `cmd/nornicdb/main.go` only.

**Commit:** `17dcda3` (Task 1).

### Out-of-scope race in pkg/nornicdb (NOT caused by this plan)

**Found during:** verification's `go test -race -count=1 ./pkg/server/` step.

**Issue:** Several pkg/server tests fail under `-race` when run together (not in isolation): `TestStartQdrantGRPCInvalidPermissionBranch`, `TestConstructorHeimdallOpenAIAndStartBranches`, `TestPublicHandlersAdditionalBranches`, `TestHandleSearchDimensionMismatchAndFallbackBranches`, `TestMultiDatabase_ConcurrentAccess`. The race trace points to `pkg/nornicdb/search_services.go:206` (background clustering goroutine) outliving test teardown.

**Confirmed pre-existing:** `git stash` to drop my changes → run the same two-test combo against HEAD~1 → SAME race detected. Plan 02-02 only touches log call sites; none of the failing tests exercise the new log path.

**Action:** Logged to `.planning/phases/02-structured-logging-migration/deferred-items.md` per scope-boundary rule. Owner: pkg/nornicdb storage/search team. Out of M1 Phase 2 scope (LOG-01 surface only).

**Mitigation for plan acceptance:** plan acceptance gates `go test ./pkg/server/` (without -race) → **PASS**. `go test -race ./pkg/observability/` and `./cmd/nornicdb/` → **PASS**. The grep-zero LOG-01 surface gate is the falsifiable contract for Plan 02-02 and it **PASSES**.

## Authentication Gates

None — no auth surfaces touched. The OAuth handler log lines are migrated as informational records (`s.log.Info("oauth redirect: stored state in memory ...", "state_prefix", state[:16])`) — the redactor's allow-list catches `password`/`token`/`authorization`/`secret`/`api_key`/`credentials` if any future caller ever passes them as attribute keys.

## Bench Status

**N/A this plan.** Per Phase 1 STATE.md (commit `3ad44d9`): "Bench gate (KD-12 `make bench-cypher` / `make bench-bolt`) is N/A — the Makefile does NOT implement those targets; ROADMAP Phase 12 owns landing them." Plan 02-02 only touches operational log emission (NOT Cypher hot path, NOT Bolt wire path); no per-record allocation regression expected. The Plan 02-01 bench harness (`BenchmarkSlogHandler_Hot` = 2 allocs/op) gates the inner handler stack.

## Phase 2 Migration Progress

| Plan | Wave | Package | Sites | Status | Commits |
|------|------|---------|-------|--------|---------|
| 02-01 | Wave-0 + handler stack | pkg/observability | (handler stack + bench) | ✅ DONE | 4201a24 / a42054a / 2bf12c3 |
| **02-02** | **2** | **pkg/server** | **85 of 87 (2 deferred to 02-03)** | **✅ DONE** | **17dcda3 / c8028a8** |
| 02-03 | 3 | pkg/cypher (+ D-04d slow-query collapse) | 22 + 4 collapse-readers | ⏳ NEXT | — |
| 02-04 | 4 | pkg/storage | 48 | ⏳ pending | — |
| 02-05 | 5 | pkg/bolt | 18 | ⏳ pending | — |
| 02-06 | 6 | LOG-09 lint gate (`make lint-slog`) | — | ⏳ pending | — |

Per D-10's 8-commit cadence, this plan landed commits #3 (Task 1) and #3b (Task 2) on `otel`. The next migration wave is Plan 02-03 (cypher + D-04d collapse).

## Self-Check: PASSED

- File `pkg/server/server.go`: MODIFIED (Config + Server + ~50 sites; lines 1650 + 1654 EXCLUDED with markers)
- File `pkg/server/server_auth.go`: MODIFIED (7 sites; `log` import dropped)
- File `pkg/server/server_db.go`: MODIFIED (1 site; `log` import dropped)
- File `pkg/server/server_dbconfig.go`: MODIFIED (3 sites; `log` import dropped)
- File `pkg/server/server_helpers.go`: MODIFIED (2 sites; `log` import dropped — slow-query path now uses `s.log.Warn` fallback)
- File `pkg/server/server_middleware.go`: MODIFIED (5 sites; `log` import dropped)
- File `pkg/server/server_nornicdb.go`: MODIFIED (8 sites; `log` import dropped; embedLog child logger)
- File `pkg/server/server_qdrantgrpc.go`: MODIFIED (1 site; `log` import dropped)
- File `pkg/server/server_retention.go`: MODIFIED (1 site; `log` import dropped)
- File `pkg/server/server_router.go`: MODIFIED (5 sites; `fmt` + `log` imports dropped; dead pprof comment sanitized)
- File `cmd/nornicdb/main.go`: MODIFIED (relocate NewLogger; thread Config.Logger)
- Commit `17dcda3` (Task 1): FOUND
- Commit `c8028a8` (Task 2): FOUND

Plan 02-02 success criteria all GREEN.
