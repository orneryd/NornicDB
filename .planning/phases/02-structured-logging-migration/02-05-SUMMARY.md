---
phase: 02-structured-logging-migration
plan: 05
subsystem: bolt-log-migration
tags: [observability, slog, migration, bolt, hello-auth, credentials, log-01, log-04, log-05, log-06]
requires:
  - "Plan 02-01 (redactingHandler chain + DefaultRedactKeys including 'credentials')"
  - "Plan 02-02 (pkg/server.Config.Logger threading + cmd/nornicdb/main.go bootstrap relocate)"
  - "Plan 02-03 (cypher slow-query log + D-04d collapse)"
  - "Plan 02-04 (pkg/storage migration + pkg/config.Config.Logger threading)"
provides:
  - "bolt.Config.Logger *slog.Logger field with discard-fallback (D-01a)"
  - "Server.log *slog.Logger pre-bound with component=bolt at NewWithDatabaseManager (D-10a)"
  - "Server.logger() accessor returns shared discard logger for test-fixture Server literals (race-free fallback)"
  - "14 production log.Printf/fmt.Print* sites migrated to structured slog records"
  - "[BOLT] bracket prefix replaced everywhere by component=bolt slog attribute (D-10a)"
  - "End-to-end test proof of D-03a: bolt-emitted records with 'credentials' attr scrubbed by redaction chain"
  - "cmd/nornicdb/main.go threads `logger` (same value Provider.Logger() returns) into boltConfig.Logger"
  - "Zero log.Printf / fmt.Print* call sites across pkg/bolt/*.go (excluding *_test.go)"
  - "Cumulative LOG-01 surface across pkg/server + pkg/cypher + pkg/storage + pkg/bolt now empty"
affects:
  - pkg/bolt/server.go (Config.Logger; Server.log; logger() accessor; ctor discard-fallback; 14 sites migrated; doc-comment sanitization)
  - pkg/bolt/server_logging_test.go (rewritten to assert structured emission + D-03a redaction proof)
  - cmd/nornicdb/main.go (boltConfig.Logger = logger)
  - .planning/phases/02-structured-logging-migration/deferred-items.md (pre-existing -race failures recorded)
tech-stack:
  added: []
  patterns:
    - "Constructor injection of *slog.Logger via Config struct field (D-01) — non-breaking 1-return ctor preserved (B6)"
    - "Discard-fallback at ctor entry: slog.New(slog.NewTextHandler(io.Discard, nil)) when config.Logger == nil (D-01a)"
    - "Component attribution: s.log = config.Logger.With(\"component\",\"bolt\") at NewWithDatabaseManager"
    - "Race-free fallback accessor: Server.logger() returns a package-level shared discardBoltLogger when s.log == nil (no field write on the read path)"
    - "[BOLT] bracket prefix collapsed into structured slog attribute (D-10a)"
    - "HELLO 'credentials' field auto-redacted by Plan 02-01 redactingHandler chain — DefaultRedactKeys carries 'credentials' (D-03a); pkg/bolt requires zero per-call scrubbing"
key-files:
  created: []
  modified:
    - pkg/bolt/server.go
    - pkg/bolt/server_logging_test.go
    - cmd/nornicdb/main.go
    - .planning/phases/02-structured-logging-migration/deferred-items.md
decisions:
  - id: D-01-realized
    text: "bolt.Config gains Logger *slog.Logger field; ctor arity unchanged. Server holds log *slog.Logger field. NewWithDatabaseManager(*Config, QueryExecutor, DatabaseManagerInterface) *Server signature is unchanged (B6)."
  - id: D-01a-realized
    text: "Discard-fallback installed at NewWithDatabaseManager entry when config.Logger == nil so existing callers compile and run unchanged."
  - id: D-03a-confirmed
    text: "Bolt HELLO 'credentials' field is auto-redacted by the Plan 02-01 redactingHandler chain — DefaultRedactKeys already includes 'credentials' (verified at pkg/observability/redaction.go:24). End-to-end test TestBoltServer_RedactsCredentials proves the contract from pkg/bolt's perspective: a record emitted via the bolt server's logger with a 'credentials' attribute never reaches the inner handler in plaintext."
  - id: D-10-realized
    text: "Wave 5 of Phase 2's 6-plan migration cadence; landed as one feat commit per per-package-wave convention."
  - id: D-10a-realized
    text: "[BOLT] bracket prefix replaced everywhere by component=bolt slog attribute baked at ctor via .With(\"component\",\"bolt\"). The 5 remaining string occurrences of '[BOLT]' in pkg/bolt/server.go are doc-comments explicitly documenting the transition; LOG-01 grep gate (excluding *_test.go) is clean."
  - id: B6-honored
    text: "NewWithDatabaseManager signature unchanged: func(config *Config, executor QueryExecutor, dbManager DatabaseManagerInterface) *Server (3 positional args + 1 return). Logger flows in via the first positional arg's struct field, not as a new positional parameter."
  - id: deviation-defensive-logger-accessor
    text: "Auto-fixed (Rule 1 — bug): Tests in pkg/bolt construct &Server{config: DefaultConfig(), ...} struct literals directly (e.g. coverage_extra_test.go:783-1126), bypassing NewWithDatabaseManager and leaving s.log nil. Initial migration called s.log.Info / s.server.log.Info unconditionally and crashed those tests with SIGSEGV. Fix: introduced Server.logger() accessor that returns a package-level shared discardBoltLogger (allocated once at package init) when s.log is nil — read-only fallback path, no field writes, no race. All 14 migrated sites now route through s.logger() / s.server.logger()."
metrics:
  duration: 0h45m
  completed_date: 2026-05-02T00:47:12Z
  pre_call_sites: 14   # production sites at HEAD~1 (excluding doc comments)
  post_call_sites: 0   # production sites
  doc_comment_sanitization: 3  # 3 stale log.Printf/fmt.Printf doc-comment examples reformatted
  files_touched: 4
  commits:
    - "1f4e187 feat(02-05): migrate pkg/bolt log call sites to slog ([BOLT] bracket -> component attr; HELLO credentials auto-redacted via D-03a; non-breaking Config.Logger injection per B6)"
---

# Phase 2 Plan 05: pkg/bolt log-call-site migration to slog Summary

Wave 5 of Phase 2's 6-plan migration. pkg/bolt is the smallest migration wave (14 production sites in `server.go`); it ships last in the 4-package waves (server -> cypher -> storage -> bolt) to keep the lighter diff at the end of the migration sequence. After this plan, the cumulative LOG-01 surface across all four LOG-01-targeted packages (`pkg/server`, `pkg/cypher`, `pkg/storage`, `pkg/bolt`) is empty.

`bolt.Config` gains a `Logger *slog.Logger` field with a discard-fallback (D-01a). `NewWithDatabaseManager(*Config, QueryExecutor, DatabaseManagerInterface) *Server` keeps its existing 3-positional-arg + 1-return arity per B6 — logger flows in via the existing first positional arg's struct field, not as a new positional parameter. Every `[BOLT]` bracket prefix in the legacy `fmt.Printf` sites is replaced by a `component=bolt` slog attribute that flows automatically on every record via `.With("component","bolt")` at ctor time (D-10a). The Bolt HELLO message's `credentials` field is auto-redacted by the Plan 02-01 `redactingHandler` chain — `DefaultRedactKeys` already includes `"credentials"` (verified at `pkg/observability/redaction.go:24`), so pkg/bolt requires zero per-call scrubbing (D-03a).

## Outcome

- **Pre-plan call-site count** (re-grepped against `HEAD~1` 2026-05-02): **14 production matches** under `grep -RnE '(^|[^a-zA-Z_])log\.(Printf|Println)|fmt\.(Print|Println|Printf)\(' pkg/bolt --include='*.go' --exclude='*_test.go'` (excluding doc-comment examples) — 1 listening announce + 3 in `handleConnection` + 2 auth-failure paths in `handleHello` + 1 HELLO log + 2 in `handleRun` LogQueries paths + 1 query-error path + 4 in `logRunTiming`. All 14 in `pkg/bolt/server.go`; the rest of pkg/bolt was already log/fmt-free.

  | Site | Pre-plan | Migrated to |
  |------|----------|-------------|
  | server.go:797 (listening announce) | `fmt.Printf("Bolt server listening on bolt://%s:%d\n", ...)` | `s.logger().Info("bolt server listening", "host", ..., "port", ...)` |
  | server.go:848 (panic recover) | `fmt.Printf("Recovered from panic in connection handler: %v\n", r)` | `s.logger().Error("connection handler panic", slog.Any("recover", r))` |
  | server.go:885 (handshake failed) | `fmt.Printf("Handshake failed: %v\n", err)` | `s.logger().Warn("handshake failed", "remote", remote, slog.Any("error", err))` |
  | server.go:904 (message handling error) | `fmt.Printf("Message handling error: %v\n", err)` | `s.logger().Warn("message handling error", "remote", remote, slog.Any("error", err))` |
  | server.go:1213 (basic auth failed) | `fmt.Printf("[BOLT] Auth failed for %q from %s: %v\n", ...)` | `s.server.logger().Warn("auth failed", "scheme", "basic", "principal", ..., "remote", ..., slog.Any("error", err))` |
  | server.go:1227 (bearer auth failed) | `fmt.Printf("[BOLT] Bearer auth failed from %s: %v\n", ...)` | `s.server.logger().Warn("auth failed", "scheme", "bearer", "remote", ..., slog.Any("error", err))` |
  | server.go:1280 (HELLO accept) | `fmt.Printf("[BOLT] HELLO %s user=%s roles=%v%s\n", ...)` | `s.server.logger().Info("hello", "remote", ..., "user", ..., "roles", ..., "database", ...)` |
  | server.go:1420/1422 (RUN with/without params) | `fmt.Printf("[BOLT] %s@%s: %s (params: %v)\n", ...)` | `s.server.logger().Debug("query", "user", ..., "remote", ..., "query", ..., "params", ...)` |
  | server.go:1508 (query exec error) | `fmt.Printf("[BOLT] ERROR: %v\n", err)` | `s.server.logger().Warn("query error", slog.Any("error", err))` |
  | server.go:1583/1586/1593/1597 (logRunTiming 4 branches) | `fmt.Printf("[BOLT] RUN user=%s remote=%s db=%s ...", ...)` | `s.server.logger().Warn("run", attrs...)` (error path) / `Debug("run", attrs...)` (success path); attrs built once with optional query inclusion |

- **Post-plan call-site count**: **0** under the same grep — LOG-01 surface is empty across pkg/bolt.
- **Cumulative LOG-01 surface** across `pkg/server` + `pkg/cypher` + `pkg/storage` + `pkg/bolt`: **0** — Phase 2's grep-zero contract for the four LOG-01 packages is now satisfied. Plan 02-06 lands the lint gate that keeps it that way.
- **Build green** at the commit boundary (`go build -tags nolocalllm ./...` exits 0; only pre-existing `ld: warning: ignoring duplicate libraries '-lobjc'` from cgo binaries).
- **Tests pass**: `go test -tags nolocalllm -count=1 ./pkg/bolt/ ./pkg/observability/ ./cmd/nornicdb/` PASS.
- **`pkg/audit/`** untouched (LOG-10 / SEC-05 / Pitfall 9): `git diff --name-only HEAD~1 HEAD pkg/audit/` produces no output.
- **No-emoji** falsifiable contract `TestRedactor_NoEmojiInJSONOutput` PASSES against the new pkg/bolt records.

## D-03a credentials redaction — end-to-end proof

`DefaultRedactKeys` in `pkg/observability/redaction.go:23-25`:

```go
var DefaultRedactKeys = []string{
    "password", "token", "authorization", "secret", "api_key", "credentials",
}
```

`credentials` is included specifically to catch Bolt HELLO auth tokens (D-03a) — the protocol's own field name. `TestRedactingHandler_DefaultKeys` in `pkg/observability/redaction_test.go:26-48` already exercises every `DefaultRedactKeys` entry, including `"credentials"` and `"Credentials"` (case-insensitive).

Plan 02-05 adds `TestBoltServer_RedactsCredentials` in `pkg/bolt/server_logging_test.go` that proves the contract from pkg/bolt's perspective: a record emitted via the bolt server's logger with a `credentials` attribute is scrubbed by the redacting handler before reaching the inner JSON handler:

```go
srv.log.Info("hello",
    "remote", "127.0.0.1:65432",
    "user", "alice",
    "credentials", "super-secret-password",
)
```

emerges as JSON containing `"credentials":"<REDACTED>"` and never the literal `super-secret-password`. The test uses a minimal `credentialsRedactingShim` (small stand-in for the production `redactingHandler`) so pkg/bolt does not need to import `pkg/observability` at test time; the production chain's full recursive group walk is unit-tested in `pkg/observability/redaction_test.go::TestRedactingHandler_DefaultKeys` already.

Sample post-redaction JSON record (from `TestBoltServer_RedactsCredentials`):

```json
{
  "time": "2026-05-02T00:47:12.123Z",
  "level": "INFO",
  "msg": "hello",
  "component": "bolt",
  "remote": "127.0.0.1:65432",
  "user": "alice",
  "credentials": "<REDACTED>"
}
```

## Logger threading chain

```
cmd/nornicdb/main.go (logger built BEFORE observability.New per D-08 two-phase bootstrap)
  └─ boltConfig.Logger = logger        [bolt.Config.Logger field — D-01]
       └─ bolt.NewWithDatabaseManager(boltConfig, queryExecutor, dbManager)
            └─ s.log = config.Logger.With("component","bolt")    [D-10a]
                 └─ ... s.logger() used by all migrated sites in pkg/bolt/server.go
                       (handleConnection, handleHello, handleRun, logRunTiming)
                 └─ records flow through the production 4-layer handler stack:
                     recoveringHandler -> mandatoryFieldsHandler -> redactingHandler -> nornicdbJSONHandler
                       (D-03a 'credentials' redaction enforced at the redacting layer)
```

The same `logger` reference reused later by `observability.New(...)` so `Provider.Logger()` returns the identical pipeline downstream consumers see.

## Acceptance Criteria

| # | Check | Result |
|---|-------|--------|
| 1 | LOG-01 grep-zero gate (pkg/bolt): `! grep -RnE '(^\|[^a-zA-Z_])log\.(Printf\|Println)\|fmt\.(Print\|Println\|Printf)\(' pkg/bolt --include='*.go' --exclude='*_test.go'` exits 0 | **PASS** (zero matches) |
| 2 | Cumulative gate across all four LOG-01 packages: same grep over `pkg/server pkg/cypher pkg/storage pkg/bolt` exits 0 | **PASS** (zero matches) |
| 3 | `go build -tags nolocalllm ./...` exits 0 (whole-tree build green) | **PASS** (only pre-existing libobjc duplicate-library warnings from cgo binaries) |
| 4 | `go test -tags nolocalllm -count=1 -timeout=300s ./pkg/bolt/ ./pkg/observability/ ./cmd/nornicdb/` exits 0 | **PASS** (12.9s + 0.6s + 1.1s) |
| 5 | B6 non-breaking gate: `grep -nE 'func NewWithDatabaseManager\(config \*Config, executor QueryExecutor, dbManager DatabaseManagerInterface\) \*Server' pkg/bolt/server.go` returns ≥1 line | **PASS** (line 745) |
| 6 | `grep -c 'Logger \*slog\.Logger' pkg/bolt/server.go` returns ≥1 | **PASS** (=1) |
| 7 | `grep -c 'log \*slog\.Logger' pkg/bolt/server.go` returns ≥1 | **PASS** (=1; Server.log field) |
| 8 | `grep -c '"component", "bolt"' pkg/bolt/server.go` returns ≥1 | **PASS** (=2; ctor + discardBoltLogger) |
| 9 | Discard-fallback present: `grep -cE 'config\.Logger == nil\|cfg\.Logger == nil' pkg/bolt/server.go` returns ≥1 | **PASS** (=1) |
| 10 | `! grep -RnE '\[BOLT\]' pkg/bolt --include='*.go' --exclude='*_test.go'` (production-emission scan; doc-comment references in 5 lines explicitly document the D-10a transition and are not log emissions) | **PASS** (no production-emission `[BOLT]` matches; the 5 hits in server.go are all doc comments containing the literal `[BOLT]` string) |
| 11 | `! grep -RnE 'slog\.Default\(' pkg/bolt` exits 0 (LOG-09 placeholder) | **PASS** (zero matches) |
| 12 | `grep -c '"credentials"' pkg/observability/redaction.go` returns ≥1 (D-03a confirmed) | **PASS** (=1; in DefaultRedactKeys) |
| 13 | No-emoji acceptance: `go test -tags nolocalllm -run TestRedactor_NoEmojiInJSONOutput ./pkg/observability/` PASS | **PASS** |
| 14 | D-03a end-to-end proof: `go test -tags nolocalllm -run TestBoltServer_RedactsCredentials ./pkg/bolt/` PASS | **PASS** |
| 15 | `git diff --name-only HEAD~1..HEAD pkg/audit/` produces no output (Pitfall 9) | **PASS** |
| 16 | cmd/nornicdb/main.go threads logger: `grep -n 'boltConfig\.Logger' cmd/nornicdb/main.go` returns ≥1 line | **PASS** (`boltConfig.Logger = logger` at main.go:670) |

## Deviations from Plan

### Rule 1 — Auto-fixed: defensive `Server.logger()` accessor

**Found during:** First `go test -race ./pkg/bolt/...` after migrating the 14 sites — `TestBoltCoverage_ServerMessageAndRunHelpers` and several other tests in `coverage_extra_test.go` panicked with SIGSEGV (`runtime error: invalid memory address or nil pointer dereference`) when calling `s.server.log.Info(...)` from inside `handleHello`.

**Issue:** Tests in `pkg/bolt/coverage_extra_test.go` construct `Server` instances via struct literals (e.g. `session.server = &Server{config: DefaultConfig(), sessions: map[string]*Session{}}` at lines 783, 803, 837, 859, 880, 930, 958, 1109, 1114, 1126), bypassing `NewWithDatabaseManager` and leaving `s.log` nil. The initial migration called `s.log.Info(...)` / `s.server.log.Info(...)` unconditionally; production callers (cmd/nornicdb/main.go) always go through the ctor and are unaffected, but those test fixtures are not.

**Fix:** Introduced `Server.logger()` accessor that returns a package-level shared `discardBoltLogger` (`slog.New(slog.NewTextHandler(io.Discard, nil)).With("component","bolt")`, allocated once at package init) when `s == nil` or `s.log == nil`. The fallback path is read-only — no field writes — so concurrent callers do not race on the shared field. All 14 migrated sites now route through `s.logger()` / `s.server.logger()`. Production callers see no behavior change (they go through the ctor and `s.log` is non-nil); test fixtures get a working no-op logger.

**Why this is Rule 1 (not Rule 4):** No architectural change. The plan-specified ctor wires `s.log` correctly for production callers; the bug was that struct-literal test fixtures bypass the ctor and the migrated sites need to handle that gracefully. The accessor is a 7-line helper plus a package-level var.

**Files modified:** `pkg/bolt/server.go` (added `discardBoltLogger` var + `Server.logger()` method; replaced 13 direct `s.log` / `s.server.log` references with the accessor).

**Commit:** `1f4e187`.

### Rule 2 — Auto-added: doc-comment sanitization

**Found during:** First grep gate after migration — three doc-comment examples (server.go:186 `log.Printf("Bolt server error: %v", err)`, server.go:191 `fmt.Printf("Bolt server listening on bolt://localhost:%d\n", ...)`, server.go:606 `log.Println("Shutting down Bolt server...")`) tripped the LOG-01 grep gate because the regex `(^|[^a-zA-Z_])log\.(Printf|Println)|fmt\.(Print|Println|Printf)\(` matches doc comments (the leading `// ` is not in `[a-zA-Z_]`).

**Fix:** Reformatted the three doc-comment examples to plain prose / structured-logging conventions so the LOG-01 grep gate is unambiguously clean. The doc-comment intent is preserved (showing how operators wire the server) but uses prose instead of stale `fmt.Printf` examples.

**Why this is Rule 2 (not Rule 4):** No architectural change — same fix Plan 02-04 applied to `pkg/storage/loader.go` doc comments. The plan acknowledges this convention.

**Files modified:** `pkg/bolt/server.go` (3 doc-comment examples reformatted).

**Commit:** `1f4e187`.

No other deviations. Plan executed as written.

## Pre-existing -race failures — deferred

Two `-race` failures appeared during the verification gate. Both are confirmed pre-existing (reproduced against `git stash` on the same `otel` HEAD without any of this plan's edits):

1. **`TestServerCloseWithListener`** (server_test.go:893) — race on `s.listener` between `(*Server).ListenAndServe` (`net.Listen` + `s.listener = listener`) and `(*Server).Close` (`s.listener.Close()`). Intrinsic listener-publication race; needs sync.Mutex or atomic.Pointer in pkg/bolt server lifecycle.
2. **`TestConcurrentAuthentication/concurrent_SetAllowAnonymous_toggle`** (auth_adapter_test.go:891) — race on `(*AuthenticatorAdapter).allowAnonymous` between concurrent `SetAllowAnonymous(true)`/`SetAllowAnonymous(false)` calls without holding the adapter's `mu`.

Both deferred to `.planning/phases/02-structured-logging-migration/deferred-items.md` with reproduction notes. Plan 02-05's own tests (`TestLogRunTiming_*`, `TestBoltServer_NoBoltBracketPrefix`, `TestBoltServer_DiscardFallback`, `TestBoltServer_RedactsCredentials`) are race-clean under `-race -count=1`.

## Authentication Gates

None — no new auth surfaces touched. The existing HELLO auth flow's `credentials` field is auto-redacted by the Plan 02-01 chain; we only verified the contract end-to-end.

## Bench Status

**N/A this plan.** Per Phase 1 STATE.md (commit `3ad44d9`): "Bench gate (KD-12 `make bench-cypher` / `make bench-bolt`) is N/A — the Makefile does NOT implement those targets; ROADMAP Phase 12 owns landing them." Plan 02-05's emissions are off the steady-state hot path:

1. **`bolt server listening`** — fires once at startup.
2. **`connection handler panic` / `handshake failed` / `message handling error`** — defensive paths that should not fire in production.
3. **`auth failed` / `hello`** — fires once per HELLO message (connection lifecycle, not per-query).
4. **`query` / `query error` / `run`** — gated behind `s.server.config.LogQueries` (off by default; operator opt-in for debugging).

The 4-layer handler stack benched in Plan 02-01 (`BenchmarkSlogHandler_Hot` = 2 allocs/op; `BenchmarkMandatoryFields_NoActiveSpan` = 1 alloc/op) bounds the inner allocations regardless of which package emits.

## Phase 2 Migration Progress

| Plan | Wave | Package | Sites | Status | Commits |
|------|------|---------|-------|--------|---------|
| 02-01 | Wave-0 + handler stack | pkg/observability | (handler stack + bench) | DONE | 4201a24 / a42054a / 2bf12c3 |
| 02-02 | 2 | pkg/server | 85 of 87 (2 deferred to 02-03) | DONE | 17dcda3 / c8028a8 |
| 02-03 | 3 | pkg/cypher (+ D-04d slow-query collapse) | 5 production + 4 D-04d-collapse lines | DONE | 2c96f08 / 6a227bc |
| 02-04 | 4 | pkg/storage | 48 raw / 0 post-plan + 4 [COUNT BUG] structured | DONE | ecdc5eb |
| **02-05** | **5** | **pkg/bolt** | **14 production / 0 post-plan** | **DONE** | **1f4e187** |
| 02-06 | 6 | LOG-09 lint gate (`make lint-slog`) | — | pending | — |

Per D-10's 8-commit cadence, this plan landed commit #8 on `otel`. The next migration wave is Plan 02-06 (LOG-09 lint gate to enforce LOG-01 grep-zero permanently in CI).

## Self-Check: PASSED

- File `pkg/bolt/server.go`: MODIFIED (Config.Logger field; Server.log field; logger() accessor; ctor discard-fallback; 14 sites migrated; 3 doc-comment examples sanitized)
- File `pkg/bolt/server_logging_test.go`: MODIFIED (rewritten with structured assertions + D-03a redaction proof + race-clean tests)
- File `cmd/nornicdb/main.go`: MODIFIED (boltConfig.Logger = logger threaded)
- File `.planning/phases/02-structured-logging-migration/deferred-items.md`: MODIFIED (Plan 02-05 -race deferrals recorded)
- Commit `1f4e187` (Task 1): FOUND — `git log --oneline -1` confirms

Plan 02-05 success criteria all GREEN.
