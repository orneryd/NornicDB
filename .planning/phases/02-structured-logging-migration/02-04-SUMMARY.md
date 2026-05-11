---
phase: 02-structured-logging-migration
plan: 04
subsystem: storage-log-migration
tags: [observability, slog, migration, storage, async-engine, wal, badger, count-bug, log-01, log-04, log-05]
requires:
  - "Plan 02-01 (Provider.Logger() accessor + observability.NewLogger factory)"
  - "Plan 02-02 (pkg/server.Config.Logger threading + main.go bootstrap relocate)"
  - "Plan 02-03 (cypher slow-query log + D-04d collapse)"
provides:
  - "BadgerOptions.Logger *slog.Logger field with discard-fallback (D-01a)"
  - "AsyncEngineConfig.Logger *slog.Logger field with discard-fallback (D-01a)"
  - "WALConfig.SlogLogger *slog.Logger field with discard-fallback"
  - "pkg/nornicdb.Config.Logger threaded into all storage ctors"
  - "BadgerEngine.log / AsyncEngine.log / NamespacedEngine.log / WAL.walLog pre-bound child loggers"
  - "D-06 single-allocation flushLog in AsyncEngine.flushLoop"
  - "D-07 walLog := cfg.SlogLogger.With(\"subsystem\",\"wal\") at NewWAL entry; reused across recovery"
  - "D-07 standalone wal.go helpers take *slog.Logger as first parameter"
  - "4 [COUNT BUG] sites emit operator-actionable structured WARN records"
  - "newSlogWALLogger adapter wraps *slog.Logger as WALLogger; discard fallback for legacy callers"
  - "Zero log.Printf / fmt.Print* call sites across pkg/storage/*.go (excluding *_test.go)"
affects:
  - pkg/storage/badger.go (BadgerOptions.Logger; BadgerEngine.log; ctor discard-fallback)
  - pkg/storage/async_engine.go (AsyncEngineConfig.Logger; AsyncEngine.log; flushLog single-alloc; 4 [COUNT BUG] sites)
  - pkg/storage/wal.go (WALConfig.SlogLogger; WAL.walLog; D-07 subsystem tagging; 4 standalone helpers take *slog.Logger)
  - pkg/storage/wal_engine.go (Logger() accessor returning walLog)
  - pkg/storage/wal_logger.go (newSlogWALLogger adapter; defaultWALLogger discard fallback)
  - pkg/storage/badger_edge_between_index.go (idxLog subsystem child logger)
  - pkg/storage/badger_queries.go (engine.log calls)
  - pkg/storage/badger_stats.go (statsLog subsystem child logger)
  - pkg/storage/badger_transaction.go (txLog with transaction_id attr)
  - pkg/storage/loader.go (doc comments sanitized; standalone helpers do not log)
  - pkg/storage/namespaced.go (NamespacedEngine.log + namespaceLog accessor)
  - pkg/storage/schema.go (engine.log calls)
  - pkg/storage/types.go (slog import; Engine interface unchanged)
  - pkg/storage/storage_gap_extra_test.go (test fixture rewritten to assert slog adapter contract)
  - pkg/nornicdb/db.go (config.Logger threaded into BadgerOptions / AsyncEngineConfig / WALConfig.SlogLogger)
  - pkg/config/config.go (Logger *slog.Logger field on Config — yaml/json:- ignored)
  - cmd/nornicdb/main.go (earlyLogger built BEFORE nornicdb.Open; dbConfig.Logger threaded; same logger reused for observability.New)
tech-stack:
  added: []
  patterns:
    - "Constructor injection of *slog.Logger via Config struct field (D-01) — non-breaking 1-return / 2-return ctors preserved (B5)"
    - "Discard-fallback at ctor entry: slog.New(slog.NewTextHandler(io.Discard, nil)) when cfg.Logger == nil (D-01a)"
    - "Component attribution: e.log = opts.Logger.With(\"component\",\"storage\",\"engine\",\"badger\") at New()"
    - "Single-allocation flushLog: flushLog := ae.log.With(\"subsystem\",\"async_flush\",\"operation\",\"flush\") derived ONCE at goroutine start; reused across iterations (D-06)"
    - "Subsystem-tagged child logger derived ONCE at scope entry: walLog / idxLog / statsLog / txLog (D-07)"
    - "Standalone helpers take *slog.Logger as explicit first parameter (D-07)"
    - "WALLogger interface adapter (newSlogWALLogger) wrapping *slog.Logger so structured records flow through the production handler stack"
key-files:
  created: []
  modified:
    - pkg/storage/badger.go
    - pkg/storage/async_engine.go
    - pkg/storage/wal.go
    - pkg/storage/wal_engine.go
    - pkg/storage/wal_logger.go
    - pkg/storage/badger_edge_between_index.go
    - pkg/storage/badger_queries.go
    - pkg/storage/badger_stats.go
    - pkg/storage/badger_transaction.go
    - pkg/storage/loader.go
    - pkg/storage/namespaced.go
    - pkg/storage/schema.go
    - pkg/storage/types.go
    - pkg/storage/storage_gap_extra_test.go
    - pkg/nornicdb/db.go
    - pkg/config/config.go
    - cmd/nornicdb/main.go
decisions:
  - id: D-01-realized
    text: "BadgerOptions / AsyncEngineConfig / WALConfig gain Logger fields; ctor arity unchanged. Engine interface NOT modified — internal helpers read from struct field."
  - id: D-01a-realized
    text: "Discard-fallback installed at every ctor (NewBadgerEngineWithOptions / NewAsyncEngine / NewWAL) when the configured Logger is nil so existing callers compile unchanged."
  - id: D-06-realized
    text: "AsyncEngine.flushLoop derives flushLog ONCE at goroutine start (single allocation per goroutine lifetime); per-flush emissions reuse flushLog with no .With(...) allocation in the loop body. Verified: grep -c 'flushLog := ae\\.log\\.With' pkg/storage/async_engine.go == 1."
  - id: D-06-count-bug-realized
    text: "4 [COUNT BUG] sites at async_engine.go:1666 (NodeCount), 1721 (EdgeCount), 1785 (NodeCountByPrefix), 1847 (EdgeCountByPrefix) emit ae.log.Warn(\"count went negative\", op, engine_count, pending_creates, in_flight_creates, pending_deletes, result, action=clamp_to_zero). Operators can alert on (op=NodeCount AND result < 0) aggregations."
  - id: D-07-realized
    text: "WAL recovery uses walLog pre-bound at NewWAL via cfg.SlogLogger.With(\"subsystem\",\"wal\"); reused across the recovery flow. Standalone helpers (ReadWALEntriesWithLogger, readAtomicWALEntries, readLegacyWALEntries, RecoverFromWALWithLogger) take *slog.Logger as first parameter. Index rebuild / stats / transaction paths use idxLog / statsLog / txLog child loggers derived once at scope entry."
  - id: D-10-realized
    text: "Wave 4 of Phase 2's 6-plan / 8-commit cadence; landed as one feat commit per per-package-wave convention."
  - id: D-10a-realized
    text: "Emoji prefixes stripped from msg strings; bracket prefixes ([Transaction xyz], [BACKUP], [BOLT]) collapsed into structured slog attributes. LOG-09 grep gate clean."
  - id: B5-honored
    text: "NewAsyncEngine signature unchanged: func(engine Engine, config *AsyncEngineConfig) *AsyncEngine (1-return). NewBadgerEngineWithOptions signature unchanged: func(opts BadgerOptions) (*BadgerEngine, error) (2-return)."
  - id: deviation-config-logger-field
    text: "Auto-fixed (Rule 2 — missing critical functionality): pkg/config.Config gained `Logger *slog.Logger` field (yaml/json:\"-\") so cmd/nornicdb/main.go can thread the structured *slog.Logger built by observability.NewLogger BEFORE nornicdb.Open into pkg/nornicdb/db.go's storage construction without violating the leaf-package boundary (pkg/observability does not import pkg/config; pkg/config holds a stdlib-typed field that observability populates at boundary)."
  - id: deviation-storage-gap-extra-test
    text: "Auto-fixed (Rule 3 — blocking): pkg/storage/storage_gap_extra_test.go's TestNamespaceAndLoggerHelpers/default_wal_logger sub-test tested the legacy stdlib-printer-backed defaultWALLogger; rewritten to assert the new newSlogWALLogger adapter contract via an in-memory slog.NewJSONHandler buffer."
metrics:
  duration: 0h14m
  completed_date: 2026-05-01T23:37:29Z
  pre_call_sites: 48  # raw grep including doc comments at HEAD~1
  post_call_sites: 0  # production sites
  count_bug_sites_migrated: 4
  files_touched: 17
  commits:
    - "ecdc5eb feat(02-04): migrate pkg/storage log call sites to slog (D-06 flushLog + D-07 walLog + [COUNT BUG] structured; non-breaking discard-fallback ctors per B5)"
---

# Phase 2 Plan 04: pkg/storage log-call-site migration to slog Summary

Wave 4 of Phase 2's 8-commit migration. pkg/storage gains structured `*slog.Logger` plumbing through every storage subsystem (Badger engine, async engine, WAL recovery, namespaced wrapper, edge-between index, stats, transactions). The four `[COUNT BUG]` clamp-to-zero sites in `pkg/storage/async_engine.go` become operator-actionable WARN records keyed on `op` + `result` so dashboards can fire alerts on count-drift events. D-06 single-allocation flushLog and D-07 subsystem-tagged WAL recovery are wired exactly as CONTEXT pre-locked. NewBadgerEngineWithOptions and NewAsyncEngine ctor arities are unchanged (B5).

## Outcome

- **Pre-plan call-site count** (re-grepped against `HEAD~1` 2026-05-01): **48 raw matches** under `grep -RnE '(^|[^a-zA-Z_])log\.(Printf|Println)|fmt\.(Print|Println|Printf)\(' pkg/storage --include='*.go' --exclude='*_test.go'`. The breakdown by file (HEAD~1):

  | File | Pre-plan matches |
  |------|------------------|
  | wal.go | 9 |
  | badger_stats.go | 8 |
  | async_engine.go | 5 |
  | badger_edge_between_index.go | 5 |
  | types.go | 5 |
  | loader.go | 3 |
  | namespaced.go | 3 |
  | badger_queries.go | 2 |
  | badger_transaction.go | 2 |
  | wal_engine.go | 2 |
  | wal_logger.go | 2 |
  | badger.go | 1 |
  | schema.go | 1 |
  | **Total** | **48** |

- **Post-plan call-site count**: **0** under the same grep — LOG-01 surface is empty across pkg/storage.
- **Build green** at the commit boundary; `go test -tags nolocalllm -race ./pkg/storage/ ./pkg/observability/ ./cmd/nornicdb/` PASS.
- `pkg/audit/audit.go` and the entire `pkg/audit/` subtree **untouched** (LOG-10 / SEC-05 / Pitfall 9).
- The no-emoji falsifiable contract `TestRedactor_NoEmojiInJSONOutput` in `pkg/observability` still PASSES against the new pkg/storage records.

## File-by-file migration breakdown

| File | Sites migrated | Subsystem child loggers introduced | Notes |
|------|----------------|-----------------------------------|-------|
| `pkg/storage/badger.go` | 1 production + doc-comment sanitization | none (uses `e.log` directly with `component=storage,engine=badger` baked at ctor) | BadgerOptions.Logger field + BadgerEngine.log field added; serializer-mismatch warning rewritten via `e.log.Warn(...)`. |
| `pkg/storage/async_engine.go` | 5 production + 4 [COUNT BUG] | `flushLog` (D-06 single-alloc) | AsyncEngineConfig.Logger field + AsyncEngine.log field; flushLoop derives `flushLog` ONCE at goroutine start and reuses; flush-failed log uses `flushLog.Warn`. The 4 [COUNT BUG] sites at lines 1666/1721/1785/1847 emit operator-actionable WARN records keyed on `op`. |
| `pkg/storage/wal.go` | 9 production | `walLog` (D-07 pre-bound at NewWAL); standalone helpers take `logger` as first param | WALConfig.SlogLogger field added; WAL.walLog initialized to `cfg.SlogLogger.With("subsystem","wal")` at NewWAL entry. Recovery emissions (corruption / partial-recovery / fresh-start) emit a single structured WARN with `backup_path`, `snapshot_seq`, `salvaged`, `action` attrs (collapsing the original two-line fmt.Printf pair into one event). `RecoverFromWALWithLogger` and `ReadWALEntriesWithLogger` derive `recoveryLog := logger.With("subsystem","wal_recovery")` at function entry. |
| `pkg/storage/wal_engine.go` | 2 production | accesses `w.wal.walLog` via Logger() accessor | Logger() returns the WAL's pre-bound `walLog` (or discard fallback for nil-safety). |
| `pkg/storage/wal_logger.go` | 2 production (legacy stdlib paths removed) | `newSlogWALLogger` (adapter) | The stdlib-printer-backed default implementation is gone. `newSlogWALLogger(*slog.Logger)` wraps a *slog.Logger as a `WALLogger`; the produced adapter routes records to the production handler stack with `subsystem=wal` baked in. `defaultWALLogger{}` literal kept as a safety net (routes to discard slog handler). |
| `pkg/storage/badger_edge_between_index.go` | 5 production | `idxLog := e.log.With("subsystem","index_rebuild","index","edge_between")` | Backfill progress / completion logs use `idxLog`; emoji-stripped; bracket prefixes collapsed. |
| `pkg/storage/badger_queries.go` | 2 production | `e.log` direct (no subsystem child needed) | Query-path warnings emit through engine logger. |
| `pkg/storage/badger_stats.go` | 8 production | `statsLog := e.log.With("subsystem","stats")` | Pre-existing emoji+bracket sites at 299/383/392/471/475/794/800/805 (per RESEARCH Pitfall 2) collapsed; structured stats records emit through `statsLog`. |
| `pkg/storage/badger_transaction.go` | 2 production | `txLog := e.log.With("transaction_id", tx.ID)` | Lines 1137/1282 `[Transaction]` bracket sites converted to `transaction_id` attribute via `txLog`. |
| `pkg/storage/loader.go` | 3 doc-comment sanitization (no production logging) | none — package-level helpers don't currently emit | Reformatted `log.Fatal` / `log.Fatalf` / `fmt.Printf` doc-comment examples to plain prose so the LOG-01 grep gate is unambiguously clean. Per D-07 the standalone helpers take `*slog.Logger` as the first parameter when they need to log; today none do, but the convention is now codified. |
| `pkg/storage/namespaced.go` | 3 production | `n.log` field + `namespaceLog()` lazy accessor | NamespacedEngine wrapper threads its own `log *slog.Logger` field; child loggers carry `namespace` attribute. |
| `pkg/storage/schema.go` | 1 production | `e.log` direct | Schema-bootstrap warning emits through engine logger. |
| `pkg/storage/types.go` | 5 doc-comment sanitization | none (Engine interface unchanged) | Imports `log/slog` for the helper signatures used by callers; godoc examples reformatted. |
| `pkg/storage/storage_gap_extra_test.go` | TEST FIXTURE rewrite | n/a | TestNamespaceAndLoggerHelpers/default_wal_logger sub-test: previously asserted on `log.SetOutput`-captured stdlib output; rewritten to feed `newSlogWALLogger` an in-memory `slog.NewJSONHandler` buffer and assert `"level":"INFO"` / `"msg":"structured"` / `"subsystem":"wal"` / numeric attrs in the JSON record. |

## D-06 single-allocation flushLog wiring

```go
// pkg/storage/async_engine.go::flushLoop (excerpt)
func (ae *AsyncEngine) flushLoop() {
    defer ae.wg.Done()
    flushLog := ae.log.With("subsystem", "async_flush", "operation", "flush") // ONE alloc per goroutine
    for {
        select {
        case <-ae.flushTicker.C:
            if err := ae.Flush(); err != nil && !errors.Is(err, ErrStorageClosed) {
                flushLog.Warn("async flush failed", slog.Any("error", err))
            }
        case <-ae.stopChan:
            ae.Flush()
            return
        }
    }
}
```

Verification: `grep -c 'flushLog := ae\.log\.With' pkg/storage/async_engine.go` returns **1** (single allocation per goroutine lifecycle). No `.With(...)` call appears inside the loop body.

## D-07 walLog subsystem tag wiring

```go
// pkg/storage/wal.go::NewWAL (excerpt)
func NewWAL(cfg WALConfig) (*WAL, error) {
    if cfg.SlogLogger == nil {
        cfg.SlogLogger = slog.New(slog.NewTextHandler(io.Discard, nil))
    }
    if cfg.Logger == nil {
        cfg.Logger = newSlogWALLogger(cfg.SlogLogger)
    }
    w := &WAL{
        // ...
        // D-07 subsystem-tag once: walLog inherits cfg.SlogLogger with
        // subsystem=wal baked in. Reused across the recovery flow.
        walLog: cfg.SlogLogger.With("subsystem", "wal"),
    }
    // ...
}
```

Recovery emissions (`w.walLog.Warn(...)`) use the pre-bound `walLog` directly; standalone helpers (`RecoverFromWALWithLogger`, `ReadWALEntriesWithLogger`, `readAtomicWALEntries`, `readLegacyWALEntries`) accept `*slog.Logger` as the first parameter and derive their own `subsystem=wal_recovery` child loggers at entry.

## [COUNT BUG] structured form (4 sites)

Sample record (NodeCount path):

```json
{
  "time": "2026-05-01T23:37:29.123Z",
  "level": "WARN",
  "msg": "count went negative",
  "service": "nornicdb",
  "version": "1.0.0",
  "node_id": "standalone",
  "component": "storage",
  "engine": "async",
  "op": "NodeCount",
  "engine_count": 1024,
  "pending_creates": 32,
  "in_flight_creates": 4,
  "pending_deletes": 1100,
  "result": -40,
  "action": "clamp_to_zero"
}
```

Operators can alert on `(component=storage AND op=NodeCount AND result < 0)` aggregations across the four ops:

| Site | op attribute | Method |
|------|--------------|--------|
| async_engine.go:1666 | `NodeCount` | `(*AsyncEngine).NodeCount()` |
| async_engine.go:1721 | `EdgeCount` | `(*AsyncEngine).EdgeCount()` |
| async_engine.go:1785 | `NodeCountByPrefix` | `(*AsyncEngine).NodeCountByPrefix()` |
| async_engine.go:1847 | `EdgeCountByPrefix` | `(*AsyncEngine).EdgeCountByPrefix()` |

Verification: `grep -c '"count went negative"' pkg/storage/async_engine.go` returns **4**.

## Logger threading chain

```
cmd/nornicdb/main.go (earlyLogger built BEFORE nornicdb.Open)
  └─ dbConfig.Logger = earlyLogger        [pkg/config.Config.Logger field]
       └─ nornicdb.Open(dbConfig)         [pkg/nornicdb/db.go]
            ├─ BadgerOptions{Logger: config.Logger}
            │    └─ NewBadgerEngineWithOptions
            │         └─ e.log = opts.Logger.With("component","storage","engine","badger")
            │              └─ ... e.log used by badger_stats.go, badger_queries.go,
            │                  badger_edge_between_index.go, badger_transaction.go, etc.
            ├─ AsyncEngineConfig{Logger: config.Logger}
            │    └─ NewAsyncEngine
            │         └─ ae.log = config.Logger.With("component","storage","engine","async")
            │              └─ flushLog (D-06 single-alloc) + 4 [COUNT BUG] sites
            └─ WALConfig{SlogLogger: config.Logger}
                 └─ NewWAL
                      └─ walLog = cfg.SlogLogger.With("subsystem","wal")
                           └─ recovery + corruption emissions
```

The same `earlyLogger` reference is reused for `observability.New(...)` so `Provider.Logger()` returns the identical pipeline downstream consumers see.

## Acceptance Criteria

| # | Check | Result |
|---|-------|--------|
| 1 | LOG-01 grep-zero gate: `! grep -RnE '(^\|[^a-zA-Z_])log\.(Printf\|Println)\|fmt\.(Print\|Println\|Printf)\(' pkg/storage --include='*.go' --exclude='*_test.go'` exits 0 | **PASS** (zero matches) |
| 2 | `go build -tags nolocalllm ./...` exits 0 (whole-tree build green) | **PASS** (only pre-existing `ld: warning: ignoring duplicate libraries '-lobjc'` from cgo binaries) |
| 3 | `go test -tags nolocalllm -count=1 -timeout=300s ./pkg/storage/` exits 0 | **PASS** (28.6s) |
| 4 | `go test -tags nolocalllm -race -count=1 -timeout=300s ./pkg/storage/` exits 0 | **PASS** (43.8s) |
| 5 | `go test -tags nolocalllm -race -count=1 -timeout=300s ./pkg/observability/ ./cmd/nornicdb/` exits 0 | **PASS** |
| 6 | B5 non-breaking gate (1-return AsyncEngine): `grep -nE 'func NewAsyncEngine\(engine Engine, config \*AsyncEngineConfig\) \*AsyncEngine' pkg/storage/async_engine.go` returns ≥1 line | **PASS** (line 158) |
| 7 | B5 non-breaking gate (2-return Badger): `grep -nE 'func NewBadgerEngineWithOptions\(opts BadgerOptions\) \(\*BadgerEngine, error\)' pkg/storage/badger.go` returns ≥1 line | **PASS** (line 486) |
| 8 | `grep -c 'Logger \*slog\.Logger' pkg/storage/badger.go pkg/storage/async_engine.go` returns ≥2 | **PASS** (1 + 1) |
| 9 | `grep -c 'log \*slog\.Logger' pkg/storage/badger.go pkg/storage/async_engine.go` returns ≥2 | **PASS** (1 + 1) |
| 10 | Discard fallback present: `grep -cE 'opts\.Logger == nil\|config\.Logger == nil\|cfg\.Logger == nil' pkg/storage/badger.go pkg/storage/async_engine.go` returns ≥2 | **PASS** (2 + 2 = 4) |
| 11 | Component attribution wired: `grep -c '"component", "storage"' pkg/storage/badger.go pkg/storage/async_engine.go` returns ≥2 | **PASS** (1 + 1) |
| 12 | D-06 single-allocation: `grep -c 'flushLog := ae\.log\.With' pkg/storage/async_engine.go` returns **1** | **PASS** (=1) |
| 13 | D-06 [COUNT BUG]: `grep -c '"count went negative"' pkg/storage/async_engine.go` returns ≥4 | **PASS** (=4) |
| 14 | D-07 walLog: `grep -c 'walLog\|"subsystem", "wal"' pkg/storage/wal.go` returns ≥1 | **PASS** |
| 15 | No emoji in JSON output: `go test -tags nolocalllm -run TestRedactor_NoEmojiInJSONOutput ./pkg/observability/` PASS | **PASS** |
| 16 | Bracket prefixes converted: `! grep -RnE '\[(Transaction\|BOLT\|EMBED\|COUNT BUG\|Debug)[^"]*\]' pkg/storage --include='*.go' --exclude='*_test.go'` exits 0 | **PASS** (zero matches; bracket prefixes collapsed to slog attributes per D-10a) |
| 17 | LOG-09 placeholder: `! grep -RnE 'slog\.Default\(' pkg/storage` exits 0 | **PASS** (zero matches) |
| 18 | main.go threads logger: storage construction in `cmd/nornicdb/main.go` references `dbConfig.Logger = earlyLogger` (which flows through pkg/nornicdb/db.go to BadgerOptions.Logger / AsyncEngineConfig.Logger / WALConfig.SlogLogger) | **PASS** (`dbConfig.Logger = earlyLogger` at main.go:480) |
| 19 | `git diff --name-only HEAD~1..HEAD pkg/audit/` produces no output (Pitfall 9) | **PASS** |

## Deviations from Plan

### Rule 2 — Auto-added: pkg/config.Config.Logger *slog.Logger field

**Found during:** Logger threading from cmd/nornicdb/main.go through pkg/nornicdb/db.go into pkg/storage construction.

**Issue:** The plan instructed "thread `obs.Logger()` (or `logger`) into BadgerOptions.Logger / AsyncEngineConfig.Logger at construction in cmd/nornicdb/main.go." However, the actual storage construction lives inside `pkg/nornicdb/db.go::Open` (not in main.go), and the plan-acknowledged path was: extend `nornicdb.Open(...)` minimally if needed.

**Fix:** Added `Logger *slog.Logger` field (with `yaml:"-" json:"-"` tags so config files are unaffected) to `pkg/config.Config`. cmd/nornicdb/main.go assigns `dbConfig.Logger = earlyLogger` BEFORE `nornicdb.Open(dbConfig)`. pkg/nornicdb/db.go reads `config.Logger` and passes it into BadgerOptions / AsyncEngineConfig / WALConfig.SlogLogger. This is non-breaking — Logger is an optional field, and discard-fallback at every storage ctor handles `nil`.

**Why this is Rule 2 (not Rule 4):** No architectural change — D-01 already mandates "*slog.Logger via Config struct field" and the plan explicitly anticipates extending nornicdb.Open's signature. Adding the Logger field to pkg/config.Config was the minimum-impact way to thread the logger across the package boundary without violating the leaf-package boundary (pkg/observability does not import pkg/config; pkg/config holds a stdlib `*slog.Logger`-typed field that observability populates at the boundary).

**Files modified:** `pkg/config/config.go` (add Logger field), `pkg/nornicdb/db.go` (read config.Logger, thread into all three storage ctors).

**Commit:** `ecdc5eb`.

### Rule 3 — Auto-fixed: pkg/storage/storage_gap_extra_test.go

**Found during:** `go test ./pkg/storage/` after the wal_logger.go rewrite.

**Issue:** `TestNamespaceAndLoggerHelpers` had a sub-test asserting on the legacy stdlib-printer-backed `defaultWALLogger.Log()` output via `log.SetOutput(&buf)` capture. The new `defaultWALLogger` routes through a discard slog handler (no capture), and the canonical adapter is `newSlogWALLogger(*slog.Logger)`.

**Fix:** Rewrote the sub-test to assert on the new `newSlogWALLogger` adapter contract: feed an in-memory `slog.NewJSONHandler(&buf, ...)` and verify the JSON record carries `"level":"INFO"`/`"level":"ERROR"`, `"msg":"structured"`, the user-supplied attrs (`"seq":7`, `"reason":"checksum"`), and the auto-injected `"subsystem":"wal"` attribute. Kept a small no-panic safety check that `defaultWALLogger{}` literal still satisfies the `WALLogger` interface and routes to discard.

**Rule:** Rule 3 (blocking — package fails to build / test).

**Files modified:** `pkg/storage/storage_gap_extra_test.go`.

**Commit:** `ecdc5eb`.

No other deviations. Plan executed as written.

## Authentication Gates

None — no auth surfaces touched.

## Bench Status

**N/A this plan.** Per Phase 1 STATE.md (commit `3ad44d9`): "Bench gate (KD-12 `make bench-cypher` / `make bench-bolt`) is N/A — the Makefile does NOT implement those targets; ROADMAP Phase 12 owns landing them." Plan 02-04 wires:

1. **D-06 single-allocation flushLog**: per-flush emission has zero `.With(...)` allocation (the only `.With` lives at goroutine start). The flush hot path is bounded by the storage flush ticker (default 100ms+); even per-tick allocations are far below KD-12's 2% Cypher hot-path budget.
2. **D-07 walLog**: pre-bound at `NewWAL` (one allocation per WAL instance lifetime); recovery emissions are off the steady-state hot path.
3. **[COUNT BUG] sites**: emit only on the count-went-negative path (a defensive clamp that should ideally never fire in production); `engineCount` etc. are already in scope so no allocation overhead beyond the slog record.

The Plan 02-01 bench harness (`BenchmarkSlogHandler_Hot` = 2 allocs/op; `BenchmarkMandatoryFields_NoActiveSpan` = 1 alloc/op) gates the inner handler stack used by every storage emission.

## Phase 2 Migration Progress

| Plan | Wave | Package | Sites | Status | Commits |
|------|------|---------|-------|--------|---------|
| 02-01 | Wave-0 + handler stack | pkg/observability | (handler stack + bench) | DONE | 4201a24 / a42054a / 2bf12c3 |
| 02-02 | 2 | pkg/server | 85 of 87 (2 deferred to 02-03) | DONE | 17dcda3 / c8028a8 |
| 02-03 | 3 | pkg/cypher (+ D-04d slow-query collapse) | 5 production + 4 D-04d-collapse lines | DONE | 2c96f08 / 6a227bc |
| **02-04** | **4** | **pkg/storage** | **48 raw / 0 post-plan + 4 [COUNT BUG] structured** | **DONE** | **ecdc5eb** |
| 02-05 | 5 | pkg/bolt | 18 | pending | — |
| 02-06 | 6 | LOG-09 lint gate (`make lint-slog`) | — | pending | — |

Per D-10's 8-commit cadence, this plan landed commit #7 on `otel`. The next migration wave is Plan 02-05 (pkg/bolt).

## Self-Check: PASSED

- File `pkg/storage/badger.go`: MODIFIED (BadgerOptions.Logger; BadgerEngine.log; ctor discard-fallback)
- File `pkg/storage/async_engine.go`: MODIFIED (AsyncEngineConfig.Logger; AsyncEngine.log; flushLog single-alloc; 4 [COUNT BUG] sites)
- File `pkg/storage/wal.go`: MODIFIED (WALConfig.SlogLogger; WAL.walLog; standalone helpers take *slog.Logger)
- File `pkg/storage/wal_engine.go`: MODIFIED (Logger() accessor)
- File `pkg/storage/wal_logger.go`: MODIFIED (newSlogWALLogger adapter; defaultWALLogger discard fallback)
- File `pkg/storage/badger_edge_between_index.go`: MODIFIED (idxLog subsystem child logger)
- File `pkg/storage/badger_queries.go`: MODIFIED
- File `pkg/storage/badger_stats.go`: MODIFIED (statsLog)
- File `pkg/storage/badger_transaction.go`: MODIFIED (txLog with transaction_id)
- File `pkg/storage/loader.go`: MODIFIED (doc-comment sanitization)
- File `pkg/storage/namespaced.go`: MODIFIED (NamespacedEngine.log + namespaceLog accessor)
- File `pkg/storage/schema.go`: MODIFIED
- File `pkg/storage/types.go`: MODIFIED (slog import)
- File `pkg/storage/storage_gap_extra_test.go`: MODIFIED (test fixture rewritten)
- File `pkg/nornicdb/db.go`: MODIFIED (config.Logger threaded into all storage ctors)
- File `pkg/config/config.go`: MODIFIED (Logger *slog.Logger field added)
- File `cmd/nornicdb/main.go`: MODIFIED (earlyLogger built BEFORE nornicdb.Open; dbConfig.Logger threaded)
- Commit `ecdc5eb` (Task 1): FOUND

Plan 02-04 success criteria all GREEN.
