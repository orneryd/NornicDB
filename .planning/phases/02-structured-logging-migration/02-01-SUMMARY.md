---
phase: 02-structured-logging-migration
plan: 01
subsystem: observability
tags: [observability, slog, handler, redaction, mandatory-fields, recovering, bench, testenv]
requires: []
provides:
  - "observability.NewLogger(LoggerConfig, ServiceInfo) -> (*slog.Logger, io.Writer, error)"
  - "Provider.Logger() *slog.Logger accessor"
  - "Provider.New(ctx, cfg, info, logger, writerRef) two-phase bootstrap"
  - "TestEnv.CaptureRecords() / LoggedRecords() helpers (D-12)"
  - "DefaultRedactKeys allow-list + NORNICDB_LOG_REDACT_EXTRA support"
  - "BenchmarkSlogHandler_Hot vs BenchmarkSlogHandler_Stdlib (D-02c)"
affects:
  - cmd/nornicdb/main.go (runServe bootstrap)
  - pkg/observability/provider.go (New signature; Logger() accessor; D-09a Sync)
  - pkg/observability/testenv.go (Buffer + CaptureRecords + LoggedRecords)
tech-stack:
  added: []
  patterns:
    - "log/slog stdlib (Go 1.21+, slogtest from 1.22+)"
    - "sync.Pool of *[]byte for buffer reuse (golang/example/slog-handler-guide)"
    - "trace.SpanContextFromContext + IsValid() guard (Pitfall 4)"
    - "strings.NewReplacer (immutable; package-init allocated) for CRLF strip"
key-files:
  created:
    - pkg/observability/logger.go
    - pkg/observability/logging.go
    - pkg/observability/redaction.go
    - pkg/observability/recovering.go
    - pkg/observability/mandatory_fields.go
  modified:
    - pkg/observability/provider.go
    - pkg/observability/testenv.go
    - pkg/observability/listener_test.go
    - pkg/observability/provider_test.go
    - cmd/nornicdb/main.go
    - cmd/nornicdb/serve_test.go
decisions:
  - id: D-08-realized
    text: "NewLogger called BEFORE observability.New in cmd/nornicdb/main.go runServe"
  - id: D-09-realized
    text: "recoveringHandler is provably outermost (TestHandlerStack_OrderInvariant)"
  - id: D-09a-realized
    text: "Provider.Shutdown opportunistic Sync via interface assertion"
  - id: deviation-import-cycle
    text: "NewLogger accepts local LoggerConfig (not config.LoggingConfig) — pkg/config already imports pkg/observability so the obvious signature would create an import cycle. Caller in cmd/nornicdb/main.go translates between the two structs."
metrics:
  duration: 0h22m
  completed_date: 2026-05-01T21:39:33Z
  commits:
    - "4201a24 test(02-01): wave-0 RED scaffolding"
    - "a42054a feat(02-01): pkg/observability slog stack + cmd/nornicdb runServe bootstrap"
---

# Phase 2 Plan 01: pkg/observability slog stack + bootstrap Summary

Production `*slog.Logger` factory + 4-layer handler stack lands in `pkg/observability`; `cmd/nornicdb/main.go` `runServe` updated in lockstep so the whole-tree build is green at every commit boundary. Phase 2 migration waves (02-02..02-05) consume `Provider.Logger()` from this plan via constructor injection per D-01.

## Outcome

- All Phase 1 tests still pass (race-clean under `-race -count=10`).
- New 4-layer handler stack: `recoveringHandler -> mandatoryFieldsHandler -> redactingHandler -> nornicdbJSONHandler`.
- `Provider.Logger()` returns the bootstrapped `*slog.Logger` for D-01 consumption.
- `BenchmarkSlogHandler_Hot` reports **2 allocs/op** for the cypher.exec representative shape — meets PERF-04 ≤2 budget.
- `pkg/audit/audit.go` untouched (LOG-10 / SEC-05 / Pitfall 9).

## Bench Results (Apple M1 Max, darwin/arm64, Go 1.26.2)

| Bench | ns/op | B/op | allocs/op | Notes |
|-------|-------|------|-----------|-------|
| `BenchmarkSlogHandler_Hot` | 869.7 | 112 | **2** | Custom handler, cypher.exec representative shape |
| `BenchmarkSlogHandler_Stdlib` | 1026.0 | 80 | 1 | stdlib `slog.NewJSONHandler` baseline |
| `BenchmarkMandatoryFields_NoActiveSpan` | 656.8 | 32 | 1 | Phase-1 noop ctx — Pitfall 4 IsValid() guard verified |
| `BenchmarkFullStack_Hot` | (informational) | | | recovering -> mandatory -> redact -> json |

Custom handler is **~15% faster** than stdlib on the representative cypher.exec shape; the 32 B/op delta reflects the parity-vs-perf trade-off documented in D-02b.

## File LOC (PERF-06 cap = 800)

| File | LOC | Status |
|------|-----|--------|
| `pkg/observability/logger.go` | 100 | OK |
| `pkg/observability/logging.go` | 512 | OK (largest; under cap) |
| `pkg/observability/redaction.go` | 140 | OK |
| `pkg/observability/recovering.go` | 69 | OK |
| `pkg/observability/mandatory_fields.go` | 88 | OK |
| `pkg/observability/testenv.go` | 198 | OK (Phase 1 baseline 118, +80 for D-12) |
| `pkg/observability/provider.go` | 240 | OK (Phase 1 baseline 205, +35 for Logger/writerRef/Sync) |

## Coverage

| Metric | Phase 1 baseline | Plan 02-01 | PERF-05 gate (≥90%) |
|--------|------------------|-----------|---------------------|
| `pkg/observability` line coverage | 92.1% | **91.2%** | PASS |

Coverage delta: -0.9% (acceptable — large new surface area in `logging.go` with some defensive paths exercised but not every branch). All exported APIs and all hot-path code paths covered.

## Race Stability (Phase 1 baseline reproduction)

`go test -tags nolocalllm -race -count=10 -timeout=300s ./pkg/observability/`: **PASS** (4.5s wall).

`TestTestEnv_RecordCapture_Race` exercises 50 concurrent goroutines logging into the captured buffer through the `lockedWriter` shim — race-clean.

## Acceptance Criteria

| # | Check | Result |
|---|-------|--------|
| 1 | `go build -tags nolocalllm ./...` exits 0 | PASS |
| 2 | `go test -tags nolocalllm -race -count=10 ./pkg/observability/` exits 0 | PASS |
| 3 | `BenchmarkSlogHandler_Hot allocs/op <= 2` | PASS (=2) |
| 4 | `BenchmarkMandatoryFields_NoActiveSpan` confirms IsValid() guard | PASS (1 alloc — Record.Clone, not TraceID().String()) |
| 5 | All `pkg/observability/*.go` <= 800 LOC | PASS |
| 6 | Coverage >= 90% | PASS (91.2%) |
| 7 | No `slog.SetDefault` / `slog.Default()` calls | PASS (only doc-comment references) |
| 8 | `password` / `credentials` present in redaction.go | PASS |
| 9 | `sc.IsValid()` in mandatory_fields.go | PASS (2 occurrences) |
| 10 | `r.Clone()` / `slog.NewRecord()` in mutating handlers | PASS |
| 11 | `recover()` in recovering.go | PASS (1 occurrence) |
| 12 | `observability.NewLogger` BEFORE `observability.New` in main.go | PASS |
| 13 | `pkg/audit/` untouched | PASS (`git diff HEAD~2..HEAD pkg/audit/` empty) |

## Deviations from Plan

### Rule 3 — Auto-fixed blocking issue

**Import cycle: `NewLogger` cannot accept `config.LoggingConfig`.**

- **Found during:** Task 1 RED test compilation.
- **Issue:** Plan signature `NewLogger(cfg config.LoggingConfig, info ServiceInfo)` was not buildable — `pkg/config/config.go:56` already imports `github.com/orneryd/nornicdb/pkg/observability` for `ObservabilityConfig`. Importing `pkg/config` from `pkg/observability` creates a cycle.
- **Fix:** Defined a local `LoggerConfig` struct in `pkg/observability/logger.go` with the fields actually consumed by the factory (`Level`, `Format`, `Output`). The runServe site in `cmd/nornicdb/main.go` translates `config.LoggingConfig` → `observability.LoggerConfig` at the boundary.
- **Files modified:** `pkg/observability/logger.go` (introduces `LoggerConfig`); `cmd/nornicdb/main.go` (translates at call site); test files reference `LoggerConfig` instead of `config.LoggingConfig`.
- **Commit:** `a42054a`.
- **Plan amendment:** Task 2 directive `NewLogger(cfg config.LoggingConfig, ...)` should read `NewLogger(cfg LoggerConfig, ...)` for downstream plans (02-02..02-05). They consume `Provider.Logger()` directly, not `NewLogger`, so no further migration impact.

No other deviations. Plan executed as written.

## Authentication Gates

None — no auth surfaces touched.

## Self-Check: PASSED

- File `pkg/observability/logger.go`: FOUND
- File `pkg/observability/logging.go`: FOUND
- File `pkg/observability/redaction.go`: FOUND
- File `pkg/observability/recovering.go`: FOUND
- File `pkg/observability/mandatory_fields.go`: FOUND
- File `pkg/observability/provider.go`: MODIFIED
- File `pkg/observability/testenv.go`: MODIFIED
- File `cmd/nornicdb/main.go`: MODIFIED
- Commit `4201a24` (Task 1 RED): FOUND
- Commit `a42054a` (Task 2 GREEN): FOUND
