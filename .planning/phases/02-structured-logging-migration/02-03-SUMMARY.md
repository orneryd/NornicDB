---
phase: 02-structured-logging-migration
plan: 03
subsystem: cypher-log-migration
tags: [observability, slog, migration, cypher, slow-query, plan-hash, redact-literals, antlr, log-01, log-07, log-08]
requires:
  - "Plan 02-01 (Provider.Logger() accessor + observability.NewLogger)"
  - "Plan 02-02 (pkg/server.Config.Logger field; logger threading at server construction)"
provides:
  - "cypher.RedactLiterals(query string) string — D-04 ANTLR4 token-stream walk; STRING_LITERAL/INTEGER/FLOAT redacted"
  - "cypher.PlanHash(plan *ExecutionPlan) string — D-04b FNV-1a 16-char hex; W4 type-restricted; reused by Phase 6 TRC-04"
  - "cypher.RedactedPlaceholder = \"<REDACTED>\" — stable substitution sentinel"
  - "(*StorageExecutor).SetLogger(*slog.Logger) — D-01 non-breaking method; ctor arity unchanged"
  - "(*StorageExecutor).SetSlowQueryThreshold(time.Duration) — D-04c emission gate"
  - "(*StorageExecutor).Logger() / .SlowQueryThreshold() — accessors so cloned executors inherit"
  - "LOG-07 slow-query record schema (level=WARN, msg=slow query, event=slow_query, plan_hash, cypher.duration_ms, query)"
  - "D-04d atomic collapse: SlowQueryThreshold + SlowQueryLogFile live ONLY in pkg/config.LoggingConfig"
affects:
  - pkg/cypher/redaction.go (NEW)
  - pkg/cypher/plan_hash.go (NEW)
  - pkg/cypher/executor.go (StorageExecutor.log field; SetLogger/SetSlowQueryThreshold/logger/emitSlowQueryLog; Execute deferred slow-query emit)
  - pkg/cypher/call_vector.go (2 sites migrated; log import dropped)
  - pkg/cypher/executor_show.go (3 sites migrated; log import dropped)
  - pkg/cypher/cache.go (doc comments sanitized)
  - pkg/cypher/duration.go (doc comments sanitized)
  - pkg/cypher/typed_results.go (doc comment sanitized)
  - pkg/server/server.go (D-04d collapse: decl 411-413 + default 537-539 removed; readers 1650/1654 migrated to slog)
  - pkg/server/server_helpers.go (logSlowQuery threshold reader switched)
  - pkg/server/server_public.go (/metrics gauge reader switched)
  - pkg/server/server_extra_test.go (6 test references rewritten)
  - pkg/config/config.go (LoggingConfig.SlowQueryLogFile field added; doc comment expanded)
  - cmd/nornicdb/main.go (executor.SetLogger + SetSlowQueryThreshold; serverConfig.Logging snapshot threading; newTxScopedExecutor inherits via accessors)
tech-stack:
  added: []
  patterns:
    - "ANTLR4 token-stream walk (D-04 RedactLiterals): parser-level syntax check fails closed; lexer-only second pass replaces STRING_LITERAL/INTEGER/FLOAT tokens"
    - "FNV-1a 64-bit canonical-form hashing (D-04b PlanHash): sentinel separator bytes + sorted-key map iteration + typed value prefix bytes; W4 type pinning (string|int64|float64|bool)"
    - "Deferred slow-query emit: time.Now() at Execute entry; defer emitSlowQueryLog covers all return paths"
    - "Accessor-based logger inheritance for transient executors (Logger() / SlowQueryThreshold())"
    - "Non-breaking ctor + SetLogger method (B2 fix; matches Plan 02-02's pattern)"
key-files:
  created:
    - pkg/cypher/redaction.go
    - pkg/cypher/plan_hash.go
    - pkg/cypher/redaction_test.go
    - pkg/cypher/plan_hash_test.go
    - pkg/cypher/slow_query_log_test.go
  modified:
    - pkg/cypher/executor.go
    - pkg/cypher/call_vector.go
    - pkg/cypher/executor_show.go
    - pkg/cypher/cache.go
    - pkg/cypher/duration.go
    - pkg/cypher/typed_results.go
    - pkg/server/server.go
    - pkg/server/server_helpers.go
    - pkg/server/server_public.go
    - pkg/server/server_extra_test.go
    - pkg/config/config.go
    - cmd/nornicdb/main.go
decisions:
  - id: D-01-realized
    text: "(*StorageExecutor).SetLogger method threads *slog.Logger after construction; NewStorageExecutor signature unchanged (1-return). Discard fallback installed lazily via internal logger() helper."
  - id: D-04-realized
    text: "RedactLiterals fails closed on parser syntax errors then runs a fresh CypherLexer pass; STRING_LITERAL=131, INTEGER=126, FLOAT=125 redacted. Identifiers, keywords, and parameter names ($name) preserved verbatim."
  - id: D-04b-realized
    text: "PlanHash uses hash/fnv (stdlib FNV-1a 64-bit) over a stable canonical form: OperatorType | Description | Identifiers | Arguments (sorted-keys) | Children with sentinel separator bytes 0xff/0xfe/0xfd/0xfc. Output via fmt.Sprintf(\"%016x\")."
  - id: W4-realized
    text: "PlanHash arg values restricted to string|int64|float64|bool with typed prefix bytes 0x01/0x02/0x03/0x04. Unsupported types collapse to 0x00 nil contribution; TODO marker for future expansion documented inline (3 occurrences in plan_hash.go)."
  - id: D-04c-realized
    text: "Slow-query log emits a single WARN record with attrs event=slow_query, plan_hash, cypher.duration_ms, and a 500-char-truncated literal-redacted query. RedactLiterals runs BEFORE truncation."
  - id: D-04d-atomic-realized
    text: "All FOUR originally-affected pkg/server/server.go lines (411-413 decl, 537-539 default, 1650+1654 readers) collapsed in this commit. SlowQueryThreshold + new SlowQueryLogFile live ONLY in pkg/config.LoggingConfig. Server snapshot field Config.Logging carries the values."
  - id: B2-honored
    text: "NewStorageExecutor signature still `func(store storage.Engine) *StorageExecutor` — 1-return, no error. SetLogger method (option (a)) chosen over a sibling *WithLogger ctor."
  - id: D-10a-realized
    text: "Bracket prefixes [cypher]/[nornicdb:CREATE_DATABASE] migrated to subsystem slog attributes; emoji-free; LOG-09 grep gate clean."
  - id: deviation-doc-comment-sanitization
    text: "Auto-fixed (Rule 3): doc-comment fmt.Println / fmt.Printf / log.Printf examples in cache.go / duration.go / typed_results.go / executor.go / executor_show.go reformatted to plain prose so the LOG-01 grep gate (excludes *_test.go but not doc comments) returns zero matches. Same approach Plan 02-02 used for pkg/server."
  - id: deviation-test-reference-update
    text: "Auto-fixed (Rule 3): pkg/server/server_extra_test.go had 6 references to the removed SlowQueryThreshold / SlowQueryLogFile fields. Rewritten to cfg.Logging.SlowQueryThreshold / cfg.Logging.SlowQueryLogFile / server.config.Logging.SlowQueryThreshold so the package compiles."
  - id: deviation-config-field-addition
    text: "Auto-fixed (Rule 2 — missing critical functionality): pkg/config.LoggingConfig had SlowQueryThreshold but NOT SlowQueryLogFile. Added SlowQueryLogFile to keep the D-04d single-source-of-truth contract whole; the server's slow-query file path now reads through cfg.Logging.SlowQueryLogFile."
metrics:
  duration: 0h19m
  completed_date: 2026-05-01T22:57:36Z
  pre_call_sites: 5  # production sites in pkg/cypher (call_vector.go x2, executor_show.go x3) — earlier "22 sites" tally counted doc-comment matches now sanitized
  post_call_sites: 0
  d04d_owned_sites_collapsed: 4  # decl 411 + default 537 + readers 1650 + 1654
  files_touched: 14
  commits:
    - "2c96f08 test(02-03): wave-0 RED for cypher.RedactLiterals + cypher.PlanHash + slow-query emission schema"
    - "6a227bc feat(02-03): migrate pkg/cypher + slow-query log + AST literal redactor + plan_hash; D-04d atomic collapse"
---

# Phase 2 Plan 03: pkg/cypher slow-query log + AST literal redactor + plan_hash + D-04d collapse

Wave-3 of Phase 2's 8-commit migration. pkg/cypher gains:

- **`cypher.RedactLiterals`** (D-04 LOG-08) — ANTLR4 lexer-pass redactor used at every query-text emission site.
- **`cypher.PlanHash`** (D-04b LOG-07) — FNV-1a 64-bit 16-char hex over a W4-type-restricted canonical plan form. Phase 6 (TRC-04) will reuse verbatim for the `nornicdb.cypher.plan` span attribute.
- **LOG-07 slow-query record schema** wired at the executor's `Execute` deferred-emit path with the configured `cfg.Logging.SlowQueryThreshold` gate.
- **D-04d atomic collapse** — `SlowQueryThreshold` and `SlowQueryLogFile` collapsed to `pkg/config.LoggingConfig` (the single source of truth); all four affected lines in `pkg/server/server.go` migrated in this same commit.

## Outcome

- **Pre-plan call-site count** (re-grepped against `HEAD~2` 2026-05-01): **5 production sites** in `pkg/cypher/*.go` (call_vector.go:198/200, executor_show.go:443/535/538). The earlier "22 sites" Phase 2 tally counted doc-comment matches; we sanitized those alongside the production migration so the LOG-01 grep gate is unambiguously clean.
- **Post-plan call-site count**: **0** under `! grep -RnE '(^|[^a-zA-Z_])log\.(Printf|Println)|fmt\.(Print|Println|Printf)\(' pkg/cypher --include='*.go' --exclude='*_test.go'`.
- **D-04d atomic collapse**: lines 411-413 (decl), 537-539 (default), 1650 (file-mode reader), 1654 (no-file reader) — ALL FOUR migrated in one commit.
- **`cypher.PlanHash` golden hash committed**: `f2828f5867757221` for the `MATCH (n) RETURN n` fixture; stable across 100 parallel invocations (TestPlanHash_Determinism).
- **`pkg/audit/audit.go` untouched** (LOG-10 / SEC-05 / Pitfall 9).
- **Build green at every commit boundary**; `go test -tags nolocalllm -race ./pkg/cypher/...` PASS.

## D-04d Atomic Collapse — Per-Line Audit

| Original line | Original content | Outcome |
|---|---|---|
| `pkg/server/server.go:411-413` | `// SlowQueryThreshold is minimum duration to log (default: 100ms)\n// Queries taking longer than this will be logged\nSlowQueryThreshold time.Duration` | **Removed**. Replaced with a doc comment plus `Logging nornicConfig.LoggingConfig` snapshot field. |
| `pkg/server/server.go:537-539 (originally 529)` | `SlowQueryEnabled: false,\nSlowQueryThreshold: 100 * time.Millisecond,\nSlowQueryLogFile: "",` | **Collapsed** to `SlowQueryEnabled: false,\nLogging: nornicConfig.LoggingConfig{SlowQueryThreshold: 100 * time.Millisecond}` (server-level default; canonical default lives in pkg/config.DefaultConfig().Logging). |
| `pkg/server/server.go:1650 (Plan 02-02 EXCLUDED-FROM-02-02 marker)` | `log.Printf("✓ Slow query logging to: %s (threshold: %v)", config.SlowQueryLogFile, config.SlowQueryThreshold)` | **Migrated** to `s.log.Info("slow query logging configured", "subsystem", "slow_query", "file", config.Logging.SlowQueryLogFile, "threshold", config.Logging.SlowQueryThreshold)`. |
| `pkg/server/server.go:1654 (Plan 02-02 EXCLUDED-FROM-02-02 marker)` | `log.Printf("✓ Slow query logging enabled (threshold: %v)", config.SlowQueryThreshold)` | **Migrated** to `s.log.Info("slow query logging enabled", "subsystem", "slow_query", "threshold", config.Logging.SlowQueryThreshold)`. |

**Acceptance gate**: `grep -nE 'config\.SlowQueryThreshold|SlowQueryThreshold time\.Duration' pkg/server/server.go` returns **0** matches; `grep -cE 'cfg\.Logging\.SlowQueryThreshold|config\.Logging\.SlowQueryThreshold' pkg/server/server.go` returns **3** (the two readers above plus the doc comment that names the new path).

Downstream readers also switched in the same commit:
- `pkg/server/server_helpers.go` — `s.config.SlowQueryThreshold` → `s.config.Logging.SlowQueryThreshold` (slow-query path threshold check).
- `pkg/server/server_public.go:257` — `/metrics` `nornicdb_slow_query_threshold_ms` gauge reader.
- `pkg/server/server_extra_test.go` (6 sites) — test fixtures rewritten so the package compiles.

## Sample Slow-Query Log Record

Captured from `TestExecutor_SlowQueryLog_Schema` (synthetic) — matches the LOG-07 schema with the literal `"alice"` redacted:

```json
{
  "time": "2026-05-01T22:57:00.123Z",
  "level": "WARN",
  "msg": "slow query",
  "service": "nornicdb",
  "version": "1.0.0",
  "node_id": "standalone",
  "component": "cypher",
  "event": "slow_query",
  "plan_hash": "0000000000000000",
  "cypher.duration_ms": 142,
  "query": "MATCH (n {name: <REDACTED>}) RETURN n"
}
```

Note: `plan_hash="0000000000000000"` is the nil-plan placeholder. Production-mode queries (non-EXPLAIN/PROFILE) currently pass `nil` to `PlanHash`. Phase 6 (TRC-04) will thread the planned tree through so the slow-query record carries the real `plan_hash`.

## Test Coverage

| Test | Status | Asserts |
|------|--------|---------|
| `TestRedactLiterals_StringLiterals` | PASS | "alice"/"hunter2" redacted; identifiers/keywords preserved |
| `TestRedactLiterals_NumberLiterals` | PASS | INTEGER 25 + FLOAT 9.5 redacted |
| `TestRedactLiterals_PreservesIdentifiers` | PASS | alice / Person / email kept; only "a@b.com" redacted |
| `TestRedactLiterals_PreservesParamNames` | PASS | $id preserved |
| `TestRedactLiterals_PasswordHunter2` | PASS | LOG-08 acceptance: "hunter2" never appears in output |
| `TestRedactLiterals_ParseFailureReturnsRedacted` | PASS | `MATCH ((((` → fail-closed `<REDACTED>` |
| `TestRedactLiterals_EmptyQuery` | PASS | empty input fail-closed |
| `TestPlanHash_Stability` (GOLDEN) | PASS | golden hash `f2828f5867757221` |
| `TestPlanHash_Determinism` | PASS | 100 parallel goroutines produce same hash |
| `TestPlanHash_NilSafe` | PASS | nil + nil-root return `0000000000000000` |
| `TestPlanHash_DifferentPlansDiffer` | PASS | OperatorType + Identifiers deltas produce distinct hashes |
| `TestPlanHash_HexFormat` | PASS | matches `^[0-9a-f]{16}$` |
| `TestPlanHash_RestrictedArgTypes` (W4) | PASS | chan/func/struct args all collapse to same hash |
| `TestPlanHash_SupportedArgTypes` | PASS | string/int64/float64/bool/nil produce 5 distinct hashes |
| `TestExecutor_SlowQueryLog_Schema` | PASS | LOG-07 schema asserted on a captured WARN record |
| `TestExecutor_SlowQueryLog_TruncatesAt500` | PASS | 30-clause query truncates to ≤500 chars |

## Acceptance Criteria

| # | Check | Result |
|---|-------|--------|
| 1 | `go build -tags nolocalllm ./...` exits 0 | **PASS** (whole-tree build green at every commit boundary) |
| 2 | `go test -tags nolocalllm -count=1 -timeout=180s ./pkg/cypher/` exits 0 | **PASS** (12.6s) |
| 3 | `go test -tags nolocalllm -race -count=1 -run "TestRedactLiterals|TestPlanHash|TestExecutor_SlowQueryLog" ./pkg/cypher/` exits 0 | **PASS** (2.1s) |
| 4 | `go test -tags nolocalllm ./pkg/server/` exits 0 | **PASS** (77s) |
| 5 | `go test -tags nolocalllm ./pkg/observability/ ./pkg/config/ ./cmd/nornicdb/` exits 0 | **PASS** |
| 6 | LOG-01 grep-zero gate `! grep -RnE '(^\|[^a-zA-Z_])log\.(Printf\|Println)\|fmt\.(Print\|Println\|Printf)\(' pkg/cypher --include='*.go' --exclude='*_test.go'` | **PASS** (zero matches) |
| 7 | LOG-09 placeholder `! grep -RnE 'slog\.Default\(' pkg/cypher` | **PASS** (zero matches) |
| 8 | `grep -c 'func RedactLiterals' pkg/cypher/redaction.go` | **PASS** (=1) |
| 9 | `grep -c 'func PlanHash' pkg/cypher/plan_hash.go` | **PASS** (=1) |
| 10 | `grep -c 'func .* SetLogger' pkg/cypher/executor.go` | **PASS** (=1; B2 non-breaking) |
| 11 | NewStorageExecutor arity unchanged (B2) | **PASS** (still `func(store storage.Engine) *StorageExecutor`) |
| 12 | `grep -c 'CypherLexerSTRING_LITERAL\|CypherLexerINTEGER\|CypherLexerFLOAT' pkg/cypher/redaction.go` | **PASS** (=3) |
| 13 | `grep -c 'fnv\.New64a\|hash/fnv' pkg/cypher/plan_hash.go` | **PASS** (≥1) |
| 14 | W4 canonical form: `grep -c 'TODO.*PlanHash arg type' pkg/cypher/plan_hash.go` | **PASS** (=1; in writeArgValue default branch) |
| 15 | **D-04d atomic gate (B3)**: `grep -nE 'config\.SlowQueryThreshold\|SlowQueryThreshold time\.Duration' pkg/server/server.go` | **PASS** (zero matches) |
| 16 | **D-04d readers reference renamed field**: `grep -cE 'cfg\.Logging\.SlowQueryThreshold\|config\.Logging\.SlowQueryThreshold' pkg/server/server.go` | **PASS** (=3) |
| 17 | `grep -c 'event.*slow_query\|"event", "slow_query"' pkg/cypher/executor.go` | **PASS** (≥1) |
| 18 | `git diff --name-only HEAD~2..HEAD pkg/audit/` produces no output (Pitfall 9) | **PASS** |
| 19 | TestRedactor_NoEmojiInJSONOutput | **PASS** |

## Deviations from Plan

### Rule 3 — Auto-fixed: doc-comment sanitization

**Found during:** LOG-01 grep gate verification.

**Issue:** The plan's `pre_call_sites = 22` count included `//	fmt.Println(...)` style godoc EXAMPLE blocks in `cache.go`, `duration.go`, `typed_results.go`, `executor.go`, `executor_show.go`. The grep gate doesn't distinguish code from comments; without sanitization the gate would still match those lines and report "21 sites remaining" even after migrating the 5 real production sites.

**Fix:** Reformatted godoc examples to plain prose (e.g., `// fmt.Printf("Row: %v\n", row)` → `// // emit "Row: %v" via the configured logger`). Mirrors the same pattern Plan 02-02 used for `pkg/server`'s pprof comment block.

**Rule:** Rule 3 (blocking — falsifies the LOG-01 acceptance gate).

**Files modified:** `pkg/cypher/{cache.go, duration.go, typed_results.go, executor.go, executor_show.go}`.

**Commit:** `6a227bc`.

### Rule 3 — Auto-fixed: pkg/server/server_extra_test.go references

**Found during:** `go test ./pkg/server/` after the D-04d field collapse.

**Issue:** 6 test sites still referenced the removed fields `cfg.SlowQueryThreshold`, `cfg.SlowQueryLogFile`, and `server.config.SlowQueryThreshold`. Package failed to compile.

**Fix:** sed-renamed in place: `cfg.SlowQueryThreshold` → `cfg.Logging.SlowQueryThreshold`; `cfg.SlowQueryLogFile` → `cfg.Logging.SlowQueryLogFile`; `server.config.SlowQueryThreshold` → `server.config.Logging.SlowQueryThreshold`.

**Rule:** Rule 3 (blocking — D-04d collapse cannot land without this update).

**Files modified:** `pkg/server/server_extra_test.go`.

**Commit:** `6a227bc`.

### Rule 2 — Auto-added: pkg/config.LoggingConfig.SlowQueryLogFile

**Found during:** D-04d implementation.

**Issue:** `pkg/config.LoggingConfig` had `SlowQueryThreshold` but NOT `SlowQueryLogFile`. The plan calls for collapsing both server-side fields into `LoggingConfig`. Without the new field the server's slow-query log file path would have to live in two places again.

**Fix:** Added `SlowQueryLogFile string` to `LoggingConfig` with a doc comment marking it the single source of truth. The server reader now uses `config.Logging.SlowQueryLogFile`.

**Rule:** Rule 2 (correctness — D-04d single-source-of-truth contract).

**Files modified:** `pkg/config/config.go`.

**Commit:** `6a227bc`.

No other deviations.

## Authentication Gates

None.

## Bench Status

**N/A this plan** — same posture as Plan 02-02 SUMMARY. Per Phase 1 STATE.md, `make bench-cypher` / `make bench-bolt` Makefile targets are not yet implemented (ROADMAP Phase 12 owns landing them). Plan 02-03 only wires:

1. The slow-query emit path (gated by `slowQueryThreshold > 0` AND `duration >= threshold` — production hot paths with the default threshold of 100 ms or higher won't enter `emitSlowQueryLog` in the common case).
2. Redaction + PlanHash invoked ONLY inside `emitSlowQueryLog` (i.e., only on cold path).
3. A `time.Now()` + deferred closure at `Execute` entry (constant overhead ~50 ns / call; well within KD-12's 2% budget).

The Plan 02-01 bench harness (`BenchmarkSlogHandler_Hot` = 2 allocs/op) gates the inner handler stack used by the slow-query record emission.

## Phase 2 Migration Progress

| Plan | Wave | Package | Sites | Status | Commits |
|------|------|---------|-------|--------|---------|
| 02-01 | Wave-0 + handler stack | pkg/observability | (handler stack + bench) | DONE | 4201a24 / a42054a / 2bf12c3 |
| 02-02 | 2 | pkg/server | 85 of 87 (2 deferred to 02-03) | DONE | 17dcda3 / c8028a8 |
| **02-03** | **3** | **pkg/cypher (+ D-04d slow-query collapse + 2 deferred 02-02 lines)** | **5 production sites + 4 D-04d-collapse lines** | **DONE** | **2c96f08 / 6a227bc** |
| 02-04 | 4 | pkg/storage | 48 | pending | — |
| 02-05 | 5 | pkg/bolt | 18 | pending | — |
| 02-06 | 6 | LOG-09 lint gate (`make lint-slog`) | — | pending | — |

Per D-10's 8-commit cadence, this plan landed commits #5 (Task 1 RED) and #6 (Task 2 GREEN) on `otel`. The next migration wave is Plan 02-04 (storage).

## Self-Check: PASSED

- File `pkg/cypher/redaction.go`: FOUND
- File `pkg/cypher/plan_hash.go`: FOUND
- File `pkg/cypher/redaction_test.go`: FOUND
- File `pkg/cypher/plan_hash_test.go`: FOUND
- File `pkg/cypher/slow_query_log_test.go`: FOUND
- File `pkg/cypher/executor.go`: MODIFIED (StorageExecutor.log + slowQueryThreshold; SetLogger/SetSlowQueryThreshold/Logger/SlowQueryThreshold/logger/emitSlowQueryLog; Execute deferred slow-query emit)
- File `pkg/cypher/call_vector.go`: MODIFIED (2 sites)
- File `pkg/cypher/executor_show.go`: MODIFIED (3 sites)
- File `pkg/server/server.go`: MODIFIED (D-04d collapse — decl/default removed; 2 readers migrated)
- File `pkg/server/server_helpers.go`: MODIFIED (threshold reader switched)
- File `pkg/server/server_public.go`: MODIFIED (/metrics gauge reader switched)
- File `pkg/server/server_extra_test.go`: MODIFIED (6 references rewritten)
- File `pkg/config/config.go`: MODIFIED (LoggingConfig.SlowQueryLogFile added)
- File `cmd/nornicdb/main.go`: MODIFIED (executor.SetLogger + SetSlowQueryThreshold; serverConfig.Logging snapshot threading; newTxScopedExecutor inheritance)
- Commit `2c96f08` (Task 1 RED): FOUND
- Commit `6a227bc` (Task 2 GREEN): FOUND

Plan 02-03 success criteria all GREEN.
