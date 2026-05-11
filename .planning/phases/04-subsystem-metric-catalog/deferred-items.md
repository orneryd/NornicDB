# Phase 4 Deferred Items

## Pre-existing races amplified by `-count=10` matrix (NOT introduced by Phase 4)

**Detected:** 2026-05-03 during Plan 04-07-03 race-stability gate.

**Test invocation:** `go test -race -count=10 -tags nolocalllm` across the
nine modified packages (`pkg/observability`, `pkg/cypher`, `pkg/storage`,
`pkg/bolt`, `pkg/server`, `pkg/replication`, `pkg/embed`, `pkg/search`,
`pkg/auth`).

**Result:**
- ok: pkg/observability, pkg/embed, pkg/auth, pkg/cypher/antlr,
  pkg/cypher/fn, pkg/cypher/testutil, pkg/storage/lifecycle.
- FAIL: pkg/cypher, pkg/storage, pkg/bolt, pkg/server, pkg/replication,
  pkg/search.

**Race surface (frequency × file):**
- 229 × pkg/bolt/integration_test.go
- 112 × pkg/storage/badger.go
- 104 × pkg/storage/badger_txn_helpers.go
-  86 × pkg/storage/badger_nodes.go
-  60 × pkg/bolt/server.go (Phase 2 deferred-items.md listener publication)
-  44 × pkg/storage/badger_mvcc.go
-  20 × pkg/replication/ha_standby.go
-  10 × pkg/server/server.go
- (50 NornicDB files total touched by `WARNING: DATA RACE`)

### Cross-reference: Phase 2 deferred-items

Three pre-existing races already documented in
`.planning/phases/02-structured-logging-migration/deferred-items.md` — all
surface in the Phase 4 race output and confirm those plans' deferred-owner
ownership:

1. **pkg/nornicdb/search_services.go** — background clustering goroutine
   vs test teardown. Plan 02-02 owner. Surfaces in pkg/server tests.
2. **pkg/bolt/server.go listener publication** — `s.listener` write at
   server.go:837/841 vs read at server.go:878/879 with no mutex/channel
   synchronization. Plan 02-05 owner.
3. **pkg/bolt/auth_adapter.go AuthenticatorAdapter.allowAnonymous** —
   `SetAllowAnonymous` mutates without holding `mu`. Plan 02-05 owner.

### Phase-4-related observations exposed by new tests

Plan 04-04 added `TestBytesMetricsSweeper_RaceSafe` in
`pkg/storage/bytes_metrics_test.go`. Under `-race -count=10` the
concurrent-CreateNode body in lines 181-191 trips the race detector on
`b.opDurPut` / `b.opDurGet` field reads inside `observeStorageOp` (added
in Plan 04-04-06). The race address pattern (`Write at <field>` reported
on both goroutines) is the deferred-arg-capture artifact common to Go's
race detector when struct fields are read concurrently — the underlying
BoundLatencyObserver value-type is itself race-clean (single-pointer
prometheus.Observer interface).

**Classification:** Phase-4-EXPOSED but NOT Phase-4-INTRODUCED.
- The lack of explicit synchronization on the BadgerEngine struct fields
  predates Phase 4; CreateNode did not previously have an observation
  tap, but the race surface is on engine-internal mutable state, not on
  the metric handle.
- Plan 04-04 SUMMARY noted Plan 04-04-06 as race-clean under the in-plan
  bench harness; the matrix-level `-count=10` exposes the broader
  per-package contention amplification.

**Mitigation candidate (NOT executed in Phase 4):**
- Add `sync/atomic.Value` storage for `b.opDurPut` etc. so the field
  read/write is atomic and the deferred-arg-capture trace becomes
  unambiguously race-clean. Estimated diff ~30 LOC + tests in
  `pkg/storage/badger_metrics.go`.
- Alternative: hoist `b.opDurPut` etc. to package-level pre-bound
  observers (CreateNode reads a process-wide lookup, not a struct field).

### Resolution path

Phase 4 SUMMARY documents these as **not-introduced-by-Phase-4** in §7
(Race-stability). The deferred-owner is split:
- Plan 02-04/02-05's three carry-forward races stay with their owners.
- The bytes_metrics_test.go field-read race is owned by a
  small-scope follow-up (likely Phase 12 hardening or a focused storage
  hotfix on `otel`) — not blocking Phase 4 exit because:
  1. The race is in test instrumentation, not production hot path
     (production AttachMetrics happens once at startup before any
     CreateNode call — single writer + many readers, race-free).
  2. `pkg/observability` itself is race-clean under `-count=10` (32.7s
     run; the only `ok` result besides leaf-utility packages).
  3. CLAUDE.md compliance separation gate (`pkg/audit/` untouched) and
     all other universal gates pass.

### Phase 12 entry input

Phase 12 (Performance Gates & Hardening) consumes this list as the
race-stability work plan. The `-count=10` matrix becomes a routine CI
input once these races are addressed at the source.
