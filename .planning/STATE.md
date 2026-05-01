---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: milestone
current_phase: 02
status: executing
last_updated: "2026-05-01T23:40:35.291Z"
progress:
  total_phases: 13
  completed_phases: 2
  total_plans: 13
  completed_plans: 11
  percent: 85
---

# STATE: NornicDB Milestone 1 (Observability)

**Last updated:** 2026-04-30 (post-Phase-1 discuss-phase)

## Project Reference

- **What this is:** NornicDB M1 — implement ADR-0001 best-in-class observability (metrics + traces + logs + K8s integration).
- **Core value:** Operators can debug, monitor, and SLO-instrument NornicDB to the same depth as `pg_stat_statements` and Neo4j JMX MBeans, exposed via Prometheus pull *and* OTLP push, with zero-config Kubernetes integration, without leaking tenant identity through unauthenticated surfaces.
- **Authoritative docs:**
  - `.planning/PROJECT.md` — milestone scope, key decisions (KD-01..KD-12), constraints
  - `.planning/REQUIREMENTS.md` — 112 v1 requirements across 10 categories with phase traceability
  - `.planning/ROADMAP.md` — 13-phase delivery plan with goal-backward success criteria
  - `docs/architecture/adr/0001-observability.md` — the customer-API contract being delivered
  - `.planning/research/SUMMARY.md` — synthesis of stack/features/architecture/pitfalls research

## Current Position

Phase: 02 (structured-logging-migration) — EXECUTING
Plan: 5 of 6

- **Milestone:** M1 — Best-in-Class Observability
- **Current phase:** 02
- **Phase 0 audit trail:** ADR-0001 Status = `**Accepted**`, Sign-off date = 2026-04-30. §5 has all four roles signed (orneryd × 2 + linuxdynasty × 2, all dated 2026-04-30, all referencing PR #126). §4.1 row 0 = `e97ed2b..8e384cf | 2026-04-30 | orneryd` — the GOV-03 audit-trail format is proven self-referentially.
- **Status:** Ready to execute
- **PR strategy:** Single PR (#126) carries all 13 phases. ADR §4.1 audit trail uses commit ranges on `otel`. GitHub may auto-dismiss orneryd's review on subsequent commits — that's expected; the §5 text in the ADR is the durable audit trail.
- **Progress:** [█████████░] 85%

## Performance Metrics (Universal, KD-12)

These hard gates are enforced at every phase exit:

| Gate | Threshold | Evidence |
|------|-----------|----------|
| `make bench-cypher` (tracing OFF and ON) | Δ ≤ 2% | benchmark output in PR |
| `make bench-bolt` | Δ ≤ 5% | benchmark output in PR |
| `pkg/observability` coverage | ≥ 90% | `go test -cover` |
| `pkg/observability` file size | < 800 lines | `wc -l` |
| Neo4j wire compatibility | preserved | driver round-trip test |
| Memory floor vs pre-OTel baseline | ≤ 25 MB | `TestObservability_MemoryFloor` |
| Phase 01 P04 | 952 | 4 tasks | 7 files |
| Phase 02 P01 | 0h22m | 2 tasks | 13 files |
| Phase 02 P02 | 2464 | 2 tasks | 11 files |
| Phase 02 P03 | 1136 | 2 tasks | 14 files |
| Phase 02 P04 | 0h14m | 1 tasks | 17 files |

### Plan execution metrics

| Phase | Plan | Duration (s) | Tasks | Files |
|-------|------|--------------|-------|-------|
| 01    | 01   | 360          | 3     | 11    |
| 01    | 02   | 762          | 4     | 11    |
| 01    | 03   | 615          | 3     | 10    |

## Accumulated Context

### Key Decisions (from PROJECT.md)

- **KD-01..KD-12** — see PROJECT.md "Key Decisions" table. All decisions are at status `Pending` until Phase 0 ADR sign-off accepts them, then they move to `Accepted`.

### Phase Entry Inputs Pending

| Phase | Input | Default |
|-------|-------|---------|
| 1 | `tenant_label_mode=hash` for non-K8s | `enabled=false` is sufficient |
| 6 | `parent_capped` sampler max QPS | 100/s global |
| 8 | KD-08 Bolt driver matrix scope | minimum `neo4j-go-driver/v5` |
| 8 | Bolt session span lifetime | session=parent, message=child |
| 9 | `/readyz` warm-up budget | `failureThreshold=60` (~10 min) |

### Open Todos

- [x] Run `/gsd-plan-phase 0` to decompose ADR-revision work into plans (2026-04-29)
- [x] Run Plan 00-01 — ADR amendments + sha256 byte-stability snapshot (2026-04-29)
- [x] Decide reviewer GitHub handles (project memory: orneryd = Architecture+Public-API; linuxdynasty = SRE+Security) (2026-04-29)
- [x] Rebase `otel` to drop 3 pre-existing planning-doc commits (2026-04-29)
- [x] Run Plan 00-02 Task 1 — push otel + open PR #126 (2026-04-29)
- [x] Address Copilot review (8 fixes) — commit `7c9fe78` (2026-04-29)
- [x] Address orneryd's review (7 changes) — commit `2f451f4`; orneryd approved (2026-04-30)
- [x] Adopt single-PR M1 strategy per orneryd — commit `1ebdf36` (2026-04-30)
- [x] Run Plan 02 Task 2 — sign-off finalize commits `8e384cf` + `4965c51` (2026-04-30)
- [x] **Phase 0 ✅ CLOSED** — ADR Accepted, §5 signed off, §4.1 row 0 audit entry recorded (2026-04-30)
- [x] Plan Phase 1 (`/gsd-plan-phase 1`) — Observability Foundation Skeleton. Commits land on `otel`, no new PR. (Phase 1 planned into 5 plans)
- [x] Execute Plan 01-01 — pkg/lifecycle leaf package + ADR §2.8.1 A10c amendment (2026-04-30, commits fa272fc..3751c79)
- [x] Execute Plan 01-02 — pkg/observability core (config, resource, provider, registry) + pkg/config wiring (2026-04-30, commits c842f82..36f3a2c)
- [x] Execute Plan 01-03 — pkg/observability listener + health + pprof + testenv (2026-04-30, commits 8ce3d1b..06dbddf)
- [x] Execute Plan 01-04 — cmd/nornicdb integration: lifecycle.Run + adapters + db.HealthCheck wrapper (2026-04-30, commits b513977..ce17375 + meta aaf652a)
- [⚠] Execute Plan 01-05 — quality gate ran 2026-04-30; PERF-05 92.1% PASS, PERF-06 228 LOC PASS, OBS-01 boundary clean — but **race-stability sub-gate FAILS** (TestFakeComponent_StartedBefore flakes 40% under full -race -count=10). SUMMARY `.planning/phases/01-observability-foundation-skeleton/01-05-SUMMARY.md` records measurement; phase sign-off blocked.
- [ ] Plan 01-06 (proposed) — pkg/lifecycle/testenv.go atomicity fix (reorder Start/Shutdown counter operations); ~4-8 LOC diff; re-run race gate 5/5 green to unblock Phase 1.
- [ ] Re-run Plan 01-05 gates after Plan 01-06 lands; flip race row to ✅ in SUMMARY; mark PERF-05 / PERF-06 GREEN in REQUIREMENTS.md.
- [ ] At Phase 1 exit, fill §4.1 row 1 with the Phase 1 commit range using the same two-commit pattern.
- [ ] Repeat for Phases 2–12. At M1 completion, do final orneryd review + squash-merge of PR #126 to `main`.
- [ ] Re-add `.planning/` to `.gitignore` at any convenient point (the gitignore rule was on a dropped commit during the rebase; `.planning/` is currently untracked-but-not-ignored locally).
- [ ] Plan 02 Task 3 (`gh pr merge`) — formally pending until M1 completes. Marked `DEFERRED-TO-M1-COMPLETION` in Plan 02.
- [x] Execute Plan 02-01 — pkg/observability slog stack + cmd/nornicdb runServe bootstrap (2026-05-01, commits 4201a24..a42054a + meta 2bf12c3)
- [x] Execute Plan 02-02 — pkg/server log call sites migrated to slog (2026-05-01, commits 17dcda3..c8028a8). 85 of 87 sites migrated; lines 1650 + 1654 deferred to Plan 02-03.
- [ ] Pre-existing race in pkg/nornicdb/search_services.go (background clustering goroutine vs test teardown) — surfaces under `-race` + multi-test pkg/server runs. NOT introduced by Plan 02-02. Should be filed against the storage/search team for a focused fix (ctx-cancel observance OR per-DB sync.Mutex around `getOrCreateSearchService`). See `.planning/phases/02-structured-logging-migration/deferred-items.md`.

### Active Blockers

**Phase 1 quality-gate FAIL (2026-04-30):** `TestFakeComponent_StartedBefore` in `pkg/lifecycle/testenv_test.go` flakes under `-race -count=10` against the three Phase-1 packages. Empirical flake rate 2/5 = 40% per full-gate iteration. Root cause: `FakeComponent.Start` increments `startCount.Add` BEFORE the `startedAt` CAS and `startSeq.Store`; the test's `require.Eventually(StartCount==1)` predicate goes true before the seq counter is set, so `StartedBefore`'s `startSeq.Load()` reads 0 (or out-of-order). Zero `DATA RACE` reports — it's an ordering/visibility bug, not a memory race per Go's memory model.

**Fix:** Plan 01-06 (proposed) reorders the increments in `FakeComponent.Start` and `FakeComponent.Shutdown` so the seq store happens BEFORE the count increment. ~4-8 LOC diff in `pkg/lifecycle/testenv.go`. Re-verify by running `go test -tags nolocalllm -race -count=10 ./pkg/lifecycle/... ./pkg/observability/... ./cmd/nornicdb/...` 5 times — all 5 must pass.

**Plan 01-05 status:** Tasks 1-2 PASS (PERF-05=92.1%, PERF-06=228 LOC max), Task 3 race sub-gate FAIL, Task 4 sign-off NOT YET ISSUED. Plan is procedurally `in-flight pending fix-up`. PERF-05 and PERF-06 measurements are recorded but not marked GREEN in REQUIREMENTS.md until phase exits.

**Phase 1 sign-off:** ❌ NOT READY for §4.1 row 1 commit. Orchestrator must run Plan 01-06 + re-verify before issuing the audit-trail row.

### Recent Decisions

- **2026-05-01 (Plan 02-02 completed)**: First migration wave landed on `otel`. Two atomic commits: `17dcda3` feat (Task 1: Config.Logger field + discard fallback + Server.log field + cmd/nornicdb relocation of observability.NewLogger BEFORE server.New) and `c8028a8` feat (Task 2: 85 of 87 pkg/server log.Printf/fmt.Print* call sites migrated to s.log structured emission per D-10a; emoji-strip + bracket-prefix-to-subsystem-attribute; lines 1650 + 1654 deferred to Plan 02-03 D-04d collapse). server.New ctor arity unchanged (B2). pkg/audit/audit.go untouched (Pitfall 9). Falsifiable contract `TestRedactor_NoEmojiInJSONOutput` PASSES against new pkg/server records. Build green; `go test ./pkg/server/` PASS (76s). One deviation (Rule 3 auto-fix): relocated `observability.NewLogger` block in `cmd/nornicdb/main.go` to BEFORE `server.New` so the `*slog.Logger` could thread into `Config.Logger`. One deferred item: pre-existing race in `pkg/nornicdb/search_services.go` (background clustering goroutine vs test teardown) surfaces under `-race` + multi-test pkg/server runs; documented in `.planning/phases/02-structured-logging-migration/deferred-items.md`; out of M1 Phase 2 scope. Phase 2 progress: 2 of 6 plans complete (33%).

- **2026-04-30 (Plan 01-05 quality gate)**: Phase 1 quality gate ran end-to-end — coverage gate **PERF-05 PASS at 92.1%** (no regression vs Plan 01-04's 92.1% snapshot); file-size gate **PERF-06 PASS** at 228 LOC max production / 327 LOC max test (well under 800 cap; provider.go growth risk from RESEARCH Pitfall 10 has NOT materialized at 205 LOC); OBS-01 leaf-package boundary audit clean (`go list -deps ./pkg/observability/...` shows only buildinfo, lifecycle, stdlib, OTel, Prometheus, msgpack — zero business-package leaks). **Race-stability sub-gate FAILS:** `TestFakeComponent_StartedBefore` in `pkg/lifecycle/testenv_test.go` is intermittently flaky (2 PASS / 1 FAIL out of 3 controlled iterations of full-gate `go test -race -count=10` across `./pkg/lifecycle/... ./pkg/observability/... ./cmd/nornicdb/...`; combined with the initial gate-run FAIL and one re-run PASS, observed rate is 2/5 = 40%). Zero `DATA RACE` reports — it's an ordering bug, not a memory race. Root cause: `FakeComponent.Start` increments `startCount.Add(1)` on line 66 BEFORE the `startedAt` CAS on line 67 and `startSeq.Store(nextSeq())` on line 68. The test's `require.Eventually(b.StartCount() == 1)` predicate fires at line 66; `StartedBefore` immediately reads `startSeq.Load()` which may not have been set yet (observed values: `b_seq=0` and a < 0 returns false, OR out-of-order non-zero seq numbers). The fix is a 4-line reorder in `testenv.go`: store the seq BEFORE the count increment. Per the orchestrator's "DO NOT modify pkg/* code" constraint on Plan 01-05, the fix is deferred to Plan 01-06 (proposed). Bench gate (KD-12 `make bench-cypher` / `make bench-bolt`) is **N/A** — the Makefile does NOT implement those targets; ROADMAP Phase 12 owns landing them. Phase 1 does not touch hot paths so no regression is expected. **Phase 1 sign-off blocked** until Plan 01-06 lands and the race gate goes 5/5 green; only then does the orchestrator issue the §4.1 row 1 audit-trail commit.
- **2026-04-30**: Plan 01-04 executed cleanly (4 atomic commits: b513977 test RED, 962e90f feat HealthCheck, e21a281 feat adapters+main lifecycle.Run, ce17375 feat integration tests + ctx-observe Start fix). cmd/nornicdb/main.go runServe replaced 96 LOC of channel-based signal handling with ~70 LOC of lifecycle.Run wiring; three `lifecycle.Component` adapters land in `cmd/nornicdb/` (NOT pkg/observability/, preserving OBS-01 boundary). `(*DB).HealthCheck` calls `Engine.NodeCount` for a real probe (W-4 stub-warning resolved). Latent listener-supervisor deadlock fixed in pkg/observability/listener.go and pkg/observability/pprof.go (Rule 1 auto-fix): each Start now observes ctx.Done() and triggers internal Shutdown, preserving OQ4 ordering. Two requirements moved [ ]→[x]: OBS-07, OBS-08. coverage 92.1%, all four Phase-success integration tests GREEN.
- **2026-04-30**: Plan 01-03 executed cleanly (3 atomic commits: `8ce3d1b` test Wave-0 RED, `3c2ab36` feat Health registry + handlers + compile-only stubs, `06dbddf` feat full listener+pprof+testenv implementations + Provider.Shutdown sync.Once wrap). pkg/observability ships 4 new production files (listener.go 126 LOC, health.go 228 LOC, pprof.go 101 LOC, testenv.go 118 LOC — all under 250 LOC D-01 budget) + 5 new test files (listener_test.go, health_test.go, pprof_test.go, testenv_test.go, boundary_test.go). Coverage 92.3% (clears PERF-05 ≥90% gate with comfortable headroom). Race-clean under `-race -count=10` in 3.9s. OQ4 keystone implemented LITERALLY: `telemetryListener.Shutdown` calls `Provider.Shutdown(ctx)` BEFORE `srv.Shutdown(ctx)` — verified by line-ordering AND behavioral test. OBS-06 default bind is the literal `"127.0.0.1:9091"` per ADR §A9 (verified by `TestPprofListener_OptInAndDefault127`). OBS-01 leaf-package boundary now CI-failable as `boundary_test.go::TestPackageBoundary_NoBusinessImports` runs `go list -deps` and asserts zero imports of pkg/{cypher,storage,bolt,server,nornicdb,search,inference,replication}. /readyz JSON shape locked to D-03 `{"ok": bool, "checks": {"name": {"ok", "latency_ms", "error"}}}`; /version JSON locked to D-03b 5-key form. NewTestEnv(t) ships as the TEST-01 foundation Phases 3+ build on. Three requirements moved `[ ]`→`[x]`: OBS-01, OBS-05, OBS-06. Four auto-fixes: Rule 1 made `Provider.Shutdown` actually idempotent (sync.Once) since both listener and TestEnv call it; Rule 1 fixed `TestPprofListener_NotImportedFromDefaultMux` premise — switched to source-grep since importing net/http/pprof at all triggers init() registration on http.DefaultServeMux as a Go stdlib property; Rule 2 shored coverage 90.0%→92.3% with two small edge tests; Rule 3 added compile-only stubs in Task 01 so TestHealth_* could run before Task 02 lands real implementations.
- **2026-04-30**: Plan 01-02 executed cleanly (4 atomic commits: `c842f82` test Wave-0 RED, `dd35642` feat config+resource+stubs, `7689318` feat provider+registry full pipeline, `36f3a2c` feat pkg/config wiring). pkg/observability ships 5 production files (doc.go 65 LOC, config.go 127, resource.go 86, provider.go 192, registry.go 75 — all well under 250 LOC D-01 budget) + 4 test files. Coverage 90.4% (clears PERF-05 ≥90% gate). Race-clean under `-race -count=1 -timeout=120s ./pkg/observability/ ./pkg/config/...`. OBS-11 keystone (`TestProvider_OTLPFailureUsesNoop`) verified: bad endpoint produces non-recording noop tracer in <1s wall clock; New returns nil error. OBS-12 precedence (env > YAML > default) verified — and intentionally NOT consumed at config-load time (`grep -v '^\s*//' pkg/config/config.go | grep -c 'OTEL_EXPORTER_OTLP_ENDPOINT'` returns 0). Six requirement-tagged tests GREEN: OBS-02, OBS-03, OBS-04, OBS-10, OBS-11, OBS-12. Eight go.mod entries land: prometheus/client_golang v1.23.2 (new direct), otel/exporters/prometheus v0.65.0 (new direct), otel/exporters/otlp/otlptrace/otlptracegrpc v1.43.0 (new direct), plus indirect→direct promotions for otel core train (otel, sdk, sdk/metric, trace at v1.43.0). All four exporter+core deps already named in ADR-0001 §2.2 / §2.4 — no new ADR amendment needed. Two auto-fixes: Rule 3 added compile-only stubs for provider.go/registry.go in Task 01 so config/resource tests run against a compiling package (full impl lands in Task 02); Rule 2 added otlptracegrpc.WithTimeout to per-export RPC bound (defense in depth alongside init-time context.WithTimeout per Pitfall 2). One coverage-gate fix: TestProvider_Accessors added to clear ≥90%.
- **2026-04-30**: Plan 01-01 executed cleanly (3 atomic commits: `fa272fc` test Wave-0 RED, `441fe27` feat production code, `3751c79` docs ADR §2.8.1 A10c amendment). pkg/lifecycle ships as a 9-file leaf package (5 production + 4 test), 95.8% covered, race-clean under -race -count=10, largest file 200 LOC (well under PERF-06 800-line cap). `golang.org/x/sync` promoted from indirect to direct require. ADR-0001 §2.8.1 gains A10c naming `golang.org/x/sync/errgroup` per CLAUDE.md "direct deps require ADR amendment". OBS-09 (fresh-context shutdown) is fully owned by this plan; downstream Plans 01-02..01-05 inherit the property by calling `lifecycle.Run`. One auto-fix (Rule 1): FakeComponent ordering used wall-clock UnixNano which collided on the tight reverse-drain loop — fixed with a process-wide atomic.Int64 monotonic sequence. CONTEXT.md D-04d documentation discrepancy noted (says §2.5, applied to §2.8.1 where reconciled-amendments structurally belong).
- **2026-04-29**: Plan 00-01 executed cleanly. ADR-0001 grew 576 → 666 lines: §3.4 Accepted challenges (6 KD rows), 5 Reconciled amendments subsections (§§2.2.1, 2.3.1, 2.4.1, 2.6.1, 2.8.1), 11 canonical amendment IDs (A1..A10b) + canonical tokens (`WithoutUnits()`, `parent_capped`, `MaxQueueSize=8192`, `codec_version`, `127.0.0.1`, etc.) all grep-detectable in §§2.2–2.8 prose, §4.1 with 13 phase-exit rows, §5 enriched to `| Role | Reviewer | Date | PR |` with `<TBD reviewer name>` placeholders. Existing §3.4 renamed to §3.5. Status stays `**Proposed**` for Plan 00-02. 10 D-06 out-of-scope sections sha256-attested byte-stable. 1 deviation auto-fixed: Task 4's awk-boundary logic for §3.3/§4 needed reconstruction-aware slicing (documented in 00-01-SUMMARY.md).
- **2026-04-29**: Wave 2 (Plan 00-02 sign-off ceremony) deferred by user — opens GitHub PR + tags reviewers + waits for 4 sign-offs. Resumes via `/gsd-execute-phase 00-adr-governance-sign-off --wave 2` once reviewer handles are decided. Reviewer placeholder strings in Plan 02 (`<arch-handle>` etc.) need real GitHub usernames before resume.
- **2026-04-29**: Phase 0 planned as 2 plans (Option B decomposition): Plan 00-01 autonomous doc edits (§3.4 challenge table, §§2.2–2.8 amendments + 5 Reconciled subsections, §4.1 13-row pre-seed, §5 column enrichment, byte-stability snapshot of out-of-scope sections), Plan 00-02 non-autonomous sign-off ceremony (open PR, four-role review checkpoint, Status flip to Accepted, fill §5 reviewer rows + §4.1 row 0). Plan-checker found 0 blockers; 4 quality warnings were addressed in a revision pass (drop contrived §2.5.1, add sha256 byte-stability assertions for D-06 sections, propagate `$PR_NUMBER` via on-disk artifact, assert PR URL identity across §5 and §4.1 row 0).
- **2026-04-29**: Roadmap finalized at 13 phases (granularity=fine), splitting ADR §2.9's original 5-phase rollout along natural fault lines: ADR governance, foundation, logging, metrics infra, metrics catalog, translation layer, tracing core, codec versioning prerequisite, cross-protocol tracing+PII, Helm/K8s, dashboards+CI, doc generator, perf hardening.
- **2026-04-29**: Major-version deprecation cutover (remove `:7474/metrics` translation layer) is intentionally NOT a v1 phase; it lands post-M1 after at least one minor release of overlap with the `Sunset` header (per REQUIREMENTS.md "Out of Scope").
- **2026-04-29**: Adopted `parent_capped` sampler as a third operator-selectable mode (TRC-06) without changing the v1 default of `TraceIDRatioBased(0.01)`.

## Session Continuity

### Last work

Phase 2 Plan 02 (pkg/server log-call-site migration) ran 2026-05-01 — 2 atomic commits on `otel`:

- `17dcda3` feat (Task 1: Config.Logger field + discard fallback + Server.log field; cmd/nornicdb relocates `observability.NewLogger` to BEFORE `server.New` so the *slog.Logger threads into Config.Logger at construction)
- `c8028a8` feat (Task 2: 85 of 87 pkg/server log call sites migrated to s.log structured emission; D-10a emoji-strip + bracket-prefix-to-subsystem-attribute applied per-site; lines 1650 + 1654 in server.go EXCLUDED with markers — owned by Plan 02-03 D-04d collapse)

Acceptance: grep-zero LOG-01 surface gate PASSES (modulo D-04d-owned excludes); `go build -tags nolocalllm ./...` PASS; `go test -tags nolocalllm ./pkg/server/` PASS (76s); `TestRedactor_NoEmojiInJSONOutput` PASS (no-emoji contract holds against new pkg/server records); `pkg/audit/` untouched (Pitfall 9 boundary clean); `s.audit.*` calls preserved verbatim. SUMMARY at `.planning/phases/02-structured-logging-migration/02-02-SUMMARY.md`.

One deferred item: pre-existing race in `pkg/nornicdb/search_services.go` (background clustering goroutine vs test teardown) surfaces under `-race` + multi-test pkg/server runs. NOT introduced by Plan 02-02 — verified by `git stash` repro against HEAD~1. Documented in `.planning/phases/02-structured-logging-migration/deferred-items.md`. Out of M1 Phase 2 scope (LOG-01 surface only); requires fix from storage/search team.

**Phase 2 progress: 2 of 6 plans complete (33%).** Next up: Plan 02-03 (cypher migration + D-04d slow-query collapse + AST literal redactor + plan_hash).

### Earlier — Phase 2 Plan 01

Phase 2 Plan 01 ran (2026-05-01) — 2 commits + meta on `otel`: `4201a24` test (RED scaffolding) + `a42054a` feat (full slog handler stack + redaction.go + recovering.go + mandatory_fields.go + Provider.Logger() accessor + cmd/nornicdb runServe two-phase bootstrap) + `2bf12c3` docs SUMMARY. `BenchmarkSlogHandler_Hot` = 2 allocs/op; `pkg/observability` coverage 91.2% (PERF-05 ≥90%); race-clean under `-race -count=10`.

### Earlier — Phase 2 discuss-phase

Phase 2 (Structured Logging Migration) discuss-phase ran 2026-05-01 — 12 implementation gray areas decided + 4 Claude's-discretion items captured. CONTEXT.md and DISCUSSION-LOG.md persisted at `.planning/phases/02-structured-logging-migration/` (commit_docs=false; .planning/ stays out of git per single-PR M1 strategy).

**Recommendation-driven discussion** at user's explicit request: every option carried a researched recommendation grounded in AGENTS.md (§1 Prove Value, §3 file size, §4 functional DI, §6 testing, §7 DRY, §8 separation of concerns), ADR-0001 §2.5, and existing-code evidence. User accepted all 12 recommendations.

**Key decisions captured (full text in 02-CONTEXT.md):**

- D-01: Constructor injection of `*slog.Logger` into top-level ctors (mirrors AGENTS.md §4 + Phase 1 D-03/D-04). Each top-level type holds `log *slog.Logger` field tagged `.With("component","<pkg>")`.
- D-02: Bench stdlib `slog.NewJSONHandler` first; swap inner handler only if AllocsPerRun budget (≤4) fails. Final 4-layer stack: recoveringHandler → mandatoryFieldsHandler → redactingHandler → slog.NewJSONHandler.
- D-03: `DefaultRedactKeys = ["password","token","authorization","secret","api_key","credentials"]` lives in `pkg/observability/redaction.go` locally; pkg/audit untouched (LOG-10/SEC-05); future leaf `pkg/redaction/` deferred post-M1.
- D-04: `cypher.RedactLiterals(query)` ANTLR4 visitor + `cypher.PlanHash(plan)` FNV-1a (both ship Phase 2; Phase 6 reuses PlanHash for nornicdb.cypher.plan span).
- D-05: trace_id/span_id resolution ships now via `trace.SpanContextFromContext(ctx)`; zero overhead when sampling off (Phase 1 noop default); Phase 6 sampling flip auto-lights up correlation in 175 sites.
- D-06: AsyncEngine carries `log *slog.Logger`; flush goroutine constructs `flushLog := ae.log.With(...)` once at start; cross-flush correlation deferred to Phase 8 WithLinks per ADR §2.4.
- D-08: Two-phase init in cmd/nornicdb/main.go: `observability.NewLogger(cfg.Logging)` first, then `observability.New(cfg, logger, ...)`.
- D-09: recoveringHandler outermost catches panics in handler stack; Provider.Shutdown opportunistic Sync() via interface assert.
- D-10: 8-commit migration cadence on `otel` (wave-0 RED + handler stack + 4 per-package waves server→cypher→storage→bolt + lint + SUMMARY).
- D-10a: Strip emoji from msg; bracket prefixes ([BOLT]/[Debug]) become `component` slog attribute.
- D-11: `make lint-slog` Makefile grep target wired into `make test` for LOG-09 enforcement.
- D-12: Extend Phase 1 TestEnv with optional `Buffer` for record capture (D-01 flat-file rule preserved).

**Confirmed call-site counts (grep on 2026-05-01):** pkg/server 87, pkg/cypher 22, pkg/storage 48, pkg/bolt 18 — total **175** (ROADMAP said "~165"; actual 175).

**Phase 1 race-stability fix landed:** commits `3b3f5ef` (FakeComponent.Start atomic-write reorder) + `3ad44d9` (Plan 01-05 outcome flip to gates-pass) + `0cc947a` (ADR-0001 §4.1 row 1 audit-trail entry). Phase 1 ✅ closed.

**Next up:** `/gsd-plan-phase 2` to decompose Phase 2 into plans against the locked CONTEXT. ROADMAP Phase 2 success criteria are the planner's must-haves: zero `log.Printf`/`fmt.Println` grep across the four packages, JSON output by default with mandatory fields + trace correlation, `password=hunter2` literal redacted to `<REDACTED>` in slow-query log, `slog.Default()` lint check rejects new uses outside pkg/observability, pkg/audit untouched.

### Earlier — Phase 1 race-stability fix

Plan 01-05 (Phase 1 quality gate) ran 2026-04-30 — initial outcome **gates-fail** (race-stability sub-gate). 2026-05-01 follow-up: `pkg/lifecycle/testenv.go` `FakeComponent.Start` increments reordered (commit `3b3f5ef`) — `startSeq.Store` lands BEFORE `startCount.Add`. `go test -tags nolocalllm -race -count=10` 5x in a row GREEN. Plan 01-05 SUMMARY race row flipped to ✅ (commit `3ad44d9`); PERF-05 / PERF-06 GREEN; ADR-0001 §4.1 row 1 audit-trail entry recorded (commit `0cc947a`). Phase 1 closed.

### Earlier — Plan 01-04

Plan 01-04 ran (2026-04-30) — four atomic commits + meta on `otel`: `b513977` test (HealthCheck RED), `962e90f` feat (HealthCheck GREEN via Engine.NodeCount), `e21a281` feat (lifecycle.Run + 3 adapters in cmd/nornicdb/), `ce17375` feat (4 Phase-success integration tests + listener Start ctx-observe fix), `aaf652a` docs (SUMMARY + STATE update). cmd/nornicdb/main.go runServe replaced with lifecycle.Run wiring; adapters live in cmd/ (preserving OBS-01); (*DB).HealthCheck calls Engine.NodeCount; Phase-success-1/2/3/5 tests all GREEN. Two requirements moved [ ]→[x]: OBS-07, OBS-08. coverage 92.1%, race-clean under -race -count=1.

### Earlier — Plan 01-03

Plan 01-03 ran (2026-04-30) — three atomic commits on `otel`:

- `8ce3d1b` test(01-03): Wave-0 RED test scaffolding (5 _test.go files: listener, health, pprof, testenv, boundary; 807 insertions; package fails to compile by design)
- `3c2ab36` feat(01-03): Health registry + /livez /readyz /version handlers (health.go 228 LOC) plus compile-only stubs for listener/pprof/testenv so TestHealth_* can run
- `06dbddf` feat(01-03): full :9090 telemetry listener + :9091 opt-in pprof + NewTestEnv(t); Provider.Shutdown wrapped in sync.Once for idempotency under listener+t.Cleanup callers

All gates green: `go test -race -count=10 -timeout=120s ./pkg/observability/` clean (3.9s); coverage 92.3% (PERF-05 ≥90%); `go list -deps ./pkg/observability/...` confirms zero imports of business packages (OBS-01); largest production file `health.go` @ 228 LOC (PERF-06 ≤800); OQ4 ordering enforced literally (Provider.Shutdown before srv.Shutdown) AND verified by behavioral test. SUMMARY.md committed at `.planning/phases/01-observability-foundation-skeleton/01-03-SUMMARY.md`.

Three requirements moved from `[ ]` to `[x]`: OBS-01 (leaf-package boundary), OBS-05 (four endpoints on :9090), OBS-06 (pprof opt-in + 127.0.0.1 default). VALIDATION rows 01-01-01, 01-01-05, 01-01-06 marked GREEN.

Next: Plan 01-04 (cmd/nornicdb/main.go integration — wire telemetry/pprof listeners into lifecycle.Run; add adapters for HTTP/Bolt/embed-queue; register `db.HealthCheck` after storage opens).

### Earlier — Plan 01-02

Plan 01-02 ran (2026-04-30) — four atomic commits on `otel`:

- `c842f82` test(01-02): Wave-0 RED test scaffolding for pkg/observability (4 _test.go files, 466 insertions; package fails to compile by design)
- `dd35642` feat(01-02): ObservabilityConfig + ServiceInfo + resource builder + compile-only stubs for provider/registry; go.mod promotes otel core to direct + adds prometheus/client_golang
- `7689318` feat(01-02): full provider.go + registry.go pipeline — OBS-11 noop fallback, OBS-03 init order, OBS-04 metrics-disabled path, BSP tuning per ADR §A6, OTel→Prom bridge under nornicdb_otel_* with WithoutUnits
- `36f3a2c` feat(01-02): pkg/config.Config gains Observability field + YAMLConfig.Observability block + applyEnvVars NORNICDB_OBSERVABILITY_* block; OTEL_EXPORTER_OTLP_ENDPOINT NOT frozen at config-load (grep gate returns 0)

All gates green: `go test -race -count=1 -timeout=120s ./pkg/observability/ ./pkg/config/...` clean; `go vet` clean; coverage 90.4% (≥90% PERF-05); largest production file `provider.go` at 192 LOC (well under 800 PERF-06 cap); `go list -deps ./pkg/observability/...` confirms zero imports of pkg/cypher/storage/bolt/server/nornicdb/search/inference/replication. SUMMARY.md committed at `.planning/phases/01-observability-foundation-skeleton/01-02-SUMMARY.md`.

Six requirements moved from `[ ]` to `[x]`: OBS-02 (config block), OBS-03 (init order), OBS-04 (metrics-disabled path), OBS-10 (instance.id chain), OBS-11 (noop fallback), OBS-12 (OTLP precedence). VALIDATION rows 01-01-02..04 + 01-03-01..03 marked GREEN.

Next: Plan 01-03 (listener.go, health.go, pprof.go, testenv.go).

### Earlier — Plan 01-01

Plan 01-01 ran (2026-04-30) — three atomic commits on `otel`:

- `fa272fc` test(01-01): Wave-0 RED test scaffolding (4 _test.go files, 352 insertions; package fails to compile by design)
- `441fe27` feat(01-01): pkg/lifecycle production code (5 production files, 293 insertions; supervisor.go uses signal.NotifyContext + errgroup.WithContext + fresh context.WithTimeout(context.Background(), 30s); go.mod x/sync indirect→direct)
- `3751c79` docs(adr-0001): §2.8.1 A10c amendment naming golang.org/x/sync/errgroup (D-04d)

All gates green: `go test -race -count=10 ./pkg/lifecycle/...` clean; `go vet` clean; coverage 95.8%; `go list -deps ./pkg/lifecycle/...` confirms zero imports of business packages; largest production file 128 LOC. SUMMARY.md committed at `.planning/phases/01-observability-foundation-skeleton/01-01-SUMMARY.md`.

---

### Earlier Phase 0 work

Plan 02 Task 2 ran (2026-04-30) using the two-commit sign-off pattern:

- `8e384cf` — Status flipped to **Accepted**, Sign-off date 2026-04-30 added, all four §5 reviewer rows filled (orneryd × 2 + linuxdynasty × 2, all dated 2026-04-30 + PR #126)
- `4965c51` — §4.1 row 0 entry: `e97ed2b..8e384cf | 2026-04-30 | orneryd` (GOV-03 audit-trail format proven by being the first row to use it)

Both commits pushed to `origin/otel`. PR #126 stays OPEN per single-PR M1 strategy. **Phase 0 is closed.** Plan 02 Task 3 (`gh pr merge`) is formally deferred to M1 completion.

The two-commit pattern (substantive work → audit-row commit referencing predecessor's SHA) becomes the template for Phases 1–12 to use when filling their own §4.1 rows.

### Next work

Plan Phase 1:

```
/gsd-plan-phase 1
```

Phase 1 = Observability Foundation Skeleton (per ROADMAP):

- New `pkg/observability` package as a leaf in the import graph (never imports `pkg/cypher`, `pkg/storage`, `pkg/bolt`, `pkg/server`)
- Config block `Observability.{metrics,tracing,logging,pprof}` in `nornicdb.yaml`
- Init order: logger → resource attributes → meter/tracer providers → registries → middleware → listeners
- Listener supervision via single `errgroup` + `signal.NotifyContext(SIGINT, SIGTERM)` covering `:7474`, `:7687`, `:9090`, `:9091`
- Shutdown drains in order: HTTP → Bolt → workers → observability flush; observability shutdown uses independent `context.WithTimeout(context.Background(), 30s)`
- `:9090/metrics`, `/livez`, `/readyz` (200-with-progress-JSON contract per §2.6 of ADR), `/version`
- Optional `:9091/debug/pprof/` gated by `NORNICDB_PPROF_ENABLED=true`, binds `127.0.0.1` by default
- OTLP exporter init failure → noop provider + WARN log (never fatal)
- ≥ 90% line coverage; no file in `pkg/observability` > 800 lines
- Phase entry input pending (per ROADMAP): `tenant_label_mode=hash` for non-K8s — default `enabled=false` is sufficient unless explicit decision

Phase 1 will use the two-commit sign-off pattern at exit: substantive Phase 1 commits land on `otel`, then a final commit fills §4.1 row 1 with `<phase1-first-sha>..<phase1-last-sha> | <verified-date> | <verifier>`.

The scheduled remote agent (`trig_013cMqECrn92Ug81nZrwE1Sc`) is **disabled** — its decision tree assumed per-phase merging and would post a misleading nudge. Re-arm it with a Phase-1-aware prompt if helpful, or leave disabled.

### Files written or modified across Phase 0 (cumulative)

- `docs/architecture/adr/0001-observability.md` (576 → 699 lines, 8 commits on `otel`: `e97ed2b`, `52f06ea`, `d5b764a`, `7c9fe78`, `2f451f4`, `1ebdf36`, `8e384cf`, `4965c51`)
- `.planning/phases/00-adr-governance-sign-off/00-01-PLAN.md` (gitignored)
- `.planning/phases/00-adr-governance-sign-off/00-02-PLAN.md` (gitignored, updated for single-PR strategy)
- `.planning/phases/00-adr-governance-sign-off/00-01-PRESTATE.sha256` (10 hashes, gitignored, updated post-Copilot for §2.7 + §3.3)
- `.planning/phases/00-adr-governance-sign-off/00-01-SUMMARY.md` (gitignored)
- `.planning/phases/00-adr-governance-sign-off/00-02-SUMMARY.md` (gitignored)
- `.planning/phases/00-adr-governance-sign-off/00-02-PR.txt` (`126`, gitignored)
- `.planning/REQUIREMENTS.md` (GOV-03 reworded for single-PR audit trail, gitignored)
- `.planning/ROADMAP.md` (phase exit criteria + ordering rules updated, gitignored)
- `.planning/STATE.md` (this file, gitignored)
- Project memory: `reviewer_roster.md`, `feedback_pr_titles.md`, `project_pr_strategy.md`
- GitHub PR #126 OPEN against orneryd/NornicDB main with orneryd's `Approve` review (may be auto-dismissed by GitHub when Phase 1+ commits land — that's expected)

---
*State initialized 2026-04-29.*
