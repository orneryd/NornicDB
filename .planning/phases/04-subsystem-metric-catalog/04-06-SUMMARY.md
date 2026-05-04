---
phase: 04-subsystem-metric-catalog
plan: 04-06
subsystem: observability
tags: [observability, metrics, replication, auth, lifecycle-component, peer-gc, risk-3, gap-1, gap-6, met-14, met-15, met-25, prometheus, raft, ha-standby, multi-region, peer-tracker, classify-auth-result]

# Dependency graph
requires:
  - phase: 04-subsystem-metric-catalog (04-01)
    provides: catalog_replication_test.go + catalog_auth_test.go RED scaffolds; testenv.AssertCardinalityCeiling helper; closed-enum forbidden-label discipline
  - phase: 04-subsystem-metric-catalog (04-02)
    provides: pkg/bolt/server.go HELLO completion call site (observeAuthAttempt) and SetAuthMetrics setter — this plan supplies the non-nil bag
  - phase: 04-subsystem-metric-catalog (04-04)
    provides: pkg/storage/bytes_metrics.go lifecycle.Component pattern (mirrored by peer_metrics_gc); registration order between bytes-sweeper and workersC
  - phase: 04-subsystem-metric-catalog (04-05)
    provides: cmd/nornicdb embed_search_metrics.go bag-injection pattern; redstubs cleanup precedent
  - phase: 03 (telemetry foundation)
    provides: typed metric constructors (NewCounterVec/NewGaugeVec/NewLatencyHistogram); ForbiddenLabels D-03a panic; AssertCardinalityCeiling test seam; lifecycle.Component contract
  - phase: 01 (lifecycle supervisor)
    provides: lifecycle.Component interface; supervisor errgroup + drain order

provides:
  - 10 replication metric families per ADR §2.3 (role, term, commit_index, apply_index, leader_changes_total, lag_bytes, lag_entries, rtt_seconds, apply_duration_seconds, last_contact_seconds)
  - 1 auth metric family auth_attempts_total{result, protocol} per GAP-6 / MET-15
  - GAP-1 last_contact_seconds{peer} keystone — Phase 9 alert rule data source
  - PeerLabel(PeerConfigLike) helper with RISK-3 corrected resolution (PeerConfig.ID || .Addr; never socket-derived)
  - Mode-aware peer cardinality ceiling (ha_standby=8, raft=16, multi_region=64)
  - peer_metrics_gc lifecycle.Component (D-05b) with PeerTracker + sweep eviction
  - replicator_metrics observation seam (D-15a per-event role/term/index updates)
  - ClassifyAuthResult mapper from error sentinels to closed result enum
  - Authenticator.RecordAttempt + SetAuthMetrics
  - Bolt SetAuthMetrics non-nil bag (lights up Plan 04-02 HELLO call site)
  - cmd/nornicdb integration (initReplicationAuthMetrics + lifecycle component registration)

affects:
  - phase 04-07 (observability hardening) — race-stability -count=10 includes pkg/replication; bench artifact bench-replication-auth-04-06.txt
  - phase 07 (codec versioning) — replicator already instrumented; codec_version field addition does not require metric changes
  - phase 08 (TRC-20 replication spans) — `nornicdb.replication.append` + `nornicdb.replication.apply` spans piggy-back on observation chokepoints
  - phase 09 (Helm/Grafana) — alert rule `time() - nornicdb_replication_last_contact_seconds{peer="..."} > N` uses GAP-1 keystone

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Per-event observation at existing log sites (D-15a): single-line metric addition where the lifecycle log already fires; zero polling overhead"
    - "Metrics observation seam (replicator_metrics.go): non-exported helper struct holds bag + tracker + pre-bound observers; nil-safe at receiver level"
    - "MetricsAware optional interface: Replicator implementations expose SetReplicatorMetrics(bag, tracker) with atomic.Pointer for race-clean injection"
    - "RISK-3 PeerConfigLike boundary: pkg/observability declares accessor interface (GetID/GetAddr); pkg/replication satisfies via concrete PeerConfig — leaf-package boundary preserved (D-02d)"
    - "Mode-aware cardinality ceiling stored in package-private map; constructor records mode for ceiling assertions"
    - "lifecycle.Component for stale-peer GC: 5min sweep / 24h staleness production defaults; defer-recover wraps sweep per RISK-8"
    - "Rebind-on-reconnect contract: replicator MUST NOT cache Bound observers across reconnects; WithLabelValues called fresh every observation site (Pitfall 3 mitigation)"
    - "ClassifyAuthResult: errors.Is-aware mapper from auth sentinels to {success, failure, denied} closed enum"
    - "Audit boundary: pkg/audit/audit.go untouched — auth_attempts_total is parallel observability signal, not audit replacement (T-04-06)"

key-files:
  created:
    - pkg/observability/catalog_replication.go        # 10 families + PeerLabel + RoleEnum + modeCeiling
    - pkg/observability/catalog_auth.go               # auth_attempts_total{result,protocol}
    - pkg/observability/catalog_replication_bench_test.go  # 4 sub-benches all 0 allocs/op
    - pkg/observability/catalog_auth_bench_test.go    # 2 sub-benches all 0 allocs/op
    - pkg/replication/peer_metrics_gc.go              # PeerMetricsGC lifecycle.Component + PeerTracker
    - pkg/replication/peer_metrics_gc_test.go         # eviction + race + cardinality + lifecycle tests
    - pkg/replication/replicator_metrics.go           # replicatorMetrics observation seam + MetricsAware
    - pkg/replication/replicator_metrics_test.go      # role transition + per-peer + apply + reconnect tests
    - pkg/auth/auth_metrics.go                        # ClassifyAuthResult + SetAuthMetrics + RecordAttempt
    - pkg/auth/auth_metrics_test.go                   # closed-enum + nil-safe + all-protocols + no-PII tests
    - cmd/nornicdb/replication_auth_metrics.go        # initReplicationAuthMetrics integration helper
    - cmd/nornicdb/replication_auth_metrics_test.go   # startup wiring + lifecycle component tests
    - .planning/phases/04-subsystem-metric-catalog/bench-replication-auth-04-06.txt
  modified:
    - pkg/observability/catalog_redstubs.go    # drop AuthMetrics + ReplicationMetrics stubs (replaced)
    - pkg/observability/catalog_replication_test.go  # RED → GREEN with full test suite
    - pkg/observability/catalog_auth_test.go         # RED → GREEN with full test suite
    - pkg/replication/replicator.go            # StandaloneReplicator metricsHolder + SetReplicatorMetrics
    - pkg/replication/raft.go                  # 4 D-15a observation sites + Apply duration + SetReplicatorMetrics
    - pkg/replication/ha_standby.go            # Start + Promote D-15a observation + SetReplicatorMetrics
    - pkg/auth/auth.go                         # Authenticator.metrics field
    - pkg/nornicdb/db.go                       # GetReplicator() accessor
    - cmd/nornicdb/main.go                     # initReplicationAuthMetrics wiring + peerGC component registration

key-decisions:
  - "RISK-3 corrected: peer label sources from PeerConfig.ID then .Addr (NOT .Name — that field doesn't exist on the actual struct)"
  - "Mode-aware cardinality ceiling: ha_standby=8 / raft=16 / multi_region=64 (D-05a)"
  - "D-05b stale-peer GC: 5min sweep + 24h staleness production defaults; lifecycle.Component drains AFTER workers, BEFORE telemetry"
  - "D-08a tenantLabelsEnabled accepted but IGNORED — replication is per-cluster, not per-database"
  - "D-15a per-event observation: zero polling; single-line metric additions at existing role-transition log sites (becomeLeader, stepping down, Promoted, Bootstrap)"
  - "leader_changes_total increments ONCE per leader-boundary transition (non-leader↔leader); not on every role write"
  - "Pitfall 3 / Q7 mitigation: WithLabelValues called fresh every observation; replicator does NOT cache Bound observers across reconnects"
  - "PeerConfigLike interface for D-02d boundary: pkg/observability never imports pkg/replication; concrete PeerConfig satisfies via GetID/GetAddr accessors"
  - "Auth bag has NO tenant flag — auth events are global per CONTEXT MET-21 (surfacing tenant on unauthenticated counter would itself be a leak)"
  - "ClassifyAuthResult mapping: nil → success; ErrInvalidCredentials/ErrInvalidToken/ErrSessionExpired → failure; ErrAccountLocked/ErrInsufficientRole/ErrNoCredentials → denied; unknown → failure (defensive)"
  - "T-04-06 audit boundary: pkg/audit/audit.go is NOT modified — auth_attempts_total is a parallel observability signal, not a replacement"

patterns-established:
  - "MetricsAware optional interface pattern: subsystems with multiple impls (Standalone/HAStandby/Raft/MultiRegion) all expose SetReplicatorMetrics via embedded metricsHolder (atomic.Pointer)"
  - "Closed-enum mapper pattern: ClassifyAuthResult is the canonical (error → enum) seam; defense in depth against drift via Phase 3 D-03a registration panic"
  - "Per-event observation at existing log sites (D-15a): grep for log.Printf transition sites and add a single metrics line; no new code paths"
  - "Bench template for hot-path: 0-alloc target via pre-bound observer + cached label cell; document Pitfall 3 contract in bench comment"

requirements-completed: [MET-14, MET-15, TEST-02, MET-25]

# Metrics
duration: 23min
completed: 2026-05-04
---

# Phase 04 Plan 04-06: Replication + Auth Subsystem Metric Catalogs Summary

**10 Replication families (incl. GAP-1 last_contact_seconds) and 1 Auth family (auth_attempts_total) GREEN; RISK-3 peer-label fix landed (PeerConfig.ID, not .Name); mode-aware cardinality ceiling and D-05b stale-peer GC lifecycle.Component prevent unbounded peer-label growth; all hot-path benches at 0 allocs/op.**

## Performance

- **Duration:** 23 min
- **Started:** 2026-05-04T02:36:08Z
- **Completed:** 2026-05-04T02:59:18Z
- **Tasks:** 7
- **Files created:** 13
- **Files modified:** 9

## Accomplishments
- 10 replication metric families register and emit per ADR §2.3 / MET-14 (role, term, commit_index, apply_index, leader_changes_total, lag_bytes{peer}, lag_entries{peer}, rtt_seconds{peer}, apply_duration_seconds, last_contact_seconds{peer} — the GAP-1 keystone for Phase 9 alert rules)
- 1 auth metric family auth_attempts_total{result, protocol} registers and increments per GAP-6 / MET-15; closed enums {success, failure, denied} × {bolt, http, grpc} = 9 cells
- RISK-3 corrected: PeerLabel(PeerConfigLike) reads PeerConfig.ID then PeerConfig.Addr (the .Name reference in CONTEXT D-05 was obsolete — RESEARCH §RISK-3 verified the actual struct shape)
- D-05a mode-aware cardinality ceiling stored and asserted: ha_standby=8 / raft=16 / multi_region=64
- D-05b PeerMetricsGC lifecycle.Component evicts stale peers (5min sweep / 24h staleness defaults); race-clean across `-race -count=10`
- D-15a per-event observation: role/term/commit_index/apply_index gauges set at existing transition log sites in raft.go (becomeLeader, stepping down on vote-resp + AE-resp, bootstrap-leader) and ha_standby.go (Start, Promote); zero polling overhead
- D-11/D-05e auth_attempts_total wired through Bolt (Plan 04-02 HELLO call site lights up), HTTP/gRPC adapters (via Authenticator.RecordAttempt), and tested across all 9 cells
- ClassifyAuthResult is errors.Is-aware: wrapped errors are unwrapped to find the underlying sentinel; defensive bucket sends unknown errors to "failure" (preserving "denied" for RBAC misconfig)
- pkg/audit/audit.go untouched per T-04-06 — auth_attempts_total is a parallel observability signal, not a replacement
- BenchmarkObserve_Hot{Replication,Auth}: 6 sub-benches, ALL at 0 allocs/op (budget ≤ 2)

## Task Commits

Each task committed atomically (--no-verify per worktree-isolated execution policy):

1. **Task 04-06-01: catalog_auth.go GREEN** — `4c136bf` (feat) — auth_attempts_total{result,protocol} + 5 named tests including TestAuthMetrics_NoUserLabel (T-04-01 PII discipline)
2. **Task 04-06-02: catalog_replication.go GREEN** — `bc27eaf` (feat) — 10 families + RISK-3 fix + D-05a/D-08a + 7 named tests
3. **Task 04-06-04: peer_metrics_gc.go** — `2b04eef` (feat) — D-05b lifecycle.Component + PeerTracker + 8 tests including TestPeerCardinality_ModeAware (1k synthetic peers → ≤ ceiling) and TestPeerGC_RaceSafe (-race -count=10)
4. **Task 04-06-03: replicator instrumentation** — `f03cb7e` (feat) — replicator_metrics observation seam + MetricsAware on Standalone/HAStandby/Raft + 4 D-15a observation sites in raft.go + 9 tests including TestRaftReplicator_BootstrapEmitsRole (live state machine)
5. **Task 04-06-05: pkg/auth metrics wiring** — `7935937` (feat) — ClassifyAuthResult + SetAuthMetrics + RecordAttempt + 9 tests
6. **Task 04-06-06: cmd/nornicdb integration** — `0739291` (feat) — initReplicationAuthMetrics + peerGC lifecycle component + 4 tests
7. **Task 04-06-07: hot-path benches** — `e65c865` (test) — 6 sub-benches all 0 allocs/op + bench-replication-auth-04-06.txt

_Note: Tasks were ordered 01 → 02 → 04 → 03 → 05 → 06 → 07 (04-06-04 hoisted before 04-06-03 because the replicator instrumentation needs the PeerTracker exposed by the GC component)._

**Plan metadata commit:** to follow (this SUMMARY.md commit).

## Files Created/Modified

### pkg/observability/
- `catalog_replication.go` (CREATED) — 10 families + PeerLabel + RoleEnum + modeCeiling map + Mode/Ceiling/TenantLabelsEnabled accessors
- `catalog_auth.go` (CREATED) — AuthMetrics{AuthAttempts *CounterVec} + AllowedAuthResults/Protocols closed enums
- `catalog_replication_test.go` (REWRITTEN) — TestReplicationMetrics_TenFamilies/RegistersTen/TenantFlagIgnored, TestPeerLabel_StablePerCfg/NeverRawIP, TestModeAwareCeiling, TestRoleEnum_ClosedMapping
- `catalog_auth_test.go` (REWRITTEN) — TestAuthMetrics_AuthAttemptsTotal/RegistersOneFamily, TestAuthResult_ClosedEnum/Protocol_ClosedEnum, TestMetricCardinality_Auth, TestAuthMetrics_NoUserLabel (D-03a panic verification)
- `catalog_replication_bench_test.go` (CREATED) — 4 sub-benches: lag_bytes_set, apply_duration_bound, role_set, leader_changes_inc
- `catalog_auth_bench_test.go` (CREATED) — 2 sub-benches: cached-cell Inc, WithLabelValues every call
- `catalog_redstubs.go` (MODIFIED) — drop AuthMetrics + ReplicationMetrics stubs; file remains as historical pointer

### pkg/replication/
- `peer_metrics_gc.go` (CREATED) — PeerMetricsGC lifecycle.Component + PeerTracker (Mark/StaleSince/Forget/Len) + sweep with defer-recover (T-04-08)
- `peer_metrics_gc_test.go` (CREATED) — TestPeerGC_Evicts/DoesNotEvictRecent/RaceSafe/ShutdownStops/StartViaShutdown/NilMetrics, TestPeerTracker_RaceSafe, TestPeerCardinality_ModeAware (1k synthetic peers × 3 modes)
- `replicator_metrics.go` (CREATED) — replicatorMetrics struct (bag + tracker + pre-bound applyDur) + observeRoleTransition/observePeerLag/observePeerRTT/observeApplyDuration; MetricsAware interface; metricsHolder atomic.Pointer pattern
- `replicator_metrics_test.go` (CREATED) — TestRoleTransition_GaugeUpdate (boundary semantics), TestPerPeer_LagBytes_Observation, TestApplyDuration_Observed (100 samples), TestPeerReconnect_NoStaleHandle, TestReplicatorMetrics_NilSafe, TestStandaloneReplicator_SetReplicatorMetrics, TestRaftReplicator_BootstrapEmitsRole (live raft state machine), TestPerPeerRTTObservation, TestRaftMetricsAwareInterface
- `replicator.go` (MODIFIED) — StandaloneReplicator metricsHolder field + SetReplicatorMetrics method; observability import
- `raft.go` (MODIFIED) — RaftReplicator metricsHolder field + SetReplicatorMetrics; D-15a observation at 4 transition sites (Bootstrap line 295, becomeLeader line 526, stepping-down vote-resp line 456, stepping-down AE-resp line 691); Apply outer chokepoint apply_duration observation
- `ha_standby.go` (MODIFIED) — HAStandbyReplicator metricsHolder field + SetReplicatorMetrics; D-15a observation at Start (line 245 — primary→leader, standby→standby) and Promote (line 545 — leader transition)

### pkg/auth/
- `auth_metrics.go` (CREATED) — ClassifyAuthResult mapper, AuthMetricsRecorder interface, Authenticator.SetAuthMetrics/AuthMetrics/RecordAttempt, authMetricsHolder
- `auth_metrics_test.go` (CREATED) — TestClassifyAuthResult_ClosedEnum (10 cases incl. wrapped errors), TestRecordAttempt_NilSafe, TestAuthAttempts_AllProtocols (9 cells), TestAuthAttempts_HTTP/HTTPFail/HTTPDenied, TestAuthAttempts_NoUserLabel (T-04-01), TestSetAuthMetrics_Idempotent
- `auth.go` (MODIFIED) — Authenticator.metrics authMetricsHolder field

### pkg/nornicdb/
- `db.go` (MODIFIED) — GetReplicator() accessor (used by cmd/nornicdb to inject ReplicationMetrics into the live replicator)

### cmd/nornicdb/
- `replication_auth_metrics.go` (CREATED) — initReplicationAuthMetrics integration helper; replicationAuthWiring return struct; peerGCInterval/peerGCStaleness override hooks
- `replication_auth_metrics_test.go` (CREATED) — TestServerStartup_ReplicationAuthMetricsRegistered (10+1 families), TestPeerGC_StartedByLifecycle, TestInitReplicationAuth_AuthInjected/NoReplicator/ReplicatorMetricsAware
- `main.go` (MODIFIED) — initReplicationAuthMetrics wiring (lines 859-875) + peerGC lifecycle component registration (between bytesSweeper and workersC per RESEARCH §Q4)

### .planning/phases/04-subsystem-metric-catalog/
- `bench-replication-auth-04-06.txt` (CREATED) — bench artifact

## Decisions Made

All decisions followed the plan as specified. The notable RISK-3 fix (peer label sources from PeerConfig.ID, fallback to .Addr; the CONTEXT D-05 reference to .Name was obsolete) was already encoded in the plan and verified against pkg/replication/config.go:357 during implementation. The RoleEnum wire contract (-1=unknown, 0=follower, 1=candidate, 2=leader, 3=standby) is the alert-rule contract — flipping these values would be a breaking change requiring an ADR amendment.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] commitIndex/lastApplied read under r.mu without logMu**
- **Found during:** Task 04-06-03 (raft.go observation site instrumentation)
- **Issue:** The vote-response stepdown site at line 456 originally observed `r.commitIndex, r.lastApplied` while holding only r.mu.Lock — but those fields are protected by r.logMu (separate mutex). This would race with apply-loop writers under -race.
- **Fix:** Acquire logMu.RLock around the read at both stepdown sites (vote-resp + AE-resp); same pattern Health() uses (raft.go:1384-1387).
- **Files modified:** pkg/replication/raft.go
- **Verification:** TestRoleTransition_GaugeUpdate + TestRaftReplicator_BootstrapEmitsRole pass under -race.
- **Committed in:** f03cb7e (Task 04-06-03 commit)

**2. [Rule 3 - Blocking] catalog_redstubs.go unused imports after stub deletion**
- **Found during:** Task 04-06-02 (replication catalog GREEN)
- **Issue:** After dropping ReplicationMetrics + AuthMetrics stubs, the prometheus import and stubPanic constant became unused — go build failed.
- **Fix:** Removed the import and stubPanic; added an explanatory comment that all stubs have shipped and the file remains as a historical pointer to the per-subsystem catalog files.
- **Files modified:** pkg/observability/catalog_redstubs.go
- **Verification:** go build clean; full pkg/observability test suite GREEN.
- **Committed in:** bc27eaf (Task 04-06-02 commit)

**3. [Rule 1 - Bug] TestPeerCardinality_ModeAware initial draft tested unrealistic flow**
- **Found during:** Task 04-06-04 (peer GC tests)
- **Issue:** First draft of the cardinality test created series via `bag.LagBytes.WithLabelValues(peer).Set(1)` for 1k synthetic peers without Marking them — but the GC only knows about Marked peers, so StaleSince returned empty and the test failed (got 992 series, ceiling 8).
- **Fix:** Restructured the driver to match the production contract: every observation site Marks the tracker. The test now Marks all 1k peers, waits past staleness, then re-Marks the last `ceiling` (simulating "live" peers). Sweep evicts the rest, leaving cardinality at or under the ceiling.
- **Files modified:** pkg/replication/peer_metrics_gc_test.go
- **Verification:** All three modes (ha_standby/raft/multi_region) pass; full pkg/replication suite GREEN under `-race -count=10`.
- **Committed in:** 2b04eef (Task 04-06-04 commit)

**4. [Rule 3 - Blocking] itoa name conflict in pkg/observability test files**
- **Found during:** Task 04-06-02 (replication catalog GREEN)
- **Issue:** Initial test added a tiny `itoa` helper in catalog_replication_test.go but listener_test.go already declared one — package-level collision.
- **Fix:** Renamed to `peerItoa`.
- **Files modified:** pkg/observability/catalog_replication_test.go
- **Verification:** Build clean; tests GREEN.
- **Committed in:** bc27eaf (Task 04-06-02 commit)

---

**Total deviations:** 4 auto-fixed (2 Rule 1 bugs, 2 Rule 3 blocking)
**Impact on plan:** All fixes were minor scope-bounded corrections (locking discipline, build hygiene, test framing). No scope creep. The plan's specified architecture and acceptance criteria all stand as written.

## Issues Encountered

**Worktree base mismatch at startup.** The worktree's HEAD pointed at commit `1757d5a` (storage fixes branch) instead of the expected `70746a2` (Plan 04-05 SUMMARY commit on otel branch). Followed the explicit reset protocol in the worktree_branch_check stanza — `git reset --hard 70746a28...` corrected the base, and execution proceeded normally.

**ld duplicate-libobjc warnings.** Persistent linker noise on macOS arm64 (`ld: warning: ignoring duplicate libraries: '-lobjc'`) appears on every cgo-linked test binary. Cosmetic; tests pass.

## Threat Flags

None. The plan's `<threat_model>` already covers every new surface this plan introduced (T-04-01 PII, T-04-02 cardinality, T-04-03 hot-path, T-04-05 GC race, T-04-06 audit boundary, T-04-08 sweep panic). All have verifiers in place.

## User Setup Required

None — Plan 04-06 ships pure observability code paths. Bag construction is automatic at server startup. Operators wishing to alert on the new GAP-1 keystone can add the Phase 9 rule:

```
# alert on staleness
ALERT replication_peer_stale
  IF time() - nornicdb_replication_last_contact_seconds > 60
```

…but this is part of Phase 9's Helm/Grafana deliverable, not Plan 04-06.

## Next Phase Readiness

- Plan 04-07 (subsystem catalog hardening): inputs ready — race-stability tests already pass at `-count=10`; bench artifact bench-replication-auth-04-06.txt cumulates with bench-embed-search-04-05.txt and bench-storage-mvcc-04-04.txt.
- Phase 7 (codec versioning prerequisite): the replicator is fully instrumented; codec_version field addition does not require any metric changes.
- Phase 8 (TRC-20 replication spans): `nornicdb.replication.append` and `nornicdb.replication.apply` spans piggy-back on the same observation chokepoints — `observeApplyDuration(ctx, ...)` already accepts a context.Context for future exemplar emission once Phase 6 flips the sampler.
- Phase 9 (Helm/Grafana): GAP-1 keystone (`last_contact_seconds{peer}`) is live; alert rule template lands in Phase 9.

## Self-Check: PASSED

All claimed files and commits verified:

- `pkg/observability/catalog_replication.go` exists
- `pkg/observability/catalog_auth.go` exists
- `pkg/replication/peer_metrics_gc.go` exists
- `pkg/replication/replicator_metrics.go` exists
- `pkg/auth/auth_metrics.go` exists
- `cmd/nornicdb/replication_auth_metrics.go` exists
- All 7 task commits exist in git log: 4c136bf, bc27eaf, 2b04eef, f03cb7e, 7935937, 0739291, e65c865
- `go test -race -count=1 ./pkg/{observability,replication,auth}/...` GREEN
- `go test -race -count=10 -run "TestPeerGC|TestPeerCardinality_ModeAware" ./pkg/replication/...` GREEN
- `go vet -tags nolocalllm ./pkg/replication/... ./pkg/auth/... ./pkg/observability/...` clean
- `go build -tags "nolocalllm noui" ./pkg/... ./cmd/...` succeeds
- `BenchmarkObserve_Hot/(replication|auth)`: all sub-benches at 0 allocs/op (budget ≤ 2)

---
*Phase: 04-subsystem-metric-catalog*
*Completed: 2026-05-04*
