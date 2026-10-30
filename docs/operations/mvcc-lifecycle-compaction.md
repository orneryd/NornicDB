# MVCC Lifecycle and Compaction

**Status:** Implemented and shipping
**Owner:** Storage/Engine

This document describes the MVCC lifecycle and compaction-control architecture that ships in NornicDB today. The lifecycle subsystem bounds storage growth from MVCC history while preserving snapshot-read correctness, and gives operators an admin surface to inspect and steer compaction work.

The implementation lives in:

- `pkg/storage/lifecycle/` — manager, planner, pressure controller, priority scheduler, emergency controller, reader registry, watermark, metrics
- `pkg/storage/badger_lifecycle.go` — engine wiring (controller injection, status, manual operations)
- `pkg/storage/types.go` — `MVCCLifecycleController` interface, `MVCCLifecycleDebtKey`, `SnapshotReaderInfo`
- `pkg/server/server_dbconfig.go` — admin HTTP routes (`/admin/databases/{db}/mvcc/*`)

For day-to-day usage see [MVCC Lifecycle Admin API](../user-guides/mvcc-lifecycle-admin-api.md) and [Historical Reads & MVCC Retention](../user-guides/historical-reads-mvcc-retention.md).

---

## 1. Purpose

Control MVCC history growth, prevent compaction starvation, and preserve snapshot correctness under sustained read/write load.

## 2. Goals

1. Bound storage growth while preserving current temporal semantics.
2. Make retention pressure actionable, not just observable.
3. Avoid global compaction stalls caused by long-running readers.
4. Provide predictable operator behavior under normal and emergency pressure.
5. Ensure fairness across tenants/workloads.

## 3. Non-goals

1. No breaking changes to existing snapshot-read semantics.
2. No removal of retained-floor behavior.
3. No mandatory tiered-history rollout.

---

## 4. Architecture

A single `MVCCLifecycleManager` (in `pkg/storage/lifecycle/manager.go`) owns reader tracking, watermark computation, prune planning, fenced apply, metrics, and pressure policy. Existing maintenance APIs (`PruneMVCCVersions`, `RebuildMVCCHeads`) remain as delegating wrappers. Storage engines (Badger, WAL, Async, Namespaced) all forward `TopLifecycleDebtKeys`, `LifecycleStatus`, and the manual-operation methods through the controller interface.

## 5. Core Data and Safety Model

`safe_floor` per logical key:

```text
safe_floor = min(
  oldest_reader_version,
  ttl_bound_version,
  max_versions_bound_version
)
new_floor = monotonic_max(previous_floor, safe_floor)
```

Floor advances only, never regresses. Pruning and chain-cap actions only act above `safe_floor`. Snapshot reads below the retained floor return not-found, matching the documented historical-read contract.

## 6. Reader Watermark Model

Reader tracking lives in `pkg/storage/lifecycle/reader_registry.go`. Each active reader records its ID, snapshot version, start time, and namespace. The manager computes `oldest_reader_version`, `oldest_reader_ts`, and `oldest_reader_age_seconds` on demand. Watermark is runtime state. Correctness depends on persisted floor and head invariants, not persisted watermark state.

## 7. Planner and Apply Execution

The planner (`pkg/storage/lifecycle/planner.go`) reads persisted MVCC heads plus the version keyspace iterator and emits an immutable run plan with per-key version fences. The apply phase (`pkg/storage/lifecycle/apply.go`) re-checks each fence before mutating storage; on a fence mismatch it skips the key, increments mismatch accounting, and requeues with backoff.

### 7.1 Fence Retry and Invalid-Plan Rules

- Initial requeue delay: 100 ms.
- Exponential backoff with jitter, capped at 5 s.
- Per-key retries per run: 3.
- Cross-run retries: configurable, default 20 within a 10-minute rolling window.
- After repeated mismatch a key enters a hot-contention cooldown (default 60 s) while other keys continue to be served.

Backoff and cooldown together prevent same-cycle replan loops; all retries pass through the backoff queue.

## 8. Work Prioritization and Fairness

Priority and cost lives in `pkg/storage/lifecycle/priority.go`:

```text
priority = f(debt_bytes, tombstone_depth, key_hotness, key_age)
```

Cost uses iterator seeks, value-log reads, and bytes rewritten/deleted as proxies. The scheduler maximises score-over-cost. Anti-starvation comes from priority aging, a max-skip count per key, and a reserved slice for the oldest unserved high-debt keys. Multi-tenant isolation is enforced via per-namespace lifecycle budget caps and a minimum guaranteed maintenance slice per namespace.

## 9. Pressure Policy and Backpressure

Pressure (`pkg/storage/lifecycle/pressure.go`) has three bands — `normal`, `high`, `critical` — with hysteresis windows on transitions to avoid flapping.

- **High:** rate-limit new long snapshots; emit client-warning headers/fields on degraded responses.
- **Critical:** reject new long snapshots; short snapshots continue normally.

Pinned-bytes pressure is mandatory and feeds the controller. The `mvcc_bytes_pinned_by_oldest_reader` metric drives enforcement, not just observation. Snapshot lifetime policy supports configurable max lifetime, graceful cancel, and hard kill under sustained critical pressure.

### 9.1 Baseline Threshold Guidance

Default baseline (operator-tunable):

1. `high_enter`: `max(5 GiB, 0.15 * data_dir_free_space)`
2. `high_exit`: `0.8 * high_enter`
3. `critical_enter`: `max(20 GiB, 0.35 * data_dir_free_space)`
4. `critical_exit`: `0.8 * critical_enter`

Default debounce windows:

1. enter window: 30 s sustained breach
2. exit window: 120 s sustained below exit threshold

## 10. Snapshot Kill Semantics

Forced expiration is scoped per active transaction reader. Cancel points sit at transaction operation and commit boundaries so torn-row semantics aren't possible. Both graceful cancel and hard expiration return a deterministic transient/resource-pressure error code, and structured audit events fire on every forced expiration.

### 10.1 Session Scope

Forced expiration applies cleanly to transaction-scoped readers. If a deployment multiplexes many queries on a long-lived shared snapshot at a higher session/router layer, all queries on that snapshot will fail consistently after hard expiration with the same error-code family — the failure is at the snapshot layer, not at any specific query.

## 11. Extreme Churn Guardrails

`max_chain_hard_cap` is enforced in prune planning and remains bounded by `safe_floor` invariants. Emergency mode (`pkg/storage/lifecycle/emergency.go`) activates on a debt-growth slope detected during critical pressure. While active it raises the compaction budget, tightens long-snapshot admission, and uses a separate emergency-prioritisation order. All adjustments respect the configured resource ceilings.

## 12. Resource Ceilings

The lifecycle manager enforces hard runtime, IO, and best-effort CPU caps within each cycle. Limits are exposed as configuration:

- max CPU share
- max IO budget per interval
- max runtime per cycle

Emergency mode budget adjustments are clamped by these ceilings.

## 13. Metrics

Pressure metrics:

- `mvcc_bytes_pinned_by_oldest_reader`
- `mvcc_compaction_debt_bytes`
- `mvcc_compaction_debt_keys`

Lifecycle metrics:

- `mvcc_active_snapshot_readers`
- `mvcc_oldest_reader_age_seconds`
- `mvcc_prunable_bytes_total`
- `mvcc_pruned_bytes_total`
- `mvcc_tombstone_chain_max_depth`
- `mvcc_floor_lag_versions`
- `mvcc_prune_run_duration_seconds`
- `mvcc_prune_run_keys_scanned_total`
- `mvcc_prune_stale_plan_skips_total`

All metrics are exposed as global aggregates. Per-namespace breakdowns are available for the high-traffic families (`compaction_debt_*`, `prunable_bytes_*`, `pruned_bytes_*`); the remaining families are shipped as global aggregates only.

### 13.1 Metrics Cadence and Overhead Controls

- Per-key debt sampling: default 5%, with a full scan every 20 cycles.
- Aggregation: 10 s rollup for hot counters, 60 s rollup for debt histograms.
- Per-namespace aggregates are always on. Per-key detail is gated behind a debug flag and capped cardinality (admin debt-inspection endpoint).

## 14. API and Operator Surface

HTTP routes (admin permission, mounted by `pkg/server/server_dbconfig.go`):

- `GET /admin/databases/{db}/mvcc` and `…/mvcc/status` — pressure band, oldest reader, pinned bytes, debt, last-run summary, fence-mismatch count.
- `POST /admin/databases/{db}/mvcc/pause` / `…/resume` — toggle automatic cycles without changing the configured interval.
- `POST /admin/databases/{db}/mvcc/prune` — trigger a prune cycle on demand.
- `POST /admin/databases/{db}/mvcc/schedule` — change the lifecycle interval at runtime; `0s` enables manual-only mode.
- `GET /admin/databases/{db}/mvcc/debt?limit=N` — top N debt keys.

The browser admin UI exposes the same controls under **Security → MVCC Lifecycle**, with confirmation gates on destructive actions, a database picker that excludes `system` and composite databases, and live displays of pressure band, debt, pinned bytes, and active readers.

Pressure-induced degradations propagate to clients through standard warning headers/fields on the response.

## 15. Replication and Coordination

Lifecycle decisions are node-local. The reader watermark is node-local runtime state because active readers are node-local. In replicated deployments the local lifecycle behaviour remains log/order-safe for local-store invariants, and no cross-node watermark coordination is required.

A control-plane coordinated pressure-hint mode is intentionally not part of the current implementation; the local model is sufficient for the deployment topologies NornicDB ships today.

## 16. Implementation Sequence (historical)

The subsystem was built in the following order. All steps are in main and exercised by tests today:

1. Manager scaffolding and config.
2. Reader registry and watermark computation.
3. Planner and apply with version fences.
4. Prioritisation, fairness, and cost model.
5. Pressure bands and admission enforcement.
6. Metrics and status endpoint.
7. Emergency mode and ceiling-aware budget adjustment.
8. Legacy maintenance methods delegating into the lifecycle subsystem.

The original plan called for a feature flag during rollout. The shipped implementation is config-driven (interval, ceilings, pressure thresholds) instead — operators control behaviour through the admin API and configuration rather than a binary on/off toggle.

## 17. Test Coverage

- Unit tests in `pkg/storage/lifecycle/` cover floor monotonicity, fence correctness, scoring/fairness, hysteresis transitions, and policy-action triggers.
- Integration tests in `pkg/storage/` cover active-reader tombstone compaction, high-churn prune bounding, and the operator debt-inspection path.
- Server-route tests cover the admin HTTP surface end-to-end.
- Reliability scenarios (long reader + high churn, staggered medium readers, multi-tenant contention, stale-plan races under concurrent writes) are exercised through the lifecycle and storage integration suites.
- Restart-with-history and watermark-reset behaviour is exercised through the storage MVCC tests; broader reliability suites covering every transition pattern are an ongoing area to extend.
- Performance benchmarks for end-to-end lifecycle debt-reduction rate and read/write latency impact are not committed yet — see "Future work" below.

## 18. Operator Acceptance

- Storage growth is bounded by the configured retention policy under the churn scenarios exercised in storage tests.
- No snapshot correctness regressions have been observed in the covered changes.
- Compaction makes progress with active readers in tested cases.
- Operators inspect pressure and debt through the admin endpoints and UI, with `pinned_bytes` and the debt counters as the primary explanatory signals.
- Per-namespace budgets prevent any single namespace from monopolising lifecycle work; this is exercised by integration tests.
- Emergency mode stabilises debt without exceeding the configured resource ceilings in tested scenarios.

## 19. Future Work

The shipped subsystem covers the operational requirements of every deployment NornicDB targets today. The following items remain open as opportunities, not gaps:

- **Performance non-regression gate:** publish before/after benchmarks with confidence intervals (read p50/p95/p99, write p50/p95/p99, throughput, allocs/op, storage-growth slope) under the validation protocol described in section 20.
- **Tiered temporal history:** the architecture is compatible (floor advancement, debt accounting, and compaction decisions are explicit and policy-driven), but no tiered storage path is implemented. Scope for a future plan if a deployment needs warm/cold tier splits.
- **Control-plane coordinated pressure hints:** cross-node pressure signalling for replicated deployments. Out of scope for the current local-decision model.
- **Multiplexed-query forced-failure:** if higher layers ever expose a true shared-snapshot session abstraction, that layer can map snapshot-scope expiration onto its multiplexed queries; not needed by the current transaction-scoped readers.
- **Iterator-boundary narrowing indexes:** the planner uses head and version keyspace iteration today; an optional narrowing index could reduce planner work for very large keyspaces with sparse churn.

## 20. Performance Non-Regression Validation Protocol

Reserved for the perf gate above. Any change to lifecycle behaviour must:

1. Run before/after benchmarks with the same dataset, config, and hardware.
2. Include cache-warm and cache-busted runs.
3. Report p50/p95/p99 read latency, p50/p95/p99 write latency, throughput, allocs/op, and storage-growth slope.
4. Publish results with confidence intervals.

Throughput regression budget: ≤3% reads, ≤5% writes. Latency regressions outside that envelope require an explicit waiver, a documented tradeoff, and a rollback plan. Under pressure or emergency mode, latency may degrade, but the system must remain within SLO error budgets and recover to baseline after pressure subsides.
