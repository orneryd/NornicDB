# Knowledge Policy Metrics Reference

All instruments live under the `nornicdb_knowledge_policy_*` prefix and are
registered by `pkg/observability/catalog_knowledge_policy.go` against the
shared Prometheus registry. Global bag handle published via
`observability.SetKnowledgePolicyMetrics`; the `pkg/storage/badger_decay_filter.go`
read path resolves it lazily via `observability.GetKnowledgePolicyMetrics()`.

See the overarching observability ADR at `docs/architecture/observability.md`.

## At-a-glance

| Metric                          | Type      | Labels                                | Best for                                  |
| ------------------------------- | --------- | ------------------------------------- | ----------------------------------------- |
| `scored_total`                  | Counter   | `entity_kind`, `result`, `[database]` | Decay workload + visible/suppressed ratio |
| `decay_score`                   | Histogram | `entity_kind`, `[database]`           | Score distribution (sampled 1/32)         |
| `suppressions_total`            | Counter   | `entity_kind`, `reason`, `[database]` | Why suppressions happen                   |
| `access_flush_batch_rows`       | Histogram | —                                     | Flush batch pressure                      |
| `access_flush_duration_seconds` | Histogram | —                                     | Flush cost                                |
| `access_flush_buffer_fullness`  | Gauge     | —                                     | Backpressure tripwire                     |
| `on_access_mutations_total`     | Counter   | `result`, `[database]`                | Policy-mutation workload                  |
| `deindex_enqueued_total`        | Counter   | `entity_kind`, `[database]`           | Secondary-index churn                     |
| `read_filter_dropped_total`     | Counter   | `entity_kind`, `[database]`           | Read-path visibility drops                |
| `reconcile_total`               | Counter   | `trigger`, `[database]`               | Schema/policy churn                       |

The `database` label is present only when
`cfg.Observability.Metrics.TenantLabelsEnabled` is true (Phase 5 K8s autodetect flips this). Flush-level families (`access_flush_*`) are intentionally not tenant-scoped because the flusher is cross-namespace.

## Enum values

```go
// pkg/observability/catalog_knowledge_policy.go
AllowedKnowledgePolicyEntityKinds = { "node", "edge", "property" }
AllowedKnowledgePolicyScoreResults = { "visible", "suppressed", "no_decay" }
AllowedKnowledgePolicySuppressReasons = { "below_threshold", "score_floor",
                                           "on_access", "explicit_flag", "rule_cap" }
AllowedKnowledgePolicyOnAccessResults = { "applied", "skipped_no_policy", "error" }
AllowedKnowledgePolicyReconcileTriggers = { "schema_change", "startup", "manual" }
```

These are closed enums — any mismatched label value indicates a bug in the
fire site, not a user-provided label. Adding a new value is a breaking change
that must touch the catalog, the documentation, and any alerting rules.

## Per-instrument reference

### `nornicdb_knowledge_policy_scored_total`

Counter. Labels: `entity_kind`, `result`, optional `database`.

Fires on every `Scorer.score()` call plus every early-return path
(`cb == nil` → `result="no_decay"`). The ratio
`scored_total{result="suppressed"} / scored_total{result="visible"}`
is the working-set attrition rate.

Interpretation:

- `result="visible"` — scored and returned to the caller.
- `result="suppressed"` — scored and dropped for visibility (also emits
  `suppressions_total{reason}` with the concrete cause).
- `result="no_decay"` — entity has no binding OR decay is disabled; the
  score is fixed at 1.0 and bypasses the decay path. Can dominate the
  counter in databases with sparse bindings — prefer
  `suppressed / (visible + suppressed)` over
  `suppressed / (visible + suppressed + no_decay)` for the attrition rate.

### `nornicdb_knowledge_policy_decay_score`

Histogram. Buckets: `ScoreBuckets` (0.05, 0.1, 0.2, …, 0.9, 0.95, 1.0).
Labels: `entity_kind`, optional `database`.

**Sampled 1 in 32.** The hot path pays an `atomic.Add` + bitmask test on
every scoring call but only Observes every 32nd. The sampling is
deterministic (counter-based), not random — sufficient to preserve the
bucket distribution shape which is what operators care about.

Interpretation:

- Healthy: bimodal distribution — cluster near 0.9–1.0 (fresh) and a long
  tail under the configured threshold.
- Unhealthy: flat across all buckets → half-life is mis-tuned; every entity
  decays at the same rate and nothing ever gets suppressed.
- Unhealthy: all mass at 1.0 → decay is effectively disabled for everything
  with a binding (check `cb.NoDecay` flag wiring).

### `nornicdb_knowledge_policy_suppressions_total`

Counter. Labels: `entity_kind`, `reason`, optional `database`.

Fired in concert with `scored_total{result="suppressed"}`; the two counters
increment together, one per `result=suppressed` outcome, and `suppressions_total` carries the detailed reason.

- `below_threshold` — post-decay score < binding's `VisibilityThreshold`.
  The common case.
- `score_floor` — binding's `DecayFloor` is non-zero and the final score
  sits at that floor; indicates a floor-policy decision.
- `on_access` — the AccessFlusher's suppression-recheck callback flipped
  the entity to suppressed. Fires AFTER the flush commits to storage.
- `explicit_flag` — `node.VisibilitySuppressed` / `edge.VisibilitySuppressed`
  was already `true` on read; no scoring ran.
- `rule_cap` — (reserved) per-rule cap / limit tripped. Pre-registered so
  future rule-aware paths don't force a breaking enum extension.

### `nornicdb_knowledge_policy_access_flush_batch_rows`

Histogram. Buckets: `RowCountBuckets` (1..1M). No labels.

One sample per `AccessFlusher.Flush()` including the empty-batch short
circuit (a zero-row sample is a useful signal for timer-driven waste).

Interpretation:

- p99 near `AccessFlushBufferSize` → flush can't keep up; raise
  `AccessFlushBufferSize` or lower `DecayInterval`.
- p50 at zero with non-zero p95 → the buffer-full trigger fires rarely
  relative to the timer; decay rate is dominated by DecayInterval, not
  entity access.

### `nornicdb_knowledge_policy_access_flush_duration_seconds`

Histogram. Buckets: `LatencyBucketsSeconds`. No labels.

End-to-end wall-clock time for `AccessFlusher.Flush()`. Correlates with
storage write p99 — if flush duration spikes while storage stays healthy,
the on-access-mutation evaluation or the suppression-recheck callback is
the bottleneck.

### `nornicdb_knowledge_policy_access_flush_buffer_fullness`

GaugeFunc. No labels. Evaluated on every scrape via
`observability.NewKnowledgePolicyMetrics`'s `bufferFullnessFn` callback
which reads `accessFlusher.BufferFullness()`.

Values: 0.0..1.0. Sustained values > 0.9 indicate the flush interval is
insufficient.

### `nornicdb_knowledge_policy_on_access_mutations_total`

Counter. Labels: `result`, optional `database`.

- `applied` — one or more mutations in the policy's `OnAccess` block fired
  for the entity.
- `skipped_no_policy` — the entity hit the flusher but no on-access policy
  matched its labels/edge type. Useful for sizing the on-access workload.
- `error` — at least one mutation expression failed to evaluate. Check
  structured logs for the concrete error.

Note that an entity with multiple successful mutations increments
`applied` exactly once (aggregate-per-row, not per-mutation).

### `nornicdb_knowledge_policy_deindex_enqueued_total`

Counter. Labels: `entity_kind`, optional `database`.

Fired from `SuppressionRecheckFunc` when `EnqueueDeindexIfSuppressed`
returns `becameSuppressed=true`. Upstream cost generator for the search
index subsystem — rising edges here should correlate with
`nornicdb_storage_index_rebuild_total` spikes.

### `nornicdb_knowledge_policy_read_filter_dropped_total`

Counter. Labels: `entity_kind`, optional `database`.

**Experimental for the first two releases.** Fired from
`pkg/storage/badger_decay_filter.go` on every node/edge dropped by the
read-path filter. Useful for measuring the visibility layer's impact on
Cypher result-set sizes.

Cardinality caveat: the fire site runs on every `Scorer.score()` result,
which can be >1M per second under heavy scans. The counter itself is
cheap (`prometheus.Counter.Inc` is lock-free), but be aware this family
generates high sample volumes.

### `nornicdb_knowledge_policy_reconcile_total`

Counter. Labels: `trigger`, optional `database`.

- `startup` — fires once when `db.Open()` initializes the knowledge-layer
  subsystem. Useful as a liveness signal: if it's never incremented, decay
  is disabled.
- `schema_change` — `SetKnowledgePolicyChangedHook` fired (Cypher
  CREATE/DROP DECAY PROFILE etc.). The paired
  `nornicdb.knowledge_policy.reconcile` span carries `changes_count`.
- `manual` — (reserved) administrative reconcile invocation; not yet fired
  in code.

## Spans

- `nornicdb.knowledge_policy.flush` — root span for each non-empty
  `AccessFlusher.Flush()` cycle. Downstream storage writes
  (`nornicdb.storage.Put*`) nest beneath it, producing one coherent trace
  per flush batch. Attributes:
  `batch.row_count`, `batch.property_suppression_changes`.
- `nornicdb.knowledge_policy.reconcile` — root span for each policy-change
  reconcile (`ReconcileDecaySuppressionWithChanges`). Attributes:
  `trigger`, `changes_count`, `database`.

## Recommended dashboard slices

**Primary panel (5 series):**

- `rate(nornicdb_knowledge_policy_scored_total[1m])` split by `result`.
- `sum by (reason) (rate(nornicdb_knowledge_policy_suppressions_total[5m]))`.
- `histogram_quantile(0.5, ...)` and `histogram_quantile(0.95, ...)` over
  `nornicdb_knowledge_policy_decay_score`.
- `histogram_quantile(0.99, ...)` over
  `nornicdb_knowledge_policy_access_flush_duration_seconds`.
- `rate(nornicdb_knowledge_policy_deindex_enqueued_total[1m])`.

**Debug panel:**

- `nornicdb_knowledge_policy_access_flush_buffer_fullness` (as a line —
  values in 0..1).
- `sum by (trigger) (rate(nornicdb_knowledge_policy_reconcile_total[5m]))`.
- `sum by (entity_kind, reason)
  (rate(nornicdb_knowledge_policy_suppressions_total[5m]))`.

## Operational runbook

- **`access_flush_buffer_fullness` > 0.9 sustained (5m+)** — the flush
  interval can't keep up. Raise `Memory.AccessFlushBufferSize`
  or lower `Memory.DecayInterval` (the two trade throughput for
  latency). Check `access_flush_duration_seconds` — if p99 is > 200ms
  the per-row work is expensive, not the timer.
- **`deindex_enqueued_total` flat while `suppressions_total{reason="on_access"}`
  is rising** — suppression-recheck callback is firing but
  `EnqueueDeindexIfSuppressed` returns `false` (entity wasn't actually
  flipped). Check `VisibilitySuppressed` flag propagation.
- **`decay_score` histogram completely flat (all samples in one bucket)** —
  half-life is mis-tuned or the anchor (`ScoreFrom`) is pinned to a fixed
  value. Sample a handful of entities with `CALL nornicdb.explainScore(id)`
  (if implemented) to inspect the computation.
- **`reconcile_total{trigger="schema_change"}` increasing unexpectedly** —
  a hot loop somewhere is re-writing decay DDL. Check audit log.
