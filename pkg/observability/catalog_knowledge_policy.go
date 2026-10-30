// Package observability — Knowledge-policy metric bag.
//
// Owns ten families (see docs/plans/knowledge-policy-observability-plan.md):
//
//	nornicdb_knowledge_policy_scored_total{entity_kind,result[, database]}
//	nornicdb_knowledge_policy_decay_score{entity_kind[, database]}       (sampled 1/32)
//	nornicdb_knowledge_policy_suppressions_total{entity_kind,reason[, database]}
//	nornicdb_knowledge_policy_access_flush_batch_rows                     (no tenant label)
//	nornicdb_knowledge_policy_access_flush_duration_seconds               (no tenant label)
//	nornicdb_knowledge_policy_access_flush_buffer_fullness                (GaugeFunc; D-15b)
//	nornicdb_knowledge_policy_on_access_mutations_total{result[, database]}
//	nornicdb_knowledge_policy_deindex_enqueued_total{entity_kind[, database]}
//	nornicdb_knowledge_policy_read_filter_dropped_total{entity_kind[, database]}
//	nornicdb_knowledge_policy_reconcile_total{trigger[, database]}
//
// Closed enums:
//
//	entity_kind ∈ AllowedKnowledgePolicyEntityKinds = {node, edge, property}
//	result      ∈ AllowedKnowledgePolicyScoreResults = {visible, suppressed, no_decay}
//	reason      ∈ AllowedKnowledgePolicySuppressReasons = {below_threshold, score_floor,
//	                                                        on_access, explicit_flag, rule_cap}
//	on_access_result ∈ AllowedKnowledgePolicyOnAccessResults = {applied, skipped_no_policy, error}
//	trigger     ∈ AllowedKnowledgePolicyReconcileTriggers = {schema_change, startup, manual}
//
// Forbidden-label discipline: the graph-schema node `label` axis and user
// property keys are DELIBERATELY excluded — user DDL can create unbounded
// values. Use exemplar trace attributes or structured logs for per-label
// detail, NEVER Prometheus labels.
//
// Hot-path discipline (MET-25): `Scorer.score()` runs per node returned from
// any Cypher match when decay is enabled. The bag pre-binds the per-
// entity_kind counters and histogram at construction; the caller caches the
// Bound* observers in a struct field and calls Observe/Inc without a
// WithLabelValues lookup on the hot path.
//
// Sampling: DecayScore is sampled 1/32 via ObserveDecayScoreSampled so a
// 1M-row scan pays only ~31k histogram Observe calls instead of 1M. Counter
// fires (ScoredTotal, SuppressionsTotal) are cheap enough to fire every time.
//
// D-08 forward-compat: NewKnowledgePolicyMetrics(reg, tenantLabelsEnabled,
// bufferFullnessFn) decides label-set shape ONCE at construction. When
// tenantLabelsEnabled is false, the `database` label is OMITTED from all
// labelnames. Flush-level metrics (batch_rows, duration, buffer_fullness)
// are intentionally NOT tenant-scoped — the AccessFlusher is cross-namespace.
package observability

import (
	"context"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// Closed-enum values. Keep in sync with docs/plans/knowledge-policy-
// observability-plan.md and catalog_knowledge_policy_test.go.
var (
	AllowedKnowledgePolicyEntityKinds = []string{
		"node",
		"edge",
		"property",
	}
	AllowedKnowledgePolicyScoreResults = []string{
		"visible",
		"suppressed",
		"no_decay",
	}
	AllowedKnowledgePolicySuppressReasons = []string{
		"below_threshold", // score fell under binding's VisibilityThreshold
		"score_floor",     // explicit ScoreFloor hit
		"on_access",       // on-access policy marked for suppression
		"explicit_flag",   // node.VisibilitySuppressed already true on read
		"rule_cap",        // per-rule cap / limit tripped
	}
	AllowedKnowledgePolicyOnAccessResults = []string{
		"applied",
		"skipped_no_policy",
		"error",
	}
	AllowedKnowledgePolicyReconcileTriggers = []string{
		"schema_change",
		"startup",
		"manual",
	}
)

// DecayScoreSampleDenominator is the 1/N sampling rate for the decay_score
// histogram. One sample in DecayScoreSampleDenominator is recorded; the rest
// are dropped. Keep power-of-two for cheap bitmask tests.
const DecayScoreSampleDenominator = 32

// KnowledgePolicyMetrics is the typed handle-bag for the knowledge_policy
// subsystem. One bag per Provider; constructed at cmd/nornicdb startup and
// attached to the Scorer/AccessFlusher through constructor injection, plus
// published to the pkg/storage read-path filter via SetKnowledgePolicyMetrics.
type KnowledgePolicyMetrics struct {
	// Scored counts scoring evaluations by entity_kind × result.
	Scored *prometheus.CounterVec

	// DecayScore is a 0.0..1.0 score distribution sampled 1/32. See
	// ObserveDecayScoreSampled for the chokepoint.
	DecayScore *prometheus.HistogramVec

	// Suppressions counts suppression events by entity_kind × reason.
	Suppressions *prometheus.CounterVec

	// AccessFlushBatchRows observes the row count of each AccessFlusher
	// flush — reused RowCountBuckets (1..1M).
	AccessFlushBatchRows *RowCountHistogram

	// AccessFlushDuration observes end-to-end flush wall-clock time.
	AccessFlushDuration *LatencyHistogram

	// OnAccessMutations counts on-access policy evaluations by result.
	OnAccessMutations *prometheus.CounterVec

	// DeindexEnqueued counts visibility-flip deindex enqueues.
	DeindexEnqueued *prometheus.CounterVec

	// ReadFilterDropped counts read-path filter suppressions (called on
	// every node/edge returned by Badger when decay is enabled).
	ReadFilterDropped *prometheus.CounterVec

	// Reconcile counts policy-change reconcile passes by trigger.
	Reconcile *prometheus.CounterVec

	// tenantLabelsEnabled is captured at construction so Bind helpers can
	// drop the database arg. Subsystems are bool-agnostic.
	tenantLabelsEnabled bool

	// Sampling counter for the decay_score histogram. Incremented on every
	// score observation; an Observe fires when counter % denom == 0. Using
	// an atomic counter avoids the per-goroutine RNG state of rand.IntN on
	// hot reads — the exact sampling distribution is unimportant for a
	// histogram.
	decayScoreSampleCounter atomic.Uint64
}

// NewKnowledgePolicyMetrics constructs the knowledge-policy bag against reg.
//
// bufferFullnessFn is the passive-scrape callback for access_flush_buffer_fullness
// — typically reads len(accumulator.buffer)/maxBufferSize from the current
// AccessFlusher. The callback is wrapped in a defer-recover that returns 0 on
// panic (RISK-8 / Pitfall 1) so a buggy callback cannot poison the scrape.
//
// Validation + Pitfall-8 panic semantics inherited from Phase 3 typed
// constructors: missing suffix, forbidden labels, or invalid subsystem panic
// at registration.
func NewKnowledgePolicyMetrics(
	reg *prometheus.Registry,
	tenantLabelsEnabled bool,
	bufferFullnessFn func() float64,
) *KnowledgePolicyMetrics {
	// Label-set shapes keyed off tenantLabelsEnabled (D-08).
	entityKindLabels := []string{"entity_kind"}
	entityKindResultLabels := []string{"entity_kind", "result"}
	entityKindReasonLabels := []string{"entity_kind", "reason"}
	onAccessLabels := []string{"result"}
	triggerLabels := []string{"trigger"}
	if tenantLabelsEnabled {
		entityKindLabels = append(entityKindLabels, "database")
		entityKindResultLabels = append(entityKindResultLabels, "database")
		entityKindReasonLabels = append(entityKindReasonLabels, "database")
		onAccessLabels = append(onAccessLabels, "database")
		triggerLabels = append(triggerLabels, "database")
	}

	kp := &KnowledgePolicyMetrics{
		tenantLabelsEnabled: tenantLabelsEnabled,
	}

	kp.Scored = NewCounterVec(reg,
		MetricOpts{
			Subsystem: "knowledge_policy",
			Name:      "scored_total",
			Help: "Knowledge-policy scoring evaluations by entity_kind (closed enum: " +
				"node, edge, property) and result (closed enum: visible, suppressed, " +
				"no_decay). Fires on every Scorer.score() call; suppressed/visible " +
				"ratio = working-set attrition rate.",
		},
		entityKindResultLabels)

	kp.DecayScore = NewScoreHistogramVec(reg,
		MetricOpts{
			Subsystem: "knowledge_policy",
			Name:      "decay_score",
			Help: "Post-decay score distribution (0.0..1.0), sampled 1/32 to bound " +
				"hot-path cost. Bimodal = healthy, flat = misconfigured half-life. " +
				"See ObserveDecayScoreSampled chokepoint.",
		},
		entityKindLabels)

	kp.Suppressions = NewCounterVec(reg,
		MetricOpts{
			Subsystem: "knowledge_policy",
			Name:      "suppressions_total",
			Help: "Suppression events by entity_kind and reason (closed enum: " +
				"below_threshold, score_floor, on_access, explicit_flag, rule_cap). " +
				"Subset of scored_total{result=\"suppressed\"} broken down by cause.",
		},
		entityKindReasonLabels)

	// Flush-level metrics are NOT tenant-scoped (AccessFlusher is cross-
	// namespace). Plan explicitly documents this asymmetry.
	kp.AccessFlushBatchRows = NewRowCountHistogram(reg,
		MetricOpts{
			Subsystem: "knowledge_policy",
			Name:      "access_flush_batch_rows",
			Help: "Row count of each AccessFlusher flush. p99 near maxBufferSize " +
				"indicates backpressure; consider raising maxBufferSize or lowering " +
				"DecayInterval.",
		},
		[]string{})

	kp.AccessFlushDuration = NewLatencyHistogram(reg,
		MetricOpts{
			Subsystem: "knowledge_policy",
			Name:      "access_flush_duration_seconds",
			Help: "End-to-end wall-clock time for AccessFlusher.Flush(). Correlates " +
				"with storage write p99 on the same process.",
		},
		[]string{})

	kp.OnAccessMutations = NewCounterVec(reg,
		MetricOpts{
			Subsystem: "knowledge_policy",
			Name:      "on_access_mutations_total",
			Help: "On-access policy evaluations by result (closed enum: applied, " +
				"skipped_no_policy, error). Fires from resolveOnAccessPolicy / " +
				"applyOnAccessMutations in pkg/knowledgepolicy/on_access_runtime.go.",
		},
		onAccessLabels)

	kp.DeindexEnqueued = NewCounterVec(reg,
		MetricOpts{
			Subsystem: "knowledge_policy",
			Name:      "deindex_enqueued_total",
			Help: "Entities enqueued for secondary-index removal after a visibility " +
				"flip (EnqueueDeindexIfSuppressed). Upstream cost generator for " +
				"label/edge_between/temporal/embedding index rebuilds.",
		},
		entityKindLabels)

	kp.ReadFilterDropped = NewCounterVec(reg,
		MetricOpts{
			Subsystem: "knowledge_policy",
			Name:      "read_filter_dropped_total",
			Help: "Nodes/edges dropped by the read-path decay filter " +
				"(pkg/storage/badger_decay_filter.go). Fired via the atomic-pointer " +
				"global bridge to avoid threading a meter through every storage " +
				"iterator signature. Experimental for first two releases — label " +
				"axis may be revised once real-world cardinality is observed.",
		},
		entityKindLabels)

	kp.Reconcile = NewCounterVec(reg,
		MetricOpts{
			Subsystem: "knowledge_policy",
			Name:      "reconcile_total",
			Help: "Policy-change reconcile passes by trigger (closed enum: " +
				"schema_change, startup, manual). Fired from " +
				"ReconcileDecaySuppressionWithChanges.",
		},
		triggerLabels)

	// MET-26 / D-15b: access_flush_buffer_fullness via GaugeFunc — passive
	// scrape reads the callback on every /metrics fetch so config / runtime
	// state is always fresh. Defer-recover returns 0 on panic.
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nornicdb",
		Subsystem: "knowledge_policy",
		Name:      "access_flush_buffer_fullness",
		Help: "Fraction of AccessFlusher buffer occupancy (0.0..1.0). " +
			"GaugeFunc reading len(buffer)/maxBufferSize on every scrape. " +
			"Sustained >0.9 indicates the flush interval can't keep up.",
	}, func() (val float64) {
		defer func() {
			if r := recover(); r != nil {
				val = 0
			}
		}()
		if bufferFullnessFn == nil {
			return 0
		}
		return bufferFullnessFn()
	}))

	return kp
}

// TenantLabelsEnabled reports whether this bag was constructed with the
// D-08 tenant-flag enabled. Read-only after construction.
func (k *KnowledgePolicyMetrics) TenantLabelsEnabled() bool {
	return k.tenantLabelsEnabled
}

// IncScored increments scored_total for the given (entity_kind, result)
// tuple. Tenant-flag-aware: database arg dropped when tenantLabelsEnabled
// is false. Callers pass the database unconditionally.
func (k *KnowledgePolicyMetrics) IncScored(entityKind, result, database string) {
	if k == nil {
		return
	}
	if k.tenantLabelsEnabled {
		k.Scored.WithLabelValues(entityKind, result, database).Inc()
		return
	}
	k.Scored.WithLabelValues(entityKind, result).Inc()
}

// IncSuppression increments suppressions_total for the given tuple.
func (k *KnowledgePolicyMetrics) IncSuppression(entityKind, reason, database string) {
	if k == nil {
		return
	}
	if k.tenantLabelsEnabled {
		k.Suppressions.WithLabelValues(entityKind, reason, database).Inc()
		return
	}
	k.Suppressions.WithLabelValues(entityKind, reason).Inc()
}

// IncOnAccess increments on_access_mutations_total for the given result.
func (k *KnowledgePolicyMetrics) IncOnAccess(result, database string) {
	if k == nil {
		return
	}
	if k.tenantLabelsEnabled {
		k.OnAccessMutations.WithLabelValues(result, database).Inc()
		return
	}
	k.OnAccessMutations.WithLabelValues(result).Inc()
}

// IncDeindexEnqueued increments deindex_enqueued_total for the entity_kind.
func (k *KnowledgePolicyMetrics) IncDeindexEnqueued(entityKind, database string) {
	if k == nil {
		return
	}
	if k.tenantLabelsEnabled {
		k.DeindexEnqueued.WithLabelValues(entityKind, database).Inc()
		return
	}
	k.DeindexEnqueued.WithLabelValues(entityKind).Inc()
}

// IncReadFilterDropped increments read_filter_dropped_total. Called from the
// storage layer via the atomic-pointer bridge (GetKnowledgePolicyMetrics).
func (k *KnowledgePolicyMetrics) IncReadFilterDropped(entityKind, database string) {
	if k == nil {
		return
	}
	if k.tenantLabelsEnabled {
		k.ReadFilterDropped.WithLabelValues(entityKind, database).Inc()
		return
	}
	k.ReadFilterDropped.WithLabelValues(entityKind).Inc()
}

// IncReconcile increments reconcile_total for the trigger.
func (k *KnowledgePolicyMetrics) IncReconcile(trigger, database string) {
	if k == nil {
		return
	}
	if k.tenantLabelsEnabled {
		k.Reconcile.WithLabelValues(trigger, database).Inc()
		return
	}
	k.Reconcile.WithLabelValues(trigger).Inc()
}

// ObserveDecayScoreSampled samples the decay_score histogram at 1/
// DecayScoreSampleDenominator. Callers invoke this on every score evaluation;
// the helper drops ~31/32 without a lock or per-goroutine RNG state so the
// hot path pays only a single atomic increment and a bitmask test.
//
// Using a uniform deterministic sampler (rather than rand.IntN) produces a
// slightly biased sample but the distribution shape is preserved at the
// histogram-bucket level — which is all operators care about.
func (k *KnowledgePolicyMetrics) ObserveDecayScoreSampled(
	ctx context.Context, entityKind, database string, score float64,
) {
	if k == nil {
		return
	}
	n := k.decayScoreSampleCounter.Add(1)
	if n%DecayScoreSampleDenominator != 0 {
		return
	}
	if k.tenantLabelsEnabled {
		k.DecayScore.WithLabelValues(entityKind, database).Observe(score)
		return
	}
	k.DecayScore.WithLabelValues(entityKind).Observe(score)
	_ = ctx
}

// ObserveAccessFlushBatchRows records one batch-size sample.
func (k *KnowledgePolicyMetrics) ObserveAccessFlushBatchRows(ctx context.Context, rows float64) {
	if k == nil {
		return
	}
	k.AccessFlushBatchRows.Observe(ctx, nil, rows)
}

// ObserveAccessFlushDuration records one flush-duration sample.
func (k *KnowledgePolicyMetrics) ObserveAccessFlushDuration(ctx context.Context, sec float64) {
	if k == nil {
		return
	}
	k.AccessFlushDuration.Observe(ctx, nil, sec)
}
