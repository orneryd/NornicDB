// Package observability — Cypher metric bag (Plan 04-03 GREEN).
//
// Owns eleven families per MET-08 + ADR §2.3:
//
//	nornicdb_cypher_queries_total{op_type[, database]}
//	nornicdb_cypher_query_duration_seconds{op_type[, database]}
//	nornicdb_cypher_planner_duration_seconds{op_type}
//	nornicdb_cypher_planner_cache_hits_total
//	nornicdb_cypher_planner_cache_misses_total
//	nornicdb_cypher_planner_cache_size
//	nornicdb_cypher_rows_returned{op_type}
//	nornicdb_cypher_active_transactions
//	nornicdb_cypher_transaction_conflicts_total{[database]}
//	nornicdb_cypher_slow_queries_total{[database]}
//	nornicdb_cypher_slow_query_threshold_seconds   (GaugeFunc; D-15b)
//
// Closed enums (CONTEXT D-04 corrected per RISK-1; D-04b parse_error sixth value):
//
//	op_type ∈ AllowedCypherOpTypes = {read, write, schema, admin, fabric, parse_error}
//
// RISK-1 corrected classifier surface — the actual normal-path classifier
// reads `*QueryInfo` from QueryAnalyzer (see pkg/cypher/op_type.go), NOT a
// non-existent `plan.Root.Op` field. The closed enum above is the contract;
// `pkg/cypher/op_type.go::classifyOpType` is the producer; this bag is the
// consumer. Three observation sites (admin-dispatch, parse-error site,
// normal-path-after-Analyze) cover all six values.
//
// Forbidden-label discipline (Phase 3 D-03 / registration.go ForbiddenLabels):
// `query`, `user`, `user_id`, `ip`, `uuid`, `email` would all be cardinality
// bombs for Cypher and panic at registration. `op_type` is bounded by the
// closed enum above; `database` is bounded by the per-tenant-flag axis (D-08);
// callers MUST NEVER pass raw query text or query_hash as a Prometheus label.
//
// D-12a planner cache locality: planner_cache_hits_total /
// planner_cache_misses_total / planner_cache_size live HERE under the cypher
// subsystem, not under the cross-cutting cache subsystem. Planner cache is
// Cypher-specific. The cross-cutting cache bag (catalog_cache.go) carries a
// separate `cache_hits_total{cache="query_result"}` mirror that the Cypher
// query-result cache emits in parallel — both layers see the metric.
//
// MET-25 hot path: subsystems pre-bind via `BindQueryDuration("read", db)`
// and cache the resulting BoundLatencyObserver in struct fields at
// constructor time (see pkg/cypher/executor.go). The chokepoint Observe()
// call funnels through observeWithExemplar — zero exemplar overhead under
// Phase 1's NeverSample default; lights up automatically at Phase 6's
// sampler flip with no second-pass migration.
//
// MET-26 / D-15b slow_query_threshold_seconds: registered directly on reg
// via prometheus.NewGaugeFunc(slowQueryThresholdFn) — NO struct field. The
// callback wraps a defer-recover that returns 0 on panic per RESEARCH RISK-8 /
// Pitfall 1. The value reflects cfg.Logging.SlowQueryThreshold (or whatever
// the injected callback reads) on every scrape — config reload is automatic;
// no event wiring required.
//
// D-08 forward-compat: NewCypherMetrics(reg, tenantLabelsEnabled bool,
// slowQueryThresholdFn func() float64) decides label-set shape ONCE at
// construction. When false (Phase 4 default outside K8s), the `database`
// label is OMITTED from the labelnames slice — not set to empty string.
// Phase 5's K8s autodetect flips the bool's default value with no
// re-registration.
package observability

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
)

// AllowedCypherOpTypes is the closed enum for the `op_type` Cypher label per
// CONTEXT D-04 (corrected per RISK-1). Mirrors the QueryAnalyzer-derived
// classification + admin-dispatch + fabric-routing + parse-error sites.
//
// Adding a new op_type = enum update + ADR §2.3 amendment + new test case in
// pkg/cypher/op_type_test.go::TestOpType_AllClauseShapes (D-04c table-driven).
var AllowedCypherOpTypes = []string{
	"read",        // QueryInfo.IsReadOnly
	"write",       // QueryInfo.IsWriteQuery
	"schema",      // QueryInfo.IsSchemaQuery
	"admin",       // admin-dispatch path (SHOW DATABASES, CREATE/DROP DATABASE) — Site 1
	"fabric",      // USE-clause fabric routing
	"parse_error", // validateSyntax() err path — Site 2; D-04b sixth enum value
}

// CypherMetrics is the typed handle-bag (CONTEXT D-02 / D-02a) for the Cypher
// subsystem. One bag per Provider; constructed at cmd/nornicdb startup
// between Phase 1's "registries" and "listeners" init steps (D-02c).
//
// Hot-path discipline (MET-25): the Cypher executor (pkg/cypher/executor.go)
// is the SOLE call site; it threads (op_type[, database]) through the
// `BindQueryDuration` / `BindQueries` helpers which return tenant-flag-
// agnostic Bound observers. StorageExecutor caches BoundLatencyObserver
// in struct fields for the high-frequency op_types (read, write).
//
// Dual-access pattern (D-02): the bag exposes the raw *prometheus.CounterVec
// and Phase-3 typed *LatencyHistogram / *RowCountHistogram so subsystem
// tests can drive AssertCardinalityCeiling and edge cases.
//
// MET-26 slow_query_threshold_seconds is NOT a struct field — it's a
// GaugeFunc registered directly on reg. The callback (slowQueryThresholdFn)
// reads cfg.Logging.SlowQueryThreshold().Seconds() on every scrape so config
// reload flows through automatically (D-15b). RISK-8 / Pitfall 1: callback
// wraps a defer-recover that returns 0 on panic.
type CypherMetrics struct {
	// Queries is the per-op_type query-count counter; same label-set as
	// QueryDuration so the `_total` and `_seconds_count` series have
	// identical cardinality.
	Queries *prometheus.CounterVec

	// QueryDuration is a latency histogram with Phase-3-locked buckets
	// (LatencyBucketsSeconds: ~100us to 10s). Labels: {op_type[, database]}.
	QueryDuration *LatencyHistogram

	// PlannerDuration is the latency histogram for the planning step
	// (parse + plan + cache lookup); same buckets as QueryDuration.
	// Labels: {op_type} only — planner cost is the same regardless of
	// tenant. Tenant-flag-independent surface.
	PlannerDuration *LatencyHistogram

	// PlannerCacheHits / PlannerCacheMisses are the planner-specific
	// cache counters per D-12a (Cypher subsystem owns these — NOT the
	// cross-cutting cache subsystem).
	PlannerCacheHits   *prometheus.CounterVec
	PlannerCacheMisses *prometheus.CounterVec

	// PlannerCacheSize is the gauge tracking current planner cache
	// occupancy. Set by pkg/cypher/cache.go on Get/Put/Evict.
	PlannerCacheSize prometheus.Gauge

	// RowsReturned is the result-set-size histogram with Phase-3-locked
	// buckets (RowCountBuckets: 1 to 1M). Labels: {op_type}.
	RowsReturned *RowCountHistogram

	// ActiveTransactions is the gauge of currently-open Cypher
	// transactions. Inc'd at transaction Begin; Dec'd at Commit/Rollback.
	ActiveTransactions prometheus.Gauge

	// TransactionConflicts counts ErrConflict events surfaced from the
	// storage layer (D-16: storage detects, Cypher counts — preserves
	// AGENTS.md §8 separation). Labels: {[database]}.
	TransactionConflicts *prometheus.CounterVec

	// SlowQueries counts queries that exceeded the slow-query threshold
	// (matches the Phase 2 D-04c slow-query log emission gate).
	// Labels: {[database]}.
	SlowQueries *prometheus.CounterVec

	// tenantLabelsEnabled is captured at construction so Bind helpers can
	// drop the database arg when the bag was registered without it (D-08).
	// Subsystems are bool-agnostic; only this struct knows the shape.
	tenantLabelsEnabled bool
}

// NewCypherMetrics constructs the Cypher bag against reg.
//
// tenantLabelsEnabled is the D-08 forward-compat hook. When true, the
// `database` label is included in Queries, QueryDuration, TransactionConflicts,
// SlowQueries; when false, it is omitted. Phase 5's K8s autodetect (MET-22)
// decides the value.
//
// slowQueryThresholdFn is the D-15b live-read callback for the
// slow_query_threshold_seconds GaugeFunc — typically reads
// cfg.Logging.SlowQueryThreshold().Seconds(). The callback is wrapped in a
// defer-recover that returns 0 on panic (RESEARCH RISK-8 / Pitfall 1) so a
// buggy callback cannot poison the entire /metrics scrape.
//
// Validation + Pitfall-8 panic semantics inherited from Phase 3 typed
// constructors: missing _total/_seconds/_rows suffixes or forbidden
// labels (e.g. accidentally passing "query" as a label) panic at
// registration.
func NewCypherMetrics(reg *prometheus.Registry, tenantLabelsEnabled bool, slowQueryThresholdFn func() float64) *CypherMetrics {
	queryLabels := []string{"op_type"}
	if tenantLabelsEnabled {
		queryLabels = append(queryLabels, "database")
	}

	tenantOnlyLabels := []string{}
	if tenantLabelsEnabled {
		tenantOnlyLabels = append(tenantOnlyLabels, "database")
	}

	cm := &CypherMetrics{
		tenantLabelsEnabled: tenantLabelsEnabled,
	}

	cm.Queries = NewCounterVec(reg,
		MetricOpts{
			Subsystem: "cypher",
			Name:      "queries_total",
			Help: "Cypher queries by op_type (closed enum: read, write, schema, " +
				"admin, fabric, parse_error). database label included when " +
				"tenant-labels-enabled per D-08.",
		},
		queryLabels)

	cm.QueryDuration = NewLatencyHistogram(reg,
		MetricOpts{
			Subsystem: "cypher",
			Name:      "query_duration_seconds",
			Help: "Cypher query latency seconds. Same label-set as queries_total " +
				"so _total and _seconds_count series have identical cardinality.",
		},
		queryLabels)

	cm.PlannerDuration = NewLatencyHistogram(reg,
		MetricOpts{
			Subsystem: "cypher",
			Name:      "planner_duration_seconds",
			Help: "Cypher planning step latency (parse + plan + cache lookup). " +
				"Tenant-independent — planner cost is the same per op_type regardless " +
				"of database.",
		},
		[]string{"op_type"})

	cm.PlannerCacheHits = NewCounterVec(reg,
		MetricOpts{
			Subsystem: "cypher",
			Name:      "planner_cache_hits_total",
			Help: "Cypher planner cache hits. D-12a: planner cache lives under " +
				"the cypher subsystem, not the cross-cutting cache subsystem.",
		},
		[]string{})

	cm.PlannerCacheMisses = NewCounterVec(reg,
		MetricOpts{
			Subsystem: "cypher",
			Name:      "planner_cache_misses_total",
			Help: "Cypher planner cache misses. D-12a: planner cache lives under " +
				"the cypher subsystem, not the cross-cutting cache subsystem.",
		},
		[]string{})

	cm.PlannerCacheSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "nornicdb",
		Subsystem: "cypher",
		Name:      "planner_cache_size",
		Help:      "Current Cypher planner cache occupancy (entries). D-12a.",
	})
	reg.MustRegister(cm.PlannerCacheSize)

	cm.RowsReturned = NewRowCountHistogram(reg,
		MetricOpts{
			Subsystem: "cypher",
			Name:      "rows_returned_rows",
			Help: "Cypher result-set size in rows. Long-tail buckets per " +
				"RowCountBuckets (1 to 1M).",
		},
		[]string{"op_type"})

	cm.ActiveTransactions = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "nornicdb",
		Subsystem: "cypher",
		Name:      "active_transactions",
		Help: "Currently-open Cypher transactions. Inc on Begin; Dec on " +
			"Commit/Rollback (deferred pair).",
	})
	reg.MustRegister(cm.ActiveTransactions)

	cm.TransactionConflicts = NewCounterVec(reg,
		MetricOpts{
			Subsystem: "cypher",
			Name:      "transaction_conflicts_total",
			Help: "MVCC conflicts surfaced from storage at the Cypher transaction " +
				"wrapper (D-16: storage detects, Cypher counts; storage layer " +
				"never imports observability — preserves AGENTS.md §8 separation).",
		},
		tenantOnlyLabels)

	cm.SlowQueries = NewCounterVec(reg,
		MetricOpts{
			Subsystem: "cypher",
			Name:      "slow_queries_total",
			Help: "Cypher queries that exceeded slow_query_threshold_seconds. " +
				"Matches the Phase 2 D-04c slow-query log emission gate.",
		},
		tenantOnlyLabels)

	// MET-26 / D-15b: slow_query_threshold_seconds via GaugeFunc — reads the
	// callback on every scrape so config reload reflects automatically.
	// RISK-8 / Pitfall 1 mitigation: defer-recover returns 0 on panic so a
	// buggy callback cannot poison the entire scrape.
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nornicdb",
		Subsystem: "cypher",
		Name:      "slow_query_threshold_seconds",
		Help: "Configured slow-query threshold (seconds). GaugeFunc — reflects " +
			"cfg.Logging.SlowQueryThreshold() on every scrape; no event wiring " +
			"required for config reload (D-15b). Returns 0 on callback panic " +
			"(RISK-8 mitigation).",
	}, func() (val float64) {
		defer func() {
			if r := recover(); r != nil {
				val = 0
			}
		}()
		if slowQueryThresholdFn == nil {
			return 0
		}
		return slowQueryThresholdFn()
	}))

	return cm
}

// TenantLabelsEnabled reports whether this bag was constructed with the
// D-08 tenant-flag enabled. Read-only after construction.
func (c *CypherMetrics) TenantLabelsEnabled() bool { return c.tenantLabelsEnabled }

// BindQueryDuration returns a pre-bound BoundLatencyObserver for the
// (opType, database) tuple. When the bag was constructed with
// tenantLabelsEnabled=false, the database arg is dropped. Subsystems are
// bool-agnostic and pass database unconditionally.
//
// Hot-path discipline (MET-25): subsystem callers SHOULD hoist the Bind
// call out of the request loop (cache the BoundLatencyObserver in a
// struct field at construction). Per-call BindQueryDuration still pays a
// WithLabelValues lookup.
func (c *CypherMetrics) BindQueryDuration(opType, database string) BoundLatencyObserver {
	if c.tenantLabelsEnabled {
		return c.QueryDuration.Bind(opType, database)
	}
	return c.QueryDuration.Bind(opType)
}

// BindQueries returns a pre-bound prometheus.Counter for the
// (opType, database) tuple. Tenant-flag-aware per BindQueryDuration above.
func (c *CypherMetrics) BindQueries(opType, database string) prometheus.Counter {
	if c.tenantLabelsEnabled {
		return c.Queries.WithLabelValues(opType, database)
	}
	return c.Queries.WithLabelValues(opType)
}

// BindTransactionConflicts returns a pre-bound prometheus.Counter for the
// (database) tuple — D-16 wiring site at the Cypher transaction wrapper
// where storage's ErrConflict surfaces. Tenant-flag-aware: drops the
// database arg when the bag was constructed with tenantLabelsEnabled=false.
func (c *CypherMetrics) BindTransactionConflicts(database string) prometheus.Counter {
	if c.tenantLabelsEnabled {
		return c.TransactionConflicts.WithLabelValues(database)
	}
	return c.TransactionConflicts.WithLabelValues()
}

// BindSlowQueries returns a pre-bound prometheus.Counter for the (database)
// tuple — emitted at the Phase 2 D-04c slow-query gate alongside the
// existing slow-query log record. Tenant-flag-aware.
func (c *CypherMetrics) BindSlowQueries(database string) prometheus.Counter {
	if c.tenantLabelsEnabled {
		return c.SlowQueries.WithLabelValues(database)
	}
	return c.SlowQueries.WithLabelValues()
}

// ObserveQueryDuration is a thin convenience wrapper around
// BindQueryDuration().Observe — used by tests and cold paths. Hot-path
// callers should hoist Bind calls out of the request loop.
func (c *CypherMetrics) ObserveQueryDuration(ctx context.Context, opType, database string, sec float64) {
	c.BindQueryDuration(opType, database).Observe(ctx, sec)
}
