// Package observability — Storage metric bag (Plan 04-04 GREEN).
//
// Owns eight families per MET-10 + ADR §2.3:
//
//	nornicdb_storage_nodes_total                              (GaugeFunc; D-15b)
//	nornicdb_storage_edges_total                              (GaugeFunc; D-15b)
//	nornicdb_storage_bytes{kind}
//	nornicdb_storage_op_duration_seconds{op}
//	nornicdb_storage_compactions_total{level, result}
//	nornicdb_storage_compaction_duration_seconds{level}
//	nornicdb_storage_wal_lag_bytes
//	nornicdb_storage_index_rebuild_total{[database,] index, result}
//
// Closed enums (CONTEXT D-07 / D-13c):
//
//	kind   ∈ AllowedStorageBytesKinds = {nodes, edges, index, wal, search}
//	op     ∈ AllowedStorageOps        = {get, put, delete, scan}
//	index  ∈ AllowedStorageIndexes    = {label, edge_between, temporal,
//	                                     embedding, user_created}
//	result ∈ AllowedStorageResults    = {success, failure, aborted}
//
// D-13c the `index` enum is the keystone of T-04-02 mitigation: arbitrary
// user-created index names NEVER become label values. The pure mapping
// function `pkg/storage.classifyIndexName(internal string) string` buckets
// any unknown name to `user_created`. RESEARCH §Q11 cardinality ceiling.
//
// Forbidden-label discipline (Phase 3 D-03 / registration.go ForbiddenLabels):
// `path`, `query`, `user`, `user_id`, `ip`, `uuid`, `embedding_text`,
// `trace_id`, `span_id`, `email` would all be cardinality bombs and panic
// at registration. Storage hot paths never label by raw key bytes or full
// paths.
//
// D-07 bytes{kind} is populated by a 30s lifecycle.Component sweep that
// calls `badger.DB.EstimateSize(prefix)` for each prefix. Sweep lives in
// pkg/storage/bytes_metrics.go (Plan 04-04-04). RESEARCH §Q3.
//
// MET-25 hot path: per-op observation uses pre-bound BoundLatencyObserver
// cached in BadgerEngine struct fields at constructor time. The bag's
// OpDuration.Bind("get") is hoisted out of the request loop. CompactionDuration
// is bound at compaction-event hook attachment.
//
// RISK-6 wal_lag_bytes semantics: best-effort heuristic estimate of WAL
// backlog (vlog size minus LSM size from `badger.DB.Size()`). Alerting
// SHOULD use 5-minute trends, not single scrapes. The metric is documented
// here and in the bytes_metrics.go sweeper that populates it.
//
// D-08 forward-compat: NewStorageMetrics(reg, tenantLabelsEnabled, probe)
// decides whether the `database` label is included in IndexRebuildTotal
// ONCE at construction. The plan deliberately does NOT add database to
// op_duration (D-08a) — per-database op latency lives at the Cypher
// subsystem (Plan 04-03).
package observability

import (
	"github.com/prometheus/client_golang/prometheus"
)

// AllowedStorageBytesKinds is the closed enum for the `kind` label per
// CONTEXT D-07. Adding a new bucket = enum update + ADR §2.3 amendment +
// new sweep entry in pkg/storage/bytes_metrics.go.
var AllowedStorageBytesKinds = []string{"nodes", "edges", "index", "wal", "search"}

// AllowedStorageOps is the closed enum for the `op` label per CONTEXT D-07.
// Mirrors the four storage-engine chokepoints: GetNode/GetEdge (get),
// CreateNode/CreateEdge (put), DeleteNode/DeleteEdge (delete), iterator
// scans (scan). Hot-path observation uses pre-bound observers (MET-25).
var AllowedStorageOps = []string{"get", "put", "delete", "scan"}

// AllowedStorageIndexes is the closed enum for the `index` label per
// CONTEXT D-13c. The `user_created` bucket catches arbitrary user index
// names — the pure classifier function `pkg/storage.classifyIndexName`
// maps unknown names here. Cardinality bounded at 5.
var AllowedStorageIndexes = []string{"label", "edge_between", "temporal", "embedding", "user_created"}

// AllowedStorageResults is the closed enum for the `result` label on
// compactions_total and index_rebuild_total. Note: NOT used on
// op_duration_seconds (per D-16, op_duration omits result; conflict
// classification lives at Cypher subsystem).
var AllowedStorageResults = []string{"success", "failure", "aborted"}

// StorageProbe is the seam between pkg/storage stat accessors and the
// observability nodes_total / edges_total GaugeFunc callbacks (D-02d
// leaf-package boundary — pkg/observability never imports pkg/storage).
// *BadgerEngine's existing NodeCount() / EdgeCount() return (int64, error);
// callers wrap the error-discarding form via a thin adapter at the
// cmd/nornicdb wiring site (see Plan 04-04-07).
//
// IDDictCounterNodes / IDDictCounterEdges expose the monotonic counters
// of allocated numIDs. IDDictFreelistNodes / IDDictFreelistEdges report
// the number of entries currently parked on the debounced freelist,
// awaiting TTL expiry before they can be reclaimed. Together these give
// operators visibility into (counter-freelist) = roughly the live-entity
// count and freelist pending work.
type StorageProbe interface {
	NodeCount() int64
	EdgeCount() int64
	IDDictCounterNodes() uint64
	IDDictCounterEdges() uint64
	IDDictFreelistNodes() int64
	IDDictFreelistEdges() int64
}

// StorageMetrics is the typed handle-bag (CONTEXT D-02 / D-02a) for the
// storage subsystem. One bag per Provider; constructed at cmd/nornicdb
// startup and injected into *BadgerEngine via AttachMetrics(...).
//
// Hot-path discipline (MET-25): the storage struct caches
// BoundLatencyObserver in struct fields at constructor time so per-op
// observation pays zero WithLabelValues lookup overhead.
//
// Dual-access pattern (D-02): the bag exposes the raw *prometheus.GaugeVec
// / *prometheus.CounterVec / *LatencyHistogram so subsystem tests can
// drive AssertCardinalityCeiling and edge cases.
type StorageMetrics struct {
	// Bytes is the storage size gauge per kind. Populated every 30s by
	// the bytes_metrics_sweeper lifecycle.Component (D-07).
	Bytes *prometheus.GaugeVec

	// OpDuration is the per-op latency histogram. Hot-path observation
	// uses pre-bound observers cached in BadgerEngine struct fields
	// (MET-25). Phase-3-locked LatencyBucketsSeconds.
	OpDuration *LatencyHistogram

	// CompactionsTotal counts BadgerDB compactions by level and result.
	// Cardinality ceiling = ~14 (7 levels × 2 results). Bounded by
	// BadgerDB's own compaction-level surface — no user input flows here.
	CompactionsTotal *prometheus.CounterVec

	// CompactionDuration histograms compaction wall-clock time per level.
	CompactionDuration *LatencyHistogram

	// WALLagBytes is a heuristic estimate of WAL backlog (vlog size
	// minus LSM size from badger.DB.Size()). Best-effort; alerting
	// should use 5-minute trends, not single scrapes. RESEARCH §Q3 /
	// RISK-6.
	WALLagBytes prometheus.Gauge

	// IndexRebuildTotal counts index-rebuild events by [database], index,
	// result. Closed `index` enum per D-13c. database label gated by
	// tenant flag (D-08).
	IndexRebuildTotal *prometheus.CounterVec

	// tenantLabelsEnabled captured at construction so caller helpers can
	// decide arity uniformly. Subsystems pass database unconditionally
	// through Bind helpers; the bag drops the arg internally when off.
	tenantLabelsEnabled bool
}

// NewStorageMetrics constructs the storage bag against reg.
//
// tenantLabelsEnabled (D-08) decides whether `database` is included in
// IndexRebuildTotal. The other families are tenant-flag-independent:
//   - bytes/op_duration aggregate across databases at the storage layer
//     (per-DB lives at Cypher subsystem, Plan 04-03).
//   - nodes_total/edges_total are process-wide gauges.
//   - compactions/wal are BadgerDB-level — single physical database.
//
// probe is the StorageProbe accessor surface — typically *BadgerEngine.
// nil is tolerated as a defensive fallback (returns 0 from each gauge).
func NewStorageMetrics(reg *prometheus.Registry, tenantLabelsEnabled bool, probe StorageProbe) *StorageMetrics {
	bag := &StorageMetrics{
		tenantLabelsEnabled: tenantLabelsEnabled,
	}

	// MET-10 / D-15b: nodes_total and edges_total via GaugeFunc reading
	// the StorageProbe accessors at scrape time. defer-recover returns 0
	// on panic (RESEARCH RISK-8 / Pitfall 1).
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nornicdb",
		Subsystem: "storage",
		Name:      "nodes_total",
		Help: "Total node count in storage. GaugeFunc — reads " +
			"StorageProbe.NodeCount() on every scrape; returns 0 on " +
			"probe panic (RISK-8 mitigation).",
	}, func() (val float64) {
		defer func() {
			if r := recover(); r != nil {
				val = 0
			}
		}()
		if probe == nil {
			return 0
		}
		return float64(probe.NodeCount())
	}))

	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nornicdb",
		Subsystem: "storage",
		Name:      "edges_total",
		Help: "Total edge count in storage. GaugeFunc — reads " +
			"StorageProbe.EdgeCount() on every scrape; returns 0 on " +
			"probe panic (RISK-8 mitigation).",
	}, func() (val float64) {
		defer func() {
			if r := recover(); r != nil {
				val = 0
			}
		}()
		if probe == nil {
			return 0
		}
		return float64(probe.EdgeCount())
	}))

	// ID dictionary counter gauges: monotonic per-kind counter of numID
	// allocations. Together with the freelist gauge, operators can see
	// (counter - freelist_pending) ≈ live-entity count, and watch the
	// freelist drain as TTL expires.
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nornicdb",
		Subsystem: "storage",
		Name:      "id_dict_counter_nodes",
		Help: "Monotonic counter of node numIDs ever allocated. Does not " +
			"decrease — recycled numIDs don't change this number; the " +
			"freelist gauge tracks reusable parked IDs.",
	}, func() (val float64) {
		defer func() {
			if r := recover(); r != nil {
				val = 0
			}
		}()
		if probe == nil {
			return 0
		}
		return float64(probe.IDDictCounterNodes())
	}))
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nornicdb",
		Subsystem: "storage",
		Name:      "id_dict_counter_edges",
		Help:      "Monotonic counter of edge numIDs ever allocated.",
	}, func() (val float64) {
		defer func() {
			if r := recover(); r != nil {
				val = 0
			}
		}()
		if probe == nil {
			return 0
		}
		return float64(probe.IDDictCounterEdges())
	}))

	// Freelist pending gauges: count of deleted numIDs parked on the
	// debounced freelist, awaiting TTL expiry before allocation can
	// recycle them. Healthy-steady-state patterns: zero during pure
	// growth; non-zero pulses during churn that drain back to zero as
	// the TTL elapses.
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nornicdb",
		Subsystem: "storage",
		Name:      "id_dict_freelist_nodes",
		Help: "Node numIDs parked on the debounced freelist awaiting " +
			"TTL expiry. When it drops to zero, allocation falls back " +
			"to bumping the counter.",
	}, func() (val float64) {
		defer func() {
			if r := recover(); r != nil {
				val = 0
			}
		}()
		if probe == nil {
			return 0
		}
		return float64(probe.IDDictFreelistNodes())
	}))
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nornicdb",
		Subsystem: "storage",
		Name:      "id_dict_freelist_edges",
		Help:      "Edge numIDs parked on the debounced freelist.",
	}, func() (val float64) {
		defer func() {
			if r := recover(); r != nil {
				val = 0
			}
		}()
		if probe == nil {
			return 0
		}
		return float64(probe.IDDictFreelistEdges())
	}))

	bag.Bytes = NewGaugeVec(reg,
		MetricOpts{
			Subsystem: "storage",
			Name:      "bytes",
			Help: "Storage size in bytes by kind (closed enum: nodes, edges, " +
				"index, wal, search). Populated every 30s by the " +
				"bytes_metrics_sweeper lifecycle.Component (D-07).",
		},
		[]string{"kind"})

	bag.OpDuration = NewLatencyHistogram(reg,
		MetricOpts{
			Subsystem: "storage",
			Name:      "op_duration_seconds",
			Help: "Storage operation latency by op (closed enum: get, put, " +
				"delete, scan). Hot-path: pre-bound observers cached in " +
				"BadgerEngine struct fields (MET-25).",
		},
		[]string{"op"})

	bag.CompactionsTotal = NewCounterVec(reg,
		MetricOpts{
			Subsystem: "storage",
			Name:      "compactions_total",
			Help: "BadgerDB compactions by level and result. Bounded by " +
				"BadgerDB's compaction-level surface — no user input flows " +
				"to label values.",
		},
		[]string{"level", "result"})

	bag.CompactionDuration = NewLatencyHistogram(reg,
		MetricOpts{
			Subsystem: "storage",
			Name:      "compaction_duration_seconds",
			Help:      "BadgerDB compaction wall-clock time by level.",
		},
		[]string{"level"})

	bag.WALLagBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "nornicdb",
		Subsystem: "storage",
		Name:      "wal_lag_bytes",
		Help: "Heuristic estimate of WAL backlog (vlog size minus LSM " +
			"size from badger.DB.Size()). Best-effort; alerting should " +
			"use 5-minute trends, not single scrapes (RISK-6).",
	})
	reg.MustRegister(bag.WALLagBytes)

	indexLabels := []string{"index", "result"}
	if tenantLabelsEnabled {
		indexLabels = []string{"database", "index", "result"}
	}
	bag.IndexRebuildTotal = NewCounterVec(reg,
		MetricOpts{
			Subsystem: "storage",
			Name:      "index_rebuild_total",
			Help: "Index-rebuild events by [database], index (closed enum: " +
				"label, edge_between, temporal, embedding, user_created), " +
				"and result (success, failure, aborted). D-13c: arbitrary " +
				"user index names bucket to user_created — closed-enum " +
				"discipline prevents cardinality bombs.",
		},
		indexLabels)

	return bag
}

// TenantLabelsEnabled reports whether this bag was constructed with the
// D-08 tenant-flag enabled.
func (s *StorageMetrics) TenantLabelsEnabled() bool { return s.tenantLabelsEnabled }

// BindIndexRebuild returns a pre-bound prometheus.Counter for the
// (database, index, result) tuple. Tenant-flag-aware — drops database
// when the bag was constructed with tenantLabelsEnabled=false.
func (s *StorageMetrics) BindIndexRebuild(database, index, result string) prometheus.Counter {
	if s.tenantLabelsEnabled {
		return s.IndexRebuildTotal.WithLabelValues(database, index, result)
	}
	return s.IndexRebuildTotal.WithLabelValues(index, result)
}
