// Package observability — Search metric bag (Plan 04-05 GREEN).
//
// Owns four families per MET-13 + ADR §2.3:
//
//	nornicdb_search_requests_total{[database,] mode, result}
//	nornicdb_search_duration_seconds{[database,] mode, stage}
//	nornicdb_search_candidates                          (RowCountHistogram)
//	nornicdb_search_index_size_bytes{kind}              (GaugeFunc; D-15b)
//
// Closed enums (CONTEXT MET-13 / D-15b):
//
//	mode   ∈ AllowedSearchModes        = {vector, bm25, hybrid}
//	result ∈ AllowedSearchResults      = {success, no_results, error}
//	stage  ∈ AllowedSearchStages       = {embed, index, fuse}
//	kind   ∈ AllowedSearchIndexKinds   = {hnsw, bm25}
//
// stage is the keystone: per-stage observation lets SREs see whether
// embed (LLM call), index (vector / BM25 lookup), or fuse (RRF + rerank)
// dominates the latency budget. Closed enum at internal call sites
// prevents arbitrary stage-name labeling.
//
// Forbidden-label discipline (Phase 3 D-03 / registration.go ForbiddenLabels):
// `query` is the keystone — raw query text MUST NEVER be a label value.
// The registration.go panic catches anyone passing `query` as a label.
//
// MET-25 hot path: per-stage observation uses pre-bound observers cached
// at the call site. The bag's BindDuration helper returns a
// BoundLatencyObserver that callers store in struct fields per MET-25.
//
// D-15b GaugeFunc panic safety: the index_size_bytes callback per kind
// wraps defer-recover that returns 0 on probe panic — same pattern as
// Plan 04-04 storage.bytes / mvcc.pinned_bytes.
//
// D-02d leaf-package boundary: pkg/observability never imports pkg/search.
// SearchProbe is declared HERE; pkg/search.Service satisfies it via thin
// adapter or a setter that AttachesMetrics.
//
// D-08 forward-compat: NewSearchMetrics(reg, tenantLabelsEnabled, probe)
// decides whether the `database` label is included in requests_total +
// duration_seconds ONCE at construction. candidates and index_size_bytes
// are global — bytes per index kind, candidates per request distribution.
package observability

import (
	"github.com/prometheus/client_golang/prometheus"
)

// AllowedSearchModes is the closed enum for the `mode` label per CONTEXT
// MET-13. Mirrors the three search code paths in pkg/search/search.go:
// vectorSearchOnly (vector), fullTextSearchOnly (bm25), rrfHybridSearch
// (hybrid). Adding a mode = enum update + ADR §2.3 amendment.
var AllowedSearchModes = []string{"vector", "bm25", "hybrid"}

// AllowedSearchResults is the closed enum for the `result` label on
// requests_total. no_results is distinct from error — an empty result
// set is a successful search; error reflects a pipeline failure.
var AllowedSearchResults = []string{"success", "no_results", "error"}

// AllowedSearchStages is the closed enum for the `stage` label on
// duration_seconds per CONTEXT MET-13. Three pipeline stages:
//   - embed: text → vector (the embedder call)
//   - index: vector / BM25 lookup
//   - fuse:  RRF + rerank + MMR
//
// Closed at the call site (string literal at each observation point).
var AllowedSearchStages = []string{"embed", "index", "fuse"}

// AllowedSearchIndexKinds is the closed enum for the `kind` label on
// index_size_bytes per CONTEXT MET-13 / D-15b. Two index kinds:
//   - hnsw: vector index (HNSW or IVF-HNSW or IVFPQ — all vector indexes
//           bucket here from a capacity-planning perspective)
//   - bm25: full-text index
var AllowedSearchIndexKinds = []string{"hnsw", "bm25"}

// SearchProbe is the seam between pkg/search index-size accessors and the
// observability index_size_bytes GaugeFunc callback (D-02d leaf-package
// boundary — pkg/observability never imports pkg/search). pkg/search
// (or a thin adapter) satisfies this interface.
//
// IndexSizeBytes returns the on-disk or in-memory size of the named index
// kind. Implementations should sum across all per-database indexes when a
// process hosts multiple databases. Defensive callers may return 0 if the
// size is not yet computable (e.g., index still building).
type SearchProbe interface {
	IndexSizeBytes(kind string) uint64
}

// SearchMetrics is the typed handle-bag (CONTEXT D-02 / D-02a) for the
// search subsystem. One bag per Provider; constructed at cmd/nornicdb
// startup and injected into pkg/search.Service via AttachMetrics.
//
// Hot-path discipline (MET-25): subsystem callers cache
// BoundLatencyObserver in struct fields per (database, mode, stage)
// tuple at constructor time. The Duration.Bind helper is the entry
// point.
//
// Dual-access pattern (D-02): the bag exposes raw *prometheus.CounterVec
// / *RowCountHistogram / *prometheus.GaugeVec so subsystem tests can
// drive AssertCardinalityCeiling and edge cases.
type SearchMetrics struct {
	// Requests counts search requests by [database], mode, result.
	// Cardinality ceiling per RESEARCH §Q11 — 9 tenant-OFF (3 modes ×
	// 3 results); ceiling × max-databases when tenant-ON.
	Requests *prometheus.CounterVec

	// Duration is the per-stage latency histogram. Hot-path: pre-bound
	// via BindDuration([database], mode, stage) cached in
	// pkg/search.Service struct fields per MET-25. Phase-3-locked
	// LatencyBucketsSeconds (≤10s tail; embed stage's long-tail belongs
	// to nornicdb_embed_duration_seconds).
	Duration *LatencyHistogram

	// Candidates is the per-request candidates-count distribution
	// (count of results considered before the fuse stage trims to
	// SearchOptions.Limit). RowCountBuckets up to 1M.
	Candidates *RowCountHistogram

	// IndexSizeBytes is the per-kind size gauge. Populated by the
	// SearchProbe.IndexSizeBytes(kind) callback on every scrape. Closed
	// kind enum {hnsw, bm25}.
	IndexSizeBytes *prometheus.GaugeVec

	// tenantLabelsEnabled captured at construction so caller helpers can
	// decide arity uniformly. Subsystems pass database unconditionally
	// through BindDuration / RequestInc helpers; the bag drops the arg
	// internally when off.
	tenantLabelsEnabled bool
}

// NewSearchMetrics constructs the search bag against reg.
//
// tenantLabelsEnabled (D-08) decides whether `database` is included in
// requests_total + duration_seconds. candidates is unlabeled (per-request
// distribution) and index_size_bytes uses only the kind label
// (process-wide bytes per kind).
//
// probe is the SearchProbe accessor surface — typically a thin adapter
// over pkg/search.Service. nil is tolerated as a defensive fallback
// (returns 0 from each kind gauge); the GaugeFunc callbacks wrap
// defer-recover returning 0 on panic per RISK-8.
func NewSearchMetrics(reg *prometheus.Registry, tenantLabelsEnabled bool, probe SearchProbe) *SearchMetrics {
	bag := &SearchMetrics{
		tenantLabelsEnabled: tenantLabelsEnabled,
	}

	requestLabels := []string{"mode", "result"}
	durationLabels := []string{"mode", "stage"}
	if tenantLabelsEnabled {
		requestLabels = []string{"database", "mode", "result"}
		durationLabels = []string{"database", "mode", "stage"}
	}

	bag.Requests = NewCounterVec(reg,
		MetricOpts{
			Subsystem: "search",
			Name:      "requests_total",
			Help: "Search requests by [database], mode (closed enum: " +
				"vector, bm25, hybrid), and result (closed enum: success, " +
				"no_results, error). database label gated by tenant flag " +
				"(D-08).",
		},
		requestLabels)

	bag.Duration = NewLatencyHistogram(reg,
		MetricOpts{
			Subsystem: "search",
			Name:      "duration_seconds",
			Help: "Per-stage search latency by [database], mode, stage. " +
				"stage is a closed enum {embed, index, fuse} representing " +
				"the three pipeline phases. Hot-path: pre-bound observers " +
				"cached in pkg/search.Service struct fields per MET-25. " +
				"database label gated by tenant flag (D-08).",
		},
		durationLabels)

	bag.Candidates = NewRowCountHistogram(reg,
		MetricOpts{
			Subsystem: "search",
			Name:      "candidates_rows",
			Help: "Distribution of candidate counts per search request " +
				"(vector + BM25 candidates considered before the fuse " +
				"stage trims to SearchOptions.Limit). RowCountBuckets " +
				"(1 to 1M).",
		},
		nil)

	// IndexSizeBytes via prometheus.NewGaugeVec — but populated by a
	// per-kind GaugeFunc that reads the SearchProbe at scrape time. The
	// GaugeVec is the surface for AssertCardinalityCeiling tests and for
	// hot-path Set() updates from cache-warmth callbacks; the GaugeFuncs
	// are the live-read path per D-15b.
	bag.IndexSizeBytes = NewGaugeVec(reg,
		MetricOpts{
			Subsystem: "search",
			Name:      "index_size_bytes",
			Help: "Search index size in bytes by kind (closed enum: hnsw, " +
				"bm25). Populated by Set() at index-build/load events; " +
				"GaugeFunc fallback reads SearchProbe.IndexSizeBytes(kind) " +
				"on every scrape (D-15b; defer-recover returns 0 on panic " +
				"per RISK-8 mitigation).",
		},
		[]string{"kind"})

	// Per-kind live-read collector for index sizes (D-15b). One custom
	// Collector emits the AllowedSearchIndexKinds series at scrape time —
	// `prometheus.NewGaugeFunc` cannot host multiple ConstLabel variants
	// of the same fqName, so a tiny custom Collector is the idiomatic
	// pattern. The Collect callback wraps defer-recover per kind so a
	// probe panic for one kind does not poison the scrape for the other
	// kind (RISK-8 mitigation).
	reg.MustRegister(&searchIndexSizeCollector{
		probe: probe,
		desc: prometheus.NewDesc(
			"nornicdb_search_index_size_bytes_live",
			"Live-read of search index size in bytes by kind via SearchProbe "+
				"(D-15b GaugeFunc-equivalent custom collector). "+
				"defer-recover returns 0 on probe panic per RISK-8.",
			[]string{"kind"}, nil,
		),
	})

	return bag
}

// searchIndexSizeCollector implements prometheus.Collector to emit the
// per-kind index size series at scrape time. Pattern mirrors the standard
// GaugeFunc pattern but supports multiple variable-label values for the
// same fqName.
type searchIndexSizeCollector struct {
	probe SearchProbe
	desc  *prometheus.Desc
}

func (c *searchIndexSizeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c *searchIndexSizeCollector) Collect(ch chan<- prometheus.Metric) {
	for _, kind := range AllowedSearchIndexKinds {
		val := c.readKindSafe(kind)
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, val, kind)
	}
}

// readKindSafe returns 0 on probe panic per RISK-8 (Pitfall 1) — same
// defer-recover pattern as Plan 04-04 MVCC GaugeFuncs.
func (c *searchIndexSizeCollector) readKindSafe(kind string) (val float64) {
	defer func() {
		if r := recover(); r != nil {
			val = 0
		}
	}()
	if c.probe == nil {
		return 0
	}
	return float64(c.probe.IndexSizeBytes(kind))
}

// TenantLabelsEnabled reports whether this bag was constructed with the
// D-08 tenant-flag enabled.
func (s *SearchMetrics) TenantLabelsEnabled() bool { return s.tenantLabelsEnabled }

// BindDuration returns a pre-bound BoundLatencyObserver for the
// (database, mode, stage) tuple per MET-25. Tenant-flag-aware — drops
// database when the bag was constructed with tenantLabelsEnabled=false.
//
// Subsystem callers cache this in struct fields at construction time;
// the per-request observation pays zero WithLabelValues lookup overhead.
func (s *SearchMetrics) BindDuration(database, mode, stage string) BoundLatencyObserver {
	if s.tenantLabelsEnabled {
		return s.Duration.Bind(database, mode, stage)
	}
	return s.Duration.Bind(mode, stage)
}

// IncRequest is the tenant-flag-aware helper to bump requests_total.
// Drops database when the bag was constructed with tenantLabelsEnabled=false.
func (s *SearchMetrics) IncRequest(database, mode, result string) {
	if s.tenantLabelsEnabled {
		s.Requests.WithLabelValues(database, mode, result).Inc()
		return
	}
	s.Requests.WithLabelValues(mode, result).Inc()
}
