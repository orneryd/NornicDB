// Package search — Plan 04-05-05 observability wiring.
//
// AttachMetrics injects an *observability.SearchMetrics bag into the
// Service. observeSearch is the single chokepoint helper called from
// Search() / rrfHybridSearch() at the per-stage boundaries to record
// requests_total + duration_seconds{stage} + candidates_rows
// observations.
//
// The chokepoint design preserves AGENTS.md §7 DRY — every search code
// path (vector-only, BM25-only, hybrid, fallbacks) flows through Search()
// and the per-stage timings are picked off the same SearchMetrics bag. No
// per-mode-method duplication.
//
// Database label: today the Service does not carry a database name; the
// observation helper flows database="" and the bag's
// tenantLabelsEnabled-aware Bind helpers drop the empty arg when the bag
// was constructed with tenantLabelsEnabled=false (Plan 04-05-06 wires
// tenantLabelsEnabled from cfg.Observability.Metrics.TenantLabelsEnabled).
// Multi-database observation is a future enhancement; M1 ships single-DB
// observation per CONTEXT MET-21 omission discussion.
//
// Stage mapping per CONTEXT D-15b / MET-13 closed enum
// AllowedSearchStages = {embed, index, fuse}:
//   - index: vector + BM25 lookup (combined; the two indexes execute in
//     parallel-ish at observation cadence — capacity-planning wants the
//     joint stage budget, not per-index micro-detail)
//   - fuse:  RRF + MMR + rerank
//   - embed: NOT observed here (the embedder call lives in pkg/embed and
//     is already covered by nornicdb_embed_duration_seconds — Plan
//     04-05-02). The search stage enum reserves "embed" for completeness;
//     a future refactor that pushes the embed call into Search would
//     observe it under search_duration_seconds{stage="embed"} too.
//
// MET-25 hot path discipline: per-stage observation pays one
// WithLabelValues lookup per Search call (NOT per request — Search is
// called once per logical user request). The bag's BindDuration is the
// pre-bound entry point; for tenant-OFF (single global database) the
// BoundLatencyObserver is cached at AttachMetrics time so the per-stage
// observation pays zero per-call lookup.
package search

import (
	"context"
	"time"

	"github.com/orneryd/nornicdb/pkg/observability"
)

// AttachMetrics injects the observability.SearchMetrics bag into the
// Service for per-stage observation (Plan 04-05-05). Pre-binds the
// stage-specific observers under tenant-OFF so the request hot path
// pays zero WithLabelValues overhead per stage.
//
// nil is tolerated: the observation helpers no-op when metrics is nil
// (defensive for embedded-library callers + tests that don't wire
// observability).
//
// Idempotent: subsequent calls overwrite the previous bag and re-bind
// observers.
func (s *Service) AttachMetrics(metrics *observability.SearchMetrics) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metrics = metrics
	if metrics == nil {
		s.boundDurationIndex = observability.BoundLatencyObserver{}
		s.boundDurationFuse = observability.BoundLatencyObserver{}
		return
	}
	// Pre-bind under tenant-OFF for the (mode="hybrid", stage=*) tuples
	// most commonly observed. Other (mode, stage) tuples lazy-bind via
	// metrics.BindDuration at the call site — still cheap (one
	// WithLabelValues per Search call, NOT per inner candidate).
	s.boundDurationIndex = metrics.BindDuration("", "hybrid", "index")
	s.boundDurationFuse = metrics.BindDuration("", "hybrid", "fuse")
}

// observeSearchStage records a per-stage duration observation. mode is
// passed unconditionally (the helper picks the pre-bound observer for
// hybrid / falls back to BindDuration for other modes). database is
// flowed through but dropped when tenantLabelsEnabled=false.
//
// Returns a no-op closure when metrics is nil so the call site reads
// uniformly across observability-attached and embedded-library use.
func (s *Service) observeSearchStage(ctx context.Context, mode, stage string, dur time.Duration) {
	if s.metrics == nil {
		return
	}
	// Fast path for the hybrid hot-path — pre-bound observers cached at
	// AttachMetrics time. Non-hybrid modes pay one BindDuration per
	// observation but are far less frequent.
	if mode == "hybrid" {
		switch stage {
		case "index":
			s.boundDurationIndex.Observe(ctx, dur.Seconds())
			return
		case "fuse":
			s.boundDurationFuse.Observe(ctx, dur.Seconds())
			return
		}
	}
	s.metrics.BindDuration("", mode, stage).Observe(ctx, dur.Seconds())
}

// observeSearchRequest records the requests_total + candidates_rows
// observations at the end of a Search call. mode is the path taken
// (vector / bm25 / hybrid); result is the closed enum
// {success, no_results, error}. candidates is the count BEFORE the fuse
// stage trims to SearchOptions.Limit (so dashboards see the natural
// distribution, not the user-facing limit).
func (s *Service) observeSearchRequest(mode, result string, candidates int) {
	if s.metrics == nil {
		return
	}
	s.metrics.IncRequest("", mode, result)
	if candidates >= 0 {
		s.metrics.Candidates.Vec().WithLabelValues().Observe(float64(candidates))
	}
}

// classifySearchResult maps a (response, error) pair to the closed
// AllowedSearchResults enum {success, no_results, error}. error wins; an
// empty result set is "no_results" (success with zero hits).
func classifySearchResult(resp *SearchResponse, err error) string {
	if err != nil {
		return "error"
	}
	if resp == nil || len(resp.Results) == 0 {
		return "no_results"
	}
	return "success"
}

// IndexSizeBytes satisfies observability.SearchProbe (Plan 04-05-04). It
// returns a best-effort byte estimate of the named index kind:
//   - hnsw: HNSWIndex.Size() (vector count) × vector dimensions ×
//     sizeof(float32). Captures the dominant memory cost of HNSW; node
//     graph metadata is comparatively small.
//   - bm25: 0 (no native size accessor on the BM25 index today; deferred
//     to a future enhancement that exposes fulltextIndex.SizeBytes()).
//
// kind values outside AllowedSearchIndexKinds return 0 — defense in
// depth even though the closed enum at the call site means this branch
// is unreachable from the GaugeFunc collector.
func (s *Service) IndexSizeBytes(kind string) uint64 {
	if s == nil {
		return 0
	}
	switch kind {
	case "hnsw":
		s.hnswMu.RLock()
		defer s.hnswMu.RUnlock()
		if s.hnswIndex == nil {
			return 0
		}
		// 4 bytes per float32; assume DefaultVectorDimensions when the
		// index has not yet pinned a dimension.
		dims := DefaultVectorDimensions
		return uint64(s.hnswIndex.Size()) * uint64(dims) * 4
	case "bm25":
		return 0 // BM25 index size accessor TBD; deferred future work.
	default:
		return 0
	}
}
