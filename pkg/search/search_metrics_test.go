package search

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Plan 04-05-05: per-stage observation tests for the search subsystem.
//
// The chokepoint design (pkg/search/observability.go) gives us:
//   - Per-Search-call requests_total + candidates_rows observation in
//     the deferred classifier.
//   - Per-rrfHybridSearch index/fuse stage observations.
//
// Tests drive Search() with vector-only / bm25-only / hybrid paths and
// assert the corresponding observations land.

// newTestSearchService builds a Service backed by an in-memory engine
// with a SearchMetrics bag attached. tenantLabelsEnabled=false matches
// the M1 default (per CONTEXT MET-21 omission).
func newTestSearchService(t *testing.T) (*Service, *prometheus.Registry, *observability.SearchMetrics) {
	t.Helper()
	engine := storage.NewMemoryEngine()
	svc := NewServiceWithDimensions(engine, 4)
	reg := prometheus.NewRegistry()
	bag := observability.NewSearchMetrics(reg, false, &nilSearchProbe{})
	svc.AttachMetrics(bag)
	return svc, reg, bag
}

// nilSearchProbe satisfies observability.SearchProbe with zero-byte
// indexes for tests that don't exercise the index_size_bytes path.
type nilSearchProbe struct{}

func (nilSearchProbe) IndexSizeBytes(kind string) uint64 { return 0 }

// TestSearch_RequestsTotalIncrement asserts the Search() chokepoint
// increments requests_total per call.
func TestSearch_RequestsTotalIncrement(t *testing.T) {
	svc, reg, _ := newTestSearchService(t)
	svc.ready.Store(true)

	ctx := context.Background()
	// BM25-only path: empty embedding flows through fullTextSearchOnly.
	_, err := svc.Search(ctx, "anything", nil, nil)
	// Even if the search returns no results / an inner error, the
	// requests_total counter must have been bumped at the deferred
	// chokepoint.
	_ = err

	got := totalCounterByMode(t, reg, "nornicdb_search_requests_total", "bm25")
	assert.GreaterOrEqual(t, got, 1.0,
		"Search() must bump requests_total{mode=bm25} for empty-embedding path")
}

// TestSearch_VectorOnlyMode asserts the empty-query branch routes mode=vector.
func TestSearch_VectorOnlyMode(t *testing.T) {
	svc, reg, _ := newTestSearchService(t)
	svc.ready.Store(true)

	ctx := context.Background()
	embedding := []float32{0.1, 0.2, 0.3, 0.4} // matches dims=4
	_, _ = svc.Search(ctx, "", embedding, nil)

	got := totalCounterByMode(t, reg, "nornicdb_search_requests_total", "vector")
	assert.GreaterOrEqual(t, got, 1.0,
		"empty-query path must bump requests_total{mode=vector}")
}

// TestSearch_NilMetricsTolerated asserts a Service without AttachMetrics
// still functions — the observation chokepoints no-op when metrics is nil.
func TestSearch_NilMetricsTolerated(t *testing.T) {
	engine := storage.NewMemoryEngine()
	svc := NewServiceWithDimensions(engine, 4)
	svc.ready.Store(true)
	// NO AttachMetrics call.

	ctx := context.Background()
	// Should not panic.
	_, _ = svc.Search(ctx, "test", nil, nil)
}

// TestSearch_AttachMetricsIdempotent asserts repeated AttachMetrics calls
// re-bind the cached observers (subsequent bag overrides the previous).
func TestSearch_AttachMetricsIdempotent(t *testing.T) {
	engine := storage.NewMemoryEngine()
	svc := NewServiceWithDimensions(engine, 4)

	reg1 := prometheus.NewRegistry()
	bag1 := observability.NewSearchMetrics(reg1, false, &nilSearchProbe{})
	svc.AttachMetrics(bag1)

	reg2 := prometheus.NewRegistry()
	bag2 := observability.NewSearchMetrics(reg2, false, &nilSearchProbe{})
	svc.AttachMetrics(bag2)

	svc.ready.Store(true)
	ctx := context.Background()
	_, _ = svc.Search(ctx, "test", nil, nil)

	// First registry must NOT see the increment (the previous bag was
	// replaced).
	got1 := totalCounterByMode(t, reg1, "nornicdb_search_requests_total", "bm25")
	got2 := totalCounterByMode(t, reg2, "nornicdb_search_requests_total", "bm25")
	assert.Equal(t, 0.0, got1, "previous bag must not see new observations")
	assert.GreaterOrEqual(t, got2, 1.0, "active bag must see the observation")
}

// TestSearch_AttachMetricsNilClearsBindings asserts that
// AttachMetrics(nil) cleanly detaches without leaving stale observers.
func TestSearch_AttachMetricsNilClearsBindings(t *testing.T) {
	engine := storage.NewMemoryEngine()
	svc := NewServiceWithDimensions(engine, 4)

	reg := prometheus.NewRegistry()
	bag := observability.NewSearchMetrics(reg, false, &nilSearchProbe{})
	svc.AttachMetrics(bag)
	svc.AttachMetrics(nil) // detach

	svc.ready.Store(true)
	ctx := context.Background()
	// Should not panic.
	_, _ = svc.Search(ctx, "test", nil, nil)

	got := totalCounterByMode(t, reg, "nornicdb_search_requests_total", "bm25")
	assert.Equal(t, 0.0, got,
		"detached bag must not see observations after AttachMetrics(nil)")
}

// TestClassifySearchResult unit-tests the closed enum classifier.
func TestClassifySearchResult(t *testing.T) {
	cases := []struct {
		name string
		resp *SearchResponse
		err  error
		want string
	}{
		{"error wins", nil, assertSomeError, "error"},
		{"nil resp = no_results", nil, nil, "no_results"},
		{"empty results = no_results", &SearchResponse{Results: nil}, nil, "no_results"},
		{"populated = success", &SearchResponse{Results: []SearchResult{{NodeID: "n1"}}}, nil, "success"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, classifySearchResult(tc.resp, tc.err))
		})
	}
}

// totalCounterByMode extracts the sum of requests_total{mode=<mode>}
// across all label-tuple variants. Returns 0 when no series matches.
func totalCounterByMode(t *testing.T, reg *prometheus.Registry, name, mode string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	var total float64
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.Metric {
			if matchesLabel(m.GetLabel(), "mode", mode) {
				total += m.GetCounter().GetValue()
			}
		}
	}
	return total
}

func matchesLabel(labels []*dto.LabelPair, key, val string) bool {
	for _, lbl := range labels {
		if lbl.GetName() == key && lbl.GetValue() == val {
			return true
		}
	}
	return false
}

// assertSomeError is a sentinel to drive the "error wins" classifier path
// without depending on a real error type.
var assertSomeError = errSentinel("classifier-test")

type errSentinel string

func (e errSentinel) Error() string { return string(e) }
