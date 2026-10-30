package main

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Plan 04-05-06: TestServerStartup_EmbedSearchMetricsRegistered verifies
// that NewEmbedMetrics + NewSearchMetrics construct cleanly against the
// same registry as the other Phase-4 bags (no AlreadyRegisteredError),
// and that all 11 expected family lines (6 embed + 1 ffi + 4 search)
// surface from a single Gather().
//
// We do NOT boot the full server — direct exercise of the constructor
// chokepoint mirrors the cache_metrics_test.go pattern.
func TestServerStartup_EmbedSearchMetricsRegistered(t *testing.T) {
	te := observability.NewTestEnv(t)

	// Construct the embed bag with a nil probe (queue_depth returns 0).
	var embedBag *observability.EmbedMetrics
	require.NotPanics(t, func() {
		embedBag = observability.NewEmbedMetrics(te.Registry, nil)
	}, "Pitfall 8: embed bag must not collide with Phase-1 runtime collectors")
	require.NotNil(t, embedBag)

	// Construct the search bag (tenant-OFF, nil probe).
	var searchBag *observability.SearchMetrics
	require.NotPanics(t, func() {
		searchBag = observability.NewSearchMetrics(te.Registry, false, nil)
	}, "Pitfall 8: search bag must not collide with embed bag or runtime collectors")
	require.NotNil(t, searchBag)

	// Materialize *Vec families so Gather() surfaces them (Plan 04-01
	// Deviation 1: client_golang only emits *Vec families with ≥1 child
	// series; GaugeFunc + flat counters surface unconditionally).
	embedBag.Processed.WithLabelValues("ollama", "bge-m3", "success", "cpu").Inc()
	embedBag.Duration.Bind("ollama", "bge-m3", "cpu").Observe(nil, 0.001)
	embedBag.FFIPanicTotal.WithLabelValues("cpu").Inc()
	searchBag.IncRequest("", "vector", "success")
	searchBag.BindDuration("", "vector", "embed").Observe(nil, 0.001)
	searchBag.Candidates.Vec().WithLabelValues().Observe(10)
	searchBag.IndexSizeBytes.WithLabelValues("hnsw").Set(0)

	// Verify all 11 expected families surface.
	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	got := make(map[string]bool, len(mfs))
	for _, mf := range mfs {
		got[mf.GetName()] = true
	}

	wantFamilies := []string{
		// Embed (6 + 1 ffi)
		"nornicdb_embed_queue_depth",
		"nornicdb_embed_processed_total",
		"nornicdb_embed_duration_seconds",
		"nornicdb_embed_cache_hits_total",
		"nornicdb_embed_cache_misses_total",
		"nornicdb_embed_worker_running",
		"nornicdb_embed_ffi_panics_total",
		// Search (4)
		"nornicdb_search_requests_total",
		"nornicdb_search_duration_seconds",
		"nornicdb_search_candidates_rows",
		"nornicdb_search_index_size_bytes",
	}
	for _, want := range wantFamilies {
		assert.True(t, got[want], "expected family %q after NewEmbedMetrics + NewSearchMetrics", want)
	}

	// Verify the search live-read collector also surfaces (D-15b).
	assert.True(t, got["nornicdb_search_index_size_bytes_live"],
		"search index_size_bytes_live custom collector must register")

	// Subsystem family count — at least 11 from this plan, plus go/process
	// runtime collectors from Phase 1.
	embedSearch := 0
	for name := range got {
		if strings.HasPrefix(name, "nornicdb_embed_") || strings.HasPrefix(name, "nornicdb_search_") {
			embedSearch++
		}
	}
	assert.GreaterOrEqual(t, embedSearch, 11,
		"≥ 11 embed+search family lines per Plan 04-05 goal_backward (got %d)", embedSearch)
}

// TestEmbedQueueProbe_NilSafe asserts that the probe adapter tolerates a
// nil EmbedQueue (test mode / embedded-library callers).
func TestEmbedQueueProbe_NilSafe(t *testing.T) {
	p := embedQueueProbe{q: nil}
	assert.Equal(t, 0, p.QueueLen())
}

// TestSearchServiceProbe_NilSafe asserts that the probe adapter tolerates
// a nil Service.
func TestSearchServiceProbe_NilSafe(t *testing.T) {
	p := searchServiceProbe{svc: nil}
	for _, kind := range observability.AllowedSearchIndexKinds {
		assert.Equal(t, uint64(0), p.IndexSizeBytes(kind))
	}
}

// TestAttachEmbedMetricsToEmbedder_NilTolerated asserts the attachment
// helper returns false (and does not panic) on nil inputs.
func TestAttachEmbedMetricsToEmbedder_NilTolerated(t *testing.T) {
	assert.False(t, attachEmbedMetricsToEmbedder(nil, nil))
	te := observability.NewTestEnv(t)
	bag := observability.NewEmbedMetrics(te.Registry, nil)
	assert.False(t, attachEmbedMetricsToEmbedder(nil, bag),
		"nil embedder must not be attached")
}
