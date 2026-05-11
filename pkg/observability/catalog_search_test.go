package observability

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Plan 04-05-04 GREEN: NewSearchMetrics ships 4 families per MET-13 +
// closed stage/kind enums + D-15b index_size_bytes GaugeFunc + D-08
// tenant-flag forward-compat.

// searchProbeStub is the test seam for SearchProbe.IndexSizeBytes used by
// the per-kind GaugeFunc callbacks.
type searchProbeStub struct {
	hnswBytes uint64
	bm25Bytes uint64
}

func (s searchProbeStub) IndexSizeBytes(kind string) uint64 {
	switch kind {
	case "hnsw":
		return s.hnswBytes
	case "bm25":
		return s.bm25Bytes
	default:
		return 0
	}
}

// panickySearchProbe asserts the GaugeFunc defer-recover safety net per
// RISK-8 / Pitfall 1.
type panickySearchProbe struct{}

func (panickySearchProbe) IndexSizeBytes(kind string) uint64 {
	panic("simulated probe panic")
}

// TestSearchMetrics_RegistersFour asserts MET-13's four search families per
// ADR §2.3: requests_total, duration_seconds, candidates_rows, index_size_bytes.
func TestSearchMetrics_RegistersFour(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewSearchMetrics(te.Registry, false, searchProbeStub{hnswBytes: 1024, bm25Bytes: 512})
	require.NotNil(t, bag)

	// Materialize *Vec families so they appear in Gather (Plan 04-01
	// Deviation 1: client_golang Gather() only emits *Vec families with
	// ≥1 child series).
	bag.IncRequest("", "vector", "success")
	bag.BindDuration("", "vector", "embed").Observe(context.Background(), 0.001)
	bag.Candidates.Vec().WithLabelValues().Observe(10)
	bag.IndexSizeBytes.WithLabelValues("hnsw").Set(0)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	names := metricNames(mfs)
	for _, want := range []string{
		"nornicdb_search_requests_total",
		"nornicdb_search_duration_seconds",
		"nornicdb_search_candidates_rows",
		"nornicdb_search_index_size_bytes",
	} {
		assert.Contains(t, names, want, "MET-13: Search family %q must register", want)
	}
}

// TestSearchStage_ClosedEnum asserts MET-13: stage label accepts only
// {embed, index, fuse}. duration_seconds{mode, stage} cardinality
// ceiling = 9 tenant-OFF (3 modes × 3 stages); RESEARCH §Q11.
func TestSearchStage_ClosedEnum(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewSearchMetrics(te.Registry, false, searchProbeStub{})
	require.NotNil(t, bag)
	te.AssertCardinalityCeiling(t, "nornicdb_search_duration_seconds", 9, func(tenant string) {
		for _, mode := range AllowedSearchModes {
			for _, stage := range AllowedSearchStages {
				bag.BindDuration("", mode, stage).Observe(context.Background(), 0.001)
			}
		}
		_ = tenant
	})
}

// TestSearchIndexKind_ClosedEnum asserts MET-13 / D-15b: index_size_bytes
// {kind} accepts only {hnsw, bm25}. Cardinality ceiling = 2.
func TestSearchIndexKind_ClosedEnum(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewSearchMetrics(te.Registry, false, searchProbeStub{})
	require.NotNil(t, bag)
	te.AssertCardinalityCeiling(t, "nornicdb_search_index_size_bytes", 2, func(tenant string) {
		for _, kind := range AllowedSearchIndexKinds {
			bag.IndexSizeBytes.WithLabelValues(kind).Set(0)
		}
		_ = tenant
	})
}

// TestSearchResult_ClosedEnum asserts CONTEXT MET-13: result label accepts
// only {success, no_results, error}.
func TestSearchResult_ClosedEnum(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewSearchMetrics(te.Registry, false, searchProbeStub{})
	require.NotNil(t, bag)
	te.AssertCardinalityCeiling(t, "nornicdb_search_requests_total", 9, func(tenant string) {
		for _, mode := range AllowedSearchModes {
			for _, result := range AllowedSearchResults {
				bag.IncRequest("", mode, result)
			}
		}
		_ = tenant
	})
}

// TestIndexSizeGaugeFunc asserts the per-kind live-read GaugeFunc reads
// through SearchProbe.IndexSizeBytes(kind) per D-15b.
func TestIndexSizeGaugeFunc(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewSearchMetrics(te.Registry, false, searchProbeStub{
		hnswBytes: 4096,
		bm25Bytes: 2048,
	})
	require.NotNil(t, bag)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)

	got := map[string]float64{}
	for _, mf := range mfs {
		if mf.GetName() != "nornicdb_search_index_size_bytes_live" {
			continue
		}
		for _, m := range mf.Metric {
			for _, lbl := range m.GetLabel() {
				if lbl.GetName() == "kind" {
					got[lbl.GetValue()] = m.GetGauge().GetValue()
				}
			}
		}
	}
	assert.Equal(t, 4096.0, got["hnsw"], "GaugeFunc must reflect SearchProbe.IndexSizeBytes(\"hnsw\")")
	assert.Equal(t, 2048.0, got["bm25"], "GaugeFunc must reflect SearchProbe.IndexSizeBytes(\"bm25\")")
}

// TestIndexSizeGaugeFunc_PanicSafe asserts RISK-8: a panicking probe must
// not poison the scrape; the callback returns 0 per kind.
func TestIndexSizeGaugeFunc_PanicSafe(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewSearchMetrics(te.Registry, false, panickySearchProbe{})
	require.NotNil(t, bag)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err, "Gather must succeed even when probe panics")

	for _, mf := range mfs {
		if mf.GetName() != "nornicdb_search_index_size_bytes_live" {
			continue
		}
		for _, m := range mf.Metric {
			assert.Equal(t, 0.0, m.GetGauge().GetValue(),
				"panicky probe must surface 0 per kind (RISK-8)")
		}
	}
}

// TestTenantFlag_LabelOmit_Search asserts D-08: when tenantLabelsEnabled
// is false, the database label is omitted from requests_total +
// duration_seconds. With tenantLabelsEnabled=true, the label IS present.
func TestTenantFlag_LabelOmit_Search(t *testing.T) {
	t.Run("tenant-OFF: no database label", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewSearchMetrics(te.Registry, false, searchProbeStub{})
		bag.IncRequest("any-db", "vector", "success") // database arg dropped
		bag.BindDuration("any-db", "vector", "embed").Observe(context.Background(), 0.001)

		mfs, err := te.Registry.Gather()
		require.NoError(t, err)
		for _, mf := range mfs {
			if mf.GetName() == "nornicdb_search_requests_total" || mf.GetName() == "nornicdb_search_duration_seconds" {
				for _, m := range mf.Metric {
					for _, lbl := range m.GetLabel() {
						assert.NotEqual(t, "database", lbl.GetName(),
							"tenant-OFF: %s must not carry database label", mf.GetName())
					}
				}
			}
		}
	})

	t.Run("tenant-ON: database label present", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewSearchMetrics(te.Registry, true, searchProbeStub{})
		bag.IncRequest("nornicdb", "vector", "success")
		bag.BindDuration("nornicdb", "vector", "embed").Observe(context.Background(), 0.001)

		mfs, err := te.Registry.Gather()
		require.NoError(t, err)
		var sawDBOnRequests, sawDBOnDuration bool
		for _, mf := range mfs {
			if mf.GetName() == "nornicdb_search_requests_total" {
				for _, m := range mf.Metric {
					for _, lbl := range m.GetLabel() {
						if lbl.GetName() == "database" {
							sawDBOnRequests = true
						}
					}
				}
			}
			if mf.GetName() == "nornicdb_search_duration_seconds" {
				for _, m := range mf.Metric {
					for _, lbl := range m.GetLabel() {
						if lbl.GetName() == "database" {
							sawDBOnDuration = true
						}
					}
				}
			}
		}
		assert.True(t, sawDBOnRequests, "tenant-ON: requests_total must carry database label")
		assert.True(t, sawDBOnDuration, "tenant-ON: duration_seconds must carry database label")
	})
}
