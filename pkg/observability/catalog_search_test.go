package observability

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Plan 04-01 Wave-0 RED: NewSearchMetrics + the closed stage and kind enums
// land in Plan 04-05. Compile-fail until then.

// TestSearchMetrics_RegistersFour asserts MET-13's four search families per
// ADR §2.3: requests_total, duration_seconds, candidates, index_size_bytes.
func TestSearchMetrics_RegistersFour(t *testing.T) {
	t.Skip("RED: pending Plan 04-05 (Search bag delivery)")

	te := NewTestEnv(t)
	bag := NewSearchMetrics(te.Registry, false /* tenantLabelsEnabled */)
	require.NotNil(t, bag)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	names := metricNames(mfs)
	for _, want := range []string{
		"nornicdb_search_requests_total",
		"nornicdb_search_duration_seconds",
		"nornicdb_search_candidates",
		"nornicdb_search_index_size_bytes",
	} {
		assert.Contains(t, names, want, "MET-13: Search family %q must register", want)
	}
}

// TestSearchStage_ClosedEnum asserts CONTEXT §domain MET-13: stage label
// accepts only {embed, index, fuse}. duration_seconds{database?, mode, stage}
// cardinality ceiling = 9 tenant-OFF (3 modes × 3 stages); RESEARCH §Q11.
func TestSearchStage_ClosedEnum(t *testing.T) {
	t.Skip("RED: pending Plan 04-05 (Search bag delivery)")

	te := NewTestEnv(t)
	bag := NewSearchMetrics(te.Registry, false)
	require.NotNil(t, bag)
	te.AssertCardinalityCeiling(t, "nornicdb_search_duration_seconds", 9, func(tenant string) {
		for _, mode := range []string{"vector", "text", "hybrid"} {
			for _, stage := range []string{"embed", "index", "fuse"} {
				bag.Duration.Bind(mode, stage).Observe(nil, 0.001)
			}
		}
		_ = tenant
	})
}

// TestIndexSizeKind_ClosedEnum asserts CONTEXT MET-13 / D-15b: index_size_bytes
// {kind} accepts only {hnsw, bm25}. Cardinality ceiling = 2.
func TestIndexSizeKind_ClosedEnum(t *testing.T) {
	t.Skip("RED: pending Plan 04-05 (Search bag delivery)")

	te := NewTestEnv(t)
	bag := NewSearchMetrics(te.Registry, false)
	require.NotNil(t, bag)
	te.AssertCardinalityCeiling(t, "nornicdb_search_index_size_bytes", 2, func(tenant string) {
		for _, kind := range []string{"hnsw", "bm25"} {
			bag.IndexSizeBytes.WithLabelValues(kind).Set(0)
		}
		_ = tenant
	})
}
