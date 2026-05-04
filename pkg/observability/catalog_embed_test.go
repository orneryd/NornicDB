package observability

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Plan 04-01 Wave-0 RED: NewEmbedMetrics + the closed mode enum land in
// Plan 04-05. Compile-fail until then.

// TestEmbedMetrics_ProcessedTotalLabels asserts MET-12 + CONTEXT D-06:
// processed_total carries {provider, model, result, mode}. Six families
// register total per CONTEXT enumeration.
func TestEmbedMetrics_ProcessedTotalLabels(t *testing.T) {
	t.Skip("RED: pending Plan 04-05 (Embeddings bag delivery)")

	te := NewTestEnv(t)
	bag := NewEmbedMetrics(te.Registry)
	require.NotNil(t, bag)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	names := metricNames(mfs)
	for _, want := range []string{
		"nornicdb_embed_queue_depth",
		"nornicdb_embed_processed_total",
		"nornicdb_embed_duration_seconds",
		"nornicdb_embed_cache_hits_total",
		"nornicdb_embed_cache_misses_total",
		"nornicdb_embed_worker_running",
		"nornicdb_embed_ffi_panics_total",
	} {
		assert.Contains(t, names, want, "MET-12: Embed family %q must register", want)
	}
}

// TestEmbedMode_ClosedEnum asserts CONTEXT D-06a: mode label accepts only
// {gpu, cpu, cuda, metal, vulkan}. Cardinality ceiling for ffi_panics_total
// is 5 (one per mode).
func TestEmbedMode_ClosedEnum(t *testing.T) {
	t.Skip("RED: pending Plan 04-05 (Embeddings bag delivery)")

	te := NewTestEnv(t)
	bag := NewEmbedMetrics(te.Registry)
	require.NotNil(t, bag)
	te.AssertCardinalityCeiling(t, "nornicdb_embed_ffi_panics_total", 5, func(tenant string) {
		for _, mode := range []string{"gpu", "cpu", "cuda", "metal", "vulkan"} {
			bag.FFIPanicTotal.WithLabelValues(mode).Inc()
		}
		_ = tenant
	})
}
