package observability

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Plan 04-05-02 GREEN: NewEmbedMetrics ships 6 families + ffi_panics_total
// + queue_depth GaugeFunc per CONTEXT MET-12 + D-06a + D-09 + D-15b.

// embedProbeStub is the test seam for EmbedProbe.QueueLen used by the
// nornicdb_embed_queue_depth GaugeFunc callback.
type embedProbeStub struct {
	queueLen int
}

func (e embedProbeStub) QueueLen() int { return e.queueLen }

// panickyEmbedProbe asserts the GaugeFunc defer-recover safety net per
// RISK-8 / Pitfall 1.
type panickyEmbedProbe struct{}

func (panickyEmbedProbe) QueueLen() int { panic("simulated probe panic") }

// TestEmbedMetrics_ProcessedTotalLabels asserts MET-12 + CONTEXT D-06:
// processed_total carries {provider, model, result, mode}. Seven families
// register total per CONTEXT enumeration (6 + ffi_panics_total).
func TestEmbedMetrics_ProcessedTotalLabels(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewEmbedMetrics(te.Registry, embedProbeStub{queueLen: 0})
	require.NotNil(t, bag)

	// Materialize *Vec families so they appear in Gather (Plan 04-01
	// Deviation 1: client_golang Gather() only emits *Vec families with
	// ≥1 child series).
	bag.Processed.WithLabelValues("ollama", "bge-m3", "success", "cpu").Inc()
	bag.Duration.Bind("ollama", "bge-m3", "cpu").Observe(context.Background(), 0.001)
	bag.CacheHits.Inc()
	bag.CacheMisses.Inc()
	bag.WorkerRunning.Set(1)
	bag.FFIPanicTotal.WithLabelValues("cpu").Inc()

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
	te := NewTestEnv(t)
	bag := NewEmbedMetrics(te.Registry, embedProbeStub{})
	require.NotNil(t, bag)
	te.AssertCardinalityCeiling(t, "nornicdb_embed_ffi_panics_total", 5, func(tenant string) {
		for _, mode := range AllowedEmbedBackends {
			bag.FFIPanicTotal.WithLabelValues(mode).Inc()
		}
		_ = tenant
	})
}

// TestQueueDepthGaugeFunc asserts the queue_depth callback reads through
// EmbedProbe.QueueLen() per D-15b.
func TestQueueDepthGaugeFunc(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewEmbedMetrics(te.Registry, embedProbeStub{queueLen: 42})
	require.NotNil(t, bag)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)

	for _, mf := range mfs {
		if mf.GetName() == "nornicdb_embed_queue_depth" {
			require.Len(t, mf.Metric, 1)
			assert.Equal(t, 42.0, mf.Metric[0].GetGauge().GetValue(),
				"queue_depth GaugeFunc must reflect EmbedProbe.QueueLen()")
			return
		}
	}
	t.Fatal("nornicdb_embed_queue_depth not found in Gather()")
}

// TestQueueDepthGaugeFunc_PanicSafe asserts RISK-8: a panicking probe
// must not poison the scrape; the callback returns 0.
func TestQueueDepthGaugeFunc_PanicSafe(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewEmbedMetrics(te.Registry, panickyEmbedProbe{})
	require.NotNil(t, bag)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err, "Gather must succeed even when probe panics")

	for _, mf := range mfs {
		if mf.GetName() == "nornicdb_embed_queue_depth" {
			require.Len(t, mf.Metric, 1)
			assert.Equal(t, 0.0, mf.Metric[0].GetGauge().GetValue(),
				"queue_depth must return 0 on probe panic per RISK-8")
			return
		}
	}
	t.Fatal("nornicdb_embed_queue_depth not found in Gather()")
}

// TestQueueDepthGaugeFunc_NilProbe asserts the constructor tolerates nil
// probe (defensive fallback) — returns 0.
func TestQueueDepthGaugeFunc_NilProbe(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewEmbedMetrics(te.Registry, nil)
	require.NotNil(t, bag)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == "nornicdb_embed_queue_depth" {
			require.Len(t, mf.Metric, 1)
			assert.Equal(t, 0.0, mf.Metric[0].GetGauge().GetValue())
			return
		}
	}
	t.Fatal("nornicdb_embed_queue_depth not found in Gather()")
}

// TestEmbeddingLatency_LongTailBuckets asserts the duration histogram uses
// the long-tail bucket set per MET-05 (tail ≥ 60s).
func TestEmbeddingLatency_LongTailBuckets(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewEmbedMetrics(te.Registry, embedProbeStub{})
	require.NotNil(t, bag)

	bag.Duration.Bind("local", "bge-m3", "metal").Observe(context.Background(), 0.5)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)

	for _, mf := range mfs {
		if mf.GetName() != "nornicdb_embed_duration_seconds" {
			continue
		}
		require.NotEmpty(t, mf.Metric)
		hist := mf.Metric[0].GetHistogram()
		require.NotNil(t, hist)
		// Last bucket bound MUST be ≥ 60 (MET-05 long-tail rule). Phase 3
		// EmbeddingLatencyBucketsSeconds tail is 600.
		buckets := hist.GetBucket()
		require.NotEmpty(t, buckets)
		last := buckets[len(buckets)-1]
		assert.GreaterOrEqual(t, last.GetUpperBound(), float64(60),
			"MET-05: embedding histogram tail must be ≥ 60s; got %v", last.GetUpperBound())
		return
	}
	t.Fatal("nornicdb_embed_duration_seconds not found in Gather()")
}

// TestMetricCardinality_Embed asserts the processed_total cardinality stays
// within the RESEARCH §Q11 ceiling under adversarial label drive.
func TestMetricCardinality_Embed(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewEmbedMetrics(te.Registry, embedProbeStub{})
	require.NotNil(t, bag)

	// Closed-enum drive (4 providers × ~3 sample models × 3 results × 5
	// modes = 180); ceiling 250 leaves headroom for new providers/models
	// without rebreaking the cardinality wall (RESEARCH §Q11).
	te.AssertCardinalityCeiling(t, "nornicdb_embed_processed_total", 250, func(tenant string) {
		for _, provider := range AllowedEmbedProviders {
			for _, model := range []string{"bge-m3", "nomic-embed-text", "text-embedding-3-small"} {
				for _, result := range AllowedEmbedResults {
					for _, mode := range AllowedEmbedBackends {
						bag.Processed.WithLabelValues(provider, model, result, mode).Inc()
					}
				}
			}
		}
		_ = tenant
	})
}
