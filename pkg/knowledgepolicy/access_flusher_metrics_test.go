package knowledgepolicy

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func newFlusherWithMetrics(t *testing.T) (*AccessFlusher, *mockStore, *observability.KnowledgePolicyMetrics) {
	t.Helper()
	store := newMockStore()
	acc := NewAccessAccumulator(true, 64)
	f := NewAccessFlusher(acc, store, time.Hour)
	reg := prometheus.NewRegistry()
	bag := observability.NewKnowledgePolicyMetrics(reg, false, func() float64 { return f.BufferFullness() })
	f.SetMetrics(bag)
	return f, store, bag
}

// totalHistogramSamples walks a HistogramVec and returns the summed sample
// count across all label-value tuples.
func totalHistogramSamples(t *testing.T, vec *prometheus.HistogramVec) uint64 {
	t.Helper()
	ch := make(chan prometheus.Metric, 8)
	vec.Collect(ch)
	close(ch)
	var total uint64
	for m := range ch {
		pm := &dto.Metric{}
		require.NoError(t, m.Write(pm))
		if pm.Histogram != nil {
			total += pm.Histogram.GetSampleCount()
		}
	}
	return total
}

// TestFlusher_MetricsFireOnNonEmptyFlush pins that a flush with rows emits
// exactly one batch_rows sample and exactly one duration sample.
func TestFlusher_MetricsFireOnNonEmptyFlush(t *testing.T) {
	f, _, bag := newFlusherWithMetrics(t)

	f.accumulator.IncrementAccess("n1")
	f.accumulator.IncrementAccess("n2")
	f.Flush()

	require.EqualValues(t, 1, totalHistogramSamples(t, bag.AccessFlushBatchRows.Vec()),
		"batch_rows must receive exactly one sample per flush")
	require.EqualValues(t, 1, totalHistogramSamples(t, bag.AccessFlushDuration.Vec()),
		"duration must receive exactly one sample per flush")
}

// TestFlusher_MetricsFireOnEmptyFlush pins that the short-circuit empty
// flush STILL emits both histograms (a zero-row sample is a useful signal
// for detecting timer-driven waste).
func TestFlusher_MetricsFireOnEmptyFlush(t *testing.T) {
	f, _, bag := newFlusherWithMetrics(t)

	f.Flush()

	require.EqualValues(t, 1, totalHistogramSamples(t, bag.AccessFlushBatchRows.Vec()),
		"batch_rows must receive one zero-row sample")
	require.EqualValues(t, 1, totalHistogramSamples(t, bag.AccessFlushDuration.Vec()),
		"duration must still record even on empty flush")
}

// TestFlusher_BufferFullness pins the GaugeFunc callback reads the real
// accumulator state. Uses the accumulator's max-buffer limit.
func TestFlusher_BufferFullness(t *testing.T) {
	f, _, _ := newFlusherWithMetrics(t)

	require.EqualValues(t, 0, f.BufferFullness(), "empty accumulator → 0 fullness")
	// Add 16 distinct entities into a 64-slot accumulator → 0.25.
	for i := 0; i < 16; i++ {
		f.accumulator.IncrementAccess(uniqueID(i))
	}
	require.InDelta(t, 0.25, f.BufferFullness(), 1e-9, "16 / 64 = 0.25")

	// Draining resets the counter.
	_ = f.accumulator.DrainAll()
	require.EqualValues(t, 0, f.BufferFullness(), "drain → 0")
}

// TestFlusher_ResolveMetricsFallsBackToGlobal pins the lazy global-ref
// fallback used in production wiring (main.go publishes the bag AFTER
// NewAccessFlusher has already constructed the flusher).
func TestFlusher_ResolveMetricsFallsBackToGlobal(t *testing.T) {
	store := newMockStore()
	acc := NewAccessAccumulator(true, 0)
	f := NewAccessFlusher(acc, store, time.Hour)
	// No local metrics attached.

	reg := prometheus.NewRegistry()
	bag := observability.NewKnowledgePolicyMetrics(reg, false, nil)
	prev := observability.GetKnowledgePolicyMetrics()
	observability.SetKnowledgePolicyMetrics(bag)
	t.Cleanup(func() { observability.SetKnowledgePolicyMetrics(prev) })

	acc.IncrementAccess("n1")
	f.Flush()

	require.EqualValues(t, 1, totalHistogramSamples(t, bag.AccessFlushBatchRows.Vec()),
		"flusher with nil local metrics must pick up the global ref at fire time")
}

// uniqueID returns a short deterministic string per int. Used so the
// accumulator sees distinct keys (BufferFullness counts distinct entities).
func uniqueID(i int) string {
	return "n-" + time.Unix(0, int64(i)).String()
}
