package search

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/observability"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestServiceIndexSizeBytes(t *testing.T) {
	engine := newTestMemoryEngine(t)
	t.Cleanup(func() { engine.Close() })

	svc := NewServiceWithDimensions(engine, 4)
	require.Equal(t, uint64(0), (*Service)(nil).IndexSizeBytes("hnsw"))
	require.Equal(t, uint64(0), svc.IndexSizeBytes("hnsw"))
	require.Equal(t, uint64(0), svc.IndexSizeBytes("bm25"))
	require.Equal(t, uint64(0), svc.IndexSizeBytes("unknown"))

	idx := NewHNSWIndex(4, DefaultHNSWConfig())
	require.NoError(t, idx.Add("a", []float32{1, 0, 0, 0}))
	require.NoError(t, idx.Add("b", []float32{0, 1, 0, 0}))

	svc.hnswMu.Lock()
	svc.hnswIndex = idx
	svc.hnswMu.Unlock()

	want := uint64(idx.Size()) * uint64(DefaultVectorDimensions) * 4
	require.Equal(t, want, svc.IndexSizeBytes("hnsw"))
}

func TestObserveSearchStageDirect(t *testing.T) {
	engine := newTestMemoryEngine(t)
	t.Cleanup(func() { engine.Close() })
	svc := NewServiceWithDimensions(engine, 4)
	reg := prometheus.NewRegistry()
	svc.AttachMetrics(observability.NewSearchMetrics(reg, false, &nilSearchProbe{}))

	ctx := context.Background()
	svc.observeSearchStage(ctx, "hybrid", "index", 3*time.Millisecond)
	svc.observeSearchStage(ctx, "hybrid", "fuse", 5*time.Millisecond)
	svc.observeSearchStage(ctx, "vector", "embed", 7*time.Millisecond)

	require.Equal(t, uint64(1), histogramSampleCountByLabels(t, reg, "nornicdb_search_duration_seconds", map[string]string{"mode": "hybrid", "stage": "index"}))
	require.Equal(t, uint64(1), histogramSampleCountByLabels(t, reg, "nornicdb_search_duration_seconds", map[string]string{"mode": "hybrid", "stage": "fuse"}))
	require.Equal(t, uint64(1), histogramSampleCountByLabels(t, reg, "nornicdb_search_duration_seconds", map[string]string{"mode": "vector", "stage": "embed"}))

	svc.AttachMetrics(nil)
	svc.observeSearchStage(ctx, "hybrid", "index", time.Millisecond)
	require.Equal(t, uint64(1), histogramSampleCountByLabels(t, reg, "nornicdb_search_duration_seconds", map[string]string{"mode": "hybrid", "stage": "index"}))
}

func TestRecordSearchError(t *testing.T) {
	_, span := noop.NewTracerProvider().Tracer("search-test").Start(context.Background(), "span")
	recordSearchError(span, nil)
	recordSearchError(span, errors.New("boom"))
}

func histogramSampleCountByLabels(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) uint64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, metric := range mf.Metric {
			if metricLabelsMatch(metric.GetLabel(), labels) {
				require.NotNil(t, metric.Histogram)
				return metric.Histogram.GetSampleCount()
			}
		}
	}
	t.Fatalf("histogram %q with labels %v not found", name, labels)
	return 0
}

func metricLabelsMatch(got []*dto.LabelPair, want map[string]string) bool {
	for key, val := range want {
		matched := false
		for _, label := range got {
			if label.GetName() == key && label.GetValue() == val {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}
