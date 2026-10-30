package observability

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type recordingSpanProcessor struct {
	started  int
	ended    int
	flushed  int
	shutdown int
}

func (r *recordingSpanProcessor) OnStart(context.Context, sdktrace.ReadWriteSpan) { r.started++ }
func (r *recordingSpanProcessor) OnEnd(sdktrace.ReadOnlySpan)                     { r.ended++ }
func (r *recordingSpanProcessor) ForceFlush(context.Context) error {
	r.flushed++
	return nil
}
func (r *recordingSpanProcessor) Shutdown(context.Context) error {
	r.shutdown++
	return nil
}

func TestCatalogBindHelpersObserveWithTenantLabels(t *testing.T) {
	ctx := context.Background()

	t.Run("bolt bind message duration", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewBoltMetrics(te.Registry)
		bag.BindMessageDuration("run").Observe(ctx, 0.002)

		metric := onlyMetric(t, te, "nornicdb_bolt_message_duration_seconds")
		require.Equal(t, map[string]string{"op": "run"}, metricLabels(metric))
		require.Equal(t, uint64(1), metric.GetHistogram().GetSampleCount())
	})

	t.Run("cypher observe query duration with tenant", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewCypherMetrics(te.Registry, true, nil)
		require.True(t, bag.TenantLabelsEnabled())
		bag.ObserveQueryDuration(ctx, "write", "neo4j", 0.003)

		metric := onlyMetric(t, te, "nornicdb_cypher_query_duration_seconds")
		require.Equal(t, map[string]string{"op_type": "write", "database": "neo4j"}, metricLabels(metric))
		require.Equal(t, uint64(1), metric.GetHistogram().GetSampleCount())
	})

	t.Run("http observe request duration with tenant", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewHTTPMetrics(te.Registry, true)
		require.True(t, bag.TenantLabelsEnabled())
		bag.ObserveRequestDuration(ctx, "GET", "/db/{name}", "2xx", "neo4j", 0.004)

		metric := onlyMetric(t, te, "nornicdb_http_request_duration_seconds")
		require.Equal(t, map[string]string{
			"method":        "GET",
			"path_template": "/db/{name}",
			"status_class":  "2xx",
			"database":      "neo4j",
		}, metricLabels(metric))
		require.Equal(t, uint64(1), metric.GetHistogram().GetSampleCount())
	})

	t.Run("search bind duration and request with tenant", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewSearchMetrics(te.Registry, true, nil)
		require.True(t, bag.TenantLabelsEnabled())
		bag.BindDuration("neo4j", "vector", "fuse").Observe(ctx, 0.005)
		bag.IncRequest("neo4j", "vector", "success")

		duration := onlyMetric(t, te, "nornicdb_search_duration_seconds")
		require.Equal(t, map[string]string{"database": "neo4j", "mode": "vector", "stage": "fuse"}, metricLabels(duration))
		require.Equal(t, uint64(1), duration.GetHistogram().GetSampleCount())

		request := onlyMetric(t, te, "nornicdb_search_requests_total")
		require.Equal(t, map[string]string{"database": "neo4j", "mode": "vector", "result": "success"}, metricLabels(request))
		require.Equal(t, float64(1), request.GetCounter().GetValue())
	})
}

func TestBSPSelfMetricsDelegatesAndRecordsDepth(t *testing.T) {
	inner := &recordingSpanProcessor{}
	queueDepth := prometheus.NewGauge(prometheus.GaugeOpts{Name: "bsp_queue_depth_test"})
	dropped := prometheus.NewCounter(prometheus.CounterOpts{Name: "bsp_dropped_test"})
	setBSPMetricsRefs(queueDepth, dropped)

	metrics := newBSPSelfMetrics(inner, 0)
	metrics.OnStart(context.Background(), nil)
	metrics.OnEnd(nil)
	require.NoError(t, metrics.ForceFlush(context.Background()))
	require.NoError(t, metrics.Shutdown(context.Background()))

	require.Equal(t, 1, inner.started)
	require.Equal(t, 1, inner.ended)
	require.Equal(t, 1, inner.flushed)
	require.Equal(t, 1, inner.shutdown)
	require.Equal(t, float64(0), testutil.ToFloat64(queueDepth))
	require.Equal(t, float64(1), testutil.ToFloat64(dropped))

	// Nil metric refs remain safe while preserving delegation.
	setBSPMetricsRefs(nil, nil)
	withoutRefs := newBSPSelfMetrics(inner, 10)
	withoutRefs.OnEnd(nil)
	require.Equal(t, 2, inner.ended)
}

func TestTenantAwareMetricHelperBranches(t *testing.T) {
	ctx := context.Background()

	t.Run("knowledge policy tenant helpers", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewKnowledgePolicyMetrics(te.Registry, true, func() float64 { return 0.75 })
		require.True(t, bag.TenantLabelsEnabled())
		bag.IncScored("node", "visible", "neo4j")
		bag.IncSuppression("node", "below_threshold", "neo4j")
		bag.IncOnAccess("applied", "neo4j")
		bag.IncDeindexEnqueued("node", "neo4j")
		bag.IncReadFilterDropped("node", "neo4j")
		bag.IncReconcile("startup", "neo4j")
		for i := 0; i < DecayScoreSampleDenominator; i++ {
			bag.ObserveDecayScoreSampled(ctx, "node", "neo4j", 0.7)
		}
		bag.ObserveAccessFlushBatchRows(ctx, 3)
		bag.ObserveAccessFlushDuration(ctx, 0.01)

		metric := onlyMetric(t, te, "nornicdb_knowledge_policy_suppressions_total")
		require.Equal(t, map[string]string{"entity_kind": "node", "reason": "below_threshold", "database": "neo4j"}, metricLabels(metric))
		require.Equal(t, float64(1), metric.GetCounter().GetValue())
	})

	t.Run("mvcc tenant labels", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewMVCCMetrics(te.Registry, true, nil)
		require.True(t, bag.TenantLabelsEnabled())
		bag.UpdateBand("neo4j", 0.99)

		mfs, err := te.Registry.Gather()
		require.NoError(t, err)
		var found bool
		for _, mf := range mfs {
			if mf.GetName() != "nornicdb_mvcc_pressure_band" {
				continue
			}
			for _, metric := range mf.Metric {
				labels := metricLabels(metric)
				if labels["database"] == "neo4j" && metric.GetGauge().GetValue() == 1 {
					found = true
				}
			}
		}
		require.True(t, found)
	})

	t.Run("storage index rebuild tenant labels", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewStorageMetrics(te.Registry, true, nil)
		require.True(t, bag.TenantLabelsEnabled())
		bag.BindIndexRebuild("neo4j", "embedding", "success").Inc()

		metric := onlyMetric(t, te, "nornicdb_storage_index_rebuild_total")
		require.Equal(t, map[string]string{"database": "neo4j", "index": "embedding", "result": "success"}, metricLabels(metric))
		require.Equal(t, float64(1), metric.GetCounter().GetValue())
	})
}

func onlyMetric(t *testing.T, te *TestEnv, name string) *dto.Metric {
	t.Helper()
	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == name {
			require.Len(t, mf.Metric, 1, name)
			return mf.Metric[0]
		}
	}
	require.FailNowf(t, "missing metric", "metric family %s was not gathered", name)
	return nil
}

func metricLabels(metric *dto.Metric) map[string]string {
	labels := make(map[string]string, len(metric.Label))
	for _, label := range metric.Label {
		labels[label.GetName()] = label.GetValue()
	}
	return labels
}
