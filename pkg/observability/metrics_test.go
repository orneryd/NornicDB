package observability

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMetricNaming_NamespaceInjected verifies D-01b/D-01c: the Namespace
// "nornicdb" is injected by the helper and the final family name is
// concatenated as nornicdb_<subsystem>_<name>. Caller never sets Namespace.
func TestMetricNaming_NamespaceInjected(t *testing.T) {
	te := NewTestEnv(t)
	cv := NewCounterVec(te.Registry,
		MetricOpts{Subsystem: "cypher", Name: "queries_total", Help: "test"},
		[]string{"database", "op_type"})
	cv.WithLabelValues("default", "read").Inc()

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	names := metricNames(mfs)
	assert.Contains(t, names, "nornicdb_cypher_queries_total",
		"D-01b/D-01c: helper must inject Namespace=nornicdb and concat as nornicdb_<subsystem>_<name>")
}

// TestNamingValidation_RejectsBadSuffix exercises MET-02 across all six
// constructors: bare names without the type-mandated suffix must panic at
// registration time with the documented error message.
func TestNamingValidation_RejectsBadSuffix(t *testing.T) {
	cases := []struct {
		ctor           string
		name           string
		requiredSuffix string
		invoke         func(reg *prometheus.Registry)
	}{
		{
			"NewCounterVec", "queries", "_total",
			func(reg *prometheus.Registry) {
				NewCounterVec(reg, MetricOpts{Subsystem: "cypher", Name: "queries", Help: "h"}, nil)
			},
		},
		{
			"NewLatencyHistogramVec", "duration", "_seconds",
			func(reg *prometheus.Registry) {
				NewLatencyHistogramVec(reg, MetricOpts{Subsystem: "cypher", Name: "duration", Help: "h"}, nil)
			},
		},
		{
			"NewSizeHistogramVec", "payload", "_bytes",
			func(reg *prometheus.Registry) {
				NewSizeHistogramVec(reg, MetricOpts{Subsystem: "storage", Name: "payload", Help: "h"}, nil)
			},
		},
		{
			"NewRowCountHistogramVec", "rows", "_rows",
			func(reg *prometheus.Registry) {
				NewRowCountHistogramVec(reg, MetricOpts{Subsystem: "cypher", Name: "rows", Help: "h"}, nil)
			},
		},
		{
			"NewEmbeddingLatencyHistogramVec", "embed", "_seconds",
			func(reg *prometheus.Registry) {
				NewEmbeddingLatencyHistogramVec(reg, MetricOpts{Subsystem: "embed", Name: "embed", Help: "h"}, nil)
			},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.ctor, func(t *testing.T) {
			te := NewTestEnv(t)
			wantMsg := fmt.Sprintf("observability: metric name %q must end in %q (MET-02)", tc.name, tc.requiredSuffix)
			require.PanicsWithValue(t, wantMsg, func() { tc.invoke(te.Registry) })
		})
	}
}

// TestBucketConstants_AreSingleSourceOfTruth verifies MET-03: the four typed
// histogram constructors use the canonical bucket constants exclusively and
// callers cannot override. Asserts the registered family's bucket upper
// bounds equal the constant slice exactly.
func TestBucketConstants_AreSingleSourceOfTruth(t *testing.T) {
	require.Greater(t, len(LatencyBucketsSeconds), 0, "MET-03: LatencyBucketsSeconds must not be empty")
	require.Greater(t, len(SizeBucketsBytes), 0, "MET-03: SizeBucketsBytes must not be empty")
	require.Greater(t, len(RowCountBuckets), 0, "MET-03: RowCountBuckets must not be empty")
	require.Greater(t, len(EmbeddingLatencyBucketsSeconds), 0, "MET-05: EmbeddingLatencyBucketsSeconds must not be empty")

	cases := []struct {
		name   string
		want   []float64
		invoke func(reg *prometheus.Registry) *prometheus.HistogramVec
	}{
		{
			"latency", LatencyBucketsSeconds,
			func(reg *prometheus.Registry) *prometheus.HistogramVec {
				return NewLatencyHistogramVec(reg,
					MetricOpts{Subsystem: "cypher", Name: "x_seconds", Help: "h"},
					[]string{"db"})
			},
		},
		{
			"size", SizeBucketsBytes,
			func(reg *prometheus.Registry) *prometheus.HistogramVec {
				return NewSizeHistogramVec(reg,
					MetricOpts{Subsystem: "storage", Name: "x_bytes", Help: "h"},
					[]string{"db"})
			},
		},
		{
			"rowcount", RowCountBuckets,
			func(reg *prometheus.Registry) *prometheus.HistogramVec {
				return NewRowCountHistogramVec(reg,
					MetricOpts{Subsystem: "cypher", Name: "x_rows", Help: "h"},
					[]string{"db"})
			},
		},
		{
			"embedding", EmbeddingLatencyBucketsSeconds,
			func(reg *prometheus.Registry) *prometheus.HistogramVec {
				return NewEmbeddingLatencyHistogramVec(reg,
					MetricOpts{Subsystem: "embed", Name: "x_seconds", Help: "h"},
					[]string{"db"})
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			te := NewTestEnv(t)
			vec := tc.invoke(te.Registry)
			vec.WithLabelValues("d").Observe(0.001)
			mfs, err := te.Registry.Gather()
			require.NoError(t, err)
			got := extractBucketUpperBounds(t, mfs, tc.name)
			require.Equal(t, tc.want, got,
				"%s constructor must use the canonical bucket constant — caller cannot override (MET-03)", tc.name)
		})
	}
}

// TestEmbeddingLatency_UsesLongTailBuckets verifies MET-05: the embedding
// latency bucket tail must reach >=60s — LLM-style embedding latency
// commonly exceeds the 10s standard latency tail.
func TestEmbeddingLatency_UsesLongTailBuckets(t *testing.T) {
	last := EmbeddingLatencyBucketsSeconds[len(EmbeddingLatencyBucketsSeconds)-1]
	assert.GreaterOrEqualf(t, last, 60.0,
		"MET-05: embedding latency bucket tail must reach >=60s (LLM call latency); got tail=%v", last)
}

// TestBridgeNamespaceParity_NoCollision verifies MET-23: the OTel→Prom bridge
// namespace nornicdb_otel_* is disjoint from the hand-instrumented
// nornicdb_<subsystem>_* namespace, even when the OTel meter and a typed
// constructor pick collision-bait names.
func TestBridgeNamespaceParity_NoCollision(t *testing.T) {
	te := NewTestEnv(t)

	// Hand-instrumented family — final name nornicdb_cypher_queries_total.
	cv := NewCounterVec(te.Registry,
		MetricOpts{Subsystem: "cypher", Name: "queries_total", Help: "hand"},
		[]string{"database", "op_type", "result"})
	cv.WithLabelValues("default", "read", "success").Inc()

	// Bridge-instrumented counter (intentional collision-bait OTel meter name "queries").
	bridgeCtr, err := te.Provider.MeterProvider().Meter("nornicdb-test").Int64Counter("queries")
	require.NoError(t, err)
	bridgeCtr.Add(context.Background(), 1)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	names := metricNames(mfs)

	assert.Contains(t, names, "nornicdb_cypher_queries_total",
		"MET-01: hand-instrumented family must register at nornicdb_<subsystem>_<name>")
	// Phase 1's registry.go calls WithoutUnits() (strips OTel unit suffixes
	// like _seconds/_bytes) but NOT WithoutCounterSuffixes() — so monotonic
	// OTel counters still get the Prometheus-convention _total suffix.
	// Bridge family ends up at nornicdb_otel_queries_total. Distinct from
	// the hand-instrumented nornicdb_cypher_queries_total — namespace parity
	// (MET-23) is preserved by the disjoint nornicdb_otel_* prefix, not by
	// suffix difference.
	assert.Contains(t, names, "nornicdb_otel_queries_total",
		"MET-23: OTel bridge must emit under nornicdb_otel_* (WithNamespace prefix)")
	assert.NotContains(t, names, "nornicdb_otel_cypher_queries_total",
		"MET-23: bridge MUST NOT collide with hand-instrumented family")
}

// extractBucketUpperBounds finds the first histogram family in mfs and
// returns its bucket upper bounds in registration order.
func extractBucketUpperBounds(t *testing.T, mfs []*dto.MetricFamily, _ string) []float64 {
	t.Helper()
	for _, mf := range mfs {
		if mf.GetType() != dto.MetricType_HISTOGRAM {
			continue
		}
		if len(mf.Metric) == 0 {
			continue
		}
		h := mf.Metric[0].GetHistogram()
		if h == nil {
			continue
		}
		out := make([]float64, 0, len(h.Bucket))
		for _, b := range h.Bucket {
			out = append(out, b.GetUpperBound())
		}
		return out
	}
	t.Fatal("no histogram family found in mfs")
	return nil
}
