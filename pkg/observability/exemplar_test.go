package observability

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestExemplarEmission_OnlyWhenSpanValid verifies MET-24 / D-02a: the
// LatencyHistogram wrapper attaches an exemplar only when
// trace.SpanContextFromContext(ctx).IsValid() returns true. Four cases
// exercise every branch of the IsValid()→ExemplarObserver chokepoint.
func TestExemplarEmission_OnlyWhenSpanValid(t *testing.T) {
	cases := []struct {
		name         string
		buildCtx     func(t *testing.T) context.Context
		wantExemplar bool
	}{
		{"no-context", func(*testing.T) context.Context { return context.Background() }, false},
		{"ctx-no-span", func(*testing.T) context.Context { return context.Background() }, false},
		{"ctx-with-noop-span", func(t *testing.T) context.Context {
			tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()))
			t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
			ctx, span := tp.Tracer("noop").Start(context.Background(), "op")
			t.Cleanup(func() { span.End() })
			return ctx
		}, false},
		{"ctx-with-real-span", func(t *testing.T) context.Context {
			exp := tracetest.NewInMemoryExporter()
			tp := sdktrace.NewTracerProvider(
				sdktrace.WithSampler(sdktrace.AlwaysSample()),
				sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exp)),
			)
			t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
			ctx, span := tp.Tracer("real").Start(context.Background(), "op")
			t.Cleanup(func() { span.End() })
			return ctx
		}, true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			te := NewTestEnv(t)
			vec := NewLatencyHistogramVec(te.Registry,
				MetricOpts{Subsystem: "cypher", Name: "x_seconds", Help: "h"},
				[]string{"database", "op_type"})
			h := &LatencyHistogram{vec: vec}
			bound := h.Bind("db1", "read")

			ctx := tc.buildCtx(t)
			bound.Observe(ctx, 0.001)

			mfs, err := te.Registry.Gather()
			require.NoError(t, err)
			got := findAnyExemplar(t, mfs)
			if tc.wantExemplar {
				require.NotNil(t, got, "expected exemplar attached to at least one bucket (IsValid()=true)")
				tid := labelValue(got, "trace_id")
				sid := labelValue(got, "span_id")
				assert.Len(t, tid, 32, "trace_id must be 32 hex chars (W3C TraceID)")
				assert.Len(t, sid, 16, "span_id must be 16 hex chars (W3C SpanID)")
			} else {
				assert.Nil(t, got, "no exemplar must be attached when SpanContext.IsValid()=false (D-02a)")
			}
		})
	}
}

// findAnyExemplar walks every histogram family + bucket and returns the
// first non-nil exemplar encountered, or nil if none is attached.
func findAnyExemplar(t *testing.T, mfs []*dto.MetricFamily) *dto.Exemplar {
	t.Helper()
	for _, mf := range mfs {
		if mf.GetType() != dto.MetricType_HISTOGRAM {
			continue
		}
		for _, m := range mf.Metric {
			for _, b := range m.GetHistogram().GetBucket() {
				if ex := b.GetExemplar(); ex != nil {
					return ex
				}
			}
		}
	}
	return nil
}

// labelValue extracts the named label from an exemplar's label set, or
// returns "" if absent.
func labelValue(ex *dto.Exemplar, name string) string {
	for _, lp := range ex.GetLabel() {
		if lp.GetName() == name {
			return lp.GetValue()
		}
	}
	return ""
}

// TestHistogramWrappers_AllFamilies exercises all four wrapper families
// (Latency / Size / RowCount / EmbeddingLatency) through their full surface:
// New*Histogram constructor, Vec() accessor, cold-path Observe(ctx, lvs, val),
// and Bind(...).Observe(ctx, val) hot path. Asserts each wrapper's *Vec
// registers under nornicdb_<subsystem>_<name> and that Observe/Bind both
// produce a sample on the wrapped *prometheus.HistogramVec.
//
// Coverage rationale (Plan 03-04 Rule 2): Plan 03-03 shipped four wrapper
// families but `TestExemplarEmission_OnlyWhenSpanValid` only drives
// LatencyHistogram. Without this test the Size/RowCount/EmbeddingLatency
// constructor + Vec + Observe + Bind paths were 0% covered, dragging the
// pkg/observability total below the PERF-05 ≥90% gate.
func TestHistogramWrappers_AllFamilies(t *testing.T) {
	type family struct {
		name        string
		fullName    string
		observe     func(reg *prometheus.Registry, ctx context.Context)
		bindObserve func(reg *prometheus.Registry, ctx context.Context)
	}

	cases := []family{
		{
			name:     "LatencyHistogram",
			fullName: "nornicdb_cypher_latency_seconds",
			observe: func(reg *prometheus.Registry, ctx context.Context) {
				h := NewLatencyHistogram(reg,
					MetricOpts{Subsystem: "cypher", Name: "latency_seconds", Help: "h"},
					[]string{"database", "op_type"})
				require.NotNil(t, h.Vec(), "Vec() accessor must expose raw *HistogramVec for cardinality assertions")
				h.Observe(ctx, []string{"db1", "read"}, 0.001)
			},
			bindObserve: func(reg *prometheus.Registry, ctx context.Context) {
				h := NewLatencyHistogram(reg,
					MetricOpts{Subsystem: "cypher", Name: "latency_seconds", Help: "h"},
					[]string{"database", "op_type"})
				bound := h.Bind("db1", "read")
				bound.Observe(ctx, 0.002)
			},
		},
		{
			name:     "SizeHistogram",
			fullName: "nornicdb_storage_payload_bytes",
			observe: func(reg *prometheus.Registry, ctx context.Context) {
				h := NewSizeHistogram(reg,
					MetricOpts{Subsystem: "storage", Name: "payload_bytes", Help: "h"},
					[]string{"database", "op"})
				require.NotNil(t, h.Vec())
				h.Observe(ctx, []string{"db1", "put"}, 1024)
			},
			bindObserve: func(reg *prometheus.Registry, ctx context.Context) {
				h := NewSizeHistogram(reg,
					MetricOpts{Subsystem: "storage", Name: "payload_bytes", Help: "h"},
					[]string{"database", "op"})
				bound := h.Bind("db1", "put")
				bound.Observe(ctx, 4096)
			},
		},
		{
			name:     "RowCountHistogram",
			fullName: "nornicdb_cypher_result_rows",
			observe: func(reg *prometheus.Registry, ctx context.Context) {
				h := NewRowCountHistogram(reg,
					MetricOpts{Subsystem: "cypher", Name: "result_rows", Help: "h"},
					[]string{"database"})
				require.NotNil(t, h.Vec())
				h.Observe(ctx, []string{"db1"}, 42)
			},
			bindObserve: func(reg *prometheus.Registry, ctx context.Context) {
				h := NewRowCountHistogram(reg,
					MetricOpts{Subsystem: "cypher", Name: "result_rows", Help: "h"},
					[]string{"database"})
				bound := h.Bind("db1")
				bound.Observe(ctx, 100)
			},
		},
		{
			name:     "EmbeddingLatencyHistogram",
			fullName: "nornicdb_embed_inference_seconds",
			observe: func(reg *prometheus.Registry, ctx context.Context) {
				h := NewEmbeddingLatencyHistogram(reg,
					MetricOpts{Subsystem: "embed", Name: "inference_seconds", Help: "h"},
					[]string{"backend", "model"})
				require.NotNil(t, h.Vec())
				h.Observe(ctx, []string{"cpu", "bge-small"}, 0.150)
			},
			bindObserve: func(reg *prometheus.Registry, ctx context.Context) {
				h := NewEmbeddingLatencyHistogram(reg,
					MetricOpts{Subsystem: "embed", Name: "inference_seconds", Help: "h"},
					[]string{"backend", "model"})
				bound := h.Bind("cpu", "bge-small")
				bound.Observe(ctx, 0.300)
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name+"/Observe", func(t *testing.T) {
			te := NewTestEnv(t)
			tc.observe(te.Registry, context.Background())
			got, err := testutil.GatherAndCount(te.Registry, tc.fullName)
			require.NoError(t, err)
			assert.Equalf(t, 1, got, "Observe must produce exactly one series for %s", tc.fullName)
		})
		t.Run(tc.name+"/Bind", func(t *testing.T) {
			te := NewTestEnv(t)
			tc.bindObserve(te.Registry, context.Background())
			got, err := testutil.GatherAndCount(te.Registry, tc.fullName)
			require.NoError(t, err)
			assert.Equalf(t, 1, got, "Bind().Observe must produce exactly one series for %s", tc.fullName)
		})
	}
}
