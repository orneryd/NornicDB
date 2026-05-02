package observability

import (
	"context"
	"testing"

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
