package observability

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestListener_OpenMetricsContentTypeRoundTrip verifies MET-24: a request
// with `Accept: application/openmetrics-text` round-trips through the
// listener and returns a Content-Type prefix of `application/openmetrics-text`
// (the version+charset suffix may vary; HasPrefix is the stable assertion).
func TestListener_OpenMetricsContentTypeRoundTrip(t *testing.T) {
	env := NewTestEnv(t)
	l, _ := newTestListener(t, env)
	t.Cleanup(func() { _ = l.Shutdown(context.Background()) })

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Accept", "application/openmetrics-text")
	l.srv.Handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	ct := rec.Header().Get("Content-Type")
	require.Truef(t, strings.HasPrefix(ct, "application/openmetrics-text"),
		"MET-24: Accept: application/openmetrics-text must yield Content-Type prefix application/openmetrics-text; got %q", ct)
}

// TestListener_OpenMetricsExemplarPresent verifies that after observing a
// histogram with a valid (sampled) span context, the OpenMetrics exposition
// of /metrics carries a `trace_id="…"` exemplar label. Exemplars are only
// serialized in OpenMetrics format — `text/plain;version=0.0.4` drops them.
func TestListener_OpenMetricsExemplarPresent(t *testing.T) {
	env := NewTestEnv(t)
	l, _ := newTestListener(t, env)
	t.Cleanup(func() { _ = l.Shutdown(context.Background()) })

	vec := NewLatencyHistogramVec(env.Registry,
		MetricOpts{Subsystem: "cypher", Name: "x_seconds", Help: "h"},
		[]string{"database", "op_type"})
	h := &LatencyHistogram{vec: vec}
	bound := h.Bind("db1", "read")

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(tracetest.NewInMemoryExporter())),
	)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("real").Start(context.Background(), "op")
	bound.Observe(ctx, 0.001)
	span.End()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Accept", "application/openmetrics-text")
	l.srv.Handler.ServeHTTP(rec, req)

	body, _ := io.ReadAll(rec.Body)
	assert.Contains(t, string(body), `trace_id="`,
		"MET-24: OpenMetrics-format response must include trace_id exemplar label after observation with valid span context")
}

// TestListener_EnableOpenMetricsLineExists is a falsifiability gate against
// regression of the Phase-1-shipped `EnableOpenMetrics: true` line in
// listener.go (RESEARCH §2 Key Finding #1). If a future PR removes it, the
// MET-24 round-trip silently falls back to text/plain;version=0.0.4 and
// exemplars are dropped — this source-grep self-test catches that.
func TestListener_EnableOpenMetricsLineExists(t *testing.T) {
	src, err := os.ReadFile("listener.go")
	require.NoError(t, err)
	assert.Contains(t, string(src), "EnableOpenMetrics: true",
		"RESEARCH §2 Key Finding #1: Phase 1 shipped this line at lines 60-66; Plan 03-04 verifies it persists. If absent, MET-24 round-trip fails.")
}
