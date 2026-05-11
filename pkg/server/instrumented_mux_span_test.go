package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// spanTracerSetup installs an in-memory SpanExporter as the global
// TracerProvider, plus a W3C propagator, and returns the exporter + a
// teardown closure. Consumers use exporter.GetSpans() post-request to
// assert span shape.
func spanTracerSetup(t *testing.T) (*tracetest.InMemoryExporter, func()) {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSyncer(exp),
	)
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	return exp, func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	}
}

// TestInstrumentedMux_Span_UsesRoutePattern covers TRC-12: the span name
// must be "nornicdb.http.<route-template>" and carry http.route +
// http.response.status_code attributes.
func TestInstrumentedMux_Span_UsesRoutePattern(t *testing.T) {
	exp, teardown := spanTracerSetup(t)
	defer teardown()

	reg := prometheus.NewRegistry()
	bag := observability.NewHTTPMetrics(reg, false)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /db/{database}/foo", func(w http.ResponseWriter, r *http.Request) {
		// Handler should see the span in its ctx (not a no-op).
		sc := trace.SpanContextFromContext(r.Context())
		assert.True(t, sc.IsValid(), "handler must receive span-bearing ctx")
		w.WriteHeader(http.StatusOK)
	})
	wrapped := instrumentedMux(mux, bag)

	req := httptest.NewRequest(http.MethodGet, "/db/mydb/foo", nil)
	rr := httptest.NewRecorder()
	wrapped.ServeHTTP(rr, req)

	spans := exp.GetSpans()
	require.Len(t, spans, 1, "exactly one HTTP span per request")
	s := spans[0]
	assert.Equal(t, "nornicdb.http.GET /db/{database}/foo", s.Name,
		"TRC-12: span name must be nornicdb.http.<route-template>")

	attrs := attrsToMap(s.Attributes)
	assert.Equal(t, "GET /db/{database}/foo", attrs["http.route"].AsString())
	assert.Equal(t, int64(http.StatusOK), attrs["http.response.status_code"].AsInt64())
	assert.Equal(t, "mydb", attrs["nornicdb.database"].AsString(),
		"database path value must be captured as a span attribute")
}

// TestInstrumentedMux_Span_UnmatchedRoute covers the 404 path: r.Pattern
// is empty so the span must fall back to the "_NOT_FOUND_" bucket. Same
// rule as the metric cardinality guard.
func TestInstrumentedMux_Span_UnmatchedRoute(t *testing.T) {
	exp, teardown := spanTracerSetup(t)
	defer teardown()

	reg := prometheus.NewRegistry()
	bag := observability.NewHTTPMetrics(reg, false)

	mux := http.NewServeMux()
	wrapped := instrumentedMux(mux, bag)

	req := httptest.NewRequest(http.MethodGet, "/totally/unknown", nil)
	rr := httptest.NewRecorder()
	wrapped.ServeHTTP(rr, req)

	spans := exp.GetSpans()
	require.Len(t, spans, 1)
	assert.Equal(t, "nornicdb.http._NOT_FOUND_", spans[0].Name,
		"unmatched routes must bucket to _NOT_FOUND_ (cardinality guard)")
}

// TestInstrumentedMux_Span_PropagatesParent covers TRC-11 + TRC-12: an
// incoming traceparent header must become the server span's parent.
func TestInstrumentedMux_Span_PropagatesParent(t *testing.T) {
	exp, teardown := spanTracerSetup(t)
	defer teardown()

	reg := prometheus.NewRegistry()
	bag := observability.NewHTTPMetrics(reg, false)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	wrapped := instrumentedMux(mux, bag)

	// Craft a valid W3C traceparent: version 00, trace-id, span-id, flags=01.
	const incomingTP = "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01"
	req := httptest.NewRequest(http.MethodGet, "/livez", nil)
	req.Header.Set("traceparent", incomingTP)
	rr := httptest.NewRecorder()
	wrapped.ServeHTTP(rr, req)

	spans := exp.GetSpans()
	require.Len(t, spans, 1)
	s := spans[0]
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", s.SpanContext.TraceID().String(),
		"TRC-11: incoming traceparent trace-id must be preserved on the server span")
	assert.Equal(t, "0102030405060708", s.Parent.SpanID().String(),
		"TRC-11: incoming traceparent span-id must become the server span's parent")
}

// TestInstrumentedMux_Span_5xxMarksError covers OTel semantic convention:
// server-error status codes must mark the span as Error.
func TestInstrumentedMux_Span_5xxMarksError(t *testing.T) {
	exp, teardown := spanTracerSetup(t)
	defer teardown()

	reg := prometheus.NewRegistry()
	bag := observability.NewHTTPMetrics(reg, false)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /boom", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	wrapped := instrumentedMux(mux, bag)

	req := httptest.NewRequest(http.MethodGet, "/boom", nil)
	rr := httptest.NewRecorder()
	wrapped.ServeHTTP(rr, req)

	spans := exp.GetSpans()
	require.Len(t, spans, 1)
	// codes.Error == 1 in OTel.
	assert.Equal(t, "Error", spans[0].Status.Code.String(),
		"TRC-12: 5xx must mark span status as Error")
}

func attrsToMap(attrs []attribute.KeyValue) map[attribute.Key]attribute.Value {
	m := make(map[attribute.Key]attribute.Value, len(attrs))
	for _, a := range attrs {
		m[a.Key] = a.Value
	}
	return m
}
