package bolt

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestStartSessionSpan(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(nil)

	ctx, span := startSessionSpan("127.0.0.1:54321")
	require.NotNil(t, ctx)
	require.NotNil(t, span)
	span.End()

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, "nornicdb.bolt.session", spans[0].Name())
}

func TestStartMessageSpan_NilCtx(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(nil)

	ctx, span := startMessageSpan(nil, "run")
	require.NotNil(t, ctx)
	require.NotNil(t, span)
	span.End()

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, "nornicdb.bolt.run", spans[0].Name())
}

func TestExtractTraceparent(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(nil)

	// Create a parent span and extract its traceparent header value.
	parentCtx, parentSpan := tp.Tracer("test").Start(context.Background(), "parent")
	parentSpan.End()
	parentSC := parentSpan.SpanContext()

	// Inject parent's traceparent into a carrier.
	carrier := propagation.MapCarrier{}
	prop := propagation.TraceContext{}
	prop.Inject(parentCtx, carrier)
	traceparent := carrier.Get("traceparent")
	require.NotEmpty(t, traceparent)

	// Now simulate a Bolt metadata containing nornicdb.traceparent.
	metadata := map[string]any{
		"nornicdb.traceparent": traceparent,
	}

	extracted := extractTraceparent(context.Background(), metadata)

	// Verify the trace context was propagated by re-injecting and comparing trace IDs.
	outCarrier := propagation.MapCarrier{}
	prop.Inject(extracted, outCarrier)
	outTP := outCarrier.Get("traceparent")
	require.NotEmpty(t, outTP, "extracted context must produce a traceparent on inject")

	// Parse the trace ID from both traceparents.
	parentParts := splitTraceparent(traceparent)
	extractedParts := splitTraceparent(outTP)
	require.Len(t, parentParts, 4)
	require.Len(t, extractedParts, 4)
	assert.Equal(t, parentParts[1], extractedParts[1],
		"extracted context must carry the parent's trace ID")
	_ = parentSC
}

func TestExtractTraceparent_NilMetadata(t *testing.T) {
	ctx := extractTraceparent(context.Background(), nil)
	assert.NotNil(t, ctx)
}

func TestExtractTraceparent_NilCtx(t *testing.T) {
	ctx := extractTraceparent(nil, map[string]any{"nornicdb.traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"})
	assert.NotNil(t, ctx)
}

func TestExtractTraceparent_NoKey(t *testing.T) {
	base := context.Background()
	ctx := extractTraceparent(base, map[string]any{"other": "value"})
	assert.Equal(t, base, ctx)
}

func splitTraceparent(tp string) []string {
	var parts []string
	start := 0
	for i := range tp {
		if tp[i] == '-' {
			parts = append(parts, tp[start:i])
			start = i + 1
		}
	}
	parts = append(parts, tp[start:])
	return parts
}
