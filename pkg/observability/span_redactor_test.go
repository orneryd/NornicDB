package observability

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestRedactingSpanProcessor_RedactsSensitiveKeys(t *testing.T) {
	inner := tracetest.NewSpanRecorder()
	redactor := NewRedactingSpanProcessor(inner)

	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(redactor))
	tracer := tp.Tracer("test")

	_, span := tracer.Start(context.Background(), "test-op")
	span.SetAttributes(
		attribute.String("auth.token", "secret-value"),
		attribute.String("db.password", "hunter2"),
		attribute.String("safe.key", "visible"),
		attribute.String("http.authorization", "Bearer xyz"),
	)
	span.End()

	spans := inner.Ended()
	require.Len(t, spans, 1)
	attrs := spans[0].Attributes()

	attrMap := make(map[string]string)
	for _, a := range attrs {
		attrMap[string(a.Key)] = a.Value.AsString()
	}

	assert.Equal(t, "REDACTED", attrMap["auth.token"])
	assert.Equal(t, "REDACTED", attrMap["db.password"])
	assert.Equal(t, "visible", attrMap["safe.key"])
	assert.Equal(t, "REDACTED", attrMap["http.authorization"])
}

func TestRedactingSpanProcessor_SubstringMatch(t *testing.T) {
	inner := tracetest.NewSpanRecorder()
	redactor := NewRedactingSpanProcessor(inner)

	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(redactor))
	tracer := tp.Tracer("test")

	_, span := tracer.Start(context.Background(), "test-op")
	span.SetAttributes(
		attribute.String("my.custom.password.field", "leak"),
		attribute.String("x-api_key-header", "leak2"),
		attribute.String("unrelated", "ok"),
	)
	span.End()

	spans := inner.Ended()
	require.Len(t, spans, 1)
	attrs := spans[0].Attributes()

	attrMap := make(map[string]string)
	for _, a := range attrs {
		attrMap[string(a.Key)] = a.Value.AsString()
	}

	assert.Equal(t, "REDACTED", attrMap["my.custom.password.field"])
	assert.Equal(t, "REDACTED", attrMap["x-api_key-header"])
	assert.Equal(t, "ok", attrMap["unrelated"])
}

func TestRedactingSpanProcessor_CaseInsensitive(t *testing.T) {
	inner := tracetest.NewSpanRecorder()
	redactor := NewRedactingSpanProcessor(inner)

	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(redactor))
	tracer := tp.Tracer("test")

	_, span := tracer.Start(context.Background(), "test-op")
	span.SetAttributes(
		attribute.String("AUTH.TOKEN", "secret"),
		attribute.String("Password", "secret2"),
		attribute.String("API_KEY", "secret3"),
	)
	span.End()

	spans := inner.Ended()
	require.Len(t, spans, 1)
	attrs := spans[0].Attributes()

	for _, a := range attrs {
		assert.Equal(t, "REDACTED", a.Value.AsString(),
			"key %q should be redacted (case-insensitive)", string(a.Key))
	}
}

func TestRedactingSpanProcessor_PreservesNonSensitive(t *testing.T) {
	inner := tracetest.NewSpanRecorder()
	redactor := NewRedactingSpanProcessor(inner)

	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(redactor))
	tracer := tp.Tracer("test")

	_, span := tracer.Start(context.Background(), "test-op")
	span.SetAttributes(
		attribute.String("http.method", "GET"),
		attribute.Int64("http.status_code", 200),
		attribute.String("peer", "node-2"),
	)
	span.End()

	spans := inner.Ended()
	require.Len(t, spans, 1)
	attrs := spans[0].Attributes()

	attrMap := make(map[string]interface{})
	for _, a := range attrs {
		attrMap[string(a.Key)] = a.Value.AsInterface()
	}

	assert.Equal(t, "GET", attrMap["http.method"])
	assert.Equal(t, int64(200), attrMap["http.status_code"])
	assert.Equal(t, "node-2", attrMap["peer"])
}

func TestRedactingSpanProcessor_ShutdownDelegates(t *testing.T) {
	inner := tracetest.NewSpanRecorder()
	redactor := NewRedactingSpanProcessor(inner)
	err := redactor.Shutdown(context.Background())
	assert.NoError(t, err)
}

func TestRedactingSpanProcessor_ForceFlushDelegates(t *testing.T) {
	inner := tracetest.NewSpanRecorder()
	redactor := NewRedactingSpanProcessor(inner)
	err := redactor.ForceFlush(context.Background())
	assert.NoError(t, err)
}
