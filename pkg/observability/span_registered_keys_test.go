package observability

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestRedactingSpanProcessorRegisteredKeys(t *testing.T) {
	const key = "custom.sealed_value"
	const credential = "synthetic-registered-credential-719832"
	previous, exists := SensitiveKeys[key]
	SensitiveKeys[key] = true
	t.Cleanup(func() {
		if exists {
			SensitiveKeys[key] = previous
		} else {
			delete(SensitiveKeys, key)
		}
	})
	if !spanSensitiveNames().isSensitive(key) {
		t.Error("compatibility classifier ignored its registered key")
	}
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(NewRedactingSpanProcessor(recorder)))
	defer func() { _ = provider.Shutdown(context.Background()) }()
	_, span := provider.Tracer("test").Start(context.Background(), "registered-fields")
	span.SetAttributes(attribute.String(key, credential), attribute.Int("token_limit", 42), attribute.String("public.context", "visible"))
	span.AddEvent("registered-event", trace.WithAttributes(attribute.String("vendor.CUSTOM-SEALED-VALUE", credential)))
	span.End()
	if len(recorder.Ended()) != 1 {
		t.Fatal("span missing")
	}
	saved := recorder.Ended()[0]
	for _, attr := range saved.Attributes() {
		if strings.Contains(attr.Value.Emit(), credential) {
			t.Error("registered span attribute disclosed its value; capture withheld")
		}
		if string(attr.Key) == "token_limit" && attr.Value.AsInt64() != 42 {
			t.Error("public token budget was over-redacted")
		}
		if string(attr.Key) == "public.context" && attr.Value.AsString() != "visible" {
			t.Error("public context was over-redacted")
		}
	}
	if len(saved.Events()) != 1 || len(saved.Events()[0].Attributes) != 1 || saved.Events()[0].Attributes[0].Value.AsString() != "REDACTED" {
		t.Error("registered alias did not reach the event projection")
	}
}
