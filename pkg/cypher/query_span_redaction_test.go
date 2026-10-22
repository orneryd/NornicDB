package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// The producer projects before truncation and before any processor/exporter can
// observe a query; the generic field redactor never needs a second Cypher policy.
func TestQuerySpanRedactsBeforeTruncation(t *testing.T) {
	const credential = "synthetic-query-span-credential-62487391"
	previous := otel.GetTracerProvider()
	defer otel.SetTracerProvider(previous)
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	for _, wrap := range []bool{false, true} {
		recorder := tracetest.NewSpanRecorder()
		var processor sdktrace.SpanProcessor = recorder
		if wrap {
			processor = observability.NewRedactingSpanProcessor(processor)
		}
		provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
		otel.SetTracerProvider(provider)
		for _, tc := range []struct{ query, want string }{
			{"RETURN '" + credential + "' AS result", "RETURN <REDACTED> AS result"},
			{"RETURN '" + credential + strings.Repeat("p", 600) + "' AS result", "RETURN <REDACTED> AS result"},
			{"RETURN $value AS result", "RETURN $value AS result"},
		} {
			_, span := executor.startExecuteSpan(context.Background(), "read", tc.query)
			span.End()
			ended := recorder.Ended()
			found := false
			for _, attr := range ended[len(ended)-1].Attributes() {
				if string(attr.Key) != "cypher.query" {
					continue
				}
				found = true
				if attr.Value.AsString() != tc.want || strings.Contains(attr.Value.AsString(), credential) {
					t.Error("query exporter bypassed literal redaction; output withheld")
				}
			}
			if !found {
				t.Error("query span attribute missing")
			}
		}
		_ = provider.Shutdown(context.Background())
	}
}
