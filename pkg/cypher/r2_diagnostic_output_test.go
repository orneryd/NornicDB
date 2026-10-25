package cypher

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func r2DiagnosticCanary(t *testing.T) string {
	t.Helper()
	var value [18]byte
	if _, err := rand.Read(value[:]); err != nil {
		t.Fatal("canary generation failed")
	}
	return hex.EncodeToString(value[:])
}

func TestR2QueryStatisticsLiteralProjection(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
	ctx := context.Background()
	first, second := r2DiagnosticCanary(t), r2DiagnosticCanary(t)
	queries := []string{
		"CREATE (n:R2Credential {password: '" + first + "'})",
		"CREATE (n:R2Credential {password: '" + second + "'})",
	}
	for i, query := range queries {
		for invocation := 0; invocation <= i; invocation++ {
			if _, err := e.Execute(ctx, query, nil); err != nil {
				t.Fatal("credential statement failed; details withheld")
			}
		}
	}
	// Distinct statements must remain distinct collector entries even when
	// their displayed projections are identical. Retrieval cannot reset them.
	for attempt := 0; attempt < 2; attempt++ {
		result, err := e.Execute(ctx, "CALL db.stats.retrieve('QUERIES')", nil)
		if err != nil || len(result.Rows) != 2 {
			t.Fatal("query statistics missing")
		}
		raw, err := json.Marshal(result.Rows)
		if err != nil || strings.Contains(string(raw), first) || strings.Contains(string(raw), second) {
			t.Fatal("saved query statistics disclosed a literal; capture withheld")
		}
		for i, row := range result.Rows {
			data := row[1].(map[string]interface{})
			if data["query"] != RedactLiterals(queries[i]) {
				t.Error("statistics did not use the existing query display policy")
			}
			summary := data["invocationSummary"].(map[string]interface{})
			if summary["invocationCount"] != int64(i+1) || len(data["invocations"].([]interface{})) != i+1 {
				t.Error("display projection changed invocation accounting")
			}
			record := e.queryStatistics.queries[queries[i]]
			timing := summary["executionTimeInUs"].(map[string]interface{})
			if timing["min"] != record.minimum || timing["max"] != record.maximum || timing["avg"] != record.total/record.count {
				t.Error("display projection changed recorded timing")
			}
		}
	}
}

// Adapted from the independent r2 real-expression regression. EXPLAIN/PROFILE
// results are deliberately outside the accepted diagnostic-output contract.
func TestR2ErrorOutputs(t *testing.T) {
	for _, wrapped := range []bool{false, true} {
		name := "plain-processor"
		if wrapped {
			name = "redacting-processor"
		}
		t.Run(name, func(t *testing.T) {
			canary := r2DiagnosticCanary(t)
			recorder := tracetest.NewSpanRecorder()
			var processor sdktrace.SpanProcessor = recorder
			if wrapped {
				processor = observability.NewRedactingSpanProcessor(processor)
			}
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
			previous := otel.GetTracerProvider()
			otel.SetTracerProvider(provider)
			defer func() { _ = provider.Shutdown(context.Background()); otel.SetTracerProvider(previous) }()
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
			for _, expression := range []string{"date", "duration"} {
				_, err := e.Execute(context.Background(), "RETURN "+expression+"('"+canary+"')", nil)
				var semantic *classifiedCypherError
				if !errors.As(err, &semantic) || semantic.code != "Neo.ClientError.Statement.SyntaxError" || semantic.detail != "InvalidArgument" {
					t.Fatal("expression error lost its type or classification; details withheld")
				}
				if !strings.Contains(err.Error(), canary) {
					t.Error("trace projection changed the caller's original error")
				}
				if !strings.Contains(err.Error(), "Text cannot be parsed to a") {
					t.Error("expression diagnostic lost its useful reason")
				}
			}
			statuses, exceptions := 0, 0
			for _, span := range recorder.Ended() {
				if span.Status().Code == codes.Error {
					statuses++
					if !strings.Contains(span.Status().Description, "Text cannot be parsed to a") {
						t.Error("span error lost its useful reason")
					}
				}
				if strings.Contains(span.Status().Description, canary) {
					t.Error("span status disclosed a literal; capture withheld")
				}
				for _, event := range span.Events() {
					for _, attr := range event.Attributes {
						if strings.Contains(attr.Value.Emit(), canary) {
							t.Error("span event disclosed a literal; capture withheld")
						}
						if string(attr.Key) == "exception.type" {
							exceptions++
							if attr.Value.AsString() != "*cypher.classifiedCypherError" {
								t.Error("span exception type changed")
							}
						}
					}
				}
			}
			if statuses != 2 || exceptions != 2 {
				t.Fatal("error status or exception event missing")
			}
			result, err := e.Execute(context.Background(), "RETURN 'ordinary-value' AS value, 123 AS number", nil)
			if err != nil || len(result.Rows) != 1 || result.Rows[0][0] != "ordinary-value" {
				t.Fatal("legitimate query result changed")
			}
		})
	}
}

type r2SpanValueError string

func (err r2SpanValueError) Error() string { return string(err) }

func TestR2SpanErrorSDKContract(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()
	tracer := provider.Tracer("test")
	for _, err := range []error{
		errors.New("context deadline exceeded"),
		r2SpanValueError("cannot complete this operation"),
		newSemanticError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType", "expected a number"),
	} {
		_, reference := tracer.Start(context.Background(), "sdk-reference")
		reference.RecordError(err)
		reference.End()
		_, projected := tracer.Start(context.Background(), "projected")
		recordSpanError(projected, err)
		projected.End()
		spans := recorder.Ended()
		before, after := spans[len(spans)-2].Events(), spans[len(spans)-1].Events()
		if len(before) != 1 || len(after) != 1 || before[0].Name != after[0].Name || len(before[0].Attributes) != len(after[0].Attributes) {
			t.Fatal("exception projection changed the SDK event contract")
		}
		for i, attr := range before[0].Attributes {
			if attr != after[0].Attributes[i] {
				t.Error("exception projection changed a public reason or original error type")
			}
		}
	}
}
