package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestExecuteNonRecordingSpanSkipsQueryProjection(t *testing.T) {
	previous := otel.GetTracerProvider()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		require.NoError(t, provider.Shutdown(context.Background()))
	})
	calls := countRedactGrammarParses(t)
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	result, err := executor.Execute(context.Background(), "RETURN 'literal remains data' AS value", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"literal remains data"}}, result.Rows)
	require.Zero(t, *calls, "a sampled-out span has no query attribute to project")
}

func TestExecuteRecordingSpanExportsProjectedQuery(t *testing.T) {
	previous := otel.GetTracerProvider()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		require.NoError(t, provider.Shutdown(context.Background()))
	})
	const literal = "synthetic-execute-span-secret-95327"
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	result, err := executor.Execute(context.Background(), "RETURN '"+literal+"' AS value", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{literal}}, result.Rows)
	var executeSpans int
	for _, span := range recorder.Ended() {
		if span.Name() != "nornicdb.cypher.execute" {
			continue
		}
		executeSpans++
		attributes := make(map[string]string)
		for _, attr := range span.Attributes() {
			attributes[string(attr.Key)] = attr.Value.Emit()
			require.NotContains(t, attr.Value.Emit(), literal, "the span must never export the original literal")
		}
		require.Equal(t, "read", attributes["cypher.op_type"])
		require.Equal(t, "RETURN <REDACTED> AS value", attributes["cypher.query"])
	}
	require.Equal(t, 1, executeSpans)
}

// BenchmarkSelectedPathEmptyGraphColdExecutor measures the public Execute path
// with a fresh executor/cache over the same empty graph for every operation.
// Constructor cost is included; fixture storage creation is outside timing.
func BenchmarkSelectedPathEmptyGraphColdExecutor(b *testing.B) {
	previous := otel.GetTracerProvider()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()))
	otel.SetTracerProvider(provider)
	b.Cleanup(func() {
		otel.SetTracerProvider(previous)
		_ = provider.Shutdown(context.Background())
	})
	store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "counted_benchmark")
	for _, bound := range []int{100000, 1000000} {
		b.Run(fmt.Sprint(bound), func(b *testing.B) {
			query := fmt.Sprintf("MATCH p = ANY SHORTEST (a)(()-[:T]->()){1,%d}(b) RETURN count(p)", bound)
			b.ReportAllocs()
			for b.Loop() {
				executor := NewStorageExecutor(store)
				result, err := executor.Execute(context.Background(), query, nil)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != int64(0) {
					b.Fatalf("unexpected empty graph count: %v", result.Rows)
				}
			}
		})
	}
}
