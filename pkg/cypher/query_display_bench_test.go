package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type queryDisplayBenchmarkSink struct{}

func (queryDisplayBenchmarkSink) OnStart(context.Context, sdktrace.ReadWriteSpan) {}
func (queryDisplayBenchmarkSink) OnEnd(sdktrace.ReadOnlySpan)                     {}
func (queryDisplayBenchmarkSink) Shutdown(context.Context) error                  { return nil }
func (queryDisplayBenchmarkSink) ForceFlush(context.Context) error                { return nil }

// Measure the public query path with tracing disabled and fully sampled. A sink
// without network I/O keeps exporter transport noise outside the comparison.
func BenchmarkQueryDisplay(b *testing.B) {
	previous := otel.GetTracerProvider()
	b.Cleanup(func() { otel.SetTracerProvider(previous) })
	for _, traced := range []bool{false, true} {
		name := "disabled"
		if traced {
			name = "sampled"
		}
		b.Run(name, func(b *testing.B) {
			otel.SetTracerProvider(noop.NewTracerProvider())
			if traced {
				provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSpanProcessor(observability.NewRedactingSpanProcessor(queryDisplayBenchmarkSink{})))
				otel.SetTracerProvider(provider)
				b.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
			}
			store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "query_display_bench")
			for i := 0; i < 32; i++ {
				_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("node%d", i)), Labels: []string{"DisplayBench"}, Properties: map[string]any{"name": "Ada"}})
				if err != nil {
					b.Fatal(err)
				}
			}
			executor := NewStorageExecutor(store)
			const query = "MATCH (n:DisplayBench) WHERE n.name = 'Ada' RETURN n.name LIMIT 10"
			if _, err := executor.Execute(context.Background(), query, nil); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := executor.Execute(context.Background(), query, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
