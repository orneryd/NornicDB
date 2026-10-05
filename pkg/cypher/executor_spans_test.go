package cypher

import (
	"context"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func spanSetup(t *testing.T) (*tracetest.InMemoryExporter, func()) {
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

func TestExecuteSpan_ReadQuery(t *testing.T) {
	exp, teardown := spanSetup(t)
	defer teardown()

	exec := NewStorageExecutor(storage.NewMemoryEngine())
	_, _ = exec.Execute(context.Background(), "RETURN 1 AS x", nil)

	spans := exp.GetSpans()
	var found bool
	for _, s := range spans {
		if s.Name == "nornicdb.cypher.execute" {
			found = true
			attrs := spanAttrs(s)
			assert.Contains(t, attrs, "cypher.query")
			assert.Equal(t, "internal", s.SpanKind.String())
			assert.Equal(t, "read", attrs["cypher.op_type"])
			break
		}
	}
	assert.True(t, found, "TRC-15: nornicdb.cypher.execute span must be emitted")
}

func TestExecuteSpan_PlanSpanEmitted(t *testing.T) {
	exp, teardown := spanSetup(t)
	defer teardown()

	exec := NewStorageExecutor(storage.NewMemoryEngine())
	_, _ = exec.Execute(context.Background(), "MATCH (n) RETURN n", nil)

	spans := exp.GetSpans()
	var found bool
	for _, s := range spans {
		if s.Name == "nornicdb.cypher.plan" {
			found = true
			attrs := spanAttrs(s)
			assert.Contains(t, attrs, "cypher.op_type")
			assert.Equal(t, "internal", s.SpanKind.String())
			break
		}
	}
	assert.True(t, found, "TRC-15: nornicdb.cypher.plan span must be emitted")
}

func TestExecuteSpan_ParseError(t *testing.T) {
	exp, teardown := spanSetup(t)
	defer teardown()

	exec := NewStorageExecutor(storage.NewMemoryEngine())
	_, err := exec.Execute(context.Background(), "INVALID GARBAGE QUERY ???", nil)
	require.Error(t, err)

	spans := exp.GetSpans()
	var found bool
	for _, s := range spans {
		if s.Name == "nornicdb.cypher.execute" {
			found = true
			attrs := spanAttrs(s)
			assert.Equal(t, "parse_error", attrs["cypher.op_type"])
			assert.Equal(t, "Error", s.Status.Code.String())
			break
		}
	}
	assert.True(t, found, "TRC-15: parse errors must mark span as Error")
}

func TestExecuteSpan_WriteQuery(t *testing.T) {
	exp, teardown := spanSetup(t)
	defer teardown()

	exec := NewStorageExecutor(storage.NewMemoryEngine())
	_, _ = exec.Execute(context.Background(), "CREATE (n:Person {name: 'Alice'})", nil)

	spans := exp.GetSpans()
	var found bool
	for _, s := range spans {
		if s.Name == "nornicdb.cypher.execute" {
			found = true
			attrs := spanAttrs(s)
			assert.Equal(t, "write", attrs["cypher.op_type"])
			break
		}
	}
	assert.True(t, found, "TRC-15: write queries must have op_type=write")
}

func TestTracedEngine_EmitsStorageSpans(t *testing.T) {
	exp, teardown := spanSetup(t)
	defer teardown()

	inner := storage.NewMemoryEngine()
	traced := storage.NewTracedEngine(inner)
	exec := NewStorageExecutor(traced)
	_, _ = exec.Execute(context.Background(), "CREATE (n:Test {x: 1})", nil)

	spans := exp.GetSpans()
	var storageSpan bool
	for _, s := range spans {
		if len(s.Name) > 16 && s.Name[:16] == "nornicdb.storage" {
			storageSpan = true
			attrs := spanAttrs(s)
			assert.Contains(t, attrs, "kind", "TRC-17: storage spans must have kind attribute")
			break
		}
	}
	assert.True(t, storageSpan, "TRC-17: storage operation must emit a nornicdb.storage.* span")
}

func TestExecuteSpanConcurrentOptions(t *testing.T) {
	exporter, teardown := spanSetup(t)
	defer teardown()
	_, unrelated := otel.Tracer("background").Start(context.Background(), "unrelated")
	unrelated.End()
	const workers = 32
	var traceIDs [workers]string
	var workersDone sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		workersDone.Add(1)
		go func() {
			defer workersDone.Done()
			parentContext, parent := otel.Tracer("test").Start(context.Background(), "parent")
			traceIDs[worker] = parent.SpanContext().TraceID().String()
			executeContext, execute := startExecuteSpan(parentContext, "read", "RETURN 1")
			planContext, plan := startPlanSpan(executeContext)
			_, operator := startOperatorSpan(planContext, &PlanOperator{OperatorType: "RETURN"})
			assert.Equal(t, parent.SpanContext().TraceID(), execute.SpanContext().TraceID())
			assert.Equal(t, execute.SpanContext().TraceID(), plan.SpanContext().TraceID())
			assert.Equal(t, plan.SpanContext().TraceID(), operator.SpanContext().TraceID())
			endOperatorSpan(operator, 1)
			plan.End()
			execute.End()
			parent.End()
		}()
	}
	workersDone.Wait()
	traceCounts := make(map[string]int, workers)
	for _, traceID := range traceIDs {
		traceCounts[traceID] = 0
	}
	require.Len(t, traceCounts, workers)
	spans := make([]tracetest.SpanStub, 0, workers*4)
	for _, span := range exporter.GetSpans() {
		traceID := span.SpanContext.TraceID().String()
		if _, owned := traceCounts[traceID]; owned {
			traceCounts[traceID]++
			spans = append(spans, span)
		}
	}
	for _, count := range traceCounts {
		require.Equal(t, 4, count)
	}
	require.Len(t, spans, workers*4)
	for _, span := range spans {
		assert.Equal(t, "internal", span.SpanKind.String())
		if span.Name == "nornicdb.cypher.execute" {
			assert.Equal(t, "RETURN 1", spanAttrs(span)["cypher.query"])
			assert.Equal(t, "read", spanAttrs(span)["cypher.op_type"])
		}
		if span.Name == "nornicdb.cypher.exec.RETURN" {
			assert.Equal(t, "1", spanAttrs(span)["rows"])
		}
	}
}

func spanAttrs(s tracetest.SpanStub) map[string]string {
	m := make(map[string]string, len(s.Attributes))
	for _, a := range s.Attributes {
		m[string(a.Key)] = a.Value.Emit()
	}
	return m
}
