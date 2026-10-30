package cypher

import (
	"context"
	"sync"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// spanRecorder runs one statement under a fresh tracer provider and returns
// the recorded spans. Each test keeps its own assertions on the span it names;
// this owns only the provider lifecycle the named span tests share.
func spanRecorder(t *testing.T, query string) []tracetest.SpanStub {
	t.Helper()
	exp, _ := fixtureowner.RecordSpans(t)

	exec := NewStorageExecutor(newTestMemoryEngine(t))
	_, _ = exec.Execute(context.Background(), query, nil)
	return exp.GetSpans()
}

// recordedSpan finds the span named name, reporting whether it was emitted.
func recordedSpan(spans []tracetest.SpanStub, name string) (tracetest.SpanStub, bool) {
	for _, s := range spans {
		if s.Name == name {
			return s, true
		}
	}
	return tracetest.SpanStub{}, false
}

func TestExecuteSpan_ReadQuery(t *testing.T) {
	spans := spanRecorder(t, "RETURN 1 AS x")

	s, found := recordedSpan(spans, "nornicdb.cypher.execute")
	if found {
		attrs := fixtureowner.SpanAttrs(s.Attributes)
		assert.Contains(t, attrs, "cypher.query")
		assert.Equal(t, "internal", s.SpanKind.String())
		assert.Equal(t, "read", attrs["cypher.op_type"])
	}
	assert.True(t, found, "TRC-15: nornicdb.cypher.execute span must be emitted")
}

func TestExecuteSpan_PlanSpanEmitted(t *testing.T) {
	spans := spanRecorder(t, "MATCH (n) RETURN n")

	s, found := recordedSpan(spans, "nornicdb.cypher.plan")
	if found {
		attrs := fixtureowner.SpanAttrs(s.Attributes)
		assert.Contains(t, attrs, "cypher.op_type")
		assert.Equal(t, "internal", s.SpanKind.String())
	}
	assert.True(t, found, "TRC-15: nornicdb.cypher.plan span must be emitted")
}

func TestExecuteSpan_ParseError(t *testing.T) {
	exp, _ := fixtureowner.RecordSpans(t)

	exec := NewStorageExecutor(newTestMemoryEngine(t))
	_, err := exec.Execute(context.Background(), "INVALID GARBAGE QUERY ???", nil)
	require.Error(t, err)

	spans := exp.GetSpans()
	s, found := recordedSpan(spans, "nornicdb.cypher.execute")
	if found {
		attrs := fixtureowner.SpanAttrs(s.Attributes)
		assert.Equal(t, "parse_error", attrs["cypher.op_type"])
		assert.Equal(t, "Error", s.Status.Code.String())
	}
	assert.True(t, found, "TRC-15: parse errors must mark span as Error")
}

func TestExecuteSpan_WriteQuery(t *testing.T) {
	spans := spanRecorder(t, "CREATE (n:Person {name: 'Alice'})")

	s, found := recordedSpan(spans, "nornicdb.cypher.execute")
	if found {
		attrs := fixtureowner.SpanAttrs(s.Attributes)
		assert.Equal(t, "write", attrs["cypher.op_type"])
	}
	assert.True(t, found, "TRC-15: write queries must have op_type=write")
}

func TestTracedEngine_EmitsStorageSpans(t *testing.T) {
	exp, _ := fixtureowner.RecordSpans(t)

	inner := newTestMemoryEngine(t)
	traced := storage.NewTracedEngine(inner)
	exec := NewStorageExecutor(traced)
	_, _ = exec.Execute(context.Background(), "CREATE (n:Test {x: 1})", nil)

	spans := exp.GetSpans()
	var storageSpan bool
	for _, s := range spans {
		if len(s.Name) > 16 && s.Name[:16] == "nornicdb.storage" {
			storageSpan = true
			attrs := fixtureowner.SpanAttrs(s.Attributes)
			assert.Contains(t, attrs, "kind", "TRC-17: storage spans must have kind attribute")
			break
		}
	}
	assert.True(t, storageSpan, "TRC-17: storage operation must emit a nornicdb.storage.* span")
}

func TestExecuteSpanConcurrentOptions(t *testing.T) {
	exporter, _ := fixtureowner.RecordSpans(t)
	executor := NewStorageExecutor(newTestMemoryEngine(t))
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
			executeContext, execute := executor.startExecuteSpan(parentContext, "read", "RETURN 1")
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
			assert.Equal(t, "RETURN <REDACTED>", fixtureowner.SpanAttrs(span.Attributes)["cypher.query"])
			assert.Equal(t, "read", fixtureowner.SpanAttrs(span.Attributes)["cypher.op_type"])
		}
		if span.Name == "nornicdb.cypher.exec.RETURN" {
			assert.Equal(t, "1", fixtureowner.SpanAttrs(span.Attributes)["rows"])
		}
	}
}
