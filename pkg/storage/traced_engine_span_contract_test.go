package storage

import (
	"context"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// tracedSpanRecorder installs an in-memory OTel tracer provider for the test and
// restores the previous provider on cleanup. It proves the TracedEngine facades
// emit exactly the contract spans the canonical startOperation owner defines.
func tracedSpanRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	fixtureowner.InstallTracerProvider(t, provider, nil)
	return recorder
}

// tracedSpanNamed returns the single ended span with the given name, failing the
// test when none or more than one exists.
func tracedSpanNamed(t *testing.T, spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	var found sdktrace.ReadOnlySpan
	for _, s := range spans {
		if s.Name() == name {
			require.Nil(t, found, "exactly one span named %q expected", name)
			found = s
		}
	}
	require.NotNil(t, found, "span named %q not recorded", name)
	return found
}

// TestTracedEngine_SpanContract pins the canonical span contract produced by
// TracedEngine.startOperation for every instrumented Engine method: one span
// per call, name nornicdb.storage.<op>, SpanKindInternal, `kind` attribute,
// parented by the SetContext context and ended by the time the facade returns.
func TestTracedEngine_SpanContract(t *testing.T) {
	inner := newTestMemoryEngine(t)
	traced := NewTracedEngine(inner)

	type want struct {
		name string
		kind string
	}
	// Paths that exercise every instrumented method once. Read operations run
	// before the destructive ones so their assertions see staged data.
	steps := []struct {
		label string
		want  want
		call  func(*TracedEngine)
	}{
		{"CreateNode", want{"nornicdb.storage.CreateNode", "node"},
			func(te *TracedEngine) {
				_, _ = te.CreateNode(&Node{ID: "trace:n1", Labels: []string{"L"}, Properties: map[string]any{}})
			}},
		{"GetNode", want{"nornicdb.storage.GetNode", "node"},
			func(te *TracedEngine) { _, _ = te.GetNode("trace:n1") }},
		{"UpdateNode", want{"nornicdb.storage.UpdateNode", "node"},
			func(te *TracedEngine) {
				_ = te.UpdateNode(NodeUpdateRequest{After: &Node{
					ID: "trace:n1", Labels: []string{"L"}, Properties: map[string]any{"v": int64(2)},
				}})
			}},
		{"GetNodesByLabel", want{"nornicdb.storage.GetNodesByLabel", "node"},
			func(te *TracedEngine) { _, _ = te.GetNodesByLabel("L") }},
		{"AllNodes", want{"nornicdb.storage.AllNodes", "node"},
			func(te *TracedEngine) { _, _ = te.AllNodes() }},
		{"BulkCreateNodes", want{"nornicdb.storage.BulkCreateNodes", "node"},
			func(te *TracedEngine) {
				_ = te.BulkCreateNodes([]*Node{
					{ID: "trace:n2", Labels: []string{"L"}, Properties: map[string]any{}},
					{ID: "trace:n3", Labels: []string{"L"}, Properties: map[string]any{}},
				})
			}},
		{"BatchGetNodes", want{"nornicdb.storage.BatchGetNodes", "node"},
			func(te *TracedEngine) { _, _ = te.BatchGetNodes([]NodeID{"trace:n2", "trace:n3"}) }},
		{"CreateEdge", want{"nornicdb.storage.CreateEdge", "edge"},
			func(te *TracedEngine) {
				_ = te.CreateEdge(&Edge{ID: "trace:e1", StartNode: "trace:n1", EndNode: "trace:n2", Type: "REL"})
			}},
		{"GetEdge", want{"nornicdb.storage.GetEdge", "edge"},
			func(te *TracedEngine) { _, _ = te.GetEdge("trace:e1") }},
		{"UpdateEdge", want{"nornicdb.storage.UpdateEdge", "edge"},
			func(te *TracedEngine) {
				_ = te.UpdateEdge(EdgeUpdateRequest{After: &Edge{
					ID: "trace:e1", StartNode: "trace:n1", EndNode: "trace:n2", Type: "REL",
					Properties: map[string]any{"k": "v2"},
				}})
			}},
		{"GetOutgoingEdges", want{"nornicdb.storage.GetOutgoingEdges", "edge"},
			func(te *TracedEngine) { _, _ = te.GetOutgoingEdges("trace:n1") }},
		{"GetIncomingEdges", want{"nornicdb.storage.GetIncomingEdges", "edge"},
			func(te *TracedEngine) { _, _ = te.GetIncomingEdges("trace:n2") }},
		{"GetEdgesByType", want{"nornicdb.storage.GetEdgesByType", "edge"},
			func(te *TracedEngine) { _, _ = te.GetEdgesByType("REL") }},
		{"AllEdges", want{"nornicdb.storage.AllEdges", "edge"},
			func(te *TracedEngine) { _, _ = te.AllEdges() }},
		{"BulkCreateEdges", want{"nornicdb.storage.BulkCreateEdges", "edge"},
			func(te *TracedEngine) {
				_ = te.BulkCreateEdges([]*Edge{
					{ID: "trace:e2", StartNode: "trace:n2", EndNode: "trace:n3", Type: "BULK"},
				})
			}},
		{"DeleteEdge", want{"nornicdb.storage.DeleteEdge", "edge"},
			func(te *TracedEngine) { _ = te.DeleteEdge("trace:e1") }},
		{"DeleteNode", want{"nornicdb.storage.DeleteNode", "node"},
			func(te *TracedEngine) { _ = te.DeleteNode("trace:n3") }},
	}

	for _, step := range steps {
		t.Run(step.label, func(t *testing.T) {
			recorder := tracedSpanRecorder(t)

			// Parent span created through the SDK tracer so its span context is
			// valid and can be asserted as the storage span's parent.
			parentCtx, parent := otel.Tracer("nornicdb/storage-test").Start(
				context.Background(), "storage-contract-parent")
			traced.SetContext(parentCtx)

			step.call(traced)
			parent.End()

			spans := recorder.Ended()
			require.Len(t, spans, 2, "exactly one storage span per facade call plus the parent")
			storageSpan := tracedSpanNamed(t, spans, step.want.name)

			require.Equal(t, step.want.name, storageSpan.Name(),
				"facade must keep its distinct operation span name")
			require.Equal(t, trace.SpanKindInternal, storageSpan.SpanKind(),
				"storage spans must be SpanKindInternal")
			require.Equal(t, step.want.kind, fixtureowner.SpanAttrs(storageSpan.Attributes())["kind"],
				"storage spans must carry the entity kind attribute")
			require.Equal(t, parent.SpanContext().SpanID(), storageSpan.Parent().SpanID(),
				"storage span must be parented by the SetContext context")
		})
	}
}

// TestTracedEngine_SpanEndsOnBackingPanic proves the storage span lifetime wraps
// the actual backing call: a panic escaping the inner engine still ends the span
// because the facade defers End immediately after starting it.
func TestTracedEngine_SpanEndsOnBackingPanic(t *testing.T) {
	recorder := tracedSpanRecorder(t)

	traced := &TracedEngine{Engine: panickingEngine{}, ctx: context.Background()}

	require.Panics(t, func() { _ = traced.DeleteNode("boom") })

	spans := recorder.Ended()
	require.Len(t, spans, 1, "span must be ended even though the backing call panicked")
	require.Equal(t, "nornicdb.storage.DeleteNode", spans[0].Name())
	require.Equal(t, trace.SpanKindInternal, spans[0].SpanKind())
	require.Equal(t, "node", fixtureowner.SpanAttrs(spans[0].Attributes())["kind"])
}

// panickingEngine panics from a mutation method to prove span lifetime is tied to
// the backing operation, not to error return paths.
type panickingEngine struct {
	Engine
}

func (panickingEngine) DeleteNode(NodeID, ...uint64) error { panic("backing engine panic") }
