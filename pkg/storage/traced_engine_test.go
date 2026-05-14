package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTracedEngine_DelegatesAllMethods exercises every method on
// *TracedEngine to confirm the wrapper:
//
//   - delegates to the underlying engine (Unwrap returns it unchanged)
//   - propagates the parent context for span attribution (SetContext)
//   - returns identical values to the inner engine (no transformation)
//
// We use NewMemoryEngine for the inner engine so reads/writes are
// fast and deterministic. The OTel SDK is not configured in tests so
// the tracer is a no-op global, which is exactly what we want — the
// wrappers must not blow up when otel.Tracer returns the noop tracer.
func TestTracedEngine_DelegatesAllMethods(t *testing.T) {
	inner := NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })

	traced := NewTracedEngine(inner)
	require.NotNil(t, traced)
	require.Same(t, Engine(inner), traced.Unwrap(),
		"Unwrap must return the inner engine unchanged")

	// SetContext stores the value; getCtx returns it. We can't observe
	// span output without an SDK, but exercising the setter keeps the
	// branch covered and proves no panic.
	type ctxKey struct{}
	parent := context.WithValue(context.Background(), ctxKey{}, "trace-test")
	traced.SetContext(parent)
	require.Equal(t, "trace-test", traced.getCtx().Value(ctxKey{}),
		"SetContext must update the stored context for span parenting")

	// CreateNode + GetNode round-trip.
	id, err := traced.CreateNode(&Node{
		ID:         "test:n1",
		Labels:     []string{"L"},
		Properties: map[string]any{"v": int64(1)},
	})
	require.NoError(t, err)
	require.Equal(t, NodeID("test:n1"), id)

	got, err := traced.GetNode("test:n1")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, NodeID("test:n1"), got.ID)
	require.Equal(t, int64(1), got.Properties["v"])

	// UpdateNode mutates the property and round-trips.
	require.NoError(t, traced.UpdateNode(&Node{
		ID:         "test:n1",
		Labels:     []string{"L"},
		Properties: map[string]any{"v": int64(2)},
	}))
	got, err = traced.GetNode("test:n1")
	require.NoError(t, err)
	require.Equal(t, int64(2), got.Properties["v"])

	// Companion node so the edge methods have an endpoint.
	_, err = traced.CreateNode(&Node{
		ID:         "test:n2",
		Labels:     []string{"L"},
		Properties: map[string]any{},
	})
	require.NoError(t, err)

	// CreateEdge + GetEdge round-trip.
	require.NoError(t, traced.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:n1", EndNode: "test:n2",
		Type: "REL", Properties: map[string]any{"k": "v"},
	}))
	gotEdge, err := traced.GetEdge("test:e1")
	require.NoError(t, err)
	require.Equal(t, EdgeID("test:e1"), gotEdge.ID)
	require.Equal(t, "REL", gotEdge.Type)

	// UpdateEdge.
	require.NoError(t, traced.UpdateEdge(&Edge{
		ID: "test:e1", StartNode: "test:n1", EndNode: "test:n2",
		Type: "REL", Properties: map[string]any{"k": "v2"},
	}))
	gotEdge, err = traced.GetEdge("test:e1")
	require.NoError(t, err)
	require.Equal(t, "v2", gotEdge.Properties["k"])

	// Label / type / direction / All* lookups must return the staged data.
	byLabel, err := traced.GetNodesByLabel("L")
	require.NoError(t, err)
	require.Len(t, byLabel, 2)

	out, err := traced.GetOutgoingEdges("test:n1")
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, EdgeID("test:e1"), out[0].ID)

	in, err := traced.GetIncomingEdges("test:n2")
	require.NoError(t, err)
	require.Len(t, in, 1)
	require.Equal(t, EdgeID("test:e1"), in[0].ID)

	byType, err := traced.GetEdgesByType("REL")
	require.NoError(t, err)
	require.Len(t, byType, 1)

	allNodes, err := traced.AllNodes()
	require.NoError(t, err)
	require.Len(t, allNodes, 2)

	allEdges, err := traced.AllEdges()
	require.NoError(t, err)
	require.Len(t, allEdges, 1)

	// BulkCreateNodes + BulkCreateEdges + BatchGetNodes.
	require.NoError(t, traced.BulkCreateNodes([]*Node{
		{ID: "test:bulk-1", Labels: []string{"L"}, Properties: map[string]any{}},
		{ID: "test:bulk-2", Labels: []string{"L"}, Properties: map[string]any{}},
	}))
	require.NoError(t, traced.BulkCreateEdges([]*Edge{
		{ID: "test:bulk-e1", StartNode: "test:bulk-1", EndNode: "test:bulk-2",
			Type: "BULK", Properties: map[string]any{}},
	}))
	batch, err := traced.BatchGetNodes([]NodeID{"test:bulk-1", "test:bulk-2", "test:nope"})
	require.NoError(t, err)
	require.Len(t, batch, 2, "BatchGetNodes must drop missing IDs")
	require.Contains(t, batch, NodeID("test:bulk-1"))
	require.Contains(t, batch, NodeID("test:bulk-2"))
	require.NotContains(t, batch, NodeID("test:nope"))

	// DeleteEdge + DeleteNode propagate.
	require.NoError(t, traced.DeleteEdge("test:e1"))
	_, err = traced.GetEdge("test:e1")
	require.ErrorIs(t, err, ErrNotFound)

	require.NoError(t, traced.DeleteNode("test:n1"))
	_, err = traced.GetNode("test:n1")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestTracedEngine_DefaultContextIsBackground(t *testing.T) {
	inner := NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })

	traced := NewTracedEngine(inner)
	// Constructor seeds ctx with context.Background; getCtx must
	// return a non-nil background context (Err() == nil, no values).
	got := traced.getCtx()
	require.NotNil(t, got)
	require.NoError(t, got.Err(), "default ctx must not be canceled")
	require.Equal(t, context.Background(), got)
}
