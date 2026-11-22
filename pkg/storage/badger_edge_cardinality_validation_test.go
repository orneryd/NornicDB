package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCardinalityValidationReadRefreshesAfterPublication(t *testing.T) {
	engine := newTestEngine(t)
	ctx := t.Context()
	for _, id := range []NodeID{"test:a", "test:b", "test:c"} {
		_, err := engine.CreateNode(ctx, &Node{ID: id})
		require.NoError(t, err)
	}
	tx, err := engine.BeginTransaction(ctx, OrdinaryBegin)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	first := &Edge{ID: "test:first", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}
	second := &Edge{ID: "test:second", StartNode: "test:a", EndNode: "test:c", Type: "LINK"}
	tx.pendingEdges[first.ID], tx.pendingEdges[second.ID] = first, second
	c := Constraint{Type: ConstraintCardinality, Label: "LINK", Direction: "OUTGOING", MaxCount: 2}

	var read cardinalityValidationRead
	require.NoError(t, tx.validateEdgeCardinality(ctx, first, c, "test", &read))
	require.Len(t, read, 1)
	require.NoError(t, tx.validateEdgeCardinality(ctx, second, c, "test", &read))

	other := &Edge{ID: "test:other", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}
	require.NoError(t, engine.CreateEdge(ctx, other))
	err = tx.validateEdgeCardinality(ctx, second, c, "test", &read)
	var violation *ConstraintViolationError
	require.ErrorAs(t, err, &violation)
	require.Equal(t, ConstraintCardinality, violation.Type)
	require.EqualError(t, err, tx.checkEdgeCardinality(ctx, second, c, "test").Error())

	require.NoError(t, engine.DeleteEdge(ctx, other.ID))
	require.NoError(t, tx.validateEdgeCardinality(ctx, first, c, "test", &read))
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, tx.validateEdgeCardinality(cancelled, first, c, "test", &read), context.Canceled)
}
