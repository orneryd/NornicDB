package storage

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestCommitConflictError(t *testing.T) {
	err := commitConflictError(badger.ErrConflict)
	require.Error(t, err)
	require.Contains(t, err.Error(), "concurrent transaction modified data")
	require.ErrorIs(t, err, ErrConflict)
}

func TestBadgerTransaction_CheckEdgeUniqueness(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(t.Context(), &Node{ID: "tenant:a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &Node{ID: "tenant:b", Labels: []string{"N"}})
	require.NoError(t, err)
	err = engine.CreateEdge(t.Context(), &Edge{ID: "tenant:committed", StartNode: "tenant:a", EndNode: "tenant:b", Type: "REL", Properties: map[string]any{"k": "v", "k2": "v2"}})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction(t.Context(), OrdinaryBegin)
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("tenant"))
	defer tx.Rollback()

	tx.pendingEdges["tenant:pending"] = &Edge{ID: "tenant:pending", Type: "REL", Properties: map[string]any{"k": "p", "k2": "q"}}

	cSingle := Constraint{Type: ConstraintUnique, Label: "REL", Properties: []string{"k"}}
	err = tx.checkEdgeUniqueness(context.Background(), &Edge{ID: "tenant:new1", Type: "REL", Properties: map[string]any{"k": "p"}}, cSingle, "tenant")
	require.Error(t, err)

	err = tx.checkEdgeUniqueness(context.Background(), &Edge{ID: "tenant:new2", Type: "REL", Properties: map[string]any{"k": "v"}}, cSingle, "tenant")
	require.Error(t, err)

	cComposite := Constraint{Type: ConstraintUnique, Label: "REL", Properties: []string{"k", "k2"}}
	err = tx.checkEdgeUniqueness(context.Background(), &Edge{ID: "tenant:new3", Type: "REL", Properties: map[string]any{"k": "p", "k2": "q"}}, cComposite, "tenant")
	require.Error(t, err)

	err = tx.checkEdgeUniqueness(context.Background(), &Edge{ID: "tenant:new4", Type: "REL", Properties: map[string]any{"k": "ok", "k2": "ok"}}, cComposite, "tenant")
	require.NoError(t, err)

	// Nil key short-circuit path.
	err = tx.checkEdgeUniqueness(context.Background(), &Edge{ID: "tenant:new5", Type: "REL", Properties: map[string]any{"k": nil}}, cSingle, "tenant")
	require.NoError(t, err)
}

func TestBadgerTransaction_CheckEdgeTemporalConstraint(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(t.Context(), &Node{ID: "tenant:a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &Node{ID: "tenant:b", Labels: []string{"N"}})
	require.NoError(t, err)

	start1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end1 := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	err = engine.CreateEdge(t.Context(), &Edge{ID: "tenant:e1", StartNode: "tenant:a", EndNode: "tenant:b", Type: "REL", Properties: map[string]any{"key": "k1", "from": start1, "to": end1}})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction(t.Context(), OrdinaryBegin)
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("tenant"))
	defer tx.Rollback()

	c := Constraint{Type: ConstraintTemporal, Label: "REL", Properties: []string{"key", "from", "to"}}

	// Missing key property.
	err = tx.checkEdgeTemporalConstraint(context.Background(), &Edge{ID: "tenant:new1", Type: "REL", Properties: map[string]any{"from": start1, "to": end1}}, c, "tenant", &temporalValidationRead{})
	require.Error(t, err)

	// Invalid start type.
	err = tx.checkEdgeTemporalConstraint(context.Background(), &Edge{ID: "tenant:new2", Type: "REL", Properties: map[string]any{"key": "k1", "from": "bad", "to": end1}}, c, "tenant", &temporalValidationRead{})
	require.Error(t, err)

	// Overlap with committed edge.
	err = tx.checkEdgeTemporalConstraint(context.Background(), &Edge{ID: "tenant:new3", Type: "REL", Properties: map[string]any{"key": "k1", "from": start1.Add(10 * 24 * time.Hour), "to": end1.Add(10 * 24 * time.Hour)}}, c, "tenant", &temporalValidationRead{})
	require.Error(t, err)

	// Non-overlapping edge should pass.
	err = tx.checkEdgeTemporalConstraint(context.Background(), &Edge{ID: "tenant:new4", Type: "REL", Properties: map[string]any{"key": "k1", "from": end1.Add(time.Second), "to": end1.Add(24 * time.Hour)}}, c, "tenant", &temporalValidationRead{})
	require.NoError(t, err)
}

func TestBadgerTransaction_CheckEdgeCardinality(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(t.Context(), &Node{ID: "tenant:a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &Node{ID: "tenant:b", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &Node{ID: "tenant:c", Labels: []string{"N"}})
	require.NoError(t, err)
	err = engine.CreateEdge(t.Context(), &Edge{ID: "tenant:e1", StartNode: "tenant:a", EndNode: "tenant:b", Type: "REL"})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction(t.Context(), OrdinaryBegin)
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("tenant"))
	defer tx.Rollback()

	cOut := Constraint{Type: ConstraintCardinality, Label: "REL", Direction: "OUTGOING", MaxCount: 1}
	err = tx.checkEdgeCardinality(context.Background(), &Edge{ID: "tenant:e2", StartNode: "tenant:a", EndNode: "tenant:c", Type: "REL"}, cOut, "tenant")
	require.Error(t, err)

	// Pending edge branch + incoming branch.
	tx.pendingEdges["tenant:pending"] = &Edge{ID: "tenant:pending", StartNode: "tenant:c", EndNode: "tenant:b", Type: "REL"}
	cIn := Constraint{Type: ConstraintCardinality, Label: "REL", Direction: "INCOMING", MaxCount: 1}
	err = tx.checkEdgeCardinality(context.Background(), &Edge{ID: "tenant:new", StartNode: "tenant:a", EndNode: "tenant:b", Type: "REL"}, cIn, "tenant")
	require.Error(t, err)
}

func TestTemporalValidationReadRefreshesAfterPublication(t *testing.T) {
	engine := newTestEngine(t)
	ctx := t.Context()
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(ctx, &Node{ID: id})
		require.NoError(t, err)
	}

	tx, err := engine.BeginTransaction(ctx, OrdinaryBegin)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	properties := map[string]any{"key": "k", "from": start, "to": start.Add(time.Hour)}
	constraint := Constraint{Type: ConstraintTemporal, Label: "REL", Properties: []string{"key", "from", "to"}}
	edge := &Edge{ID: "test:candidate", StartNode: "test:a", EndNode: "test:b", Type: "REL", Properties: properties}

	var read temporalValidationRead
	require.NoError(t, tx.checkEdgeTemporalConstraint(ctx, edge, constraint, "test", &read))

	peer := *edge
	peer.ID = "test:peer"
	require.NoError(t, engine.CreateEdge(ctx, &peer))
	err = tx.checkEdgeTemporalConstraint(ctx, edge, constraint, "test", &read)
	var violation *ConstraintViolationError
	require.ErrorAs(t, err, &violation)
	require.Equal(t, ConstraintTemporal, violation.Type)
	require.ErrorContains(t, err, string(peer.ID))

	require.NoError(t, engine.DeleteEdge(ctx, peer.ID))
	require.NoError(t, tx.checkEdgeTemporalConstraint(ctx, edge, constraint, "test", &read))
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, tx.checkEdgeTemporalConstraint(cancelled, edge, constraint, "test", &read), context.Canceled)
}
