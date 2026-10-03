package storage

import (
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
	_, err := engine.CreateNode(&Node{ID: "tenant:a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "tenant:b", Labels: []string{"N"}})
	require.NoError(t, err)
	err = engine.CreateEdge(&Edge{ID: "tenant:committed", StartNode: "tenant:a", EndNode: "tenant:b", Type: "REL", Properties: map[string]any{"k": "v", "k2": "v2"}})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("tenant"))
	defer tx.Rollback()

	tx.pendingEdges["tenant:pending"] = &Edge{ID: "tenant:pending", Type: "REL", Properties: map[string]any{"k": "p", "k2": "q"}}

	cSingle := Constraint{Type: ConstraintUnique, Label: "REL", Properties: []string{"k"}}
	err = tx.checkEdgeUniqueness(&Edge{ID: "tenant:new1", Type: "REL", Properties: map[string]any{"k": "p"}}, cSingle, "tenant")
	require.Error(t, err)

	err = tx.checkEdgeUniqueness(&Edge{ID: "tenant:new2", Type: "REL", Properties: map[string]any{"k": "v"}}, cSingle, "tenant")
	require.Error(t, err)

	cComposite := Constraint{Type: ConstraintUnique, Label: "REL", Properties: []string{"k", "k2"}}
	err = tx.checkEdgeUniqueness(&Edge{ID: "tenant:new3", Type: "REL", Properties: map[string]any{"k": "p", "k2": "q"}}, cComposite, "tenant")
	require.Error(t, err)

	err = tx.checkEdgeUniqueness(&Edge{ID: "tenant:new4", Type: "REL", Properties: map[string]any{"k": "ok", "k2": "ok"}}, cComposite, "tenant")
	require.NoError(t, err)

	// Nil key short-circuit path.
	err = tx.checkEdgeUniqueness(&Edge{ID: "tenant:new5", Type: "REL", Properties: map[string]any{"k": nil}}, cSingle, "tenant")
	require.NoError(t, err)
}

func TestBadgerTransaction_CheckEdgeTemporalConstraint(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(&Node{ID: "tenant:a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "tenant:b", Labels: []string{"N"}})
	require.NoError(t, err)

	start1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end1 := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	err = engine.CreateEdge(&Edge{ID: "tenant:e1", StartNode: "tenant:a", EndNode: "tenant:b", Type: "REL", Properties: map[string]any{"key": "k1", "from": start1, "to": end1}})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("tenant"))
	defer tx.Rollback()

	c := Constraint{Type: ConstraintTemporal, Label: "REL", Properties: []string{"key", "from", "to"}}

	// Missing key property.
	err = tx.checkEdgeTemporalConstraint(&Edge{ID: "tenant:new1", Type: "REL", Properties: map[string]any{"from": start1, "to": end1}}, c, "tenant")
	require.Error(t, err)

	// Invalid start type.
	err = tx.checkEdgeTemporalConstraint(&Edge{ID: "tenant:new2", Type: "REL", Properties: map[string]any{"key": "k1", "from": "bad", "to": end1}}, c, "tenant")
	require.Error(t, err)

	// Overlap with committed edge.
	err = tx.checkEdgeTemporalConstraint(&Edge{ID: "tenant:new3", Type: "REL", Properties: map[string]any{"key": "k1", "from": start1.Add(10 * 24 * time.Hour), "to": end1.Add(10 * 24 * time.Hour)}}, c, "tenant")
	require.Error(t, err)

	// Non-overlapping edge should pass.
	err = tx.checkEdgeTemporalConstraint(&Edge{ID: "tenant:new4", Type: "REL", Properties: map[string]any{"key": "k1", "from": end1.Add(time.Second), "to": end1.Add(24 * time.Hour)}}, c, "tenant")
	require.NoError(t, err)
}

func TestBadgerTransaction_CheckEdgeCardinality(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(&Node{ID: "tenant:a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "tenant:b", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "tenant:c", Labels: []string{"N"}})
	require.NoError(t, err)
	err = engine.CreateEdge(&Edge{ID: "tenant:e1", StartNode: "tenant:a", EndNode: "tenant:b", Type: "REL"})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("tenant"))
	defer tx.Rollback()

	cOut := Constraint{Type: ConstraintCardinality, Label: "REL", Direction: "OUTGOING", MaxCount: 1}
	err = tx.checkEdgeCardinality(&Edge{ID: "tenant:e2", StartNode: "tenant:a", EndNode: "tenant:c", Type: "REL"}, cOut, "tenant")
	require.Error(t, err)

	// Pending edge branch + incoming branch.
	tx.pendingEdges["tenant:pending"] = &Edge{ID: "tenant:pending", StartNode: "tenant:c", EndNode: "tenant:b", Type: "REL"}
	cIn := Constraint{Type: ConstraintCardinality, Label: "REL", Direction: "INCOMING", MaxCount: 1}
	err = tx.checkEdgeCardinality(&Edge{ID: "tenant:new", StartNode: "tenant:a", EndNode: "tenant:b", Type: "REL"}, cIn, "tenant")
	require.Error(t, err)
}
