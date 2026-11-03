package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// An entity a transaction already deleted, itself or by deleting its node,
// is gone for that transaction: deleting it again reports ErrNotFound and
// changes nothing, and the stored counts reflect one deletion (#827).
func TestBadgerTransaction_RepeatedDeleteChangesNothing(t *testing.T) {
	engine := newTestEngine(t)
	for _, id := range []NodeID{"test:a", "test:b", "test:c"} {
		_, err := engine.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"P"}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "test:ab", StartNode: "test:a", EndNode: "test:b", Type: "R"}))
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "test:bc", StartNode: "test:b", EndNode: "test:c", Type: "R"}))

	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.DeleteGraphNode(t.Context(), "test:a", CascadeEdges))
	require.ErrorIs(t, tx.DeleteGraphNode(t.Context(), "test:a", CascadeEdges), ErrNotFound)
	require.ErrorIs(t, tx.DeleteEdge(t.Context(), "test:ab"), ErrNotFound, "deleted with its node")
	require.NoError(t, tx.DeleteEdge(t.Context(), "test:bc"))
	require.ErrorIs(t, tx.DeleteEdge(t.Context(), "test:bc"), ErrNotFound)
	require.NoError(t, tx.Commit())

	nodes, err := engine.NodeCount(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(2), nodes)
	edges, err := engine.EdgeCount(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(0), edges)
	byLabel, err := engine.NodeCountByLabel(t.Context(), "P")
	require.NoError(t, err)
	require.Equal(t, int64(2), byLabel)
	byType, err := engine.EdgeCountByType(context.Background(), "R")
	require.NoError(t, err)
	require.Equal(t, int64(0), byType)
}
