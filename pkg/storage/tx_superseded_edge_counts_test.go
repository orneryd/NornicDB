package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Relabelling a node moves its relationships' endpoint-label counts; a
// relationship the same transaction deleted no longer counts, and one it
// rewrote counts once, as its pending version (#907).
func TestTransactionRelabelSkipsDeletedAndRewrittenRelationships(t *testing.T) {
	engine := createTestBadgerEngine(t)
	for _, node := range []*Node{{ID: "test:a", Labels: []string{"Person"}}, {ID: "test:b", Labels: []string{"Thing"}}, {ID: "test:c", Labels: []string{"Thing"}}} {
		_, err := engine.CreateNode(t.Context(), node)
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "test:gone", StartNode: "test:a", EndNode: "test:b", Type: "R"}))
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "test:kept", StartNode: "test:a", EndNode: "test:c", Type: "R", Properties: map[string]interface{}{"v": int64(1)}}))

	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.DeleteEdge(t.Context(), "test:gone"))
	require.NoError(t, tx.UpdateEdge(t.Context(), EdgeUpdateRequest{After: &Edge{ID: "test:kept", StartNode: "test:a", EndNode: "test:c", Type: "R", Properties: map[string]interface{}{"v": int64(2)}}}))
	require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: &Node{ID: "test:a", Labels: []string{"person"}}, Policy: ClientNodeEdits}))
	require.NoError(t, tx.Commit())

	for label, want := range map[string]int64{"person": 1, "Person": 0} {
		count, err := engine.EdgeCountByStartLabel(context.Background(), label, "R")
		require.NoError(t, err)
		require.Equal(t, want, count, label)
	}
}
