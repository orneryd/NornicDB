package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransactionReadEdgesPreservesViewsAndPendingOverlay(t *testing.T) {
	engine := createTestBadgerEngine(t)
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:old", StartNode: "test:a", EndNode: "test:b", Type: "R"}))
	reader, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Rollback() })
	outgoing := TransactionEdgeRead{Kind: TransactionReadOutgoing, Node: "test:a"}
	incoming := TransactionEdgeRead{Kind: TransactionReadIncoming, Node: "test:b"}
	between := TransactionEdgeRead{Kind: TransactionReadBetween, Node: "test:a", Other: "test:b"}
	for _, request := range []TransactionEdgeRead{outgoing, incoming, between} {
		edges, err := reader.ReadEdges(request)
		require.NoError(t, err)
		require.Equal(t, []string{"test:old"}, sortedEdgeIDStrings(edges))
	}
	writer, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.Rollback() })
	require.NoError(t, writer.CreateEdge(&Edge{ID: "test:later", StartNode: "test:a", EndNode: "test:b", Type: "R"}))
	require.NoError(t, writer.Commit())
	require.NoError(t, reader.CreateEdge(&Edge{ID: "test:pending", StartNode: "test:a", EndNode: "test:b", Type: "P"}))
	require.NoError(t, reader.DeleteEdge("test:old"))
	for _, request := range []TransactionEdgeRead{outgoing, incoming} {
		edges, err := reader.ReadEdges(request)
		require.NoError(t, err)
		require.Equal(t, []string{"test:pending"}, sortedEdgeIDStrings(edges), "directional reads retain their pinned view and overlay")
	}
	edges, err := reader.ReadEdges(between)
	require.NoError(t, err)
	require.Equal(t, []string{"test:later", "test:pending"}, sortedEdgeIDStrings(edges), "between reads retain the engine view and overlay")
	require.Equal(t, EdgeID("test:pending"), reader.GetEdgeBetween("test:a", "test:b", "P").ID)
}

func TestTransactionReadEdgesRequestAdmission(t *testing.T) {
	engine := createTestBadgerEngine(t)
	for _, request := range []TransactionEdgeRead{
		{Kind: TransactionReadOutgoing, Node: "other:a"},
		{Kind: TransactionReadIncoming, Node: "other:b"},
		{Kind: TransactionReadBetween, Node: "test:a", Other: "other:b"},
	} {
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetNamespace("test"))
		_, err = tx.ReadEdges(request)
		require.ErrorIs(t, err, ErrCrossNamespaceTransaction)
		require.NoError(t, tx.Rollback())
		_, err = tx.ReadEdges(request)
		require.ErrorIs(t, err, ErrTransactionClosed)
	}
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.ReadEdges(TransactionEdgeRead{Kind: 255, Node: "test:a"})
	require.ErrorIs(t, err, ErrInvalidData)
	require.NoError(t, tx.Rollback())
}
