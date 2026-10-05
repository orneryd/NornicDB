package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBadgerTransactionCreatedDeletedEdgeHasNoPhysicalWrites(t *testing.T) {
	engine := newTestEngine(t)
	for _, identifier := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(&Node{ID: identifier, Labels: []string{"Node"}})
		require.NoError(t, err)
	}
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("test"))
	edge := &Edge{ID: "test:temporary", StartNode: "test:a", EndNode: "test:b", Type: "TEMP"}
	require.NoError(t, tx.CreateEdge(edge))
	visible, err := tx.GetEdge(edge.ID)
	require.NoError(t, err)
	require.Equal(t, edge.ID, visible.ID)
	require.NoError(t, tx.DeleteEdge(edge.ID))
	_, err = tx.GetEdge(edge.ID)
	require.ErrorIs(t, err, ErrNotFound)
	require.Empty(t, tx.pendingWrites)
	require.Empty(t, tx.pendingDeletes)
	require.Len(t, tx.operations, 2)
	require.NoError(t, tx.Commit())
	_, err = engine.GetEdge(edge.ID)
	require.ErrorIs(t, err, ErrNotFound)
	for _, identifier := range []NodeID{"test:a", "test:b"} {
		_, err := engine.GetNode(identifier)
		require.NoError(t, err)
	}
}

func TestBadgerTransactionRecreatedDeletedEdgePreservesCommittedDeletion(t *testing.T) {
	engine := newTestEngine(t)
	for _, identifier := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(&Node{ID: identifier, Labels: []string{"Node"}})
		require.NoError(t, err)
	}
	edge := &Edge{ID: "test:replacement", StartNode: "test:a", EndNode: "test:b", Type: "TEMP"}
	require.NoError(t, engine.CreateEdge(edge))
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.DeleteEdge(edge.ID))
	require.NoError(t, tx.CreateEdge(edge))
	visible, err := tx.GetEdge(edge.ID)
	require.NoError(t, err)
	require.Equal(t, edge.ID, visible.ID)
	require.NoError(t, tx.DeleteEdge(edge.ID))
	require.NoError(t, tx.Commit())
	_, err = engine.GetEdge(edge.ID)
	require.ErrorIs(t, err, ErrNotFound)
	outgoing, err := engine.GetOutgoingEdges(edge.StartNode)
	require.NoError(t, err)
	require.Empty(t, outgoing)
	incoming, err := engine.GetIncomingEdges(edge.EndNode)
	require.NoError(t, err)
	require.Empty(t, incoming)
}

func TestBadgerTransaction_QueryAndMutationPaths(t *testing.T) {
	engine := newTestEngine(t)

	_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"User"}, Properties: map[string]any{"name": "A"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"User"}, Properties: map[string]any{"name": "B"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL", Properties: map[string]any{"k": "v"}}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer tx.Rollback()

	// readTS zero path wrappers
	_, err = tx.GetEdgesByType("REL")
	require.NoError(t, err)
	_, err = tx.GetNodesByLabel("User")
	require.NoError(t, err)
	_, err = tx.GetFirstNodeByLabel("User")
	require.NoError(t, err)
	_, err = tx.GetEdge("test:e1")
	require.NoError(t, err)
	_, err = tx.GetEdgesBetween("test:a", "test:b")
	require.NoError(t, err)
	_, err = tx.AllNodes()
	require.NoError(t, err)

	// readTS non-zero path wrappers
	tx.readTS = MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 1}
	_, err = tx.GetEdgesByType("REL")
	require.NoError(t, err)
	_, err = tx.GetNodesByLabel("User")
	require.NoError(t, err)
	_, err = tx.GetFirstNodeByLabel("User")
	require.NoError(t, err)
	_, err = tx.GetEdge("test:e1")
	require.NoError(t, err)
	_, err = tx.GetEdgesBetween("test:a", "test:b")
	require.NoError(t, err)
	_, err = tx.AllNodes()
	require.NoError(t, err)

	// mutation paths
	_, err = tx.CreateNode(&Node{ID: "test:c", Labels: []string{"User"}, Properties: map[string]any{"name": "C"}})
	require.NoError(t, err)
	err = tx.UpdateNode(&Node{ID: "test:a", Labels: []string{"User", "Member"}, Properties: map[string]any{"name": "A2"}})
	require.NoError(t, err)
	err = tx.UpdateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL2", Properties: map[string]any{"k": "v2"}})
	require.NoError(t, err)
	err = tx.DeleteEdge("test:e1")
	require.NoError(t, err)
	err = tx.DeleteNode("test:b")
	require.NoError(t, err)
}

func TestBadgerTransaction_CreateNode_LargeEmbeddingChunk(t *testing.T) {
	engine := newTestEngine(t)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))

	huge := make([]float32, 300_000)
	_, err = tx.CreateNode(&Node{
		ID:              "test:huge-tx",
		Labels:          []string{"Doc"},
		ChunkEmbeddings: [][]float32{huge},
	})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	got, err := engine.GetNode("test:huge-tx")
	require.NoError(t, err)
	require.Len(t, got.ChunkEmbeddings, 1)
	require.Len(t, got.ChunkEmbeddings[0], len(huge))
}

func TestBadgerTransaction_CommitErrorBranches(t *testing.T) {
	engine := newTestEngine(t)

	t.Run("writes without namespace", func(t *testing.T) {
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		defer tx.Rollback()

		tx.pendingWrites["x"] = []byte("y")
		err = tx.Commit()
		require.Error(t, err)
		require.Contains(t, err.Error(), "no pinned namespace")
	})

	t.Run("flush buffered writes error", func(t *testing.T) {
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetNamespace("test"))
		defer tx.Rollback()

		tx.pendingWrites[""] = []byte("invalid-key")
		err = tx.Commit()
		require.Error(t, err)
		require.Contains(t, err.Error(), "flushing buffered writes")
	})

}
