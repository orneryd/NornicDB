package storage

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_NodeAndEdgeInputValidationBranches(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })

	_, err := engine.CreateNode(nil)
	require.ErrorIs(t, err, ErrInvalidData)
	_, err = engine.CreateNode(&Node{})
	require.ErrorIs(t, err, ErrInvalidID)
	_, err = engine.CreateNode(&Node{ID: "unprefixed"})
	require.Error(t, err)

	require.ErrorIs(t, engine.UpdateNode(nil), ErrInvalidData)
	require.ErrorIs(t, engine.UpdateNode(&Node{}), ErrInvalidID)
	require.Error(t, engine.UpdateNode(&Node{ID: "unprefixed"}))

	_, err = engine.GetNode("")
	require.ErrorIs(t, err, ErrInvalidID)

	require.ErrorIs(t, engine.UpdateNodeEmbedding(nil), ErrInvalidData)
	require.ErrorIs(t, engine.UpdateNodeEmbedding(&Node{}), ErrInvalidID)
	require.Error(t, engine.UpdateNodeEmbedding(&Node{ID: "unprefixed"}))
	require.ErrorIs(t, engine.UpdateNodeEmbedding(&Node{ID: "test:missing"}), ErrNotFound)

	require.ErrorIs(t, engine.DeleteNode(""), ErrInvalidID)
	require.NoError(t, engine.BulkDeleteNodes(nil))
	require.NoError(t, engine.BulkDeleteEdges(nil))
}

func TestBadgerEngine_EdgeCRUDErrorAndBranchPaths(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })

	require.ErrorIs(t, engine.CreateEdge(nil), ErrInvalidData)
	require.ErrorIs(t, engine.CreateEdge(&Edge{}), ErrInvalidID)
	require.ErrorIs(t, engine.CreateEdge(&Edge{
		ID: "test:e-missing-endpoints", StartNode: "test:a", EndNode: "test:b", Type: "REL",
	}), ErrNotFound)

	_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL"}))

	// Endpoint-change path where new endpoint is missing.
	require.ErrorIs(t, engine.UpdateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:missing", Type: "REL",
	}), ErrNotFound)

	// Type-change path.
	require.NoError(t, engine.UpdateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL2",
	}))

	// Endpoint+type change path.
	_, err = engine.CreateNode(&Node{ID: "test:c", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, engine.UpdateEdge(&Edge{
		ID: "test:e1", StartNode: "test:b", EndNode: "test:c", Type: "REL3",
	}))

	// Not found / invalid branches.
	require.ErrorIs(t, engine.UpdateEdge(nil), ErrInvalidData)
	require.ErrorIs(t, engine.UpdateEdge(&Edge{}), ErrInvalidID)
	require.ErrorIs(t, engine.UpdateEdge(&Edge{ID: "test:missing", StartNode: "test:a", EndNode: "test:b", Type: "REL"}), ErrNotFound)

	require.ErrorIs(t, engine.DeleteEdge(""), ErrInvalidID)
	require.ErrorIs(t, engine.DeleteEdge("test:missing"), ErrNotFound)
	require.NoError(t, engine.DeleteEdge("test:e1"))
	require.NoError(t, engine.BulkDeleteEdges([]EdgeID{"test:missing", ""}))
}

func TestBadgerEngine_DeleteNodeInTxnMissingNodeBranch(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		_, _, _, _, err := engine.deleteNodeInTxn(txn, "test:missing")
		require.ErrorIs(t, err, ErrNotFound)
		return nil
	}))
}
