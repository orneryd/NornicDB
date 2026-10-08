package storage

import (
	"errors"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestDeleteConnectedNode(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()
	for _, node := range []*Node{{ID: "test:a"}, {ID: "test:b"}, {ID: "test:c"}} {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:ab", StartNode: "test:a", EndNode: "test:b", Type: "R"}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:cb", StartNode: "test:c", EndNode: "test:b", Type: "R"}))

	// Its relationship deleted later in the transaction: the commit deletes it.
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.DeleteConnectedNode("test:a"))
	require.ErrorIs(t, tx.DeleteConnectedNode("test:a"), ErrNotFound)
	require.Equal(t, 1, tx.OperationCount())
	node, err := tx.GetNode("test:a")
	require.NoError(t, err)
	require.Equal(t, NodeID("test:a"), node.ID)
	visible, answered := tx.RelationshipEndpointVisible("test:a")
	require.True(t, answered)
	require.True(t, visible)
	require.NoError(t, tx.DeleteEdge("test:ab"))
	require.NoError(t, tx.Commit())
	_, err = engine.GetNode("test:a")
	require.ErrorIs(t, err, ErrNotFound)

	// Still connected at commit: the commit fails and changes nothing.
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.DeleteConnectedNode("test:c"))
	err = tx.Commit()
	var connected *NodeStillConnectedError
	require.True(t, errors.As(err, &connected))
	require.Equal(t, NodeID("test:c"), connected.NodeID)
	require.Equal(t, "Neo.ClientError.Schema.ConstraintValidationFailed", connected.BoltErrorCode())
	require.Contains(t, connected.Error(), "still has relationships")
	_, err = engine.GetNode("test:c")
	require.NoError(t, err)

	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	require.ErrorIs(t, tx.DeleteConnectedNode(""), ErrInvalidID)
	require.ErrorIs(t, tx.DeleteConnectedNode("test:missing"), ErrNotFound)
	require.NoError(t, tx.Rollback())
}

// The ways DeleteConnectedNode and its commit-time check fail.
func TestDeleteConnectedNodeFailures(t *testing.T) {
	engine := newTestEngine(t)
	for _, node := range []*Node{{ID: "test:a"}, {ID: "test:b"}} {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}

	// A finished transaction, or a node of another namespace.
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.Error(t, tx.DeleteConnectedNode("test:a"))
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	require.Error(t, tx.DeleteConnectedNode("other:a"))
	require.NoError(t, tx.Rollback())

	// A relationship created in the transaction still leads into the node.
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(&Edge{ID: "test:ab", StartNode: "test:a", EndNode: "test:b", Type: "R"}))
	require.NoError(t, tx.DeleteConnectedNode("test:b"))
	var connected *NodeStillConnectedError
	require.True(t, errors.As(tx.Commit(), &connected))
	require.Equal(t, NodeID("test:b"), connected.NodeID)

	// A pending relationship that can't be written fails the delete.
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	tx.pendingEdges["test:bad"] = &Edge{ID: "test:bad", StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: map[string]any{"x": make(chan int)}}
	tx.deferredEdgeWrites = map[EdgeID]struct{}{"test:bad": {}}
	tx.operations = append(tx.operations, Operation{Type: OpCreateEdge, EdgeID: "test:bad"})
	require.Error(t, tx.DeleteConnectedNode("test:a"))
	require.NoError(t, tx.Rollback())

	// A node that is gone by commit time fails it.
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	tx.connectedDeletes = map[NodeID]*Node{"test:ghost": nil}
	tx.mu.Lock()
	err = tx.resolveConnectedDeletesLocked()
	tx.mu.Unlock()
	require.ErrorIs(t, err, ErrNotFound)
	require.NoError(t, tx.Rollback())

	// Reading the relationships fails: a stored one can't be decoded.
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:ab", StartNode: "test:a", EndNode: "test:b", Type: "R"}))
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(edgeKey("test:ab"), []byte("corrupt-edge"))
	}))
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	tx.connectedDeletes = map[NodeID]*Node{"test:a": {ID: "test:a"}}
	tx.mu.Lock()
	err = tx.resolveConnectedDeletesLocked()
	tx.mu.Unlock()
	require.Error(t, err)
	require.NoError(t, tx.Rollback())
}
