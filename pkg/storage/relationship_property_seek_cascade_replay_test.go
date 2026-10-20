package storage

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// Replay has its own publication switch, separate from transaction commit.
func TestRelationshipSeekCascadeReplayPublication(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	ns := NewNamespacedEngine(engine, "test")
	for _, id := range []NodeID{"a", "b"} {
		_, err := ns.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, ns.CreateEdge(&Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "T", Properties: map[string]interface{}{"p": "hot"}}))
	edges, answered, err := ns.SeekRelationships(context.Background(), "T", []string{"p"}, []interface{}{"hot"})
	require.NoError(t, err)
	require.True(t, answered)
	require.Len(t, edges, 1)
	// Apply replay's physical deletion without the direct DeleteNode publisher.
	op := Operation{Type: OpDeleteNode, NodeID: "test:a"}
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		var err error
		op.EdgesDeleted, op.DeletedEdgeIDs, _, op.OldNode, err = engine.deleteNodeInTxn(txn, op.NodeID)
		return err
	}))
	engine.publishCommitOperations("test", ns.GetSchema(), []Operation{op}, false)
	edges, answered, err = ns.SeekRelationships(context.Background(), "T", []string{"p"}, []interface{}{"hot"})
	require.NoError(t, err)
	require.True(t, answered)
	require.Empty(t, edges)
	keys, candidates := RelationshipSeekTestBucketSize(engine, "test", "T", "p", "hot")
	require.Zero(t, keys)
	require.Zero(t, candidates)
}
