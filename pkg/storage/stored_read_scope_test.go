package storage

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStoredReadNativeViewScope(t *testing.T) {
	b := NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	for _, namespace := range []string{"tenant", "foreign"} {
		for _, local := range []string{"a", "b"} {
			_, err := b.CreateNode(&Node{ID: NodeID(namespace + ":" + local), Labels: []string{namespace}})
			require.NoError(t, err)
		}
		require.NoError(t, b.CreateEdge(&Edge{ID: EdgeID(namespace + ":e"), StartNode: NodeID(namespace + ":a"), EndNode: NodeID(namespace + ":b"), Type: "R"}))
	}
	requests := map[string]StoredReadRequest{
		"node":      {Nodes: []NodeID{"foreign:a"}},
		"edge":      {Edges: []EdgeID{"foreign:e"}},
		"adjacency": {Adjacency: []StoredAdjacencyRequest{{Node: "foreign:a", Direction: Outgoing}}},
	}
	for _, mode := range []StoredReadMode{StoredMetadataOnly, StoredSelectedProperties, StoredFullMaterialization} {
		for name, request := range requests {
			t.Run(fmt.Sprintf("tx/%d/%s", mode, name), func(t *testing.T) {
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				require.NoError(t, tx.SetNamespace("tenant"))
				_, err = tx.GetNode("foreign:a")
				require.ErrorIs(t, err, ErrCrossNamespaceTransaction, "ordinary native point read supplies the independent scope oracle")
				request.Mode = mode
				request.Physical = true
				var work StoredReadWork
				request.Work = &work
				result, err := ReadStored(context.Background(), tx, request)
				require.ErrorIs(t, err, ErrCrossNamespaceTransaction)
				require.Nil(t, result)
				require.Zero(t, work.Captures)
				require.Zero(t, work.FullMaterializations)
			})
		}
	}
	// Namespaced OpenReadSnapshot captures an already scoped native reader. Even
	// a retained operation rebound to that native reader must keep its prefix.
	scoped, err := NewNamespacedEngine(b, "tenant").OpenReadSnapshot(context.Background())
	require.NoError(t, err)
	defer scoped.Close()
	native := scoped.(*namespacedReadSnapshot).inner
	for _, mode := range []StoredReadMode{StoredMetadataOnly, StoredSelectedProperties, StoredFullMaterialization} {
		for name, request := range requests {
			t.Run(fmt.Sprintf("snapshot/%d/%s", mode, name), func(t *testing.T) {
				request.Mode = mode
				request.Physical = true
				var work StoredReadWork
				request.Work = &work
				result, err := ReadStored(context.Background(), native, request)
				require.ErrorIs(t, err, ErrInvalidID)
				require.Nil(t, result)
				require.Zero(t, work.Captures)
			})
		}
	}
	scoped.Close()
	_, err = ReadStoredMetadata(context.Background(), native, StoredReadRequest{Nodes: []NodeID{"tenant:a"}})
	require.ErrorIs(t, err, ErrStorageClosed)
}

func TestStoredReadFirstNativeReadPinsNamespace(t *testing.T) {
	b := NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	for _, id := range []NodeID{"tenant:a", "foreign:a"} {
		_, err := b.CreateNode(&Node{ID: id, Labels: []string{"L"}})
		require.NoError(t, err)
	}
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	facts, err := ReadStoredMetadata(context.Background(), tx, StoredReadRequest{Nodes: []NodeID{"tenant:a"}})
	require.NoError(t, err)
	require.True(t, facts.Nodes[0].Exists)
	require.Equal(t, "tenant", tx.Namespace())
	require.True(t, tx.observed)
	_, err = ReadStoredMetadata(context.Background(), tx, StoredReadRequest{Nodes: []NodeID{"foreign:a"}})
	require.ErrorIs(t, err, ErrCrossNamespaceTransaction)
	require.NoError(t, tx.Rollback())
	_, err = ReadStoredMetadata(context.Background(), tx, StoredReadRequest{Nodes: []NodeID{"tenant:a"}})
	require.ErrorIs(t, err, ErrTransactionClosed)
}
