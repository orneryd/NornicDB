package cypher

// transactionStorageWrapper.StreamNodesWithOptions streams the transaction's
// view through BadgerTransaction.StreamNodesWithOptions (#824), within the
// wrapper's namespace or, without one, the whole store.

import (
	"context"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTransactionStorageWrapperStreamNodesWithOptions(t *testing.T) {
	engine, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	for _, id := range []storage.NodeID{"t:1", "u:1"} {
		_, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"P"}, Properties: map[string]interface{}{"k": "v"}})
		require.NoError(t, err)
	}
	stream := func(namespace string) []string {
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		w := &transactionStorageWrapper{tx: tx, underlying: engine, namespace: namespace, separator: ":", mutatedNodeIDs: map[string]struct{}{}}
		var ids []string
		require.NoError(t, w.StreamNodesWithOptions(context.Background(), storage.StreamNodesOptions{Projection: []string{"k"}}, func(node *storage.Node) error {
			ids = append(ids, string(node.ID))
			return nil
		}))
		sort.Strings(ids)
		return ids
	}
	require.Equal(t, []string{"1"}, stream("t"))
	require.Equal(t, []string{"t:1", "u:1"}, stream(""))
}
