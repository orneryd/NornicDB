package storage_test

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/enginecontract"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Author: Lane #1644 (Codex). These public native pairs have different payload
// and visibility modes; each is compared only with its actual point contract.
func TestFoundationNativeBatchPointProperties(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	for _, id := range []storage.NodeID{"tenant:a", "tenant:b"} {
		_, err := base.CreateNode(&storage.Node{ID: id, Labels: []string{"BatchPoint"}, Properties: map[string]interface{}{"id": string(id)}, ChunkEmbeddings: [][]float32{{1, 2}}})
		require.NoError(t, err)
	}
	type pair struct {
		batch func([]storage.NodeID) (map[storage.NodeID]*storage.Node, error)
		point func(storage.NodeID) (*storage.Node, error)
	}
	pairs := map[string]pair{
		"full": {base.BatchGetNodes, base.GetNode},
		"full-context": {func(ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
			return storage.BatchGetNodesWithContext(context.Background(), base, ids)
		}, base.GetNode},
		"without-embeddings": {base.BatchGetNodesWithoutEmbeddings, base.GetNodeWithoutEmbeddings},
		"latest-visible":     {base.BatchGetNodesLatestVisible, base.GetNodeLatestVisible},
	}
	for name, p := range pairs {
		t.Run(name, func(t *testing.T) {
			enginecontract.NodeBatchPoint(t, []storage.NodeID{"tenant:a", "tenant:b", "tenant:missing"}, p.batch, p.point)
		})
	}
	require.NoError(t, base.Close())
	for name, p := range pairs {
		t.Run(name+"/closed", func(t *testing.T) {
			_, pointErr := p.point("tenant:a")
			require.ErrorIs(t, pointErr, storage.ErrStorageClosed)
			got, err := p.batch([]storage.NodeID{"tenant:a", "tenant:missing"})
			assert.ErrorIs(t, err, storage.ErrStorageClosed, "a point failure cannot be batch absence")
			assert.Empty(t, got)
		})
	}
}

func TestFoundationNativeTransactionBatchPointProperties(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	for _, id := range []storage.NodeID{"tenant:changed", "tenant:gone"} {
		_, err := base.CreateNode(&storage.Node{ID: id, Labels: []string{"BatchPoint"}, Properties: map[string]interface{}{"value": "before"}})
		require.NoError(t, err)
	}
	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	node, err := tx.GetNode("tenant:changed")
	require.NoError(t, err)
	node.Properties["value"] = "pending"
	require.NoError(t, tx.UpdateGraphNode(node, storage.ClientNodeEdits))
	_, err = tx.CreateNode(&storage.Node{ID: "tenant:fresh", Labels: []string{"BatchPoint"}})
	require.NoError(t, err)
	require.NoError(t, tx.DeleteGraphNode("tenant:gone", storage.CascadeEdges))
	ids := []storage.NodeID{"tenant:changed", "tenant:fresh", "tenant:gone", "tenant:missing"}
	enginecontract.NodeBatchPoint(t, ids, tx.BatchGetNodes, tx.GetNode)
	enginecontract.NodeBatchPoint(t, ids, func(ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
		return tx.BatchGetNodesWithContext(context.Background(), ids)
	}, tx.GetNode)
	require.NoError(t, tx.Rollback())
	_, err = tx.GetNode(ids[0])
	require.ErrorIs(t, err, storage.ErrTransactionClosed)
	_, err = tx.BatchGetNodes(ids)
	require.ErrorIs(t, err, storage.ErrTransactionClosed)
	_, err = tx.BatchGetNodesWithContext(context.Background(), ids)
	require.ErrorIs(t, err, storage.ErrTransactionClosed)
}
