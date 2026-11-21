package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransactionCarriedChunkVectorsHaveOneRetainedImage(t *testing.T) {
	ctx := t.Context()
	engine := newTestEngine(t)
	engine.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
	vectors := [][]float32{{1, 2}, {3, 4}}
	id, err := engine.CreateNode(ctx, &Node{ID: "test:carry", Properties: map[string]any{"text": "source"}, ChunkEmbeddings: vectors})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction(ctx, OrdinaryBegin)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	for step := range 4 {
		before, err := tx.GetNode(ctx, id)
		require.NoError(t, err)
		after := CopyNode(before)
		after.Properties["revision"] = int64(step)
		require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{Before: before, After: after}))
		after.ChunkEmbeddings[0][0] = 99
		view, err := tx.GetNode(ctx, id)
		require.NoError(t, err)
		require.Equal(t, vectors, view.ChunkEmbeddings)
		view.ChunkEmbeddings[0][0] = 100
	}

	first := tx.operations[0].Node.ChunkEmbeddings
	for _, operation := range tx.operations {
		require.Same(t, &first[0][0], &operation.Node.ChunkEmbeddings[0][0], "unchanged vectors share the transaction-owned backing storage across revisions")
		require.Same(t, &first[0][0], &operation.OldNode.ChunkEmbeddings[0][0], "the predecessor refers to the same carried vectors")
		require.Empty(t, operation.nodeObservedBefore.ChunkEmbeddings, "the constraint observation retains no vectors")
	}
	require.NoError(t, tx.Commit())
	final, err := engine.GetNode(ctx, id)
	require.NoError(t, err)
	require.Equal(t, vectors, final.ChunkEmbeddings)
	require.Equal(t, int64(3), final.Properties["revision"])
}
