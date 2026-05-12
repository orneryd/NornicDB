package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPendingEmbeddingsIndex_SkipsSystemNamespace(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	engine.SetEmbeddingsEnabled(true)

	// System node should never be added to pending index.
	_, err = engine.CreateNode(&Node{
		ID:     "system:meta-1",
		Labels: []string{"Meta"},
	})
	require.NoError(t, err)
	require.Equal(t, 0, engine.PendingEmbeddingsCount())

	// A normal node should be in pending index (no embeddings).
	_, err = engine.CreateNode(&Node{
		ID:     "nornic:user-1",
		Labels: []string{"Person"},
	})
	require.NoError(t, err)
	require.Equal(t, 1, engine.PendingEmbeddingsCount())

	found := engine.FindNodeNeedingEmbedding()
	require.NotNil(t, found)
	require.Equal(t, NodeID("nornic:user-1"), found.ID)

	// Refresh should not add system nodes either.
	_ = engine.RefreshPendingEmbeddingsIndex()
	require.Equal(t, 1, engine.PendingEmbeddingsCount())
}
