package storage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStagedMemoryBytesCountsDeletedOldBodies(t *testing.T) {
	engine := newTestEngine(t)
	const count = 32
	for i := 0; i < count; i++ {
		id := NodeID("test:old-" + string(rune('a'+i)))
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Old"}, Properties: map[string]any{"body": strings.Repeat("x", 32<<10)}})
		require.NoError(t, err)
	}
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	for i := 0; i < count; i++ {
		id := NodeID("test:old-" + string(rune('a'+i)))
		require.NoError(t, tx.DeleteGraphNode(id, CascadeEdges))
	}
	require.Greater(t, tx.StagedMemoryBytes(), int64(1<<20), "deleted old bodies remain owned until rollback")
	require.NoError(t, tx.Rollback())
}
