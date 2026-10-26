package storage_test

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Author: #1548 implementation lane (Codex). The release's shared projection
// helper must return owned selected values just as its full-image branch does.
func TestResidualProjectedReadOwnership(t *testing.T) {
	b, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	id, err := b.CreateNode(&storage.Node{ID: "test:projected-owned", Properties: map[string]any{"values": []int64{1, 2}, "omitted": "retained"}})
	require.NoError(t, err)
	_, err = b.GetStoredNode(id) // Warm the full image consumed by projection.
	require.NoError(t, err)
	projected, err := b.GetNodeProjected(id, []string{"values"})
	require.NoError(t, err)
	require.NotContains(t, projected.Properties, "omitted")
	projected.Properties["values"].([]int64)[0] = 99
	stored, err := b.GetStoredNode(id)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, stored.Properties["values"])
	projected, err = b.GetNodeProjected(id, []string{"values"})
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, projected.Properties["values"])
}
