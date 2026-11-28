package multidb

import (
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestSizeTrackingEngine_GetNodeProjected(t *testing.T) {
	manager, name := setupTestManager(t)
	store, err := manager.GetStorage(name)
	require.NoError(t, err)
	reader, ok := store.(storage.ProjectedNodeReader)
	require.True(t, ok, "database wrapper must preserve point projection, got %T", store)
	id, err := store.CreateNode(t.Context(), &storage.Node{ID: "projected", Labels: []string{"Doc"}, Properties: map[string]any{"key": "one", "body": strings.Repeat("large omitted body ", 512), "unique-key": int64(7)}})
	require.NoError(t, err)
	node, err := reader.GetNodeProjected(t.Context(), id, []string{"key", "absent"})
	require.NoError(t, err)
	require.Equal(t, id, node.ID)
	require.Equal(t, map[string]any{"key": "one"}, node.Properties)
	node, err = reader.GetNodeProjected(t.Context(), id, []string{"unique-key"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"unique-key": int64(7)}, node.Properties)
	node, err = reader.GetNodeProjected(t.Context(), id, nil)
	require.NoError(t, err)
	require.Len(t, node.Properties, 3)
	_, err = reader.GetNodeProjected(t.Context(), "missing", []string{"key"})
	require.Error(t, err)
}
