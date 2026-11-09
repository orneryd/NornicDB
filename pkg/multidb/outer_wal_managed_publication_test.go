package multidb

import (
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The public manager supplies the real namespacing and accounting preparation
// owners. An outer WAL must journal their accepted commits exactly as it does
// direct native writes; a component-only preparing engine cannot prove this.
func TestOuterWALManagedStoragePublishesRecoverableGraph(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	manager, err := NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	const database = "outer_managed"
	require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: database}))
	require.NoError(t, manager.SetDatabaseLimits(t.Context(), database, &Limits{Storage: StorageLimits{MaxBytes: 1 << 30}}))
	view, err := manager.GetStorage(database)
	require.NoError(t, err)
	dir := t.TempDir()
	wal, err := storage.NewWAL(dir, storage.DefaultWALConfig())
	require.NoError(t, err)
	wrapped := storage.NewWALEngine(view, wal)
	t.Cleanup(func() { require.NoError(t, wrapped.Close()) })
	for _, id := range []storage.NodeID{"a", "b"} {
		returned, createErr := wrapped.CreateNode(t.Context(), &storage.Node{ID: id, Labels: []string{"Item"}})
		require.NoError(t, createErr)
		require.Equal(t, id, returned)
	}
	require.NoError(t, wrapped.CreateEdge(t.Context(), &storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK"}))
	require.NoError(t, wrapped.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: &storage.Node{ID: "a", Labels: []string{"Item"}, Properties: map[string]any{"value": "accepted"}}}))
	require.NoError(t, wrapped.Close())

	recovered, result, err := fixtureowner.OwnResult3[*storage.MemoryEngine, storage.ReplayResult](t, storage.ErrStorageClosed)(storage.RecoverFromWALWithResult(t.Context(), dir, ""))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovered.Close()) })
	require.Zero(t, result.Failed)
	for _, id := range []storage.NodeID{"a", "b"} {
		node, readErr := storage.GetStoredNode(t.Context(), recovered, storage.NodeID(database+":"+string(id)))
		require.NoError(t, readErr)
		require.Equal(t, []string{"Item"}, node.Labels)
		if id == "a" {
			require.Equal(t, "accepted", node.Properties["value"])
		}
	}
	edge, err := storage.GetStoredEdge(t.Context(), recovered, storage.EdgeID(database+":e"))
	require.NoError(t, err)
	require.Equal(t, storage.NodeID(database+":a"), edge.StartNode)
	require.Equal(t, storage.NodeID(database+":b"), edge.EndNode)
	require.Equal(t, "LINK", edge.Type)
}
