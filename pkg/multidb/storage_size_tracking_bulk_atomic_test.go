package multidb

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Exercise actual quota rejection at the common transaction owner, rather
// than intercepting a direct engine method that managed writes no longer call.
func TestSizeTrackingEngine_BulkCreateIsAllOrNothing(t *testing.T) {
	manager, db := setupTestManager(t)
	wrapped, err := manager.GetStorage(db)
	require.NoError(t, err)
	initial, _, _ := manager.GetStorageSize(db)
	require.NoError(t, manager.SetDatabaseLimits(db, &Limits{Storage: StorageLimits{MaxNodes: 2}}))
	err = wrapped.BulkCreateNodes([]*storage.Node{
		{ID: "a", Labels: []string{"Doc"}},
		{ID: "b", Labels: []string{"Doc"}},
		{ID: "c", Labels: []string{"Doc"}},
	})
	require.ErrorIs(t, err, ErrStorageLimitExceeded)
	count, err := wrapped.NodeCount()
	require.NoError(t, err)
	require.Zero(t, count, "no node of the rejected batch stays")
	after, _, _ := manager.GetStorageSize(db)
	require.Equal(t, initial, after)

	require.NoError(t, wrapped.BulkCreateNodes([]*storage.Node{
		{ID: "a", Labels: []string{"Doc"}},
		{ID: "b", Labels: []string{"Doc"}},
	}))
	before, _, _ := manager.GetStorageSize(db)
	require.NoError(t, manager.SetDatabaseLimits(db, &Limits{Storage: StorageLimits{MaxEdges: 1}}))
	err = wrapped.BulkCreateEdges([]*storage.Edge{
		{ID: "e1", StartNode: "a", EndNode: "b", Type: "LINK"},
		{ID: "e2", StartNode: "b", EndNode: "a", Type: "LINK"},
	})
	require.ErrorIs(t, err, ErrStorageLimitExceeded)
	edges, err := wrapped.EdgeCount()
	require.NoError(t, err)
	require.Zero(t, edges, "no relationship of the rejected batch stays")
	nodes, err := wrapped.NodeCount()
	require.NoError(t, err)
	require.EqualValues(t, 2, nodes)
	after, _, _ = manager.GetStorageSize(db)
	require.Equal(t, before, after)
}
