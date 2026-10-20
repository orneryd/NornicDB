package multidb

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestHiddenNodeIDRemainsOccupiedAtAndBelowQuota(t *testing.T) {
	manager, db := setupTestManager(t)
	engine, err := manager.GetStorage(db)
	require.NoError(t, err)
	base := manager.inner.(*storage.MemoryEngine)

	_, err = engine.CreateNode(&storage.Node{
		ID: "hidden", Labels: []string{"Original"}, Properties: map[string]any{"value": int64(1)},
	})
	require.NoError(t, err)
	require.NoError(t, base.SetNodeManuallySuppressed(storage.NodeID(db+":hidden"), true))
	beforeTotal, beforeNodes, beforeEdges := manager.GetStorageSize(db)
	require.Positive(t, beforeNodes)

	// Below a quota, physical identity still wins over query visibility.
	_, err = engine.CreateNode(&storage.Node{ID: "hidden", Labels: []string{"Replacement"}})
	require.ErrorIs(t, err, storage.ErrAlreadyExists)
	require.Equal(t, []string{"Original"}, mustStoredNode(t, engine, "hidden").Labels)

	count, err := engine.NodeCount()
	require.NoError(t, err)
	require.NoError(t, manager.SetDatabaseLimits(db, &Limits{Storage: StorageLimits{MaxNodes: count, MaxBytes: beforeTotal}}))
	_, err = engine.CreateNode(&storage.Node{ID: "hidden", Labels: []string{"Replacement"}})
	require.ErrorIs(t, err, storage.ErrAlreadyExists, "the same occupied ID is not misreported as quota exhaustion")
	_, err = engine.CreateNode(&storage.Node{ID: "fresh", Labels: []string{"Fresh"}})
	require.ErrorIs(t, err, ErrStorageLimitExceeded, "the hidden stored node consumes count and byte quota")

	total, nodes, edges := manager.GetStorageSize(db)
	require.Equal(t, beforeTotal, total)
	require.Equal(t, beforeNodes, nodes)
	require.Equal(t, beforeEdges, edges)
	nodeCount, err := engine.NodeCount()
	require.EqualValues(t, 1, mustCountValue(t, nodeCount, err))

	// One added slot admits a genuinely fresh write; rejection did not poison
	// the accounting cache.
	require.NoError(t, manager.SetDatabaseLimits(db, &Limits{Storage: StorageLimits{MaxNodes: count + 1}}))
	_, err = engine.CreateNode(&storage.Node{ID: "fresh", Labels: []string{"Fresh"}})
	require.NoError(t, err)
	nodeCount, err = engine.NodeCount()
	require.EqualValues(t, 2, mustCountValue(t, nodeCount, err))
}

func TestBulkRollbackDoesNotDeleteIdenticalHiddenEdgeNoOp(t *testing.T) {
	manager, db := setupTestManager(t)
	engine, err := manager.GetStorage(db)
	require.NoError(t, err)
	base := manager.inner.(*storage.MemoryEngine)
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err = engine.CreateNode(&storage.Node{ID: id})
		require.NoError(t, err)
	}
	original := &storage.Edge{
		ID: "hidden", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"key": "same"},
	}
	require.NoError(t, engine.CreateEdge(original))
	require.NoError(t, base.SetEdgeManuallySuppressed(storage.EdgeID(db+":hidden"), true))
	storedBefore := mustStoredEdge(t, engine, "hidden")
	beforeTotal, beforeNodes, beforeEdges := manager.GetStorageSize(db)

	err = engine.BulkCreateEdges([]*storage.Edge{
		{ID: "hidden", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"key": "same"}},
		{ID: "fresh", StartNode: "a", EndNode: "b", Type: "LINK"},
		{ID: "hidden", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"key": "different"}},
	})
	require.ErrorIs(t, err, storage.ErrAlreadyExists)
	require.Equal(t, storedBefore, mustStoredEdge(t, engine, "hidden"), "rollback preserves the predecessor exactly")
	_, err = storage.GetStoredEdge(engine, "fresh")
	require.ErrorIs(t, err, storage.ErrNotFound, "only the genuinely created batch member is compensated")
	edgeCount, err := engine.EdgeCount()
	require.EqualValues(t, 1, mustCountValue(t, edgeCount, err))
	total, nodes, edges := manager.GetStorageSize(db)
	require.Equal(t, beforeTotal, total)
	require.Equal(t, beforeNodes, nodes)
	require.Equal(t, beforeEdges, edges)
}

func mustStoredNode(t *testing.T, engine storage.Engine, id storage.NodeID) *storage.Node {
	t.Helper()
	node, err := storage.GetStoredNode(engine, id)
	require.NoError(t, err)
	return node
}

func mustStoredEdge(t *testing.T, engine storage.Engine, id storage.EdgeID) *storage.Edge {
	t.Helper()
	edge, err := storage.GetStoredEdge(engine, id)
	require.NoError(t, err)
	return edge
}

func mustCountValue(t *testing.T, count int64, err error) int64 {
	t.Helper()
	require.NoError(t, err)
	return count
}
