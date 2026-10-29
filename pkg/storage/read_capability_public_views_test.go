package storage_test

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Instrumented namespaces must retain embedding-free reads, endpoint visibility
// and a consistent historical graph when used as composite constituents.
func TestReadCapabilityPublicHistoricalAndEmbeddingFreeViews(t *testing.T) {
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	tenant := storage.NewNamespacedEngine(base, "tenant")
	other := storage.NewNamespacedEngine(base, "other")
	for _, id := range []storage.NodeID{"n", "m"} {
		_, err := tenant.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]interface{}{"v": int64(1)}, ChunkEmbeddings: [][]float32{{1, 2}}})
		require.NoError(t, err)
	}
	_, err = other.CreateNode(&storage.Node{ID: "foreign", Labels: []string{"Doc"}})
	require.NoError(t, err)
	require.NoError(t, tenant.CreateEdge(&storage.Edge{ID: "e", StartNode: "n", EndNode: "m", Type: "LINK", Properties: map[string]interface{}{"v": int64(1)}}))
	wal := storage.NewWALEngine(storage.NewTracedEngine(tenant), nil)
	directQueryNode, err := tenant.GetQueryNode("n")
	require.NoError(t, err)
	queryNode, err := wal.GetQueryNode("n")
	require.NoError(t, err)
	require.Equal(t, directQueryNode.ID, queryNode.ID)
	require.Equal(t, directQueryNode.Properties, queryNode.Properties)
	// Query rows share cached vectors; embedding-free reads always strip them.
	require.Equal(t, directQueryNode.ChunkEmbeddings, queryNode.ChunkEmbeddings)
	lightNode, err := wal.GetNodeWithoutEmbeddings("n")
	require.NoError(t, err)
	require.Equal(t, storage.NodeID("n"), lightNode.ID)
	require.Equal(t, int64(1), lightNode.Properties["v"])
	require.Empty(t, lightNode.ChunkEmbeddings)
	require.True(t, wal.BatchGetNodesWithoutEmbeddingsSupported())
	nodes, err := wal.BatchGetNodesWithoutEmbeddings([]storage.NodeID{"n", "m"})
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	for _, node := range nodes {
		require.Empty(t, node.ChunkEmbeddings)
	}
	visible, answered := wal.RelationshipEndpointVisible("n")
	require.True(t, answered)
	require.True(t, visible)
	for _, direction := range []struct {
		id   storage.NodeID
		read func(storage.NodeID) ([]*storage.Edge, bool, error)
	}{{"n", wal.OutgoingEdgeHeaders}, {"m", wal.IncomingEdgeHeaders}} {
		edges, answered, err := direction.read(direction.id)
		require.NoError(t, err)
		require.True(t, answered)
		require.Len(t, edges, 1)
	}
	composite := storage.NewCompositeEngine(map[string]storage.Engine{"tenant": wal, "other": other}, map[string]string{"tenant": "tenant", "other": "other"}, map[string]string{"tenant": "read_write", "other": "read_write"})
	node, err := composite.GetNodeLatestEffective("n")
	require.NoError(t, err)
	require.Equal(t, int64(1), node.Properties["v"])
	for _, read := range []func(storage.EdgeID) (*storage.Edge, error){composite.GetEdgeLatestEffective, composite.GetEdgeLatestVisible} {
		edge, err := read("e")
		require.NoError(t, err)
		require.Equal(t, storage.EdgeID("e"), edge.ID)
	}
	head, err := composite.GetEdgeCurrentHead("e")
	require.NoError(t, err)
	require.False(t, head.Version.IsZero())
	edge, err := composite.GetEdgeVisibleAt("e", head.Version)
	require.NoError(t, err)
	require.Equal(t, int64(1), edge.Properties["v"])
	for _, read := range []func() ([]*storage.Edge, error){
		func() ([]*storage.Edge, error) { return composite.GetOutgoingEdgesVisibleAt("n", head.Version) },
		func() ([]*storage.Edge, error) { return composite.GetIncomingEdgesVisibleAt("m", head.Version) },
		func() ([]*storage.Edge, error) { return composite.GetEdgesByTypeVisibleAt("LINK", head.Version) },
		func() ([]*storage.Edge, error) { return composite.GetEdgesBetweenVisibleAt("n", "m", head.Version) },
	} {
		edges, err := read()
		require.NoError(t, err)
		require.Len(t, edges, 1)
		require.Equal(t, storage.EdgeID("e"), edges[0].ID)
	}
	// Policy values are read through instrumented inner storage, not frozen.
	policyView := storage.NewNamespacedEngine(storage.NewWALEngine(storage.NewTracedEngine(base), nil), "tenant")
	base.SetDecayEnabled(false)
	require.False(t, policyView.IsDecayEnabled())
	base.SetDecayEnabled(true)
	require.True(t, policyView.IsDecayEnabled())
	base.SetDecayEnabled(false)
	require.False(t, policyView.HasManualSuppressions())
	require.NoError(t, base.SetManualSuppression(storage.StoredEntityNodeKind, "tenant:n", true))
	require.True(t, policyView.HasManualSuppressions())
	require.NoError(t, base.SetManualSuppression(storage.StoredEntityNodeKind, "tenant:n", false))
}
