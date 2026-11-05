package multidb

import (
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStoredCountProjectionMatchesFullUsageFold(t *testing.T) {
	before, after := &storage.Node{ID: "test:old"}, &storage.Node{ID: "test:new"}
	before.SetRuntimeMetadata(storage.NodeRuntimeMetadata{VisibilitySuppressed: true})
	after.SetRuntimeMetadata(storage.NodeRuntimeMetadata{VisibilitySuppressed: true})
	changes := []storage.StoredEntityChange{{BeforeNode: before}, {AfterNode: after}, {BeforeEdge: &storage.Edge{ID: "test:gone", StartNode: "test:old", EndNode: "test:end"}}, {AfterNode: &storage.Node{ID: "test-other:foreign"}}, {AfterEdge: &storage.Edge{ID: "foreign:edge"}}}
	nodes, edges := storage.StoredEntityCountDelta("test", changes)
	require.Zero(t, nodes)
	require.EqualValues(t, -1, edges)
	for _, bytes := range []bool{false, true} {
		usage, err := storageChangesUsage("test", changes, bytes)
		require.NoError(t, err)
		require.Equal(t, nodes, usage.nodes)
		require.Equal(t, edges, usage.edges)
	}
	changes[0].BeforeNode.Properties = map[string]any{"unsupported": make(chan int)}
	_, err := storageChangesUsage("test", changes, false)
	require.NoError(t, err)
	_, err = storageChangesUsage("test", changes, true)
	require.Error(t, err)
}
