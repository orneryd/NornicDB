package linkpredict

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestGraphBuilderRelationshipTypes keeps only the relationships of the
// given types (#1185).
func TestGraphBuilderRelationshipTypes(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "test")
	for _, id := range []string{"a", "b", "c"} {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id)})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "K"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ac", StartNode: "a", EndNode: "c", Type: "L"}))

	config := DefaultBuildConfig()
	config.RelationshipTypes = []string{"K"}
	graph, err := NewGraphBuilder(engine, config).Build(context.Background())
	require.NoError(t, err)
	require.Equal(t, NodeSet{"b": {}}, graph["a"])
	require.Empty(t, graph["c"])

	graph, err = NewGraphBuilder(engine, DefaultBuildConfig()).Build(context.Background())
	require.NoError(t, err)
	require.Equal(t, NodeSet{"b": {}, "c": {}}, graph["a"])
}
