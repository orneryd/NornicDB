package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNamedPathContainsSingleMatchedNode(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "named_path_single_node"))
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE ()", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH path = (node) RETURN path", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	path := requireReturnedPath(t, result.Rows[0][0])
	require.Len(t, path.Nodes, 1)
	require.Empty(t, path.Relationships)
}

func TestNamedPathPreservesRepeatedNodesAcrossUndirectedSegments(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "named_path_repeated_nodes")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	left, err := store.CreateNode(&storage.Node{ID: "left", Labels: []string{"A"}})
	require.NoError(t, err)
	right, err := store.CreateNode(&storage.Node{ID: "right", Labels: []string{"B"}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "forward", Type: "T1", StartNode: left, EndNode: right}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "reverse", Type: "T2", StartNode: right, EndNode: left}))

	result, err := exec.Execute(ctx, "MATCH path = (node)<-->(other)<-->(node) RETURN path", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 4)
	for _, row := range result.Rows {
		path := requireReturnedPath(t, row[0])
		require.Len(t, path.Nodes, 3)
		require.Len(t, path.Relationships, 2)
		require.Equal(t, path.Nodes[0].ID, path.Nodes[2].ID)
	}
}

func TestNamedPathPreservesEveryNodeInFixedLengthSegment(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "named_path_fixed_length")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	start, err := store.CreateNode(&storage.Node{ID: "start", Labels: []string{"Start"}})
	require.NoError(t, err)
	end, err := store.CreateNode(&storage.Node{ID: "end", Labels: []string{"End"}})
	require.NoError(t, err)
	middle, err := store.CreateNode(&storage.Node{ID: "middle"})
	require.NoError(t, err)
	other, err := store.CreateNode(&storage.Node{ID: "other"})
	require.NoError(t, err)
	for _, edge := range []*storage.Edge{
		{ID: "start-middle", Type: "CONNECTED_TO", StartNode: middle, EndNode: start},
		{ID: "middle-end-one", Type: "CONNECTED_TO", StartNode: middle, EndNode: end},
		{ID: "middle-end-two", Type: "CONNECTED_TO", StartNode: middle, EndNode: end},
		{ID: "middle-other-one", Type: "CONNECTED_TO", StartNode: middle, EndNode: other},
		{ID: "middle-other-two", Type: "CONNECTED_TO", StartNode: middle, EndNode: other},
	} {
		require.NoError(t, store.CreateEdge(edge))
	}

	result, err := exec.Execute(ctx, "MATCH path = (:Start)<-[:CONNECTED_TO]-()-[:CONNECTED_TO*3..3]-(:End) RETURN path", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 4)
	for _, row := range result.Rows {
		path := requireReturnedPath(t, row[0])
		require.Len(t, path.Nodes, 5)
		require.Len(t, path.Relationships, 4)
	}
}

func requireReturnedPath(t *testing.T, value interface{}) PathResult {
	t.Helper()
	encoded, ok := value.(map[string]interface{})
	require.True(t, ok, "returned path type: %T", value)
	raw, ok := encoded["_pathResult"]
	require.True(t, ok)
	switch path := raw.(type) {
	case PathResult:
		return path
	case *PathResult:
		require.NotNil(t, path)
		return *path
	default:
		require.FailNow(t, "unexpected embedded path type", "%T", raw)
		return PathResult{}
	}
}
