package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRequiredMatchRejectsNullBoundEndpointBeforeTraversal(t *testing.T) {
	for _, population := range []int{200, 20000} {
		t.Run(fmt.Sprintf("pairs_%d", population), func(t *testing.T) {
			testRequiredMatchRejectsNullBoundEndpointBeforeTraversal(t, population)
		})
	}
}

func testRequiredMatchRejectsNullBoundEndpointBeforeTraversal(t *testing.T, population int) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "null_bound")
	for i := range population {
		start := storage.NodeID(fmt.Sprintf("a%d", i))
		end := storage.NodeID(fmt.Sprintf("b%d", i))
		_, err := store.CreateNode(t.Context(), &storage.Node{ID: start, Labels: []string{"A417"}, Properties: map[string]any{"id": fmt.Sprintf("a%d", i)}})
		require.NoError(t, err)
		_, err = store.CreateNode(t.Context(), &storage.Node{ID: end, Labels: []string{"B417"}})
		require.NoError(t, err)
		require.NoError(t, store.CreateEdge(t.Context(), &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("r%d", i)), StartNode: start, EndNode: end, Type: "R417"}))
	}
	spy := &adjacencyReadSpy{Engine: store}
	exec := NewStorageExecutor(spy)
	for _, shape := range []struct{ name, query string }{
		{"start", "OPTIONAL MATCH (a:A417 {id: 'missing'}) MATCH (a)-[:R417]->(b) RETURN count(b) AS n"},
		{"end", "OPTIONAL MATCH (a:A417 {id: 'missing'}) MATCH (b)-[:R417]->(a) RETURN count(b) AS n"},
		{"chained interior", "OPTIONAL MATCH (a:A417 {id: 'missing'}) MATCH (b)-[:R417]->(a)-[:R417]->(c) RETURN count(c) AS n"},
		{"relationship", "OPTIONAL MATCH (a:A417 {id: 'missing'})-[r:R417]->(b) MATCH (c)-[r:R417]->(d) RETURN count(d) AS n"},
	} {
		t.Run(shape.name, func(t *testing.T) {
			spy.reset()
			result, err := exec.Execute(context.Background(), shape.query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
			require.Zero(t, spy.calls, "null-bound required MATCH listed adjacency")
			require.Zero(t, spy.entries, "null-bound required MATCH read relationships")
			require.Zero(t, spy.streams, "null-bound required MATCH streamed relationship type")
		})
	}
	spy.reset()
	result, err := exec.Execute(context.Background(), "OPTIONAL MATCH (a:A417 {id: 'a0'}) MATCH (a)-[:R417]->(b) RETURN count(b) AS n", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	require.Positive(t, spy.calls, "the read observer must see a real traversal")

	spy.reset()
	result, err = exec.Execute(context.Background(), "OPTIONAL MATCH (a:A417 {id: 'missing'}) MATCH (a)-[:R417]->(b) RETURN b ORDER BY b.id LIMIT 10", nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)
	require.Zero(t, spy.calls, "null-bound ORDER BY/LIMIT match listed adjacency")
	spy.reset()
	result, err = exec.Execute(context.Background(), "MATCH (a:A417 {id: 'a0'})-[:R417]->(b) RETURN b ORDER BY b.id LIMIT 10", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Positive(t, spy.calls, "the ordered path must traverse a real bound node")
	spy.reset()
	optionalOnly, err := exec.Execute(context.Background(), "MATCH (a:A417 {id: 'a0'}) OPTIONAL MATCH (a)-[r:RMISSING]->(x) RETURN r", nil)
	require.NoError(t, err)
	require.Len(t, optionalOnly.Rows, 1)
	require.Nil(t, optionalOnly.Rows[0][0])
	optionalCalls := spy.calls
	require.Positive(t, optionalCalls, "the optional probe must be observable")
	spy.reset()
	result, err = exec.Execute(context.Background(), "MATCH (a:A417 {id: 'a0'}) OPTIONAL MATCH (a)-[r:RMISSING]->(x) MATCH (a)-[r:R417]->(b) RETURN b ORDER BY b.id LIMIT 10", nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)
	require.Equal(t, optionalCalls, spy.calls, "required MATCH added adjacency work after a null relationship binding")

	result, err = exec.Execute(context.Background(), "UNWIND ['a0','missing'] AS wanted OPTIONAL MATCH (a:A417 {id: wanted}) MATCH (a)-[:R417]->(b) RETURN count(b) AS n", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows, "a null row must not discard its real peer")
	result, err = exec.Execute(context.Background(), "OPTIONAL MATCH (z:Missing) MATCH (a:A417 {id: 'a0'})-[:R417]->(b) RETURN count(b) AS n", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows, "an unrelated null must not reject a real traversal")
}

func TestPipelineNullPatternBinding(t *testing.T) {
	for _, shape := range []struct {
		name string
		row  pipelineRow
		vars []string
		want bool
	}{
		{"start null", pipelineRow{"a": nil}, []string{"a", "b"}, true},
		{"end null", pipelineRow{"b": nil}, []string{"a", "b"}, true},
		{"middle null", pipelineRow{"b": nil}, []string{"a", "b", "c"}, true},
		{"relationship null", pipelineRow{"r": nil}, []string{"a", "b", "r"}, true},
		{"path null", pipelineRow{"p": nil}, []string{"a", "b", "p"}, true},
		{"unbound start", pipelineRow{}, []string{"a", "b"}, false},
		{"unrelated null", pipelineRow{"z": nil}, []string{"a", "b"}, false},
	} {
		t.Run(shape.name, func(t *testing.T) {
			require.Equal(t, shape.want, pipelineNullPatternBinding(shape.row, shape.vars))
		})
	}
}
