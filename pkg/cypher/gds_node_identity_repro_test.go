package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestGDSPredictIntegerIDsReadNoStoredNode: an aggregation projection's
// INTEGER ids are not stored node ids, so predict.stream reads no stored
// node's embeddings for them, even when a stored node's id has the same
// text.
func TestGDSPredictIntegerIDsReadNoStoredNode(t *testing.T) {
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(engine)
	ctx := context.Background()
	for _, id := range []string{"1", "3"} {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"P"}, ChunkEmbeddings: [][]float32{{1, 0, 0}}})
		require.NoError(t, err)
	}
	_, err := exec.Execute(ctx, "UNWIND [[1, 2], [2, 3]] AS p RETURN gds.graph.project('ints', p[0], p[1])", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "CALL gds.linkPrediction.predict.stream('ints', {sourceNode: 1, algorithm: 'common_neighbors'}) YIELD node2, semantic_score RETURN node2, semantic_score", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(3), 0.0}}, result.Rows)
}

// TestGDSAggregationIntegerIDOrderIndependent: a node given once as a node
// whose id is "5" and once as the INTEGER 5 is one node, shown as the
// INTEGER whichever row comes first.
func TestGDSAggregationIntegerIDOrderIndependent(t *testing.T) {
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(engine)
	ctx := context.Background()
	_, err := engine.CreateNode(&storage.Node{ID: "5", Labels: []string{"P"}})
	require.NoError(t, err)
	for name, pairs := range map[string]string{"nodeFirst": "[[n, 6], [5, 6]]", "integerFirst": "[[5, 6], [n, 6]]"} {
		_, err := exec.Execute(ctx, "MATCH (n:P) WHERE id(n) = '5' UNWIND "+pairs+" AS p RETURN gds.graph.project('"+name+"', p[0], p[1])", nil)
		require.NoError(t, err, name)
		result, err := exec.Execute(ctx, "CALL gds.louvain.stream('"+name+"') YIELD nodeId RETURN nodeId ORDER BY nodeId", nil)
		require.NoError(t, err, name)
		require.Equal(t, [][]interface{}{{int64(5)}, {int64(6)}}, result.Rows, name)
	}
}
