package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestLinkPredictionPredictScores: predict.stream's semantic score reads
// every chunk, its topology score doesn't depend on the other candidates,
// and its reason follows both scores (#1186).
func TestLinkPredictionPredictScores(t *testing.T) {
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(engine)
	ctx := context.Background()
	create := func(id string, chunks [][]float32) {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"P"}, ChunkEmbeddings: chunks})
		require.NoError(t, err)
	}
	link := func(from, to string) {
		require.NoError(t, engine.CreateEdge(&storage.Edge{ID: storage.EdgeID(from + to), StartNode: storage.NodeID(from), EndNode: storage.NodeID(to), Type: "R"}))
	}
	// The match is in each node's second chunk.
	create("a", [][]float32{{1, 0, 0}, {0, 1, 0}})
	create("b", nil)
	create("c", [][]float32{{0, 0, 1}, {0, 1, 0}})
	create("d", nil)
	create("e", nil)
	link("a", "b")
	link("b", "c")
	link("a", "d")
	link("d", "e")

	scores := func() map[string][]interface{} {
		t.Helper()
		result, err := exec.Execute(ctx, "CALL gds.linkPrediction.predict.stream({sourceNode: 'a', algorithm: 'common_neighbors'})", nil)
		require.NoError(t, err)
		rows := make(map[string][]interface{}, len(result.Rows))
		for _, row := range result.Rows {
			rows[row[1].(string)] = row
		}
		return rows
	}
	rows := scores()
	require.Len(t, rows, 2)
	// One common neighbour: 1 - 1/(1 + 1/2).
	require.InDelta(t, 1.0/3, rows["c"][3], 1e-9)
	require.InDelta(t, 1.0, rows["c"][4], 1e-6)
	require.Equal(t, "Moderate structural connection, strong semantic similarity", rows["c"][5])
	require.InDelta(t, 0.0, rows["e"][4], 1e-9)
	require.Equal(t, "Moderate structural connection, no semantic match", rows["e"][5])

	// Another candidate, with two common neighbours, leaves c's score alone.
	create("g", nil)
	link("b", "g")
	link("d", "g")
	rows = scores()
	require.Len(t, rows, 3)
	require.InDelta(t, 1.0/3, rows["c"][3], 1e-9)
	require.InDelta(t, 0.5, rows["g"][3], 1e-9)
	require.Equal(t, "Moderate structural connection, no semantic match", rows["g"][5])
}

func TestNodeEmbeddingGroups(t *testing.T) {
	require.Nil(t, nodeEmbeddingGroups(nil))
	node := &storage.Node{
		NamedEmbeddings: map[string][]float32{"title": {3}, "default": {2}},
		ChunkEmbeddings: [][]float32{{4}, {5}},
	}
	require.Equal(t, map[string][][]float32{"title": {{3}}, "default": {{2}, {4}, {5}}}, nodeEmbeddingGroups(node))
}
