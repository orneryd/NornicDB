package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// apoc.algo.pageRank ranks only the nodes it is given, over the
// relationships between them; the apoc.algo.* signatures declare the
// columns the procedures yield, and pageRank and louvain read their config
// map from the second argument alike (#1164).
func TestApocAlgoRanksTheGivenNodesAndDeclaresWhatItYields(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "apoc_algo"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, `CREATE (a:P {name: 'a'}), (b:P {name: 'b'}), (c:P {name: 'c'}), (d:P {name: 'd'}), (e:P {name: 'e'}),
		(a)-[:K]->(b), (b)-[:K]->(c), (c)-[:K]->(a), (c)-[:K]->(d), (d)-[:K]->(e), (e)-[:K]->(a),
		(a)-[:L]->(:Q {name: 'q'})`, nil)
	require.NoError(t, err)

	for _, query := range []string{
		"MATCH (n:P) WITH collect(n) AS ns CALL apoc.algo.pageRank(ns, {}) YIELD node, score RETURN node.name AS name, score",
		"CALL apoc.algo.pageRank('P') YIELD node, score RETURN node.name AS name, score",
		"CALL apoc.algo.pageRank(['P'], {iterations: 30, dampingFactor: 0.9}) YIELD node, score RETURN node.name AS name, score",
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Len(t, result.Rows, 5, query)
		total := 0.0
		for _, row := range result.Rows {
			require.NotEqual(t, "q", row[0], query)
			total += row[1].(float64)
		}
		require.InDelta(t, 1.0, total, 1e-9, query)
	}

	// The declared columns are the ones yielded.
	result, err := exec.Execute(ctx, "CALL apoc.algo.wcc('P') YIELD node, componentId RETURN count(node) AS n", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(5)}}, result.Rows)
	result, err = exec.Execute(ctx, "SHOW PROCEDURES YIELD name, signature WHERE name IN ['apoc.algo.wcc', 'apoc.algo.louvain'] RETURN name, signature ORDER BY name", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"apoc.algo.louvain", "apoc.algo.louvain(nodes :: ANY, relationshipTypesOrConfig :: ANY, config :: MAP) :: (node :: NODE, community :: INTEGER)"},
		{"apoc.algo.wcc", "apoc.algo.wcc(nodes :: ANY, relationshipTypesOrConfig :: ANY, config :: MAP) :: (node :: NODE, componentId :: INTEGER)"},
	}, result.Rows)
	result, err = exec.Execute(ctx, "CALL apoc.algo.louvain('P', {weightProperty: 'w'}) YIELD node, community RETURN count(node) AS n", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(5)}}, result.Rows)

	// A second argument that is a relationship-type list (the documented
	// relationship-type list, #1169) gives no config.
	result, err = exec.Execute(ctx, "CALL apoc.algo.louvain(['P'], ['K']) YIELD node RETURN count(node) AS n", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(5)}}, result.Rows)
}
