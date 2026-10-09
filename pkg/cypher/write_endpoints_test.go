package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A relationship that CREATE or MERGE writes to a node variable bound to null
// fails the statement with Neo4j's ArgumentError and writes nothing; it
// doesn't create a node in the variable's place (Neo4j 5.26.30, #907).
func TestRelationshipToMissingNodeMatchesNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "write_endpoints"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Q {id: 1})-[:R]->(:Q {id: 2})", nil)
	require.NoError(t, err)
	count := func(query string) int64 {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		return result.Rows[0][0].(int64)
	}
	for _, query := range []string{
		"OPTIONAL MATCH (x:Nope) CREATE (:W)-[:T]->(x) RETURN 1 AS v",
		"OPTIONAL MATCH (x:Nope) MERGE (x)-[:T]->(y:W) RETURN y",
		"OPTIONAL MATCH (x:Nope) WITH x CREATE (x)-[:T]->(:W) RETURN 1 AS v",
		"OPTIONAL MATCH (x:Nope) CREATE p = (x)-[:T]->(:W) RETURN p",
		"UNWIND [1, 9] AS k OPTIONAL MATCH (x:Q {id: k}) CREATE (x)-[:T]->(:W) RETURN count(*) AS c",
		"UNWIND [1, 9] AS k OPTIONAL MATCH (x:Q {id: k}) MERGE (x)-[:T]->(:W) RETURN count(*) AS c",
		"OPTIONAL MATCH (x:Nope) FOREACH (i IN [1] | CREATE (x)-[:T]->(:W)) RETURN 1 AS v",
		"OPTIONAL MATCH (x:Nope) MERGE (y:W {a: 1})-[:T]->(x) RETURN 1 AS v",
		"OPTIONAL MATCH (x:Nope), (y:Nope2) CREATE (x)-[:T]->(y) RETURN 1 AS v",
		"OPTIONAL MATCH (x:Nope) MATCH (q:Q {id: 1}) CREATE (q)-[:T]->(x) RETURN 1 AS v",
		"OPTIONAL MATCH (x:Nope) MATCH (q:Q {id: 1}) MERGE (q)-[:T]->(x) RETURN 1 AS v",
		"OPTIONAL MATCH (x:Nope) MATCH (q:Q {id: 1}) MERGE (q)-[r:T]->(x) RETURN 1 AS v",
		"OPTIONAL MATCH (x:Nope) MATCH (q:Q {id: 1}) CREATE (q)-[:T]->(x)-[:T]->(:W) RETURN 1 AS v",
	} {
		t.Run(query, func(t *testing.T) {
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.ArgumentError", code)
			require.Equal(t, int64(2), count("MATCH (n) RETURN count(n) AS c"))
			require.Equal(t, int64(1), count("MATCH ()-[r]->() RETURN count(r) AS c"))
		})
	}
	// A bound endpoint, and one the pattern creates, still work.
	result, err := exec.Execute(ctx, "OPTIONAL MATCH (x:Q {id: 1}) CREATE (x)-[:T]->(w:W) RETURN w IS NOT NULL AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{true}}, result.Rows)
	result, err = exec.Execute(ctx, "OPTIONAL MATCH (x:Q {id: 2}) MERGE (x)-[:T]->(w:W {k: 1}) RETURN w.k AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
}

func TestMissingRelationshipEndpoint(t *testing.T) {
	var node *storage.Node
	bindings := map[string]interface{}{"x": nil, "typed": node, "n": &storage.Node{ID: "n"}, "i": int64(1)}
	require.True(t, missingRelationshipEndpoint(bindings, "x"))
	require.True(t, missingRelationshipEndpoint(bindings, "typed"))
	require.False(t, missingRelationshipEndpoint(bindings, "n"))
	require.False(t, missingRelationshipEndpoint(bindings, "i"))
	require.False(t, missingRelationshipEndpoint(bindings, "unbound"))
	require.False(t, missingRelationshipEndpoint(bindings, ""))
}
