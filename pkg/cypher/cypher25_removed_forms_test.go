package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Forms Cypher 25 removed, as Neo4j 2026.09 rejects them: SET x = e and
// SET x += e for a node or relationship e, and a property value reading an
// entity the same CREATE (any of its patterns) or MERGE creates. A Cypher 5
// statement keeps them (Neo4j 5.26).
func TestCypher25RemovedForms(t *testing.T) {
	setup := "CREATE (:RfQ {id: 1, a: 1})-[:RfR {w: 2}]->(:RfQ {id: 2, b: 3})"
	for query, want := range map[string]interface{}{
		"MATCH (n:RfQ {id: 1})-[r:RfR]->(o) SET n = o RETURN n.b AS v":                        int64(3),
		"MATCH (n:RfQ {id: 1})-[r:RfR]->(o) SET n += r RETURN n.w AS v":                       int64(2),
		"MATCH (n:RfQ {id: 1})-[r:RfR]->(o) SET r = n RETURN r.id AS v":                       int64(1),
		"CREATE (a:RfW {x: 1}), (b:RfW {x: a.x + 1}) RETURN b.x AS v":                         int64(2),
		"CYPHER 25 MATCH (n:RfQ {id: 1})-[r:RfR]->(o) SET n += properties(o) RETURN n.b AS v": int64(3),
		"CYPHER 25 MATCH (o:RfQ {id: 2}) CREATE (a:RfW {x: o.b}) RETURN a.x AS v":             int64(3),
	} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "cypher25_removed"))
		_, err := exec.Execute(context.Background(), setup, nil)
		require.NoError(t, err)
		result, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "cypher25_removed_errors"))
	_, err := exec.Execute(context.Background(), setup, nil)
	require.NoError(t, err)
	for _, query := range []string{
		"CYPHER 25 MATCH (n:RfQ {id: 1})-[r:RfR]->(o) SET n = o RETURN n.b AS v",
		"CYPHER 25 MATCH (n:RfQ {id: 1})-[r:RfR]->(o) SET n = r RETURN n.w AS v",
		"CYPHER 25 MATCH (n:RfQ {id: 1})-[r:RfR]->(o) SET n += r RETURN n.w AS v",
		"CYPHER 25 MATCH (n:RfQ {id: 1})-[r:RfR]->(o) SET r = n RETURN r.id AS v",
		"CYPHER 25 CREATE (a:RfW {x: 1}), (b:RfW {x: a.x + 1}) RETURN b.x AS v",
		"CYPHER 25 MERGE (a:RfW {foo: 1})-[:RfT]->(b:RfW {foo: a.foo}) RETURN b.foo AS v",
		"CREATE (a:RfW {x: 1})-[:RfT]->(b:RfW {x: a.x}) RETURN b.x AS v",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.Error(t, err, query)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
	// Nothing was written by the rejected statements.
	result, err := exec.Execute(context.Background(), "MATCH (n:RfW) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
}
