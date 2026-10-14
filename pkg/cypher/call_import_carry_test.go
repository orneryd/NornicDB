package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A CALL (…) subquery's imports stay bound through its body's WITH
// clauses when it runs, as Neo4j keeps them in scope through the whole
// body: after WITH 1 AS y, MATCH (x)-->(m) reads the imported x, not a new
// one (#1227). MATCH, OPTIONAL MATCH, a pattern in WHERE, MERGE and SET on
// the import after a WITH, an aggregating WITH, and an UNWIND that streams
// its WITHs. Expected values recorded on Neo4j 5.26.30.
func TestCallImportsStayBoundThroughWith(t *testing.T) {
	for index, testCase := range []struct {
		query string
		want  [][]interface{}
	}{
		{"MATCH (x:N {id:'a'}) CALL (x) { WITH 1 AS y MATCH (x)-->(m) RETURN m.id AS v } RETURN v", [][]interface{}{{"b"}}},
		{"MATCH (x:N {id:'c'}) CALL (x) { WITH 1 AS y OPTIONAL MATCH (x)-->(m) RETURN m.id AS v } RETURN v", [][]interface{}{{nil}}},
		{"MATCH (x:N {id:'a'}) CALL (x) { WITH 1 AS y OPTIONAL MATCH (x)-->(m) RETURN m.id AS v } RETURN v", [][]interface{}{{"b"}}},
		{"MATCH (x:N {id:'a'}) CALL (x) { WITH 1 AS y MATCH (m:N) WHERE (x)-->(m) RETURN m.id AS v } RETURN v", [][]interface{}{{"b"}}},
		{"MATCH (x:N {id:'a'}) CALL (x) { WITH 1 AS y MATCH (m:N) WHERE NOT (x)-->(m) AND m <> x RETURN m.id AS v } RETURN v", [][]interface{}{{"c"}}},
		{"MATCH (x:N) CALL (x) { WITH 1 AS y WHERE (x)-->() RETURN x.id AS v } RETURN v ORDER BY v", [][]interface{}{{"a"}, {"b"}}},
		{"MATCH (x:N) CALL (x) { WITH 1 AS y WHERE EXISTS { (x)-->() } RETURN x.id AS v } RETURN v ORDER BY v", [][]interface{}{{"a"}, {"b"}}},
		{"MATCH (x:N {id:'c'}) CALL (x) { WITH 1 AS y MERGE (x)-[:S]->(t:T {id: 'z'}) RETURN t.id AS w } WITH count(*) AS c MATCH (n:N)-[:S]->(:T) RETURN collect(n.id) AS v", [][]interface{}{{[]interface{}{"c"}}}},
		{"MATCH (x:N {id:'a'}) CALL (x) { WITH 1 AS y MERGE (x)-[:R]->(m:N {id:'b'}) RETURN m.id AS v } RETURN v", [][]interface{}{{"b"}}},
		{"MATCH (x:N {id:'b'}) CALL (x) { WITH 1 AS y SET x.p = y RETURN x.p AS w } WITH count(*) AS c MATCH (n:N) WHERE n.p = 1 RETURN collect(n.id) AS v", [][]interface{}{{[]interface{}{"b"}}}},
		{"MATCH (x:N {id:'a'}) CALL (x) { WITH 1 AS y WITH y SET x.p = y RETURN x.p AS v } RETURN v", [][]interface{}{{int64(1)}}},
		{"MATCH (x:N {id:'b'}) CALL (x) { MATCH (n:None) WITH count(*) AS c SET x.q = c RETURN x.q AS w } WITH count(*) AS z MATCH (n:N) WHERE n.q = 0 RETURN collect(n.id) AS v", [][]interface{}{{[]interface{}{"b"}}}},
		{"MATCH (x:N {id:'a'}) CALL (x) { OPTIONAL MATCH (n:None) WITH count(n) AS c MATCH (x)-->(m) RETURN c, m.id AS v } RETURN c, v", [][]interface{}{{int64(0), "b"}}},
		{"MATCH (x:N {id:'a'}) CALL (x) { MATCH (n:None) WITH count(*) AS c RETURN x.id AS i, c } RETURN i, c", [][]interface{}{{"a", int64(0)}}},
		{"MATCH (x:N) CALL (x) { WITH 1 AS y ORDER BY x.id LIMIT 1 RETURN x.id AS v } RETURN v ORDER BY v", [][]interface{}{{"a"}, {"b"}, {"c"}}},
		{"MATCH (x:N {id:'a'}) CALL (*) { WITH 1 AS y MATCH (x)-->(m) RETURN m.id AS v } RETURN v", [][]interface{}{{"b"}}},
		{"MATCH (x:N {id:'a'}) CALL (x) { WITH 1 AS y MATCH (x)-->(m) RETURN m.id AS v UNION WITH 2 AS y MATCH (m)-->(x) RETURN m.id AS v } RETURN v", [][]interface{}{{"b"}}},
		{"MATCH (x:N) CALL (x) { WITH 1 AS y MATCH (x)-->(m) RETURN m.id AS v } RETURN x.id AS i, v ORDER BY i", [][]interface{}{{"a", "b"}, {"b", "c"}}},
		{"MATCH (x:N {id:'a'}) CALL (x) { WITH DISTINCT 1 AS y MATCH (x)-->(m) RETURN m.id AS v } RETURN v", [][]interface{}{{"b"}}},
		{"MATCH (x:N {id:'a'}) CALL (x) { WITH 1 AS y WITH y + 1 AS z MATCH (x)-->(m) RETURN m.id AS v, z } RETURN v, z", [][]interface{}{{"b", int64(2)}}},
		{"MATCH (x:N {id:'a'}) CALL (x) { UNWIND [1, 2] AS k WITH k MATCH (x)-->(m) RETURN k, m.id AS v } RETURN k, v ORDER BY k", [][]interface{}{{int64(1), "b"}, {int64(2), "b"}}},
		{"MATCH (x:N {id:'a'}) CALL (x) { WITH * MATCH (x)-->(m) RETURN m.id AS v } RETURN v", [][]interface{}{{"b"}}},
		{"MATCH (x:N {id:'a'}) CALL (x) { MATCH (m:N {id:'c'}) WITH m MATCH (x)-->(k) RETURN m.id AS mm, k.id AS v } RETURN mm, v", [][]interface{}{{"c", "b"}}},
	} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), fmt.Sprintf("call_import_carry_%d", index)))
		ctx := context.Background()
		_, err := exec.Execute(ctx, "CREATE (:N {id:'a'})-[:R]->(:N {id:'b'})-[:R]->(:N {id:'c'})", nil)
		require.NoError(t, err)
		result, err := exec.Execute(ctx, testCase.query, nil)
		require.NoError(t, err, testCase.query)
		require.Equal(t, testCase.want, result.Rows, testCase.query)
	}

	// The rewrite of a body's WITH (pipelineWithCarrying): the imports it
	// doesn't project, after its own items; WITH * and WITH x leave it.
	carried := pipelineRow{"x": nil, "z": nil}
	require.Equal(t, "WITH 1 AS y, x, z WHERE y > 0", pipelineWithCarrying("WITH 1 AS y WHERE y > 0", carried))
	require.Equal(t, "WITH DISTINCT x, 1 AS y, z ORDER BY y", pipelineWithCarrying("WITH DISTINCT x, 1 AS y ORDER BY y", carried))
	require.Equal(t, "WITH *", pipelineWithCarrying("WITH *", carried))
	require.Equal(t, "WITH x, z AS z", pipelineWithCarrying("WITH x, z AS z", carried))
}
