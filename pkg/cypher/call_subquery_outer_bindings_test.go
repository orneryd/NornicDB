package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestCallSubqueryImportsAnyOuterVariable: a CALL subquery after MATCH sees
// every variable the MATCH binds, not only its first node, as in Neo4j
// 5.26 (#648).
func TestCallSubqueryImportsAnyOuterVariable(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  [][]interface{}
	}{
		{"MATCH (o:C648 {id:'b'}) CALL (o) { RETURN o.id AS z } RETURN z ORDER BY z", [][]interface{}{{"b"}}},
		{"MATCH (i:C648 {id:'a'})-->(o) CALL (o) { RETURN o.id AS z } RETURN z ORDER BY z", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:C648 {id:'a'})-->(o) CALL (o) { WITH o RETURN o.id AS z } RETURN z ORDER BY z", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:C648 {id:'a'})-->(o) CALL { WITH o RETURN o.id AS z } RETURN z ORDER BY z", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:C648 {id:'a'})-->(o) CALL (o) { RETURN o AS z } RETURN z.id AS z ORDER BY z", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:C648 {id:'a'})-->(o) CALL (i) { RETURN i.id AS z } RETURN z, o.id AS o ORDER BY o", [][]interface{}{{"a", "b"}, {"a", "c"}}},
		{"MATCH (i:C648 {id:'a'})-->(o) WITH o CALL (o) { RETURN o.id AS z } RETURN z ORDER BY z", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:C648 {id:'a'}), (o:C648 {id:'c'}) CALL (o) { RETURN o.id AS z } RETURN i.id AS i, z", [][]interface{}{{"a", "c"}}},
		{"MATCH (n:C648 {id:'a'}) WITH n.id AS i CALL (i) { RETURN i AS j } RETURN j", [][]interface{}{{"a"}}},
		{"MATCH (n:C648 {id:'a'}) WITH n.id AS i CALL { WITH i RETURN i AS j } RETURN j", [][]interface{}{{"a"}}},
		{"MATCH (n:C648 {id:'a'}) OPTIONAL MATCH (n)-[:NONE]->(t) WITH n, t CALL { WITH n, t WITH n, t WHERE t IS NULL RETURN n.id AS j } RETURN j", [][]interface{}{{"a"}}},
		{"MATCH (n:C648 {id:'a'}) WITH n, {k: 'v'} AS m CALL (m) { RETURN m.k AS j } RETURN j", [][]interface{}{{"v"}}},
		{"MATCH (n:C648 {id:'a'}) WITH n, 'b' AS s CALL (s) { RETURN s AS j } RETURN j", [][]interface{}{{"b"}}},
		{"MATCH (n:C648 {id:'a'}) WITH n, elementId(n) AS s CALL (s) { RETURN s AS j } RETURN j = elementId(n) AS j", [][]interface{}{{true}}},
		{"MATCH (n:C648 {id:'a'}) WITH n, {id: elementId(n)} AS m CALL (m) { RETURN m AS j } RETURN j.id = elementId(n) AS j", [][]interface{}{{true}}},
		{"MATCH (n:C648 {id:'a'}) WITH n, [elementId(n)] AS l CALL (l) { RETURN size(l) AS j } RETURN j", [][]interface{}{{int64(1)}}},
		{"MATCH (i:C648 {id:'a'}) RETURN COLLECT { MATCH (i)-->(o) CALL (o) { RETURN o.id AS z } RETURN z ORDER BY z } AS l", [][]interface{}{{[]interface{}{"b", "c"}}}},
	} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call648"))
		ctx := context.Background()
		_, err := exec.Execute(ctx, "CREATE (a:C648 {id:'a'})-[:USES]->(b:C648 {id:'b'}), (a)-[:USES]->(c:C648 {id:'c'})", nil)
		require.NoError(t, err)
		result, err := exec.Execute(ctx, tc.query, nil)
		require.NoError(t, err, tc.query)
		require.Equal(t, tc.want, result.Rows, tc.query)
	}
}
