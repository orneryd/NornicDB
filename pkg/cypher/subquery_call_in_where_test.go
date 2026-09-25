package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestCallSubqueryBodyInWherePredicates verifies EXISTS / COUNT / COLLECT
// subqueries whose body holds a CALL subquery filter rows in WHERE as they
// evaluate in RETURN (#652): through the one subquery evaluator, with the row
// bound. Expected rows are Neo4j 5.26.30's.
func TestCallSubqueryBodyInWherePredicates(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "callwhere"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (a:W {id:'a'})-[:USES]->(b:W {id:'b'}), (c:W {id:'c'}), (a)-[:USES]->(c), (b)-[:USES]->(c)", nil)
	require.NoError(t, err)
	body := "CALL (i) { MATCH (i)-->(o) RETURN o }"
	for _, tc := range []struct {
		where string
		want  []interface{}
	}{
		{"WHERE COUNT { " + body + " RETURN o } > 1", []interface{}{"a"}},
		{"WHERE COUNT { " + body + " RETURN o } >= 1", []interface{}{"a", "b"}},
		{"WHERE EXISTS { " + body + " RETURN o }", []interface{}{"a", "b"}},
		{"WHERE NOT EXISTS { " + body + " RETURN o }", []interface{}{"c"}},
		{"WITH i WHERE COUNT { " + body + " RETURN o } > 1", []interface{}{"a"}},
		{"WHERE size(COLLECT { CALL (i) { MATCH (i)-->(o) RETURN o.id AS x } RETURN x }) > 1", []interface{}{"a"}},
	} {
		query := "MATCH (i:W) " + tc.where + " RETURN i.id AS id ORDER BY id"
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		got := make([]interface{}, 0, len(result.Rows))
		for _, row := range result.Rows {
			got = append(got, row[0])
		}
		require.Equal(t, tc.want, got, query)
	}
}

// TestCountSubqueryDegreeFastPathShapes pins COUNT results for the bodies the
// degree fast path (boundDegreeCount) takes and for the ones it must leave to
// the traversal kernel: a labelled or filtered far end, an undirected hop,
// two hops.
func TestCountSubqueryDegreeFastPathShapes(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "degree"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (a:D {id:'a'})-[:R]->(:D:X {id:'b'})-[:R]->(:D {id:'c'}), (a)-[:S]->(:D {id:'d'}), (:D {id:'e'})-[:R]->(a)", nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		body string
		want int64
	}{
		{"(a)-->()", 2},
		{"MATCH (a)-[:R]->()", 1},
		{"(a)-[:R|S]->()", 2},
		{"(a)<-[:R]-()", 1},
		{"()-[:R]->(a)", 1},
		{"(a)-[r]->()", 2},
		{"(a)-->(:X)", 1},
		{"(a)-->({id: 'd'})", 1},
		{"(a)--()", 3},
		{"(a)-->()-->()", 1},
		{"MATCH (a)-->(m) WHERE m.id = 'b'", 1},
	} {
		result, err := exec.Execute(ctx, "MATCH (a:D {id:'a'}) RETURN COUNT { "+tc.body+" } AS c", nil)
		require.NoError(t, err, tc.body)
		require.Equal(t, [][]interface{}{{tc.want}}, result.Rows, tc.body)
		result, err = exec.Execute(ctx, "MATCH (a:D {id:'a'}) WHERE COUNT { "+tc.body+" } = $n RETURN a.id AS id", map[string]interface{}{"n": tc.want})
		require.NoError(t, err, tc.body)
		require.Len(t, result.Rows, 1, "WHERE form: %s", tc.body)
	}
}
