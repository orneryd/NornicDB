package cypher

import (
	"context"
	"strings"
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

// TestSubqueryPredicatesOnEveryRoute pins EXISTS / COUNT / COLLECT subqueries
// in WHERE and RETURN across the statement shapes that reach different WHERE
// evaluators and clause splitters (#652): single-node, cartesian, WITH,
// OPTIONAL MATCH, relationship and named-path patterns, UNWIND … MATCH,
// aggregation, SET, DETACH DELETE and MERGE. Only a whole [NOT] EXISTS { }
// is an existence test; a comparison around a subquery (EXISTS { … } = false,
// 0 = COUNT { … }) is an expression, and a RETURN / CALL inside a subquery
// body is not a clause of the statement. Expected rows are Neo4j 5.26.30's.
func TestSubqueryPredicatesOnEveryRoute(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  [][]interface{}
	}{
		{"MATCH (i:W709p) RETURN i.id AS id, COUNT { (i)-->() } + COUNT { (i)<--() } AS s ORDER BY id", [][]interface{}{{"a", int64(2)}, {"b", int64(1)}, {"c", int64(1)}}},
		{"MATCH (i:W709p) WHERE i.f = EXISTS { (i)-->() } RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}, {"b"}}},
		{"MATCH (i:W709p) WHERE EXISTS { (i)-->() } = false RETURN i.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W709p) WHERE EXISTS { MATCH (i)-->() } = false RETURN i.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W709p) WITH i WHERE EXISTS { (i)-->() } = false RETURN i.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W709p) WHERE EXISTS { (i)-->() } <> true RETURN i.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W709p) WHERE false = EXISTS { (i)-->() } RETURN i.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W709p) WHERE COUNT { (i)-->() } + COUNT { (i)<--() } = 1 RETURN i.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W709p) WHERE 1 = COUNT { (i)-->() } RETURN i.id AS id ORDER BY id", [][]interface{}{}},
		{"MATCH (i:W709p) WHERE 0 < COUNT { (i)-->() } RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}}},
		{"MATCH (i:W709p) WHERE i.f AND COUNT { (i)-->() } > 0 RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}}},
		{"MATCH (i:W709p) WHERE size(COLLECT { MATCH (i)-->(o) RETURN o }) > 1 RETURN i.id AS id", [][]interface{}{{"a"}}},
		{"MATCH (i:W709p)-[r]->(x) WHERE EXISTS { CALL (x) { MATCH (x)<--(y) RETURN y } RETURN y } RETURN x.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W709p)-[r]->(x) WHERE NOT EXISTS { CALL (x) { MATCH (x)-->(y) RETURN y } RETURN y } RETURN x.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W709p)-[r]->(x) WHERE COUNT { CALL (i) { MATCH (i)-->(y) RETURN y } RETURN y } = 2 RETURN x.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W709p)-[r]->(x) WHERE EXISTS { (x)-->() } = false RETURN x.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W709p)-[r]->(x) WHERE NOT  EXISTS { (x)<--(i) } RETURN x.id AS id ORDER BY id", [][]interface{}{}},
		{"MATCH (i:W709p) WHERE EXISTS { (i)-->() } OR i.id = 'c' RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}, {"c"}}},
		{"MATCH (i:W709p) WHERE NOT EXISTS { (i)-->() } AND i.f RETURN i.id AS id ORDER BY id", [][]interface{}{{"c"}}},
		{"MATCH (i:W709p) WHERE NOT  EXISTS { (i)-->() } RETURN i.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W709p) WHERE NOT EXISTS { (i)-->() } = true RETURN i.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W709p) RETURN i.id AS id, size(COLLECT { MATCH (i)-->(o) CALL (o) { RETURN o.id AS z } RETURN z }) AS l ORDER BY id", [][]interface{}{{"a", int64(2)}, {"b", int64(0)}, {"c", int64(0)}}},
		{"MATCH (i:W709p) RETURN i.id AS id, COLLECT { MATCH (i)-->(o) RETURN o.id ORDER BY o.id }[0] AS l ORDER BY id", [][]interface{}{{"a", "b"}, {"b", nil}, {"c", nil}}},
		{"MATCH (i:W709p) RETURN i.id AS id, EXISTS { MATCH (i)-->(o) CALL (o) { RETURN o.id AS one } RETURN o } AS e, COUNT { MATCH (i)-->(o) CALL (o) { RETURN o.id AS one } RETURN o } AS c ORDER BY id", [][]interface{}{{"a", true, int64(2)}, {"b", false, int64(0)}, {"c", false, int64(0)}}},
		{"MATCH (i:W709p) WHERE EXISTS { MATCH (i)-->(o) CALL (o) { RETURN o.id AS one } RETURN o } RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}}},
		{"MATCH (i:W709p)-[r]->(x) WHERE EXISTS { MATCH (i)-->(y) WHERE y.id = 'b' OR y.id = 'zz' } RETURN x.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W709p)-[r]->(x) WHERE EXISTS { MATCH (i)-->(y) WHERE y.id = 'zz' AND y.f } OR x.id = 'c' RETURN x.id AS id ORDER BY id", [][]interface{}{{"c"}}},
		{"MATCH p = (i:W709p)-[r]->(x) WHERE length(p) = COUNT { (x)<--() } RETURN x.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH p = (i:W709p)-[r]->(x) WHERE length(p) = 1 AND NOT EXISTS { (x)-->() } RETURN x.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W709p)-[r]->(x) WHERE COUNT { (i)-->() } = 2 AND x.f RETURN x.id AS id ORDER BY id", [][]interface{}{{"c"}}},
		{"MATCH (i:W709p)-[r]->(x) WHERE 0 = COUNT { (x)-->() } RETURN x.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W709p) WHERE EXISTS { MATCH (i)-->(y) WHERE y.id = 'b' OR y.id = 'zz' } RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}}},
		{"MATCH (i:W709p) WHERE EXISTS { (i)-->() } = false RETURN i.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W709p), (z:W709p {id:'a'}) WHERE EXISTS { (i)-->() } = false RETURN i.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W709p) WITH i WHERE EXISTS { (i)-->() } = false RETURN i.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (z:W709p {id:'a'}) OPTIONAL MATCH (z)-->(i) WHERE EXISTS { (i)-->() } = false RETURN i.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W709p)-[r]-(x) WHERE EXISTS { (i)-->() } = false RETURN DISTINCT i.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"UNWIND ['a','b','c'] AS k MATCH (i:W709p {id:k}) WHERE EXISTS { (i)-->() } = false RETURN i.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W709p) WHERE EXISTS { (i)-->() } = false WITH count(*) AS c RETURN c", [][]interface{}{{int64(2)}}},
		{"MATCH p=(i:W709p)-->() WHERE EXISTS { (i)-->() } = false RETURN DISTINCT i.id AS id ORDER BY id", [][]interface{}{}},
		{"MATCH (i:W709p) WHERE EXISTS { (i)-->() } = false SET i.hit = true RETURN i.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W709p) WHERE EXISTS { (i)-->() } = false DETACH DELETE i RETURN count(*) AS c", [][]interface{}{{int64(2)}}},
		{"MATCH (i:W709p) WHERE EXISTS { (i)-->() } = false RETURN count(i) AS c", [][]interface{}{{int64(2)}}},
		{"MATCH (i:W709p) WITH i, 1 AS one WHERE EXISTS { (i)-->() } = false RETURN i.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MERGE (i:W709p {id:'a'}) WITH i WHERE EXISTS { (i)-->() } = false RETURN i.id AS id", [][]interface{}{}},
		{"MATCH (i:W709p) WHERE COUNT { (i)-->() } = 2 RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}}},
		{"MATCH (i:W709p), (z:W709p {id:'a'}) WHERE COUNT { (i)-->() } = 2 RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}}},
		{"MATCH (i:W709p) WITH i WHERE COUNT { (i)-->() } = 2 RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}}},
		{"MATCH (z:W709p {id:'a'}) OPTIONAL MATCH (z)-->(i) WHERE COUNT { (i)-->() } = 2 RETURN i.id AS id ORDER BY id", [][]interface{}{{nil}}},
		{"MATCH (i:W709p)-[r]-(x) WHERE COUNT { (i)-->() } = 2 RETURN DISTINCT i.id AS id ORDER BY id", [][]interface{}{{"a"}}},
		{"UNWIND ['a','b','c'] AS k MATCH (i:W709p {id:k}) WHERE COUNT { (i)-->() } = 2 RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}}},
		{"MATCH (i:W709p) WHERE COUNT { (i)-->() } = 2 WITH count(*) AS c RETURN c", [][]interface{}{{int64(1)}}},
		{"MATCH p=(i:W709p)-->() WHERE COUNT { (i)-->() } = 2 RETURN DISTINCT i.id AS id ORDER BY id", [][]interface{}{{"a"}}},
		{"MATCH (i:W709p) WHERE COUNT { (i)-->() } = 2 SET i.hit = true RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}}},
		{"MATCH (i:W709p) WHERE COUNT { (i)-->() } = 2 DETACH DELETE i RETURN count(*) AS c", [][]interface{}{{int64(1)}}},
		{"MATCH (i:W709p) WHERE COUNT { (i)-->() } = 2 RETURN count(i) AS c", [][]interface{}{{int64(1)}}},
		{"MATCH (i:W709p) WITH i, 1 AS one WHERE COUNT { (i)-->() } = 2 RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}}},
		{"MERGE (i:W709p {id:'a'}) WITH i WHERE COUNT { (i)-->() } = 2 RETURN i.id AS id", [][]interface{}{{"a"}}},
		{"MATCH (i:W709p) WHERE NOT EXISTS { CALL (i) { MATCH (i)-->(o) RETURN o } RETURN o } RETURN i.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W709p), (z:W709p {id:'a'}) WHERE NOT EXISTS { CALL (i) { MATCH (i)-->(o) RETURN o } RETURN o } RETURN i.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W709p) WITH i WHERE NOT EXISTS { CALL (i) { MATCH (i)-->(o) RETURN o } RETURN o } RETURN i.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (z:W709p {id:'a'}) OPTIONAL MATCH (z)-->(i) WHERE NOT EXISTS { CALL (i) { MATCH (i)-->(o) RETURN o } RETURN o } RETURN i.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W709p)-[r]-(x) WHERE NOT EXISTS { CALL (i) { MATCH (i)-->(o) RETURN o } RETURN o } RETURN DISTINCT i.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"UNWIND ['a','b','c'] AS k MATCH (i:W709p {id:k}) WHERE NOT EXISTS { CALL (i) { MATCH (i)-->(o) RETURN o } RETURN o } RETURN i.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W709p) WHERE NOT EXISTS { CALL (i) { MATCH (i)-->(o) RETURN o } RETURN o } WITH count(*) AS c RETURN c", [][]interface{}{{int64(2)}}},
		{"MATCH p=(i:W709p)-->() WHERE NOT EXISTS { CALL (i) { MATCH (i)-->(o) RETURN o } RETURN o } RETURN DISTINCT i.id AS id ORDER BY id", [][]interface{}{}},
		{"MATCH (i:W709p) WHERE NOT EXISTS { CALL (i) { MATCH (i)-->(o) RETURN o } RETURN o } SET i.hit = true RETURN i.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W709p) WHERE NOT EXISTS { CALL (i) { MATCH (i)-->(o) RETURN o } RETURN o } DETACH DELETE i RETURN count(*) AS c", [][]interface{}{{int64(2)}}},
		{"MATCH (i:W709p) WHERE NOT EXISTS { CALL (i) { MATCH (i)-->(o) RETURN o } RETURN o } RETURN count(i) AS c", [][]interface{}{{int64(2)}}},
		{"MATCH (i:W709p) WITH i, 1 AS one WHERE NOT EXISTS { CALL (i) { MATCH (i)-->(o) RETURN o } RETURN o } RETURN i.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MERGE (i:W709p {id:'a'}) WITH i WHERE NOT EXISTS { CALL (i) { MATCH (i)-->(o) RETURN o } RETURN o } RETURN i.id AS id", [][]interface{}{}},
		{"MATCH (i:W709p) WHERE size(COLLECT { MATCH (i)-->(o) RETURN o.id }) > 1 RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}}},
		{"MATCH (i:W709p), (z:W709p {id:'a'}) WHERE size(COLLECT { MATCH (i)-->(o) RETURN o.id }) > 1 RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}}},
		{"MATCH (i:W709p) WITH i WHERE size(COLLECT { MATCH (i)-->(o) RETURN o.id }) > 1 RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}}},
		{"MATCH (z:W709p {id:'a'}) OPTIONAL MATCH (z)-->(i) WHERE size(COLLECT { MATCH (i)-->(o) RETURN o.id }) > 1 RETURN i.id AS id ORDER BY id", [][]interface{}{{nil}}},
		{"MATCH (i:W709p)-[r]-(x) WHERE size(COLLECT { MATCH (i)-->(o) RETURN o.id }) > 1 RETURN DISTINCT i.id AS id ORDER BY id", [][]interface{}{{"a"}}},
		{"UNWIND ['a','b','c'] AS k MATCH (i:W709p {id:k}) WHERE size(COLLECT { MATCH (i)-->(o) RETURN o.id }) > 1 RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}}},
		{"MATCH (i:W709p) WHERE size(COLLECT { MATCH (i)-->(o) RETURN o.id }) > 1 WITH count(*) AS c RETURN c", [][]interface{}{{int64(1)}}},
		{"MATCH p=(i:W709p)-->() WHERE size(COLLECT { MATCH (i)-->(o) RETURN o.id }) > 1 RETURN DISTINCT i.id AS id ORDER BY id", [][]interface{}{{"a"}}},
		{"MATCH (i:W709p) WHERE size(COLLECT { MATCH (i)-->(o) RETURN o.id }) > 1 SET i.hit = true RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}}},
		{"MATCH (i:W709p) WHERE size(COLLECT { MATCH (i)-->(o) RETURN o.id }) > 1 DETACH DELETE i RETURN count(*) AS c", [][]interface{}{{int64(1)}}},
		{"MATCH (i:W709p) WHERE size(COLLECT { MATCH (i)-->(o) RETURN o.id }) > 1 RETURN count(i) AS c", [][]interface{}{{int64(1)}}},
		{"MATCH (i:W709p) WITH i, 1 AS one WHERE size(COLLECT { MATCH (i)-->(o) RETURN o.id }) > 1 RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}}},
		{"MERGE (i:W709p {id:'a'}) WITH i WHERE size(COLLECT { MATCH (i)-->(o) RETURN o.id }) > 1 RETURN i.id AS id", [][]interface{}{{"a"}}},
		{"MATCH (i:W709p) WHERE 0 = COUNT { (i)<--() } OR i.id = 'b' RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}, {"b"}}},
		{"MATCH (i:W709p), (z:W709p {id:'a'}) WHERE 0 = COUNT { (i)<--() } OR i.id = 'b' RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}, {"b"}}},
		{"MATCH (i:W709p) WITH i WHERE 0 = COUNT { (i)<--() } OR i.id = 'b' RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}, {"b"}}},
		{"MATCH (z:W709p {id:'a'}) OPTIONAL MATCH (z)-->(i) WHERE 0 = COUNT { (i)<--() } OR i.id = 'b' RETURN i.id AS id ORDER BY id", [][]interface{}{{"b"}}},
		{"MATCH (i:W709p)-[r]-(x) WHERE 0 = COUNT { (i)<--() } OR i.id = 'b' RETURN DISTINCT i.id AS id ORDER BY id", [][]interface{}{{"a"}, {"b"}}},
		{"UNWIND ['a','b','c'] AS k MATCH (i:W709p {id:k}) WHERE 0 = COUNT { (i)<--() } OR i.id = 'b' RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}, {"b"}}},
		{"MATCH (i:W709p) WHERE 0 = COUNT { (i)<--() } OR i.id = 'b' WITH count(*) AS c RETURN c", [][]interface{}{{int64(2)}}},
		{"MATCH p=(i:W709p)-->() WHERE 0 = COUNT { (i)<--() } OR i.id = 'b' RETURN DISTINCT i.id AS id ORDER BY id", [][]interface{}{{"a"}}},
		{"MATCH (i:W709p) WHERE 0 = COUNT { (i)<--() } OR i.id = 'b' SET i.hit = true RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}, {"b"}}},
		{"MATCH (i:W709p) WHERE 0 = COUNT { (i)<--() } OR i.id = 'b' DETACH DELETE i RETURN count(*) AS c", [][]interface{}{{int64(2)}}},
		{"MATCH (i:W709p) WHERE 0 = COUNT { (i)<--() } OR i.id = 'b' RETURN count(i) AS c", [][]interface{}{{int64(2)}}},
		{"MATCH (i:W709p) WITH i, 1 AS one WHERE 0 = COUNT { (i)<--() } OR i.id = 'b' RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}, {"b"}}},
		{"MERGE (i:W709p {id:'a'}) WITH i WHERE 0 = COUNT { (i)<--() } OR i.id = 'b' RETURN i.id AS id", [][]interface{}{{"a"}}},
	} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "subqueryroutes"))
		ctx := context.Background()
		_, err := exec.Execute(ctx, "CREATE (a:W709p {id:'a', f:true})-[:USES]->(b:W709p {id:'b', f:false}), (a)-[:USES]->(c:W709p {id:'c', f:true})", nil)
		require.NoError(t, err)
		result, err := exec.Execute(ctx, tc.query, nil)
		require.NoError(t, err, tc.query)
		if strings.Contains(tc.query, "ORDER BY") {
			require.Equal(t, tc.want, result.Rows, tc.query)
		} else {
			require.ElementsMatch(t, tc.want, result.Rows, tc.query)
		}
	}
}

// TestLeadingCountSubqueryInWhere pins a WHERE that starts with COUNT { … }:
// it is a value compared like any other, so it holds for a body correlated
// through a property or a WITH / UNWIND value, an uncorrelated body and a
// CALL UNION body, as the parenthesised form and RETURN do; and a number
// inside a subquery body is not a WHERE syntax error (#652). Expected rows
// are Neo4j 5.26.30's.
func TestLeadingCountSubqueryInWhere(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  [][]interface{}
	}{
		{"MATCH (i:W) WHERE COUNT { MATCH (o:W) WHERE o.n > i.n RETURN o } > 1 RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}}},
		{"MATCH (i:W) WHERE COUNT { MATCH (o:W) WHERE o.n > i.n RETURN o } = 0 RETURN i.id AS id ORDER BY id", [][]interface{}{{"c"}}},
		{"MATCH (i:W) WHERE COUNT { MATCH (o:W) WHERE o.n > i.n } > 1 RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}}},
		{"MATCH (i:W) WHERE (COUNT { MATCH (o:W) WHERE o.n > i.n RETURN o }) > 1 RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}}},
		{"MATCH (i:W) WHERE COUNT { MATCH (o:X) RETURN o } > 0 RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}, {"b"}, {"c"}}},
		{"MATCH (i:W) WHERE COUNT { MATCH (o:W) RETURN o } = 3 RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}, {"b"}, {"c"}}},
		{"MATCH (i:W) WITH i, i.n AS k WHERE COUNT { MATCH (o:W) WHERE o.n > k RETURN o } > 0 RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}, {"b"}}},
		{"UNWIND [1, 2, 3] AS k WITH k WHERE COUNT { MATCH (o:W) WHERE o.n > k RETURN o } > 0 RETURN k ORDER BY k", [][]interface{}{{int64(1)}, {int64(2)}}},
		{"MATCH (i:W) WHERE COUNT { CALL () { RETURN 1 AS one UNION RETURN 2 AS one } RETURN one } = 2 RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}, {"b"}, {"c"}}},
		{"MATCH (i:W) WHERE COUNT { MATCH (i)-[:USES]->(o:W) WHERE o.n > 2 RETURN o } = 1 RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}, {"b"}}},
		{"MATCH (i:W) WHERE EXISTS { MATCH (i)-[:USES]->(o:W) WHERE o.n > 2 } RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}, {"b"}}},
		{"MATCH (i:W) WHERE COUNT { (i)-[:USES]->() } = 2 RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}}},
		{"MATCH (i:W) WHERE COUNT { (i)<--() } > 0 AND i.n > 1 RETURN i.id AS id ORDER BY id", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:W) WHERE size(COLLECT { MATCH (i)-->(o:W) WHERE o.n > 2 RETURN o.n }) = 1 RETURN i.id AS id ORDER BY id", [][]interface{}{{"a"}, {"b"}}},
		{"MATCH (i:W)-[r]->(x) WHERE COUNT { MATCH (o:W) WHERE o.n > x.n RETURN o } = 0 RETURN i.id AS i, x.id AS x ORDER BY i, x", [][]interface{}{{"a", "c"}, {"b", "c"}}},
	} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "leadingcount"))
		ctx := context.Background()
		_, err := exec.Execute(ctx, "CREATE (a:W {id:'a', n:1})-[:USES]->(b:W {id:'b', n:2}), (c:W {id:'c', n:3}), (a)-[:USES]->(c), (b)-[:USES]->(c), (:X)", nil)
		require.NoError(t, err)
		result, err := exec.Execute(ctx, tc.query, nil)
		require.NoError(t, err, tc.query)
		require.Equal(t, tc.want, result.Rows, tc.query)
	}
}
