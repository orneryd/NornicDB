package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestMapPropertySubscriptInMatchWhere: a subscript on a map variable's
// property (v.h[k]) reads the row's map in a MATCH's own WHERE, a
// variable-length or quantified step's, and a subquery's, as h[k] and the
// post-WITH form do. The WHERE writer keeps a map out of the predicate text,
// and the row evaluator reads a map-literal receiver ({b: true}[k]) whole.
// Expected values recorded on Neo4j 5.26.30.
func TestMapPropertySubscriptInMatchWhere(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "map_property_subscript"))
		ctx := context.Background()
		_, err := exec.Execute(ctx, "CREATE (a:N {id:'a'})-[:R {id:'ab'}]->(b:N {id:'b'})-[:R {id:'bc'}]->(c:N {id:'c'})", nil)
		require.NoError(t, err)
		const walk = "MATCH (:N {id:'a'})-[rs:R*1..2]-(e) WHERE all(x IN rs WHERE v.h[startNode(x).id] IS NULL) RETURN e.id AS id"
		params := map[string]interface{}{"m": map[string]interface{}{"b": true}}
		for query, want := range map[string][][]interface{}{
			"WITH {b: true} AS m MATCH (:N {id:'a'})-[rs:R*1..2]-(e) WHERE all(x IN rs WHERE m[startNode(x).id] IS NULL) RETURN e.id AS id": {{"b"}},
			"MATCH (:N {id:'a'})-[rs:R*1..2]-(e) WHERE all(x IN rs WHERE $m[startNode(x).id] IS NULL) RETURN e.id AS id":                    {{"b"}},
			"WITH {h: {b: true}} AS v " + walk:                                                                                                                                                           {{"b"}},
			"CALL () { RETURN {b: true} AS mm } WITH {h: mm} AS v " + walk:                                                                                                                               {{"b"}},
			"CALL () { RETURN {b: true} AS mm } CALL (mm) { WITH {h: mm} AS v0 RETURN 1 AS one } WITH {h: mm} AS v " + walk:                                                                              {{"b"}},
			"WITH {h: {b: true}} AS v MATCH (:N {id:'a'})-[rs:R*1..2]-(e) WITH v, rs, e WHERE all(x IN rs WHERE v.h[startNode(x).id] IS NULL) RETURN e.id AS id":                                         {{"b"}},
			"WITH {h: {b: true}} AS v WITH v.h AS h MATCH (:N {id:'a'})-[rs:R*1..2]-(e) WHERE all(x IN rs WHERE h[startNode(x).id] IS NULL) RETURN e.id AS id":                                           {{"b"}},
			"WITH {h: {b: true}} AS v MATCH (:N {id:'a'})-[x:R]-(e) WHERE v.h[startNode(x).id] IS NULL RETURN e.id AS id":                                                                                {{"b"}},
			"WITH {a: {b: {k: 'b'}}} AS v MATCH (:N {id:'a'})-[rs:R*1..2]-(e) WHERE all(x IN rs WHERE v.a.b[startNode(x).id] IS NULL) RETURN e.id AS id ORDER BY id":                                     {{"b"}, {"c"}},
			"WITH {h: {ab: true}} AS v MATCH (s:N {id:'a'})-[r:R]->(e)-[r2:R]->(f) WHERE any(k IN [r.id, r2.id] WHERE v.h[k] IS NOT NULL) RETURN count(*) AS c":                                          {{int64(1)}},
			"WITH {h: {ab: true}} AS v MATCH (s:N {id:'a'}) MATCH p = (s)(()-[r:R WHERE v.h[r.id] IS NULL]->()){1,3}(e) RETURN e.id AS id":                                                               {},
			"WITH {h: {bc: true}} AS v MATCH (s:N {id:'a'})-[q:R]->(e) WHERE NOT EXISTS { MATCH (e)-[r:R]->() WHERE v.h[r.id] IS NOT NULL } RETURN count(*) AS c":                                        {{int64(0)}},
			"WITH {h: {bc: true}} AS v MATCH (s:N {id:'a'})-[r:R]->(e) CALL (r, v) { MATCH (e2)-[r2:R]->() WHERE e2.id = endNode(r).id AND v.h[r2.id] IS NULL RETURN count(*) AS n } RETURN sum(n) AS c": {{int64(0)}},
			"WITH {l: ['b']} AS v MATCH (n:N) WHERE v.l[0] = n.id RETURN n.id AS id":                                                                                                                     {{"b"}},
			"WITH {h: {b: true}} AS v MATCH (n:N) WHERE v.h[n.id] IS NULL RETURN n.id AS id ORDER BY id":                                                                                                 {{"a"}, {"c"}},
			"WITH 'c' AS k RETURN {b: true, c: 2}[k] AS v":                                                                                                                                               {{int64(2)}},
		} {
			result, err := exec.Execute(ctx, query, params)
			require.NoError(t, err, query)
			if len(want) == 0 {
				require.Empty(t, result.Rows, query)
				continue
			}
			require.Equal(t, want, result.Rows, query)
		}
	})
}

// rowSubscriptReceiverStart reads a map literal and a map projection as a
// receiver whole, and a subquery from its keyword.
func TestRowSubscriptReceiverStartBraces(t *testing.T) {
	for expression, want := range map[string]string{
		"{b: true}[k]":           "{b: true}",
		"x = {b: true}[k]":       "{b: true}",
		"RETURN {b: true}[k]":    "{b: true}",
		"n {.a}[k]":              "n {.a}",
		"COUNT { MATCH (n) }[0]": "COUNT { MATCH (n) }",
	} {
		open := closingBracketGroupStart(expression)
		start := rowSubscriptReceiverStart(originalBraceView(expression), open)
		require.GreaterOrEqual(t, start, 0, expression)
		require.Equal(t, want, expression[start:open], expression)
	}
	require.Equal(t, -1, rowSubscriptReceiverStart(expressionBraceView("b: true}[k]"), len("b: true}")))
}
