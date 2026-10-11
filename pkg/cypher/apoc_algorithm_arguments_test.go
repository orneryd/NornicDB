package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// apoc.algo.* read their evaluated arguments: the whole-graph algorithms
// run over a LIST<NODE> (APOC's form), a label, or every node; the path
// algorithms take nodes or node ids (#907).
func TestApocAlgorithmsReadEvaluatedArguments(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(ctx, "CREATE (a:P {name: 'a', lat: 1.0, lon: 1.0})-[:K]->(b:P {name: 'b', lat: 2.0, lon: 2.0})-[:K]->(c:P {name: 'c', lat: 3.0, lon: 3.0}), (:Other {name: 'o'})", nil)
	require.NoError(t, err)

	rows := func(t *testing.T, query string) [][]interface{} {
		t.Helper()
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return result.Rows
	}
	for _, procedure := range []string{"pageRank", "betweenness", "closeness", "louvain", "labelPropagation", "wcc"} {
		require.Len(t, rows(t, "MATCH (n:P) WITH collect(n) AS nodes CALL apoc.algo."+procedure+"(nodes) YIELD node RETURN node.name"), 3, procedure+" over a node list")
		require.Len(t, rows(t, "CALL apoc.algo."+procedure+"('P') YIELD node RETURN node.name"), 3, procedure+" over a label")
		require.Len(t, rows(t, "CALL apoc.algo."+procedure+"(['P']) YIELD node RETURN node.name"), 3, procedure+" over a one-label list")
		require.Len(t, rows(t, "CALL apoc.algo."+procedure+"() YIELD node RETURN node.name"), 4, procedure+" over every node")
		// A value of another type: rejected when the statement is checked (a
		// literal against a STRING signature) or when the call reads it.
		_, err := exec.Execute(ctx, "WITH 1 AS v CALL apoc.algo."+procedure+"(v) YIELD node RETURN node", nil)
		require.ErrorContains(t, err, "must be LIST<NODE> or STRING", procedure)
		_, err = exec.Execute(ctx, "WITH ['P', 'Other'] AS v CALL apoc.algo."+procedure+"(v) YIELD node RETURN node", nil)
		require.ErrorContains(t, err, "must be LIST<NODE> or STRING", procedure)
	}
	require.Len(t, rows(t, "MATCH (n:P) WITH collect(n) AS nodes CALL apoc.algo.pageRank(nodes, {iterations: 5, dampingFactor: 0.5}) YIELD node RETURN node"), 3)
	_, err = exec.Execute(ctx, "MATCH (n:P) WITH collect(n) + [1] AS nodes CALL apoc.algo.pageRank(nodes) YIELD node RETURN node", nil)
	require.ErrorContains(t, err, "must be LIST<NODE> or STRING")

	// Path algorithms: nodes (or node ids); values of another type bound by
	// WITH reach the call and are its TypeError.
	for _, tc := range []struct{ procedure, extra string }{{"dijkstra", ", ''"}, {"aStar", ", '', 'lat', 'lon'"}, {"allSimplePaths", ", 5"}} {
		require.Len(t, rows(t, "MATCH (a:P {name: 'a'}), (c:P {name: 'c'}) CALL apoc.algo."+tc.procedure+"(a, c, 'K'"+tc.extra+") YIELD path RETURN path"), 1, tc.procedure)
		_, err := exec.Execute(ctx, "MATCH (c:P {name: 'c'}) WITH c, null AS s CALL apoc.algo."+tc.procedure+"(s, c, 'K'"+tc.extra+") YIELD path RETURN path", nil)
		require.ErrorContains(t, err, "argument startNode is null", tc.procedure)
		_, err = exec.Execute(ctx, "MATCH (a:P {name: 'a'}) WITH a, 1.5 AS e CALL apoc.algo."+tc.procedure+"(a, e, 'K'"+tc.extra+") YIELD path RETURN path", nil)
		require.ErrorContains(t, err, "argument endNode must be NODE", tc.procedure)
		_, err = exec.Execute(ctx, "MATCH (a:P {name: 'a'}), (c:P {name: 'c'}) WITH a, c, 7 AS k CALL apoc.algo."+tc.procedure+"(a, c, k"+tc.extra+") YIELD path RETURN path", nil)
		require.ErrorContains(t, err, "argument relTypesAndDirections must be STRING", tc.procedure)
	}
	for _, tc := range []struct{ procedure, args, argument string }{
		{"dijkstra", "(a, c, 'K', bad)", "weightPropertyName"},
		{"aStar", "(a, c, 'K', '', bad)", "latPropertyName"},
		{"aStar", "(a, c, 'K', '', 'lat', bad)", "lonPropertyName"},
	} {
		_, err := exec.Execute(ctx, "MATCH (a:P {name: 'a'}), (c:P {name: 'c'}) WITH a, c, 1 AS bad CALL apoc.algo."+tc.procedure+tc.args+" YIELD path RETURN path", nil)
		require.ErrorContains(t, err, "argument "+tc.argument+" must be STRING", tc.procedure)
	}
}

func TestProcedureNodeAndOptionalStringReaders(t *testing.T) {
	id, err := requiredProcedureNodeID("p", []interface{}{&storage.Node{ID: "n1"}}, 0, "node")
	require.NoError(t, err)
	require.Equal(t, storage.NodeID("n1"), id)
	id, err = requiredProcedureNodeID("p", []interface{}{"n2"}, 0, "node")
	require.NoError(t, err)
	require.Equal(t, storage.NodeID("n2"), id)
	_, err = requiredProcedureNodeID("p", nil, 0, "node")
	require.ErrorContains(t, err, "is null")
	_, err = requiredProcedureNodeID("p", []interface{}{int64(1)}, 0, "node")
	require.ErrorContains(t, err, "must be NODE")

	text, err := optionalProcedureString("p", nil, 0, "name", "fallback")
	require.NoError(t, err)
	require.Equal(t, "fallback", text)
	text, err = optionalProcedureString("p", []interface{}{"x"}, 0, "name", "fallback")
	require.NoError(t, err)
	require.Equal(t, "x", text)
	_, err = optionalProcedureString("p", []interface{}{int64(1)}, 0, "name", "fallback")
	require.ErrorContains(t, err, "must be STRING")
}
