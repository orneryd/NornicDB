package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// toString, toStringOrNull and toStringList write lists, maps, nodes,
// relationships and paths as text in a Cypher 25 statement (Neo4j 2026.09);
// a Cypher 5 statement keeps Neo4j 5.26's compile-time SyntaxError,
// run-time TypeError and nulls. A list known to hold one type is
// toString's SyntaxError in both.
func TestCypher25ToStringWritesValues(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "cypher25_to_string"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:TsA:`Ts B` {x: 1})-[:TsR {w: 2}]->(:TsC)", nil)
	require.NoError(t, err)
	const syntaxError, typeError = "Neo.ClientError.Statement.SyntaxError", "Neo.ClientError.Statement.TypeError"
	for _, tc := range []struct {
		query             string
		params            map[string]interface{}
		cypher25, cypher5 interface{}
	}{
		{"RETURN toString([1, 'a']) AS v", nil, "[1, a]", syntaxError},
		{"RETURN toString({b: 'x', a: 1}) AS v", nil, "{a: 1, b: x}", syntaxError},
		{"RETURN toString({`a b`: 1, c: {d: []}}) AS v", nil, "{`a b`: 1, c: {d: []}}", syntaxError},
		{"RETURN toString([0.1, null]) AS v", nil, "[0.1, null]", syntaxError},
		{"RETURN toString([]) AS v", nil, "[]", syntaxError},
		{"RETURN toString([1.0, -0.0, 1e20, 'a\"b', point({x: 1, y: 2})]) AS v", nil, "[1.0, -0.0, 1.0E20, a\"b, point({x: 1.0, y: 2.0, crs: 'cartesian'})]", syntaxError},
		{"RETURN toString([localdatetime('2020-01-02T03:04'), duration('PT1H'), time('12:00+01:00')]) AS v", nil, "[2020-01-02T03:04:00, PT1H, 12:00:00+01:00]", syntaxError},
		{"RETURN toString([1]) AS v", nil, syntaxError, syntaxError},
		{"RETURN toString([[0.1]]) AS v", nil, syntaxError, syntaxError},
		{"WITH {a: 1} AS m RETURN toString(m) AS v", nil, "{a: 1}", syntaxError},
		{"MATCH (n) WITH count(n) AS c WITH c, {a: [1, 'x']} AS m RETURN toString(m) AS v", nil, "{a: [1, x]}", syntaxError},
		{"RETURN toString($p) AS v", map[string]interface{}{"p": []interface{}{int64(1), "a"}}, "[1, a]", typeError},
		{"RETURN toStringOrNull([1]) AS v", nil, "[1]", nil},
		{"RETURN toStringOrNull({a: 1}) AS v", nil, "{a: 1}", nil},
		{"RETURN toStringList([1, null, 'a', [2], {a: 1}]) AS v", nil,
			[]interface{}{"1", "null", "a", "[2]", "{a: 1}"}, []interface{}{"1", nil, "a", nil, nil}},
		{"RETURN toStringList(null) AS v", nil, nil, nil},
		{"MATCH (n:TsA) RETURN toString(n) AS v", nil, "(:TsA:`Ts B`)", syntaxError},
		{"MATCH ()-[r:TsR]->() RETURN toString(r) AS v", nil, "[:TsR]", syntaxError},
		{"MATCH p = (:TsA)-[:TsR]->() RETURN toString(p) AS v", nil, "(:TsA:`Ts B`)-[:TsR]->(:TsC)", syntaxError},
		{"MATCH p = (:TsC)<-[:TsR]-() RETURN toString(p) AS v", nil, "(:TsC)<-[:TsR]-(:TsA:`Ts B`)", syntaxError},
		{"MATCH p = (:TsC) RETURN toString(p) AS v", nil, "(:TsC)", syntaxError},
		{"MATCH (n:TsA) RETURN toString({n: n, l: [n]}) AS v", nil, "{l: [(:TsA:`Ts B`)], n: (:TsA:`Ts B`)}", syntaxError},
		{"MATCH (n:TsA) RETURN toStringOrNull(n) AS v", nil, "(:TsA:`Ts B`)", nil},
	} {
		for prefix, want := range map[string]interface{}{"CYPHER 25 ": tc.cypher25, "": tc.cypher5} {
			query := prefix + tc.query
			result, err := exec.Execute(ctx, query, tc.params)
			if code, isCode := want.(string); isCode && strings.HasPrefix(code, "Neo.") {
				require.Error(t, err, query)
				requireStatusCode(t, err, code)
				continue
			}
			require.NoError(t, err, query)
			require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
		}
	}
}

func TestCypher25ValueTextOfOtherValues(t *testing.T) {
	_, ok := cypher25ValueText(nil)
	require.False(t, ok)
	_, ok = cypher25ValueText(struct{}{})
	require.False(t, ok)
	_, ok = cypher25ValueText([]interface{}{struct{}{}})
	require.False(t, ok)
	_, ok = cypher25ValueText(map[string]interface{}{"a": struct{}{}})
	require.False(t, ok)
	_, ok = cypher25ValueText(map[string]int{"a": 1})
	require.False(t, ok)
	_, ok = cypher25ValueText((*storage.Node)(nil))
	require.False(t, ok)
	_, ok = cypher25ValueText((*storage.Edge)(nil))
	require.False(t, ok)
	start := &storage.Node{ID: "a", Labels: []string{"A"}}
	end := &storage.Node{ID: "b"}
	edge := &storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "T"}
	text, ok := cypher25ValueText(PathResult{Nodes: []*storage.Node{start, end}, Relationships: []*storage.Edge{edge}})
	require.True(t, ok)
	require.Equal(t, "(:A)-[:T]->()", text)
	text, ok = cypher25ValueText(&PathResult{Nodes: []*storage.Node{start}})
	require.True(t, ok)
	require.Equal(t, "(:A)", text)
	text, ok = cypher25ValueText(map[string]interface{}{"_pathResult": &PathResult{Nodes: []*storage.Node{end, start}, Relationships: []*storage.Edge{edge}}})
	require.True(t, ok)
	require.Equal(t, "()<-[:T]-(:A)", text)
	text, ok = cypher25ValueText(map[string]interface{}{"_pathResult": true, "nodes": []interface{}{start, end}, "rels": []interface{}{edge}})
	require.True(t, ok)
	require.Equal(t, "(:A)-[:T]->()", text)
	for _, path := range []interface{}{
		map[string]interface{}{"_pathResult": true},
		map[string]interface{}{"_pathResult": true, "nodes": []interface{}{"x"}},
		map[string]interface{}{"_pathResult": true, "nodes": []interface{}{start, end}, "rels": []interface{}{"x"}},
		PathResult{Nodes: []*storage.Node{start}, Relationships: []*storage.Edge{edge}},
	} {
		_, ok = cypher25ValueText(path)
		require.False(t, ok)
	}
	require.Nil(t, convertToStringInVersion([]interface{}{struct{}{}}, true))
	require.Equal(t, "1", convertToStringInVersion(int64(1), false))
	require.Equal(t, "null", convertToStringInVersion(nil, true))
}
