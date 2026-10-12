package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A procedure argument whose static type its parameter can't take is a
// compile-time SyntaxError: a literal, a value bound by WITH, a pattern
// variable. A value bound by UNWIND is checked only when the call runs.
// Recorded on Neo4j 5.26.30 (#907).
func TestProcedureArgumentStaticTypes(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	_, err := exec.Execute(ctx, "CREATE (:S)-[:R]->(:S)", nil)
	require.NoError(t, err)

	for query, message := range map[string]string{
		"WITH 1.5 AS v CALL db.resampleIndex(v)":                "expected String but was Float",
		"CALL db.resampleIndex(1.5)":                            "expected String but was Float",
		"CALL db.resampleIndex(1 + 0.5)":                        "expected String but was Float",
		"CALL db.resampleIndex([1, 2])":                         "expected String but was List<T>",
		"CALL db.resampleIndex([1, 1.5])":                       "expected String but was List<T>",
		"CALL db.resampleIndex(['a'])":                          "expected String but was List<String>",
		"CALL db.resampleIndex([[1]])":                          "expected String but was List<List<T>>",
		"CALL db.resampleIndex([{a: 1}])":                       "expected String but was List<Map>, List<Node> or List<Relationship>",
		"CALL db.awaitIndex('x', 1.5)":                          "expected Integer but was Float",
		"CALL db.awaitIndex('x', true)":                         "expected Integer but was Boolean",
		"WITH 'a' AS s CALL db.awaitIndex('x', s + 1)":          "expected Integer but was String",
		"MATCH (n:S) CALL db.resampleIndex(n)":                  "expected String but was Node",
		"MATCH ()-[r:R]->() CALL db.resampleIndex(r)":           "expected String but was Relationship",
		"MATCH p = (:S)-->() CALL db.resampleIndex(p)":          "expected String but was Path",
		"WITH 1.5 AS v WITH v AS w CALL db.resampleIndex(w)":    "expected String but was Float",
		"WITH 1.5 AS v WITH * CALL db.resampleIndex(v)":         "expected String but was Float",
		"WITH 1 AS v CALL db.resampleIndex(v) YIELD x RETURN x": "expected String but was Integer",
		"WITH 1.5 AS v CALL db.index.vector.queryNodes('missing_vec', v, [1.0, 2.0]) YIELD node RETURN node": "expected Integer but was Float",
		"WITH [1, 2] AS v CALL db.index.fulltext.queryNodes(v, 'x') YIELD node RETURN node":                  "expected String but was List<T>",
		"WITH {a: 1} AS v CALL db.index.vector.createNodeIndex('sweep_vec', 'Q', 'emb', v, 'cosine')":        "expected Integer but was Map",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		var semanticError *SemanticError
		require.ErrorAs(t, err, &semanticError, query)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", semanticError.Code, query)
		require.Contains(t, err.Error(), "Type mismatch: "+message, query)
	}

	// Checked when the call runs, or not a type error at all.
	for _, query := range []string{
		"UNWIND [1.5] AS v CALL db.resampleIndex(v)",
		"UNWIND [1.5] AS v WITH v AS w CALL db.resampleIndex(w)",
		"WITH null AS v CALL db.resampleIndex(v)",
		"CALL db.awaitIndex('x', 1 + 1)",
		"WITH 2 AS k CALL db.index.vector.queryNodes('missing_vec', k, [1.0, 2.0]) YIELD node RETURN node",
		"UNWIND [1.5] AS v WITH 1.5 AS w, v CALL db.resampleIndex(v)",
	} {
		_, err := exec.Execute(ctx, query, nil)
		if err != nil {
			require.NotContains(t, err.Error(), "Type mismatch", query)
		}
	}
}

func TestProcedureParameterAcceptsStaticType(t *testing.T) {
	for _, tc := range []struct {
		parameter, typeName, expected string
		accepted                      bool
	}{
		{"STRING", "String", "String", true},
		{"STRING", "Integer", "String", false},
		{"BOOLEAN", "Boolean", "Boolean", true},
		{"BOOLEAN", "String", "Boolean", false},
		{"INTEGER", "Float", "Integer", false},
		{"FLOAT", "Integer", "Float", true},
		{"NUMBER", "Float", "Number", true},
		{"NUMBER", "String", "Number", false},
		{"STRING", "Float, Integer, String or List<T>", "String", true},
		{"INTEGER", "Any", "Integer", true},
		{"MAP", "String", "", true},
		{"LIST<NODE>", "String", "", true},
	} {
		expected, accepted := procedureParameterAcceptsStaticType(tc.parameter, tc.typeName)
		require.Equal(t, tc.accepted, accepted, tc)
		require.Equal(t, tc.expected, expected, tc)
	}
	require.Equal(t, "List<T>", procedureArgumentTypeName("List<Float>, List<Integer> or List<Number>"))
	require.Equal(t, "List<List<T>>", procedureArgumentTypeName("List<List<Integer>>"))
	require.Equal(t, "List<String>", procedureArgumentTypeName("List<String>"))
}

func TestProjectUnwoundValues(t *testing.T) {
	unwound := map[string]struct{}{"v": {}, "u": {}}
	require.Nil(t, projectUnwoundValues(nil, "WITH v AS w"))
	require.Equal(t, map[string]struct{}{"w": {}}, projectUnwoundValues(unwound, "WITH v AS w, 1 AS x"))
	require.Equal(t, map[string]struct{}{"v": {}}, projectUnwoundValues(unwound, "WITH v"))
	require.Equal(t, unwound, projectUnwoundValues(unwound, "WITH *"))
	require.Nil(t, projectUnwoundValues(unwound, "WITH 1.5 AS v"))
}
