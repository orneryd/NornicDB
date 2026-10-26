package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A list comprehension's elements are its projection's, typed with its
// local as an element of the list it iterates: UNWIND over
// [at IN range(0, 1) | [at]] binds a list, which a subscript reads, not
// the Boolean that at IN range(0, 1) | [at] read as one list-literal element
// gave it. Expected values recorded on Neo4j 5.26.30.
func TestListComprehensionElementTypes(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "comprehension_element_types"))
	ctx := context.Background()
	rows := func(t *testing.T, query string, params map[string]interface{}) [][]interface{} {
		t.Helper()
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err, query)
		return result.Rows
	}
	list := func(values ...interface{}) []interface{} { return values }

	readings := map[string]interface{}{"readings": list(map[string]interface{}{
		"reading": map[string]interface{}{"party_ids": list("a", "b"), "party_versions": list(int64(1), int64(2))},
	})}
	for _, testCase := range []struct {
		query  string
		params map[string]interface{}
		want   [][]interface{}
	}{
		{"UNWIND [at IN range(0, 1) | [at]] AS e RETURN collect(e[0]) AS v", nil, [][]interface{}{{list(int64(0), int64(1))}}},
		{"UNWIND [at IN range(0, 1) | [at, 'p']] AS e WITH DISTINCT e[1] AS v RETURN collect(v) AS v", nil, [][]interface{}{{list("p")}}},
		{"CREATE (r:Tmp {ids: ['a'], vs: [1]}) WITH r UNWIND [at IN range(0, size(r.ids) - 1) | ['p', r.ids[at], r.vs[at]]] AS entry WITH DISTINCT entry[1] AS e RETURN collect(e) AS v", nil, [][]interface{}{{list("a")}}},
		{"UNWIND $readings AS item WITH item.reading AS r UNWIND [at IN range(0, size(coalesce(r.party_ids, [])) - 1) | ['party', r.party_ids[at], r.party_versions[at]]] AS entry WITH DISTINCT entry[1] AS element RETURN collect(element) AS v", readings, [][]interface{}{{list("a", "b")}}},
		{"WITH [{reading: {party_ids: ['a', 'b'], party_versions: [1, 2]}}] AS readings UNWIND readings AS item WITH item.reading AS r UNWIND [at IN range(0, size(coalesce(r.party_ids, [])) - 1) | ['party', r.party_ids[at], r.party_versions[at]]] AS entry WITH DISTINCT entry[1] AS element RETURN collect(element) AS v", nil, [][]interface{}{{list("a", "b")}}},
		// Controls: a list literal of lists, and two comprehensions joined.
		{"UNWIND [[0, 'p'], [1, 'p']] AS e WITH DISTINCT e[1] AS v RETURN collect(v) AS v", nil, [][]interface{}{{list("p")}}},
		{"UNWIND [at IN range(0, 1) | [at]] + [at IN range(2, 3) | [at]] AS e RETURN collect(e[0]) AS v", nil, [][]interface{}{{list(int64(0), int64(1), int64(2), int64(3))}}},
		// A comprehension without a projection is a list of its elements;
		// a projection of a Boolean or a map is a list of that.
		{"UNWIND [at IN range(0, 3) WHERE at > 1] AS e RETURN collect(e + 1) AS v", nil, [][]interface{}{{list(int64(3), int64(4))}}},
		{"UNWIND [at IN [[1], [2]] WHERE size(at) > 0] AS e RETURN collect(e[0]) AS v", nil, [][]interface{}{{list(int64(1), int64(2))}}},
		{"UNWIND [at IN range(0, 1) | at > 0] AS e RETURN collect(NOT e) AS v", nil, [][]interface{}{{list(true, false)}}},
		{"UNWIND [at IN range(0, 1) | {k: at}] AS e RETURN collect(e.k) AS v", nil, [][]interface{}{{list(int64(0), int64(1))}}},
		{"WITH [at IN range(0, 1) | [at]] AS l RETURN l[0][0] AS v", nil, [][]interface{}{{int64(0)}}},
		{"RETURN [at IN range(0, 1) | [at]][1][0] AS v", nil, [][]interface{}{{int64(1)}}},
	} {
		require.Equal(t, testCase.want, rows(t, testCase.query, testCase.params), testCase.query)
	}
	// A list's property is a type error before the statement runs, as in
	// Neo4j: the element is a List<Integer>.
	_, err := exec.Execute(ctx, "UNWIND [at IN range(0, 1) | [at]] AS e RETURN collect(e.k) AS v", nil)
	require.Error(t, err)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
	require.Contains(t, err.Error(), "Type mismatch")

	scope := staticTypeScope{}
	require.Equal(t, "List<Integer>", listElementOperand(typeScopeOf(scope), scope, originalBraceView("[at IN range(0, 1) | [at]]")).typeName())
	require.Equal(t, "Integer", listElementOperand(typeScopeOf(scope), scope, originalBraceView("[at IN range(0, 3) WHERE at > 1]")).typeName())
	require.Equal(t, "List<T>", listElementOperand(typeScopeOf(scope), scope, originalBraceView("[at IN x | [at, 'p']]")).typeName())
}
