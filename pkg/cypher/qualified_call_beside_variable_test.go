package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A name followed by an argument list is a function call, whatever variables
// are in scope: text.locate(…) beside a variable
// named text, apoc.coll.sort(…) beside one named apoc. A property of the
// variable, or of the call's result, is still a property.
func TestQualifiedCallBesideVariable(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "qcall"))
	ctx := context.Background()
	span := map[string]interface{}{"start": int64(0), "end": int64(7)}
	for query, want := range map[string]interface{}{
		"WITH 'Invoice 42' AS text RETURN text.locate(text, 'Invoice', {}) AS v":                 span,
		"WITH 'Invoice 42' AS text RETURN text.locate(text, 'Invoice') AS v":                     span,
		"UNWIND ['Invoice 42'] AS text RETURN text.locate(text, 'Invoice') AS v":                 span,
		"WITH 'Invoice 42' AS text RETURN text.locate(text, 'Invoice').end AS v":                 int64(7),
		"WITH [3, 1] AS apoc RETURN apoc.coll.sort(apoc) AS v":                                   []interface{}{int64(1), int64(3)},
		"WITH [3, 1] AS apoc WITH apoc.coll.sort(apoc) AS sorted RETURN sorted AS v":             []interface{}{int64(1), int64(3)},
		"WITH {locate: 5} AS text RETURN text.locate AS v":                                       int64(5),
		"WITH 'Invoice 42' AS text WHERE text.locate(text, 'Invoice') IS NOT NULL RETURN 1 AS v": int64(1),
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
	_, err := exec.Execute(ctx, "WITH 'abc' AS text RETURN text.upper AS v", nil)
	require.Error(t, err, "a property of a string is still a type error")

	require.True(t, isQualifiedFunctionCall("text.locate(a, b)"))
	require.True(t, isQualifiedFunctionCall("apoc.coll.sort (x)"))
	require.False(t, isQualifiedFunctionCall("upper(x)"), "not qualified")
	require.False(t, isQualifiedFunctionCall("text.locate(a).end"), "a property of the call")
	require.False(t, isQualifiedFunctionCall("text.locate"))
	require.False(t, isQualifiedFunctionCall("f(a) + g.h(b)"))
	require.False(t, isQualifiedFunctionCall("a.(b)"), "a dot without a name after it")
	require.False(t, isQualifiedFunctionCall("(a).b(c)"), "not a name first")
}
