package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// apoc.coll functions read their arguments after the function's own name
// (extractFuncArgs), not at a hard-coded offset: apoc.coll.subtract,
// .contains and .flatten skipped their first character and returned nothing.
// apoc.coll.sort sorts in Cypher's order, strings included (it compared
// numbers only and left strings as they were).
func TestApocCollFunctionsReadTheirArguments(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "apoc_coll"))
	ctx := context.Background()
	for query, want := range map[string]interface{}{
		"RETURN apoc.coll.sort(['b', 'a', 'c']) AS v":                                                            []interface{}{"a", "b", "c"},
		"WITH split('b a c', ' ') AS a RETURN apoc.coll.sort(a) AS v":                                            []interface{}{"a", "b", "c"},
		"RETURN apoc.coll.sort([3, 1, 2]) AS v":                                                                  []interface{}{int64(1), int64(2), int64(3)},
		"RETURN apoc.coll.sort([2, null, 1]) AS v":                                                               []interface{}{int64(1), int64(2), nil},
		"RETURN apoc.coll.subtract([1, 2, 3, 2], [2]) AS v":                                                      []interface{}{int64(1), int64(3)},
		"RETURN apoc.coll.subtract(['a', 'b', 'c'], ['b']) AS v":                                                 []interface{}{"a", "c"},
		"WITH split('alpha beta gamma', ' ') AS a, split('beta', ' ') AS b RETURN apoc.coll.subtract(a, b) AS v": []interface{}{"alpha", "gamma"},
		"RETURN apoc.coll.contains([1, 2, 3], 2) AS v":                                                           true,
		"RETURN apoc.coll.contains(['a'], 'b') AS v":                                                             false,
		"RETURN apoc.coll.flatten([[1, 2], [3]]) AS v":                                                           []interface{}{int64(1), int64(2), int64(3)},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
}

// The expression evaluator's own copies read their arguments after the
// function's name too (a path or WHERE expression reaches it), and
// apoc.convert.fromJsonList keeps a fraction, nesting and a number too large
// for an INTEGER as JSON gives them.
func TestEvaluatorReadsAPOCArguments(t *testing.T) {
	e := setupTestExecutor(t)
	ctx := context.Background()
	for expression, want := range map[string]interface{}{
		"apoc.coll.flatten([[1, 2], [3]])":                                               []interface{}{int64(1), int64(2), int64(3)},
		"apoc.coll.toSet(context.Background(), [1, 1, 2])":                               []interface{}{int64(1), int64(2)},
		"apoc.coll.sum([1, 2, 3])":                                                       float64(6),
		"apoc.coll.reverse([1, 2])":                                                      []interface{}{int64(2), int64(1)},
		"apoc.map.merge({a: 1}, {b: 2})":                                                 map[string]interface{}{"a": int64(1), "b": int64(2)},
		"apoc.map.fromPairs([['a', 1]])":                                                 map[string]interface{}{"a": int64(1)},
		"apoc.convert.fromJsonList('[1.5, [2], {\"a\": 3}, 1e2, 99999999999999999999]')": []interface{}{1.5, []interface{}{int64(2)}, map[string]interface{}{"a": int64(3)}, float64(100), float64(1e20)},
	} {
		require.Equal(t, want, e.evaluateExpressionWithContext(ctx, expression, nil, nil), expression)
	}
	require.NotNil(t, e.evaluateExpressionWithContext(ctx, "duration('P1D')", nil, nil))
	require.Nil(t, e.evaluateExpressionWithContext(ctx, "apoc.convert.fromJsonMap('[1]')", nil, nil))
}
