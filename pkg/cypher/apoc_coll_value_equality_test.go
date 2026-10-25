package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestApocCollectionsCompareAnyValues pins the built-in apoc.coll functions
// that compare items on the one equality owner (cypherValueIndex,
// cypherValueEquals), as APOC 5.26.0 compares them (core apoc/coll/Coll.java,
// apoc/util/Util.java):
//   - toSet, union, intersection and subtract are sets of AnyValues
//     (ValueUtils.of into a HashSet): 1 and 1.0 are one item, lists and maps
//     compare element by element, a null is one item, every NaN its own;
//   - contains, containsAll, indexOf and occurrences use Util.valueEquals:
//     the same equality, except that a null equals nothing.
//
// Before, items were keyed by their Go type and printed text, so 1 and 1.0
// were two items, and [1] and ['1'] one.
func TestApocCollectionsCompareAnyValues(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
	ctx := context.Background()
	list := func(items ...interface{}) []interface{} { return items }
	for query, want := range map[string]interface{}{
		"RETURN apoc.coll.toSet([[1], ['1'], [1]]) AS v":                             list(list(int64(1)), list("1")),
		"RETURN apoc.coll.toSet([{a: 1}, {a: '1'}, {a: 1.0}]) AS v":                  list(map[string]interface{}{"a": int64(1)}, map[string]interface{}{"a": "1"}),
		"RETURN apoc.coll.toSet([1, 1.0, 2, 2.0]) AS v":                              list(int64(1), int64(2)),
		"RETURN apoc.coll.toSet(['1', 1, null, null]) AS v":                          list("1", int64(1), nil),
		"RETURN apoc.coll.toSet([[1, null], [1.0, null]]) AS v":                      list(list(int64(1), nil)),
		"RETURN size(apoc.coll.toSet([0.0 / 0.0, 0.0 / 0.0])) AS v":                  int64(2),
		"RETURN apoc.coll.union([1, 2], [2.0, 3, '3']) AS v":                         list(int64(1), int64(2), int64(3), "3"),
		"RETURN apoc.coll.intersection([1, 2.0, '3', [4], 1], [2, 1.0, [4.0]]) AS v": list(int64(1), float64(2), list(int64(4))),
		"RETURN apoc.coll.subtract([1, 1, 2, 3.0, [1], '3'], [3, [1.0]]) AS v":       list(int64(1), int64(2), "3"),
		"RETURN apoc.coll.contains([1, 2], 2.0) AS v":                                true,
		"RETURN apoc.coll.contains([[1]], [1.0]) AS v":                               true,
		"RETURN apoc.coll.contains(['1'], 1) AS v":                                   false,
		"RETURN apoc.coll.contains([null], null) AS v":                               false,
		"RETURN apoc.coll.containsAll([1, 2], [2.0, 1]) AS v":                        true,
		"RETURN apoc.coll.containsAll([1], []) AS v":                                 true,
		"RETURN apoc.coll.containsAll([], []) AS v":                                  false,
		"RETURN apoc.coll.containsAll([null, 1], [null]) AS v":                       false,
		"RETURN apoc.coll.containsAny([1, 2], [3, 2.0]) AS v":                        true,
		"RETURN apoc.coll.containsAny([null], [null]) AS v":                          false,
		"RETURN apoc.coll.indexOf([1, 2, 3], 2.0) AS v":                              int64(1),
		"RETURN apoc.coll.indexOf([null], null) AS v":                                int64(-1),
		"RETURN apoc.coll.occurrences([1, 1.0, '1', 2], 1) AS v":                     int64(2),
		"RETURN apoc.coll.occurrences([null, null], null) AS v":                      int64(0),
	} {
		for _, programs := range []bool{true, false} {
			rowValueProgramsDisabled.Store(!programs)
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			require.Equal(t, want, result.Rows[0][0], "programs %v: %s", programs, query)
		}
	}
	rowValueProgramsDisabled.Store(false)

	// A typed list parameter is a list like any other.
	result, err := exec.Execute(ctx, "RETURN apoc.coll.intersection($a, $b) AS i, apoc.coll.indexOf($a, 'y') AS x", map[string]interface{}{
		"a": []string{"x", "y", "x"}, "b": []string{"x"},
	})
	require.NoError(t, err)
	require.Equal(t, []interface{}{list("x"), int64(1)}, result.Rows[0])

	require.Equal(t, list(list(int64(1)), list(int64(2)), list(int64(3))), apocCollSplit(list(int64(1), int64(0), int64(2), float64(0), int64(3)), int64(0)))
	require.Equal(t, list(list(int64(1), nil, int64(2))), apocCollSplit(list(int64(1), nil, int64(2)), nil))
}
