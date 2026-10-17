package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestApocCollToSetValues pins apoc.coll.toSet as APOC gives it: the first
// occurrence of each value in order, an empty list for an empty list, null
// for null, any list (a parameter's typed one too), and values of different
// types kept apart ('1' and 1).
func TestApocCollToSetValues(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	params := map[string]interface{}{"ints": []int64{3, 1, 3}, "words": []string{"b", "a", "b"}}
	for query, want := range map[string]interface{}{
		"RETURN apoc.coll.toSet([]) AS v":                                  []interface{}{},
		"RETURN apoc.coll.toSet([x IN [1, 2] WHERE x > 5]) AS v":           []interface{}{},
		"RETURN apoc.coll.toSet(null) AS v":                                nil,
		"RETURN apoc.coll.toSet(null) IS NULL AS v":                        true,
		"RETURN apoc.coll.toSet($ints) AS v":                               []interface{}{int64(3), int64(1)},
		"RETURN apoc.coll.toSet($words) AS v":                              []interface{}{"b", "a"},
		"RETURN apoc.coll.toSet([1, '1', 1, true, true, null, null]) AS v": []interface{}{int64(1), "1", true, nil},
		"RETURN apoc.coll.toSet([[1], [1], {a: 1}, {a: 1}]) AS v":          []interface{}{[]interface{}{int64(1)}, map[string]interface{}{"a": int64(1)}},
		"WITH [] AS empty RETURN apoc.coll.toSet(empty) AS v":              []interface{}{},
	} {
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows[0][0], query)
	}
}
