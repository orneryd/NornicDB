package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// An aggregate may be the argument of a namespaced function, as of any other
// function: the function's name is neither a
// variable nor, by its last segment, an aggregate. An aggregate expression
// that reads a variable it doesn't group by, and an aggregate inside an
// aggregate, are still errors.
func TestAggregateInsideQualifiedFunction(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "aggfn"))
	ctx := context.Background()
	for query, want := range map[string]interface{}{
		"UNWIND [2, 1] AS x RETURN apoc.coll.sort(collect(x)) AS s":                 []interface{}{int64(1), int64(2)},
		"UNWIND ['b', 'a'] AS x RETURN apoc.text.join(collect(x), ',') AS s":        "b,a",
		"UNWIND [2, 1] AS x RETURN apoc.coll.sort([y IN collect(x) | y]) AS s":      []interface{}{int64(1), int64(2)},
		"UNWIND ['b', 'a'] AS x RETURN apoc.coll.sum([count(x)]) AS s":              float64(2),
		"UNWIND [2, 1] AS x WITH apoc.coll.sort(collect(x)) AS s RETURN s":          []interface{}{int64(1), int64(2)},
		"UNWIND [1, 2] AS x RETURN apoc.coll.sum(collect(x)) + count(*) AS s":       float64(5),
		"UNWIND [2, 1] AS x RETURN apoc.coll.sort(collect(x))[0] AS s":              int64(1),
		"UNWIND ['b', 'a'] AS x RETURN size(apoc.coll.toSet(collect(x))) AS s":      int64(2),
		"UNWIND [2, 1] AS x RETURN apoc.coll.sort(collect(DISTINCT x)) AS s":        []interface{}{int64(1), int64(2)},
		"UNWIND [2, 1] AS x RETURN apoc.coll.sort( collect( x ) ) AS s":             []interface{}{int64(1), int64(2)},
		"UNWIND [2, 1] AS x RETURN apoc.coll.sort(collect(x)) AS s ORDER BY s":      []interface{}{int64(1), int64(2)},
		"UNWIND [2, 1] AS x RETURN toString(size(apoc.coll.sort(collect(x)))) AS s": "2",
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
	for _, query := range []string{
		"UNWIND [1, 2] AS x RETURN apoc.coll.sum(collect(x)) + x AS s",
		"UNWIND [1, 2] AS x RETURN count(sum(x)) AS s",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
	}
}
