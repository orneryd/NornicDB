package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Slicing a value whose type is only known at run time slices a list of
// that one value; indexing it is a TypeError (Neo4j 5.26.30, #907).
func TestSliceAndIndexOfScalarsMatchNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "slice_scalar"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:B {big: 9007199254740993, s: 'abc', f: 2.5})", nil)
	require.NoError(t, err)
	for _, testCase := range []struct {
		query string
		want  interface{}
	}{
		{"MATCH (n:B) RETURN n.big[1..] AS v", []interface{}{}},
		{"MATCH (n:B) RETURN n.big[..2] AS v", []interface{}{int64(9007199254740993)}},
		{"MATCH (n:B) RETURN n.big[0..1] AS v", []interface{}{int64(9007199254740993)}},
		{"MATCH (n:B) RETURN n.s[1..] AS v", []interface{}{}},
		{"MATCH (n:B) RETURN n.f[..1] AS v", []interface{}{2.5}},
		{"WITH {a: 5} AS m RETURN m.a[..2] AS v", []interface{}{int64(5)}},
		{"WITH [1, 2, 3] AS l, 1 AS i RETURN l[i..] AS v", []interface{}{int64(2), int64(3)}},
		{"WITH [1, 2, 3] AS l RETURN l[-2..] AS v", []interface{}{int64(2), int64(3)}},
		{"WITH [1, 2, 3] AS l RETURN l[..null] AS v", nil},
		{"MATCH (n:B) RETURN n.missing[..1] AS v", nil},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			result, err := exec.Execute(ctx, testCase.query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{testCase.want}}, result.Rows)
		})
	}
	for _, query := range []string{
		"MATCH (n:B) RETURN n.big[0] AS v",
		"MATCH (n:B) RETURN n.s[0] AS v",
		"MATCH (n:B) WITH n WHERE n.big[0] RETURN 1 AS v",
		"MATCH (n:B) WITH n WHERE n.big[-1] RETURN 1 AS v",
		"WITH {a: 5} AS m RETURN m.a[0] AS v",
	} {
		t.Run(query, func(t *testing.T) {
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.TypeError", code)
		})
	}
	_, err = exec.Execute(ctx, "WITH 5 AS x RETURN x[..2] AS v", nil)
	require.Error(t, err)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
}

// A slice bound that fails to evaluate fails the slice; one that isn't an
// integer leaves the slice unhandled (ok false).
func TestRowListSliceBounds(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "slice_bounds"))
	values := map[string]interface{}{"l": []interface{}{int64(1), int64(2)}, "z": int64(0)}
	for _, bounds := range [][2]string{{"1 / z", ""}, {"", "1 / z"}} {
		_, _, err := exec.evaluateRowListSlice(values["l"], bounds[0], bounds[1], values)
		require.Error(t, err, "bounds %q", bounds)
	}
	value, ok, err := exec.evaluateRowListSlice(values["l"], "1", "", values)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []interface{}{int64(2)}, value)

	for _, bounds := range [][2]interface{}{{"a", nil}, {nil, 1.5}} {
		_, ok := cypherListSlice(values["l"], bounds[0], bounds[1], bounds[0] != nil, bounds[1] != nil)
		require.False(t, ok, "bounds %v", bounds)
	}
}
