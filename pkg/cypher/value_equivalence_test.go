package cypher

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCypherEquivalenceKey(t *testing.T) {
	equivalent := [][2]interface{}{
		{int64(1), 1.0},
		{1, int64(1)},
		{int32(7), uint8(7)},
		{float32(2.5), 2.5},
		{0.0, math.Copysign(0, -1)},
		{int64(0), math.Copysign(0, -1)},
		{nil, nil},
		{[]interface{}{int64(1), "a"}, []interface{}{1.0, "a"}},
		{[]int64{1, 2}, []interface{}{1.0, 2.0}},
		{map[string]interface{}{"a": int64(1), "b": nil}, map[string]interface{}{"b": nil, "a": 1.0}},
		{map[interface{}]interface{}{"a": 1}, map[string]interface{}{"a": 1.0}},
		{&storage.Node{ID: "n1", Labels: []string{"A"}}, &storage.Node{ID: "n1"}},
		{&storage.Edge{ID: "r1"}, &storage.Edge{ID: "r1", Type: "R"}},
		{(*storage.Node)(nil), nil},
		{(*storage.Edge)(nil), nil},
		{math.Ldexp(1, 63), uint64(1) << 63},
		{float64(math.MinInt64), int64(math.MinInt64)},
		{[]interface{}{[]interface{}{1}}, []interface{}{[]interface{}{1.0}}},
	}
	for _, pair := range equivalent {
		require.Equal(t, cypherEquivalenceKey(pair[0]), cypherEquivalenceKey(pair[1]), "%#v ≡ %#v", pair[0], pair[1])
	}

	different := [][2]interface{}{
		{int64(9007199254740993), 9007199254740992.0},
		{uint64(math.MaxUint64), math.Ldexp(1, 64)},
		{1.5, int64(1)},
		{"1", int64(1)},
		{"N", nil},
		{true, false},
		{"Bt", true},
		{[]byte("a"), "a"},
		{[]byte{1}, []interface{}{1}},
		{math.Inf(1), math.Inf(-1)},
		{math.Inf(1), math.MaxFloat64},
		{math.NaN(), 0.0},
		{math.NaN(), math.NaN()},
		{[]interface{}{math.NaN()}, []interface{}{math.NaN()}},
		{&storage.Node{ID: "x"}, &storage.Edge{ID: "x"}},
		{&storage.Node{ID: "x"}, &storage.Node{ID: "y"}},
		{[]interface{}{"a", "b"}, []interface{}{"a|1:Sb"}},
		{[]interface{}{"ab"}, []interface{}{"a", "b"}},
		{[]interface{}{nil}, []interface{}{}},
		{map[string]interface{}{"a": 1}, map[string]interface{}{"b": 1}},
		{map[string]interface{}{"a": 1}, []interface{}{"a", 1}},
		{map[string]interface{}{}, nil},
	}
	for _, pair := range different {
		require.NotEqual(t, cypherEquivalenceKey(pair[0]), cypherEquivalenceKey(pair[1]), "%#v vs %#v", pair[0], pair[1])
	}

	stamp := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	require.Equal(t, cypherEquivalenceKey(stamp), cypherEquivalenceKey(stamp.Add(0)), "other values keep their type and value")
	require.NotEqual(t, cypherEquivalenceKey(stamp), cypherEquivalenceKey(stamp.Add(time.Second)))
	require.Equal(t, "I18446744073709551616", cypherEquivalenceKey(math.Ldexp(1, 64)), "a whole float past int64 keeps its exact value")
}

// TestValueEquivalenceThroughExecute pins Neo4j's equivalence where NornicDB
// deduplicates and groups: UNION (#897 regression), DISTINCT, count and
// collect(DISTINCT …), grouping keys, percentiles over DISTINCT values, and
// simple CASE, which compares as = does (#907).
func TestValueEquivalenceThroughExecute(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	for _, test := range []struct {
		query string
		rows  [][]interface{}
	}{
		{"RETURN 1 AS x UNION RETURN 1.0 AS x", [][]interface{}{{int64(1)}}},
		{"RETURN 0.0 AS x UNION RETURN -0.0 AS x", [][]interface{}{{0.0}}},
		{"RETURN [1] AS x UNION RETURN [1.0] AS x", [][]interface{}{{[]interface{}{int64(1)}}}},
		{"RETURN 1 AS x UNION RETURN '1' AS x", [][]interface{}{{int64(1)}, {"1"}}},
		{"RETURN 1 AS x UNION ALL RETURN 1.0 AS x", [][]interface{}{{int64(1)}, {1.0}}},
		{"UNWIND [1, 1.0] AS x RETURN DISTINCT x", [][]interface{}{{int64(1)}}},
		{"UNWIND [1, 1.0] AS x WITH DISTINCT x RETURN count(*) AS c", [][]interface{}{{int64(1)}}},
		{"UNWIND [1, 1.0] AS x RETURN count(DISTINCT x) AS c", [][]interface{}{{int64(1)}}},
		{"UNWIND [0.0, -0.0, 0] AS x RETURN count(DISTINCT x) AS c", [][]interface{}{{int64(1)}}},
		{"UNWIND [1, 1.0] AS x RETURN size(collect(DISTINCT x)) AS c", [][]interface{}{{int64(1)}}},
		{"UNWIND [[1], [1.0]] AS x RETURN count(DISTINCT x) AS c", [][]interface{}{{int64(1)}}},
		{"UNWIND [{a: 1}, {a: 1.0}] AS x RETURN count(DISTINCT x) AS c", [][]interface{}{{int64(1)}}},
		{"UNWIND [0/0.0, 0/0.0] AS x RETURN count(DISTINCT x) AS c", [][]interface{}{{int64(2)}}},
		{"UNWIND [[0/0.0], [0/0.0]] AS x RETURN count(DISTINCT x) AS c", [][]interface{}{{int64(2)}}},
		{"UNWIND [null, null, 1] AS x RETURN x, count(*) AS c ORDER BY x", [][]interface{}{{int64(1), int64(1)}, {nil, int64(2)}}},
		{"UNWIND [9007199254740993, 9007199254740992.0] AS x RETURN count(DISTINCT x) AS c", [][]interface{}{{int64(2)}}},
		{"UNWIND [1, 1.0, 2] AS x RETURN x AS k, count(*) AS c ORDER BY k", [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(1)}}},
		{"UNWIND [1, 1.0, 2, 3] AS x RETURN percentileDisc(DISTINCT x, 0.5) AS p", [][]interface{}{{int64(2)}}},
		{"RETURN CASE [1, 2] WHEN [1, 2.0] THEN 'eq' ELSE 'ne' END AS r", [][]interface{}{{"eq"}}},
		{"RETURN CASE [1, null] WHEN [1, null] THEN 'eq' ELSE 'ne' END AS r", [][]interface{}{{"ne"}}},
		{"RETURN CASE 1 WHEN 1.0 THEN 'eq' ELSE 'ne' END AS r", [][]interface{}{{"eq"}}},
		{"RETURN CASE null WHEN null THEN 'eq' ELSE 'ne' END AS r", [][]interface{}{{"ne"}}},
	} {
		result, err := exec.Execute(ctx, test.query, nil)
		require.NoError(t, err, test.query)
		require.Equal(t, test.rows, result.Rows, test.query)
	}
}
