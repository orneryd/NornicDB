package cypher

import (
	"context"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The coll.* functions (#907); every expected value and error is Neo4j
// 2026.09's (notes: c25_coll_probe_2026-10-10).
func TestCollFunctionsMatchNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "coll_functions"))
	ctx := context.Background()
	l := func(values ...interface{}) []interface{} { return append([]interface{}{}, values...) }
	m := func(key string, value interface{}) map[string]interface{} { return map[string]interface{}{key: value} }
	for query, want := range map[string]interface{}{
		"coll.distinct([1, 1.0, 2, '2', null, null, [1], [1], {a:1}, {a:1}])": l(int64(1), int64(2), "2", nil, l(int64(1)), m("a", int64(1))),
		"coll.distinct([])":                                    l(),
		"coll.distinct(null)":                                  nil,
		"coll.distinct([1.0, 1])":                              l(1.0),
		"coll.distinct([[1, 1.0], [1.0, 1]])":                  l(l(int64(1), 1.0)),
		"coll.flatten([1, [2, [3, [4]]], [], null])":           l(int64(1), int64(2), l(int64(3), l(int64(4))), nil),
		"coll.flatten([1, [2, [3, [4]]]], 0)":                  l(int64(1), l(int64(2), l(int64(3), l(int64(4))))),
		"coll.flatten([1, [2, [3, [4]]]], 2)":                  l(int64(1), int64(2), int64(3), l(int64(4))),
		"coll.flatten([1, [2, [3, [4]]]], 10)":                 l(int64(1), int64(2), int64(3), int64(4)),
		"coll.flatten([[1, [2]], [[3]]], 1)":                   l(int64(1), l(int64(2)), l(int64(3))),
		"coll.flatten([1, 2])":                                 l(int64(1), int64(2)),
		"coll.flatten([1, [2]], null)":                         nil,
		"coll.flatten(null)":                                   nil,
		"coll.indexOf([1, 2, 1.0, null], 1.0)":                 int64(0),
		"coll.indexOf([1, 2, null], null)":                     nil,
		"coll.indexOf([1, 2], 3)":                              int64(-1),
		"coll.indexOf(null, 1)":                                nil,
		"coll.indexOf([[1], {a: 1}], {a: 1})":                  int64(1),
		"coll.indexOf([null, 1], 1)":                           int64(1),
		"coll.indexOf([null, 2], 1)":                           int64(-1),
		"coll.indexOf([[1, null]], [1, null])":                 int64(-1),
		"coll.indexOf([[1, null], [1, 2]], [1, 2])":            int64(1),
		"coll.indexOf([1, 2], '1')":                            int64(-1),
		"coll.insert([1, 2, 3], 0, 'x')":                       l("x", int64(1), int64(2), int64(3)),
		"coll.insert([1, 2, 3], 3, 'x')":                       l(int64(1), int64(2), int64(3), "x"),
		"coll.insert([1, 2, 3], 1, null)":                      l(int64(1), nil, int64(2), int64(3)),
		"coll.insert([1, 2, 3], null, 'x')":                    nil,
		"coll.insert(null, 0, 'x')":                            nil,
		"coll.insert([], 0, 1)":                                l(int64(1)),
		"coll.insert([1], toInteger('0'), 2)":                  l(int64(2), int64(1)),
		"coll.max([1, 2.5, 'a', null, [1], true])":             nil,
		"coll.max([null, null])":                               nil,
		"coll.max([])":                                         nil,
		"coll.max(null)":                                       nil,
		"coll.max([1, 1.0])":                                   int64(1),
		"coll.max([1, null])":                                  nil,
		"coll.max([[1], [1, 2], [0, 9]])":                      l(int64(1), int64(2)),
		"coll.min([1, 2.5, 'a', null, [1], true])":             l(int64(1)),
		"coll.min([3, 1, 2])":                                  int64(1),
		"coll.min([1.0, 1])":                                   1.0,
		"coll.min([null])":                                     nil,
		"coll.min([null, 3])":                                  int64(3),
		"coll.min([1, 0.0/0.0])":                               int64(1),
		"coll.remove([1, 2, 3], 0)":                            l(int64(2), int64(3)),
		"coll.remove([1, 2, 3], 2)":                            l(int64(1), int64(2)),
		"coll.remove([1, 2, 3], null)":                         nil,
		"coll.remove(null, 0)":                                 nil,
		"coll.sort([3, 'a', 1.5, null, [2], true, {a: 1}, 2])": l(m("a", int64(1)), l(int64(2)), "a", true, 1.5, int64(2), int64(3), nil),
		"coll.sort([])":                                        l(),
		"coll.sort(null)":                                      nil,
		"coll.sort(['b', 'A', 'a'])":                           l("A", "a", "b"),
		"coll.sort([1.0, 1, 0.5])":                             l(0.5, 1.0, int64(1)),
		"coll.sort([[2, 1], [1, 2], [1], [1, null]])":          l(l(int64(1)), l(int64(1), int64(2)), l(int64(1), nil), l(int64(2), int64(1))),
		"coll.sort([{b: 1}, {a: 2}, {a: 1}])":                  l(m("a", int64(1)), m("a", int64(2)), m("b", int64(1))),
		"coll.sort(['b', 'a', 'B', '', 'ä', 'z'])":             l("", "B", "a", "b", "z", "ä"),
	} {
		result, err := exec.Execute(ctx, "CYPHER 25 RETURN "+query+" AS x", nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}

	// NaN: never equivalent (two stay apart), after every number in order.
	result, err := exec.Execute(ctx, "RETURN coll.distinct([0.0/0.0, 0.0/0.0, 1]) AS d, coll.max([1, 0.0/0.0]) AS m, coll.sort([2, 0.0/0.0, 1]) AS s", nil)
	require.NoError(t, err)
	distinct := result.Rows[0][0].([]interface{})
	require.Len(t, distinct, 3)
	require.True(t, math.IsNaN(distinct[0].(float64)) && math.IsNaN(distinct[1].(float64)))
	require.True(t, math.IsNaN(result.Rows[0][1].(float64)))
	sorted := result.Rows[0][2].([]interface{})
	require.Equal(t, []interface{}{int64(1), int64(2)}, sorted[:2])
	require.True(t, math.IsNaN(sorted[2].(float64)))

	for query, want := range map[string]string{
		"coll.flatten([1, [2, [3, [4]]]], -1)": "Function argument to 'coll.flatten()' is out of range",
		"coll.insert([1, 2, 3], 4, 'x')":       "Function argument to 'coll.insert()' is out of range",
		"coll.insert([1, 2, 3], -1, 'x')":      "Function argument to 'coll.insert()' is out of range",
		"coll.remove([1, 2, 3], 3)":            "Function argument to 'coll.remove()' is out of range",
		"coll.remove([1, 2, 3], -1)":           "Function argument to 'coll.remove()' is out of range",
		"coll.remove([], 0)":                   "The argument `list` in the `coll.remove()` function must not be empty.",
	} {
		_, err := exec.Execute(ctx, "RETURN "+query+" AS x", nil)
		require.ErrorContains(t, err, want, query)
		requireStatusCode(t, err, "Neo.ClientError.Statement.ArgumentError")
	}
	for query, want := range map[string]string{
		"coll.flatten([1, 2], 1.5)":     "Type mismatch: expected Integer but was Float",
		"coll.remove([1, 2], 1.0)":      "Type mismatch: expected Integer but was Float",
		"coll.insert([1, 2], 1.0, 'x')": "Type mismatch: expected Integer but was Float",
		"coll.max('abc')":               "Type mismatch: expected List<T> but was String",
		"coll.sort([1], [2])":           "Too many parameters for function 'coll.sort'",
		"coll.insert([1], 0)":           "Insufficient parameters for function 'coll.insert'",
		"coll.flatten([1], 1, 2)":       "Too many parameters for function 'coll.flatten'",
	} {
		_, err := exec.Execute(ctx, "RETURN "+query+" AS x", nil)
		require.ErrorContains(t, err, want, query)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
	// Run-time argument types (a property's type is known per row).
	_, err = exec.Execute(ctx, "CREATE (:CollRT {a: 'x', f: 1.5})", nil)
	require.NoError(t, err)
	for query, want := range map[string]string{
		"MATCH (n:CollRT) RETURN coll.sort(n.a) AS x":         "Invalid input for function 'coll.sort()'",
		"MATCH (n:CollRT) RETURN coll.remove([1], n.f) AS x":  "Invalid input for function 'coll.remove()'",
		"MATCH (n:CollRT) RETURN coll.flatten([1], n.f) AS x": "Invalid input for function 'coll.flatten()'",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.ErrorContains(t, err, want, query)
		requireStatusCode(t, err, "Neo.ClientError.Statement.TypeError")
	}

	result, err = exec.Execute(ctx, "SHOW FUNCTIONS YIELD name WHERE name STARTS WITH 'coll.' RETURN collect(name) AS fns", nil)
	require.NoError(t, err)
	require.Equal(t, []interface{}{"coll.distinct", "coll.flatten", "coll.flatten", "coll.indexOf", "coll.insert", "coll.max", "coll.min", "coll.remove", "coll.sort"}, result.Rows[0][0])
	require.Equal(t, "2 or 3", collArity(2, 3))
}
