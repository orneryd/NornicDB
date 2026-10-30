package cypher

import (
	"context"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The Cypher 25 coll.*, string.* and cardinality() functions, replace()'s
// limit and the GQL function aliases, with Neo4j 2026.09's answers.
func TestCypher25Functions(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "c25_functions"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:F25 {id: 1})-[:R]->(:F25 {id: 2})-[:R]->(:F25 {id: 3})", nil)
	require.NoError(t, err)
	value := func(expression string) interface{} {
		result, err := exec.Execute(ctx, "CYPHER 25 RETURN "+expression+" AS v", nil)
		require.NoError(t, err, expression)
		require.Len(t, result.Rows, 1, expression)
		return result.Rows[0][0]
	}
	l := func(items ...interface{}) []interface{} { return append([]interface{}{}, items...) }
	for expression, want := range map[string]interface{}{
		"coll.distinct([true, false, false, true, true, false])": l(true, false),
		"coll.distinct([1, 1.0, '1', null, null, [1], [1]])":     l(int64(1), "1", nil, l(int64(1))),
		"coll.distinct([])":                               l(),
		"coll.distinct(null)":                             nil,
		"coll.flatten([1, [2, [3]]])":                     l(int64(1), int64(2), l(int64(3))),
		"coll.flatten([1, [2, [3, [4]]]], 2)":             l(int64(1), int64(2), int64(3), l(int64(4))),
		"coll.flatten([1, [2, [3]]], 0)":                  l(int64(1), l(int64(2), l(int64(3)))),
		"coll.flatten([false, ['a']], 2)":                 l(false, "a"),
		"coll.flatten([null, [null]])":                    l(nil, nil),
		"coll.flatten([1, [2]], null)":                    nil,
		"coll.indexOf(['A', 'new', 'function'], 'new')":   int64(1),
		"coll.indexOf([1, 2], 3)":                         int64(-1),
		"coll.indexOf([1.0, 2], 1)":                       int64(0),
		"coll.indexOf([1, null, 2], null)":                nil,
		"coll.insert([1, 2, 4], 2, 3)":                    l(int64(1), int64(2), int64(3), int64(4)),
		"coll.insert([1, 2], 0, 9)":                       l(int64(9), int64(1), int64(2)),
		"coll.insert([1, 2], 2, 9)":                       l(int64(1), int64(2), int64(9)),
		"coll.insert([1, 2], 1, null)":                    l(int64(1), nil, int64(2)),
		"coll.insert([1, 2], null, 9)":                    nil,
		"coll.max([1.5, 2, 5.4, 0, 4])":                   5.4,
		"coll.min([1.5, 2, 5.4, 0, 4])":                   int64(0),
		"coll.max(['a', 1])":                              int64(1),
		"coll.max([null, 1, null])":                       nil,
		"coll.min([true, 1, 'a', null])":                  "a",
		"coll.max([[1, 2], [1, 3]])":                      l(int64(1), int64(3)),
		"coll.max([])":                                    nil,
		"coll.remove(['a', 'a', 'b', 'c', 'd'], 2)":       l("a", "a", "c", "d"),
		"coll.remove([1, 2, 3], 0)":                       l(int64(2), int64(3)),
		"coll.remove([1, 2], null)":                       nil,
		"coll.sort([3, 1, 4, 2, 'a', 'c', 'b'])":          l("a", "b", "c", int64(1), int64(2), int64(3), int64(4)),
		"coll.sort([3, null, 1])":                         l(int64(1), int64(3), nil),
		"coll.sort([[2], [1, 2], [1]])":                   l(l(int64(1)), l(int64(1), int64(2)), l(int64(2))),
		"coll.sort([])":                                   l(),
		"string.indexOf('hello', 'l')":                    int64(2),
		"string.indexOf('héllo', 'l')":                    int64(2),
		"string.indexOf('hello', 'z')":                    int64(-1),
		"string.indexOf('hello', '')":                     int64(0),
		"string.indexOf(null, 'l')":                       nil,
		"string.join(['one', 'two'], ', ')":               "one, two",
		"string.join(['a', null, 'b'], '-')":              "a-b",
		"string.join([], ', ')":                           "",
		"string.join(['a'], null)":                        nil,
		"string.regexReplace('hello', 'l.', 'w')":         "hewo",
		"string.regexReplace('aaa', 'a', 'b')":            "bbb",
		"string.regexReplace('a1b2', '(\\\\d)', '<$1>')":  "a<1>b<2>",
		"string.regexReplace('a1', '(\\\\d)', '$10')":     "a10",
		"string.regexReplace('a1', '(\\\\d)', '<\\\\$>')": "a<$>",
		"string.regexReplace('hello', 'l', null)":         nil,
		"cardinality([1, 2, 3])":                          int64(3),
		"cardinality({a: 1, b: 2})":                       int64(2),
		"cardinality([])":                                 int64(0),
		"cardinality(null)":                               nil,
		"replace('hello world', 'l', '', 1)":              "helo world",
		"replace('hello world', 'l', 'L', 2)":             "heLLo world",
		"replace('hello', 'l', 'L', 0)":                   "hello",
		"replace('aaa', '', 'b', 1)":                      "baaa",
		"replace('hello', 'l', 'L', null)":                nil,
		"ceiling(1.2)":                                    2.0,
		"ceiling(null)":                                   nil,
		"ln(1)":                                           0.0,
		"path_length(null)":                               nil,
		"duration_between(date('2020-01-01'), null)":      nil,
	} {
		require.Equal(t, want, value(expression), expression)
	}
	require.True(t, math.IsInf(value("ln(0)").(float64), -1))

	rows := func(query string) [][]interface{} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return result.Rows
	}
	require.Equal(t, [][]interface{}{{int64(5)}}, rows("CYPHER 25 MATCH p = (:F25 {id: 1})-->()-->() RETURN cardinality(p) AS v"))
	require.Equal(t, [][]interface{}{{int64(1)}}, rows("CYPHER 25 MATCH p = (n:F25 {id: 1}) RETURN cardinality(p) AS v"))
	require.Equal(t, [][]interface{}{{int64(1)}}, rows("CYPHER 25 MATCH p = (:F25 {id: 1})-->() RETURN path_length(p) AS l"))
	result, err := exec.Execute(ctx, "CYPHER 25 UNWIND [3, 1, null, 2] AS x RETURN collect_list(x), percentile_cont(x, 0.5) AS c, percentile_disc(x, 0.5) AS d, stdev_samp(x) AS s, stdev_pop(x) AS p", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"collect_list(x)", "c", "d", "s", "p"}, result.Columns, "an alias's column keeps the client's text")
	require.Equal(t, l(int64(3), int64(1), int64(2)), result.Rows[0][0])
	require.Equal(t, int64(2), result.Rows[0][1], "an exact element keeps its type, as in Neo4j")
	require.Equal(t, int64(2), result.Rows[0][2])
	require.InDelta(t, 1.0, result.Rows[0][3], 1e-12)
	require.InDelta(t, math.Sqrt(2.0/3.0), result.Rows[0][4], 1e-12)
	require.Equal(t, [][]interface{}{{l(int64(1), int64(2))}}, rows("CYPHER 25 UNWIND [1, 1, 2] AS x RETURN collect_list(DISTINCT x) AS v"))
	require.Equal(t, [][]interface{}{{nil, nil}}, rows("RETURN stdev_samp(null) AS a, stdev_pop(null) AS b"))
	// Not an alias call: a property, a parameter, a string, a map key.
	require.Equal(t, [][]interface{}{{"ln(1)", int64(1)}}, rows("WITH {ln: 1} AS m RETURN 'ln(1)' AS s, m.ln AS v"))
	for _, temporal := range []string{"local_time('12:00') = localtime('12:00')", "local_datetime('2020-01-02T03:04') = localdatetime('2020-01-02T03:04')",
		"zoned_time('12:00+01:00') = time('12:00+01:00')", "zoned_datetime('2020-01-02T03:04Z') = datetime('2020-01-02T03:04Z')",
		"duration_between(date('2020-01-01'), date('2020-03-04')) = duration.between(date('2020-01-01'), date('2020-03-04'))"} {
		require.Equal(t, true, value(temporal), temporal)
	}

	for expression, code := range map[string]string{
		"coll.distinct('x')":                     "Neo.ClientError.Statement.SyntaxError",
		"coll.flatten([1, [2]], -1)":             "Neo.ClientError.Statement.ArgumentError",
		"coll.insert([1, 2], 5, 3)":              "Neo.ClientError.Statement.ArgumentError",
		"coll.insert([1, 2], -1, 9)":             "Neo.ClientError.Statement.ArgumentError",
		"coll.insert([1, 2], 1.5, 9)":            "Neo.ClientError.Statement.SyntaxError",
		"coll.remove([1, 2, 3], 3)":              "Neo.ClientError.Statement.ArgumentError",
		"coll.sort('x')":                         "Neo.ClientError.Statement.SyntaxError",
		"string.indexOf(1, 'l')":                 "Neo.ClientError.Statement.SyntaxError",
		"string.join(['a', 1], '-')":             "Neo.ClientError.Statement.TypeError",
		"string.join('a', '-')":                  "Neo.ClientError.Statement.SyntaxError",
		"string.regexReplace('hello', '[', 'x')": "Neo.ClientError.Statement.SemanticError",
		"cardinality('abc')":                     "Neo.ClientError.Statement.SyntaxError",
		"replace('hello', 'l', 'L', -1)":         "Neo.ClientError.Statement.SyntaxError",
		"replace('aaa', 'a', 'b', 1.5)":          "Neo.ClientError.Statement.SyntaxError",
		"path_length([1])":                       "Neo.ClientError.Statement.SyntaxError",
	} {
		_, err := exec.Execute(ctx, "CYPHER 25 RETURN "+expression+" AS v", nil)
		require.Error(t, err, expression)
		requireStatusCode(t, err, code)
	}
	_, err = exec.Execute(ctx, "WITH -1 AS n RETURN replace('aaa', 'a', 'b', n) AS v", nil)
	requireStatusCode(t, err, "Neo.ClientError.Statement.ArgumentError")
	require.ErrorContains(t, err, "Function argument to 'replace()' is out of range")
}
