package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newValueSemanticsExecutor returns an executor over (:Q {id: 1})-[:R]->
// (:Q {id: 2})-[:R]->(:Q {id: 3}).
func newValueSemanticsExecutor(t *testing.T) (*StorageExecutor, context.Context) {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Q {id: 1})-[:R {w: 1}]->(:Q {id: 2})-[:R {w: 2}]->(:Q {id: 3})", nil)
	require.NoError(t, err)
	return exec, ctx
}

// TestComparisonOperatorsMatchNeo4j pins Neo4j 5.26's comparison of maps,
// lists, durations, points, nodes, relationships and paths (#907).
func TestComparisonOperatorsMatchNeo4j(t *testing.T) {
	exec, ctx := newValueSemanticsExecutor(t)
	for query, want := range map[string][]interface{}{
		// Maps: fewer keys first, then key names, then values in key order.
		"RETURN {a: 1} < {a: 2}, {a: 1} < {b: 1}, {a: 1, b: 'x'} > {a: 2}, {} < {a: 1}, {a: 1, c: 1} < {a: 1, b: 9}": {true, true, true, true, false},
		"RETURN {a: 1} < {a: 'x'}, {a: null} <= {a: null}, {a: 1, b: null} < {a: 2, b: 1}, {a: 2} <= {a: 2}":         {nil, nil, true, true},
		// Lists: element by element.
		"RETURN [1, 2] < [1, 3], [1] < ['a'], [1, null] < [2, 1], [1, null] < [1, 2], [] < [null]": {true, nil, true, nil, true},
		// No order, but <= and >= of equal values.
		"RETURN duration('P1D') < duration('PT1H'), duration('P1D') <= duration('P1D'), duration('P1D') <= duration('PT24H')":                     {nil, true, nil},
		"RETURN point({x: 1, y: 2}) < point({x: 1, y: 2}), point({x: 1, y: 2}) >= point({x: 1, y: 2}), point({x: 1, y: 2}) < point({x: 2, y: 1})": {nil, true, nil},
		// Equal values with no order are equal inside a list or map.
		"RETURN {a: duration('P1D')} <= {a: duration('P1D')}, [duration('P1D')] < [duration('P1D'), 1], {a: point({x: 1, y: 2}), b: 1} < {a: point({x: 1, y: 2}), b: 2}, [duration('P1D')] < [duration('PT1H')]": {true, true, true, nil},
		// A map against a value of another kind has no order.
		"WITH {a: 1} AS m, point({x: 1, y: 2}) AS p, duration('P1D') AS d RETURN m < p, p >= m, m < d": {nil, nil, nil},
		// Nodes, relationships and paths order among their own kind only.
		"MATCH (a:Q {id: 1})-[r:R]->(b) RETURN a < a, a <= a, a = b, a <> b, a < 1, a = 1, a <> 1, a < r, r < r, r >= r, (a < b) <> (b < a)": {false, true, false, true, nil, false, true, nil, false, true, true},
		"MATCH p1 = (a:Q {id: 1})-[:R]->(b), p0 = (a) RETURN p0 < p1, p1 < p0, p0 <= p0, p1 > p1, p1 < a":                                    {true, false, true, false, nil},
		"MATCH (a:Q {id: 1}) WITH a, {a: 1} AS m, [1] AS l RETURN a <> m, a <> l, m <> l, a = m":                                             {true, true, true, false},
	} {
		t.Run(query, func(t *testing.T) {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{want}, result.Rows)
		})
	}

	t.Run("in WHERE", func(t *testing.T) {
		result, err := exec.Execute(ctx, "WITH {a: 1, b: 'x'} AS m, duration('P1D') AS du WHERE m >= m AND du <= du AND m > {a: 2} RETURN 1 AS v", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
		result, err = exec.Execute(ctx, "MATCH (n:Q {id: 1}) WHERE n <> 1 RETURN n.id AS v", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	})
}

// TestOrderByMapsMatchesNeo4j checks that ORDER BY orders maps as the
// comparison operators do, values by ORDER BY's order across types.
func TestOrderByMapsMatchesNeo4j(t *testing.T) {
	exec, ctx := newValueSemanticsExecutor(t)
	result, err := exec.Execute(ctx, "UNWIND [{b: 1}, {a: 1, c: 1}, {a: 2}, {a: 1}, {}, {a: 1, b: 9}, {a: 'x'}, {a: null}, {a: [1]}] AS m RETURN m ORDER BY m", nil)
	require.NoError(t, err)
	var got []interface{}
	for _, row := range result.Rows {
		got = append(got, row[0])
	}
	require.Equal(t, []interface{}{
		map[string]interface{}{}, map[string]interface{}{"a": []interface{}{int64(1)}}, map[string]interface{}{"a": "x"},
		map[string]interface{}{"a": int64(1)}, map[string]interface{}{"a": int64(2)}, map[string]interface{}{"a": nil},
		map[string]interface{}{"b": int64(1)}, map[string]interface{}{"a": int64(1), "b": int64(9)}, map[string]interface{}{"a": int64(1), "c": int64(1)},
	}, got)
}

// TestIsNormalizedMatchesNeo4j pins x IS [NOT] [form] NORMALIZED: a Boolean
// for a string, null for any other value.
func TestIsNormalizedMatchesNeo4j(t *testing.T) {
	exec, ctx := newValueSemanticsExecutor(t)
	result, err := exec.Execute(ctx, "RETURN 'x' IS NORMALIZED, 'é' IS NFC NORMALIZED, 'é' IS NFD NORMALIZED, 'é' IS NOT NFKC NORMALIZED, "+
		"3 IS NORMALIZED, 0.5 IS NOT NFKD NORMALIZED, null IS NORMALIZED, {a: 1} IS NORMALIZED", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{true, false, true, true, nil, nil, nil, nil}}, result.Rows)
	result, err = exec.Execute(ctx, "WITH 'ab' AS s, 1 AS i WHERE s IS NORMALIZED AND NOT coalesce(i IS NORMALIZED, false) RETURN 1 AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
}

// TestMapProjectionMatchesNeo4j pins map projection's receivers, fields and
// errors.
func TestMapProjectionMatchesNeo4j(t *testing.T) {
	exec, ctx := newValueSemanticsExecutor(t)
	for query, want := range map[string]interface{}{
		"WITH {a: 1, b: 'x'} AS m RETURN m{.a} AS v":              map[string]interface{}{"a": int64(1)},
		"WITH {a: 1, b: 'x'} AS m RETURN m{.*} AS v":              map[string]interface{}{"a": int64(1), "b": "x"},
		"WITH {a: 1} AS m RETURN m{.a, a: 2} AS v":                map[string]interface{}{"a": int64(2)},
		"WITH {a: 1} AS m, 5 AS b RETURN m {b} AS v":              map[string]interface{}{"b": int64(5)},
		"WITH null AS z RETURN z{.a} AS v":                        nil,
		"MATCH (n:Q {id: 1}) RETURN n{.id, k: n.id + 1} AS v":     map[string]interface{}{"id": int64(1), "k": int64(2)},
		"MATCH ()-[r:R {w: 1}]->() RETURN r{.*} AS v":             map[string]interface{}{"w": int64(1)},
		"WITH date('2020-01-02') AS d RETURN d{.year, k: 1} AS v": map[string]interface{}{"year": int64(2020), "k": int64(1)},
		"WITH duration('P1D') AS du RETURN du{.days} AS v":        map[string]interface{}{"days": int64(1)},
		"UNWIND [{a: 1}] AS m RETURN m{.a} AS v":                  map[string]interface{}{"a": int64(1)},
		"WITH null AS z RETURN z + {a: 2} AS v":                   nil,
		"RETURN [1] + {a: 2} AS v":                                []interface{}{int64(1), map[string]interface{}{"a": int64(2)}},
		"MATCH (n:Q {id: 1}) RETURN COUNT { (n)-->() } AS v":      int64(1),
		"MATCH (n:Q {id: 1}) RETURN EXISTS { (n)-->() } AS v":     true,
		"WITH {normalized: 1} AS m RETURN m.normalized AS v":      int64(1),
	} {
		t.Run(query, func(t *testing.T) {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{want}}, result.Rows)
		})
	}
	for query, code := range map[string]string{
		"RETURN 3{.a} AS v":                                                                 "Neo.ClientError.Statement.SyntaxError",
		"RETURN {a: 2}{.a} AS v":                                                            "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:Q) RETURN n.id{.a} AS v":                                                  "Neo.ClientError.Statement.SyntaxError",
		"WITH 'x' AS s WHERE 'x'{.a} RETURN 1 AS v":                                         "Neo.ClientError.Statement.SyntaxError",
		"WITH 1 AS i RETURN i{.a} AS v":                                                     "Neo.ClientError.Statement.SyntaxError",
		"WITH date('2020-01-02') AS d RETURN d{.a} AS v":                                    "Neo.ClientError.Statement.TypeError",
		"WITH date('2020-01-02') AS d RETURN d{.*} AS v":                                    "Neo.ClientError.Statement.TypeError",
		"UNWIND [1, {a: 1}] AS x RETURN x{.a} AS v":                                         "Neo.ClientError.Statement.TypeError",
		"MATCH p = (:Q {id: 1})-->() WITH [p, {a: 1}] AS l UNWIND l AS x RETURN x{.a} AS v": "Neo.ClientError.Statement.TypeError",
		"RETURN [1, 2]{.a} AS v":                                                            "Neo.ClientError.Statement.SyntaxError",
		"RETURN date('2020-01-02')['a'] AS v":                                               "Neo.ClientError.Statement.TypeError",
		"RETURN (1 / 0) IS NORMALIZED AS v":                                                 "Neo.ClientError.Statement.ArithmeticError",
	} {
		t.Run(query, func(t *testing.T) {
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err)
			requireStatusCode(t, err, code)
		})
	}
}

// TestTemporalFieldsAndUnaryPlusMatchNeo4j pins reading fields of temporal
// values and durations, unary plus, and an integer followed by a dot.
func TestTemporalFieldsAndUnaryPlusMatchNeo4j(t *testing.T) {
	exec, ctx := newValueSemanticsExecutor(t)
	result, err := exec.Execute(ctx, "WITH date('2020-01-02') AS d, duration('P1D') AS du RETURN d['year'], d.year, +3, + 3, +null, +d, +-3, +du = du", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, []interface{}{int64(2020), int64(2020), int64(3), int64(3), nil}, result.Rows[0][:5])
	require.Equal(t, []interface{}{int64(-3), true}, result.Rows[0][6:])
	for query, code := range map[string]string{
		"WITH date('2020-01-02') AS d RETURN d.a AS v":            "Neo.ClientError.Statement.TypeError",
		"WITH date('2020-01-02') AS d WHERE d['a'] RETURN 1 AS v": "Neo.ClientError.Statement.TypeError",
		"WITH duration('P1D') AS du WHERE du.id RETURN 1 AS v":    "Neo.ClientError.Statement.TypeError",
		"RETURN +'a' AS v": "Neo.ClientError.Statement.SyntaxError",
		"RETURN 5. AS v":   "Neo.ClientError.Statement.SyntaxError",
		"RETURN 5.e3 AS v": "Neo.ClientError.Statement.SyntaxError",
	} {
		t.Run(query, func(t *testing.T) {
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err)
			requireStatusCode(t, err, code)
		})
	}
	result, err = exec.Execute(ctx, "RETURN [1, 2, 3][0..1] AS a, 5.0 AS b", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{[]interface{}{int64(1)}, 5.0}}, result.Rows)
}

// TestValueOrderingPathForms orders every representation of a path the
// executor carries, and a path against other values.
func TestValueOrderingPathForms(t *testing.T) {
	short := PathResult{Nodes: []*storage.Node{{ID: "a"}}}
	long := PathResult{Nodes: []*storage.Node{{ID: "a"}, {ID: "b"}}, Relationships: []*storage.Edge{{ID: "r"}}}
	for _, pair := range [][2]interface{}{
		{short, long}, {&short, &long}, {map[string]interface{}{"_pathResult": short}, map[string]interface{}{"_pathResult": &long}},
	} {
		order, comparable := compareCypherOrderedValues(pair[0], pair[1])
		require.True(t, comparable)
		require.Equal(t, -1, order)
	}
	for _, notPath := range []interface{}{(*PathResult)(nil), map[string]interface{}{"_pathResult": (*PathResult)(nil)}, int64(1)} {
		_, isPath := cypherPathElements(notPath)
		require.False(t, isPath)
	}
	_, comparable := compareCypherOrderedValues(short, map[string]interface{}{"a": int64(1)})
	require.False(t, comparable, "a path and a map have no order")
	exec, ctx := newValueSemanticsExecutor(t)
	result, err := exec.Execute(ctx, "MATCH p = (a:Q {id: 1}), q = (a)-->() RETURN p < q AS a, p < 1 AS b, 1 < p AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{true, nil, nil}}, result.Rows)
}

// TestValueSemanticsParsingForms covers the text forms the checks read.
func TestValueSemanticsParsingForms(t *testing.T) {
	for expression, want := range map[string]string{
		"[1, 2]{.a}": "[1, 2]", "f(1){.a}": "f(1)", "m {.a}": "m", "'x'{.a}": "'x'", "{a: 1}{.a}": "{a: 1}",
	} {
		receiver, projected := staticMapProjectionReceiver(expression)
		require.True(t, projected, expression)
		require.Equal(t, want, receiver, expression)
	}
	for _, expression := range []string{"x + {a: 1}", "THEN {a: 1}", "COUNT { (n)-->() }", "EXISTS { MATCH (n) }", "COLLECT { MATCH (n) RETURN n }", "{a: 1", "(1{.a}", "{a: {b: 1}", "x", "x}"} {
		_, projected := staticMapProjectionReceiver(expression)
		require.False(t, projected, expression)
	}
	for _, expression := range []string{"x.normalized", "normalized", "n.`is normalized`"} {
		_, _, _, ok := splitNormalizationPredicate(expression)
		require.False(t, ok, expression)
	}
	first := PathResult{Nodes: []*storage.Node{{ID: "a"}}}
	second := PathResult{Nodes: []*storage.Node{{ID: "b"}}}
	order, comparable := compareCypherOrderedValues(second, first)
	require.True(t, comparable)
	require.Equal(t, 1, order)
}
