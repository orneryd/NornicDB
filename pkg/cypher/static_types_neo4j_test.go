package cypher

import (
	"context"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestStaticTypeChecksMatchNeo4j pins the compile-time type checks, scope
// checks and their runtime counterparts to the pinned Neo4j 5.26's answers
// (#907 step 1, #900): the status code of a rejected statement, or the rows of
// an accepted one.
func TestStaticTypeChecksMatchNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "static_types"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Probe {big: 9007199254740993, s: 'abc', x: [1, 2, 3]})-[:R]->(:Probe {big: 1})", nil)
	require.NoError(t, err)

	const syntax, typeError, argument = "Neo.ClientError.Statement.SyntaxError", "Neo.ClientError.Statement.TypeError", "Neo.ClientError.Statement.ArgumentError"
	for _, tc := range []struct {
		query string
		code  string
		rows  [][]interface{}
	}{
		// A list is not a boolean.
		{query: "MATCH (n:Probe) WHERE [1] RETURN n", code: syntax},
		{query: "RETURN [] AND null AS v", code: syntax},
		{query: "RETURN NOT ['text'] AS v", code: syntax},
		{query: "WITH null AS z WHERE z[1..] RETURN 1 AS v", code: syntax},
		{query: "MATCH (n:Probe) WHERE n.x[1..] RETURN 1 AS v", code: syntax},
		{query: "MATCH (n:Probe) WHERE COLLECT { MATCH (n)-->(b) RETURN b.big } RETURN 1 AS v", code: syntax},
		{query: "WITH null AS z RETURN z[1..] AS v", rows: [][]interface{}{{nil}}},
		// A map literal after a boolean operator.
		{query: "RETURN true AND {a: 2} AS v", code: syntax},
		{query: "WITH true AS t RETURN t OR {a: 2} AS v", code: syntax},
		// Duration arithmetic.
		{query: "WITH duration('P1D') AS du, 1 AS i RETURN du + i AS v", code: syntax},
		{query: "RETURN duration('PT1H') + 0.5 AS v", code: syntax},
		{query: "RETURN duration('PT1H') * 9007199254740993 AS v", code: argument},
		{query: "RETURN duration('P1D') * 9007199254740993 AS v", code: argument},
		{query: "MATCH (n:Probe) WHERE n.big > 5 RETURN duration('PT1H') * n.big AS v", code: argument},
		{query: "RETURN toString(duration('PT1H') / 0.5) AS v", rows: [][]interface{}{{"PT2H"}}},
		// Map projections take a map, a node or a relationship.
		{query: "WITH 'ab' AS s RETURN s{.a} AS v", code: syntax},
		{query: "WITH 1 AS i RETURN i{.a} AS v", code: syntax},
		{query: "MATCH p = (n:Probe)-->() RETURN p{.a} AS v", code: syntax},
		{query: "MATCH (n:Probe) WHERE n{.big} RETURN 1 AS v", code: syntax},
		// Results of expressions the operator check can't read.
		{query: "MATCH (n:Probe) WHERE properties(n) RETURN 1 AS v", code: syntax},
		{query: "MATCH (n:Probe) WHERE size([(n)-->() | 1]) RETURN 1 AS v", code: syntax},
		{query: "MATCH (n:Probe) WHERE COUNT { MATCH (n)-->() } RETURN 1 AS v", code: syntax},
		// Unary operators over null or an unknown value are numbers.
		{query: "WITH null AS z WHERE +z RETURN 1 AS v", code: syntax},
		{query: "WITH null AS z WHERE -z RETURN 1 AS v", code: syntax},
		{query: "MATCH (n:Probe) WHERE +n.big RETURN 1 AS v", code: syntax},
		{query: "RETURN -null AS v", rows: [][]interface{}{{nil}}},
		// =~ at run time: a non-string text is null, a non-string pattern a TypeError.
		{query: "MATCH (n:Probe {big: 1}) RETURN n.big =~ 'x' AS v", rows: [][]interface{}{{nil}}},
		{query: "MATCH (n:Probe {big: 1}) RETURN 'x' =~ n.big AS v", code: typeError},
		{query: "RETURN 'x' =~ 1 AS v", code: syntax},
		// Undefined variables, in every position.
		{query: "MATCH (n:Probe) WHERE zz RETURN 1 AS v", code: syntax},
		{query: "MATCH (n:Probe) WHERE zz.a RETURN 1 AS v", code: syntax},
		{query: "UNWIND [zz] AS v RETURN v", code: syntax},
		{query: "MATCH (n:Probe) RETURN CASE WHEN zz THEN 1 ELSE 0 END AS v", code: syntax},
		// Names that aren't variables.
		{query: "RETURN null.a AS v", rows: [][]interface{}{{nil}}},
		{query: "WITH [1] AS not, 1 AS x WITH not, x WHERE x IN not RETURN 1 AS n", rows: [][]interface{}{{int64(1)}}},
		{query: "RETURN normalize('a', NFC) AS v", rows: [][]interface{}{{"a"}}},
		{query: "RETURN [1, 'a'] IS :: LIST<INTEGER> | LIST<STRING> AS v", rows: [][]interface{}{{false}}},
		{query: "RETURN [x IN [1, 2, 3] WHERE x > 1 | x * 2] AS r", rows: [][]interface{}{{[]interface{}{int64(4), int64(6)}}}},
		// A projection item repeated in ORDER BY is its column.
		{query: "UNWIND ['abc', 'bbbbbbb', 'x'] AS s RETURN size(s) AS s ORDER BY size(s) DESC", rows: [][]interface{}{{int64(7)}, {int64(3)}, {int64(1)}}},
		// A list literal after an operator is a whole subscript receiver.
		{query: "RETURN [[1], [2, 3]] + [5, [6, 7], 10][2] AS a", rows: [][]interface{}{{[]interface{}{[]interface{}{int64(1)}, []interface{}{int64(2), int64(3)}, int64(10)}}}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			result, err := exec.Execute(ctx, tc.query, nil)
			if tc.code != "" {
				requireStatusCode(t, err, tc.code)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.rows, result.Rows)
		})
	}
}

// TestStaticOperatorCheckerScopedBranches covers the checker's rules over a
// clause scope (a node, a string, a list, a map): errors inside CASE, type
// predicates, unary plus, slices, subscripts and map projections are
// reported, and each receiver kind takes its own keys.
func TestStaticOperatorCheckerScopedBranches(t *testing.T) {
	checker := staticOperatorChecker{scope: staticTypeScope{
		kinds:  matchSemanticScope{"n": matchBindingNode},
		values: map[string]string{"s": "String", "l": "List<Integer>", "m": "Map", "z": "Null"},
	}}
	for _, tc := range []struct {
		expr  string
		want  string
		fails bool
	}{
		{expr: "duration('P1D') + [1]", want: "List<T>"},
		{expr: "(1 + true) IS :: INTEGER", fails: true},
		{expr: "s IS :: STRING", want: "Boolean"},
		{expr: "+(1 + true)", fails: true},
		{expr: "+'a'", fails: true},
		{expr: "+date('2020-01-01')", want: "Date"},
		{expr: "CASE (1 + true) WHEN 1 THEN 1 END", fails: true},
		{expr: "CASE 1 WHEN 1 + true THEN 1 END", fails: true},
		{expr: "CASE WHEN 1 + true THEN 1 END", fails: true},
		{expr: "CASE WHEN 1 THEN 1 END", fails: true},
		{expr: "CASE WHEN true THEN 1 + true END", fails: true},
		{expr: "CASE WHEN true THEN 1 ELSE 1 + true END", fails: true},
		{expr: "CASE 1 WHEN 1 THEN 'a' ELSE 2 END", want: ""},
		{expr: "s.x", fails: true},
		{expr: "z.x", want: ""},
		{expr: "l[1 + true..]", fails: true},
		{expr: "s[1..]", fails: true},
		{expr: "l[(0)..[1, 2][0]]", want: "List<Integer>"},
		{expr: "l[1 + true]", fails: true},
		{expr: "l['a']", fails: true},
		{expr: "l[0]", want: "Integer"},
		{expr: "z[0]", want: ""},
		{expr: "m[1]", fails: true},
		{expr: "n[1]", fails: true},
		{expr: "n['a']", want: ""},
		{expr: "1['a']", fails: true},
		{expr: "date('2020-01-01')['a']", want: ""},
		{expr: "1[0]", fails: true},
		{expr: "1[x]", fails: true},
		{expr: "date('2020-01-01')[x]", want: ""},
		{expr: "l[0]x", want: ""},
		{expr: "m{.a, b: 1 + true}", fails: true},
		{expr: "s{.a}", fails: true},
		{expr: "m{.a, b: 1}", want: "Map"},
		{expr: "zz", fails: false, want: ""},
	} {
		got, err := checker.check(tc.expr)
		if tc.fails {
			require.Error(t, err, tc.expr)
			continue
		}
		require.NoError(t, err, tc.expr)
		require.Equal(t, tc.want, got.kind, tc.expr)
	}
	complete := checker
	complete.scope.complete = true
	_, err := complete.check("zz + 1")
	require.ErrorContains(t, err, "zz")
	_, err = complete.check("null + 1")
	require.NoError(t, err)
}

// TestDurationScaleError pins when duration * number and duration / number
// overflow, as Neo4j's ArgumentError.
func TestDurationScaleError(t *testing.T) {
	day := parseDuration("P1D")
	hour := parseDuration("PT1H")
	for _, tc := range []struct {
		op          byte
		left, right interface{}
		fails       bool
	}{
		{op: '*', left: hour, right: int64(9007199254740993), fails: true},
		{op: '*', left: int64(9007199254740993), right: day, fails: true},
		{op: '*', left: day, right: 2.5},
		{op: '/', left: hour, right: 1e-15},
		{op: '/', left: hour, right: 1e-16, fails: true},
		{op: '/', left: hour, right: int64(0)},
		{op: '/', left: int64(2), right: hour},
		{op: '+', left: hour, right: int64(1)},
		{op: '*', left: hour, right: "a"},
		{op: '*', left: hour, right: math.NaN()},
		{op: '*', left: int64(1), right: int64(2)},
	} {
		err := durationScaleError(tc.op, tc.left, tc.right)
		if tc.fails {
			requireStatusCode(t, err, "Neo.ClientError.Statement.ArgumentError")
			require.ErrorContains(t, err, "Duration arithmetic overflows")
			continue
		}
		require.NoError(t, err, "%c %v %v", tc.op, tc.left, tc.right)
	}
	scaled, handled := scaleTemporalDuration(hour, 1e19)
	require.True(t, handled)
	require.Nil(t, scaled, "a product that doesn't fit has no value")

	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "duration_overflow"))
	_, err := exec.Execute(context.Background(), "RETURN duration('PT1H') * 9007199254740993 AS v", nil)
	require.ErrorContains(t, err, "Duration arithmetic overflows")
	_, err = exec.Execute(context.Background(), "WITH 1 AS x WHERE toUpper(x) = 'A' RETURN x", nil)
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
}
