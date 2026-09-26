package cypher

import (
	"context"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

var rowCorpusNaN = math.NaN()

// TestRowEvaluatorMatchesNeo4j runs expressions through the row evaluator
// (UNWIND … WITH … RETURN): math and conversion functions, the Neo4j 5 string
// and list functions, valueType, reduce, subscripts, map projections and
// SKIP / LIMIT values. Each expected result is Neo4j 5.26.30's, collected
// from the pinned image; an expected error is its status code.
func TestRowEvaluatorMatchesNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "rowcorpus"))
	ctx := context.Background()
	for _, tc := range []struct {
		query string
		rows  [][]interface{}
		err   string
	}{
		{query: "UNWIND [1] AS one WITH one RETURN pi() AS v", rows: [][]interface{}{[]interface{}{float64(3.141592653589793)}}},
		{query: "UNWIND [1] AS one WITH one RETURN e() AS v", rows: [][]interface{}{[]interface{}{float64(2.718281828459045)}}},
		{query: "UNWIND [1] AS one WITH one RETURN round(2.5) AS v", rows: [][]interface{}{[]interface{}{float64(3.0)}}},
		{query: "UNWIND [1] AS one WITH one RETURN round(-2.5) AS v", rows: [][]interface{}{[]interface{}{float64(-2.0)}}},
		{query: "UNWIND [1] AS one WITH one RETURN round(2.567, 2) AS v", rows: [][]interface{}{[]interface{}{float64(2.57)}}},
		{query: "UNWIND [1] AS one WITH one RETURN round(2.565, 2, 'HALF_EVEN') AS v", rows: [][]interface{}{[]interface{}{float64(2.56)}}},
		{query: "UNWIND [1] AS one WITH one RETURN round(2.5, 0, 'CEILING') AS v", rows: [][]interface{}{[]interface{}{float64(3.0)}}},
		{query: "UNWIND [1] AS one WITH one RETURN round(2.5, 0, 'FLOOR') AS v", rows: [][]interface{}{[]interface{}{float64(2.0)}}},
		{query: "UNWIND [1] AS one WITH one RETURN round(2.5, 0, 'HALF_DOWN') AS v", rows: [][]interface{}{[]interface{}{float64(2.0)}}},
		{query: "UNWIND [1] AS one WITH one RETURN round(2.5, 0, 'UP') AS v", rows: [][]interface{}{[]interface{}{float64(3.0)}}},
		{query: "UNWIND [1] AS one WITH one RETURN round(2.5, 0, 'DOWN') AS v", rows: [][]interface{}{[]interface{}{float64(2.0)}}},
		{query: "UNWIND [1] AS one WITH one RETURN round(null) AS v", rows: [][]interface{}{[]interface{}{nil}}},
		{query: "UNWIND [1] AS one WITH one RETURN round(1.5, 0, null) AS v", rows: [][]interface{}{[]interface{}{nil}}},
		{query: "UNWIND [1] AS one WITH one RETURN round('a') AS v", err: "Neo.ClientError.Statement.SyntaxError"},
		{query: "UNWIND [1] AS one WITH one RETURN sin(0.5) AS v", rows: [][]interface{}{[]interface{}{float64(0.479425538604203)}}},
		{query: "UNWIND [1] AS one WITH one RETURN cos(0.5) AS v", rows: [][]interface{}{[]interface{}{float64(0.8775825618903728)}}},
		{query: "UNWIND [1] AS one WITH one RETURN tan(0.5) AS v", rows: [][]interface{}{[]interface{}{float64(0.5463024898437905)}}},
		{query: "UNWIND [1] AS one WITH one RETURN asin(0.5) AS v", rows: [][]interface{}{[]interface{}{float64(0.5235987755982989)}}},
		{query: "UNWIND [1] AS one WITH one RETURN atan(0.5) AS v", rows: [][]interface{}{[]interface{}{float64(0.4636476090008061)}}},
		{query: "UNWIND [1] AS one WITH one RETURN atan2(1, 2) AS v", rows: [][]interface{}{[]interface{}{float64(0.4636476090008061)}}},
		{query: "UNWIND [1] AS one WITH one RETURN cot(0.5) AS v", rows: [][]interface{}{[]interface{}{float64(1.830487721712452)}}},
		{query: "UNWIND [1] AS one WITH one RETURN sqrt(16) AS v", rows: [][]interface{}{[]interface{}{float64(4.0)}}},
		{query: "UNWIND [1] AS one WITH one RETURN sqrt(-1) AS v", rows: [][]interface{}{[]interface{}{rowCorpusNaN}}},
		{query: "UNWIND [1] AS one WITH one RETURN exp(1) AS v", rows: [][]interface{}{[]interface{}{float64(2.718281828459045)}}},
		{query: "UNWIND [1] AS one WITH one RETURN log(10) AS v", rows: [][]interface{}{[]interface{}{float64(2.302585092994046)}}},
		{query: "UNWIND [1] AS one WITH one RETURN log10(1000) AS v", rows: [][]interface{}{[]interface{}{float64(3.0)}}},
		{query: "UNWIND [1] AS one WITH one RETURN abs(-3) AS v", rows: [][]interface{}{[]interface{}{int64(3)}}},
		{query: "UNWIND [1] AS one WITH one RETURN abs(-3.5) AS v", rows: [][]interface{}{[]interface{}{float64(3.5)}}},
		{query: "UNWIND [1] AS one WITH one RETURN sign(-2) AS v", rows: [][]interface{}{[]interface{}{int64(-1)}}},
		{query: "UNWIND [1] AS one WITH one RETURN sign(0.0) AS v", rows: [][]interface{}{[]interface{}{int64(0)}}},
		{query: "UNWIND [1] AS one WITH one RETURN ceil(1.2) AS v", rows: [][]interface{}{[]interface{}{float64(2.0)}}},
		{query: "UNWIND [1] AS one WITH one RETURN floor(-1.2) AS v", rows: [][]interface{}{[]interface{}{float64(-2.0)}}},
		{query: "UNWIND [1] AS one WITH one RETURN radians(180) AS v", rows: [][]interface{}{[]interface{}{float64(3.141592653589793)}}},
		{query: "UNWIND [1] AS one WITH one RETURN degrees(3.141592653589793) AS v", rows: [][]interface{}{[]interface{}{float64(180.0)}}},
		{query: "UNWIND [1] AS one WITH one RETURN isNaN(0.0/0.0) AS v", rows: [][]interface{}{[]interface{}{true}}},
		{query: "UNWIND [1] AS one WITH one RETURN isNaN(1) AS v", rows: [][]interface{}{[]interface{}{false}}},
		{query: "UNWIND [1] AS one WITH one RETURN sin('a') AS v", err: "Neo.ClientError.Statement.SyntaxError"},
		{query: "UNWIND [1] AS one WITH one RETURN sqrt(null) AS v", rows: [][]interface{}{[]interface{}{nil}}},
		{query: "UNWIND [1] AS one WITH one RETURN abs('x') AS v", err: "Neo.ClientError.Statement.SyntaxError"},
		{query: "UNWIND [1] AS one WITH one RETURN toUpper('abc') AS v", rows: [][]interface{}{[]interface{}{"ABC"}}},
		{query: "UNWIND [1] AS one WITH one RETURN toLower('AbC') AS v", rows: [][]interface{}{[]interface{}{"abc"}}},
		{query: "UNWIND [1] AS one WITH one RETURN toUpper(1) AS v", err: "Neo.ClientError.Statement.SyntaxError"},
		{query: "UNWIND [1] AS one WITH one RETURN toInteger('12') AS v", rows: [][]interface{}{[]interface{}{int64(12)}}},
		{query: "UNWIND [1] AS one WITH one RETURN toInteger('x') AS v", rows: [][]interface{}{[]interface{}{nil}}},
		{query: "UNWIND [1] AS one WITH one RETURN toInteger(1.9) AS v", rows: [][]interface{}{[]interface{}{int64(1)}}},
		{query: "UNWIND [1] AS one WITH one RETURN toInteger([1]) AS v", err: "Neo.ClientError.Statement.TypeError"},
		{query: "UNWIND [1] AS one WITH one RETURN toFloat('1.5') AS v", rows: [][]interface{}{[]interface{}{float64(1.5)}}},
		{query: "UNWIND [1] AS one WITH one RETURN toFloat('x') AS v", rows: [][]interface{}{[]interface{}{nil}}},
		{query: "UNWIND [1] AS one WITH one RETURN toFloat(2) AS v", rows: [][]interface{}{[]interface{}{float64(2.0)}}},
		{query: "UNWIND [1] AS one WITH one RETURN toBoolean('true') AS v", rows: [][]interface{}{[]interface{}{true}}},
		{query: "UNWIND [1] AS one WITH one RETURN toBoolean('TRUE') AS v", rows: [][]interface{}{[]interface{}{true}}},
		{query: "UNWIND [1] AS one WITH one RETURN toBoolean('x') AS v", rows: [][]interface{}{[]interface{}{nil}}},
		{query: "UNWIND [1] AS one WITH one RETURN substring('hello', 1, 3) AS v", rows: [][]interface{}{[]interface{}{"ell"}}},
		{query: "UNWIND [1] AS one WITH one RETURN substring('hello', 2) AS v", rows: [][]interface{}{[]interface{}{"llo"}}},
		{query: "UNWIND [1] AS one WITH one RETURN substring('hello', -1) AS v", err: "Neo.DatabaseError.Statement.ExecutionFailed"},
		{query: "UNWIND [1] AS one WITH one RETURN left('hello', 2) AS v", rows: [][]interface{}{[]interface{}{"he"}}},
		{query: "UNWIND [1] AS one WITH one RETURN right('hello', 2) AS v", rows: [][]interface{}{[]interface{}{"lo"}}},
		{query: "UNWIND [1] AS one WITH one RETURN replace('a-b-c', '-', '+') AS v", rows: [][]interface{}{[]interface{}{"a+b+c"}}},
		{query: "UNWIND [1] AS one WITH one RETURN split('a,b,c', ',') AS v", rows: [][]interface{}{[]interface{}{[]interface{}{"a", "b", "c"}}}},
		{query: "UNWIND [1] AS one WITH one RETURN split(1, ',') AS v", err: "Neo.ClientError.Statement.SyntaxError"},
		{query: "UNWIND [1] AS one WITH one RETURN char_length('héllo') AS v", rows: [][]interface{}{[]interface{}{int64(5)}}},
		{query: "UNWIND [1] AS one WITH one RETURN character_length('abc') AS v", rows: [][]interface{}{[]interface{}{int64(3)}}},
		{query: "UNWIND [1] AS one WITH one RETURN char_length(null) AS v", rows: [][]interface{}{[]interface{}{nil}}},
		{query: "UNWIND [1] AS one WITH one RETURN char_length(1) AS v", err: "Neo.ClientError.Statement.SyntaxError"},
		{query: "UNWIND [1] AS one WITH one RETURN upper('x') AS v", rows: [][]interface{}{[]interface{}{"X"}}},
		{query: "UNWIND [1] AS one WITH one RETURN lower('X') AS v", rows: [][]interface{}{[]interface{}{"x"}}},
		{query: "UNWIND [1] AS one WITH one RETURN btrim('  x  ') AS v", rows: [][]interface{}{[]interface{}{"x"}}},
		{query: "UNWIND [1] AS one WITH one RETURN btrim('xxaxx', 'x') AS v", rows: [][]interface{}{[]interface{}{"a"}}},
		{query: "UNWIND [1] AS one WITH one RETURN ltrim('xxa', 'x') AS v", rows: [][]interface{}{[]interface{}{"a"}}},
		{query: "UNWIND [1] AS one WITH one RETURN rtrim('axx', 'x') AS v", rows: [][]interface{}{[]interface{}{"a"}}},
		{query: "UNWIND [1] AS one WITH one RETURN ltrim('  a') AS v", rows: [][]interface{}{[]interface{}{"a"}}},
		{query: "UNWIND [1] AS one WITH one RETURN rtrim('a  ') AS v", rows: [][]interface{}{[]interface{}{"a"}}},
		{query: "UNWIND [1] AS one WITH one RETURN trim(LEADING 'x' FROM 'xxaxx') AS v", rows: [][]interface{}{[]interface{}{"axx"}}},
		{query: "UNWIND [1] AS one WITH one RETURN trim(TRAILING 'x' FROM 'xxaxx') AS v", rows: [][]interface{}{[]interface{}{"xxa"}}},
		{query: "UNWIND [1] AS one WITH one RETURN trim(BOTH 'x' FROM 'xxaxx') AS v", rows: [][]interface{}{[]interface{}{"a"}}},
		{query: "UNWIND [1] AS one WITH one RETURN trim('  a  ') AS v", rows: [][]interface{}{[]interface{}{"a"}}},
		{query: "UNWIND [1] AS one WITH one RETURN trim(null) AS v", rows: [][]interface{}{[]interface{}{nil}}},
		{query: "UNWIND [1] AS one WITH one RETURN btrim('ab', 'ab') AS v", rows: [][]interface{}{[]interface{}{""}}},
		{query: "UNWIND [1] AS one WITH one RETURN normalize('Å') AS v", rows: [][]interface{}{[]interface{}{"Å"}}},
		{query: "UNWIND [1] AS one WITH one RETURN normalize('Å', NFD) AS v", rows: [][]interface{}{[]interface{}{"Å"}}},
		{query: "UNWIND [1] AS one WITH one RETURN normalize('Å', NFKC) AS v", rows: [][]interface{}{[]interface{}{"Å"}}},
		{query: "UNWIND [1] AS one WITH one RETURN normalize('Å', NFKD) AS v", rows: [][]interface{}{[]interface{}{"Å"}}},
		{query: "UNWIND [1] AS one WITH one RETURN normalize(null) AS v", rows: [][]interface{}{[]interface{}{nil}}},
		{query: "UNWIND [1] AS one WITH one RETURN normalize(1) AS v", err: "Neo.ClientError.Statement.SyntaxError"},
		{query: "UNWIND [1] AS one WITH one RETURN size(normalize('Å', NFD)) AS v", rows: [][]interface{}{[]interface{}{int64(2)}}},
		{query: "UNWIND [1] AS one WITH one RETURN toIntegerList(['1', 'x', 2.5, null, true]) AS v", rows: [][]interface{}{[]interface{}{[]interface{}{int64(1), nil, int64(2), nil, int64(1)}}}},
		{query: "UNWIND [1] AS one WITH one RETURN toFloatList(['1.5', 'x', 2, null]) AS v", rows: [][]interface{}{[]interface{}{[]interface{}{float64(1.5), nil, float64(2.0), nil}}}},
		{query: "UNWIND [1] AS one WITH one RETURN toStringList([1, 2.5, true, null, 'a']) AS v", rows: [][]interface{}{[]interface{}{[]interface{}{"1", "2.5", "true", nil, "a"}}}},
		{query: "UNWIND [1] AS one WITH one RETURN toBooleanList(['true', 'x', 1, 0, null]) AS v", rows: [][]interface{}{[]interface{}{[]interface{}{true, nil, true, false, nil}}}},
		{query: "UNWIND [1] AS one WITH one RETURN toIntegerList(null) AS v", rows: [][]interface{}{[]interface{}{nil}}},
		{query: "UNWIND [1] AS one WITH one RETURN toIntegerList([]) AS v", rows: [][]interface{}{[]interface{}{[]interface{}{}}}},
		{query: "UNWIND [1] AS one WITH one RETURN toIntegerList('x') AS v", err: "Neo.ClientError.Statement.SyntaxError"},
		{query: "UNWIND [1] AS one WITH one RETURN nullIf(1, 1) AS v", rows: [][]interface{}{[]interface{}{nil}}},
		{query: "UNWIND [1] AS one WITH one RETURN nullIf(1, 2) AS v", rows: [][]interface{}{[]interface{}{int64(1)}}},
		{query: "UNWIND [1] AS one WITH one RETURN nullIf(null, 1) AS v", rows: [][]interface{}{[]interface{}{nil}}},
		{query: "UNWIND [1] AS one WITH one RETURN nullIf('a', 'a') AS v", rows: [][]interface{}{[]interface{}{nil}}},
		{query: "UNWIND [1] AS one WITH one RETURN valueType(1) AS v", rows: [][]interface{}{[]interface{}{"INTEGER NOT NULL"}}},
		{query: "UNWIND [1] AS one WITH one RETURN valueType(1.5) AS v", rows: [][]interface{}{[]interface{}{"FLOAT NOT NULL"}}},
		{query: "UNWIND [1] AS one WITH one RETURN valueType('a') AS v", rows: [][]interface{}{[]interface{}{"STRING NOT NULL"}}},
		{query: "UNWIND [1] AS one WITH one RETURN valueType(true) AS v", rows: [][]interface{}{[]interface{}{"BOOLEAN NOT NULL"}}},
		{query: "UNWIND [1] AS one WITH one RETURN valueType(null) AS v", rows: [][]interface{}{[]interface{}{"NULL"}}},
		{query: "UNWIND [1] AS one WITH one RETURN valueType([1, 2]) AS v", rows: [][]interface{}{[]interface{}{"LIST<INTEGER NOT NULL> NOT NULL"}}},
		{query: "UNWIND [1] AS one WITH one RETURN valueType([1, 'a']) AS v", rows: [][]interface{}{[]interface{}{"LIST<STRING NOT NULL | INTEGER NOT NULL> NOT NULL"}}},
		{query: "UNWIND [1] AS one WITH one RETURN valueType([1, null]) AS v", rows: [][]interface{}{[]interface{}{"LIST<INTEGER> NOT NULL"}}},
		{query: "UNWIND [1] AS one WITH one RETURN valueType([]) AS v", rows: [][]interface{}{[]interface{}{"LIST<NOTHING> NOT NULL"}}},
		{query: "UNWIND [1] AS one WITH one RETURN valueType({a: 1}) AS v", rows: [][]interface{}{[]interface{}{"MAP NOT NULL"}}},
		{query: "UNWIND [1] AS one WITH one RETURN valueType(date('2024-01-01')) AS v", rows: [][]interface{}{[]interface{}{"DATE NOT NULL"}}},
		{query: "UNWIND [1] AS one WITH one RETURN valueType(duration('P1D')) AS v", rows: [][]interface{}{[]interface{}{"DURATION NOT NULL"}}},
		{query: "UNWIND [1] AS one WITH one RETURN valueType([[1], ['a']]) AS v", rows: [][]interface{}{[]interface{}{"LIST<LIST<STRING NOT NULL> NOT NULL | LIST<INTEGER NOT NULL> NOT NULL> NOT NULL"}}},
		{query: "UNWIND [1] AS one WITH one RETURN valueType(localtime('12:00')) AS v", rows: [][]interface{}{[]interface{}{"LOCAL TIME NOT NULL"}}},
		{query: "UNWIND [1] AS one WITH one RETURN valueType(datetime('2024-01-01T00:00Z')) AS v", rows: [][]interface{}{[]interface{}{"ZONED DATETIME NOT NULL"}}},
		{query: "UNWIND [1] AS one WITH one RETURN reduce(acc = 0, x IN [1, 2, 3] | acc + x) AS v", rows: [][]interface{}{[]interface{}{int64(6)}}},
		{query: "UNWIND [1] AS one WITH one RETURN reduce(acc = '', x IN ['a', 'b'] | acc + x) AS v", rows: [][]interface{}{[]interface{}{"ab"}}},
		{query: "UNWIND [1] AS one WITH one RETURN reduce(acc = 0, x IN [] | acc + x) AS v", rows: [][]interface{}{[]interface{}{int64(0)}}},
		{query: "UNWIND [1] AS one WITH one RETURN reduce(acc = 0, x IN null | acc + x) AS v", rows: [][]interface{}{[]interface{}{nil}}},
		{query: "UNWIND [1] AS one WITH one RETURN reduce(acc = 1, x IN [2, 3] | acc * x) AS v", rows: [][]interface{}{[]interface{}{int64(6)}}},
		{query: "UNWIND [1] AS one WITH one RETURN reduce(acc = [], x IN [1, 2] | acc + [x * 2]) AS v", rows: [][]interface{}{[]interface{}{[]interface{}{int64(2), int64(4)}}}},
		{query: "UNWIND [1] AS one WITH one WITH [1, 2, 3] AS l RETURN reduce(acc = 0, x IN l | acc + x) AS v", rows: [][]interface{}{[]interface{}{int64(6)}}},
		{query: "UNWIND [1] AS one WITH one WITH [1, 2, 3] AS l RETURN l[0] AS v", rows: [][]interface{}{[]interface{}{int64(1)}}},
		{query: "UNWIND [1] AS one WITH one WITH [1, 2, 3] AS l RETURN l[-1] AS v", rows: [][]interface{}{[]interface{}{int64(3)}}},
		{query: "UNWIND [1] AS one WITH one WITH [1, 2, 3] AS l RETURN l[1..] AS v", rows: [][]interface{}{[]interface{}{[]interface{}{int64(2), int64(3)}}}},
		{query: "UNWIND [1] AS one WITH one WITH [1, 2, 3] AS l RETURN l[..-1] AS v", rows: [][]interface{}{[]interface{}{[]interface{}{int64(1), int64(2)}}}},
		{query: "UNWIND [1] AS one WITH one WITH [1, 2, 3] AS l RETURN l[5] AS v", rows: [][]interface{}{[]interface{}{nil}}},
		{query: "UNWIND [1] AS one WITH one WITH [1, 2, 3] AS l RETURN l[null] AS v", rows: [][]interface{}{[]interface{}{nil}}},
		{query: "UNWIND [1] AS one WITH one WITH {a: 1} AS m RETURN m['a'] AS v", rows: [][]interface{}{[]interface{}{int64(1)}}},
		{query: "UNWIND [1] AS one WITH one WITH {a: 1} AS m RETURN m['b'] AS v", rows: [][]interface{}{[]interface{}{nil}}},
		{query: "UNWIND [1] AS one WITH one WITH null AS l RETURN l[0] AS v", rows: [][]interface{}{[]interface{}{nil}}},
		{query: "UNWIND [1] AS one WITH one WITH {a: 1, b: 2} AS m RETURN m {.a, c: 3} AS v", rows: [][]interface{}{[]interface{}{map[string]interface{}{"a": int64(1), "c": int64(3)}}}},
		{query: "UNWIND [1] AS one WITH one WITH {a: 1, b: 2} AS m RETURN m {.*} AS v", rows: [][]interface{}{[]interface{}{map[string]interface{}{"a": int64(1), "b": int64(2)}}}},
		{query: "UNWIND [1] AS one WITH one WITH {a: 1, b: 2} AS m RETURN m {.*, b: null} AS v", rows: [][]interface{}{[]interface{}{map[string]interface{}{"a": int64(1), "b": nil}}}},
		{query: "UNWIND [1] AS one WITH one WITH {a: 1} AS m RETURN m {.missing} AS v", rows: [][]interface{}{[]interface{}{map[string]interface{}{"missing": nil}}}},
		{query: "UNWIND [1] AS one WITH one WITH null AS m RETURN m {.a} AS v", rows: [][]interface{}{[]interface{}{nil}}},
		{query: "UNWIND [1] AS one WITH one WITH {a: {b: 2}} AS m RETURN m {x: m.a.b} AS v", rows: [][]interface{}{[]interface{}{map[string]interface{}{"x": int64(2)}}}},
		{query: "UNWIND [1, 2, 3] AS x RETURN x AS v SKIP 1", rows: [][]interface{}{[]interface{}{int64(2)}, []interface{}{int64(3)}}},
		{query: "UNWIND [1, 2, 3] AS x RETURN x AS v LIMIT 1", rows: [][]interface{}{[]interface{}{int64(1)}}},
		{query: "UNWIND [1, 2, 3] AS x RETURN x AS v SKIP -1", err: "Neo.ClientError.Statement.SyntaxError"},
		{query: "UNWIND [1, 2, 3] AS x RETURN x AS v LIMIT -1", err: "Neo.ClientError.Statement.SyntaxError"},
		{query: "UNWIND [1, 2, 3] AS x RETURN x AS v LIMIT 1.5", err: "Neo.ClientError.Statement.SyntaxError"},
		{query: "UNWIND [1, 2, 3] AS x RETURN x AS v LIMIT 'a'", err: "Neo.ClientError.Statement.SyntaxError"},
		{query: "UNWIND [1, 2, 3] AS x RETURN x AS v SKIP 1.0", err: "Neo.ClientError.Statement.SyntaxError"},
		{query: "UNWIND [1, 2, 3] AS x RETURN x AS v LIMIT null", err: "Neo.ClientError.Statement.SyntaxError"},
		{query: "UNWIND [1, 2, 3] AS x RETURN x AS v SKIP 0 LIMIT 0", rows: [][]interface{}{}},
		{query: "UNWIND [1, 2, 3] AS x RETURN x AS v LIMIT 1 + 1", rows: [][]interface{}{[]interface{}{int64(1)}, []interface{}{int64(2)}}},
	} {
		result, err := exec.Execute(ctx, tc.query, nil)
		if tc.err != "" {
			require.ErrorContains(t, err, tc.err, tc.query)
			continue
		}
		require.NoError(t, err, tc.query)
		rows := result.Rows
		if rows == nil {
			rows = [][]interface{}{}
		}
		require.Equal(t, len(tc.rows), len(rows), tc.query)
		for i := range tc.rows {
			for j := range tc.rows[i] {
				want, got := tc.rows[i][j], rows[i][j]
				if f, ok := want.(float64); ok && math.IsNaN(f) {
					g, isFloat := got.(float64)
					require.True(t, isFloat && math.IsNaN(g), tc.query)
					continue
				}
				require.Equal(t, want, got, tc.query)
			}
		}
	}
}
