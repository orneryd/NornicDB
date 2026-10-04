package cypher

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMonsterExpressionAdmission(t *testing.T) {
	for _, query := range []string{
		`CREATE (a:T {n: reduce(s = 0, x IN [1, 2] \| s + x)}) RETURN a.n`,
		`CREATE (n:T {a: [x IN [1, 2] \| x * 2]}) RETURN n.a`,
		`CREATE (:Before) CREATE (a:T {n: reduce(s = 0, x IN [1, 2] \| s + x)}) RETURN a.n`,
		`MATCH (n:K {id: 1}) OPTIONAL<tab>MATCH (n)-[:R]->(m) RETURN n.id`,
		`RETURN startNode(r).id AS s`,
		`RETURN endNode(r).id AS s`,
		`CREATE (n:T) SET n.x = startNode(r).id RETURN n`,
		`MATCH (n:K) SET n.x = endNode(r).id RETURN n`,
	} {
		for _, populated := range []bool{false, true} {
			t.Run(query+map[bool]string{false: "/empty", true: "/populated"}[populated], func(t *testing.T) {
				exec, _ := newTestExecutor(t)
				ctx := context.Background()
				if populated {
					_, err := exec.Execute(ctx, `CREATE (:K {id: 1})-[:R]->(:K {id: 2})`, nil)
					require.NoError(t, err)
				}
				before, err := exec.Execute(ctx, `MATCH (n) RETURN n`, nil)
				require.NoError(t, err)
				_, err = exec.Execute(ctx, query, nil)
				require.Error(t, err)
				after, err := exec.Execute(ctx, `MATCH (n) RETURN n`, nil)
				require.NoError(t, err)
				require.Equal(t, before.Rows, after.Rows)
			})
		}
	}
}

func TestMonsterScalarFunctionContracts(t *testing.T) {
	for _, test := range []struct {
		expression string
		value      interface{}
		want       interface{}
	}{
		{"toString($v)", float64(1), "1.0"},
		{"toString($v)", float64(1e-7), "1.0E-7"},
		{"toString($v)", math.Copysign(0, -1), "-0.0"},
		{"toUpper($v)", "straße", "STRASSE"},
		{"upper($v)", "straße", "STRASSE"},
		{"toLower($v)", "\u0130", "i\u0307"},
		{"toLower($v)", "\u0130I", "i\u0307i"},
		{"lower($v)", "\u0130", "i\u0307"},
		{"toInteger($v)", true, int64(1)},
		{"toInteger($v)", false, int64(0)},
		{"toIntegerOrNull($v)", true, int64(1)},
		{"toBoolean($v)", int64(1), true},
		{"toBoolean($v)", int64(0), false},
		{"toBoolean($v)", int64(-1), true},
		{"toFloatOrNull($v)", true, nil},
		{"toStringOrNull($v)", float64(1), "1.0"},
		{"toStringList([$v])", float64(1), []interface{}{"1.0"}},
		{"'v=' + $v", float64(1), "v=1.0"},
		{"toString($v)", float64(1e16), "1.0E16"},
		{"toString($v)", math.Inf(1), "Infinity"},
		{"toString($v)", math.Inf(-1), "-Infinity"},
		{"toString($v)", math.NaN(), "NaN"},
		{"toString($v)", nil, nil},
		{"toUpper($v)", nil, nil},
		{"toLower($v)", nil, nil},
		{"toInteger($v)", nil, nil},
		{"toFloat($v)", nil, nil},
		{"toBoolean($v)", nil, nil},
		{"size($v)", nil, nil},
		{"abs($v)", nil, nil},
		{"substring($v, 1)", nil, nil},
		{"substring('abc', $v)", nil, nil},
		{"substring('abc', 1, $v)", nil, nil},
		{"left($v, 1)", nil, nil},
		{"right($v, 1)", nil, nil},
		{"replace($v, 'a', 'b')", nil, nil},
		{"replace('a', $v, 'b')", nil, nil},
		{"split($v, ',')", nil, nil},
		{"split('a,b', $v)", nil, nil},
		{"reverse($v)", nil, nil},
		{"head($v)", nil, nil},
		{"last($v)", nil, nil},
		{"tail($v)", nil, nil},
	} {
		t.Run(test.expression+"/"+fmt.Sprint(test.value), func(t *testing.T) {
			for _, query := range []string{
				"RETURN " + test.expression + " AS value",
				"WITH " + test.expression + " AS value RETURN value",
				"UNWIND [0] AS unused RETURN " + test.expression + " AS value",
				"CREATE (n:T {value: " + test.expression + "}) RETURN n.value AS value",
				"CREATE (n:T) SET n.value = " + test.expression + " RETURN n.value AS value",
			} {
				t.Run(query, func(t *testing.T) {
					exec, _ := newTestExecutor(t)
					result, err := exec.Execute(context.Background(), query, map[string]interface{}{"v": test.value})
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{test.want}}, result.Rows)
				})
			}
		})
	}
}

func TestMonsterIssueBodyReplay(t *testing.T) {
	t.Run("null functions in CREATE properties", func(t *testing.T) {
		executor, _ := newTestExecutor(t)
		result, err := executor.Execute(context.Background(), "CREATE (n:T {upper: toUpper(null), text: toString(null), part: substring(null, 1), rest: tail(null)}) RETURN properties(n)", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{map[string]interface{}{}}}, result.Rows)
	})
	for query, want := range map[string]interface{}{
		"RETURN toString(1.0)":                       "1.0",
		"RETURN toString(1e-7)":                      "1.0E-7",
		"RETURN toString(-0.0)":                      "-0.0",
		"RETURN toString(123456789.0)":               "1.23456789E8",
		"RETURN toStringOrNull(1.0)":                 "1.0",
		"RETURN 'v=' + 1.0":                          "v=1.0",
		"WITH 3.0 AS f RETURN toString(f)":           "3.0",
		"RETURN toUpper(null)":                       nil,
		"RETURN toInteger(true)":                     int64(1),
		"RETURN toBoolean(1)":                        true,
		"RETURN radians(180)":                        math.Pi,
		"RETURN char_length('abc')":                  int64(3),
		"RETURN character_length('abc')":             int64(3),
		"RETURN upper('a')":                          "A",
		"RETURN btrim('  a  ')":                      "a",
		"RETURN ltrim('xxa', 'x')":                   "a",
		"RETURN normalize('a')":                      "a",
		"RETURN trim(BOTH 'x' FROM 'xax')":           "a",
		"RETURN valueType(1)":                        "INTEGER NOT NULL",
		"RETURN nullIf(1, 1)":                        nil,
		"RETURN 9007199254740993 > 9007199254740992": true,
		"RETURN 9007199254740993 = $v":               false, // an integer and a float compare exactly (#893)
		"UNWIND [9007199254740993, 9007199254740992] AS x WITH x ORDER BY x RETURN collect(x)": []interface{}{int64(9007199254740992), int64(9007199254740993)},
	} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/explicit=%v", query, explicit), func(t *testing.T) {
				exec, _ := newTestExecutor(t)
				ctx := context.Background()
				if explicit {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				result, err := exec.Execute(ctx, query, map[string]interface{}{"v": float64(9007199254740992)})
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{want}}, result.Rows)
				if explicit {
					_, err := exec.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
				}
			})
		}
	}
	for _, function := range []string{"all", "any", "none", "single"} {
		exec, _ := newTestExecutor(t)
		for _, query := range []string{
			"RETURN " + function + "(x IN null WHERE x > 0)",
			"WITH null AS m RETURN " + function + "(x IN m WHERE x > 0)",
		} {
			result, err := exec.Execute(context.Background(), query, nil)
			require.NoError(t, err, query)
			require.Equal(t, [][]interface{}{{nil}}, result.Rows, query)
		}
	}
}

func TestMonsterIssueBodyErrors(t *testing.T) {
	for _, query := range []string{
		"RETURN toFloat(true)",
		"RETURN datetime('x')", "RETURN localtime('x')", "RETURN time('x')", "RETURN localdatetime('x')", "RETURN duration('x')",
		"RETURN datetime('2020-02-30T10:00')",
		"WITH 'x' AS s RETURN datetime(s)", "UNWIND ['x'] AS s RETURN duration(s)",
		"CREATE (n:TP {d: datetime('x')}) RETURN n.d",
		"RETURN date({year: 2020, month: 13})", "RETURN date({year: 2020, month: 0})", "RETURN date({year: 2020, month: 2, day: 30})",
		"RETURN datetime({year: 2020, month: 1, day: 1, hour: 25})", "RETURN localtime({hour: 24, minute: 61})",
		"UNWIND [1] AS a UNWIND [0] AS b RETURN a / b",
		"UNWIND [1] AS a UNWIND [0] AS b RETURN CASE WHEN true THEN a / b END",
		"UNWIND [1] AS a UNWIND [0] AS b RETURN [a / b]",
		"UNWIND [1] AS a UNWIND [0] AS b RETURN {k: a / b}",
		"UNWIND [1] AS a UNWIND [0] AS b RETURN toString(a / b)",
		"UNWIND [1] AS a UNWIND [0] AS b RETURN coalesce(a / b, 2)",
		"RETURN coalesce(1/0, 2)", "RETURN CASE WHEN true THEN 1/0 END",
	} {
		t.Run(query, func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			_, err := exec.Execute(context.Background(), query, nil)
			require.Error(t, err)
			result, err := exec.Execute(context.Background(), "MATCH (n) RETURN count(n)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
		})
	}
}

func TestMonsterUnresolvedExpressionProtocol(t *testing.T) {
	exec, _ := newTestExecutor(t)
	for _, expression := range []string{"unknownVariable", "(n)-->()", "->()", ">()"} {
		require.Equal(t, expression, exec.evaluateExpressionFromValues(expression, map[string]interface{}{}))
	}
	for _, query := range []string{
		`RETURN '\\| <tab>' AS value`,
		"RETURN 1 AS value // \\ <tab>",
		"RETURN 1 AS value /* \\ <tab> */",
		"WITH 2 AS tab RETURN 1<tab>1 AS value",
		"MATCH (n:K) OPTIONAL\tMATCH (n)-->(m) RETURN n",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
}

func TestMonsterPropertyExpressionReverification(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			executor, _ := newTestExecutor(t)
			ctx := context.Background()
			if explicit {
				_, err := executor.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			result, err := executor.Execute(ctx, "CREATE (n:T {s: 'a' + 'b', l: size([1,2]), m: {k: 1}.k}) RETURN n.s, n.l, n.m", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"ab", int64(2), int64(1)}}, result.Rows)
			if explicit {
				_, err = executor.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
			for _, query := range []string{
				"CREATE (n:Invalid {v: 1 +}) RETURN n.v",
				"CREATE (:Before) CREATE (n:Invalid {v: 1 +}) RETURN n.v",
				"MERGE (n:Invalid {v: 1 +}) RETURN n.v",
				"MATCH (n:T) SET n.v = 1 + RETURN n.v",
			} {
				if explicit {
					_, err = executor.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				_, err := executor.Execute(ctx, query, nil)
				require.Error(t, err, query)
				require.Contains(t, statusText(err), "SyntaxError", query)
				if explicit {
					_, err = executor.Execute(ctx, "ROLLBACK", nil)
					require.NoError(t, err)
				}
			}
			result, err = executor.Execute(ctx, "MATCH (n) RETURN count(n)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
		})
	}
}
