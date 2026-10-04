package cypher

// NornicDB #893: integer edge cases as Neo4j has them. The most negative
// integer divided by -1 wraps; an integer and a float compare by their exact
// values, except =, < and > of two literals, which Neo4j folds as floats; size() of a stored value of the wrong type is a TypeError that
// renders the value; negating the most negative integer overflows. Answers
// are Neo4j 5.26.30's.

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIssue893IntegerEdges(t *testing.T) {
	for _, mode := range []string{"auto-commit", "explicit transaction"} {
		t.Run(mode, func(t *testing.T) {
			exec := newAsyncStackTestExecutor(t)
			ctx := context.Background()
			for _, setup := range []string{
				"CREATE (:E {id: 1, v: -9223372036854775808, f: 2.5, b: true, s: 'x', l: [1], big: 9007199254740993})",
				"CREATE (:T {d: date('2020-01-02'), du: duration('P1D'), p: point({x: 1, y: 2})})",
			} {
				_, err := exec.Execute(ctx, setup, nil)
				require.NoError(t, err)
			}
			// Each statement runs on its own: in an explicit transaction an
			// error fails the transaction.
			run := func(query string, params ...map[string]interface{}) (*ExecuteResult, error) {
				if mode == "explicit transaction" {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					defer func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) }()
				}
				if len(params) > 0 {
					return exec.Execute(ctx, query, params[0])
				}
				return exec.Execute(ctx, query, nil)
			}
			for _, tc := range []struct {
				query string
				rows  [][]interface{}
			}{
				{"RETURN -9223372036854775808 / -1 AS r", [][]interface{}{{int64(math.MinInt64)}}},
				{"WITH -9223372036854775808 AS x RETURN x / -1 AS r, x % -1 AS m", [][]interface{}{{int64(math.MinInt64), int64(0)}}},
				{"MATCH (n:E) RETURN n.v / -1 AS r", [][]interface{}{{int64(math.MinInt64)}}},
				{"MATCH (n:E) RETURN abs(n.v) AS r", [][]interface{}{{int64(math.MinInt64)}}},
				{"MATCH (n:E) RETURN n.big = 9007199254740992.0 AS eq, n.big > 9007199254740992.0 AS gt, n.big < 9007199254740994.0 AS lt", [][]interface{}{{false, true, true}}},
				{"MATCH (n:E) WHERE n.big = 9007199254740992.0 RETURN n.id", [][]interface{}{}},
				{"MATCH (n:E) WHERE n.big > 9007199254740992.0 RETURN n.id", [][]interface{}{{int64(1)}}},
				{"RETURN 1 = 1.0 AS a, 2 > 1.5 AS b, -1 < -0.5 AS c, 3 <> 3.0 AS d", [][]interface{}{{true, true, true, false}}},
				// =, < and > of two constant numeric expressions compare as
				// floats, as Neo4j folds them; <>, <= and >= and values from
				// variables, functions, lists and IN compare exactly.
				{"RETURN 9007199254740993 = 9007199254740992.0 AS v", [][]interface{}{{true}}},
				{"MATCH (n:E) WHERE 9007199254740992 + 1 = 9007199254740992.0 RETURN n.id AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (n:E) WHERE 9007199254740992 + 1 <> 9007199254740992.0 RETURN n.id AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (n:E) WHERE 9007199254740992 + 1 > 9007199254740992.0 RETURN n.id AS v", [][]interface{}{}},
				{"MATCH (n:E) WHERE n.id + 9007199254740992 = 9007199254740992.0 RETURN n.id AS v", [][]interface{}{}},
				{"RETURN 9007199254740993 > 9007199254740992.0 AS v", [][]interface{}{{false}}},
				{"RETURN 9007199254740993 <> 9007199254740992.0 AS v", [][]interface{}{{true}}},
				{"RETURN 9007199254740992.0 = 9007199254740993 AS v", [][]interface{}{{true}}},
				{"RETURN 9007199254740993 >= 9007199254740992.0 AS v", [][]interface{}{{true}}},
				{"WITH 9007199254740993 AS i RETURN i = 9007199254740992.0 AS v", [][]interface{}{{false}}},
				{"WITH 9007199254740992.0 AS f RETURN 9007199254740993 = f AS v", [][]interface{}{{false}}},
				{"WITH 9007199254740993 AS i, 9007199254740992.0 AS f RETURN i = f AS v", [][]interface{}{{false}}},
				{"RETURN 9007199254740992 + 1 = 9007199254740992.0 AS v", [][]interface{}{{true}}},
				{"RETURN -9007199254740993 = -9007199254740992.0 AS v", [][]interface{}{{true}}},
				{"RETURN [9007199254740993] = [9007199254740992.0] AS v", [][]interface{}{{false}}},
				{"RETURN 9007199254740993 IN [9007199254740992.0] AS v", [][]interface{}{{false}}},
				{"RETURN CASE 9007199254740993 WHEN 9007199254740992.0 THEN 1 ELSE 0 END AS v", [][]interface{}{{int64(1)}}},
				{"RETURN CASE WHEN 9007199254740993 = 9007199254740992.0 THEN 1 ELSE 0 END AS v", [][]interface{}{{int64(1)}}},
				{"UNWIND [9007199254740993] AS i RETURN i = 9007199254740992.0 AS v", [][]interface{}{{false}}},
				{"RETURN toInteger('9007199254740993') = 9007199254740992.0 AS v", [][]interface{}{{false}}},
				{"RETURN 9007199254740993 = toFloat('9007199254740992.0') AS v", [][]interface{}{{false}}},
				{"RETURN 9007199254740993 = 9007199254740992.0 AND true AS v", [][]interface{}{{true}}},
				{"RETURN NOT (9007199254740993 = 9007199254740992.0) AS v", [][]interface{}{{false}}},
				{"MATCH (n:E) RETURN 9007199254740993 = 9007199254740992.0 AS v", [][]interface{}{{true}}},
				{"MATCH (n:E) WHERE 9007199254740993 = 9007199254740992.0 RETURN n.id AS v", [][]interface{}{{int64(1)}}},
				{"RETURN 9007199254740993 < 9007199254740992.0 AS v", [][]interface{}{{false}}},
				{"RETURN 9007199254740993 = 9007199254740993.0 AS v", [][]interface{}{{true}}},
				{"RETURN 9007199254740993 <= 9007199254740992.0 AS v", [][]interface{}{{false}}},
				{"RETURN 9007199254740992.0 <> 9007199254740993 AS v", [][]interface{}{{true}}},
				{"RETURN 9007199254740992.0 < 9007199254740993 AS v", [][]interface{}{{false}}},
				{"RETURN 9007199254740992.0 > 9007199254740993 AS v", [][]interface{}{{false}}},
				{"RETURN 9007199254740992.0 <= 9007199254740993 AS v", [][]interface{}{{true}}},
				{"RETURN 9007199254740992.0 >= 9007199254740993 AS v", [][]interface{}{{false}}},
				{"RETURN NOT (9007199254740993 <> 9007199254740992.0) AS v", [][]interface{}{{false}}},
				{"RETURN CASE 9007199254740992.0 WHEN 9007199254740993 THEN 1 ELSE 0 END AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (n:E) WHERE 9007199254740993 <> 9007199254740992.0 RETURN count(*) AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (n:E) WHERE 9007199254740993 <= 9007199254740992.0 RETURN count(*) AS v", [][]interface{}{{int64(0)}}},
				{"MATCH (n:E) WHERE 9007199254740993 > 9007199254740992.0 RETURN count(*) AS v", [][]interface{}{{int64(0)}}},
				{"RETURN 9007199254740993 = 9007199254740992.0 = 9007199254740993 AS v", [][]interface{}{{true}}},
				{"RETURN 1 < 9007199254740993 = 9007199254740992.0 AS v", [][]interface{}{{true}}},
				{"RETURN 9007199254740993 * 1 = 9007199254740992.0 AS v", [][]interface{}{{true}}},
				{"RETURN (9007199254740993) = (9007199254740992.0) AS v", [][]interface{}{{true}}},
				{"RETURN 9007199254740993 = 9007199254740992.0 = true AS v", [][]interface{}{{false}}},
				{"RETURN abs(9007199254740993) = 9007199254740992.0 AS v", [][]interface{}{{false}}},
				{"RETURN 1 + 9007199254740992 > 9007199254740992.0 AS v", [][]interface{}{{false}}},
			} {
				result, err := run(tc.query)
				require.NoError(t, err, tc.query)
				require.Equal(t, tc.rows, result.Rows, tc.query)
			}
			// A parameter isn't folded: it compares exactly.
			for _, tc := range []struct {
				query  string
				params map[string]interface{}
			}{
				{"RETURN $i = 9007199254740992.0 AS v", map[string]interface{}{"i": int64(9007199254740993)}},
				{"RETURN $i = $f AS v", map[string]interface{}{"i": int64(9007199254740993), "f": 9007199254740992.0}},
				{"RETURN 9007199254740993 = $f AS v", map[string]interface{}{"f": 9007199254740992.0}},
			} {
				result, err := run(tc.query, tc.params)
				require.NoError(t, err, tc.query)
				require.Equal(t, [][]interface{}{{false}}, result.Rows, tc.query)
			}
			for _, tc := range []struct{ query, code, message string }{
				{"UNWIND [1, 'ab'] AS x RETURN size(x) AS r", "Neo.ClientError.Statement.TypeError", "Invalid input for function 'size()': Expected a String or List, got: Long(1)"},
				{"UNWIND [1] AS x RETURN size(x) AS r", "Neo.ClientError.Statement.SyntaxError", "Type mismatch: expected String or List<T> but was Integer"},
				{"UNWIND [{a: 1}] AS m RETURN size(m) AS r", "Neo.ClientError.Statement.SyntaxError", "Type mismatch: expected String or List<T> but was Map"},
				{"MATCH (n:E) RETURN n.v * -1 AS r", "Neo.ClientError.Statement.ArithmeticError", "long overflow"},
				{"MATCH (n:E) RETURN -n.v AS r", "Neo.ClientError.Statement.ArithmeticError", "long overflow"},
				{"MATCH (n:E) RETURN size(n.v) AS r", "Neo.ClientError.Statement.TypeError", "Invalid input for function 'size()': Expected a String or List, got: Long(-9223372036854775808)"},
				{"MATCH (n:E) RETURN size(n.f) AS r", "Neo.ClientError.Statement.TypeError", "Invalid input for function 'size()': Expected a String or List, got: Double(2.500000e+00)"},
				{"MATCH (n:E) RETURN size(n.b) AS r", "Neo.ClientError.Statement.TypeError", "Invalid input for function 'size()': Expected a String or List, got: Boolean('true')"},
				{"MATCH (n:E) WHERE size(n.f) > 0 RETURN n.id", "Neo.ClientError.Statement.TypeError", "Invalid input for function 'size()': Expected a String or List, got: Double(2.500000e+00)"},
				{"RETURN size(1) AS r", "Neo.ClientError.Statement.SyntaxError", "Type mismatch: expected String or List<T> but was Integer"},
				{"WITH 1 AS x RETURN size(x) AS r", "Neo.ClientError.Statement.SyntaxError", "Type mismatch: expected String or List<T> but was Integer"},
				{"WITH 2.5 AS x RETURN size(x) AS r", "Neo.ClientError.Statement.SyntaxError", "Type mismatch: expected String or List<T> but was Float"},
				{"MATCH (n:T) RETURN size(n.d) AS r", "Neo.ClientError.Statement.TypeError", "Invalid input for function 'size()': Expected a String or List, got: 2020-01-02"},
				{"MATCH (n:T) RETURN size(n.du) AS r", "Neo.ClientError.Statement.TypeError", "Invalid input for function 'size()': Expected a String or List, got: P1D"},
				{"MATCH (n:T) RETURN size(n.p) AS r", "Neo.ClientError.Statement.TypeError", "Invalid input for function 'size()': Expected a String or List, got: point({x: 1.0, y: 2.0, crs: 'cartesian'})"},
			} {
				_, err := run(tc.query)
				require.Equal(t, tc.code+": "+tc.message, statusText(err), tc.query)
			}
		})
	}
}
