package cypher

// NornicDB #893: integer edge cases as Neo4j has them. The most negative
// integer divided by -1 wraps; an integer and a float compare by their exact
// values; size() of a stored value of the wrong type is a TypeError that
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
			run := func(query string) (*ExecuteResult, error) {
				if mode == "explicit transaction" {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					defer func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) }()
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
			} {
				result, err := run(tc.query)
				require.NoError(t, err, tc.query)
				require.Equal(t, tc.rows, result.Rows, tc.query)
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
