package cypher

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/stretchr/testify/require"
)

func TestCanonicalBoundWrittenProjectionNames(t *testing.T) {
	cases := []struct {
		name, query string
		columns     []string
		rows        [][]interface{}
		explicit    []string
	}{
		{"quoted first", "WITH 1 AS x RETURN `x` + 1, x + 1", []string{"`x` + 1", "x + 1"}, [][]interface{}{{int64(2), int64(2)}}, nil},
		{"quoted second", "WITH 1 AS x RETURN x + 1, `x` + 1", []string{"x + 1", "`x` + 1"}, [][]interface{}{{int64(2), int64(2)}}, nil},
		{"decoded binding", "WITH 1 AS `x y` RETURN `x y`, `x y` + 1", []string{"x y", "`x y` + 1"}, [][]interface{}{{int64(1), int64(2)}}, nil},
		{"mixed wildcard", "WITH 1 AS `x y` RETURN *, `x y` + 1 AS y", []string{"x y", "y"}, [][]interface{}{{int64(1), int64(2)}}, []string{"y"}},
		{"wildcard sorting", "WITH 1 AS `b z`, 2 AS `a z` RETURN *", []string{"a z", "b z"}, [][]interface{}{{int64(2), int64(1)}}, []string{}},
		{"empty input", "UNWIND [] AS ignored WITH 1 AS x RETURN `x` + 1, x + 1", []string{"`x` + 1", "x + 1"}, [][]interface{}{}, nil},
		{"union", "WITH 1 AS x RETURN `x` + 1 UNION ALL WITH 2 AS x RETURN `x` + 1", []string{"`x` + 1"}, [][]interface{}{{int64(2)}, {int64(3)}}, nil},
	}
	for _, mode := range []string{"", "EXPLAIN ", "PROFILE ", "EXPLAIN /* mode gap */\n  ", "PROFILE /* mode gap */\n  "} {
		for _, tc := range cases {
			t.Run(mode+tc.name, func(t *testing.T) {
				exec, ctx := newUnitExecutor(t)
				result, err := exec.Execute(ctx, mode+tc.query, nil)
				require.NoError(t, err, mode+tc.query)
				require.Equal(t, tc.columns, result.Columns)
				exec.StatementFacts(mode+tc.query, resultmetadata.KnownScope(nil), false, RawSource).Visit(func(statement resultmetadata.Statement) {
					if len(statement.Branches) != 0 {
						statement = statement.Branches[0]
					}
					last := statement.Clauses[len(statement.Clauses)-1]
					require.NotNil(t, last.Projection)
					expected := tc.explicit
					if expected == nil {
						expected = tc.columns
					}
					last.Projection.WrittenNames.Visit(func(columns []string) {
						require.Equal(t, expected, columns)
					}, func() { t.Fatal("constructor omitted written projection declarations") })
				}, func() { t.Fatal("header construction declined") }, func(reason resultmetadata.Unresolved) { t.Fatal(reason.Err) })
				if mode == "" || strings.HasPrefix(mode, "PROFILE") {
					if len(tc.rows) == 0 {
						require.Empty(t, result.Rows)
					} else {
						require.Equal(t, tc.rows, result.Rows)
					}
				}
			})
		}
	}
}
