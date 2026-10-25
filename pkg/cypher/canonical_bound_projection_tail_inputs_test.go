package cypher

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Author: #1640 lane (Codex). Root expression handles and symbol handles must
// select the same constructor-owned tail constants. Weak input availability is
// a NornicDB bare-CALL extension; a declared-and-dropped local stays dropped.
func TestCanonicalBoundProjectionTailInputs(t *testing.T) {
	for _, open := range []string{"CALL { ", "CALL (value) { ", "CALL (*) { "} {
		for _, input := range []string{"WITH 7 AS value ", "UNWIND [] AS value ", "UNWIND [7] AS value "} {
			for _, project := range []string{"count(*) AS total, 1 AS marker", "DISTINCT seed AS total, 1 AS marker"} {
				for _, tail := range []string{"ORDER BY value", "ORDER BY value+1", "WHERE value=7", "WHERE value+1=8"} {
					for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
						t.Run(fmt.Sprintf("%s/%s/%s/%s/%s", open, input, project, tail, mode), func(t *testing.T) {
							executor, ctx := newUnitExecutor(t)
							result, err := executor.Execute(ctx, mode+input+open+"UNWIND [1,2] AS seed WITH "+project+" "+tail+" RETURN total AS x } RETURN x ORDER BY x", nil)
							if open == "CALL { " && strings.HasPrefix(tail, "ORDER BY") {
								require.Error(t, err, "a weak input is not an explicitly retained ordering constant")
								require.Equal(t, "In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: value", err.Error())
								var status interface {
									BoltErrorCode() string
									BoltErrorDetail() string
								}
								require.True(t, errors.As(err, &status))
								require.Equal(t, "Neo.ClientError.Statement.SyntaxError", status.BoltErrorCode())
								require.Equal(t, "UndefinedVariable", status.BoltErrorDetail())
								return
							}
							require.NoError(t, err)
							require.Equal(t, []string{"x"}, result.Columns)
							if mode == "EXPLAIN " || input == "UNWIND [] AS value " {
								require.Empty(t, result.Rows)
							} else if project == "count(*) AS total, 1 AS marker" {
								require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
							} else {
								require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, result.Rows)
							}
						})
					}
				}
			}
		}
	}
	for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
		for _, input := range []string{"WITH 7 AS value ", "UNWIND [] AS value "} {
			t.Run("dropped-local/"+mode+input, func(t *testing.T) {
				executor, ctx := newUnitExecutor(t)
				_, err := executor.Execute(ctx, mode+input+"CALL { UNWIND [2,3] AS value WITH count(*) AS total ORDER BY value RETURN total AS x } RETURN x", nil)
				require.Error(t, err, "WITH dropped the body declaration; the original weak input cannot return")
			})
		}
	}
}
