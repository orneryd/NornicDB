package cypher

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Author: #1640 lane. Parameter value admission consumes the release framing
// owner's mode fact for every supported preamble/mode order, including FINISH.
func TestCanonicalBoundFramedParameterMode(t *testing.T) {
	t.Setenv("NORNICDB_CYPHER_QUERY_NORMALIZATION", "false")
	for _, version := range []string{"5", "25"} {
		for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
			for _, order := range []string{"preamble-first", "mode-first"} {
				t.Run(version+"/"+mode+"/"+order, func(t *testing.T) {
					exec, ctx := newUnitExecutor(t)
					prefix := "CYPHER " + version + " " + mode
					if order == "mode-first" {
						prefix = mode + "CYPHER " + version + " "
					}
					query := prefix + "UNWIND [] AS input WITH $missing AS value /* before */ FINISH /* after */"
					result, err := exec.Execute(ctx, query, nil)
					if strings.HasPrefix(mode, "EXPLAIN") {
						require.NoError(t, err)
						require.Empty(t, result.Columns)
						require.Empty(t, result.Rows)
						return
					}
					require.Error(t, err)
					code, detail, message := boundRuleError(err)
					require.Equal(t, "Neo.ClientError.Statement.ParameterMissing", code)
					require.Equal(t, "MissingParameter", detail)
					require.Equal(t, "Neo.ClientError.Statement.ParameterMissing: Expected parameter(s): missing", message)
				})
			}
		}
	}
}
