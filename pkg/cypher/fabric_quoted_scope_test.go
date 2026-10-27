package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Fabric keeps backtick-quoted variables as written for its planner and
// remote participants. Its statement-wide scope check must still read them
// as the single-graph route does, and report them as the client wrote them.
func TestFabricScopeValidationReadsQuotedVariables(t *testing.T) {
	ctx := context.Background()
	exec := newFabricCallRowsExecutor(t)
	for _, name := range []string{"`x`", "`a b`", "`embedding`"} {
		for _, body := range []string{
			"WITH 7 AS @ FOREACH (@ IN [9] | CREATE (:QuotedScope)) RETURN @ AS k",
			"CREATE (@:QuotedScope {k:7}) RETURN @.k AS k",
			"UNWIND [7] AS @ WITH @ WHERE @ > 0 RETURN @ AS k",
			"WITH 7 AS @ CALL (@) { RETURN @ AS inner } RETURN inner AS k",
		} {
			query := strings.ReplaceAll(body, "@", name)
			for _, prefix := range []string{"", "USE comp.one "} {
				result, err := exec.Execute(ctx, prefix+query, nil)
				require.NoError(t, err, prefix+query)
				require.Equal(t, []string{"k"}, result.Columns, prefix+query)
				require.Len(t, result.Rows, 1, prefix+query)
				require.EqualValues(t, 7, result.Rows[0][0], prefix+query)
			}
		}
		query := "WITH 1 AS " + name + " RETURN `missing name`, " + name + " AS k"
		_, nativeErr := exec.Execute(ctx, query, nil)
		require.Error(t, nativeErr, query)
		_, fabricErr := exec.Execute(ctx, "USE comp.one "+query, nil)
		require.Error(t, fabricErr, query)
		require.Equal(t, nativeErr.Error(), fabricErr.Error(), query)
		require.Contains(t, fabricErr.Error(), "`missing name`", query)
	}
}

// Both the quoted-name and label-expression rewrites belong to the scope
// validation copy, including the EXPLAIN and PROFILE composite entries.
func TestFabricQuotedLabelScopeComposition(t *testing.T) {
	ctx := context.Background()
	exec := newFabricCallRowsExecutor(t)
	for _, body := range []string{
		"WITH null AS `a b` RETURN (`a b`):A AS v",
		"WITH $`a b` AS `a``b` RETURN (`a``b`):A|B AS v",
	} {
		for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
			query := mode + "USE comp.one " + body
			result, err := exec.Execute(ctx, query, map[string]interface{}{"a b": nil})
			require.NoError(t, err, query)
			if mode != "EXPLAIN " {
				require.Equal(t, []string{"v"}, result.Columns, query)
				require.Equal(t, [][]interface{}{{nil}}, result.Rows, query)
			}
		}
	}
	for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
		query := mode + "USE comp.one WITH null AS `a b` RETURN (`missing name`):A AS v"
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), "Variable `missing name` not defined", query)
	}
}
