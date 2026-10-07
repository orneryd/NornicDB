package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestUnion_BoundContextResolvesBranchValues pins the final §6.2 piece: UNION
// branches execute against the caller's bound child context, so RETURN
// expressions can reference values from the value scope (bare identifiers,
// property access and arithmetic) without query-text re-entry.
func TestUnion_BoundContextResolvesBranchValues(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := withValueBindings(context.Background(), map[string]interface{}{
		"x":    int64(7),
		"row":  map[string]interface{}{"name": "ann"},
		"flag": true,
	})

	res, err := exec.executeUnion(ctx, "RETURN x AS v UNION RETURN 2 AS v", false)
	require.NoError(t, err)
	require.Equal(t, []string{"v"}, res.Columns)
	require.Equal(t, [][]interface{}{{int64(7)}, {int64(2)}}, res.Rows)

	// Compound expressions over bound values resolve through the row
	// evaluator with the seeded scope.
	res, err = exec.executeUnion(ctx, "RETURN x + 1 AS v UNION RETURN 0 AS v", false)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(8)}, {int64(0)}}, res.Rows)

	// Property access on a bound map.
	res, err = exec.executeUnion(ctx, "RETURN row.name AS v UNION RETURN 'z' AS v", false)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"ann"}, {"z"}}, res.Rows)

	// A UNION inside a CALL subquery still composes its branches.
	sub, err := exec.Execute(ctx, "CALL { RETURN 1 AS a UNION RETURN 2 AS a }", getParamsFromContext(ctx))
	require.NoError(t, err)
	require.NotNil(t, sub)
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, sub.Rows)
}
