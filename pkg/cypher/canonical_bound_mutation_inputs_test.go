package cypher

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// Author: #1640 lane (Codex). Bare CALL weak readability must not turn an
// otherwise private CREATE/MERGE declaration into an imported declaration.
// Declared imports remain strong. Each mode runs even if another mode rejects.
func TestCanonicalBoundMutationDeclarationInputs(t *testing.T) {
	for _, sample := range []struct{ name, body string }{
		{"create_node", "CREATE (value:WeakDeclaration)"},
		{"merge_node", "MERGE (value:WeakDeclaration)"},
		{"create_relationship", "CREATE ()-[value:WeakDeclaration]->()"},
		{"merge_relationship", "MERGE ()-[value:WeakDeclaration]->()"},
		{"foreach_create", "FOREACH (item IN [1] | CREATE (value:WeakDeclaration))"},
		{"foreach_merge", "FOREACH (item IN [1] | MERGE (value:WeakDeclaration))"},
	} {
		for _, scoped := range []bool{false, true} {
			for _, input := range []string{"WITH 7 AS value ", "UNWIND [] AS value ", "UNWIND [7] AS value "} {
				for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
					t.Run(fmt.Sprintf("%s/scoped=%v/%s/%s", sample.name, scoped, input, mode), func(t *testing.T) {
						executor, ctx := newUnitExecutor(t)
						open := "CALL { "
						if scoped {
							open = "CALL (value) { "
						}
						query := mode + input + open + sample.body + " WITH 1 AS bridge RETURN 1 AS x } RETURN x"
						result, err := executor.Execute(ctx, query, nil)
						if scoped {
							require.Error(t, err)
							var status interface{ BoltErrorCode() string }
							require.True(t, errors.As(err, &status), "%v", err)
							require.Equal(t, "Neo.ClientError.Statement.SyntaxError", status.BoltErrorCode())
						} else {
							require.NoError(t, err)
							require.Equal(t, []string{"x"}, result.Columns)
							if mode == "EXPLAIN " || input == "UNWIND [] AS value " {
								require.Empty(t, result.Rows)
							} else {
								require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
							}
						}
					})
				}
			}
		}
	}
	// Weak inputs remain readable in property expressions; declaration membership
	// is a separate fact, never implemented by erasing those input names.
	for _, body := range []string{"CREATE (local:WeakDeclaration {v:value})", "MERGE (local:WeakDeclaration {v:value})", "FOREACH (item IN [1] | CREATE (local:WeakDeclaration {v:value}))"} {
		for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
			t.Run("property-read/"+mode+body, func(t *testing.T) {
				executor, ctx := newUnitExecutor(t)
				result, err := executor.Execute(ctx, mode+"WITH 7 AS value CALL { "+body+" WITH 1 AS bridge RETURN 1 AS x } RETURN x", nil)
				require.NoError(t, err)
				require.Equal(t, []string{"x"}, result.Columns)
				if mode == "EXPLAIN " {
					require.Empty(t, result.Rows)
				} else {
					require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
				}
			})
		}
	}
}
