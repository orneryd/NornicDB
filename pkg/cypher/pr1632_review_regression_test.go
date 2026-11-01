// Author of retained-input changes: Codex R17 implementation agent, 29 October 2026.
package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/internal/statementfactstest"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPR1632ReviewAdmission(t *testing.T) {
	for _, normalize := range []string{"true", "false"} {
		t.Run(normalize, func(t *testing.T) {
			t.Setenv("NORNICDB_CYPHER_QUERY_NORMALIZATION", normalize)
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "review"))

			for _, sample := range statementfactstest.ReviewAdmissionCases() {
				for _, explicit := range []bool{false, true} {
					for _, prefix := range []string{"", "EXPLAIN ", "PROFILE "} {
						t.Run(fmt.Sprintf("%s/explicit=%v/mode=%s", sample.Name, explicit, prefix), func(t *testing.T) {
							ctx := context.Background()
							if explicit {
								_, err := exec.Execute(ctx, "BEGIN", nil)
								require.NoError(t, err)
								defer exec.Execute(ctx, "ROLLBACK", nil)
							}
							result, err := exec.Execute(ctx, prefix+sample.Query, nil)
							if sample.Code == "" {
								require.NoError(t, err, sample.Query)
								require.Equal(t, sample.Columns, result.Columns, sample.Query)
								require.Empty(t, result.Rows, sample.Query)
							} else {
								require.Error(t, err, sample.Query)
								require.Equal(t, sample.Code, errorStatusCode(err), sample.Query)
							}
						})
					}
				}
			}
		})
	}
}

func TestPR1632CommentedReturnStarFact(t *testing.T) {
	for _, clause := range []string{"RETURN *", "RETURN DISTINCT /* c */ *", "RETURN /* c */ *", "RETURN DISTINCT * /* c */"} {
		require.True(t, returnsStar(retainedClauseForTest(t.Context(), clause)), clause)
	}
	require.False(t, returnsStar(retainedClauseForTest(t.Context(), "RETURN 2 * 3 AS product")))
}
