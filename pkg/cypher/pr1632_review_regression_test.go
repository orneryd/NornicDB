package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPR1632ReviewAdmission(t *testing.T) {
	for _, normalize := range []string{"true", "false"} {
		t.Run(normalize, func(t *testing.T) {
			t.Setenv("NORNICDB_CYPHER_QUERY_NORMALIZATION", normalize)
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "review"))
			for _, query := range []string{
				"UNWIND [] AS r MATCH (n) WITH n, reverse(CASE WHEN true THEN null ELSE [1] END) AS v RETURN abs(v) AS a",
				"UNWIND [] AS r MATCH (n) WITH n, reverse(CASE WHEN true THEN null ELSE 1 END) AS v RETURN abs(v) AS a",
				"UNWIND [] AS r MATCH (n) WITH n, reverse(CASE WHEN true THEN null ELSE [1] END) AS v RETURN -v AS a",
				"WITH 1 AS x CALL (*) { RETURN DISTINCT /* c */ * } RETURN *",
			} {
				t.Run(query, func(t *testing.T) {
					_, err := exec.Execute(context.Background(), query, nil)
					require.Error(t, err, query)
					require.Equal(t, "Neo.ClientError.Statement.SyntaxError", errorStatusCode(err), query)
				})
			}
		})
	}
}

func TestPR1632CommentedReturnStarFact(t *testing.T) {
	for _, clause := range []string{"RETURN *", "RETURN DISTINCT /* c */ *", "RETURN /* c */ *", "RETURN DISTINCT * /* c */"} {
		require.True(t, returnsStar(clause), clause)
	}
	require.False(t, returnsStar("RETURN 2 * 3 AS product"))
}
