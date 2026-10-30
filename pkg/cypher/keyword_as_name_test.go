package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestClauseKeywordsAsNames: a clause keyword used as a name (an alias, a
// variable, a property or map key, a label) is that name, as Neo4j reads it
// (#740), and the clauses around it still split where they are. Results are
// Neo4j 5.26.30's.
func TestClauseKeywordsAsNames(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "kwname"))
	ctx := context.Background()
	for _, keyword := range []string{"set", "match", "return", "with", "delete", "create", "merge", "remove", "unwind", "union", "limit", "skip", "foreach", "order", "SET", "Limit"} {
		result, err := exec.Execute(ctx, "RETURN 1 AS "+keyword, nil)
		require.NoError(t, err, keyword)
		require.Equal(t, []string{keyword}, result.Columns, keyword)
		require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows, keyword)
	}
	for statement, want := range map[string][][]interface{}{
		"WITH 1 AS set RETURN set + 1 AS v":                                     {{int64(2)}},
		"UNWIND [1] AS set RETURN set AS v":                                     {{int64(1)}},
		"WITH 1 AS limit RETURN limit AS v":                                     {{int64(1)}},
		"WITH 1 AS skip RETURN skip + 1 AS v":                                   {{int64(2)}},
		"UNWIND [2] AS delete RETURN delete AS v":                               {{int64(2)}},
		"WITH 1 AS merge RETURN merge AS v":                                     {{int64(1)}},
		"WITH {set: 1} AS m RETURN m.set AS v":                                  {{int64(1)}},
		"RETURN [x IN [1] | x] AS return":                                       {{[]interface{}{int64(1)}}},
		"UNWIND [3, 1, 2] AS skip RETURN skip ORDER BY skip SKIP 1 LIMIT 1":     {{int64(2)}},
		"WITH 2 AS limit UNWIND [1, 2, 3] AS x WITH x WHERE x < limit RETURN x": {{int64(1)}},
	} {
		result, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
		require.Equal(t, want, result.Rows, statement)
	}

	// The clauses themselves still split where they are.
	for _, statement := range []string{
		"CREATE (:KW {id: 1})",
		"MATCH (n:KW) FOREACH (x IN [1] | SET n.f = x)",
		"MERGE (n:KW {id: 1}) ON CREATE SET n.c = true ON MATCH SET n.m = true",
		"MATCH (n:KW) WITH n SET n.w = 1",
		"OPTIONAL MATCH (n:KW) RETURN DISTINCT n.id AS id ORDER BY id SKIP 0 LIMIT 5",
		"MATCH (n:KW) WHERE n.id = 1 RETURN n.f AS f, n.m AS m, n.w AS w",
		"MATCH (n:KW) WHERE 'abc' STARTS WITH 'a' RETURN count(n) AS c",
		"MATCH (n:KW) DETACH DELETE n",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	result, err := exec.Execute(ctx, "MATCH (n:KW) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
}
