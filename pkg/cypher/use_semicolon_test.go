package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestUseClauseSemicolon: after USE <db>, a ';' followed by another
// statement is Neo4j's SyntaxError "Expected exactly one statement per query
// but got: <n>", not a lookup of the database; a trailing ';' leaves a USE
// with no clause.
func TestUseClauseSemicolon(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
	ctx := context.Background()
	for query, message := range map[string]string{
		"USE nornic; MATCH (n) RETURN count(n) AS c": "Expected exactly one statement per query but got: 2",
		"USE nosuch; MATCH (n) RETURN count(n) AS c": "Expected exactly one statement per query but got: 2",
		"USE nornic; RETURN 1 AS x; RETURN 2 AS y":   "Expected exactly one statement per query but got: 3",
		"USE nornic;": "Query cannot conclude with USE GRAPH (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.EqualError(t, err, message, query)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
	result, err := exec.Execute(ctx, "USE nornic RETURN ';' AS x;", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{";"}}, result.Rows)
}
