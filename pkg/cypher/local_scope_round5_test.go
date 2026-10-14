package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestLocalScopeRound5: the review's statements (#1227, round 5), on each
// parser. Expected values recorded on Neo4j 5.26.30.
func TestLocalScopeRound5(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "local_scope_round5"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:N {id:'a'})-[:R]->(:N {id:'b'})-[:R]->(:N {id:'c'})", nil)
	require.NoError(t, err)
	rows := func(t *testing.T, query string) [][]interface{} {
		t.Helper()
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return result.Rows
	}
	rejects := func(t *testing.T, query, message string) {
		t.Helper()
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
		require.Contains(t, err.Error(), message, query)
	}

	t.Run("#5 a CALL (x) body sees its imports after a WITH, and may not declare them again", func(t *testing.T) {
		require.Equal(t, [][]interface{}{{int64(7)}}, rows(t, "WITH 7 AS x CALL (x) { WITH 1 AS y RETURN x AS v } RETURN v"))
		require.Equal(t, [][]interface{}{{int64(7)}}, rows(t, "WITH 7 AS x CALL (x) { WITH 1 AS y RETURN x AS v UNION WITH 2 AS y RETURN x AS v } RETURN v"))
		require.Equal(t, [][]interface{}{{int64(7)}}, rows(t, "WITH 7 AS x CALL (x) { WITH x, 1 AS y WITH y RETURN x AS v } RETURN v"))
		require.Equal(t, [][]interface{}{{int64(8)}}, rows(t, "WITH 7 AS x, 8 AS z CALL (x) { WITH 1 AS z RETURN x + z AS v } RETURN v"))
		require.Equal(t, [][]interface{}{{int64(7)}}, rows(t, "WITH 7 AS x CALL (x) { WITH x AS x RETURN x AS v } RETURN v"))
		rejects(t, "WITH 7 AS x CALL (x) { WITH 1 AS x RETURN x AS v } RETURN v", "is shadowing an imported variable")
		rejects(t, "WITH 7 AS x CALL (x) { UNWIND [1] AS x RETURN x AS v } RETURN v", "is shadowing an imported variable")
		rejects(t, "WITH 7 AS x CALL (x) { WITH 1 AS y RETURN x AS v UNION WITH 1 AS x RETURN x AS v } RETURN v", "is shadowing an imported variable")
		// An expression subquery's outer variable is protected the same
		// way, by the same walk.
		rejects(t, "WITH 7 AS x RETURN EXISTS { WITH 1 AS x RETURN x } AS v", "already declared in outer scope")
		rejects(t, "WITH 7 AS x RETURN COUNT { UNWIND [1] AS x RETURN x } AS v", "already declared in outer scope")
		require.Equal(t, [][]interface{}{{[]interface{}{int64(7)}}}, rows(t, "WITH 7 AS x RETURN COLLECT { WITH x AS x RETURN x } AS v"))
	})
}
