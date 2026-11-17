package cypher

import (
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRequestedReadScopeChecksResolvedStatements(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "requested_read"))
		ctx := WithReadOnlyStatements(t.Context())
		for _, query := range []string{
			"RETURN 1 AS v",
			"EXPLAIN CREATE (:ReadScopeExplained)",
			"CALL apoc.cypher.run('RETURN 1 AS v',{}) YIELD value RETURN value",
			"CALL apoc.cypher.run('CALL apoc.cypher.run(\"RETURN 1 AS v\",{}) YIELD value RETURN value',{}) YIELD value RETURN value",
		} {
			_, err := e.Execute(ctx, query, nil)
			require.NoError(t, err, query)
		}
		for _, query := range []string{
			"CREATE (:ReadScopeRefused)",
			"CREATE INDEX read_scope_idx FOR (n:ReadScopeRefused) ON (n.id)",
			"CALL apoc.cypher.run('CREATE (:ReadScopeRefused) RETURN 1 AS v',{}) YIELD value RETURN value",
			"CALL apoc.cypher.run($body,{}) YIELD value RETURN value",
		} {
			_, err := e.Execute(ctx, query, map[string]any{"body": "CREATE (:ReadScopeRefused) RETURN 1 AS v"})
			require.Error(t, err, query)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.AccessMode", code, query)
		}
		_, missing := e.Execute(ctx, "CALL db.notAProcedure()", nil)
		require.Error(t, missing)
		code, _ := nornicerrors.Neo4jStatus(missing)
		require.Equal(t, "Neo.ClientError.Procedure.ProcedureNotFound", code)

		result, err := e.Execute(t.Context(), "MATCH (n:ReadScopeRefused) RETURN count(n) AS c", nil)
		require.NoError(t, err)
		require.Equal(t, [][]any{{int64(0)}}, result.Rows)
		// A read restriction on one request never changes a later caller's rights.
		_, err = e.Execute(t.Context(), "CREATE (:ReadScopeRefused)", nil)
		require.NoError(t, err)
	})
}
