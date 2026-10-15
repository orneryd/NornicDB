package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestStatementGraphFreeWrites(t *testing.T) {
	for _, query := range []string{
		"CREATE (:R2 {n: 1})",
		"CALL { CREATE (:R2 {n: 2}) } RETURN 1 AS one",
		"UNWIND [x IN [1, 2] WHERE x > 0 | x] AS x CREATE (:R2 {n: x})",
		"WITH 1 AS x WHERE x > 0 CREATE (:R2 {n: x})",
		"CREATE (:R2 {xs: [(1)]})",
		"CREATE (:R2 {text: 'shortestPath'})",
		"FOREACH (x IN [1, 2] | CREATE (:R2 {n: x}))",
	} {
		t.Run(query, func(t *testing.T) {
			exec, _ := nestedUseRouterExecutor(t, "nornic")
			requirements := QueryPermissionRequirements(query)
			_, unrestricted := exec.Execute(context.Background(), query, nil)
			ctx := WithDatabasePermissionResolver(context.Background(), "nornic", func(_, permission string) bool { return permission == "write" })
			result, restricted := exec.Execute(ctx, query, nil)
			t.Logf("requirements=%+v unrestricted=%v writeOnly=%v result=%v", requirements, unrestricted, restricted, result)
			require.NoError(t, unrestricted)
			require.False(t, requirements.Read)
			require.NoError(t, restricted, "a graph-free write should require write alone")
		})
	}
}

func TestStatementParameterizedCustomProcedureCache(t *testing.T) {
	t.Cleanup(ClearUserProcedures)
	ctx := context.Background()
	base := newTestMemoryEngine(t)
	caller := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(base, "vault"), 100, time.Minute)
	installer := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(base, "nornic"), 100, time.Minute)
	install := func(body string) {
		_, err := installer.Execute(ctx, "CALL apoc.custom.installProcedure('r2cache() :: (c :: INTEGER)', '"+body+"', 'nornic', 'read')", nil)
		require.NoError(t, err)
	}
	query := "CALL apoc.cypher.run($statement, {}) YIELD value RETURN value.c AS c"
	params := map[string]interface{}{"statement": "CALL custom.r2cache() YIELD c RETURN c"}
	install("RETURN 1 AS c")
	before, err := caller.Execute(ctx, query, params)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, before.Rows)
	install("RETURN 2 AS c")
	after, err := caller.Execute(ctx, query, params)
	require.NoError(t, err)
	t.Logf("before=%v after=%v outerCacheable=%v", before.Rows, after.Rows, isCacheableReadQuery(query))
	require.Equal(t, [][]interface{}{{int64(2)}}, after.Rows)
	_, err = installer.Execute(ctx, "CALL apoc.custom.dropProcedure('r2cache')", nil)
	require.NoError(t, err)
	_, err = caller.Execute(ctx, query, params)
	require.Error(t, err)
}

func TestStatementSystemLiteralTails(t *testing.T) {
	for _, query := range []string{
		"SHOW PROCEDURES YIELD name RETURN name, 'shortestPath' AS hint LIMIT 1",
		"SHOW PROCEDURES YIELD name RETURN name, [(1)] AS xs LIMIT 1",
	} {
		t.Run(query, func(t *testing.T) {
			for _, prefix := range []string{"", "USE system ", ":USE system\n"} {
				exec, _ := nestedUseRouterExecutor(t, "system")
				result, err := exec.Execute(context.Background(), prefix+query, nil)
				require.NoError(t, err)
				require.Len(t, result.Rows, 1)
				if result.Columns[1] == "hint" {
					require.Equal(t, "shortestPath", result.Rows[0][1])
				} else {
					require.Equal(t, []interface{}{int64(1)}, result.Rows[0][1])
				}
			}
		})
	}
}
