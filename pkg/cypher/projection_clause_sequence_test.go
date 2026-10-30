package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func requireSyntaxError(t *testing.T, err error, query string) {
	t.Helper()
	require.Error(t, err, query)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
}

func TestMalformedProjectionAliasesRejectedLikeNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "alias_reject"))
	ctx := context.Background()
	for _, query := range []string{
		"RETURN 1 AS x RETURN 2 AS y",
		"RETURN 1 AS x y",
		"RETURN 1 AS x\r\nRETURN 2 AS y",
	} {
		_, err := exec.Execute(ctx, query, nil)
		requireSyntaxError(t, err, query)
	}
	for _, query := range []string{
		"RETURN 1 AS x",
		"RETURN 1 AS `x y`",
		"RETURN 1 AS x, 2 AS y",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
}

func TestDanglingComparisonBeforeReturnRejectedLikeNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "where_reject"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "MATCH (n) WHERE n.x = RETURN n", nil)
	requireSyntaxError(t, err, "MATCH (n) WHERE n.x = RETURN n")
}

func TestEmptyProjectionAndDanglingModifiersRejectedLikeNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "modifier_reject"))
	ctx := context.Background()
	for _, query := range []string{
		"RETURN",
		"RETURN 1 ORDER BY",
		"RETURN 1 SKIP",
		"RETURN 1 LIMIT",
		"WITH 1 AS x RETURN",
		"MATCH (n) RETURN n ORDER BY",
		"MATCH (n) RETURN n SKIP",
		"RETURN DISTINCT",
		"WITH 1 AS",
		"MATCH (n) WHERE = 1 RETURN n",
		"MATCH () WHERE RETURN n",
		"WITH 1 AS x WHERE RETURN x",
		"MATCH (n) WHERE n.x IN RETURN n",
		"CALL { RETURN 1 AS x } RETURN",
	} {
		_, err := exec.Execute(ctx, query, nil)
		requireSyntaxError(t, err, query)
	}
	for _, query := range []string{
		"RETURN 1 ORDER BY 1",
		"RETURN 1 SKIP 0",
		"RETURN 1 LIMIT 1",
		"WITH 1 AS x RETURN *",
		"WITH 1 AS x RETURN x",
		"MATCH (n) WHERE n.x = 1 RETURN n",
		"MATCH (n) WHERE EXISTS { (m)--(n) } RETURN n",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
}
