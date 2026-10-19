package bolt

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBoltWriteProcedureStatementRollback(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("u5 wire rejected after write")
	require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{
		Name: "u5wire.writefail", Signature: "u5wire.writefail() :: (value :: INTEGER)", Mode: cypher.ProcedureModeWrite,
		Returns: []cypher.ProcedureColumn{{Name: "value", Type: "INTEGER"}},
	}, func(ctx context.Context, exec *cypher.StorageExecutor, _ string, _ []interface{}) (*cypher.ExecuteResult, error) {
		_, err := exec.Execute(ctx, "CREATE (:U5WireWrite) RETURN 1 AS value", nil)
		if err != nil { return nil, err }
		return nil, failure
	}))
	require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{
		Name: "u5wire.fail", Signature: "u5wire.fail() :: (rejected :: INTEGER)", Mode: cypher.ProcedureModeRead,
		Returns: []cypher.ProcedureColumn{{Name: "rejected", Type: "INTEGER"}},
	}, func(context.Context, *cypher.StorageExecutor, string, []interface{}) (*cypher.ExecuteResult, error) { return nil, failure }))
	base, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "u5_wire")
	_, port := startBoltIntegrationServerWithExplicitTx(t, store)
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(ctx) })
	for _, query := range []string{
		"CALL u5wire.writefail() YIELD value RETURN value",
		"CALL apoc.cypher.doIt($statement,{}) YIELD value CALL u5wire.fail() YIELD rejected RETURN value",
	} {
		t.Run(query, func(t *testing.T) {
			session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "u5_wire", AccessMode: neo4j.AccessModeWrite})
			defer session.Close(ctx)
			result, err := session.Run(ctx, query, map[string]interface{}{"statement": "CREATE (:U5WireWrite) RETURN 1 AS value"})
			if err == nil { _, err = result.Consume(ctx) }
			require.ErrorContains(t, err, failure.Error())
			nodes, err := store.GetNodesByLabel("U5WireWrite")
			require.NoError(t, err)
			require.Empty(t, nodes, "Bolt failure must leave no committed procedure writes")
		})
	}
}
