package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type administrationReviewManager struct {
	DatabaseManagerInterface
	stores map[string]storage.Engine
}

func (m administrationReviewManager) GetStorageForUse(name, _ string) (interface{}, error) {
	return m.stores[name], nil
}
func (m administrationReviewManager) ResolveDatabase(name string) (string, error) { return name, nil }
func (m administrationReviewManager) IsCompositeDatabase(string) bool             { return false }
func (m administrationReviewManager) Exists(name string) bool                     { return m.stores[name] != nil }

func administrationReviewExecutor(t *testing.T, database string) (*StorageExecutor, storage.Engine) {
	t.Helper()
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	system := storage.NewNamespacedEngine(base, "system")
	manager := administrationReviewManager{stores: map[string]storage.Engine{
		"system": system, "nornic": storage.NewNamespacedEngine(base, "nornic"),
	}}
	exec := NewStorageExecutor(manager.stores[database])
	exec.SetDatabaseManager(manager)
	return exec, system
}

func TestRejectedSystemStatementFailsTransaction(t *testing.T) {
	ctx := context.Background()
	exec, system := administrationReviewExecutor(t, "system")
	_, err := exec.handleBegin()
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO viewer", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Review)", nil)
	require.Error(t, err)
	_, err = exec.handleCommit()
	require.Error(t, err)
	grants, err := auth.NewProcedurePrivilegeStore(system).List(ctx)
	require.NoError(t, err)
	require.Empty(t, grants)
}

func TestSystemStatementPolicyAllRoutes(t *testing.T) {
	for _, route := range []string{"session", "USE", "procedure body"} {
		t.Run(route, func(t *testing.T) {
			ctx := context.Background()
			exec, _ := administrationReviewExecutor(t, "system")
			query := "CREATE INDEX review_idx FOR (n:Review) ON (n.x)"
			var err error
			if route == "procedure body" {
				_, err = exec.executeUserStatement(ctx, "USE system "+query, nil)
			} else if route == "USE" {
				_, err = exec.Execute(ctx, "USE system "+query, nil)
			} else {
				_, err = exec.Execute(ctx, query, nil)
			}
			requireStatusCode(t, err, "Neo.ClientError.Statement.SemanticError")
		})
	}
}

func TestSystemStatementPolicyExecutionModes(t *testing.T) {
	for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
		for _, statement := range []string{
			"CREATE INDEX review_idx FOR (n:Review) ON (n.x)",
			"DROP INDEX review_idx IF EXISTS",
			"MATCH (n:Review) RETURN count(n)",
		} {
			requireStatusCode(t, systemDatabaseStatementError(mode+statement), "Neo.ClientError.Statement.SemanticError")
		}
	}
}

func TestAdministrationTransactionRoutesToSystem(t *testing.T) {
	ctx := context.Background()
	exec, system := administrationReviewExecutor(t, "nornic")
	_, err := exec.handleBegin()
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO viewer", nil)
	require.NoError(t, err)
	require.False(t, allowsBoosted(t, system, "viewer", "db.cdc.query"))
	result, err := exec.Execute(ctx, "SHOW PRIVILEGES", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	_, err = exec.handleCommit()
	require.NoError(t, err)
	require.True(t, allowsBoosted(t, system, "viewer", "db.cdc.query"))
}

func TestAdministrationAndDataStatementsDoNotMix(t *testing.T) {
	for _, database := range []string{"system", "nornic"} {
		t.Run(database, func(t *testing.T) {
			ctx := context.Background()
			exec, system := administrationReviewExecutor(t, database)
			_, err := exec.handleBegin()
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "GRANT EXECUTE PROCEDURE db.a ON DBMS TO viewer", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "CALL dbms.components()", nil)
			requireStatusCode(t, err, "Neo.ClientError.Transaction.ForbiddenDueToTransactionType")
			_, err = exec.handleCommit()
			require.Error(t, err)
			grants, err := auth.NewProcedurePrivilegeStore(system).List(ctx)
			require.NoError(t, err)
			require.Empty(t, grants)
		})
	}
}
