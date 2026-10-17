package bolt

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBoltDropRoleUnsupportedChangesNothing(t *testing.T) {
	ctx := context.Background()
	inner := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	system := storage.NewNamespacedEngine(inner, "system")
	roles := auth.NewRoleStore(system)
	require.NoError(t, roles.CreateRole(ctx, "ops"))
	grants := auth.NewProcedurePrivilegeStore(system)
	require.NoError(t, grants.Grant(ctx, "ops", "db.a", true, false))
	exec := cypher.NewStorageExecutor(system)
	server := &Server{config: DefaultConfig(), sessions: map[string]*Session{}}
	admin := &BoltAuthResult{Authenticated: true, Username: "admin", Roles: []string{"admin"},
		Permissions: []string{string(auth.PermRead), string(auth.PermWrite), string(auth.PermSchema), string(auth.PermAdmin)}}
	// This server has no role owner (SetRoleAdministration), so every role
	// command is unsupported, as in Neo4j Community, and changes nothing.
	for query, expected := range map[string]string{
		"CREATE ROLE new":             "Neo.ClientError.Statement.UnsupportedAdministrationCommand",
		"DROP ROLE ops":               "Neo.ClientError.Statement.UnsupportedAdministrationCommand",
		"RENAME ROLE ops TO new":      "Neo.ClientError.Statement.UnsupportedAdministrationCommand",
		"GRANT ROLE ops TO reader":    "Neo.ClientError.Statement.UnsupportedAdministrationCommand",
		"REVOKE ROLE ops FROM reader": "Neo.ClientError.Statement.UnsupportedAdministrationCommand",
	} {
		executed, code := runOnBoltSession(t, server, admin, query, func(ctx context.Context, query string, params map[string]any) error {
			_, err := exec.Execute(ctx, query, params)
			return err
		})
		require.True(t, executed)
		require.Equal(t, expected, code, query)
	}
	_, err := system.GetNode("role:ops")
	require.NoError(t, err)
	allowed, err := grants.AllowsBoosted([]string{"ops"}, "db.a")
	require.NoError(t, err)
	require.True(t, allowed)
}

func TestBoltRejectedStatementDiscardsAdministrationTransaction(t *testing.T) {
	ctx := context.Background()
	inner := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	system := storage.NewNamespacedEngine(inner, "system")
	exec := cypher.NewStorageExecutor(system)
	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	server := &Server{config: DefaultConfig(), sessions: map[string]*Session{}}
	admin := &BoltAuthResult{Authenticated: true, Username: "admin", Roles: []string{"admin"},
		Permissions: []string{string(auth.PermRead), string(auth.PermWrite), string(auth.PermSchema), string(auth.PermAdmin)}}
	run := func(query string) string {
		_, code := runOnBoltSession(t, server, admin, query, func(ctx context.Context, query string, params map[string]any) error {
			_, err := exec.Execute(ctx, query, params)
			return err
		})
		return code
	}
	require.Empty(t, run("GRANT EXECUTE BOOSTED PROCEDURE db.a ON DBMS TO viewer"))
	require.Equal(t, "Neo.ClientError.Transaction.ForbiddenDueToTransactionType", run("CALL dbms.components()"))
	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.Error(t, err)
	grants, err := auth.NewProcedurePrivilegeStore(system).List(ctx)
	require.NoError(t, err)
	require.Empty(t, grants)
}
