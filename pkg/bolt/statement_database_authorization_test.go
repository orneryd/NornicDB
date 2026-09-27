package bolt

import (
	"context"
	"fmt"
	"testing"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestBoltEveryDatabaseAStatementSelectsIsAuthorized: over Bolt a statement
// may reach only the databases its principal may use, whichever way it
// selects one (the session's database, USE, :USE, a USE in a CALL subquery,
// a composite constituent, whose access is its composite's and its target
// database's).
func TestBoltEveryDatabaseAStatementSelectsIsAuthorized(t *testing.T) {
	mgr, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })
	for _, name := range []string{"da", "db"} {
		require.NoError(t, mgr.CreateDatabase(name))
	}
	require.NoError(t, mgr.CreateCompositeDatabase("cmp", []multidb.ConstituentRef{
		{Alias: "a", DatabaseName: "da", Type: "local", AccessMode: "read_write"},
		{Alias: "b", DatabaseName: "db", Type: "local", AccessMode: "read_write"},
	}))

	authCfg := auth.DefaultAuthConfig()
	authCfg.JWTSecret = []byte("test-secret-key-for-jwt-signing!!")
	authCfg.SecurityEnabled = true
	authenticator, err := auth.NewAuthenticator(authCfg, storage.NewMemoryEngine())
	require.NoError(t, err)
	_, err = authenticator.CreateUser("admin", "admin-password", []auth.Role{auth.RoleAdmin})
	require.NoError(t, err)
	_, err = authenticator.CreateUser("limited", "limited-password", []auth.Role{auth.RoleEditor})
	require.NoError(t, err)

	server := NewWithDatabaseManager(&Config{
		Port:            0,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
		RequireAuth:     true,
		Authenticator:   NewAuthenticatorAdapter(authenticator),
	}, &mockExecutor{}, mgr)
	allowlist := map[string][]string{string(auth.RoleEditor): {"da", "cmp"}}
	server.SetDatabaseAccessModeResolver(func(roles []string) auth.DatabaseAccessMode {
		for _, role := range roles {
			if role == string(auth.RoleAdmin) {
				return auth.FullDatabaseAccessMode
			}
		}
		return auth.NewAllowlistDatabaseAccessMode(allowlist, roles)
	})
	server.SetResolvedAccessResolver(func(roles []string, _ string) auth.ResolvedAccess {
		return auth.ResolvedAccess{Read: true, Write: true}
	})
	port := startBoltTestServer(t, server)
	ctx := context.Background()

	connect := func(user, password string) neo4jdriver.DriverWithContext {
		driver, err := neo4jdriver.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4jdriver.BasicAuth(user, password, ""))
		require.NoError(t, err)
		t.Cleanup(func() { _ = driver.Close(context.Background()) })
		return driver
	}
	run := func(driver neo4jdriver.DriverWithContext, database, statement string) (any, error) {
		session := driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: database})
		defer func() { _ = session.Close(ctx) }()
		result, err := session.Run(ctx, statement, nil)
		if err != nil {
			return nil, err
		}
		records, err := result.Collect(ctx)
		if err != nil || len(records) == 0 {
			return nil, err
		}
		return records[0].Values[0], nil
	}
	admin := connect("admin", "admin-password")
	_, err = run(admin, "db", "CREATE (:S {v: 'in-db'})")
	require.NoError(t, err)
	_, err = run(admin, "da", "CREATE (:S {v: 'in-da'})")
	require.NoError(t, err)

	limited := connect("limited", "limited-password")
	for _, request := range []struct{ db, statement string }{
		{"db", "MATCH (n:S) RETURN collect(n.v) AS vs"},
		{"da", "USE db MATCH (n:S) RETURN collect(n.v) AS vs"},
		{"da", "USE db CREATE (:S {v: 'denied'})"},
		{"da", ":USE db"},
		{"da", "CALL { USE db MATCH (n:S) RETURN collect(n.v) AS vs } RETURN vs"},
		{"cmp", "USE cmp.b MATCH (n:S) RETURN collect(n.v) AS vs"},
		{"cmp", "CALL { USE cmp.b MATCH (n:S) RETURN collect(n.v) AS vs } RETURN vs"},
		{"da", "USE cmp.b MATCH (n:S) RETURN collect(n.v) AS vs"},
		{"cmp.b", "MATCH (n:S) RETURN collect(n.v) AS vs"},
	} {
		_, err := run(limited, request.db, request.statement)
		require.Error(t, err, "%s on %s", request.statement, request.db)
		var neo4jErr *neo4jdriver.Neo4jError
		require.ErrorAs(t, err, &neo4jErr, "%s on %s", request.statement, request.db)
		require.Equal(t, "Neo.ClientError.Security.Forbidden", neo4jErr.Code, "%s on %s", request.statement, request.db)
	}
	for _, request := range []struct{ db, statement string }{
		{"da", "MATCH (n:S) RETURN collect(n.v) AS vs"},
		{"da", "USE da MATCH (n:S) RETURN collect(n.v) AS vs"},
		{"cmp", "USE cmp.a MATCH (n:S) RETURN collect(n.v) AS vs"},
		{"cmp", "CALL { USE cmp.a MATCH (n:S) RETURN collect(n.v) AS vs } RETURN vs"},
		{"da", "USE cmp.a MATCH (n:S) RETURN collect(n.v) AS vs"},
		{"cmp.a", "MATCH (n:S) RETURN collect(n.v) AS vs"},
	} {
		value, err := run(limited, request.db, request.statement)
		require.NoError(t, err, "%s on %s", request.statement, request.db)
		require.Equal(t, []any{"in-da"}, value, "%s on %s", request.statement, request.db)
	}

	value, err := run(admin, "db", "MATCH (n:S) RETURN collect(n.v) AS vs")
	require.NoError(t, err)
	require.Equal(t, []any{"in-db"}, value, "nothing was written to db")
}
