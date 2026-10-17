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

// Use a real executor and the production privilege resolver: a RUN-only
// permission probe would miss a different decision during USE routing.
func TestBoltDatabaseSelectionUsesStatementPermissions(t *testing.T) {
	ctx := context.Background()
	users, _, fixture, stores := boltProductionFixture(t)
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })
	config := fixture.config
	config.Port = 0
	config.RequireAuth = true
	server := NewWithDatabaseManager(config, &mockExecutor{}, mgr)
	server.SetResolvedAccessResolver(fixture.resolvedAccessResolver)
	server.SetDatabaseAccessModeResolver(func(roles []string) auth.DatabaseAccessMode {
		return auth.NewAllowlistDatabaseAccessMode(map[string][]string{
			"loader": {"nornic"}, "observer": {"nornic"}, "blocked": {"nornic"},
		}, roles)
	})
	port := startBoltTestServer(t, server)
	for _, access := range []struct {
		role        string
		read, write bool
	}{
		{"loader", false, true},
		{"observer", true, false},
		{"blocked", false, false},
	} {
		require.NoError(t, stores.roles.CreateRole(ctx, access.role))
		require.NoError(t, stores.entitlements.Set(ctx, access.role, []string{"read", "write"}))
		require.NoError(t, stores.privileges.SavePrivilege(ctx, access.role, "nornic", access.read, access.write))
		_, err := users.CreateUser(access.role, "password123", []auth.Role{auth.Role(access.role)})
		require.NoError(t, err)
		driver, err := neo4jdriver.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4jdriver.BasicAuth(access.role, "password123", ""))
		require.NoError(t, err)
		t.Cleanup(func() { _ = driver.Close(context.Background()) })
		for _, explicit := range []bool{false, true} {
			for _, prefix := range []string{"", "USE nornic ", ":USE nornic\n"} {
				t.Run(fmt.Sprintf("%s/explicit=%v/%s", access.role, explicit, prefix), func(t *testing.T) {
					session := driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: "nornic"})
					defer func() { _ = session.Close(ctx) }()
					var tx neo4jdriver.ExplicitTransaction
					if explicit {
						var err error
						tx, err = session.BeginTransaction(ctx)
						require.NoError(t, err)
						defer func() { _ = tx.Close(ctx) }()
					}
					var result neo4jdriver.ResultWithContext
					var err error
					if tx != nil {
						result, err = tx.Run(ctx, prefix+"CREATE (:Loaded)", nil)
					} else {
						result, err = session.Run(ctx, prefix+"CREATE (:Loaded)", nil)
					}
					if err == nil {
						_, err = result.Consume(ctx)
					}
					if access.write {
						require.NoError(t, err)
						if tx != nil {
							require.NoError(t, tx.Commit(ctx))
						}
					} else {
						var denied *neo4jdriver.Neo4jError
						require.ErrorAs(t, err, &denied)
						require.Equal(t, "Neo.ClientError.Security.Forbidden", denied.Code)
						require.Equal(t, fmt.Sprintf("Write operations are not allowed for user '%s' with roles [%s].", access.role, access.role), denied.Msg)
					}
				})
			}
		}
	}
	nornic, err := mgr.GetStorage("nornic")
	require.NoError(t, err)
	nodes, err := nornic.GetNodesByLabel("Loaded")
	require.NoError(t, err)
	require.Len(t, nodes, 6, "only the write-only role's statements committed")
}
