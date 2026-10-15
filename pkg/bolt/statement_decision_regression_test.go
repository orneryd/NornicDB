package bolt

import (
	"context"
	"fmt"
	"testing"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestStatementBoltUseAndFabric(t *testing.T) {
	ctx := context.Background()
	users, _, fixture, stores := boltProductionFixture(t)
	mgr, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })
	for _, database := range []string{"other", "r2source", "r2target"} {
		require.NoError(t, mgr.CreateDatabase(database))
	}
	require.NoError(t, mgr.CreateCompositeDatabase("r2cmp", []multidb.ConstituentRef{
		{Alias: "source", DatabaseName: "r2source", Type: "local", AccessMode: "read_write"},
		{Alias: "target", DatabaseName: "r2target", Type: "local", AccessMode: "read_write"},
	}))
	require.NoError(t, mgr.CreateCompositeDatabase("r2blockedcmp", []multidb.ConstituentRef{
		{Alias: "source", DatabaseName: "nornic", Type: "local", AccessMode: "read_write"},
	}))
	source, err := mgr.GetStorage("r2source")
	require.NoError(t, err)
	_, err = cypher.NewStorageExecutor(source).Execute(ctx, "CREATE (:R2Source {v: 1})", nil)
	require.NoError(t, err)
	require.NoError(t, stores.roles.CreateRole(ctx, "r2loader"))
	require.NoError(t, stores.entitlements.Set(ctx, "r2loader", []string{"read", "write"}))
	for database, access := range map[string]auth.ResolvedAccess{
		"nornic": {Read: true}, "other": {Write: true}, "r2cmp": {Read: true}, "r2source": {Read: true}, "r2target": {Write: true},
	} {
		require.NoError(t, stores.privileges.SavePrivilege(ctx, "r2loader", database, access.Read, access.Write))
	}
	_, err = users.CreateUser("r2loader", "password123", []auth.Role{"r2loader"})
	require.NoError(t, err)
	config := fixture.config
	config.Port = 0
	config.RequireAuth = true
	server := NewWithDatabaseManager(config, &mockExecutor{}, mgr)
	server.SetResolvedAccessResolver(fixture.resolvedAccessResolver)
	server.SetDatabaseAccessModeResolver(func(roles []string) auth.DatabaseAccessMode {
		return auth.NewAllowlistDatabaseAccessMode(map[string][]string{"r2loader": {"nornic", "other", "r2cmp", "r2source", "r2target"}}, roles)
	})
	port := startBoltTestServer(t, server)
	driver, err := neo4jdriver.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4jdriver.BasicAuth("r2loader", "password123", ""))
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	fabricQuery := "CALL { USE r2cmp.source MATCH (n:R2Source) RETURN n.v AS v } CALL { USE r2cmp.target WITH v CREATE (n:R2Loaded {v: v}) RETURN n.v AS loaded } RETURN loaded"
	for _, route := range []struct {
		database, query string
		explicit        bool
	}{
		{"other", "CREATE (:R2)", false},
		{"other", "USE other CREATE (:R2)", false},
		{"nornic", "USE other CREATE (:R2)", false},
		{"nornic", ":USE other\nCREATE (:R2)", false},
		{"other", "USE other CREATE (:R2)", true},
		{"other", ":USE other\nCREATE (:R2)", true},
		{"nornic", "/* comment */ USE other CREATE (:R2)", false},
		{"nornic", "EXPLAIN USE other CREATE (:R2)", false},
		{"nornic", "PROFILE USE other CREATE (:R2)", false},
		{"nornic", "\xef\xbb\xbf USE other CREATE (:R2)", false},
		{"r2cmp", fabricQuery, false},
		{"r2cmp", fabricQuery, true},
		{"r2cmp", ":USE r2cmp\n" + fabricQuery, false},
		{"r2cmp", ":USE r2cmp\n" + fabricQuery, true},
	} {
		t.Run(fmt.Sprintf("%s/explicit=%v/%s", route.database, route.explicit, route.query), func(t *testing.T) {
			session := driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: route.database})
			defer func() { _ = session.Close(ctx) }()
			var result neo4jdriver.ResultWithContext
			var runErr error
			if route.explicit {
				tx, err := session.BeginTransaction(ctx)
				require.NoError(t, err)
				defer func() { _ = tx.Close(ctx) }()
				result, runErr = tx.Run(ctx, route.query, nil)
				if runErr == nil {
					_, runErr = result.Consume(ctx)
				}
				if runErr == nil {
					runErr = tx.Commit(ctx)
				}
			} else {
				result, runErr = session.Run(ctx, route.query, nil)
				if runErr == nil {
					_, runErr = result.Consume(ctx)
				}
			}
			t.Logf("err=%v", runErr)
			require.NoError(t, runErr)
		})
	}
	for _, prefix := range []string{"USE other ", ":USE other\n"} {
		session := driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: "nornic"})
		result, err := session.Run(ctx, prefix+"CREATE (n:SelectedEntity) RETURN n, elementId(n) AS eid", nil)
		require.NoError(t, err)
		require.True(t, result.Next(ctx))
		record := result.Record()
		require.Equal(t, record.Values[1], record.Values[0].(neo4jdriver.Node).ElementId)
		summary, err := result.Consume(ctx)
		require.NoError(t, err)
		require.Equal(t, "other", summary.Database().Name())
		require.NoError(t, session.Close(ctx))
	}
	for _, explicit := range []bool{false, true} {
		for _, query := range []string{
			"USE r2blockedcmp.source RETURN 1 AS one",
			"CALL { USE r2blockedcmp.source RETURN 1 AS one } RETURN one",
			"CALL { USE r2blockedcmp.source MATCH (n) RETURN count(n) AS c } RETURN c",
		} {
			session := driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: "nornic"})
			var result neo4jdriver.ResultWithContext
			var runErr error
			if explicit {
				tx, err := session.BeginTransaction(ctx)
				require.NoError(t, err)
				result, runErr = tx.Run(ctx, query, nil)
				if runErr == nil {
					_, runErr = result.Consume(ctx)
				}
				require.NoError(t, tx.Rollback(ctx))
			} else {
				result, runErr = session.Run(ctx, query, nil)
				if runErr == nil {
					_, runErr = result.Consume(ctx)
				}
			}
			var denied *neo4jdriver.Neo4jError
			require.ErrorAs(t, runErr, &denied, query)
			require.Equal(t, "Neo.ClientError.Security.Forbidden", denied.Code)
			require.NoError(t, session.Close(ctx))
		}
		session := driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: "nornic"})
		tx, err := session.BeginTransaction(ctx)
		require.NoError(t, err)
		_, err = tx.Run(ctx, ":USE other\nCREATE (:RejectedSelection)", nil)
		var refused *neo4jdriver.Neo4jError
		require.ErrorAs(t, err, &refused)
		require.Equal(t, "Neo.ClientError.Statement.AccessMode", refused.Code)
		require.NoError(t, tx.Rollback(ctx))
		require.NoError(t, session.Close(ctx))
	}
}
