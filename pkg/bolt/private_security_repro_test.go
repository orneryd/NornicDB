package bolt

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type privateReproAccessMode struct{}

func (privateReproAccessMode) CanSeeDatabase(name string) bool {
	return name == "nornic" || name == "private_cmp"
}

func (privateReproAccessMode) CanAccessDatabase(name string) bool {
	return name == "nornic" || name == "private_cmp"
}

func TestPrivateReproBoltBareBeginPoolReuse(t *testing.T) {
	store := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "nornic")
	manager := &mockDBManager{stores: map[string]storage.Engine{"nornic": store}, defaultDB: "nornic"}
	server := NewWithDatabaseManager(&Config{Port: 0, ReadBufferSize: 8192, WriteBufferSize: 8192}, &mockExecutor{}, manager)
	t.Cleanup(func() { _ = server.Close() })
	port := startBoltTestServer(t, server)
	ctx := context.Background()
	driver, err := neo4jdriver.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4jdriver.NoAuth(), func(c *neo4jdriver.Config) {
		c.MaxConnectionPoolSize = 1
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(ctx) })
	require.NoError(t, driver.VerifyConnectivity(ctx))
	run := func(query string) {
		session := driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeWrite, DatabaseName: "nornic"})
		result, runErr := session.Run(ctx, query, nil)
		require.NoError(t, runErr, query)
		_, consumeErr := result.Consume(ctx)
		require.NoError(t, consumeErr, query)
		require.NoError(t, session.Close(ctx))
	}
	run("CREATE (:PrivateBoltControl)")
	session := driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeWrite, DatabaseName: "nornic"})
	_, err = session.Run(ctx, "BEGIN", nil)
	require.Error(t, err)
	require.NoError(t, session.Close(ctx))
	run("CREATE (:PrivateBoltTxProbe)")
	result, err := databaseScopedExecutor(t, store).Execute(ctx, "MATCH (n:PrivateBoltControl) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Rows[0][0], "ordinary auto-commit writes remain durable")
	result, err = databaseScopedExecutor(t, store).Execute(ctx, "MATCH (n:PrivateBoltTxProbe) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Rows[0][0])
}

func TestPrivateReproBoltConstituentAccess(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	manager, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.CreateDatabase("private_denied"))
	require.NoError(t, manager.CreateCompositeDatabase("private_cmp", []multidb.ConstituentRef{
		{Alias: "b", DatabaseName: "private_denied", Type: "local", AccessMode: "read_write"},
	}))
	server := NewWithDatabaseManager(&Config{Port: 0, ReadBufferSize: 8192, WriteBufferSize: 8192}, &mockExecutor{}, manager)
	server.SetDatabaseAccessModeResolver(func([]string) auth.DatabaseAccessMode { return privateReproAccessMode{} })
	t.Cleanup(func() { _ = server.Close() })
	port := startBoltTestServer(t, server)
	ctx := context.Background()
	driver, err := neo4jdriver.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4jdriver.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(ctx) })
	require.NoError(t, driver.VerifyConnectivity(ctx))
	deniedStore, err := manager.GetStorage("private_denied")
	require.NoError(t, err)
	_, err = databaseScopedExecutor(t, deniedStore).Execute(ctx, "CREATE (:PrivateSecret {v: 'hidden'})", nil)
	require.NoError(t, err)
	run := func(database, query string) (any, error) {
		session := driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeRead, DatabaseName: database})
		defer func() { _ = session.Close(ctx) }()
		result, runErr := session.Run(ctx, query, nil)
		if runErr != nil {
			return nil, runErr
		}
		if result.Next(ctx) {
			return result.Record().Values[0], nil
		}
		return nil, result.Err()
	}
	_, err = run("private_denied", "MATCH (n:PrivateSecret) RETURN n.v")
	require.Error(t, err, "direct constituent access must be denied")
	require.Contains(t, err.Error(), "not allowed")
	value, err := run("private_cmp", "CALL { USE private_cmp.b MATCH (n:PrivateSecret) RETURN n.v AS v } RETURN v")
	require.Error(t, err)
	require.Nil(t, value)
	_, err = run("private_cmp", "USE private_cmp.b CREATE (:PrivateSecret {v: 'written'}) RETURN 1")
	require.Error(t, err)
	result, err := databaseScopedExecutor(t, deniedStore).Execute(ctx, "MATCH (n:PrivateSecret) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Rows[0][0])
}
