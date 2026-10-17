package bolt

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A RUN or BEGIN naming a database the principal may not use is refused by
// the one admission decision (cypher.AdmitGraph) before anything else: an
// existing database and a missing one get the same Forbidden error, and no
// database connection is admitted for either. A database the principal may
// use still reports a missing name as not found.
func TestBoltAdmissionBeforeResolution(t *testing.T) {
	ctx := context.Background()
	users, _, fixture, stores := boltProductionFixture(t)
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })
	require.NoError(t, mgr.CreateDatabase("tenantsecret"))
	config := fixture.config
	config.Port = 0
	config.RequireAuth = true
	server := NewWithDatabaseManager(config, &mockExecutor{}, mgr)
	server.SetResolvedAccessResolver(fixture.resolvedAccessResolver)
	server.SetDatabaseAccessModeResolver(func(roles []string) auth.DatabaseAccessMode {
		return auth.NewAllowlistDatabaseAccessMode(map[string][]string{"limited": {"nornic"}}, roles)
	})
	var mu sync.Mutex
	admitted := map[string]int{}
	server.SetDatabaseConnectionAdmission(nil, func(database string) error {
		mu.Lock()
		defer mu.Unlock()
		admitted[database]++
		return nil
	}, func(string) {})
	port := startBoltTestServer(t, server)
	require.NoError(t, stores.roles.CreateRole(ctx, "limited"))
	require.NoError(t, stores.entitlements.Set(ctx, "limited", []string{"read", "write"}))
	_, err = users.CreateUser("limited", "password123", []auth.Role{"limited"})
	require.NoError(t, err)
	driver, err := neo4jdriver.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4jdriver.BasicAuth("limited", "password123", ""))
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })

	failure := func(database string, explicit bool) (code, message string) {
		t.Helper()
		session := driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: database})
		defer func() { _ = session.Close(ctx) }()
		var err error
		if explicit {
			var tx neo4jdriver.ExplicitTransaction
			tx, err = session.BeginTransaction(ctx)
			if err == nil {
				var result neo4jdriver.ResultWithContext
				if result, err = tx.Run(ctx, "RETURN 1", nil); err == nil {
					_, err = result.Consume(ctx)
				}
				_ = tx.Close(ctx)
			}
		} else {
			var result neo4jdriver.ResultWithContext
			if result, err = session.Run(ctx, "RETURN 1", nil); err == nil {
				_, err = result.Consume(ctx)
			}
		}
		var neoErr *neo4jdriver.Neo4jError
		if err == nil || !errors.As(err, &neoErr) {
			return "", fmt.Sprint(err)
		}
		return neoErr.Code, neoErr.Msg
	}
	for _, explicit := range []bool{false, true} {
		existingCode, existingMessage := failure("tenantsecret", explicit)
		missingCode, missingMessage := failure("tenantmissing", explicit)
		require.Equal(t, "Neo.ClientError.Security.Forbidden", existingCode, "explicit=%v", explicit)
		require.Equal(t, existingCode, missingCode, "explicit=%v", explicit)
		// Neo4j 5.26's text names the principal, not the database, so an existing
		// and a missing database read the same.
		require.Equal(t, "Database access is not allowed for user 'limited' with roles [limited].", existingMessage, "explicit=%v", explicit)
		require.Equal(t, existingMessage, missingMessage, "explicit=%v", explicit)
	}
	mu.Lock()
	require.Zero(t, admitted["tenantsecret"], "no connection admitted for a refused database")
	require.Zero(t, admitted["tenantmissing"])
	mu.Unlock()

	code, _ := failure("nornic", false)
	require.Empty(t, code, "the allowed database runs")
}

type admissionConfiguredExecutor struct {
	mockExecutor
	base       *cypher.StorageExecutor
	configured int
}

func (e *admissionConfiguredExecutor) BaseCypherExecutor() *cypher.StorageExecutor { return e.base }
func (e *admissionConfiguredExecutor) ConfigureDatabaseExecutor(_ *cypher.StorageExecutor, _ string, _ storage.Engine) {
	e.configured++
}

func TestBoltStatementAdmissionAndBeginServiceTiming(t *testing.T) {
	f := newImpersonationFixture(t)
	f.grant("svc", auth.ImpersonateAllUsers, false)
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	manager := &mockDBManager{defaultDB: "nornic", stores: map[string]storage.Engine{"nornic": storage.NewNamespacedEngine(base, "nornic")}}
	provider := &admissionConfiguredExecutor{base: f.executor}
	f.server.dbManager = manager
	f.server.executor = provider
	extra := imp("alice936")
	_, code := f.run("CREATE (:DeniedBeforeResolution)", extra)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code)
	require.Zero(t, manager.getCalls)
	require.Zero(t, provider.configured)
	require.Empty(t, f.server.executors)
	// BEGIN admits a graph and opens its transaction; it does not admit a
	// statement or initialize query/search/embedding services.
	require.Empty(t, f.begin(extra))
	require.Equal(t, 1, manager.getCalls)
	require.Zero(t, provider.configured)
	require.Empty(t, f.server.executors)
	_, code = f.run("CREATE (:DeniedInTransaction)", nil)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code)
	require.Zero(t, provider.configured)
	require.Empty(t, f.server.executors)
	_, code = f.run("MATCH (n) RETURN count(n)", extra)
	require.Empty(t, code)
	require.Equal(t, 1, provider.configured)
}
