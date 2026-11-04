package bolt

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// hostExecutorPolicyExecutor stands in for cmd/nornicdb's DBQueryExecutor on
// the paths a host factory replaces: ConfigureDatabaseExecutor is the legacy
// per-protocol configurator, and BaseCypherExecutor the base executor Bolt
// inherited embedder/inference wiring from. Both are recorded so a test can
// prove which wiring owner ran.
type hostExecutorPolicyExecutor struct {
	baseExec   *cypher.StorageExecutor
	configured []string
}

func (e *hostExecutorPolicyExecutor) Execute(context.Context, string, map[string]any) (*QueryResult, error) {
	return &QueryResult{}, nil
}

func (e *hostExecutorPolicyExecutor) ConfigureDatabaseExecutor(_ *cypher.StorageExecutor, dbName string, _ storage.Engine) {
	e.configured = append(e.configured, dbName)
}

func (e *hostExecutorPolicyExecutor) BaseCypherExecutor() *cypher.StorageExecutor {
	return e.baseExec
}

func newPolicyTestSession(cfg *Config, manager DatabaseManagerInterface, forwardedAuthHeader string) *Session {
	return &Session{
		server:              &Server{config: cfg, dbManager: manager, executors: make(map[string]QueryExecutor)},
		forwardedAuthHeader: forwardedAuthHeader,
	}
}

func autoCommitScopedExecutor(t *testing.T, session *Session, dbName string) *cypher.StorageExecutor {
	t.Helper()
	executor, err := session.getExecutorForDatabase(dbName)
	require.NoError(t, err)
	adapter, ok := executor.(*boltQueryExecutorAdapter)
	require.True(t, ok, "auto-commit executor adapter type")
	return adapter.executor
}

func columnValues(t *testing.T, result *cypher.ExecuteResult, column string) []any {
	t.Helper()
	for i, name := range result.Columns {
		if name != column {
			continue
		}
		values := make([]any, 0, len(result.Rows))
		for _, row := range result.Rows {
			values = append(values, row[i])
		}
		return values
	}
	t.Fatalf("column %q not found in %v", column, result.Columns)
	return nil
}

// TestSessionDatabaseExecutorOptions_UsesHostQueryCachePolicy is the fail-first
// reproduction of the Northwind realserver benchmark finding: a host that
// resolved the per-database query-result cache policy (for example
// --query-cache-size 0) did not reach the executors Bolt builds for its own
// databases, so they kept cypher.NewStorageExecutor's environment-derived
// default (1000 entries) and every repeated query was a result-cache hit.
//
// The deterministic observation is the constructed executor's own policy plus
// the host wiring that ran, not a latency measurement.
func TestSessionDatabaseExecutorOptions_UsesHostQueryCachePolicy(t *testing.T) {
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	manager := &mockDBManager{stores: map[string]storage.Engine{"bench_db": store}, defaultDB: "bench_db"}
	hostLogger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	legacy := &hostExecutorPolicyExecutor{}

	cfg := DefaultConfig()
	var callbackArgs []string
	cfg.DatabaseExecutorOptions = func(dbName, authToken string) cypher.DatabaseScopedExecutorOptions {
		callbackArgs = append(callbackArgs, dbName, authToken)
		return cypher.DatabaseScopedExecutorOptions{
			Name: "wrong_database", Storage: manager, AuthToken: "wrong-token", UseAuthenticatedStorage: true,
			QueryCachePolicy: true, QueryCacheMaxEntries: 7, QueryCacheTTL: 37 * time.Second,
			Configure: func(executor *cypher.StorageExecutor, _ storage.Engine) {
				executor.SetSettingsResolver(func() cypher.SettingsSnapshot {
					return cypher.SettingsSnapshot{
						Configured: map[string]string{"db.nornic.query_cache.max_entries": "7"},
					}
				})
				executor.SetLogger(hostLogger)
				executor.SetSlowQueryThreshold(3 * time.Second)
			},
		}
	}

	session := newPolicyTestSession(cfg, manager, "")
	session.server.executor = legacy
	executor := autoCommitScopedExecutor(t, session, "bench_db")

	maxEntries, ttl := executor.QueryCachePolicy()
	require.Equal(t, 7, maxEntries, "host result-cache policy must reach the Bolt database-scoped executor")
	require.Equal(t, 37*time.Second, ttl)
	require.Equal(t, []string{"bench_db", ""}, callbackArgs)
	require.Empty(t, legacy.configured,
		"the host factory supplies the complete configuration; the legacy per-protocol configurator must not run afterwards")
	require.False(t, executor.Logger().Enabled(context.Background(), slog.LevelInfo),
		"the host logger must reach the executor")
	require.True(t, cypher.NewStorageExecutor(store).Logger().Enabled(context.Background(), slog.LevelInfo),
		"an executor the host did not configure keeps the info-level fallback logger")
	require.Equal(t, 3*time.Second, executor.SlowQueryThreshold())

	// The host's settings resolver reaches SHOW SETTINGS for this database, so
	// the configuration Bolt publishes is the one the host resolved.
	settings, err := executor.Execute(context.Background(), "SHOW SETTINGS db.nornic.query_cache.max_entries", nil)
	require.NoError(t, err)
	require.Equal(t, []any{"db.nornic.query_cache.max_entries"}, columnValues(t, settings, "name"))
	require.Equal(t, []any{"7"}, columnValues(t, settings, "value"))
}

// TestSessionDatabaseExecutorOptions_DisabledResultCacheCountsNoHits proves the
// production consequence: with the host policy at zero entries an identical
// repeated query is never a result-cache hit, while a non-default positive
// policy on another database still caches (same harness, so the counter
// instrument itself is proven). Statement-text caches stay valid either way.
func TestSessionDatabaseExecutorOptions_DisabledResultCacheCountsNoHits(t *testing.T) {
	inner := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	manager, err := multidb.NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	for _, name := range []string{"bench_cache_off", "bench_cache_on"} {
		require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: name}))
	}

	cfg := DefaultConfig()
	cfg.DatabaseExecutorOptions = func(dbName, _ string) cypher.DatabaseScopedExecutorOptions {
		options := cypher.DatabaseScopedExecutorOptions{
			QueryCachePolicy: true, QueryCacheTTL: 41 * time.Second, StatementCacheManager: manager,
		}
		if dbName == "bench_cache_on" {
			options.QueryCacheMaxEntries = 6
		}
		return options
	}
	session := newPolicyTestSession(cfg, manager, "")

	const probe = "MATCH (n:CachePolicyProbe) RETURN count(n) AS c"
	for _, tc := range []struct {
		database   string
		wantMax    int
		wantHits   float64
		wantMisses float64
	}{
		{database: "bench_cache_off", wantMax: 0, wantHits: 0, wantMisses: 0},
		{database: "bench_cache_on", wantMax: 6, wantHits: 1, wantMisses: 1},
	} {
		t.Run(tc.database, func(t *testing.T) {
			executor := autoCommitScopedExecutor(t, session, tc.database)
			maxEntries, ttl := executor.QueryCachePolicy()
			require.Equal(t, tc.wantMax, maxEntries)
			require.Equal(t, 41*time.Second, ttl, "per-database TTL must be the resolved value")

			bag := observability.NewCacheMetrics(prometheus.NewRegistry())
			executor.SetCacheMetrics(bag)
			for range 2 {
				_, err := executor.Execute(context.Background(), probe, nil)
				require.NoError(t, err)
			}
			require.Equal(t, tc.wantHits, testutil.ToFloat64(bag.Hits.WithLabelValues("query_result")))
			require.Equal(t, tc.wantMisses, testutil.ToFloat64(bag.Misses.WithLabelValues("query_result")))

			// The result cache is the only cache the policy disables.
			require.NotNil(t, executor.StatementCaches())
			require.True(t, executor.StatementCacheSelectionValid())
		})
	}
}

// TestSessionDatabaseExecutorOptions_ExplicitTransactionUsesHostPolicy covers
// the other executor lifetime: an explicit transaction builds a fresh executor
// per session, which must carry the same host policy.
func TestSessionDatabaseExecutorOptions_ExplicitTransactionUsesHostPolicy(t *testing.T) {
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	manager := &mockDBManager{stores: map[string]storage.Engine{"bench_tx": store}, defaultDB: "bench_tx"}
	cfg := DefaultConfig()
	cfg.DatabaseExecutorOptions = func(string, string) cypher.DatabaseScopedExecutorOptions {
		return cypher.DatabaseScopedExecutorOptions{QueryCachePolicy: true, QueryCacheMaxEntries: 11, QueryCacheTTL: 13 * time.Second}
	}
	session := newPolicyTestSession(cfg, manager, "")

	executor, err := session.getExecutorForDatabase("bench_tx", true)
	require.NoError(t, err)
	txAdapter, ok := executor.(*transactionalBoltQueryExecutorAdapter)
	require.True(t, ok, "explicit transaction adapter type")
	maxEntries, ttl := txAdapter.executor.QueryCachePolicy()
	require.Equal(t, 11, maxEntries)
	require.Equal(t, 13*time.Second, ttl)
}

// TestSessionDatabaseExecutorOptions_PreservesForwardedAuthSelection keeps the
// protocol-owned storage selection intact: a forwarded bearer header still
// resolves storage with the token and still bypasses the shared executor cache,
// even though the host factory reports unauthenticated storage for that
// database (the HTTP path only authenticates databases with a remote
// constituent).
func TestSessionDatabaseExecutorOptions_PreservesForwardedAuthSelection(t *testing.T) {
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	manager := &mockDBManager{stores: map[string]storage.Engine{"bench_db": store}, defaultDB: "bench_db"}
	cfg := DefaultConfig()
	var callbackArgs []string
	cfg.DatabaseExecutorOptions = func(dbName, authToken string) cypher.DatabaseScopedExecutorOptions {
		callbackArgs = append(callbackArgs, dbName, authToken)
		return cypher.DatabaseScopedExecutorOptions{
			Name: "host_name", Storage: manager, AuthToken: "", UseAuthenticatedStorage: false,
			QueryCachePolicy: true, QueryCacheMaxEntries: 3, QueryCacheTTL: 17 * time.Second,
		}
	}
	session := newPolicyTestSession(cfg, manager, "Bearer forwarded-token")

	first := autoCommitScopedExecutor(t, session, "bench_db")
	second := autoCommitScopedExecutor(t, session, "bench_db")

	require.Equal(t, []string{"bench_db", "Bearer forwarded-token", "bench_db", "Bearer forwarded-token"}, callbackArgs,
		"the host factory receives the resolved database name and the session forwarded token for each executor it builds")
	require.Equal(t, "bench_db", manager.lastGetDB)
	require.Equal(t, "Bearer forwarded-token", manager.lastAuth)
	require.Equal(t, 2, manager.getCalls, "auth-scoped resolution must not enter the shared executor cache")
	require.NotSame(t, first, second)
	maxEntries, _ := first.QueryCachePolicy()
	require.Equal(t, 3, maxEntries)
}

// TestSessionDatabaseExecutorOptions_CompositeAuthForwardsExactDatabaseAndToken
// checks the composite path with a forwarded token: the exact resolved database
// name reaches the host factory, and the remote constituent engine is created
// with the session's forwarded token (the host factory's own
// UseAuthenticatedStorage=false must not override this protocol decision).
func TestSessionDatabaseExecutorOptions_CompositeAuthForwardsExactDatabaseAndToken(t *testing.T) {
	inner := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	var remoteDatabases, remoteTokens []string
	manager, err := multidb.NewDatabaseManager(t.Context(), inner, &multidb.Config{
		DefaultDatabase: "nornic",
		SystemDatabase:  "system",
		RemoteEngineFactory: func(ref multidb.ConstituentRef, authToken string) (storage.Engine, error) {
			remoteDatabases = append(remoteDatabases, ref.DatabaseName)
			remoteTokens = append(remoteTokens, authToken)
			// A local engine stands in for the remote constituent: this test
			// establishes the forwarded token, not remote transport.
			return fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "bench_local"}))
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "bench_composite", []multidb.ConstituentRef{
		{Alias: "local1", DatabaseName: "bench_local", Type: "local", AccessMode: "read_write"},
		{Alias: "r1", DatabaseName: "bench_remote", Type: "remote", AccessMode: "read", URI: "bolt://127.0.0.1:7687", AuthMode: "oidc_forwarding"},
	}))

	cfg := DefaultConfig()
	var callbackDatabases []string
	cfg.DatabaseExecutorOptions = func(dbName, _ string) cypher.DatabaseScopedExecutorOptions {
		callbackDatabases = append(callbackDatabases, dbName)
		return cypher.DatabaseScopedExecutorOptions{
			QueryCachePolicy: true, QueryCacheMaxEntries: 0, StatementCacheManager: manager,
			AuthToken: "", UseAuthenticatedStorage: false,
		}
	}
	session := newPolicyTestSession(cfg, manager, "Bearer bench-token")

	executor := autoCommitScopedExecutor(t, session, "bench_composite")

	require.Equal(t, []string{"bench_composite"}, callbackDatabases)
	require.Equal(t, []string{"bench_remote"}, remoteDatabases)
	require.Equal(t, []string{"Bearer bench-token"}, remoteTokens)
	maxEntries, _ := executor.QueryCachePolicy()
	require.Equal(t, 0, maxEntries, "host policy still applies on the authenticated composite path")
}

// TestSessionDatabaseExecutorOptions_HostStatementCacheOwner covers statement
// caching parity: every executor lifetime for a database shares the statement
// caches of the host-supplied manager, including while the result cache is
// disabled.
func TestSessionDatabaseExecutorOptions_HostStatementCacheOwner(t *testing.T) {
	inner := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	manager, err := multidb.NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "bench_stmt"}))

	cfg := DefaultConfig()
	cfg.DatabaseExecutorOptions = func(string, string) cypher.DatabaseScopedExecutorOptions {
		return cypher.DatabaseScopedExecutorOptions{
			QueryCachePolicy: true, QueryCacheMaxEntries: 0, StatementCacheManager: manager,
		}
	}
	session := newPolicyTestSession(cfg, manager, "")

	autoCommit := autoCommitScopedExecutor(t, session, "bench_stmt")
	firstTx, err := session.getExecutorForDatabase("bench_stmt", true)
	require.NoError(t, err)
	secondTx, err := session.getExecutorForDatabase("bench_stmt", true)
	require.NoError(t, err)

	want := autoCommit.StatementCaches()
	require.NotNil(t, want)
	require.Same(t, want, firstTx.(*transactionalBoltQueryExecutorAdapter).executor.StatementCaches())
	require.Same(t, want, secondTx.(*transactionalBoltQueryExecutorAdapter).executor.StatementCaches())
	require.True(t, autoCommit.StatementCacheSelectionValid())
}

// TestSessionDatabaseExecutorOptions_WithoutHostFactoryKeepsStandaloneWiring is
// the unchanged host boundary: an embedder that supplies no host factory keeps
// configuring its own executors through the per-protocol configurator and the
// constructor's environment-derived cache policy.
func TestSessionDatabaseExecutorOptions_WithoutHostFactoryKeepsStandaloneWiring(t *testing.T) {
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	manager := &mockDBManager{stores: map[string]storage.Engine{"nornic": store}, defaultDB: "nornic"}
	base := cypher.NewStorageExecutor(store)
	legacy := &hostExecutorPolicyExecutor{baseExec: base}

	session := newPolicyTestSession(DefaultConfig(), manager, "")
	session.server.executor = legacy
	executor := autoCommitScopedExecutor(t, session, "nornic")

	require.Equal(t, []string{"nornic"}, legacy.configured)
	wantMax, wantTTL := cypher.NewStorageExecutor(store).QueryCachePolicy()
	maxEntries, ttl := executor.QueryCachePolicy()
	require.Equal(t, wantMax, maxEntries)
	require.Equal(t, wantTTL, ttl)
}
