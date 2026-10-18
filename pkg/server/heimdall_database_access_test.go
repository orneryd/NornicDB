package server

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// heimdallAuthDisabled is a request context the server marks as served with
// authentication disabled (withBifrostRBAC without a principal).
func heimdallAuthDisabled() context.Context {
	return auth.WithRequestAuthenticationDisabled(context.Background())
}

// heimdallPrincipal is a request context carrying a principal, as the server
// attaches it to Bifrost requests (withBifrostRBAC): the databases it may
// select and its permission resolver.
func heimdallPrincipal(databases []string, allows func(database string, permission auth.Permission) bool) context.Context {
	selections := make(map[string]string, len(databases))
	for _, database := range databases {
		selections[database] = database
	}
	return auth.WithRequestPrincipal(context.Background(), allows, auth.NewRequestDatabaseScope("", selections))
}

// heimdallPermissions allows the given permissions on every database.
func heimdallPermissions(allowed map[auth.Permission]bool) func(string, auth.Permission) bool {
	return func(_ string, permission auth.Permission) bool { return allowed[permission] }
}

func requireHeimdallForbidden(t *testing.T, err error) {
	t.Helper()
	var localized *localization.LocalizedError
	require.True(t, errors.As(err, &localized), "want a Forbidden error, got %v", err)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", localized.Code)
}

// Heimdall's database router checks the request principal's permissions on
// the database it runs against: Discover and Stats need read there, and
// Query runs its statement under the principal's resolver, so the executor
// refuses a read without read and a write without write, as on HTTP and Bolt.
func TestHeimdallRouterChecksDatabasePermissions(t *testing.T) {
	server, _ := setupTestServer(t)
	router := newHeimdallDBRouter(server.db, server.dbManager, nil)

	_, err := router.Query(heimdallAuthDisabled(), "nornic", "CREATE (:HeimdallSeed {name: 'seed'})", nil)
	require.NoError(t, err)

	noAccess := heimdallPrincipal([]string{"nornic"}, heimdallPermissions(nil))
	_, err = router.Discover(noAccess, "nornic", "seed", nil, 5, 0)
	requireHeimdallForbidden(t, err)
	_, err = router.Stats(noAccess, "nornic")
	requireHeimdallForbidden(t, err)
	_, err = router.Query(noAccess, "nornic", "MATCH (n:HeimdallSeed) RETURN n.name AS name", nil)
	requireHeimdallForbidden(t, err)

	writeOnly := heimdallPrincipal([]string{"nornic"}, heimdallPermissions(map[auth.Permission]bool{"access": true, auth.PermWrite: true}))
	_, err = router.Discover(writeOnly, "nornic", "seed", nil, 5, 0)
	requireHeimdallForbidden(t, err)
	_, err = router.Stats(writeOnly, "nornic")
	requireHeimdallForbidden(t, err)
	_, err = router.Query(writeOnly, "nornic", "MATCH (n:HeimdallSeed) RETURN n.name AS name", nil)
	require.ErrorContains(t, err, "read permission")

	readOnly := heimdallPrincipal([]string{"nornic"}, heimdallPermissions(map[auth.Permission]bool{"access": true, auth.PermRead: true}))
	_, err = router.Discover(readOnly, "nornic", "seed", nil, 5, 0)
	require.NoError(t, err)
	stats, err := router.Stats(readOnly, "nornic")
	require.NoError(t, err)
	require.GreaterOrEqual(t, stats.NodeCount, int64(1))
	rows, err := router.Query(readOnly, "nornic", "MATCH (n:HeimdallSeed) RETURN n.name AS name", nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	_, err = router.Query(readOnly, "nornic", "CREATE (:HeimdallDenied)", nil)
	require.ErrorContains(t, err, "write permission")

	rows, err = router.Query(heimdallAuthDisabled(), "nornic", "MATCH (n:HeimdallDenied) RETURN count(n) AS n", nil)
	require.NoError(t, err)
	require.EqualValues(t, 0, rows[0]["n"])
}

// A call whose context carries no principal, such as an action run in the
// background, is refused unless the server marked the request as served with
// authentication disabled; nothing runs.
func TestHeimdallRouterRefusesCallWithoutPrincipal(t *testing.T) {
	server, _ := setupTestServer(t)
	router := newHeimdallDBRouter(server.db, server.dbManager, nil)

	_, err := router.Query(context.Background(), "nornic", "CREATE (:HeimdallUnattributed)", nil)
	requireHeimdallForbidden(t, err)
	_, err = router.Discover(context.Background(), "nornic", "x", nil, 5, 0)
	requireHeimdallForbidden(t, err)
	_, err = router.Stats(context.Background(), "nornic")
	requireHeimdallForbidden(t, err)

	rows, err := router.Query(heimdallAuthDisabled(), "nornic", "MATCH (n:HeimdallUnattributed) RETURN count(n) AS n", nil)
	require.NoError(t, err)
	require.EqualValues(t, 0, rows[0]["n"])

	// The server marks only requests it serves with authentication disabled.
	request := httptest.NewRequest("GET", "/api/bifrost/status", nil)
	require.False(t, auth.RequestAuthenticationDisabledFromContext(server.withBifrostRBAC(request).Context()))
	server.auth = nil
	require.True(t, auth.RequestAuthenticationDisabledFromContext(server.withBifrostRBAC(request).Context()))
}

// A principal gets the same refusal for a database it may not use whether
// that database exists or not, and the router opens nothing for it.
func TestHeimdallRouterDeniesBeforeResolvingDatabase(t *testing.T) {
	server, _ := setupTestServer(t)
	router := newHeimdallDBRouter(server.db, server.dbManager, nil)
	require.NoError(t, server.dbManager.CreateDatabase("existing_secret"))

	principal := heimdallPrincipal([]string{"nornic"}, heimdallPermissions(map[auth.Permission]bool{"access": true, auth.PermRead: true, auth.PermWrite: true}))
	sameRefusal := func(call func(database string) error) {
		t.Helper()
		existing, missing := call("existing_secret"), call("missing_secret")
		requireHeimdallForbidden(t, existing)
		requireHeimdallForbidden(t, missing)
		require.Equal(t, strings.ReplaceAll(existing.Error(), "existing_secret", "X"), strings.ReplaceAll(missing.Error(), "missing_secret", "X"))
	}
	sameRefusal(func(database string) error {
		_, err := router.Query(principal, database, "RETURN 1", nil)
		return err
	})
	sameRefusal(func(database string) error {
		_, err := router.Discover(principal, database, "x", nil, 5, 0)
		return err
	})
	sameRefusal(func(database string) error {
		_, err := router.Stats(principal, database)
		return err
	})

	router.executorsMu.RLock()
	_, cached := router.executors["existing_secret"]
	router.executorsMu.RUnlock()
	require.False(t, cached, "the router created an executor for a refused database")
}

// Discover and Stats refuse a composite database: its storage reads every
// constituent, which read on the composite alone does not cover.
func TestHeimdallRouterRefusesCompositeSearchAndStats(t *testing.T) {
	server, _ := setupTestServer(t)
	router := newHeimdallDBRouter(server.db, server.dbManager, nil)
	for _, name := range []string{"open_part", "closed_part"} {
		require.NoError(t, server.dbManager.CreateDatabase(name))
	}
	require.NoError(t, server.dbManager.CreateCompositeDatabase("portfolio", []multidb.ConstituentRef{
		{Alias: "open", DatabaseName: "open_part", Type: "local", AccessMode: "read_write"},
		{Alias: "closed", DatabaseName: "closed_part", Type: "local", AccessMode: "read_write"},
	}))
	closed, err := server.dbManager.GetStorage("closed_part")
	require.NoError(t, err)
	_, err = closed.CreateNode(&storage.Node{ID: "closed-1", Labels: []string{"Document"}, Properties: map[string]interface{}{"title": "closed notes"}})
	require.NoError(t, err)

	compositeOnly := heimdallPrincipal([]string{"portfolio"}, func(database string, permission auth.Permission) bool {
		return database == "portfolio"
	})
	_, err = router.Discover(compositeOnly, "portfolio", "closed notes", nil, 5, 1)
	require.ErrorContains(t, err, "composite")
	_, err = router.Stats(compositeOnly, "portfolio")
	require.ErrorContains(t, err, "composite")
}

// A statement the principal may not run on a database leaves no executor
// for it: the router shares a new executor, with the database's search
// service and embed queue, only once the executor has admitted a statement.
// A leading USE runs on its target, so the database it is sent to needs only
// access, as before.
func TestHeimdallRouterSharesExecutorOnlyAfterStatementAdmitted(t *testing.T) {
	server, _ := setupTestServer(t)
	router := newHeimdallDBRouter(server.db, server.dbManager, nil)
	require.NoError(t, server.dbManager.CreateDatabase("cold_db"))
	require.NoError(t, server.dbManager.CreateDatabase("warm_db"))
	principal := heimdallPrincipal([]string{"cold_db", "warm_db"}, func(database string, permission auth.Permission) bool {
		return permission == "access" || database == "warm_db" && permission == auth.PermRead
	})

	_, err := router.Query(principal, "cold_db", "MATCH (n) RETURN count(n) AS n", nil)
	require.ErrorContains(t, err, "read permission")
	require.Nil(t, router.cachedExecutor("cold_db"), "the router shared an executor for a refused statement")

	rows, err := router.Query(principal, "cold_db", "USE warm_db MATCH (n) RETURN count(n) AS n", nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Nil(t, router.cachedExecutor("cold_db"))

	_, err = router.Query(principal, "warm_db", "MATCH (n) RETURN count(n) AS n", nil)
	require.NoError(t, err)
	require.NotNil(t, router.cachedExecutor("warm_db"))
}

// An action invoked asynchronously keeps the caller's principal, not its
// cancellation: it reaches the database router as the caller, after the
// caller's request has ended.
func TestHeimdallAsyncActionKeepsCallerPrincipal(t *testing.T) {
	server, _ := setupTestServer(t)
	router := newHeimdallDBRouter(server.db, server.dbManager, nil)
	require.NoError(t, server.dbManager.CreateDatabase("async_named"))
	named, err := server.dbManager.GetStorage("async_named")
	require.NoError(t, err)
	_, err = named.CreateNode(&storage.Node{ID: "async-named-node", Labels: []string{"Doc"}})
	require.NoError(t, err)
	type outcome struct {
		rows []map[string]interface{}
		err  error
	}
	outcomes := make(chan outcome, 1)
	heimdall.RegisterBuiltinAction(heimdall.ActionFunc{
		Name:     "heimdall_async_principal_probe",
		Category: "testing",
		Handler: func(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
			database, _ := ctx.Params["database"].(string)
			rows, err := ctx.Database.Query(ctx.Context, database, "MATCH (n) RETURN count(n) AS n", nil)
			outcomes <- outcome{rows: rows, err: err}
			return &heimdall.ActionResult{Success: err == nil}, nil
		},
	})
	invoker := heimdall.NewLiveHeimdallInvoker(heimdall.GetSubsystemManager(), nil, nil, router, nil)

	request, cancel := context.WithCancel(heimdallPrincipal([]string{"nornic", "async_named"}, heimdallPermissions(map[auth.Permission]bool{"access": true, auth.PermRead: true})))
	invoker.InvokeActionAsync(request, "heimdall_async_principal_probe", map[string]interface{}{"database": "nornic"})
	cancel()
	select {
	case got := <-outcomes:
		require.NoError(t, got.err)
		require.Len(t, got.rows, 1)
	case <-time.After(10 * time.Second):
		t.Fatal("the asynchronous action did not run")
	}

	invoker.InvokeActionAsync(request, "heimdall_async_principal_probe", map[string]interface{}{"database": "async_named"})
	select {
	case got := <-outcomes:
		require.NoError(t, got.err)
		require.EqualValues(t, 1, got.rows[0]["n"])
	case <-time.After(10 * time.Second):
		t.Fatal("the named-database asynchronous action did not run")
	}

	denied := heimdallPrincipal([]string{"async_named"}, heimdallPermissions(map[auth.Permission]bool{"access": true}))
	invoker.InvokeActionAsync(denied, "heimdall_async_principal_probe", map[string]interface{}{"database": "async_named"})
	select {
	case got := <-outcomes:
		require.ErrorContains(t, got.err, "read permission")
	case <-time.After(10 * time.Second):
		t.Fatal("the denied asynchronous action did not run")
	}

	invoker.InvokeActionAsync(heimdallAuthDisabled(), "heimdall_async_principal_probe", map[string]interface{}{"database": "async_named"})
	select {
	case got := <-outcomes:
		require.NoError(t, got.err)
		require.EqualValues(t, 1, got.rows[0]["n"])
	case <-time.After(10 * time.Second):
		t.Fatal("the auth-disabled asynchronous action did not run")
	}

	// Without a principal it is refused, as before.
	invoker.InvokeActionAsync(context.Background(), "heimdall_async_principal_probe", map[string]interface{}{"database": "nornic"})
	select {
	case got := <-outcomes:
		requireHeimdallForbidden(t, got.err)
	case <-time.After(10 * time.Second):
		t.Fatal("the asynchronous action did not run")
	}
}

// A context with a permission resolver but no database scope is an
// incomplete principal, and no context at all carries none: the router
// refuses both, without resolving or opening anything.
func TestHeimdallRouterRefusesIncompletePrincipal(t *testing.T) {
	server, _ := setupTestServer(t)
	router := newHeimdallDBRouter(server.db, server.dbManager, nil)
	resolverOnly := auth.WithRequestEntitlementResolver(context.Background(), func(string, auth.Permission) bool { return true })
	var noContext context.Context

	for _, ctx := range []context.Context{resolverOnly, noContext} {
		_, err := router.Query(ctx, "nornic", "RETURN 1 AS n", nil)
		requireHeimdallForbidden(t, err)
		_, err = router.Discover(ctx, "nornic", "x", nil, 5, 0)
		requireHeimdallForbidden(t, err)
		_, err = router.Stats(ctx, "nornic")
		requireHeimdallForbidden(t, err)
	}
	require.Nil(t, router.cachedExecutor("nornic"))
}
