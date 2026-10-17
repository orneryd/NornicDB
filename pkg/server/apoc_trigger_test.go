package server

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const apocTriggerStampStatement = "UNWIND $createdNodes AS n SET n.stamp = $trigger"

// triggerPost sends statements to database's /tx/commit endpoint.
func triggerPost(t *testing.T, server *Server, token, database, statement string, params map[string]any) TransactionResponse {
	t.Helper()
	request := map[string]any{"statements": []map[string]any{{"statement": statement, "parameters": params}}}
	response := makeRequest(t, server, "POST", "/db/"+database+"/tx/commit", request, "Bearer "+token)
	var body TransactionResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body), response.Body.String())
	return body
}

// triggerExplicit runs statement in an explicit HTTP transaction (open,
// then commit) and returns the commit's response.
func triggerExplicit(t *testing.T, server *Server, token, database, statement string) TransactionResponse {
	t.Helper()
	request := map[string]any{"statements": []map[string]any{{"statement": statement}}}
	opened := makeRequest(t, server, "POST", "/db/"+database+"/tx", request, "Bearer "+token)
	location := opened.Header().Get("Location")
	require.NotEmpty(t, location, opened.Body.String())
	parsed, err := url.Parse(location)
	require.NoError(t, err)
	committed := makeRequest(t, server, "POST", parsed.Path+"/commit", map[string]any{"statements": []map[string]any{}}, "Bearer "+token)
	var body TransactionResponse
	require.NoError(t, json.Unmarshal(committed.Body.Bytes(), &body), committed.Body.String())
	return body
}

func requireTriggerOK(t *testing.T, body TransactionResponse) {
	t.Helper()
	require.Empty(t, body.Errors)
}

func triggerInstall(t *testing.T, server *Server, token, database, name string) {
	t.Helper()
	requireTriggerOK(t, triggerPost(t, server, token, "system",
		"CALL apoc.trigger.install($db, $name, $statement, {phase: 'before'})",
		map[string]any{"db": database, "name": name, "statement": apocTriggerStampStatement}))
}

// stampedCount is the number of :Doc nodes of database the trigger stamped,
// and of those it didn't.
func stampedCount(t *testing.T, server *Server, token, database string) (stamped, unstamped float64) {
	t.Helper()
	body := triggerPost(t, server, token, database, "MATCH (n:Doc) RETURN count(n.stamp), count(n) - count(n.stamp)", nil)
	requireTriggerOK(t, body)
	row := body.Results[0].Data[0].Row
	return row[0].(float64), row[1].(float64)
}

// A before trigger fires on every HTTP route that writes the database: an
// auto-commit statement, an explicit transaction, a top-level USE from
// another database, and a composite database's auto-commit and explicit
// writes to its constituent.
func TestApocTriggerFiresOnHTTPRoutes(t *testing.T) {
	t.Setenv("NORNICDB_APOC_TRIGGER_ENABLED", "true")
	server, users := setupTestServer(t)
	admin := getAuthToken(t, users, "admin")
	requireTriggerOK(t, triggerPost(t, server, admin, "system", "CREATE DATABASE trigdb", nil))
	requireTriggerOK(t, triggerPost(t, server, admin, "system", "CREATE COMPOSITE DATABASE trigcomp ALIAS part FOR DATABASE trigdb", nil))
	triggerInstall(t, server, admin, "nornic", "stamp")
	triggerInstall(t, server, admin, "trigdb", "stamp")

	requireTriggerOK(t, triggerPost(t, server, admin, "nornic", "CREATE (:Doc {name: 'auto'})", nil))
	requireTriggerOK(t, triggerExplicit(t, server, admin, "nornic", "CREATE (:Doc {name: 'explicit'})"))
	stamped, unstamped := stampedCount(t, server, admin, "nornic")
	require.Equal(t, [2]float64{2, 0}, [2]float64{stamped, unstamped})

	requireTriggerOK(t, triggerPost(t, server, admin, "nornic", "USE trigdb CREATE (:Doc {name: 'use'})", nil))
	requireTriggerOK(t, triggerPost(t, server, admin, "trigcomp", "USE trigcomp.part CREATE (:Doc {name: 'composite auto'})", nil))
	requireTriggerOK(t, triggerExplicit(t, server, admin, "trigcomp", "USE trigcomp.part CREATE (:Doc {name: 'composite explicit'})"))
	stamped, unstamped = stampedCount(t, server, admin, "trigdb")
	require.Equal(t, [2]float64{3, 0}, [2]float64{stamped, unstamped})
}

// A failing before trigger fails the HTTP statement with
// TransactionHookFailed and stores nothing.
func TestApocTriggerFailureOverHTTP(t *testing.T) {
	t.Setenv("NORNICDB_APOC_TRIGGER_ENABLED", "true")
	server, users := setupTestServer(t)
	admin := getAuthToken(t, users, "admin")
	requireTriggerOK(t, triggerPost(t, server, admin, "system",
		"CALL apoc.trigger.install('nornic', 'breaks', 'UNWIND $createdNodes AS n SET n.x = 1 / 0', {})", nil))
	body := triggerPost(t, server, admin, "nornic", "CREATE (:Doc {name: 'x'})", nil)
	require.NotEmpty(t, body.Errors)
	require.Equal(t, "Neo.ClientError.Transaction.TransactionHookFailed", body.Errors[0].Code, body.Errors[0].Message)
	stamped, unstamped := stampedCount(t, server, admin, "nornic")
	require.Zero(t, stamped+unstamped)
}

// Installing a trigger needs the admin entitlement; triggers are refused
// while disabled.
func TestApocTriggerPrivilegesAndDisabled(t *testing.T) {
	t.Setenv("NORNICDB_APOC_TRIGGER_ENABLED", "true")
	server, users := setupTestServer(t)
	reader := getAuthToken(t, users, "reader")
	body := triggerPost(t, server, reader, "system", "CALL apoc.trigger.install('nornic', 'stamp', 'RETURN 1', {})", nil)
	require.NotEmpty(t, body.Errors)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", body.Errors[0].Code)

	t.Setenv("NORNICDB_APOC_TRIGGER_ENABLED", "false")
	disabled, disabledUsers := setupTestServer(t)
	admin := getAuthToken(t, disabledUsers, "admin")
	body = triggerPost(t, disabled, admin, "system", "CALL apoc.trigger.install('nornic', 'stamp', 'RETURN 1', {})", nil)
	require.NotEmpty(t, body.Errors)
	require.Equal(t, "Neo.ClientError.Procedure.ProcedureCallFailed", body.Errors[0].Code)
}

// DROP DATABASE deletes the database's triggers: a database created later
// with the same name starts without them.
func TestApocTriggerDroppedWithTheDatabase(t *testing.T) {
	t.Setenv("NORNICDB_APOC_TRIGGER_ENABLED", "true")
	server, users := setupTestServer(t)
	admin := getAuthToken(t, users, "admin")
	requireTriggerOK(t, triggerPost(t, server, admin, "system", "CREATE DATABASE dropdb", nil))
	triggerInstall(t, server, admin, "dropdb", "stamp")
	requireTriggerOK(t, triggerPost(t, server, admin, "system", "DROP DATABASE dropdb", nil))
	requireTriggerOK(t, triggerPost(t, server, admin, "system", "CREATE DATABASE dropdb", nil))
	shown := triggerPost(t, server, admin, "system", "CALL apoc.trigger.show('dropdb')", nil)
	requireTriggerOK(t, shown)
	require.Empty(t, shown.Results[0].Data)
	requireTriggerOK(t, triggerPost(t, server, admin, "dropdb", "CREATE (:Doc)", nil))
	stamped, _ := stampedCount(t, server, admin, "dropdb")
	require.Zero(t, stamped)
}

// openTriggerTestServer opens a server over the database in dir, as a
// restart does.
func openTriggerTestServer(t *testing.T, dir string) (*Server, *nornicdb.DB, string) {
	t.Helper()
	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Database.AsyncWritesEnabled = false
	db, err := nornicdb.Open(dir, config)
	require.NoError(t, err)
	authenticator, err := auth.NewAuthenticator(auth.AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("test-secret-key-for-testing-only-32b"),
	}, storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "system"))
	require.NoError(t, err)
	_, _ = authenticator.CreateUser("admin", "password123", []auth.Role{auth.RoleAdmin})
	serverConfig := DefaultConfig()
	serverConfig.Port = 0
	serverConfig.EmbeddingEnabled = false
	server, err := New(db, authenticator, serverConfig)
	require.NoError(t, err)
	return server, db, getAuthToken(t, authenticator, "admin")
}

// Triggers are kept in the system database: after a restart they still
// fire, and a stopped one stays stopped.
func TestApocTriggerSurvivesRestart(t *testing.T) {
	t.Setenv("NORNICDB_APOC_TRIGGER_ENABLED", "true")
	dir := t.TempDir()
	server, db, admin := openTriggerTestServer(t, dir)
	triggerInstall(t, server, admin, "nornic", "stamp")
	triggerInstall(t, server, admin, "nornic", "stopped")
	requireTriggerOK(t, triggerPost(t, server, admin, "system", "CALL apoc.trigger.stop('nornic', 'stopped')", nil))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, server.Stop(ctx))
	require.NoError(t, db.Close())

	server, db, admin = openTriggerTestServer(t, dir)
	t.Cleanup(func() {
		stopTestServer(t, server)
		_ = db.Close()
	})
	shown := triggerPost(t, server, admin, "system", "CALL apoc.trigger.show('nornic') YIELD name, paused RETURN name, paused", nil)
	requireTriggerOK(t, shown)
	require.Len(t, shown.Results[0].Data, 2)
	require.Equal(t, []interface{}{"stamp", false}, shown.Results[0].Data[0].Row)
	require.Equal(t, []interface{}{"stopped", true}, shown.Results[0].Data[1].Row)
	requireTriggerOK(t, triggerPost(t, server, admin, "nornic", "CREATE (:Doc {name: 'after restart'})", nil))
	result := triggerPost(t, server, admin, "nornic", "MATCH (n:Doc) RETURN n.stamp", nil)
	requireTriggerOK(t, result)
	require.Equal(t, []interface{}{"stamp"}, result.Results[0].Data[0].Row)
}
