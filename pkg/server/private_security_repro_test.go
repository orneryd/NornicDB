package server

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// TestPrivateReproCachedExecutorsRejectBareTransactionCommands pins the
// shared-executor boundary at the server's cached per-database executor:
// bare transaction commands are rejected there, session executors keep the
// embedded explicit-transaction pattern, and one-statement scripts still
// work on a private clone.
func TestPrivateReproCachedExecutorsRejectBareTransactionCommands(t *testing.T) {
	server, _ := setupTestServer(t)
	ctx := context.Background()

	shared, err := server.getExecutorForDatabase("nornic")
	require.NoError(t, err)
	for _, query := range []string{"BEGIN", "COMMIT", "ROLLBACK", "BEGIN TRANSACTION"} {
		_, err := shared.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.False(t, shared.HasActiveTransaction(), query)
	}

	// Protocol transaction owners use their own per-session executors, which
	// are not shared and keep accepting transaction control.
	session, err := server.newExecutorForDatabase("nornic")
	require.NoError(t, err)
	_, err = session.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = session.Execute(ctx, "CREATE (:CachedSessionProbe)", nil)
	require.NoError(t, err)
	_, err = session.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)

	// The one-statement script form still runs on the shared executor's
	// private clone and leaves no transaction behind.
	_, err = shared.Execute(ctx, "BEGIN CREATE (:CachedScriptProbe) COMMIT", nil)
	require.NoError(t, err)
	require.False(t, shared.HasActiveTransaction())
	rows, err := shared.Execute(ctx, "MATCH (n:CachedScriptProbe) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), rows.Rows[0][0])
}

func TestPrivateReproHTTPBareBeginRollbackOtherRequest(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	request := func(statement string) TransactionResponse {
		recorder := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{
			"statements": []map[string]any{{"statement": statement}},
		}, token)
		require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
		var response TransactionResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
		require.Empty(t, response.Errors, statement)
		return response
	}
	count := func() float64 {
		response := request("MATCH (n:PrivateTxProbe) RETURN count(n)")
		require.Len(t, response.Results, 1)
		return response.Results[0].Data[0].Row[0].(float64)
	}
	require.Equal(t, float64(0), count())
	begin := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{
		"statements": []map[string]any{{"statement": "BEGIN"}},
	}, token)
	var beginResponse TransactionResponse
	require.NoError(t, json.Unmarshal(begin.Body.Bytes(), &beginResponse))
	require.Len(t, beginResponse.Errors, 1)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", beginResponse.Errors[0].Code)
	for _, statement := range []string{"begin transaction", "CoMmIt", "ROLLBACK TRANSACTION", "USE nornic BEGIN"} {
		recorder := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{
			"statements": []map[string]any{{"statement": statement}},
		}, token)
		var response TransactionResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
		require.Len(t, response.Errors, 1, statement)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", response.Errors[0].Code, statement)
	}
	script := request("BEGIN CREATE (:PrivateScriptProbe) COMMIT")
	require.Empty(t, script.Errors)
	t.Cleanup(func() {
		executor, err := server.getExecutorForDatabase("nornic")
		if err == nil && executor.HasActiveTransaction() {
			request("ROLLBACK")
		}
	})
	created := request("CREATE (:PrivateTxProbe) RETURN 3")
	require.Equal(t, float64(3), created.Results[0].Data[0].Row[0])
	require.Equal(t, float64(1), count(), "acknowledged write must survive unrelated requests")
}

func TestPrivateReproCompositeConstituentAccess(t *testing.T) {
	server, authenticator := setupTestServer(t)
	require.NoError(t, server.dbManager.CreateDatabase("private_allowed"))
	require.NoError(t, server.dbManager.CreateDatabase("private_denied"))
	require.NoError(t, server.dbManager.CreateCompositeDatabase("private_cmp", []multidb.ConstituentRef{
		{Alias: "a", DatabaseName: "private_allowed", Type: "local", AccessMode: "read_write"},
		{Alias: "b", DatabaseName: "private_denied", Type: "local", AccessMode: "read_write"},
	}))
	_, err := authenticator.CreateUser("private_editor", "password123", []auth.Role{auth.RoleEditor})
	require.NoError(t, err)
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(context.Background(), "editor", []string{"private_allowed", "private_cmp"}))
	for _, database := range []string{"private_allowed", "private_cmp"} {
		require.NoError(t, server.privilegesStore.SavePrivilege(context.Background(), "editor", database, true, true))
	}
	request := func(database, token, statement string) (int, TransactionResponse) {
		recorder := makeRequest(t, server, http.MethodPost, "/db/"+database+"/tx/commit", map[string]any{
			"statements": []map[string]any{{"statement": statement}},
		}, "Bearer "+token)
		var response TransactionResponse
		_ = json.Unmarshal(recorder.Body.Bytes(), &response)
		return recorder.Code, response
	}
	_, seeded := request("private_denied", getAuthToken(t, authenticator, "admin"), "CREATE (:PrivateSecret {v: 'hidden'})")
	require.Empty(t, seeded.Errors)
	_, allowedSeed := request("private_allowed", getAuthToken(t, authenticator, "admin"), "CREATE (:PrivateSecret {v: 'allowed'})")
	require.Empty(t, allowedSeed.Errors)
	userToken := getAuthToken(t, authenticator, "private_editor")
	_, allowed := request("private_cmp", userToken, "CALL { USE private_cmp.a MATCH (n:PrivateSecret) RETURN n.v AS v } RETURN v")
	require.Empty(t, allowed.Errors)
	require.Equal(t, "allowed", allowed.Results[0].Data[0].Row[0])
	code, direct := request("private_denied", userToken, "MATCH (n:PrivateSecret) RETURN n.v")
	require.Equal(t, http.StatusOK, code)
	require.NotEmpty(t, direct.Errors)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", direct.Errors[0].Code)
	require.Empty(t, direct.Results)
	code, exposed := request("private_cmp", userToken, "CALL { USE private_cmp.b MATCH (n:PrivateSecret) RETURN n.v AS v } RETURN v")
	require.Equal(t, http.StatusOK, code)
	require.Len(t, exposed.Errors, 1)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", exposed.Errors[0].Code)
	_, forbiddenWrite := request("private_cmp", userToken, "USE private_cmp.b CREATE (:PrivateSecret {v: 'written'})")
	require.Len(t, forbiddenWrite.Errors, 1)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", forbiddenWrite.Errors[0].Code)
}

// TestPrivateReproCompositePatternFormsRequireConstituentTarget pins the
// Finding-2 fix: pattern comprehensions and COUNT{}/EXISTS{} pattern
// subqueries read graph data, so on a composite root they must be refused
// with the same composite-target error as a plain MATCH instead of reading
// denied constituents.
func TestPrivateReproCompositePatternFormsRequireConstituentTarget(t *testing.T) {
	server, authenticator := setupTestServer(t)
	require.NoError(t, server.dbManager.CreateDatabase("private_allowed"))
	require.NoError(t, server.dbManager.CreateDatabase("private_denied"))
	require.NoError(t, server.dbManager.CreateCompositeDatabase("private_cmp", []multidb.ConstituentRef{
		{Alias: "a", DatabaseName: "private_allowed", Type: "local", AccessMode: "read_write"},
		{Alias: "b", DatabaseName: "private_denied", Type: "local", AccessMode: "read_write"},
	}))
	_, err := authenticator.CreateUser("private_editor", "password123", []auth.Role{auth.RoleEditor})
	require.NoError(t, err)
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(context.Background(), "editor", []string{"private_allowed", "private_cmp"}))
	for _, database := range []string{"private_allowed", "private_cmp"} {
		require.NoError(t, server.privilegesStore.SavePrivilege(context.Background(), "editor", database, true, true))
	}
	request := func(database, token, statement string) TransactionResponse {
		recorder := makeRequest(t, server, http.MethodPost, "/db/"+database+"/tx/commit", map[string]any{
			"statements": []map[string]any{{"statement": statement}},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
		var response TransactionResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
		return response
	}
	seeded := request("private_denied", getAuthToken(t, authenticator, "admin"), "CREATE (:PrivateSecret {v: 'hidden'})-[:R]->(:PrivateSecret {v: 'hidden2'})")
	require.Empty(t, seeded.Errors)
	userToken := getAuthToken(t, authenticator, "private_editor")

	// Plain MATCH stays blocked with the composite-target error.
	plain := request("private_cmp", userToken, "MATCH (n:PrivateSecret) RETURN n.v")
	require.Len(t, plain.Errors, 1)
	require.Equal(t, "Neo.ClientError.Statement.NotAllowed", plain.Errors[0].Code)

	// The formerly-leaking forms now hit the same guard.
	for _, statement := range []string{
		"RETURN [(n:PrivateSecret)-[:R]->(m) | m.v] AS v",
		"WITH 1 AS x RETURN [(n:PrivateSecret)-[:R]->(m) | n.v] AS v",
		"UNWIND [1] AS x RETURN [(n)-->(m) | m.v] AS v",
		"RETURN COUNT { (n:PrivateSecret) } AS c",
		"RETURN EXISTS { (n:PrivateSecret {v: 'hidden'}) } AS e",
	} {
		response := request("private_cmp", userToken, statement)
		require.Len(t, response.Errors, 1, statement)
		require.Equal(t, "Neo.ClientError.Statement.NotAllowed", response.Errors[0].Code, statement)
		for _, result := range response.Results {
			for _, row := range result.Data {
				require.Nil(t, row.Row, statement)
			}
		}
	}

	// A statement that reads no graph still runs on the composite root.
	allowed := request("private_cmp", userToken, "RETURN 1 AS one")
	require.Empty(t, allowed.Errors)
	require.Equal(t, float64(1), allowed.Results[0].Data[0].Row[0])
}

// TestPrivateReproShellParamsAreScopedPerClient pins the Finding-3 fix:
// :param values set by one authenticated caller are not visible to another
// caller of the same database, and neither can clear or overwrite them.
func TestPrivateReproShellParamsAreScopedPerClient(t *testing.T) {
	server, authenticator := setupTestServer(t)
	_, err := authenticator.CreateUser("zz_reader", "password123", []auth.Role{auth.RoleViewer})
	require.NoError(t, err)
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(context.Background(), "viewer", []string{"nornic"}))
	require.NoError(t, server.privilegesStore.SavePrivilege(context.Background(), "viewer", "nornic", true, true))

	request := func(token, statement string) TransactionResponse {
		recorder := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{
			"statements": []map[string]any{{"statement": statement}},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
		var response TransactionResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
		return response
	}
	adminToken := getAuthToken(t, authenticator, "admin")
	readerToken := getAuthToken(t, authenticator, "zz_reader")

	set := request(adminToken, ":param apiSecret => 'TOP-SECRET-VALUE'")
	require.Empty(t, set.Errors)

	// The other caller sees neither the name nor the value.
	listed := request(readerToken, ":params")
	require.Empty(t, listed.Errors)
	require.Empty(t, listed.Results[0].Data)
	read := request(readerToken, "RETURN $apiSecret AS s")
	require.Len(t, read.Errors, 1)

	// The admin's own view is unchanged by the other caller's list/clear/set.
	readerSet := request(readerToken, ":param limitX => 0")
	require.Empty(t, readerSet.Errors)
	readerClear := request(readerToken, ":param clear")
	require.Empty(t, readerClear.Errors)
	adminList := request(adminToken, ":params")
	require.Empty(t, adminList.Errors)
	require.Len(t, adminList.Results[0].Data, 1, "admin's parameter must survive the reader's clear")
	require.Equal(t, "apiSecret", adminList.Results[0].Data[0].Row[0])
	require.Equal(t, "TOP-SECRET-VALUE", adminList.Results[0].Data[0].Row[1])
	adminRead := request(adminToken, "RETURN $apiSecret AS s")
	require.Empty(t, adminRead.Errors)
	require.Equal(t, "TOP-SECRET-VALUE", adminRead.Results[0].Data[0].Row[0])
}

// TestPrivateReproCreateOrReplaceDatabaseRequiresAdmin pins the Finding-4 fix:
// CREATE OR REPLACE DATABASE is an admin command (Forbidden for a non-admin)
// and actually creates or replaces the database for an admin instead of
// silently doing nothing.
func TestPrivateReproCreateOrReplaceDatabaseRequiresAdmin(t *testing.T) {
	server, authenticator := setupTestServer(t)
	_, err := authenticator.CreateUser("zz_editor", "password123", []auth.Role{auth.RoleEditor})
	require.NoError(t, err)
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(context.Background(), "editor", []string{"nornic"}))
	require.NoError(t, server.privilegesStore.SavePrivilege(context.Background(), "editor", "nornic", true, true))

	request := func(token, statement string) TransactionResponse {
		recorder := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{
			"statements": []map[string]any{{"statement": statement}},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
		var response TransactionResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
		return response
	}
	adminToken := getAuthToken(t, authenticator, "admin")
	editorToken := getAuthToken(t, authenticator, "zz_editor")

	denied := request(editorToken, "CREATE OR REPLACE DATABASE zzc5")
	require.Len(t, denied.Errors, 1)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", denied.Errors[0].Code)
	require.False(t, server.dbManager.Exists("zzc5"))

	created := request(adminToken, "CREATE OR REPLACE DATABASE zzc5")
	require.Empty(t, created.Errors)
	require.True(t, server.dbManager.Exists("zzc5"))

	replaced := request(adminToken, "CREATE OR REPLACE DATABASE zzc5")
	require.Empty(t, replaced.Errors)
	require.True(t, server.dbManager.Exists("zzc5"))
}
