package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestHTTPAdmissionBeforeResolution(t *testing.T) {
	server, authenticator := setupTestServer(t)
	require.NoError(t, server.dbManager.CreateDatabase("admission_secret"))
	_, err := authenticator.CreateUser("admission_limited", "password123", []auth.Role{auth.RoleEditor})
	require.NoError(t, err)
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(context.Background(), "editor", []string{"nornic"}))
	token := "Bearer " + getAuthToken(t, authenticator, "admission_limited")

	for _, route := range []struct {
		name       string
		method     string
		suffix     string
		statements []map[string]any
	}{
		{"empty commit", http.MethodPost, "/tx/commit", []map[string]any{}},
		{"single commit", http.MethodPost, "/tx/commit", []map[string]any{{"statement": "RETURN 1"}}},
		{"multiple commit", http.MethodPost, "/tx/commit", []map[string]any{{"statement": "RETURN 1"}, {"statement": "RETURN 2"}}},
		{"open", http.MethodPost, "/tx", []map[string]any{{"statement": "RETURN 1"}}},
		{"execute", http.MethodPost, "/tx/absent", []map[string]any{{"statement": "RETURN 1"}}},
		{"commit", http.MethodPost, "/tx/absent/commit", nil},
		{"rollback", http.MethodDelete, "/tx/absent", nil},
		{"info", http.MethodGet, "", nil},
		{"cluster", http.MethodGet, "/cluster/status", nil},
	} {
		t.Run(route.name, func(t *testing.T) {
			var existingStatus int
			for _, database := range []string{"admission_secret", "admission_missing"} {
				server.executorsMu.RLock()
				beforeExecutors := len(server.executors)
				server.executorsMu.RUnlock()
				beforeSearch := server.db.GetDatabaseSearchStatus(database)
				recorder := makeRequest(t, server, route.method, "/db/"+database+route.suffix,
					map[string]any{"statements": route.statements}, token)
				var response TransactionResponse
				require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response), recorder.Body.String())
				require.Len(t, response.Errors, 1, recorder.Body.String())
				require.Equal(t, "Neo.ClientError.Security.Forbidden", response.Errors[0].Code)
				require.Equal(t, "Database access is not allowed for user 'admission_limited' with roles [editor].", response.Errors[0].Message)
				if existingStatus == 0 {
					existingStatus = recorder.Code
				} else {
					require.Equal(t, existingStatus, recorder.Code)
				}
				server.executorsMu.RLock()
				require.Len(t, server.executors, beforeExecutors)
				server.executorsMu.RUnlock()
				require.Equal(t, beforeSearch, server.db.GetDatabaseSearchStatus(database))
			}
		})
	}
}

func TestHTTPCompositeAdmissionDoesNotOpenUnusedRemotes(t *testing.T) {
	s, users := setupTestServer(t)
	remoteCalls := 0
	manager, err := multidb.NewDatabaseManager(s.db.GetBaseStorageForManager(), &multidb.Config{
		DefaultDatabase: "nornic", SystemDatabase: "system",
		RemoteEngineFactory: func(multidb.ConstituentRef, string) (storage.Engine, error) {
			remoteCalls++
			return storage.NewMemoryEngine(), nil
		},
	})
	require.NoError(t, err)
	s.dbManager = manager
	require.NoError(t, manager.CreateDatabase("admitted_local"))
	require.NoError(t, manager.CreateCompositeDatabase("admission_composite", []multidb.ConstituentRef{
		{Alias: "zlocal", DatabaseName: "admitted_local", Type: "local", AccessMode: "read_write"},
		{Alias: "remote", DatabaseName: "peer", Type: "remote", AccessMode: "read_write", URI: "http://unused.invalid"},
	}))
	local, err := manager.GetStorage("admitted_local")
	require.NoError(t, err)
	_, err = local.CreateNode(&storage.Node{ID: "admitted-node", Labels: []string{"Admitted"}})
	require.NoError(t, err)
	ctx := context.Background()
	const role = "composite_limited"
	require.NoError(t, s.roleStore.CreateRole(ctx, role))
	require.NoError(t, s.roleEntitlementsStore.Set(ctx, role, []string{"read", "write"}))
	require.NoError(t, s.allowlistStore.SaveRoleDatabases(ctx, role, []string{"admission_composite", "admitted_local"}))
	require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "admitted_local", true, false))
	_, err = users.CreateUser(role, "password123", []auth.Role{role})
	require.NoError(t, err)
	token := "Bearer " + getAuthToken(t, users, role)
	for _, tc := range []struct {
		query  string
		denied bool
	}{
		{"CALL { USE admission_composite.zlocal CREATE (:DeniedFragment) RETURN 1 AS x } RETURN x", true},
		{"CALL { USE admission_composite.remote RETURN 1 AS x } RETURN x", true},
		{"CALL { USE admission_composite.zlocal RETURN 1 AS x } RETURN x", false},
		{"CALL { USE admission_composite.zlocal MATCH (n:Admitted) RETURN n } RETURN n", false},
	} {
		recorder := makeRequest(t, s, http.MethodPost, "/db/admission_composite/tx/commit", map[string]any{"statements": []map[string]any{{"statement": tc.query}}}, token)
		var response TransactionResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
		if tc.denied {
			require.Len(t, response.Errors, 1, recorder.Body.String())
			require.Equal(t, "Neo.ClientError.Security.Forbidden", response.Errors[0].Code)
		} else {
			require.Empty(t, response.Errors, recorder.Body.String())
			if strings.Contains(tc.query, "MATCH") {
				require.Contains(t, recorder.Body.String(), "4:admitted_local:admitted-node", "entity identity must retain its physical database")
			}
		}
		require.Zero(t, remoteCalls, "%s", tc.query)
	}
}

func TestHTTPOwnTerminationWithoutReadAndBeginAttribution(t *testing.T) {
	s, authenticator := setupTestServer(t)
	ctx := context.Background()
	const role = "http_write_only"
	require.NoError(t, s.roleStore.CreateRole(ctx, role))
	// Global entitlements cap data rights; the per-database record below
	// makes this a principal that can write but cannot read this graph.
	require.NoError(t, s.roleEntitlementsStore.Set(ctx, role, []string{"read", "write"}))
	require.NoError(t, s.allowlistStore.SaveRoleDatabases(ctx, role, []string{"nornic"}))
	require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "nornic", false, true))
	for _, user := range []string{"http_term_alice", "http_term_bob"} {
		_, err := authenticator.CreateUser(user, "password123", []auth.Role{auth.Role(role)})
		require.NoError(t, err)
	}
	token := func(user string) string { return "Bearer " + getAuthToken(t, authenticator, user) }
	admin := token("admin")
	post := func(path, user, query string, params map[string]any) TransactionResponse {
		t.Helper()
		recorder := makeRequest(t, s, http.MethodPost, path, map[string]any{"statements": []map[string]any{{"statement": query, "parameters": params}}}, token(user))
		var response TransactionResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response), recorder.Body.String())
		require.Empty(t, response.Errors, recorder.Body.String())
		return response
	}
	ids := map[string]string{}
	for _, user := range []string{"http_term_alice", "http_term_bob"} {
		response := post("/db/nornic/tx", user, "CREATE (:OwnTermination)", nil)
		path := strings.TrimSuffix(response.Commit, "/commit")
		if pos := strings.Index(path, "/db/"); pos >= 0 {
			path = path[pos:]
		}
		t.Cleanup(func() { makeRequest(t, s, http.MethodDelete, path, nil, token(user)) })
	}
	listed := makeRequest(t, s, http.MethodPost, "/db/nornic/tx/commit", map[string]any{"statements": []map[string]any{{"statement": "SHOW TRANSACTIONS YIELD transactionId, username, authenticatedUser, executingUser WHERE username STARTS WITH 'http_term_' RETURN transactionId, username, authenticatedUser, executingUser"}}}, admin)
	var listing TransactionResponse
	require.NoError(t, json.Unmarshal(listed.Body.Bytes(), &listing))
	require.Empty(t, listing.Errors, listed.Body.String())
	require.Len(t, listing.Results, 1)
	for _, item := range listing.Results[0].Data {
		require.Equal(t, item.Row[1], item.Row[2], "ordinary login is the authenticated user")
		require.Equal(t, item.Row[1], item.Row[3], "ordinary login is the executing user")
		ids[item.Row[1].(string)] = item.Row[0].(string)
	}
	require.Len(t, ids, 2, "BEGIN must register each authenticated executing user")
	denied := post("/db/nornic/tx/commit", "http_term_alice", "TERMINATE TRANSACTION $id", map[string]any{"id": ids["http_term_bob"]})
	require.Equal(t, "Transaction not found.", denied.Results[0].Data[0].Row[2])
	own := post("/db/nornic/tx/commit", "http_term_alice", "TERMINATE TRANSACTION $id", map[string]any{"id": ids["http_term_alice"]})
	require.Equal(t, "Transaction terminated.", own.Results[0].Data[0].Row[2])
}

func TestHTTPStatementDenialDoesNotInitializeServices(t *testing.T) {
	server, authenticator := setupTestServer(t)
	require.NoError(t, server.dbManager.CreateDatabase("admission_readonly"))
	_, err := authenticator.CreateUser("admission_reader", "password123", []auth.Role{auth.RoleViewer})
	require.NoError(t, err)
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(context.Background(), "viewer", []string{"admission_readonly"}))
	require.NoError(t, server.privilegesStore.SavePrivilege(context.Background(), "viewer", "admission_readonly", true, false))
	token := "Bearer " + getAuthToken(t, authenticator, "admission_reader")
	before := server.db.GetDatabaseSearchStatus("admission_readonly")
	for _, suffix := range []string{"/tx/commit", "/tx"} {
		recorder := makeRequest(t, server, http.MethodPost, "/db/admission_readonly"+suffix,
			map[string]any{"statements": []map[string]any{{"statement": "CREATE (:Denied)"}}}, token)
		var response TransactionResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
		require.Len(t, response.Errors, 1, recorder.Body.String())
		require.Equal(t, "Neo.ClientError.Security.Forbidden", response.Errors[0].Code)
		server.executorsMu.RLock()
		require.Nil(t, server.executors["admission_readonly"])
		server.executorsMu.RUnlock()
		require.Equal(t, before, server.db.GetDatabaseSearchStatus("admission_readonly"))
	}
}
