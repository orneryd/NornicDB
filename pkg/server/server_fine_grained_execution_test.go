package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

func TestFineGrainedHTTPPrivilegeAdministration(t *testing.T) {
	server, users := setupTestServer(t)
	require.NoError(t, server.initializeFineGrainedRuntime(t.Context()))
	router := server.buildRouter()
	admin := getAuthToken(t, users, "admin")
	reader := getAuthToken(t, users, "reader")
	run := func(database, token, statement string) TransactionResponse {
		t.Helper()
		body, err := json.Marshal(TransactionRequest{Statements: []StatementRequest{{Statement: statement}}})
		require.NoError(t, err)
		request := httptest.NewRequest(http.MethodPost, "/db/"+database+"/tx/commit", bytes.NewReader(body))
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		var result TransactionResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
		return result
	}
	database := server.dbManager.DefaultDatabaseName()
	for _, statement := range []string{
		"CREATE ROLE document_reader IF NOT EXISTS",
		"GRANT ROLE document_reader TO reader",
		"GRANT ACCESS ON DATABASE " + database + " TO document_reader",
		"GRANT TRAVERSE ON GRAPH " + database + " NODES Article TO document_reader",
		"GRANT READ {title} ON GRAPH " + database + " NODES Article TO document_reader",
	} {
		result := run("system", admin, statement)
		require.Empty(t, result.Errors, statement)
	}
	result := run(database, admin, "CREATE (:Article {title: 'example', draft: true})")
	require.Empty(t, result.Errors)

	result = run(database, reader, "MATCH (n:Article) RETURN n.title, n.draft")
	require.Empty(t, result.Errors)
	require.Len(t, result.Results, 1)
	require.Len(t, result.Results[0].Data, 1)
	require.Equal(t, []any{"example", nil}, result.Results[0].Data[0].Row)

	result = run("system", admin, "SHOW ROLE document_reader PRIVILEGES")
	require.Empty(t, result.Errors)
	require.Len(t, result.Results[0].Data, 3)
	result = run("system", admin, "SHOW ROLE document_reader PRIVILEGES AS COMMANDS")
	require.Empty(t, result.Errors)
	require.Equal(t, []string{"command"}, result.Results[0].Columns)
	require.Len(t, result.Results[0].Data, 3)

	result = run("system", reader, "SHOW USER PRIVILEGES YIELD role, user")
	require.Empty(t, result.Errors)
	require.NotEmpty(t, result.Results[0].Data)
	for _, row := range result.Results[0].Data {
		require.Equal(t, "reader", row.Row[1])
	}
	result = run("system", reader, "SHOW USER admin PRIVILEGES")
	require.Len(t, result.Errors, 1)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", result.Errors[0].Code)

	result = run(database, reader, "CREATE (:Article {title: 'other'})")
	require.Len(t, result.Errors, 1)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", result.Errors[0].Code)
	result = run("system", reader, "CREATE DATABASE restricted_target")
	require.Len(t, result.Errors, 1)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", result.Errors[0].Code)
	require.False(t, server.dbManager.ExistsOrIsConstituent("restricted_target"))

	result = run("system", admin, "REVOKE GRANT READ {title} ON GRAPH "+database+" NODES Article FROM document_reader")
	require.Empty(t, result.Errors)
	result = run(database, reader, "MATCH (n:Article) RETURN n.title")
	require.Empty(t, result.Errors)
	require.Equal(t, []any{nil}, result.Results[0].Data[0].Row)

	// An explicit masked write is authorized using its authored property name.
	require.Empty(t, run("system", admin, "GRANT SET PROPERTY {draft} ON GRAPH "+database+" NODES Article TO document_reader").Errors)
	require.Empty(t, run(database, reader, "MATCH (n:Article) SET n.draft = null").Errors)
	result = run(database, admin, "MATCH (n:Article) RETURN n.draft")
	require.Empty(t, result.Errors)
	require.Equal(t, []any{nil}, result.Results[0].Data[0].Row)
	result = run(database, reader, "MATCH (n:Article) REMOVE n.title")
	require.Len(t, result.Errors, 1)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", result.Errors[0].Code)
	result = run(database, admin, "MATCH (n:Article) RETURN n.title")
	require.Empty(t, result.Errors)
	require.Equal(t, []any{"example"}, result.Results[0].Data[0].Row)

	require.Empty(t, run("system", admin, "CREATE DATABASE fgac_home").Errors)
	require.Empty(t, run("system", admin, "ALTER USER reader SET HOME DATABASE fgac_home").Errors)
	result = run("system", reader, "SHOW HOME DATABASE YIELD name")
	require.Empty(t, result.Errors)
	require.Equal(t, []any{"fgac_home"}, result.Results[0].Data[0].Row)
	require.Empty(t, run("system", admin, "GRANT STOP ON DATABASE fgac_home TO document_reader").Errors)
	result = run("system", reader, "STOP DATABASE fgac_home")
	require.Empty(t, result.Errors)
	result = run("system", reader, "START DATABASE fgac_home")
	require.Len(t, result.Errors, 1)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", result.Errors[0].Code)
	require.Empty(t, run("system", admin, "START DATABASE fgac_home").Errors)
	require.Empty(t, run("system", admin, "ALTER USER reader REMOVE HOME DATABASE").Errors)
	result = run("system", reader, "SHOW HOME DATABASE YIELD name")
	require.Empty(t, result.Errors)
	require.Equal(t, []any{database}, result.Results[0].Data[0].Row)

	request := httptest.NewRequest(http.MethodGet, "/auth/fgac/catalog", nil)
	request.Header.Set("Authorization", "Bearer "+admin)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var document fineGrainedCatalogResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &document))
	require.True(t, document.Enforced)
	require.Contains(t, document.Catalog.Assignments, string(auth.RoleAdmin))
}

// TestFineGrainedRoleWorkspaceOperations covers the statements sent by the
// Database Access workspace, including catalog assignment after a role rename.
func TestFineGrainedRoleWorkspaceOperations(t *testing.T) {
	server, users := setupTestServer(t)
	require.NoError(t, server.initializeFineGrainedRuntime(t.Context()))
	admin := "Bearer " + getAuthToken(t, users, "admin")
	reader := "Bearer " + getAuthToken(t, users, "reader")
	params := map[string]any{"role": "report_viewer", "name": "report_reader", "user": "reader"}
	run := func(token, statement string) TransactionResponse {
		t.Helper()
		response := makeRequest(t, server, http.MethodPost, "/db/system/tx/commit", TransactionRequest{
			Statements: []StatementRequest{{Statement: statement, Parameters: params}},
		}, token)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		var result TransactionResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
		return result
	}
	readCatalog := func() fineGrainedCatalogResponse {
		t.Helper()
		response := makeRequest(t, server, http.MethodGet, "/auth/fgac/catalog", nil, admin)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		var document fineGrainedCatalogResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &document))
		return document
	}
	require.Empty(t, run(admin, "CREATE ROLE $role").Errors)
	document := readCatalog()
	require.Contains(t, document.Roles, "report_viewer")
	database := server.dbManager.DefaultDatabaseName()
	identifier, _ := server.dbManager.DatabaseIdentity(database)
	require.NotEmpty(t, identifier)
	definition := auth.FineGrainedEntitlement{
		ID: "report_title", Action: "graph.node.property.read",
		Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, DatabaseID: identifier, Types: []string{"Report"}, Properties: []string{"title"}},
	}
	document.Catalog.Definitions = append(document.Catalog.Definitions, definition)
	document.Catalog.Assignments["report_viewer"] = []string{definition.ID}
	response := makeRequest(t, server, http.MethodPut, "/auth/fgac/catalog",
		map[string]any{"catalog": document.Catalog, "revision": document.Revision}, admin)
	require.Equal(t, http.StatusNoContent, response.Code, response.Body.String())
	require.Empty(t, run(admin, "GRANT ROLE $role TO $user").Errors)
	require.Empty(t, run(admin, "RENAME ROLE $role TO $name").Errors)
	current := readCatalog()
	require.NotContains(t, current.Roles, "report_viewer")
	require.Contains(t, current.Roles, "report_reader")
	require.Equal(t, []string{definition.ID}, current.Catalog.Assignments["report_reader"])
	result := run(admin, "SHOW ROLES WITH USERS YIELD role, member WHERE role = 'report_reader'")
	require.Empty(t, result.Errors)
	require.Len(t, result.Results, 1)
	require.Len(t, result.Results[0].Data, 1)
	require.Equal(t, []any{"report_reader", "reader"}, result.Results[0].Data[0].Row)

	require.Empty(t, run(admin, "REVOKE ROLE $name FROM $user").Errors)
	forbidden := run(reader, "GRANT ROLE $name TO $user")
	require.Len(t, forbidden.Errors, 1)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", forbidden.Errors[0].Code)
	require.Empty(t, run(admin, "DROP ROLE $name").Errors)
	require.NotContains(t, readCatalog().Roles, "report_reader")
}
