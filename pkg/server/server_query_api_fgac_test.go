package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFineGrainedQueryAPIImpersonation(t *testing.T) {
	server, users := setupTestServer(t)
	require.NoError(t, server.initializeFineGrainedRuntime(t.Context()))
	router := server.buildRouter()
	admin := getAuthToken(t, users, "admin")
	reader := getAuthToken(t, users, "reader")
	run := func(database, token, statement, target string) map[string]any {
		t.Helper()
		body := map[string]any{"statement": statement}
		if target != "" {
			body["impersonatedUser"] = target
		}
		return callFineGrainedQueryAPI(t, router, "/db/"+database+"/query/v2", token, body)
	}
	database := server.dbManager.DefaultDatabaseName()
	for _, statement := range []string{
		"CREATE ROLE query_api_visible",
		"GRANT ROLE query_api_visible TO reader",
		"GRANT TRAVERSE ON GRAPH " + database + " NODES Article TO query_api_visible",
		"GRANT READ {title} ON GRAPH " + database + " NODES Article TO query_api_visible",
	} {
		require.NotContains(t, run("system", admin, statement, ""), "errors", statement)
	}
	require.NotContains(t, run(database, admin, "CREATE (:Article {title: 'Visible', draft: true})", ""), "errors")

	denied := run("system", reader, "SHOW CURRENT USER", "admin")
	require.NotContains(t, denied, "data")
	require.Equal(t, "Neo.ClientError.Security.Forbidden", denied["errors"].([]any)[0].(map[string]any)["code"])
	target := run("system", admin, "SHOW CURRENT USER YIELD user RETURN user", "reader")
	require.NotContains(t, target, "errors")
	require.Equal(t, []any{[]any{"reader"}}, target["data"].(map[string]any)["values"])

	result := run(database, admin, "MATCH (n:Article) RETURN n", "reader")
	require.NotContains(t, result, "errors")
	node := result["data"].(map[string]any)["values"].([]any)[0].([]any)[0].(map[string]any)
	require.NotEmpty(t, node["elementId"])
	require.Equal(t, []any{"Article"}, node["labels"])
	require.Equal(t, map[string]any{"title": "Visible"}, node["properties"])
	empty := run(database, admin, "MATCH (n:Article) WHERE false RETURN n.title AS title", "reader")
	require.NotContains(t, empty, "errors")
	require.Equal(t, map[string]any{"fields": []any{"title"}, "values": []any{}}, empty["data"])
	entityPath := "/auth/fgac/entity?element_id=" + url.QueryEscape(node["elementId"].(string))
	resolved := makeRequest(t, server, http.MethodGet, entityPath, nil, "Bearer "+admin)
	require.Equal(t, http.StatusOK, resolved.Code, resolved.Body.String())
	var selected map[string]string
	require.NoError(t, json.Unmarshal(resolved.Body.Bytes(), &selected))
	databaseID, _ := server.dbManager.DatabaseIdentity(database)
	require.Equal(t, databaseID, selected["databaseId"])
	require.NotEmpty(t, selected["entityId"])
	deniedLookup := makeRequest(t, server, http.MethodGet, entityPath, nil, "Bearer "+reader)
	require.Equal(t, http.StatusForbidden, deniedLookup.Code)

	path := "/db/" + database + "/query/v2/tx"
	opened := callFineGrainedQueryAPI(t, router, path, admin, map[string]any{"impersonatedUser": "reader"})
	require.NotContains(t, opened, "errors")
	require.NotContains(t, opened, "data")
	transactionID := opened["transaction"].(map[string]any)["id"].(string)
	txPath := path + "/" + transactionID
	used := callFineGrainedQueryAPI(t, router, txPath, admin, map[string]any{"statement": "MATCH (n:Article) RETURN n.title, n.draft"})
	require.NotContains(t, used, "errors")
	require.Equal(t, []any{[]any{"Visible", nil}}, used["data"].(map[string]any)["values"])

	mismatch := callFineGrainedQueryAPI(t, router, txPath, admin, map[string]any{"impersonatedUser": "admin", "statement": "RETURN 1"})
	require.Equal(t, "Neo.ClientError.Security.Forbidden", mismatch["errors"].([]any)[0].(map[string]any)["code"])
	closed := callFineGrainedQueryAPI(t, router, txPath+"/commit", admin, map[string]any{})
	require.NotContains(t, closed, "errors")
	require.NotContains(t, closed, "data")
	require.NotEmpty(t, closed["bookmarks"])

	require.NotContains(t, run("system", admin, "ALTER USER reader SET PASSWORD CHANGE REQUIRED", ""), "errors")
	expired := run(database, reader, "MATCH (n:Article) RETURN n.title", "")
	require.Equal(t, "Neo.ClientError.Security.CredentialsExpired", expired["errors"].([]any)[0].(map[string]any)["code"])
	require.NotContains(t, run("system", admin, "ALTER USER reader SET PASSWORD CHANGE NOT REQUIRED", ""), "errors")
	require.NotContains(t, run(database, reader, "MATCH (n:Article) RETURN n.title", ""), "errors")

}

func TestFineGrainedNativeTagPropertyRules(t *testing.T) {
	server, users := setupTestServer(t)
	require.NoError(t, server.initializeFineGrainedRuntime(t.Context()))
	server.fineGrainedRuntime.SetNativeTagAuthorization(true)
	router := server.buildRouter()
	admin := getAuthToken(t, users, "admin")
	reader := getAuthToken(t, users, "reader")
	database := server.dbManager.DefaultDatabaseName()
	run := func(database, token, statement string) map[string]any {
		t.Helper()
		return callFineGrainedQueryAPI(t, router, "/db/"+database+"/query/v2", token, map[string]any{"statement": statement})
	}
	require.NotContains(t, run(database, admin, "CREATE (:Article {title: 'Public', classification: 'public'}), (:Article {title: 'Internal', classification: 'internal'})"), "errors")
	for _, statement := range []string{
		"CREATE ROLE tagged_articles",
		"GRANT ROLE tagged_articles TO reader",
		"CYPHER 25 GRANT MATCH {title} ON GRAPH " + database + " FOR (n:Article) WHERE n.classification IN abac.native.user_tags() TO tagged_articles",
	} {
		require.NotContains(t, run("system", admin, statement), "errors", statement)
	}
	query := "MATCH (n:Article) RETURN n.title, n.classification"
	result := run(database, reader, query)
	require.NotContains(t, result, "errors")
	require.Empty(t, result["data"].(map[string]any)["values"])
	require.NotContains(t, run("system", admin, "CYPHER 25 ALTER USER reader SET TAGS ['public']"), "errors")
	result = run(database, reader, query)
	require.NotContains(t, result, "errors")
	require.Equal(t, []any{[]any{"Public", nil}}, result["data"].(map[string]any)["values"])
	reject := run("system", admin, "CYPHER 25 DENY MATCH {title} ON GRAPH "+database+" FOR (n:Article) WHERE n.classification IN abac.native.user_tags() TO tagged_articles")
	require.Contains(t, reject, "errors")
}

func callFineGrainedQueryAPI(t *testing.T, router http.Handler, path, token string, body map[string]any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
	var result map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
	return result
}
