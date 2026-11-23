package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/orneryd/nornicdb/pkg/audit"
	"github.com/stretchr/testify/require"
)

func TestFineGrainedQueryAPIImpersonation(t *testing.T) {
	server, users := setupTestServer(t)
	require.NoError(t, server.initializeFineGrainedRuntime(t.Context()))
	router := server.buildRouter()
	admin := getAuthToken(t, users, "admin")
	reader := getAuthToken(t, users, "reader")
	adminClaims, err := users.ValidateToken(admin)
	require.NoError(t, err)
	readerClaims, err := users.ValidateToken(reader)
	require.NoError(t, err)
	var auditEvents bytes.Buffer
	logger := audit.NewLoggerWithWriter(&auditEvents, audit.DefaultConfig())
	server.SetAuditLogger(logger)
	t.Cleanup(func() { require.NoError(t, logger.Close()) })
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
		"CREATE ROLE query_api_private",
		"CREATE USER query_api_private SET PASSWORD 'query-api-fixture-password' CHANGE NOT REQUIRED",
		"GRANT ROLE query_api_private TO query_api_private",
		"CREATE USER query_api_admin SET PASSWORD 'query-api-admin-fixture-password' CHANGE NOT REQUIRED",
		"GRANT ROLE admin TO query_api_admin",
		"GRANT ACCESS ON DATABASE " + database + " TO query_api_private",
		"GRANT TRAVERSE ON GRAPH " + database + " NODES PrivateArticle TO query_api_private",
		"GRANT READ {title} ON GRAPH " + database + " NODES PrivateArticle TO query_api_private",
	} {
		require.NotContains(t, run("system", admin, statement, ""), "errors", statement)
	}
	require.NotContains(t, run(database, admin, "CREATE (:Article {title: 'Visible', draft: true}), (:PrivateArticle {title: 'Private', draft: true})", ""), "errors")
	for _, example := range []struct {
		target string
		rows   []any
	}{
		{"", []any{[]any{"Private", true}, []any{"Visible", true}}},
		{"reader", []any{[]any{"Visible", nil}}},
		{"query_api_private", []any{[]any{"Private", nil}}},
		{"reader", []any{[]any{"Visible", nil}}},
		{"", []any{[]any{"Private", true}, []any{"Visible", true}}},
	} {
		// Use the same statement across ordinary and impersonated requests.
		response := run(database, admin, "MATCH (n) RETURN n.title, n.draft ORDER BY n.title", example.target)
		require.NotContains(t, response, "errors", example.target)
		require.Equal(t, example.rows, response["data"].(map[string]any)["values"], example.target)
	}

	for _, path := range []string{"/db/system/query/v2", "/db/system/query/v2/tx"} {
		denied := callFineGrainedQueryAPIStatus(t, router, path, reader,
			map[string]any{"statement": "SHOW CURRENT USER", "impersonatedUser": "admin"}, http.StatusBadRequest)
		require.NotContains(t, denied, "data")
		require.NotContains(t, denied, "transaction")
		require.NotContains(t, denied, "bookmarks")
		require.Equal(t, "Neo.ClientError.Security.Forbidden", denied["errors"].([]any)[0].(map[string]any)["code"])
	}
	administration := callFineGrainedQueryAPIStatus(t, router, "/db/system/query/v2", admin,
		map[string]any{"statement": "CREATE ROLE query_api_forbidden_role", "impersonatedUser": "query_api_admin"}, http.StatusBadRequest)
	require.NotContains(t, administration, "data")
	require.NotContains(t, administration, "bookmarks")
	require.Equal(t, "Neo.ClientError.Security.Forbidden", administration["errors"].([]any)[0].(map[string]any)["code"])
	listed := run("system", admin, "SHOW ROLES YIELD role WHERE role = 'query_api_forbidden_role' RETURN role", "")
	require.NotContains(t, listed, "errors")
	require.Empty(t, listed["data"].(map[string]any)["values"])

	target := run("system", admin, "SHOW CURRENT USER YIELD user RETURN user", "reader")
	require.NotContains(t, target, "errors")
	require.Equal(t, []any{[]any{"reader"}}, target["data"].(map[string]any)["values"])
	require.NotContains(t, target, "queryType")
	require.NotContains(t, target, "resultAvailableAfter")
	require.NotContains(t, target, "resultConsumedAfter")

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
	owner := "query-v2:" + transactionOwnerKey(nil, adminClaims)
	session, found := server.txSessions.GetForOwner(transactionID, owner)
	require.True(t, found)
	require.Equal(t, readerClaims.Sub, session.ClientPolicy.Principal)
	readerOwner := "query-v2:" + transactionOwnerKey(nil, readerClaims)
	lease, acquired := server.txSessions.AcquireForOwner(transactionID, readerOwner)
	require.False(t, acquired)
	require.Nil(t, lease)
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

	require.NotContains(t, run("system", admin, "GRANT IMPERSONATE (query_api_private) ON DBMS TO query_api_visible", ""), "errors")
	delegatedSession := callFineGrainedQueryAPI(t, router, path, reader, map[string]any{"impersonatedUser": "query_api_private"})
	require.NotContains(t, delegatedSession, "errors")
	delegatedTx := path + "/" + delegatedSession["transaction"].(map[string]any)["id"].(string)
	require.NotContains(t, run("system", admin, "REVOKE GRANT IMPERSONATE (query_api_private) ON DBMS FROM query_api_visible", ""), "errors")
	revoked := callFineGrainedQueryAPI(t, router, delegatedTx, reader, map[string]any{"statement": "MATCH (n:PrivateArticle) RETURN n.title"})
	require.Equal(t, "Neo.ClientError.Security.Forbidden", revoked["errors"].([]any)[0].(map[string]any)["code"])
	require.NotContains(t, revoked, "data")
	require.NotContains(t, revoked, "transaction")
	require.NotContains(t, revoked, "bookmarks")

	require.NotContains(t, run("system", admin, "ALTER USER reader SET PASSWORD CHANGE REQUIRED", ""), "errors")
	expired := run(database, reader, "MATCH (n:Article) RETURN n.title", "")
	require.Equal(t, "Neo.ClientError.Security.CredentialsExpired", expired["errors"].([]any)[0].(map[string]any)["code"])
	delegated := run(database, admin, "MATCH (n:Article) RETURN n.title", "reader")
	require.NotContains(t, delegated, "errors")
	require.Equal(t, []any{[]any{"Visible"}}, delegated["data"].(map[string]any)["values"])
	require.NotContains(t, run("system", admin, "ALTER USER reader SET PASSWORD CHANGE NOT REQUIRED", ""), "errors")
	require.NotContains(t, run(database, reader, "MATCH (n:Article) RETURN n.title", ""), "errors")

	var attributed bool
	events := json.NewDecoder(&auditEvents)
	for events.More() {
		var event audit.Event
		require.NoError(t, events.Decode(&event))
		if event.Type == "impersonation" && event.Success && event.UserID == adminClaims.Sub &&
			event.Reason == "target=reader target_id="+readerClaims.Sub {
			attributed = true
		}
	}
	require.True(t, attributed, "successful impersonation identifies the authenticated actor and executing account")
}

func TestFineGrainedQueryAPIImpersonationCurrentProfile(t *testing.T) {
	t.Setenv("NORNICDB_QUERY_API_COMPATIBILITY", "2026.10")
	server, users := setupTestServer(t)
	require.NoError(t, server.initializeFineGrainedRuntime(t.Context()))
	router := server.buildRouter()
	admin := getAuthToken(t, users, "admin")
	reader := getAuthToken(t, users, "reader")
	path := "/db/system/query/v2"
	success := callFineGrainedQueryAPI(t, router, path, admin, map[string]any{
		"statement": "SHOW CURRENT USER YIELD user RETURN user", "impersonatedUser": "reader",
	})
	require.NotContains(t, success, "errors")
	require.Equal(t, []any{[]any{"reader"}}, success["data"].(map[string]any)["values"])
	require.Equal(t, "s", success["queryType"])
	require.GreaterOrEqual(t, success["resultAvailableAfter"].(float64), float64(0))
	require.GreaterOrEqual(t, success["resultConsumedAfter"].(float64), float64(0))

	denied := callFineGrainedQueryAPIStatus(t, router, path, reader, map[string]any{
		"statement": "SHOW CURRENT USER", "impersonatedUser": "admin",
	}, http.StatusUnauthorized)
	require.NotContains(t, denied, "data")
	require.NotContains(t, denied, "transaction")
	require.NotContains(t, denied, "bookmarks")
	require.Equal(t, "Neo.ClientError.Security.Forbidden", denied["errors"].([]any)[0].(map[string]any)["code"])
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
	return callFineGrainedQueryAPIStatus(t, router, path, token, body, http.StatusAccepted)
}

func callFineGrainedQueryAPIStatus(t *testing.T, router http.Handler, path, token string, body map[string]any, status int) map[string]any {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	require.Equal(t, status, response.Code, response.Body.String())
	var result map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
	return result
}
