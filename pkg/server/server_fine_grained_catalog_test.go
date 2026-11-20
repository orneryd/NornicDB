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

func TestFineGrainedCatalogHTTPStaging(t *testing.T) {
	server, users := setupTestServer(t)
	admin := getAuthToken(t, users, "admin")
	reader := getAuthToken(t, users, "reader")
	require.NoError(t, server.roleStore.CreateRole(t.Context(), "embedding-operator"))
	router := server.buildRouter()

	request := func(method, token string, body []byte) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, "/auth/fgac/catalog", bytes.NewReader(body))
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		return response
	}
	read := func() fineGrainedCatalogResponse {
		t.Helper()
		response := request(http.MethodGet, admin, nil)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		var document fineGrainedCatalogResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &document))
		return document
	}

	require.Equal(t, http.StatusUnauthorized, request(http.MethodGet, "", nil).Code)
	require.Equal(t, http.StatusForbidden, request(http.MethodGet, reader, nil).Code)
	before := read()
	require.False(t, before.Enforced)
	require.Empty(t, before.Revision)
	require.Contains(t, before.Actions, auth.FineGrainedAction{
		ID: "embedding.failures.retry", ResourceKind: auth.FineGrainedDatabase,
	})
	require.Contains(t, before.Actions, auth.FineGrainedAction{
		ID: "knowledge.promotion_policy.update", ResourceKind: auth.FineGrainedDatabase,
	})
	catalog := auth.FineGrainedCatalog{
		Definitions: []auth.FineGrainedEntitlement{{
			ID: "retry-reports", Action: "embedding.failures.retry",
			Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase, DatabaseID: "reports-id"},
		}},
		Assignments: map[string][]string{"embedding-operator": {"retry-reports"}},
	}
	body, err := json.Marshal(map[string]any{"catalog": catalog, "revision": before.Revision})
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, request(http.MethodPut, reader, body).Code)
	require.Empty(t, read().Revision)
	response := request(http.MethodPut, admin, body)
	require.Equal(t, http.StatusNoContent, response.Code, response.Body.String())

	after := read()
	require.NotEmpty(t, after.Revision)
	require.Equal(t, catalog, after.Catalog)
	require.False(t, after.Enforced)
	require.Equal(t, http.StatusConflict, request(http.MethodPut, admin, body).Code)
	require.Equal(t, http.StatusBadRequest, request(http.MethodPut, admin, []byte(`{"catalog":{}}`)).Code)
	for _, invalid := range [][]byte{
		bytes.Replace(body, []byte(`"databaseId"`), []byte(`"databseId"`), 1),
		append(bytes.Clone(body), []byte(` {}`)...),
	} {
		require.Equal(t, http.StatusBadRequest, request(http.MethodPut, admin, invalid).Code)
	}
	require.Equal(t, after.Revision, read().Revision)
	require.Empty(t, auth.PermissionsForRole("embedding-operator", server.roleEntitlementsStore))

	// The legacy role endpoints intentionally allow preparation in no-auth
	// mode. Staging an FGAC policy still requires an attributable administrator.
	server.auth = nil
	require.Equal(t, http.StatusUnauthorized, request(http.MethodPut, "", body).Code)
}
