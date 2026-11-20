package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFineGrainedCatalogHTTPEdit(t *testing.T) {
	server, users := setupTestServer(t)
	admin := getAuthToken(t, users, "admin")
	reader := getAuthToken(t, users, "reader")
	require.NoError(t, server.roleStore.CreateRole(t.Context(), "embedding-operator"))
	require.NoError(t, server.initializeFineGrainedRuntime(t.Context()))
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
	require.True(t, before.Enforced)
	require.NotEmpty(t, before.Revision)
	require.Contains(t, before.Actions, auth.FineGrainedAction{
		ID: "embedding.failures.retry", ResourceKind: auth.FineGrainedDatabase,
	})
	require.Contains(t, before.Actions, auth.FineGrainedAction{
		ID: "knowledge.promotion_policy.update", ResourceKind: auth.FineGrainedDatabase,
	})
	catalog := before.Catalog
	catalog.Definitions = append(catalog.Definitions, auth.FineGrainedEntitlement{
		ID: "retry-reports", Action: "embedding.failures.retry",
		Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase, DatabaseID: "reports-id"},
	})
	catalog.Assignments["embedding-operator"] = []string{"retry-reports"}
	body, err := json.Marshal(map[string]any{"catalog": catalog, "revision": before.Revision})
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, request(http.MethodPut, reader, body).Code)
	require.Equal(t, before.Revision, read().Revision)
	response := request(http.MethodPut, admin, body)
	require.Equal(t, http.StatusNoContent, response.Code, response.Body.String())

	after := read()
	require.NotEmpty(t, after.Revision)
	require.Equal(t, catalog, after.Catalog)
	require.True(t, after.Enforced)
	require.Equal(t, http.StatusConflict, request(http.MethodPut, admin, body).Code)
	policyWithoutAdmin := catalog
	policyWithoutAdmin.Assignments = map[string][]string{"embedding-operator": {"retry-reports"}}
	withoutAdmin, err := json.Marshal(map[string]any{"catalog": policyWithoutAdmin, "revision": after.Revision})
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, request(http.MethodPut, admin, withoutAdmin).Code)
	require.Equal(t, http.StatusBadRequest, request(http.MethodPut, admin, []byte(`{"catalog":{}}`)).Code)
	for _, invalid := range [][]byte{
		bytes.Replace(body, []byte(`"databaseId"`), []byte(`"databseId"`), 1),
		append(bytes.Clone(body), []byte(` {}`)...),
	} {
		require.Equal(t, http.StatusBadRequest, request(http.MethodPut, admin, invalid).Code)
	}
	require.Equal(t, after.Revision, read().Revision)
	require.Empty(t, auth.PermissionsForRole("embedding-operator", server.roleEntitlementsStore))

}

func TestFineGrainedDisabledKeepsCatalogOutOfRBAC(t *testing.T) {
	server, users := setupTestServer(t)
	admin := "Bearer " + getAuthToken(t, users, "admin")
	system, err := server.dbManager.GetStorage("system")
	require.NoError(t, err)
	_, err = system.CreateNode(t.Context(), &storage.Node{
		ID:         "fine_grained_entitlement_catalog",
		Labels:     []string{"_System", "_FineGrainedEntitlementCatalog"},
		Properties: map[string]any{"catalog": "{malformed"},
	})
	require.NoError(t, err)
	require.Nil(t, server.FineGrainedAdmission())
	me := makeRequest(t, server, http.MethodGet, "/auth/me", nil, admin)
	require.Equal(t, http.StatusOK, me.Code)
	require.NotContains(t, me.Body.String(), "fgac_enabled")
	for _, path := range []string{"/auth/fgac/catalog", "/auth/fgac/entity", "/auth/fgac/effective"} {
		require.Equal(t, http.StatusNotFound, makeRequest(t, server, http.MethodGet, path, nil, admin).Code, path)
	}
	require.Equal(t, http.StatusCreated, makeRequest(t, server, http.MethodPost, "/auth/roles", map[string]string{"name": "operator"}, admin).Code)
	require.Equal(t, http.StatusOK, makeRequest(t, server, http.MethodPatch, "/auth/roles/operator", map[string]string{"name": "renamed"}, admin).Code)
	require.Equal(t, http.StatusOK, makeRequest(t, server, http.MethodDelete, "/auth/roles/renamed", nil, admin).Code)
	persisted, err := system.GetNode(t.Context(), "fine_grained_entitlement_catalog")
	require.NoError(t, err)
	require.Equal(t, "{malformed", persisted.Properties["catalog"])
}
