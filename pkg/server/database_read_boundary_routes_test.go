package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func setupBoundaryReadPrincipals(t *testing.T, server *Server) map[string]*auth.JWTClaims {
	t.Helper()
	ctx := context.Background()
	system, err := server.dbManager.GetStorage("system")
	require.NoError(t, err)
	server.roleStore = auth.NewRoleStore(system)
	server.allowlistStore = auth.NewAllowlistStore(system)
	server.privilegesStore = auth.NewPrivilegesStore(system)
	server.roleEntitlementsStore = auth.NewRoleEntitlementsStore(system)

	roles := []struct {
		name      string
		databases []string
		reads     map[string]bool
		writes    map[string]bool
	}{
		{name: "boundary_default_reader", databases: []string{"nornic"}, reads: map[string]bool{"nornic": true}},
		{name: "boundary_named_reader", databases: []string{"named"}, reads: map[string]bool{"named": true}},
		{name: "boundary_write_only", databases: []string{"nornic", "named"}, writes: map[string]bool{"nornic": true, "named": true}},
		{name: "boundary_no_access"},
	}
	principals := make(map[string]*auth.JWTClaims, len(roles))
	for _, role := range roles {
		require.NoError(t, server.roleStore.CreateRole(ctx, role.name))
		require.NoError(t, server.allowlistStore.SaveRoleDatabases(ctx, role.name, role.databases))
		for _, database := range []string{"nornic", "named"} {
			require.NoError(t, server.privilegesStore.SavePrivilege(ctx, role.name, database, role.reads[database], role.writes[database]))
		}
		require.NoError(t, server.roleEntitlementsStore.Set(ctx, role.name, []string{"read", "write"}))
		principals[role.name] = &auth.JWTClaims{Sub: role.name, Username: role.name, Roles: []string{role.name}}
	}
	return principals
}

func boundaryHandlerRequest(t *testing.T, method, path string, body any, claims *auth.JWTClaims, handler http.HandlerFunc) *httptest.ResponseRecorder {
	t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		encoded, err = json.Marshal(body)
		require.NoError(t, err)
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(encoded))
	req.Header.Set("Content-Type", "application/json")
	if claims != nil {
		req = req.WithContext(context.WithValue(req.Context(), contextKeyClaims, claims))
	}
	recorder := httptest.NewRecorder()
	handler(recorder, req)
	return recorder
}

func TestGDPRExportRequiresDefaultDatabaseRead(t *testing.T) {
	server, _ := setupTestServer(t)
	require.NoError(t, server.dbManager.CreateDatabase("named"))
	principals := setupBoundaryReadPrincipals(t, server)

	defaultStorage, err := server.dbManager.GetStorage("nornic")
	require.NoError(t, err)
	_, err = defaultStorage.CreateNode(&storage.Node{
		ID: "default-subject", Labels: []string{"Person"}, Properties: map[string]any{
			"owner_id": "boundary_default_reader", "secret": "default database payload",
		},
	})
	require.NoError(t, err)
	namedStorage, err := server.dbManager.GetStorage("named")
	require.NoError(t, err)
	_, err = namedStorage.CreateNode(&storage.Node{
		ID: "named-subject", Labels: []string{"Person"}, Properties: map[string]any{
			"owner_id": "boundary_default_reader", "secret": "named database payload",
		},
	})
	require.NoError(t, err)

	allowed := boundaryHandlerRequest(t, http.MethodPost, "/gdpr/export", map[string]any{
		"user_id": "boundary_default_reader", "format": "json",
	}, principals["boundary_default_reader"], server.handleGDPRExport)
	require.Equal(t, http.StatusOK, allowed.Code, allowed.Body.String())
	require.Contains(t, allowed.Body.String(), "default database payload")
	require.NotContains(t, allowed.Body.String(), "named database payload")

	for _, role := range []string{"boundary_named_reader", "boundary_write_only", "boundary_no_access"} {
		denied := boundaryHandlerRequest(t, http.MethodPost, "/gdpr/export", map[string]any{
			"user_id": role, "format": "json",
		}, principals[role], server.handleGDPRExport)
		require.Equal(t, http.StatusForbidden, denied.Code, "%s: %s", role, denied.Body.String())
		require.NotContains(t, denied.Body.String(), "payload")
	}

	server.auth = nil
	open := boundaryHandlerRequest(t, http.MethodPost, "/gdpr/export", map[string]any{
		"user_id": "boundary_default_reader", "format": "json",
	}, nil, server.handleGDPRExport)
	require.Equal(t, http.StatusOK, open.Code, open.Body.String())
	require.Contains(t, open.Body.String(), "default database payload")
}

func TestStatusAggregatesOnlyReadableDatabases(t *testing.T) {
	server, _ := setupTestServer(t)
	require.NoError(t, server.dbManager.CreateDatabase("named"))
	principals := setupBoundaryReadPrincipals(t, server)

	for _, database := range []string{"nornic", "named"} {
		engine, err := server.dbManager.GetStorage(database)
		require.NoError(t, err)
		_, err = engine.CreateNode(&storage.Node{ID: storage.NodeID(database + "-status-node"), Labels: []string{"Doc"}})
		require.NoError(t, err)
	}

	assertStatus := func(role string, wantDatabases int64) {
		t.Helper()
		response := boundaryHandlerRequest(t, http.MethodGet, "/status", nil, principals[role], server.handleStatus)
		require.Equal(t, http.StatusOK, response.Code, "%s: %s", role, response.Body.String())
		var body struct {
			Database struct {
				Nodes     int64 `json:"nodes"`
				Databases int64 `json:"databases"`
			} `json:"database"`
			Startup struct {
				Ready    int64 `json:"search_ready_databases"`
				Building int64 `json:"search_building_databases"`
			} `json:"startup"`
		}
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
		require.Equal(t, wantDatabases, body.Database.Databases, role)
		if wantDatabases == 0 {
			require.Zero(t, body.Database.Nodes, role)
		} else {
			require.LessOrEqual(t, body.Database.Nodes, wantDatabases, role)
		}
		require.LessOrEqual(t, body.Startup.Ready, wantDatabases, role)
		require.LessOrEqual(t, body.Startup.Building, wantDatabases, role)
	}

	assertStatus("boundary_default_reader", 1)
	assertStatus("boundary_named_reader", 1)
	assertStatus("boundary_write_only", 0)
	assertStatus("boundary_no_access", 0)

	server.auth = nil
	response := boundaryHandlerRequest(t, http.MethodGet, "/status", nil, nil, server.handleStatus)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var body struct {
		Database struct {
			Nodes     int64 `json:"nodes"`
			Databases int64 `json:"databases"`
		} `json:"database"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Equal(t, int64(2), body.Database.Databases)
	require.LessOrEqual(t, body.Database.Nodes, int64(2))
}
