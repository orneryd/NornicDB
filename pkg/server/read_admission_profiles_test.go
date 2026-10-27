package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

func TestReadAdmissionRemoteCompositeProfiles(t *testing.T) {
	s, users := setupTestServer(t)
	ctx := context.Background()
	require.NoError(t, s.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "read_local"}))
	require.NoError(t, s.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "remote_shadow"}))
	require.NoError(t, s.dbManager.CreateAlias(t.Context(), "read_remote", "remote_shadow"))
	require.NoError(t, s.dbManager.CreateCompositeDatabase(t.Context(), "read_remote_only", []multidb.ConstituentRef{
		{Alias: "remote", DatabaseName: "read_remote", Type: "remote", AccessMode: "read", URI: "http://127.0.0.1:1"},
	}))
	require.NoError(t, s.dbManager.CreateCompositeDatabase(t.Context(), "read_mixed", []multidb.ConstituentRef{
		{Alias: "local", DatabaseName: "read_local", Type: "local", AccessMode: "read"},
		{Alias: "remote", DatabaseName: "read_remote", Type: "remote", AccessMode: "read", URI: "http://127.0.0.1:1"},
	}))
	const role = "mixed-reader"
	require.NoError(t, s.roleStore.CreateRole(ctx, role))
	require.NoError(t, s.roleEntitlementsStore.Set(ctx, role, []string{"read"}))
	_, err := users.CreateUser(role, "password123", []auth.Role{auth.Role(role)})
	require.NoError(t, err)
	claims := &auth.JWTClaims{Roles: []string{role}}
	var liveRead func() bool
	for _, phase := range []struct {
		name         string
		access, read bool
	}{
		{"allowed", true, true}, {"read-refused", true, false}, {"access-refused", false, true},
	} {
		t.Run(phase.name, func(t *testing.T) {
			names := []string{"nornic", "read_mixed", "read_remote_only", "read_local"}
			if phase.access {
				names = append(names, "read_remote")
			}
			require.NoError(t, s.allowlistStore.SaveRoleDatabases(ctx, role, names))
			for _, database := range []string{"nornic", "read_mixed", "read_remote_only", "read_local", "read_remote", "remote_shadow"} {
				readable := database != "read_remote" || phase.read
				require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, database, readable, false))
			}
			want := phase.access && phase.read
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req = req.WithContext(context.WithValue(req.Context(), contextKeyClaims, claims))
			bound := s.withBifrostRBAC(req)
			if liveRead == nil {
				_, liveRead = auth.BindRequestDatabaseRead(bound.Context(), "read_mixed")
			}
			require.Equal(t, want, liveRead(), "bound root predicate must observe policy changes")
			accessible, readable := s.databaseReadAccess(claims, "system")
			require.False(t, accessible)
			require.False(t, readable)
			require.Equal(t, want, auth.RequestDatabasePermissionAllowed(bound.Context(), "read_mixed", auth.PermRead), "request callback")
			for _, database := range []string{"read_mixed", "read_mixed.remote", "read_remote_only", "read_remote_only.remote"} {
				_, readable := s.databaseReadAccess(claims, database)
				require.Equal(t, want, readable, database)
				require.Equal(t, want, s.grpcDatabaseAccessForRoles(claims.Roles, database).Read, "grpc "+database)
			}
			response := makeRequest(t, s, http.MethodGet, "/db/read_mixed", nil, "Bearer "+getAuthToken(t, users, role))
			if want {
				require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			} else {
				require.Equal(t, http.StatusForbidden, response.Code, response.Body.String())
			}
		})
	}
	for _, profile := range []string{"admin", "auth-off"} {
		t.Run(profile, func(t *testing.T) {
			token := "Bearer " + getAuthToken(t, users, "admin")
			if profile == "auth-off" {
				saved := s.auth
				s.auth = nil
				defer func() { s.auth = saved }()
				token = ""
			}
			principal := &auth.JWTClaims{Roles: []string{"admin"}}
			if profile == "auth-off" {
				principal = nil
			}
			for _, database := range []string{"nornic", "system", "read_local", "read_mixed", "read_mixed.remote", "read_remote_only"} {
				selectable, readable := s.databaseReadAccess(principal, database)
				require.True(t, selectable, database)
				require.True(t, readable, database)
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				req = req.WithContext(context.WithValue(req.Context(), contextKeyClaims, principal))
				if database != "read_mixed.remote" {
					require.True(t, auth.RequestDatabasePermissionAllowed(s.withBifrostRBAC(req).Context(), database, auth.PermRead), database)
				}
			}
			response := makeRequest(t, s, http.MethodGet, "/db/read_mixed", nil, token)
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			var payload map[string]any
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &payload))
			require.Equal(t, true, payload["statsPartial"])
			require.NotEmpty(t, payload["constituents"])
		})
	}
}
