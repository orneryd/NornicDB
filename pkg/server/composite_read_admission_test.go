package server

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCompositeConstituentReadAdmission(t *testing.T) {
	server, users := setupTestServer(t)
	ctx := context.Background()
	for _, database := range []string{"read_open", "read_closed"} {
		require.NoError(t, server.dbManager.CreateDatabase(database))
		engine, err := server.dbManager.GetStorage(database)
		require.NoError(t, err)
		_, err = engine.CreateNode(&storage.Node{ID: "secret", Labels: []string{"CompositeRead"}})
		require.NoError(t, err)
	}
	require.NoError(t, server.dbManager.CreateCompositeDatabase("read_portfolio", []multidb.ConstituentRef{
		{Alias: "open", DatabaseName: "read_open", Type: "local", AccessMode: "read_write"},
		{Alias: "closed", DatabaseName: "read_closed", Type: "local", AccessMode: "read_write"},
	}))
	for _, role := range []string{"composite_partial", "composite_reader"} {
		require.NoError(t, server.roleStore.CreateRole(ctx, role))
		require.NoError(t, server.roleEntitlementsStore.Set(ctx, role, []string{"read", "write"}))
		require.NoError(t, server.allowlistStore.SaveRoleDatabases(ctx, role, []string{"read_portfolio", "read_open", "read_closed"}))
		for _, database := range []string{"read_portfolio", "read_open", "read_closed"} {
			require.NoError(t, server.privilegesStore.SavePrivilege(ctx, role, database, role == "composite_reader" || database != "read_closed", true))
		}
		_, err := users.CreateUser(role, "password123", []auth.Role{auth.Role(role)})
		require.NoError(t, err)
		for _, route := range []struct{ path string; body any }{
			{"/db/read_portfolio/tx/commit", map[string]any{"statements": []map[string]any{{"statement": "USE read_portfolio.closed MATCH (n:CompositeRead) RETURN count(n) AS count"}}}},
			{"/graphql", map[string]any{"query": `{ cypher(input: {statement: "USE read_portfolio.closed MATCH (n:CompositeRead) RETURN count(n) AS count", database: "read_portfolio"}) { rowCount rows } }`}},
		} {
			t.Run(role+route.path, func(t *testing.T) {
				response := makeRequest(t, server, http.MethodPost, route.path, route.body, "Bearer "+getAuthToken(t, users, role))
				body := response.Body.String()
				if role == "composite_partial" {
					require.True(t, strings.Contains(body, "permission") || strings.Contains(body, "Forbidden") || strings.Contains(body, "not allowed"), "constituent read was not denied: %d %s", response.Code, body)
				} else {
					require.NotContains(t, body, "Forbidden")
					require.NotContains(t, body, "permission")
					require.NotContains(t, body, `"errors":[{`, "reader must reach actual data: %s", body)
				}
			})
		}
	}
}
