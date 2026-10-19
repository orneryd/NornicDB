package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/graphql/models"
	"github.com/orneryd/nornicdb/pkg/graphql/resolvers"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/require"
)

func TestSelectedAliasAdmissionLiveScope(t *testing.T) {
	s, users := setupTestServer(t)
	ctx := context.Background()
	const role = "review-r3-reader"
	const alias = "review_forbidden_alias"
	require.NoError(t, s.dbManager.CreateAlias(alias, "nornic"))
	require.NoError(t, s.roleStore.CreateRole(ctx, role))
	require.NoError(t, s.roleEntitlementsStore.Set(ctx, role, []string{"read"}))
	require.NoError(t, s.allowlistStore.SaveRoleDatabases(ctx, role, []string{"nornic"}))
	require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "nornic", true, false))
	_, err := users.CreateUser(role, "password123", []auth.Role{auth.Role(role)})
	require.NoError(t, err)
	response := makeRequest(t, s, http.MethodGet, "/db/"+alias, nil, "Bearer "+getAuthToken(t, users, role))
	require.Equal(t, http.StatusForbidden, response.Code, response.Body.String())
	req := httptest.NewRequest("GET", "/api/bifrost/status", nil).WithContext(context.WithValue(ctx, contextKeyClaims, &auth.JWTClaims{Roles: []string{role}}))
	principal := s.withBifrostRBAC(req).Context()
	router := newHeimdallDBRouter(s.db, s.dbManager, nil)
	graphql := resolvers.NewResolver(s.db, s.dbManager)
	mcpDispatches := 0
	s.mcpServer.SetDatabaseScopedExecutor(func(database string) (*cypher.StorageExecutor, func(context.Context, string) (*nornicdb.Node, error), error) {
		mcpDispatches++
		require.Equal(t, "nornic", database)
		return nil, nil, nil
	})
	// Bind once while the alias is excluded; later grants and revocations must
	// affect this same predicate, without rebuilding the principal.
	canonical, boundRead := auth.BindRequestDatabaseRead(principal, alias)
	require.Equal(t, "nornic", canonical)
	token := "Bearer " + getAuthToken(t, users, role)
	for _, phase := range []struct {
		name    string
		allowed bool
	}{{"omitted", false}, {"granted", true}, {"revoked", false}} {
		names := []string{"nornic"}
		if phase.allowed {
			names = append(names, alias)
		}
		require.NoError(t, s.allowlistStore.SaveRoleDatabases(ctx, role, names))
		require.Equal(t, phase.allowed, boundRead(), "bound predicate: %s", phase.name)
		for _, selection := range []string{"nornic", alias} {
			calls := map[string]func() error{
				"MCPAdmission": func() error {
					before := mcpDispatches
					_, err := s.mcpServer.CallTool(principal, "recall", map[string]interface{}{"database": selection, "id": "missing"})
					if mcpDispatches > before {
						return nil
					}
					return err
				},
				"GraphQLCypher": func() error {
					_, e := graphql.Query().Cypher(principal, models.CypherInput{Statement: "MATCH (n) RETURN count(n)", Database: &selection})
					return e
				},
				"Scope": func() error {
					_, ok := auth.RequestDatabaseScopeFromContext(principal).Resolve(selection)
					if !ok {
						return fmt.Errorf("selection refused")
					}
					return nil
				},
				"HTTPInfo": func() error {
					r := makeRequest(t, s, http.MethodGet, "/db/"+selection, nil, token)
					if r.Code == http.StatusForbidden {
						return fmt.Errorf("selection refused")
					}
					return nil
				},
				"HTTPSearch": func() error {
					r := makeRequest(t, s, http.MethodPost, "/nornicdb/search", map[string]any{"query": "review", "database": selection}, token)
					if r.Code == http.StatusForbidden {
						return fmt.Errorf("selection refused")
					}
					return nil
				},
				"HTTPSimilarity": func() error {
					r := makeRequest(t, s, http.MethodPost, "/nornicdb/similar", map[string]any{"node_id": "missing", "database": selection}, token)
					if r.Code == http.StatusForbidden {
						return fmt.Errorf("selection refused")
					}
					return nil
				},
				"Query": func() error {
					_, e := router.Query(principal, selection, "MATCH (n) RETURN count(n) AS n", nil)
					return e
				},
				"Stats":    func() error { _, e := router.Stats(principal, selection); return e },
				"Discover": func() error { _, e := router.Discover(principal, selection, "seed", nil, 5, 0); return e },
			}
			for method, call := range calls {
				t.Run(phase.name+"/"+selection+"/"+method, func(t *testing.T) {
					err := call()
					t.Logf("selection=%s allowed=%v err=%v", selection, phase.allowed, err)
					if selection == "nornic" || phase.allowed {
						require.NoError(t, err)
					} else {
						require.Error(t, err, "forbidden alias was accepted")
					}
				})
			}
		}
	}
}
