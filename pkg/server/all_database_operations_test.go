package server

import (
	"context"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Reuse one authenticated token while changing only the system privilege.
// Every operation below consumes unpartitioned storage or worker state.
func TestAllDatabaseOperationsIncludeSystemAdmission(t *testing.T) {
	s, users := setupTestServer(t)
	ctx := context.Background()
	const role = "all-database-operator"
	require.NoError(t, s.roleStore.CreateRole(ctx, role))
	require.NoError(t, s.roleEntitlementsStore.Set(ctx, role, []string{"read", "write", "admin"}))
	require.NoError(t, s.allowlistStore.SaveRoleDatabases(ctx, role, []string{"nornic", "system"}))
	require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "nornic", true, true))
	_, err := users.CreateUser(role, "password123", []auth.Role{auth.Role(role)})
	require.NoError(t, err)
	token := "Bearer " + getAuthToken(t, users, role)
	vectors := [][]float32{{1, 0, 0}}
	_, err = s.db.GetStorage().CreateNode(&storage.Node{ID: "population-vector", Labels: []string{"Seed"}, ChunkEmbeddings: vectors})
	require.NoError(t, err)
	for _, readable := range []bool{false, true, false} {
		require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "system", readable, readable))
		for _, operation := range []struct{ method, path string }{
			{http.MethodPost, "/admin/restore"},
			{http.MethodGet, "/nornicdb/embed/stats"},
			{http.MethodGet, "/nornicdb/embed/failures"},
			{http.MethodPost, "/nornicdb/embed/retry-failures"},
			{http.MethodPost, "/nornicdb/embed/trigger"},
		} {
			response := makeRequest(t, s, operation.method, operation.path, map[string]any{}, token)
			if readable {
				require.NotEqual(t, http.StatusForbidden, response.Code, "%s: %s", operation.path, response.Body.String())
			} else {
				require.Equal(t, http.StatusForbidden, response.Code, "%s: %s", operation.path, response.Body.String())
			}
		}
		for _, regenerate := range []string{"false", "true"} {
			response := makeRequest(t, s, http.MethodPost, "/graphql", map[string]any{"query": "mutation { triggerEmbedding(regenerate: " + regenerate + ") { total } }"}, token)
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			if readable {
				require.NotContains(t, response.Body.String(), `"errors"`)
			} else {
				require.Contains(t, response.Body.String(), "read permission")
			}
		}
		if !readable {
			node, err := s.db.GetStorage().GetNode("population-vector")
			require.NoError(t, err)
			require.Equal(t, vectors, node.ChunkEmbeddings, "denied global regeneration cleared vectors")
		}
		// The granted regeneration intentionally clears vectors. Restore the
		// fixture before testing revocation with the same token.
		require.NoError(t, s.db.GetStorage().UpdateNode(storage.NodeUpdateRequest{After: &storage.Node{ID: "population-vector", Labels: []string{"Seed"}, ChunkEmbeddings: vectors}}))
	}
}
