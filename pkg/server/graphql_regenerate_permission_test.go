package server

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGraphQLRegenerateRequiresAdminBeforeClearing(t *testing.T) {
	s, users := setupTestServer(t)
	ctx := context.Background()
	const role = "embedding-writer"
	require.NoError(t, s.roleStore.CreateRole(ctx, role))
	require.NoError(t, s.allowlistStore.SaveRoleDatabases(ctx, role, []string{"nornic", "system"}))
	require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "nornic", true, true))
	require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "system", true, true))
	_, err := users.CreateUser(role, "password123", []auth.Role{auth.Role(role)})
	require.NoError(t, err)
	token := "Bearer " + getAuthToken(t, users, role)
	vectors := [][]float32{{1, 0, 0}}
	for _, admin := range []bool{false, true, false} {
		permissions := []string{"read", "write"}
		if admin {
			permissions = append(permissions, "admin")
		}
		require.NoError(t, s.roleEntitlementsStore.Set(ctx, role, permissions))
		_, err = s.db.GetStorage().CreateNode(&storage.Node{ID: "regenerate-seed", Labels: []string{"Seed"}, ChunkEmbeddings: vectors})
		require.NoError(t, err)
		for _, regenerate := range []bool{false, true} {
			query := `mutation { triggerEmbedding(regenerate: false) { total } }`
			if regenerate {
				query = `mutation { triggerEmbedding(regenerate: true) { total } }`
			}
			response := makeRequest(t, s, http.MethodPost, "/graphql", map[string]any{"query": query}, token)
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			var payload struct {
				Errors []struct {
					Message string `json:"message"`
				} `json:"errors"`
			}
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &payload))
			node, err := s.db.GetStorage().GetNode("regenerate-seed")
			require.NoError(t, err)
			if regenerate && !admin {
				require.NotEmpty(t, payload.Errors, response.Body.String())
				require.Contains(t, payload.Errors[0].Message, "admin permission")
				require.Equal(t, vectors, node.ChunkEmbeddings, "refused request cleared embeddings")
			} else {
				require.Empty(t, payload.Errors, response.Body.String())
				if regenerate {
					require.Empty(t, node.ChunkEmbeddings)
				} else {
					require.Equal(t, vectors, node.ChunkEmbeddings)
				}
			}
		}
		require.NoError(t, s.db.GetStorage().DeleteNode("regenerate-seed"))
	}
}
