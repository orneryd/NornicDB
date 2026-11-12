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
	_, err := users.CreateUser(context.Background(), role, "password123", []auth.Role{auth.Role(role)})
	require.NoError(t, err)
	token := "Bearer " + getAuthToken(t, users, role)
	vectors := [][]float32{{1, 0, 0}}
	for _, admin := range []bool{false, true, false} {
		permissions := []string{"read", "write"}
		if admin {
			permissions = append(permissions, "admin")
		}
		require.NoError(t, s.roleEntitlementsStore.Set(ctx, role, permissions))
		_, err = s.db.GetStorage().CreateNode(t.Context(), &storage.Node{ID: "regenerate-seed", Labels: []string{"Seed"}, ChunkEmbeddings: vectors})
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
			node, err := s.db.GetStorage().GetNode(t.Context(), "regenerate-seed")
			require.NoError(t, err)
			switch {
			case regenerate && !admin:
				// Authorization still precedes the destructive clear stage.
				require.NotEmpty(t, payload.Errors, response.Body.String())
				require.Contains(t, payload.Errors[0].Message, "admin permission")
				require.Equal(t, vectors, node.ChunkEmbeddings, "refused request cleared embeddings")
			case regenerate:
				// The permitted clear stage succeeds, then the fixture's missing
				// worker is reported instead of a success status. The trigger
				// stage never runs, so the cleared vectors stay cleared.
				require.NotEmpty(t, payload.Errors, response.Body.String())
				require.Contains(t, payload.Errors[0].Message, "failed to reset embedding worker: ")
				require.NotContains(t, payload.Errors[0].Message, "failed to clear existing embeddings")
				require.NotContains(t, payload.Errors[0].Message, "failed to trigger embedding")
				require.Empty(t, node.ChunkEmbeddings)
			default:
				// A permitted trigger without a worker reports that operational
				// failure instead of a status for work that was never queued.
				require.NotEmpty(t, payload.Errors, response.Body.String())
				require.Contains(t, payload.Errors[0].Message, "failed to trigger embedding: ")
				require.NotContains(t, payload.Errors[0].Message, "failed to reset embedding worker")
				require.Equal(t, vectors, node.ChunkEmbeddings)
			}
		}
		require.NoError(t, s.db.GetStorage().DeleteNode(t.Context(), "regenerate-seed"))
	}
}

// TestGraphQLTriggerEmbeddingReportsLiveWorkerStatus is the permitted success
// path: a live worker answers the mutation with its status synchronously. The
// fixture database holds no embeddable nodes (the system namespace is never
// embedded and this database enables no embeddings), so the returned status
// describes the idle queue without waiting for provider progress. The
// fixture's database cleanup owns the worker shutdown that SetEmbedder starts.
func TestGraphQLTriggerEmbeddingReportsLiveWorkerStatus(t *testing.T) {
	s, users := setupTestServer(t)
	require.True(t, s.db.SetEmbedder(&countingEmbedder{dims: 1024}), "SetEmbedder must install the test provider")
	require.NotNil(t, s.db.EmbedQueueStats(), "SetEmbedder must leave a live embed queue for the trigger")
	token := "Bearer " + getAuthToken(t, users, "admin")

	response := makeRequest(t, s, http.MethodPost, "/graphql", map[string]any{
		"query": `mutation { triggerEmbedding(regenerate: false) { pending embedded total workerRunning } }`,
	}, token)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())

	var payload struct {
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
		Data struct {
			TriggerEmbedding *struct {
				Pending       int  `json:"pending"`
				Embedded      int  `json:"embedded"`
				Total         int  `json:"total"`
				WorkerRunning bool `json:"workerRunning"`
			} `json:"triggerEmbedding"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &payload))
	require.Empty(t, payload.Errors, response.Body.String())
	status := payload.Data.TriggerEmbedding
	require.NotNil(t, status, response.Body.String())
	require.Zero(t, status.Pending)
	require.Zero(t, status.Embedded)
	require.Zero(t, status.Total)
	// This field reports in-flight embedding work, not queue existence.
	require.False(t, status.WorkerRunning)
}
