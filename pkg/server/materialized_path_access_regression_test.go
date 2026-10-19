package server

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type materializedPathAccessRecorder struct {
	mu       sync.Mutex
	accessed []string
}

func (r *materializedPathAccessRecorder) IncrementAccess(entityID string) {
	r.mu.Lock()
	r.accessed = append(r.accessed, entityID)
	r.mu.Unlock()
}

func (r *materializedPathAccessRecorder) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	accessed := r.accessed
	r.accessed = nil
	sort.Strings(accessed)
	return accessed
}

func requireMaterializedPathResponse(t *testing.T, responseBody []byte) {
	t.Helper()
	var response TransactionResponse
	require.NoError(t, json.Unmarshal(responseBody, &response))
	require.Empty(t, response.Errors)
	require.Len(t, response.Results, 1)
	require.Len(t, response.Results[0].Data, 1)
}

func TestMaterializedPathAccessHTTP(t *testing.T) {
	server, authenticator := setupTestServer(t)
	base, ok := storage.UnwrapEngine(server.db.GetBaseStorageForManager()).(*storage.BadgerEngine)
	require.True(t, ok)
	recorder := &materializedPathAccessRecorder{}
	base.SetAccessAccumulator(recorder)
	store, err := server.dbManager.GetStorage("nornic")
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "a", Labels: []string{"MaterializedPathAccess"}, Properties: map[string]interface{}{"id": "a"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "b", Labels: []string{"MaterializedPathAccess"}, Properties: map[string]interface{}{"id": "b"}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "MATERIALIZED_PATH_ACCESS"}))
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	query := "MATCH p=(:MaterializedPathAccess {id:'a'})-[:MATERIALIZED_PATH_ACCESS]->(:MaterializedPathAccess {id:'b'}) RETURN p"
	request := map[string]interface{}{"statements": []map[string]interface{}{{"statement": query}}}

	t.Run("auto-commit", func(t *testing.T) {
		response := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", request, token)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		requireMaterializedPathResponse(t, response.Body.Bytes())
		require.Equal(t, []string{"nornic:a", "nornic:b", "nornic:e"}, recorder.take())
	})

	t.Run("explicit", func(t *testing.T) {
		response := makeRequest(t, server, http.MethodPost, "/db/nornic/tx", request, token)
		require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
		requireMaterializedPathResponse(t, response.Body.Bytes())
		require.Equal(t, []string{"nornic:a", "nornic:b", "nornic:e"}, recorder.take())
		location := response.Header().Get("Location")
		require.NotEmpty(t, location)
		commitPath := location[strings.Index(location, "/db/"):] + "/commit"
		response = makeRequest(t, server, http.MethodPost, commitPath, map[string]interface{}{"statements": []interface{}{}}, token)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	})
}

func TestMaterializedEntityAccessHTTP(t *testing.T) {
	server, authenticator := setupTestServer(t)
	base, ok := storage.UnwrapEngine(server.db.GetBaseStorageForManager()).(*storage.BadgerEngine)
	require.True(t, ok)
	recorder := &materializedPathAccessRecorder{}
	base.SetAccessAccumulator(recorder)
	store, err := server.dbManager.GetStorage("nornic")
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "a", Labels: []string{"MaterializedEntityAccess"}, Properties: map[string]interface{}{"id": "a"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "b", Labels: []string{"MaterializedEntityAccess"}, Properties: map[string]interface{}{"id": "b"}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "MATERIALIZED_ENTITY_ACCESS"}))
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	query := "MATCH (a:MaterializedEntityAccess {id:'a'})-[r:MATERIALIZED_ENTITY_ACCESS]->(b:MaterializedEntityAccess {id:'b'}) RETURN a,r,b"
	request := map[string]interface{}{"statements": []map[string]interface{}{{"statement": query}}}

	t.Run("auto-commit", func(t *testing.T) {
		response := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", request, token)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		requireMaterializedPathResponse(t, response.Body.Bytes())
		require.Equal(t, []string{"nornic:a", "nornic:b", "nornic:e"}, recorder.take())
	})

	t.Run("explicit", func(t *testing.T) {
		response := makeRequest(t, server, http.MethodPost, "/db/nornic/tx", request, token)
		require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
		requireMaterializedPathResponse(t, response.Body.Bytes())
		require.Equal(t, []string{"nornic:a", "nornic:b", "nornic:e"}, recorder.take())
		location := response.Header().Get("Location")
		require.NotEmpty(t, location)
		commitPath := location[strings.Index(location, "/db/"):] + "/commit"
		response = makeRequest(t, server, http.MethodPost, commitPath, map[string]interface{}{"statements": []interface{}{}}, token)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	})
}
