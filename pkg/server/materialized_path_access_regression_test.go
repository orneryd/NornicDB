package server

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// materializedPathAccessRecorder observes the read paths' access
// acknowledgements. IncrementAccess is a required interface method, not a
// compatibility wrapper: the production engine calls it through
// storage.accessIncrementor (BadgerEngine.SetAccessAccumulator), so the fixture
// cannot drop it. The adapter owns only that required method name; the recorded
// append itself is the canonical fixtureowner.Append on the same lock.
type materializedPathAccessRecorder struct {
	mu       sync.Mutex
	accessed []string
}

// IncrementAccess is the storage.accessIncrementor callback the read path
// invokes once per accessed entity, so the record order is the engine's call
// order.
func (r *materializedPathAccessRecorder) IncrementAccess(entityID string) {
	fixtureowner.Append(&r.mu, &r.accessed, entityID)
}

// take drains every record and sorts it. It holds the producer lock across the
// drain, so records appended during a drain stay for the next take; a
// fixtureowner.Snapshot followed by a reset would drop them.
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
	_, err = store.CreateNode(t.Context(), &storage.Node{ID: "a", Labels: []string{"MaterializedPathAccess"}, Properties: map[string]interface{}{"id": "a"}})
	require.NoError(t, err)
	_, err = store.CreateNode(t.Context(), &storage.Node{ID: "b", Labels: []string{"MaterializedPathAccess"}, Properties: map[string]interface{}{"id": "b"}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(t.Context(), &storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "MATERIALIZED_PATH_ACCESS"}))
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
	_, err = store.CreateNode(t.Context(), &storage.Node{ID: "a", Labels: []string{"MaterializedEntityAccess"}, Properties: map[string]interface{}{"id": "a"}})
	require.NoError(t, err)
	_, err = store.CreateNode(t.Context(), &storage.Node{ID: "b", Labels: []string{"MaterializedEntityAccess"}, Properties: map[string]interface{}{"id": "b"}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(t.Context(), &storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "MATERIALIZED_ENTITY_ACCESS"}))
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
