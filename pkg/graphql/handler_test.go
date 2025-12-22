package graphql

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testDB creates a temporary NornicDB instance for testing
func testDB(t *testing.T) *nornicdb.DB {
	t.Helper()
	db, err := nornicdb.Open(t.TempDir(), &nornicdb.Config{
		DecayEnabled:     false,
		AutoLinksEnabled: false,
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

// testDBManager creates a DatabaseManager for testing
func testDBManager(t *testing.T, db *nornicdb.DB) *multidb.DatabaseManager {
	t.Helper()
	// Get the base storage (unwraps NamespacedEngine) - DatabaseManager creates its own NamespacedEngines
	inner := db.GetBaseStorageForManager()
	manager, err := multidb.NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	t.Cleanup(func() { manager.Close() })
	return manager
}

func TestNewHandler(t *testing.T) {
	db := testDB(t)
	dbManager := testDBManager(t, db)
	handler := NewHandler(db, dbManager)

	assert.NotNil(t, handler)
	assert.NotNil(t, handler.server)
	assert.NotNil(t, handler.playgroundHandle)
}

func TestHandler_ServeHTTP(t *testing.T) {
	db := testDB(t)
	dbManager := testDBManager(t, db)
	handler := NewHandler(db, dbManager)

	t.Run("handles introspection query", func(t *testing.T) {
		query := `{"query": "{ __schema { types { name } } }"}`
		req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewBufferString(query))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var response map[string]interface{}
		err := json.NewDecoder(rec.Body).Decode(&response)
		require.NoError(t, err)
		assert.Contains(t, response, "data")
	})

	t.Run("handles stats query", func(t *testing.T) {
		query := `{"query": "{ stats { nodeCount relationshipCount } }"}`
		req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewBufferString(query))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var response map[string]interface{}
		err := json.NewDecoder(rec.Body).Decode(&response)
		require.NoError(t, err)

		data, ok := response["data"].(map[string]interface{})
		require.True(t, ok)
		stats, ok := data["stats"].(map[string]interface{})
		require.True(t, ok)
		assert.Contains(t, stats, "nodeCount")
		assert.Contains(t, stats, "relationshipCount")
	})

	t.Run("handles labels query", func(t *testing.T) {
		query := `{"query": "{ labels }"}`
		req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewBufferString(query))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var response map[string]interface{}
		err := json.NewDecoder(rec.Body).Decode(&response)
		require.NoError(t, err)
		assert.Contains(t, response, "data")
	})

	t.Run("handles createNode mutation", func(t *testing.T) {
		query := `{
			"query": "mutation { createNode(input: { labels: [\"Person\"], properties: { name: \"Test\" } }) { id labels properties } }"
		}`
		req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewBufferString(query))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var response map[string]interface{}
		err := json.NewDecoder(rec.Body).Decode(&response)
		require.NoError(t, err)

		data, ok := response["data"].(map[string]interface{})
		require.True(t, ok, "response should have data field")
		createNode, ok := data["createNode"].(map[string]interface{})
		require.True(t, ok, "data should have createNode field")
		assert.NotEmpty(t, createNode["id"])
		assert.Contains(t, createNode["labels"], "Person")
	})

	t.Run("handles cypher query", func(t *testing.T) {
		// First create a node
		createQuery := `{
			"query": "mutation { createNode(input: { labels: [\"TestLabel\"], properties: { name: \"CypherTest\" } }) { id } }"
		}`
		req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewBufferString(createQuery))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)

		// Now query with Cypher
		cypherQuery := `{
			"query": "{ cypher(input: { statement: \"MATCH (n:TestLabel) RETURN n.name as name\" }) { columns rowCount } }"
		}`
		req = httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewBufferString(cypherQuery))
		req.Header.Set("Content-Type", "application/json")
		rec = httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var response map[string]interface{}
		err := json.NewDecoder(rec.Body).Decode(&response)
		require.NoError(t, err)

		data, ok := response["data"].(map[string]interface{})
		require.True(t, ok)
		cypher, ok := data["cypher"].(map[string]interface{})
		require.True(t, ok)
		assert.Contains(t, cypher["columns"], "name")
	})

	t.Run("returns error for invalid query", func(t *testing.T) {
		query := `{"query": "{ invalidField }"}`
		req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewBufferString(query))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		// gqlgen returns 422 for validation errors
		assert.True(t, rec.Code == http.StatusUnprocessableEntity,
			"Expected 200 or 422, got %d", rec.Code)

		var response map[string]interface{}
		err := json.NewDecoder(rec.Body).Decode(&response)
		require.NoError(t, err)
		assert.Contains(t, response, "errors")
	})

	t.Run("handles query with variables", func(t *testing.T) {
		// Create a node first
		createQuery := `{
			"query": "mutation { createNode(input: { labels: [\"Person\"], properties: { name: \"VariableTest\", age: 25 } }) { id } }"
		}`
		req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewBufferString(createQuery))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		// Query with variables
		query := `{
			"query": "query GetNodes($label: String!) { nodesByLabel(label: $label) { id labels properties } }",
			"variables": { "label": "Person" }
		}`
		req = httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewBufferString(query))
		req.Header.Set("Content-Type", "application/json")
		rec = httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var response map[string]interface{}
		err := json.NewDecoder(rec.Body).Decode(&response)
		require.NoError(t, err)
		assert.Contains(t, response, "data")
		assert.NotContains(t, response, "errors")
	})
}

func TestHandler_Playground(t *testing.T) {
	db := testDB(t)
	dbManager := testDBManager(t, db)
	handler := NewHandler(db, dbManager)

	t.Run("returns playground handler", func(t *testing.T) {
		playground := handler.Playground()
		assert.NotNil(t, playground)

		// Test that playground serves HTML
		req := httptest.NewRequest(http.MethodGet, "/playground", nil)
		rec := httptest.NewRecorder()

		playground.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		body, _ := io.ReadAll(rec.Body)
		assert.Contains(t, string(body), "html")
	})
}

func TestGraphQL_EndToEnd(t *testing.T) {
	db := testDB(t)
	dbManager := testDBManager(t, db)
	handler := NewHandler(db, dbManager)

	// Create nodes
	createPerson := func(name string) string {
		query := `{
			"query": "mutation CreatePerson($name: String!) { createNode(input: { labels: [\"Person\"], properties: { name: $name } }) { id } }",
			"variables": { "name": "` + name + `" }
		}`
		req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewBufferString(query))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		var response map[string]interface{}
		json.NewDecoder(rec.Body).Decode(&response)
		data := response["data"].(map[string]interface{})
		createNode := data["createNode"].(map[string]interface{})
		return createNode["id"].(string)
	}

	t.Run("full CRUD workflow", func(t *testing.T) {
		// Create
		aliceID := createPerson("Alice")
		assert.NotEmpty(t, aliceID)

		bobID := createPerson("Bob")
		assert.NotEmpty(t, bobID)

		// Create relationship
		relQuery := `{
			"query": "mutation { createRelationship(input: { startNodeId: \"` + aliceID + `\", endNodeId: \"` + bobID + `\", type: \"KNOWS\", properties: { since: \"2024\" } }) { id type } }"
		}`
		req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewBufferString(relQuery))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)

		// Read - query the node
		readQuery := `{
			"query": "{ node(id: \"` + aliceID + `\") { id labels properties } }"
		}`
		req = httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewBufferString(readQuery))
		req.Header.Set("Content-Type", "application/json")
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		var readResponse map[string]interface{}
		json.NewDecoder(rec.Body).Decode(&readResponse)
		data := readResponse["data"].(map[string]interface{})
		node := data["node"].(map[string]interface{})
		assert.Equal(t, aliceID, node["id"])

		// Update
		updateQuery := `{
			"query": "mutation { updateNode(input: { id: \"` + aliceID + `\", properties: { age: 30 } }) { id properties } }"
		}`
		req = httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewBufferString(updateQuery))
		req.Header.Set("Content-Type", "application/json")
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)

		// Delete
		deleteQuery := `{
			"query": "mutation { deleteNode(id: \"` + bobID + `\") }"
		}`
		req = httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewBufferString(deleteQuery))
		req.Header.Set("Content-Type", "application/json")
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)

		// Verify delete
		readQuery = `{
			"query": "{ node(id: \"` + bobID + `\") { id } }"
		}`
		req = httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewBufferString(readQuery))
		req.Header.Set("Content-Type", "application/json")
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		json.NewDecoder(rec.Body).Decode(&readResponse)
		data = readResponse["data"].(map[string]interface{})
		assert.Nil(t, data["node"]) // Node should be deleted
	})

	t.Run("graph traversal", func(t *testing.T) {
		// Create a small graph: A -> B -> C
		aID := createPerson("NodeA")
		bID := createPerson("NodeB")
		cID := createPerson("NodeC")

		// Create edges
		for _, edge := range []struct{ from, to string }{{aID, bID}, {bID, cID}} {
			query := `{
				"query": "mutation { createRelationship(input: { startNodeId: \"` + edge.from + `\", endNodeId: \"` + edge.to + `\", type: \"CONNECTS\" }) { id } }"
			}`
			req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewBufferString(query))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
		}

		// Test shortest path
		pathQuery := `{
			"query": "{ shortestPath(startNodeId: \"` + aID + `\", endNodeId: \"` + cID + `\") { id } }"
		}`
		req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewBufferString(pathQuery))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		var response map[string]interface{}
		json.NewDecoder(rec.Body).Decode(&response)
		data := response["data"].(map[string]interface{})
		path := data["shortestPath"].([]interface{})
		assert.Len(t, path, 3) // A -> B -> C
	})
}
