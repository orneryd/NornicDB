// Package server provides comprehensive tests for multi-database support.
package server

import (
	"github.com/orneryd/nornicdb/pkg/multidb"

	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"testing"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMultiDatabase_Routing tests that queries are routed to the correct database.
func TestMultiDatabase_Routing(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Create multiple databases
	createDatabase(t, server, "tenant_a", token)
	createDatabase(t, server, "tenant_b", token)

	// Add data to tenant_a
	resp := makeRequest(t, server, "POST", "/db/tenant_a/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (n:Person {name: 'Alice', tenant: 'a'}) RETURN n"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// Add data to tenant_b
	resp = makeRequest(t, server, "POST", "/db/tenant_b/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (n:Person {name: 'Bob', tenant: 'b'}) RETURN n"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// Query tenant_a - should only see Alice
	resp = makeRequest(t, server, "POST", "/db/tenant_a/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n:Person) RETURN n.name as name, n.tenant as tenant ORDER BY name"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	var result TransactionResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Results, 1)
	require.Len(t, result.Results[0].Data, 1)
	assert.Equal(t, "Alice", result.Results[0].Data[0].Row[0])
	assert.Equal(t, "a", result.Results[0].Data[0].Row[1])

	// Query tenant_b - should only see Bob
	resp = makeRequest(t, server, "POST", "/db/tenant_b/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n:Person) RETURN n.name as name, n.tenant as tenant ORDER BY name"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Results, 1)
	require.Len(t, result.Results[0].Data, 1)
	assert.Equal(t, "Bob", result.Results[0].Data[0].Row[0])
	assert.Equal(t, "b", result.Results[0].Data[0].Row[1])
}

// TestMultiDatabase_Isolation tests complete data isolation between databases.
func TestMultiDatabase_Isolation(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Create databases
	createDatabase(t, server, "prod", token)
	createDatabase(t, server, "staging", token)
	createDatabase(t, server, "dev", token)

	// Add different data to each
	databases := []struct {
		name string
		data []map[string]interface{}
	}{
		{
			name: "prod",
			data: []map[string]interface{}{
				{"statement": "CREATE (n:User {id: 1, env: 'prod'}) RETURN n"},
				{"statement": "CREATE (n:User {id: 2, env: 'prod'}) RETURN n"},
				{"statement": "CREATE (n:User {id: 3, env: 'prod'}) RETURN n"},
			},
		},
		{
			name: "staging",
			data: []map[string]interface{}{
				{"statement": "CREATE (n:User {id: 10, env: 'staging'}) RETURN n"},
				{"statement": "CREATE (n:User {id: 20, env: 'staging'}) RETURN n"},
			},
		},
		{
			name: "dev",
			data: []map[string]interface{}{
				{"statement": "CREATE (n:User {id: 100, env: 'dev'}) RETURN n"},
			},
		},
	}

	// Create data in each database
	for _, db := range databases {
		resp := makeRequest(t, server, "POST", "/db/"+db.name+"/tx/commit", map[string]interface{}{
			"statements": db.data,
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code, "failed to create data in %s", db.name)
	}

	// Verify isolation: each database only sees its own data
	for _, db := range databases {
		resp := makeRequest(t, server, "POST", "/db/"+db.name+"/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "MATCH (n:User) RETURN count(n) as count"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		var result TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Results, 1)
		require.Len(t, result.Results[0].Data, 1)

		expectedCount := int64(len(db.data))
		actualCount := int64(result.Results[0].Data[0].Row[0].(float64))
		assert.Equal(t, expectedCount, actualCount, "database %s should have %d nodes, got %d", db.name, expectedCount, actualCount)
	}

	// Verify cross-database queries don't see other data
	resp := makeRequest(t, server, "POST", "/db/prod/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n:User {env: 'staging'}) RETURN n"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	var result TransactionResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Results, 1)
	assert.Len(t, result.Results[0].Data, 0, "prod database should not see staging data")
}

// TestMultiDatabase_NonExistentDatabase tests error handling for non-existent databases.
func TestMultiDatabase_NonExistentDatabase(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	tests := []struct {
		name           string
		endpoint       string
		method         string
		expectedStatus int
		expectedCode   string
	}{
		{
			name:           "query non-existent database",
			endpoint:       "/db/nonexistent/tx/commit",
			method:         "POST",
			expectedStatus: http.StatusNotFound,
			expectedCode:   "Neo.ClientError.Database.DatabaseNotFound",
		},
		{
			name:           "get info for non-existent database",
			endpoint:       "/db/nonexistent",
			method:         "GET",
			expectedStatus: http.StatusNotFound,
			expectedCode:   "Neo.ClientError.Database.DatabaseNotFound",
		},
		{
			name:           "open transaction in non-existent database",
			endpoint:       "/db/nonexistent/tx",
			method:         "POST",
			expectedStatus: http.StatusNotFound,
			expectedCode:   "Neo.ClientError.Database.DatabaseNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body map[string]interface{}
			if tt.method == "POST" && tt.endpoint != "/db/nonexistent" {
				body = map[string]interface{}{
					"statements": []map[string]interface{}{
						{"statement": "MATCH (n) RETURN n"},
					},
				}
			}

			resp := makeRequest(t, server, tt.method, tt.endpoint, body, "Bearer "+token)
			assert.Equal(t, tt.expectedStatus, resp.Code, "expected status %d, got %d", tt.expectedStatus, resp.Code)

			if tt.expectedCode != "" {
				// All errors are returned in TransactionResponse format (even for GET requests)
				bodyBytes := resp.Body.Bytes()
				var txResp TransactionResponse
				if err := json.NewDecoder(bytes.NewReader(bodyBytes)).Decode(&txResp); err == nil && len(txResp.Errors) > 0 {
					assert.Equal(t, tt.expectedCode, txResp.Errors[0].Code, "expected error code %s, got %s", tt.expectedCode, txResp.Errors[0].Code)
				} else {
					// Fallback: try as direct error object
					var errorResp map[string]interface{}
					if err := json.NewDecoder(bytes.NewReader(bodyBytes)).Decode(&errorResp); err == nil {
						if code, ok := errorResp["code"].(string); ok {
							assert.Equal(t, tt.expectedCode, code, "expected error code %s", tt.expectedCode)
						} else {
							t.Errorf("failed to find error code in response: %s", string(bodyBytes))
						}
					} else {
						t.Errorf("failed to parse error response: %v, body: %s", err, string(bodyBytes))
					}
				}
			}
		})
	}
}

// TestMultiDatabase_DatabaseInfo tests the database info endpoint for multiple databases.
func TestMultiDatabase_DatabaseInfo(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Create databases with different amounts of data
	createDatabase(t, server, "small_db", token)
	createDatabase(t, server, "large_db", token)

	// Add data to small_db
	makeRequest(t, server, "POST", "/db/small_db/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (n:Node {id: 1})"},
			{"statement": "CREATE (n:Node {id: 2})"},
		},
	}, "Bearer "+token)

	// Add more data to large_db
	makeRequest(t, server, "POST", "/db/large_db/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (n:Node {id: 1})"},
			{"statement": "CREATE (n:Node {id: 2})"},
			{"statement": "CREATE (n:Node {id: 3})"},
			{"statement": "CREATE (n:Node {id: 4})"},
			{"statement": "CREATE (n:Node {id: 5})"},
			{"statement": "CREATE (a:Node {id: 1})-[r:RELATES_TO]->(b:Node {id: 2}) RETURN r"},
		},
	}, "Bearer "+token)

	// Get info for small_db
	resp := makeRequest(t, server, "GET", "/db/small_db", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	var info map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&info))
	assert.Equal(t, "small_db", info["name"])
	assert.Equal(t, "online", info["status"])
	assert.Equal(t, int64(2), int64(info["nodeCount"].(float64)))
	assert.Equal(t, int64(0), int64(info["edgeCount"].(float64)))

	// Get info for large_db
	resp = makeRequest(t, server, "GET", "/db/large_db", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	require.NoError(t, json.NewDecoder(resp.Body).Decode(&info))
	assert.Equal(t, "large_db", info["name"])
	assert.Equal(t, "online", info["status"])
	// The relationship CREATE statement creates 2 additional nodes (a and b)
	// So we have: 5 from explicit CREATE + 2 from relationship CREATE = 7 total
	assert.Equal(t, int64(7), int64(info["nodeCount"].(float64)))
	assert.Equal(t, int64(1), int64(info["edgeCount"].(float64)))
}

// TestMultiDatabase_DefaultDatabase tests that the default database works correctly.
func TestMultiDatabase_DefaultDatabase(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Default database should exist and work
	resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (n:Test {value: 'default'}) RETURN n"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// Verify default database info
	resp = makeRequest(t, server, "GET", "/db/nornic", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	var info map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&info))
	assert.Equal(t, "nornic", info["name"])
	assert.Equal(t, true, info["default"], "default database should be marked as default")
}

// TestMultiDatabase_ConcurrentAccess tests concurrent access to multiple databases.
func TestMultiDatabase_ConcurrentAccess(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Create multiple databases
	dbs := []string{"db1", "db2", "db3", "db4", "db5"}
	for _, dbName := range dbs {
		createDatabase(t, server, dbName, token)
	}

	// Concurrently add data to each database
	done := make(chan bool, len(dbs))
	for _, dbName := range dbs {
		go func(name string) {
			defer func() { done <- true }()
			for i := 0; i < 10; i++ {
				resp := makeRequest(t, server, "POST", "/db/"+name+"/tx/commit", map[string]interface{}{
					"statements": []map[string]interface{}{
						{"statement": "CREATE (n:Node {id: $id, db: $db}) RETURN n", "parameters": map[string]interface{}{
							"id": i,
							"db": name,
						}},
					},
				}, "Bearer "+token)
				require.Equal(t, http.StatusOK, resp.Code, "failed to create node in %s", name)
			}
		}(dbName)
	}

	// Wait for all goroutines
	for i := 0; i < len(dbs); i++ {
		<-done
	}

	// Verify each database has correct data
	for _, dbName := range dbs {
		resp := makeRequest(t, server, "POST", "/db/"+dbName+"/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "MATCH (n:Node {db: $db}) RETURN count(n) as count", "parameters": map[string]interface{}{
					"db": dbName,
				}},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		var result TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Results, 1)
		require.Len(t, result.Results[0].Data, 1)
		assert.Equal(t, int64(10), int64(result.Results[0].Data[0].Row[0].(float64)), "database %s should have 10 nodes", dbName)
	}
}

// TestMultiDatabase_ComplexQueries tests complex queries across multiple databases.
func TestMultiDatabase_ComplexQueries(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	createDatabase(t, server, "ecommerce", token)

	// Create complex graph structure
	resp := makeRequest(t, server, "POST", "/db/ecommerce/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			// Create products
			{"statement": "CREATE (p1:Product {id: 1, name: 'Laptop', price: 999.99})"},
			{"statement": "CREATE (p2:Product {id: 2, name: 'Mouse', price: 29.99})"},
			{"statement": "CREATE (p3:Product {id: 3, name: 'Keyboard', price: 79.99})"},
			// Create users
			{"statement": "CREATE (u1:User {id: 1, name: 'Alice', email: 'alice@example.com'})"},
			{"statement": "CREATE (u2:User {id: 2, name: 'Bob', email: 'bob@example.com'})"},
			// Create orders
			{"statement": "CREATE (o1:Order {id: 1, total: 1029.98, date: '2024-01-01'})"},
			{"statement": "CREATE (o2:Order {id: 2, total: 109.98, date: '2024-01-02'})"},
			// Create relationships
			{"statement": "MATCH (u:User {id: 1}), (o:Order {id: 1}) CREATE (u)-[:PLACED]->(o)"},
			{"statement": "MATCH (u:User {id: 2}), (o:Order {id: 2}) CREATE (u)-[:PLACED]->(o)"},
			{"statement": "MATCH (o:Order {id: 1}), (p:Product {id: 1}) CREATE (o)-[:CONTAINS {quantity: 1}]->(p)"},
			{"statement": "MATCH (o:Order {id: 1}), (p:Product {id: 2}) CREATE (o)-[:CONTAINS {quantity: 1}]->(p)"},
			{"statement": "MATCH (o:Order {id: 2}), (p:Product {id: 2}) CREATE (o)-[:CONTAINS {quantity: 1}]->(p)"},
			{"statement": "MATCH (o:Order {id: 2}), (p:Product {id: 3}) CREATE (o)-[:CONTAINS {quantity: 1}]->(p)"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// Complex query: Find users who ordered products over $50
	resp = makeRequest(t, server, "POST", "/db/ecommerce/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": `
				MATCH (u:User)-[:PLACED]->(o:Order)-[:CONTAINS]->(p:Product)
				WHERE p.price > 50
				RETURN DISTINCT u.name as user, p.name as product, p.price as price
				ORDER BY u.name, p.price DESC
			`},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	var result TransactionResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Results, 1)
	// Query should return users who ordered products over $50
	// Results may vary based on query execution, so we check that we get results
	require.Greater(t, len(result.Results[0].Data), 0, "should have at least one result")

	// Verify results contain expected data
	rows := result.Results[0].Data
	foundAliceLaptop := false
	foundBobKeyboard := false

	for _, row := range rows {
		user := row.Row[0].(string)
		product := row.Row[1].(string)
		price := row.Row[2].(float64)

		if user == "Alice" && product == "Laptop" && price == 999.99 {
			foundAliceLaptop = true
		}
		if user == "Bob" && product == "Keyboard" && price == 79.99 {
			foundBobKeyboard = true
		}
	}

	assert.True(t, foundAliceLaptop, "should find Alice's Laptop order")
	assert.True(t, foundBobKeyboard, "should find Bob's Keyboard order")
}

// TestMultiDatabase_GetExecutorForDatabase tests the shared database executor cache.
func TestMultiDatabase_GetExecutorForDatabase(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "nornicdb-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Database.AsyncWritesEnabled = false

	db, err := nornicdb.Open(tmpDir, config)
	require.NoError(t, err)
	defer db.Close()

	server, err := New(db, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { stopTestServer(t, server) })

	// Test with existing database
	executor, err := server.executorCache.Get("nornic")
	require.NoError(t, err)
	assert.NotNil(t, executor)

	// Test with non-existent database
	executor, err = server.executorCache.Get("nonexistent")
	assert.Error(t, err)
	assert.Nil(t, executor)
}

// createDatabase is a helper to create a database for testing.
// This uses the DatabaseManager directly. Once system commands (CREATE DATABASE) are implemented,
// tests can use the Cypher API instead.
func createDatabase(t *testing.T, server *Server, dbName string, token string) {
	t.Helper()
	err := server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: dbName})
	require.NoError(t, err, "failed to create database %s", dbName)
}
