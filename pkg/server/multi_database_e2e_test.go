// Package server provides end-to-end tests for multi-database functionality.
// These tests mirror the manual test sequence in docs/testing/MULTI_DB_E2E_TEST.md
package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMultiDatabase_E2E_FullSequence tests the complete multi-database workflow:
// 1. Verify default database exists and works
// 2. Create multiple databases
// 3. Insert data in each database
// 4. Verify data isolation
// 5. Create composite database
// 6. Query composite database
// 7. Cleanup (drop composite, then constituents)
func TestMultiDatabase_E2E_FullSequence(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Step 1: Verify default database exists and works
	t.Run("Step1_VerifyDefaultDatabase", func(t *testing.T) {
		// List databases
		resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "SHOW DATABASES"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		var result TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Results, 1)
		require.Greater(t, len(result.Results[0].Data), 0, "should have at least default and system databases")

		// Verify default database is accessible
		resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "MATCH (n) RETURN count(n) as node_count"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)
	})

	// Step 2: Create first database
	t.Run("Step2_CreateFirstDatabase", func(t *testing.T) {
		resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "CREATE DATABASE test_db_a"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		// Check for errors
		var createResult TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&createResult))
		if len(createResult.Errors) > 0 {
			t.Fatalf("CREATE DATABASE test_db_a failed: %v", createResult.Errors)
		}

		// Verify database was created
		resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "SHOW DATABASES"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		var result TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Results, 1)
		// Should have exactly: nornic, system, test_db_a
		assert.Equal(t, len(result.Results[0].Data), 3)

		// Verify test_db_a is actually in the list
		found := false
		for _, row := range result.Results[0].Data {
			if len(row.Row) > 0 {
				if name, ok := row.Row[0].(string); ok && name == "test_db_a" {
					found = true
					break
				}
			}
		}
		assert.True(t, found, "test_db_a should be in the list of databases")
	})

	// Step 3: Insert data in first database
	t.Run("Step3_InsertDataInFirstDatabase", func(t *testing.T) {
		resp := makeRequest(t, server, "POST", "/db/test_db_a/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "CREATE (alice:Person {name: 'Alice', id: 'a1', db: 'test_db_a'})"},
				{"statement": "CREATE (bob:Person {name: 'Bob', id: 'a2', db: 'test_db_a'})"},
				{"statement": "CREATE (company:Company {name: 'Acme Corp', id: 'a3', db: 'test_db_a'})"},
				{"statement": "MATCH (a:Person {name: 'Alice'}), (c:Company {name: 'Acme Corp'}) CREATE (a)-[:WORKS_FOR]->(c)"},
				{"statement": "MATCH (b:Person {name: 'Bob'}), (c:Company {name: 'Acme Corp'}) CREATE (b)-[:WORKS_FOR]->(c)"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		// Verify data was created
		resp = makeRequest(t, server, "POST", "/db/test_db_a/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "MATCH (n) RETURN count(n) as count"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		var result TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Results, 1)
		assert.Equal(t, int64(3), int64(result.Results[0].Data[0].Row[0].(float64)), "should have 3 nodes")
	})

	// Step 4: Query first database
	t.Run("Step4_QueryFirstDatabase", func(t *testing.T) {
		resp := makeRequest(t, server, "POST", "/db/test_db_a/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "MATCH (n) RETURN n.name as name, labels(n) as labels, n.db as db ORDER BY n.name"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		var result TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Results, 1)
		assert.Equal(t, 3, len(result.Results[0].Data), "should return 3 nodes")

		// Verify labels in test_db_a
		labelResp := makeRequest(t, server, "POST", "/db/test_db_a/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "MATCH (n) RETURN n.name as name, labels(n) as labels ORDER BY name"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, labelResp.Code)
		var labelResult TransactionResponse
		require.NoError(t, json.NewDecoder(labelResp.Body).Decode(&labelResult))
		require.Len(t, labelResult.Results, 1)
		nameToLabels := make(map[string][]string)
		for _, row := range labelResult.Results[0].Data {
			if len(row.Row) >= 2 {
				name, _ := row.Row[0].(string)
				if labelList, ok := row.Row[1].([]interface{}); ok {
					var labels []string
					for _, l := range labelList {
						if s, ok := l.(string); ok {
							labels = append(labels, s)
						}
					}
					nameToLabels[name] = labels
				}
			}
		}
		require.Contains(t, nameToLabels["Acme Corp"], "Company", "Acme Corp should have Company label in test_db_a")
	})

	// Step 5: Create second database
	t.Run("Step5_CreateSecondDatabase", func(t *testing.T) {
		resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "CREATE DATABASE test_db_b"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		// Check for errors
		var createResult TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&createResult))
		if len(createResult.Errors) > 0 {
			t.Fatalf("CREATE DATABASE test_db_b failed: %v", createResult.Errors)
		}

		// Verify database was created
		resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "SHOW DATABASES"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		var result TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Results, 1)

		// Verify test_db_b is in the list
		found := false
		for _, row := range result.Results[0].Data {
			if len(row.Row) > 0 {
				if name, ok := row.Row[0].(string); ok && name == "test_db_b" {
					found = true
					break
				}
			}
		}
		assert.True(t, found, "test_db_b should be in the list of databases")
	})

	// Step 6: Insert data in second database
	t.Run("Step6_InsertDataInSecondDatabase", func(t *testing.T) {
		resp := makeRequest(t, server, "POST", "/db/test_db_b/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "CREATE (charlie:Person {name: 'Charlie', id: 'b1', db: 'test_db_b'})"},
				{"statement": "CREATE (diana:Person {name: 'Diana', id: 'b2', db: 'test_db_b'})"},
				{"statement": "CREATE (order:Order {order_id: 'ORD-001', amount: 1000, db: 'test_db_b'})"},
				{"statement": "MATCH (c:Person {name: 'Charlie'}), (o:Order {order_id: 'ORD-001'}) CREATE (c)-[:PLACED]->(o)"},
				{"statement": "MATCH (d:Person {name: 'Diana'}), (o:Order {order_id: 'ORD-001'}) CREATE (d)-[:PLACED]->(o)"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		// Verify data was created
		resp = makeRequest(t, server, "POST", "/db/test_db_b/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "MATCH (n) RETURN count(n) as count"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		var result TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Results, 1)
		assert.Equal(t, int64(3), int64(result.Results[0].Data[0].Row[0].(float64)), "should have 3 nodes")
	})

	// Step 7: Query second database
	t.Run("Step7_QuerySecondDatabase", func(t *testing.T) {
		resp := makeRequest(t, server, "POST", "/db/test_db_b/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "MATCH (n) RETURN n.name as name, n.order_id as order_id, labels(n) as labels, n.db as db ORDER BY n.name"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		var result TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Results, 1)
		assert.Equal(t, 3, len(result.Results[0].Data), "should return 3 nodes")

		// Verify labels in test_db_b
		labelResp := makeRequest(t, server, "POST", "/db/test_db_b/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "MATCH (n) RETURN n.name as name, labels(n) as labels ORDER BY name"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, labelResp.Code)
		var labelResult TransactionResponse
		require.NoError(t, json.NewDecoder(labelResp.Body).Decode(&labelResult))
		require.Len(t, labelResult.Results, 1)
		nameToLabels := make(map[string][]string)
		orderLabelFound := false
		for _, row := range labelResult.Results[0].Data {
			if len(row.Row) >= 2 {
				name, _ := row.Row[0].(string)
				if labelList, ok := row.Row[1].([]interface{}); ok {
					var labels []string
					for _, l := range labelList {
						if s, ok := l.(string); ok {
							labels = append(labels, s)
							if s == "Order" {
								orderLabelFound = true
							}
						}
					}
					nameToLabels[name] = labels
				}
			}
		}
		if !orderLabelFound {
			t.Fatalf("should have at least one node with Order label in test_db_b; labels: %+v", nameToLabels)
		}
	})

	// Step 8: Verify isolation
	t.Run("Step8_VerifyIsolation", func(t *testing.T) {
		// Query test_db_a - should NOT see test_db_b data
		resp := makeRequest(t, server, "POST", "/db/test_db_a/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "MATCH (n) RETURN n.name as name, n.order_id as order_id, labels(n) as labels, n.db as db ORDER BY n.name"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		var result TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Results, 1)
		// Should only have Alice, Bob, Acme Corp
		assert.Equal(t, 3, len(result.Results[0].Data), "test_db_a should have 3 nodes")

		// Verify no Order nodes in test_db_a
		resp = makeRequest(t, server, "POST", "/db/test_db_a/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "MATCH (o:Order) RETURN count(o) as order_count"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Results, 1)
		assert.Equal(t, int64(0), int64(result.Results[0].Data[0].Row[0].(float64)), "test_db_a should have no Order nodes")

		// Query test_db_b - should NOT see test_db_a data
		resp = makeRequest(t, server, "POST", "/db/test_db_b/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "MATCH (n) RETURN n.name as name, labels(n) as labels, n.db as db ORDER BY n.name"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Results, 1)
		// Should only have Charlie, Diana, ORD-001
		assert.Equal(t, 3, len(result.Results[0].Data), "test_db_b should have 3 nodes")

		// Verify no Company nodes in test_db_b
		resp = makeRequest(t, server, "POST", "/db/test_db_b/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "MATCH (c:Company) RETURN count(c) as company_count"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Results, 1)
		assert.Equal(t, int64(0), int64(result.Results[0].Data[0].Row[0].(float64)), "test_db_b should have no Company nodes")
	})

	// Step 9: Create composite database
	t.Run("Step9_CreateCompositeDatabase", func(t *testing.T) {
		resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "CREATE COMPOSITE DATABASE test_composite ALIAS db_a FOR DATABASE test_db_a ALIAS db_b FOR DATABASE test_db_b"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		// Check for errors in CREATE response
		var createResult TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&createResult))
		if len(createResult.Errors) > 0 {
			t.Fatalf("CREATE COMPOSITE DATABASE failed: %v", createResult.Errors)
		}
		require.Len(t, createResult.Results, 1, "should have one result")
		// CREATE COMPOSITE DATABASE returns the database name, but it might be empty if command succeeded without returning data

		// Verify composite database was created
		resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "SHOW COMPOSITE DATABASES"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		var result TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Results, 1)
		if len(result.Errors) > 0 {
			t.Fatalf("SHOW COMPOSITE DATABASES failed: %v", result.Errors)
		}
		require.Equal(t, len(result.Results[0].Data), 1, "should have at least one composite database")

		// Verify manager sees the composite
		require.True(t, server.dbManager.IsCompositeDatabase("test_composite"), "manager should recognize test_composite as composite")

		// Verify test_composite is in the list
		found := false
		for _, row := range result.Results[0].Data {
			if len(row.Row) > 0 {
				if name, ok := row.Row[0].(string); ok && name == "test_composite" {
					found = true
					break
				}
			}
		}
		assert.True(t, found, "test_composite should be in the list of composite databases")
	})

	// Step 10: Query composite database
	t.Run("Step10_QueryCompositeDatabase", func(t *testing.T) {
		// Verify composite database exists before querying
		if !server.dbManager.Exists("test_composite") {
			// Dump databases for debugging
			dbs := server.dbManager.ListDatabases()
			t.Fatalf("composite database should exist; current databases: %+v", dbs)
		}

		// Query all Person nodes across both databases
		resp := makeRequest(t, server, "POST", "/db/test_composite/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "MATCH (p:Person) RETURN p.name as name, p.db as db, labels(p) as labels ORDER BY p.name"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		var result TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Results, 1)
		// Should return 4 rows: Alice, Bob, Charlie, Diana
		assert.Equal(t, 4, len(result.Results[0].Data), "composite should see 4 Person nodes")

		// Count all nodes across both databases
		resp = makeRequest(t, server, "POST", "/db/test_composite/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "MATCH (n) RETURN count(n) as total_nodes"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Results, 1)
		assert.Equal(t, int64(6), int64(result.Results[0].Data[0].Row[0].(float64)), "composite should see 6 total nodes (3+3)")

		// Count nodes by label - verify we can query across constituents
		// Use a query that properly handles labels and groups them
		resp = makeRequest(t, server, "POST", "/db/test_composite/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "MATCH (n) UNWIND labels(n) as label RETURN label, count(*) as count ORDER BY label"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Results, 1)
		if len(result.Errors) > 0 {
			t.Fatalf("Query failed: %v", result.Errors)
		}

		// Should have exactly 3 label types: Company, Order, Person
		// If not, capture detailed label data below before failing
		labelsFound := make(map[string]bool)
		totalCount := int64(0)
		for _, row := range result.Results[0].Data {
			if len(row.Row) >= 2 {
				if label, ok := row.Row[0].(string); ok && label != "" {
					labelsFound[label] = true
				}
				if count, ok := row.Row[1].(float64); ok {
					totalCount += int64(count)
				}
			}
		}

		// Verify specific nodes and their labels exist
		resp = makeRequest(t, server, "POST", "/db/test_composite/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "MATCH (n) RETURN n.name as name, labels(n) as labels ORDER BY name"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Results, 1)
		if len(result.Errors) > 0 {
			t.Fatalf("Label verification query failed: %v", result.Errors)
		}

		nameToLabels := make(map[string][]string)
		orderLabelFound := false
		for _, row := range result.Results[0].Data {
			if len(row.Row) >= 2 {
				name, _ := row.Row[0].(string)
				if labelList, ok := row.Row[1].([]interface{}); ok {
					var labels []string
					for _, l := range labelList {
						if s, ok := l.(string); ok {
							labels = append(labels, s)
							if s == "Order" {
								orderLabelFound = true
							}
						}
					}
					nameToLabels[name] = labels
				}
			}
		}

		require.Contains(t, nameToLabels, "Acme Corp", "Company node should be present")
		require.Contains(t, nameToLabels, "Alice", "Person nodes should be present")
		require.Contains(t, nameToLabels, "Bob", "Person nodes should be present")
		require.Contains(t, nameToLabels, "Charlie", "Person nodes should be present")
		require.Contains(t, nameToLabels, "Diana", "Person nodes should be present")

		assert.Contains(t, nameToLabels["Acme Corp"], "Company", "Acme Corp should have Company label")
		assert.True(t, orderLabelFound, "composite view should include a node with Order label; labels map=%+v", nameToLabels)

		require.Equal(t, 3, len(labelsFound), "expected exactly 3 label types (Company, Order, Person), got labelsFound=%+v, rows=%+v, nameToLabels=%+v", labelsFound, result.Results[0].Data, nameToLabels)
		assert.True(t, labelsFound["Company"], "should have Company label")
		assert.True(t, labelsFound["Order"], "should have Order label")
		assert.True(t, labelsFound["Person"], "should have Person label")
		assert.Equal(t, int64(6), totalCount, "total count across all labels should be 6")
	})

	// Step 11: Verify composite database isolation
	t.Run("Step11_VerifyCompositeIsolation", func(t *testing.T) {
		// Verify test_db_a still has its original data
		resp := makeRequest(t, server, "POST", "/db/test_db_a/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "MATCH (n) RETURN count(n) as node_count"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		var result TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Results, 1)
		assert.Equal(t, int64(3), int64(result.Results[0].Data[0].Row[0].(float64)), "test_db_a should still have 3 nodes")

		// Verify test_db_b still has its original data
		resp = makeRequest(t, server, "POST", "/db/test_db_b/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "MATCH (n) RETURN count(n) as node_count"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Results, 1)
		assert.Equal(t, int64(3), int64(result.Results[0].Data[0].Row[0].(float64)), "test_db_b should still have 3 nodes")
	})

	// Step 12: Cleanup - Drop composite database
	t.Run("Step12_DropCompositeDatabase", func(t *testing.T) {
		resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "DROP COMPOSITE DATABASE test_composite"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		// Verify composite database was dropped
		resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "SHOW COMPOSITE DATABASES"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		var result TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Results, 1)
		// Should not have test_composite anymore
		found := false
		for _, row := range result.Results[0].Data {
			if len(row.Row) > 0 {
				if name, ok := row.Row[0].(string); ok && name == "test_composite" {
					found = true
					break
				}
			}
		}
		assert.False(t, found, "test_composite should not exist after dropping")

		// Verify constituent databases still exist
		resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "SHOW DATABASES"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Results, 1)
		// Should still have test_db_a and test_db_b
		assert.GreaterOrEqual(t, len(result.Results[0].Data), 4, "should still have constituent databases")
	})

	// Step 13: Cleanup - Drop test databases
	t.Run("Step13_DropTestDatabases", func(t *testing.T) {
		// Drop first test database
		resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "DROP DATABASE test_db_a"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		// Drop second test database
		resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "DROP DATABASE test_db_b"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		// Verify databases were dropped
		resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "SHOW DATABASES"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		var result TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Results, 1)
		// Should only have nornic and system
		assert.LessOrEqual(t, len(result.Results[0].Data), 2, "should only have default and system databases")
	})
}

// TestMultiDatabase_E2E_DiscoveryEndpoint tests that the discovery endpoint returns default_database
func TestMultiDatabase_E2E_DiscoveryEndpoint(t *testing.T) {
	server, _ := setupTestServer(t)

	resp := makeRequest(t, server, "GET", "/", nil, "")

	require.Equal(t, http.StatusOK, resp.Code)

	var discovery map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&discovery))

	// Verify default_database field exists
	defaultDB, ok := discovery["default_database"]
	require.True(t, ok, "discovery endpoint should include default_database field")
	assert.Equal(t, "nornic", defaultDB, "default database should be 'nornic'")
}

func containsString(list []string, target string) bool {
	for _, s := range list {
		if s == target {
			return true
		}
	}
	return false
}

// TestLabelAggregationQuery tests the labels(n)[0] aggregation query which was broken
// for composite databases. The query "MATCH (n) RETURN labels(n)[0] as label, count(n) as count"
// should correctly group by the first label of each node and return counts per label.
func TestLabelAggregationQuery(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Create a test database with nodes of different labels
	resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE DATABASE label_test_db"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// Insert nodes with different labels
	resp = makeRequest(t, server, "POST", "/db/label_test_db/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (a:Person {name: 'Alice'})"},
			{"statement": "CREATE (b:Person {name: 'Bob'})"},
			{"statement": "CREATE (c:Company {name: 'Acme'})"},
			{"statement": "CREATE (o:Order {order_id: 'ORD-001'})"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// Test the labels(n)[0] aggregation query
	resp = makeRequest(t, server, "POST", "/db/label_test_db/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n) RETURN labels(n)[0] as label, count(n) as count ORDER BY label"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	var result TransactionResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Results, 1)
	if len(result.Errors) > 0 {
		t.Fatalf("Query failed: %v", result.Errors)
	}

	// Should have 3 distinct labels: Company (1), Order (1), Person (2)
	require.Equal(t, 3, len(result.Results[0].Data), "should have exactly 3 label groups, got: %+v", result.Results[0].Data)

	// Verify the counts per label
	labelCounts := make(map[string]int64)
	for _, row := range result.Results[0].Data {
		if len(row.Row) >= 2 {
			label, _ := row.Row[0].(string)
			count := int64(row.Row[1].(float64))
			labelCounts[label] = count
		}
	}

	assert.Equal(t, int64(1), labelCounts["Company"], "Company should have count 1")
	assert.Equal(t, int64(1), labelCounts["Order"], "Order should have count 1")
	assert.Equal(t, int64(2), labelCounts["Person"], "Person should have count 2")

	// Cleanup
	resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "DROP DATABASE label_test_db"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)
}

// TestUseCommandDatabaseSwitching tests that :USE command actually switches database context
func TestUseCommandDatabaseSwitching(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Create two test databases
	resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE DATABASE use_test_db_a"},
			{"statement": "CREATE DATABASE use_test_db_b"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// Insert data in use_test_db_a
	resp = makeRequest(t, server, "POST", "/db/use_test_db_a/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (a:Person {name: 'Alice', db: 'use_test_db_a'})"},
			{"statement": "CREATE (b:Person {name: 'Bob', db: 'use_test_db_a'})"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// Insert data in use_test_db_b
	resp = makeRequest(t, server, "POST", "/db/use_test_db_b/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (c:Person {name: 'Charlie', db: 'use_test_db_b'})"},
			{"statement": "CREATE (d:Person {name: 'Diana', db: 'use_test_db_b'})"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// Test :USE command switches to use_test_db_a even when querying default database
	// Send query to default database endpoint but use :USE to switch
	resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": `:USE use_test_db_a
MATCH (n)
RETURN n.name as name, n.db as db
ORDER BY n.name`},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	var result TransactionResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Results, 1)
	if len(result.Errors) > 0 {
		t.Fatalf("Query failed: %v", result.Errors)
	}

	// Should only return nodes from use_test_db_a (Alice, Bob), not from use_test_db_b
	require.Equal(t, 2, len(result.Results[0].Data), "should have 2 nodes from use_test_db_a")

	names := make([]string, 0)
	for _, row := range result.Results[0].Data {
		if len(row.Row) >= 1 {
			if name, ok := row.Row[0].(string); ok {
				names = append(names, name)
			}
		}
	}
	assert.Contains(t, names, "Alice", "should contain Alice from use_test_db_a")
	assert.Contains(t, names, "Bob", "should contain Bob from use_test_db_a")
	assert.NotContains(t, names, "Charlie", "should NOT contain Charlie from use_test_db_b")
	assert.NotContains(t, names, "Diana", "should NOT contain Diana from use_test_db_b")

	// Test :USE command switches to use_test_db_b
	resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": `:USE use_test_db_b
MATCH (n)
RETURN n.name as name
ORDER BY n.name`},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Results, 1)
	if len(result.Errors) > 0 {
		t.Fatalf("Query failed: %v", result.Errors)
	}

	// Should only return nodes from use_test_db_b (Charlie, Diana)
	require.Equal(t, 2, len(result.Results[0].Data), "should have 2 nodes from use_test_db_b")

	names = make([]string, 0)
	for _, row := range result.Results[0].Data {
		if len(row.Row) >= 1 {
			if name, ok := row.Row[0].(string); ok {
				names = append(names, name)
			}
		}
	}
	assert.Contains(t, names, "Charlie", "should contain Charlie from use_test_db_b")
	assert.Contains(t, names, "Diana", "should contain Diana from use_test_db_b")
	assert.NotContains(t, names, "Alice", "should NOT contain Alice from use_test_db_a")
	assert.NotContains(t, names, "Bob", "should NOT contain Bob from use_test_db_a")

	// Cleanup
	resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "DROP DATABASE use_test_db_a"},
			{"statement": "DROP DATABASE use_test_db_b"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)
}

// TestDefaultDatabaseIsolation tests that the default database only shows its own data
func TestDefaultDatabaseIsolation(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Create a test database
	resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE DATABASE isolation_test_db"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// Insert data in default database (nornic)
	resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (n:DefaultNode {name: 'Default Node 1', db: 'nornic'})"},
			{"statement": "CREATE (n:DefaultNode {name: 'Default Node 2', db: 'nornic'})"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// Insert data in isolation_test_db
	resp = makeRequest(t, server, "POST", "/db/isolation_test_db/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (n:TestNode {name: 'Test Node 1', db: 'isolation_test_db'})"},
			{"statement": "CREATE (n:TestNode {name: 'Test Node 2', db: 'isolation_test_db'})"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// Query default database - should ONLY see default database nodes
	resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n) RETURN n.name as name, n.db as db ORDER BY n.name"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	var result TransactionResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Results, 1)
	if len(result.Errors) > 0 {
		t.Fatalf("Query failed: %v", result.Errors)
	}

	// Should only return nodes from default database (nornic)
	names := make([]string, 0)
	for _, row := range result.Results[0].Data {
		if len(row.Row) >= 1 {
			if name, ok := row.Row[0].(string); ok {
				names = append(names, name)
			}
		}
	}

	// Verify we only see default database nodes
	for _, name := range names {
		assert.NotContains(t, name, "Test Node", "default database should NOT contain nodes from isolation_test_db")
	}

	// Verify we can see default database nodes
	defaultNodeFound := false
	for _, name := range names {
		if name == "Default Node 1" || name == "Default Node 2" {
			defaultNodeFound = true
			break
		}
	}
	assert.True(t, defaultNodeFound, "default database should contain its own nodes")

	// Query isolation_test_db - should ONLY see isolation_test_db nodes
	resp = makeRequest(t, server, "POST", "/db/isolation_test_db/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n) RETURN n.name as name, n.db as db ORDER BY n.name"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Results, 1)
	if len(result.Errors) > 0 {
		t.Fatalf("Query failed: %v", result.Errors)
	}

	// Should only return nodes from isolation_test_db
	names = make([]string, 0)
	for _, row := range result.Results[0].Data {
		if len(row.Row) >= 1 {
			if name, ok := row.Row[0].(string); ok {
				names = append(names, name)
			}
		}
	}

	// Verify we only see isolation_test_db nodes
	for _, name := range names {
		assert.NotContains(t, name, "Default Node", "isolation_test_db should NOT contain nodes from default database")
	}

	// Verify we can see isolation_test_db nodes
	testNodeFound := false
	for _, name := range names {
		if name == "Test Node 1" || name == "Test Node 2" {
			testNodeFound = true
			break
		}
	}
	assert.True(t, testNodeFound, "isolation_test_db should contain its own nodes")

	// Cleanup
	resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "DROP DATABASE isolation_test_db"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)
}

// TestDatabaseIsolationStrict verifies strict isolation - queries should ONLY see data from the specified database
func TestDatabaseIsolationStrict(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Create test databases
	resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE DATABASE strict_test_a"},
			{"statement": "CREATE DATABASE strict_test_b"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// Insert data in default database
	resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (n:Node {name: 'Default Node', id: 'default-1'})"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// Insert data in strict_test_a
	resp = makeRequest(t, server, "POST", "/db/strict_test_a/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (n:Node {name: 'Test A Node', id: 'test-a-1'})"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// Insert data in strict_test_b
	resp = makeRequest(t, server, "POST", "/db/strict_test_b/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (n:Node {name: 'Test B Node', id: 'test-b-1'})"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// Query default database - should ONLY see default database nodes
	resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n) RETURN n.name as name, n.id as id ORDER BY n.name"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	var result TransactionResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Results, 1)
	if len(result.Errors) > 0 {
		t.Fatalf("Query failed: %v", result.Errors)
	}

	// Verify we only see default database nodes
	names := make([]string, 0)
	for _, row := range result.Results[0].Data {
		if len(row.Row) >= 1 {
			if name, ok := row.Row[0].(string); ok {
				names = append(names, name)
			}
		}
	}

	// Should only contain "Default Node", not nodes from other databases
	for _, name := range names {
		assert.NotEqual(t, "Test A Node", name, "default database should NOT contain nodes from strict_test_a")
		assert.NotEqual(t, "Test B Node", name, "default database should NOT contain nodes from strict_test_b")
	}

	// Query strict_test_a using :USE - should ONLY see strict_test_a nodes
	resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": `:USE strict_test_a
MATCH (n)
RETURN n.name as name, n.id as id
ORDER BY n.name`},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Results, 1)
	if len(result.Errors) > 0 {
		t.Fatalf("Query failed: %v", result.Errors)
	}

	// Verify we only see strict_test_a nodes
	names = make([]string, 0)
	for _, row := range result.Results[0].Data {
		if len(row.Row) >= 1 {
			if name, ok := row.Row[0].(string); ok {
				names = append(names, name)
			}
		}
	}

	// Should only contain "Test A Node", not nodes from other databases
	require.Equal(t, 1, len(names), "strict_test_a should have exactly 1 node")
	assert.Equal(t, "Test A Node", names[0], "strict_test_a should contain its own node")
	for _, name := range names {
		assert.NotEqual(t, "Default Node", name, "strict_test_a should NOT contain nodes from default database")
		assert.NotEqual(t, "Test B Node", name, "strict_test_a should NOT contain nodes from strict_test_b")
	}

	// Cleanup
	resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "DROP DATABASE strict_test_a"},
			{"statement": "DROP DATABASE strict_test_b"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)
}

// TestUseCommandMultiStatement verifies that :USE applies to all statements in a multi-statement query
// This tests the exact scenario from MULTI_DB_E2E_TEST.md where :USE is followed by multiple CREATE statements
func TestUseCommandMultiStatement(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Create test databases
	resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE DATABASE multi_test_db"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// Insert data using :USE with multiple CREATE statements (like in MULTI_DB_E2E_TEST.md)
	// This simulates the exact query pattern:
	//   :USE test_db_b
	//   CREATE (charlie:Person {name: "Charlie", id: "b1", db: "test_db_b"})
	//   CREATE (diana:Person {name: "Diana", id: "b2", db: "test_db_b"})
	//   CREATE (order:Order {order_id: "ORD-001", amount: 1000, db: "test_db_b"})
	resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": `:USE multi_test_db
CREATE (charlie:Person {name: "Charlie", id: "b1", db: "multi_test_db"})
CREATE (diana:Person {name: "Diana", id: "b2", db: "multi_test_db"})
CREATE (order:Order {order_id: "ORD-001", amount: 1000, db: "multi_test_db"})
CREATE (charlie)-[:PLACED]->(order)
CREATE (diana)-[:PLACED]->(order)
RETURN charlie, diana, order`},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	var result TransactionResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Results, 1)
	if len(result.Errors) > 0 {
		t.Fatalf("Query failed: %v", result.Errors)
	}

	// First, count all nodes to see what was actually created
	resp = makeRequest(t, server, "POST", "/db/multi_test_db/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n) RETURN count(n) as count"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Results, 1)
	if len(result.Errors) > 0 {
		t.Fatalf("Count query failed: %v", result.Errors)
	}

	// Check node count
	nodeCount := 0
	if len(result.Results[0].Data) > 0 && len(result.Results[0].Data[0].Row) > 0 {
		if count, ok := result.Results[0].Data[0].Row[0].(float64); ok {
			nodeCount = int(count)
		}
	}
	t.Logf("Node count in multi_test_db: %d (expected 3)", nodeCount)
	require.Equal(t, 3, nodeCount, "multi_test_db should have exactly 3 nodes")

	// Query nodes and verify properties using direct property access (now that the bug is fixed)
	resp = makeRequest(t, server, "POST", "/db/multi_test_db/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n) RETURN labels(n) as labels, n.name as name, n.id as id, n.order_id as order_id, n.amount as amount ORDER BY labels[0], name, order_id"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Results, 1)
	if len(result.Errors) > 0 {
		t.Fatalf("Query failed: %v", result.Errors)
	}

	// Verify we can find all three nodes with their properties
	// Row format: [labels, name, id, order_id, amount]
	names := make(map[string]bool)
	ids := make(map[string]bool)
	hasOrder := false
	for _, row := range result.Results[0].Data {
		if len(row.Row) >= 2 {
			if name, ok := row.Row[1].(string); ok && name != "" {
				names[name] = true
			}
		}
		if len(row.Row) >= 3 {
			if id, ok := row.Row[2].(string); ok && id != "" {
				ids[id] = true
			}
		}
		if len(row.Row) >= 4 {
			if orderID, ok := row.Row[3].(string); ok && orderID == "ORD-001" {
				hasOrder = true
				// Verify amount
				if len(row.Row) >= 5 {
					var amountValue interface{}
					if amount, ok := row.Row[4].(float64); ok {
						amountValue = amount
						assert.Equal(t, float64(1000), amount, "Order should have amount 1000")
					} else if amount, ok := row.Row[4].(int64); ok {
						amountValue = amount
						assert.Equal(t, int64(1000), amount, "Order should have amount 1000")
					}
					require.NotNil(t, amountValue, "Order should have amount property")
				}
			}
		}
	}

	assert.True(t, names["Charlie"], "should have Charlie node")
	assert.True(t, names["Diana"], "should have Diana node")
	assert.True(t, ids["b1"], "should have Charlie with id b1")
	assert.True(t, ids["b2"], "should have Diana with id b2")
	assert.True(t, hasOrder, "should have Order node with order_id ORD-001")

	// Verify nodes are NOT in the default database
	resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n) WHERE n.id IN ['b1', 'b2'] OR n.order_id = 'ORD-001' RETURN n"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Results, 1)
	if len(result.Errors) > 0 {
		t.Fatalf("Query failed: %v", result.Errors)
	}

	// Should have 0 nodes in default database
	assert.Equal(t, 0, len(result.Results[0].Data), "default database should NOT contain nodes from multi_test_db")

	// Cleanup
	resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "DROP DATABASE multi_test_db"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)
}
