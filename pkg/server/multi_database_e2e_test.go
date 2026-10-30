// Package server provides end-to-end tests for multi-database functionality.
// These tests mirror the manual test sequence in docs/testing/MULTI_DB_E2E_TEST.md
package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
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
	ensureCompositeFixture := func(t *testing.T) {
		t.Helper()
		if !server.dbManager.Exists("test_db_a") {
			require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "test_db_a"}))
		}
		if !server.dbManager.Exists("test_db_b") {
			require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "test_db_b"}))
		}

		seedA := makeRequest(t, server, "POST", "/db/test_db_a/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "MERGE (alice:Person {id: 'a1'}) SET alice.name = 'Alice', alice.db = 'test_db_a'"},
				{"statement": "MERGE (bob:Person {id: 'a2'}) SET bob.name = 'Bob', bob.db = 'test_db_a'"},
				{"statement": "MERGE (company:Company {id: 'a3'}) SET company.name = 'Acme Corp', company.db = 'test_db_a'"},
				{"statement": "MATCH (a:Person {id: 'a1'}), (c:Company {id: 'a3'}) MERGE (a)-[:WORKS_FOR]->(c)"},
				{"statement": "MATCH (b:Person {id: 'a2'}), (c:Company {id: 'a3'}) MERGE (b)-[:WORKS_FOR]->(c)"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, seedA.Code)
		var seedARes TransactionResponse
		require.NoError(t, json.NewDecoder(seedA.Body).Decode(&seedARes))
		require.Empty(t, seedARes.Errors)

		seedB := makeRequest(t, server, "POST", "/db/test_db_b/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "MERGE (charlie:Person {id: 'b1'}) SET charlie.name = 'Charlie', charlie.db = 'test_db_b'"},
				{"statement": "MERGE (diana:Person {id: 'b2'}) SET diana.name = 'Diana', diana.db = 'test_db_b'"},
				{"statement": "MERGE (order:Order {order_id: 'ORD-001'}) SET order.owner_id = 'a1', order.amount = 1000, order.db = 'test_db_b'"},
				{"statement": "MATCH (c:Person {id: 'b1'}), (o:Order {order_id: 'ORD-001'}) MERGE (c)-[:PLACED]->(o)"},
				{"statement": "MATCH (d:Person {id: 'b2'}), (o:Order {order_id: 'ORD-001'}) MERGE (d)-[:PLACED]->(o)"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, seedB.Code)
		var seedBRes TransactionResponse
		require.NoError(t, json.NewDecoder(seedB.Body).Decode(&seedBRes))
		require.Empty(t, seedBRes.Errors)

		if !server.dbManager.IsCompositeDatabase("test_composite") {
			require.NoError(t, server.dbManager.CreateCompositeDatabase(t.Context(), "test_composite", []multidb.ConstituentRef{
				{Alias: "db_a", DatabaseName: "test_db_a", Type: "local", AccessMode: "read_write"},
				{Alias: "db_b", DatabaseName: "test_db_b", Type: "local", AccessMode: "read_write"},
			}))
		}
	}

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
				{"statement": "CREATE (order:Order {order_id: 'ORD-001', owner_id: 'a1', amount: 1000, db: 'test_db_b'})"},
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
		ensureCompositeFixture(t)

		toInt64Value := func(v interface{}) int64 {
			switch n := v.(type) {
			case float64:
				return int64(n)
			case int:
				return int64(n)
			case int64:
				return n
			default:
				t.Fatalf("expected numeric value, got %T (%v)", v, v)
				return 0
			}
		}

		require.True(t, server.dbManager.Exists("test_composite"), "composite database should exist before querying")

		// Plain root MATCH on composite should be rejected (strict Neo4j/Fabric semantics).
		resp := makeRequest(t, server, "POST", "/db/test_composite/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "MATCH (p:Person) RETURN p.name as name, p.db as db, labels(p) as labels ORDER BY p.name"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		var result TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.NotEmpty(t, result.Errors, "root composite MATCH must be rejected without constituent target")

		// Query each constituent from the composite connection via USE.
		personCount := int64(0)
		totalNodeCount := int64(0)
		labelsFound := make(map[string]bool)
		totalLabelCount := int64(0)
		nameToLabels := make(map[string][]string)
		orderLabelFound := false

		for _, alias := range []string{"db_a", "db_b"} {
			// Person rows by constituent.
			resp = makeRequest(t, server, "POST", "/db/test_composite/tx/commit", map[string]interface{}{
				"statements": []map[string]interface{}{
					{"statement": fmt.Sprintf("USE test_composite.%s MATCH (p:Person) RETURN p.name as name, p.db as db, labels(p) as labels ORDER BY p.name", alias)},
				},
			}, "Bearer "+token)
			require.Equal(t, http.StatusOK, resp.Code)
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
			require.Len(t, result.Results, 1)
			require.Empty(t, result.Errors)
			personCount += int64(len(result.Results[0].Data))

			// Total nodes by constituent.
			resp = makeRequest(t, server, "POST", "/db/test_composite/tx/commit", map[string]interface{}{
				"statements": []map[string]interface{}{
					{"statement": fmt.Sprintf("USE test_composite.%s MATCH (n) RETURN count(n) as total_nodes", alias)},
				},
			}, "Bearer "+token)
			require.Equal(t, http.StatusOK, resp.Code)
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
			require.Len(t, result.Results, 1)
			require.Empty(t, result.Errors)
			totalNodeCount += toInt64Value(result.Results[0].Data[0].Row[0])

			// Labels per constituent.
			resp = makeRequest(t, server, "POST", "/db/test_composite/tx/commit", map[string]interface{}{
				"statements": []map[string]interface{}{
					{"statement": fmt.Sprintf("USE test_composite.%s MATCH (n) UNWIND labels(n) as label RETURN label, count(*) as count ORDER BY label", alias)},
				},
			}, "Bearer "+token)
			require.Equal(t, http.StatusOK, resp.Code)
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
			require.Len(t, result.Results, 1)
			require.Empty(t, result.Errors)
			for _, row := range result.Results[0].Data {
				if len(row.Row) >= 2 {
					label, _ := row.Row[0].(string)
					if label != "" {
						labelsFound[label] = true
					}
					totalLabelCount += toInt64Value(row.Row[1])
				}
			}

			// Node names + labels per constituent.
			resp = makeRequest(t, server, "POST", "/db/test_composite/tx/commit", map[string]interface{}{
				"statements": []map[string]interface{}{
					{"statement": fmt.Sprintf("USE test_composite.%s MATCH (n) RETURN n.name as name, labels(n) as labels ORDER BY name", alias)},
				},
			}, "Bearer "+token)
			require.Equal(t, http.StatusOK, resp.Code)
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
			require.Len(t, result.Results, 1)
			require.Empty(t, result.Errors)
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
		}

		assert.Equal(t, int64(4), personCount, "composite routed queries should see 4 Person nodes")
		assert.Equal(t, int64(6), totalNodeCount, "composite routed queries should see 6 total nodes (3+3)")

		require.Contains(t, nameToLabels, "Acme Corp", "Company node should be present")
		require.Contains(t, nameToLabels, "Alice", "Person nodes should be present")
		require.Contains(t, nameToLabels, "Bob", "Person nodes should be present")
		require.Contains(t, nameToLabels, "Charlie", "Person nodes should be present")
		require.Contains(t, nameToLabels, "Diana", "Person nodes should be present")

		assert.Contains(t, nameToLabels["Acme Corp"], "Company", "Acme Corp should have Company label")
		assert.True(t, orderLabelFound, "composite view should include a node with Order label; labels map=%+v", nameToLabels)

		require.Equal(t, 3, len(labelsFound), "expected exactly 3 label types (Company, Order, Person), got labelsFound=%+v, nameToLabels=%+v", labelsFound, nameToLabels)
		assert.True(t, labelsFound["Company"], "should have Company label")
		assert.True(t, labelsFound["Order"], "should have Order label")
		assert.True(t, labelsFound["Person"], "should have Person label")
		assert.Equal(t, int64(6), totalLabelCount, "total count across all labels should be 6")

		t.Run("CorrelatedSubqueryJoin_WithParams_MultiRow", func(t *testing.T) {
			resp := makeRequest(t, server, "POST", "/db/test_composite/tx/commit", map[string]interface{}{
				"statements": []map[string]interface{}{
					{
						"statement": `USE test_composite
CALL {
  USE test_composite.db_a
  MATCH (p:Person)
  WHERE p.id IN $person_ids
  RETURN p.id AS person_id, p.name AS person_name
  ORDER BY person_id
  LIMIT $outer_limit
}
CALL {
  USE test_composite.db_b
  OPTIONAL MATCH (o:Order)
  WHERE o.owner_id = person_id AND o.amount >= $min_amount
  RETURN collect(o.order_id) AS order_ids, count(o) AS order_count
}
RETURN person_id, person_name, order_ids, order_count`,
						"parameters": map[string]interface{}{
							"person_ids":  []string{"a1", "a2"},
							"outer_limit": 2,
							"min_amount":  100,
						},
					},
				},
			}, "Bearer "+token)
			require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())

			var result TransactionResponse
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
			require.Empty(t, result.Errors, "correlated composite join should not error")
			require.Len(t, result.Results, 1)
			require.Len(t, result.Results[0].Data, 2, "expected two joined rows for a1 and a2")

			rowsByID := make(map[string][]interface{}, 2)
			for _, row := range result.Results[0].Data {
				require.Len(t, row.Row, 4)
				id, ok := row.Row[0].(string)
				require.True(t, ok)
				rowsByID[id] = row.Row
			}
			require.Contains(t, rowsByID, "a1")
			require.Contains(t, rowsByID, "a2")

			rowA1 := rowsByID["a1"]
			require.Equal(t, "Alice", rowA1[1])
			orderIDs0, ok := rowA1[2].([]interface{})
			require.True(t, ok, "order_ids should be a list")
			require.Len(t, orderIDs0, 1)
			require.Equal(t, "ORD-001", orderIDs0[0])
			assert.Equal(t, int64(1), toInt64Value(rowA1[3]))

			rowA2 := rowsByID["a2"]
			require.Equal(t, "Bob", rowA2[1])
			orderIDs1, ok := rowA2[2].([]interface{})
			require.True(t, ok, "order_ids should be a list")
			require.Len(t, orderIDs1, 0)
			assert.Equal(t, int64(0), toInt64Value(rowA2[3]))
		})

		t.Run("CorrelatedSubqueryJoin_WithThenUse_CollectSemantics", func(t *testing.T) {
			// Same clause ordering as user query shape: WITH import first, then USE in subquery.
			resp := makeRequest(t, server, "POST", "/db/test_composite/tx/commit", map[string]interface{}{
				"statements": []map[string]interface{}{
					{
						"statement": `USE test_composite
CALL {
  USE test_composite.db_a
  MATCH (t:Person)
  WHERE t.id IN ['a1', 'a2']
  RETURN t.id AS textKey128
  ORDER BY textKey128
}
CALL {
  WITH textKey128
  USE test_composite.db_b
  MATCH (tt:Order)
  WHERE tt.owner_id = textKey128
  RETURN collect(tt.order_id) AS texts
}
RETURN textKey128, texts
ORDER BY textKey128`,
					},
				},
			}, "Bearer "+token)
			require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())

			var result TransactionResponse
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
			require.Empty(t, result.Errors, "WITH ... USE ... collect() flow should not error")
			require.Len(t, result.Results, 1)
			require.NotEmpty(t, result.Results[0].Data)

			row0 := result.Results[0].Data[0].Row
			var row0Key interface{}
			var row0Texts interface{}
			if len(row0) == 1 {
				if m, ok := row0[0].(map[string]interface{}); ok {
					row0Key = m["textKey128"]
					row0Texts = m["texts"]
				}
			}
			if row0Key == nil && row0Texts == nil {
				require.Len(t, row0, 2)
				row0Key = row0[0]
				row0Texts = row0[1]
			}
			require.Equal(t, "a1", row0Key)

			texts0, ok := row0Texts.([]interface{})
			require.True(t, ok, "texts should be a list")
			require.Len(t, texts0, 1)
			require.Equal(t, "ORD-001", texts0[0])

			if len(result.Results[0].Data) > 1 {
				row1 := result.Results[0].Data[1].Row
				var row1Key interface{}
				var row1Texts interface{}
				if len(row1) == 1 {
					if m, ok := row1[0].(map[string]interface{}); ok {
						row1Key = m["textKey128"]
						row1Texts = m["texts"]
					}
				}
				if row1Key == nil && row1Texts == nil {
					require.Len(t, row1, 2)
					row1Key = row1[0]
					row1Texts = row1[1]
				}
				require.Equal(t, "a2", row1Key)
				texts1, ok := row1Texts.([]interface{})
				require.True(t, ok, "texts should be a list")
				require.Len(t, texts1, 0)
			}
		})
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
		// Check for errors
		var dropResult TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&dropResult))
		if len(dropResult.Errors) > 0 {
			t.Fatalf("DROP COMPOSITE DATABASE test_composite failed: %v", dropResult.Errors)
		}
		t.Logf("DROP COMPOSITE DATABASE result: %+v", dropResult)

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
		// Debug: log what we got
		var foundNames []string
		for _, row := range result.Results[0].Data {
			if len(row.Row) > 0 {
				if name, ok := row.Row[0].(string); ok {
					foundNames = append(foundNames, name)
					if name == "test_composite" {
						t.Logf("ERROR: Found test_composite in SHOW COMPOSITE DATABASES after dropping!")
					}
				}
			}
		}
		t.Logf("Composite databases after drop: %v", foundNames)
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
		assert.False(t, found, "test_composite should not exist after dropping. Found: %v", foundNames)

		// Verify constituent databases still exist
		resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "SHOW DATABASES"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Results, 1)
		// Debug: log what databases we have
		var dbNames []string
		for _, row := range result.Results[0].Data {
			if len(row.Row) > 0 {
				if name, ok := row.Row[0].(string); ok {
					dbNames = append(dbNames, name)
				}
			}
		}
		t.Logf("Databases after dropping composite: %v (count: %d)", dbNames, len(result.Results[0].Data))
		// Should still have test_db_a and test_db_b (plus nornic and system = 4 total)
		assert.GreaterOrEqual(t, len(result.Results[0].Data), 4, "should still have constituent databases. Found: %v", dbNames)
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
		// Check for errors
		var dropResult TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&dropResult))
		if len(dropResult.Errors) > 0 {
			t.Fatalf("DROP DATABASE test_db_a failed: %v", dropResult.Errors)
		}

		// Drop second test database
		resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "DROP DATABASE test_db_b"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)
		// Check for errors
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&dropResult))
		if len(dropResult.Errors) > 0 {
			t.Fatalf("DROP DATABASE test_db_b failed: %v", dropResult.Errors)
		}

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
		// Debug: log what databases we have
		var dbNames []string
		for _, row := range result.Results[0].Data {
			if len(row.Row) > 0 {
				if name, ok := row.Row[0].(string); ok {
					dbNames = append(dbNames, name)
				}
			}
		}
		t.Logf("Databases after dropping test databases: %v (count: %d)", dbNames, len(result.Results[0].Data))
		// Should only have nornic and system
		assert.LessOrEqual(t, len(result.Results[0].Data), 2, "should only have default and system databases. Found: %v", dbNames)
	})
}

func TestMultiDatabase_E2E_DiscoveryAndCreateDatabaseScenarios_SharedFixture(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	t.Run("discovery endpoint returns default database", func(t *testing.T) {
		resp := makeRequest(t, server, "GET", "/", nil, "")
		require.Equal(t, http.StatusOK, resp.Code)

		var discovery map[string]interface{}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&discovery))
		defaultDB, ok := discovery["default_database"]
		require.True(t, ok, "discovery endpoint should include default_database field")
		assert.Equal(t, "nornic", defaultDB, "default database should be 'nornic'")
	})

	t.Run("create database via default database endpoint", func(t *testing.T) {
		resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "CREATE DATABASE animals"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code, "CREATE DATABASE animals must return 200")

		var createResult TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&createResult))
		require.Empty(t, createResult.Errors)

		resp = makeRequest(t, server, "POST", "/db/animals/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "CREATE (n:Animal {name: 'test'}) RETURN n"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&createResult))
		require.Empty(t, createResult.Errors)

		_ = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "DROP DATABASE animals"},
			},
		}, "Bearer "+token)
	})

	t.Run("create database via system endpoint with backticks", func(t *testing.T) {
		resp := makeRequest(t, server, "POST", "/db/system/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "CREATE DATABASE `animals`"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		var result TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Empty(t, result.Errors)
		require.Len(t, result.Results, 1)
		require.NotEmpty(t, result.Results[0].Columns)
		require.NotEmpty(t, result.Results[0].Data)
		assert.Equal(t, "name", result.Results[0].Columns[0])
		require.Len(t, result.Results[0].Data[0].Row, 1)
		assert.Equal(t, "animals", result.Results[0].Data[0].Row[0])

		_ = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{{"statement": "DROP DATABASE animals"}},
		}, "Bearer "+token)
	})
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

// TestExactUserScenario reproduces the exact user scenario to verify property access
func TestExactUserScenario(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Create test_db_b database
	resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE DATABASE test_db_b"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// Exact CREATE query from user
	resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": `:USE test_db_b
CREATE (charlie:Person {name: "Charlie", id: "b1", db: "test_db_b"})
CREATE (diana:Person {name: "Diana", id: "b2", db: "test_db_b"})
CREATE (order:Order {order_id: "ORD-001", amount: 1000, db: "test_db_b"})
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
		t.Fatalf("CREATE query failed: %v", result.Errors)
	}

	// Exact MATCH query from user
	resp = makeRequest(t, server, "POST", "/db/test_db_b/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": `MATCH (n)
RETURN n.name as name, n.order_id as order_id, labels(n) as labels, n.db as db
ORDER BY n.name`},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Results, 1)
	if len(result.Errors) > 0 {
		t.Fatalf("MATCH query failed: %v", result.Errors)
	}

	// Verify results - should have 3 nodes (Charlie, Diana, Order)
	require.GreaterOrEqual(t, len(result.Results[0].Data), 3, "should have at least 3 nodes")

	// Check that properties are correctly returned (not null when they should have values)
	foundCharlie := false
	foundDiana := false
	foundOrder := false

	for _, row := range result.Results[0].Data {
		name := row.Row[0]
		orderID := row.Row[1]
		db := row.Row[3]

		// Verify db property is set
		if dbVal, ok := db.(string); ok {
			assert.Equal(t, "test_db_b", dbVal, "db property should be test_db_b")
		} else {
			t.Errorf("db property should be a string, got %T: %v", db, db)
		}

		// Check Person nodes - should have name, null for order_id
		if nameVal, ok := name.(string); ok && nameVal == "Charlie" {
			foundCharlie = true
			// Person nodes don't have order_id, so null is correct
			assert.Nil(t, orderID, "Charlie (Person) should not have order_id property")
		} else if nameVal, ok := name.(string); ok && nameVal == "Diana" {
			foundDiana = true
			// Person nodes don't have order_id, so null is correct
			assert.Nil(t, orderID, "Diana (Person) should not have order_id property")
		}

		// Check Order node - should have order_id, null for name
		if orderIDVal, ok := orderID.(string); ok && orderIDVal == "ORD-001" {
			foundOrder = true
			// Order nodes don't have name, so null is correct
			assert.Nil(t, name, "Order should not have name property")
		}
	}

	assert.True(t, foundCharlie, "should find Charlie node with name property")
	assert.True(t, foundDiana, "should find Diana node with name property")
	assert.True(t, foundOrder, "should find Order node with order_id property")

	// Cleanup
	resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "DROP DATABASE test_db_b"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)
}

// TestDatabaseIsolation_DefaultDatabase verifies that nodes from other databases are NOT visible in default database
func TestDatabaseIsolation_DefaultDatabase(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Create test databases
	resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE DATABASE test_db_a"},
			{"statement": "CREATE DATABASE test_db_b"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// Create nodes in test_db_a
	resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": `:USE test_db_a
CREATE (alice:Person {name: "Alice", id: "a1", db: "test_db_a"})
CREATE (bob:Person {name: "Bob", id: "a2", db: "test_db_a"})`},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// Create nodes in test_db_b
	resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": `:USE test_db_b
CREATE (charlie:Person {name: "Charlie", id: "b1", db: "test_db_b"})
CREATE (diana:Person {name: "Diana", id: "b2", db: "test_db_b"})
CREATE (order:Order {order_id: "ORD-001", amount: 1000, db: "test_db_b"})`},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// Query default database - should NOT see nodes from test_db_a or test_db_b
	resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": `MATCH (n)
WHERE n.db IN ["test_db_a", "test_db_b"]
RETURN count(n) as test_db_nodes`},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	var result TransactionResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Results, 1)
	if len(result.Errors) > 0 {
		t.Fatalf("Query failed: %v", result.Errors)
	}

	// Should return 0 - nodes from test_db_a and test_db_b should NOT be visible in default database
	require.GreaterOrEqual(t, len(result.Results[0].Data), 1, "should have at least one row")
	if len(result.Results[0].Data) > 0 {
		count := result.Results[0].Data[0].Row[0]
		if countVal, ok := count.(float64); ok {
			assert.Equal(t, float64(0), countVal, "default database should NOT see nodes from test_db_a or test_db_b")
		} else if countVal, ok := count.(int64); ok {
			assert.Equal(t, int64(0), countVal, "default database should NOT see nodes from test_db_a or test_db_b")
		} else {
			t.Errorf("count should be a number, got %T: %v", count, count)
		}
	}

	// Verify nodes ARE visible in their respective databases
	resp = makeRequest(t, server, "POST", "/db/test_db_a/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n) RETURN count(n) as count"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Results, 1, "expected one result set for test_db_a")
	if len(result.Results[0].Data) > 0 {
		count := result.Results[0].Data[0].Row[0]
		if countVal, ok := count.(float64); ok {
			assert.Equal(t, float64(2), countVal, "test_db_a should have 2 nodes")
		} else if countVal, ok := count.(int64); ok {
			assert.Equal(t, int64(2), countVal, "test_db_a should have 2 nodes")
		}
	}

	resp = makeRequest(t, server, "POST", "/db/test_db_b/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n) RETURN count(n) as count"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Results, 1, "expected one result set for test_db_b")
	if len(result.Results[0].Data) > 0 {
		count := result.Results[0].Data[0].Row[0]
		if countVal, ok := count.(float64); ok {
			assert.Equal(t, float64(3), countVal, "test_db_b should have 3 nodes")
		} else if countVal, ok := count.(int64); ok {
			assert.Equal(t, int64(3), countVal, "test_db_b should have 3 nodes")
		}
	}

	// Cleanup
	resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "DROP DATABASE test_db_a"},
			{"statement": "DROP DATABASE test_db_b"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)
}
