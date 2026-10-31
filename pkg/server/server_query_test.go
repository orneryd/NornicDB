package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/auth"
	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/txsession"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Transaction Endpoint Tests (Neo4j Compatible)
// =============================================================================

func TestHandleImplicitTransaction(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	tests := []struct {
		name       string
		body       map[string]interface{}
		wantStatus int
	}{
		{
			name: "valid query",
			body: map[string]interface{}{
				"statements": []map[string]interface{}{
					{"statement": "MATCH (n) RETURN n LIMIT 10"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "multiple statements",
			body: map[string]interface{}{
				"statements": []map[string]interface{}{
					{"statement": "MATCH (n) RETURN count(n) AS count"},
					{"statement": "MATCH (n) RETURN n LIMIT 5"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty statements",
			body:       map[string]interface{}{"statements": []map[string]interface{}{}},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", tt.body, "Bearer "+token)

			if resp.Code != tt.wantStatus {
				t.Errorf("expected status %d, got %d: %s", tt.wantStatus, resp.Code, resp.Body.String())
			}

			// Check Neo4j response format
			var txResp map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&txResp); err != nil {
				t.Fatalf("failed to decode response: %v", err)
			}

			if _, ok := txResp["results"]; !ok {
				t.Error("missing 'results' field in response")
			}
			if _, ok := txResp["errors"]; !ok {
				t.Error("missing 'errors' field in response")
			}
		})
	}
}

func TestHandleOpenTransaction(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Open a new transaction
	resp := makeRequest(t, server, "POST", "/db/nornic/tx", map[string]interface{}{
		"statements": []map[string]interface{}{},
	}, "Bearer "+token)

	if resp.Code != http.StatusCreated {
		t.Errorf("expected status 201, got %d: %s", resp.Code, resp.Body.String())
	}

	// Check that commit URL is returned
	var txResp map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&txResp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if txResp["commit"] == nil {
		t.Error("missing 'commit' URL in response")
	}
}

func TestHandleOpenTransaction_CompositeRemoteOpenSucceeds(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	var remoteEngines []storage.Engine
	t.Cleanup(func() {
		for _, engine := range remoteEngines {
			_ = engine.Close()
		}
	})
	dbManager, err := multidb.NewDatabaseManager(t.Context(), server.db.GetBaseStorageForManager(), &multidb.Config{
		DefaultDatabase: "nornic",
		SystemDatabase:  "system",
		RemoteEngineFactory: func(_ multidb.ConstituentRef, authToken string) (storage.Engine, error) {
			_ = authToken
			engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			remoteEngines = append(remoteEngines, engine)
			return engine, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, dbManager.CreateCompositeDatabase(t.Context(), "comp_remote", []multidb.ConstituentRef{
		{
			Alias:        "r1",
			DatabaseName: "remote_db",
			Type:         "remote",
			AccessMode:   "read_write",
			URI:          "http://remote.example",
		},
	}))

	server.dbManager = dbManager
	server.txSessions = txsession.NewManager(30*time.Second, server.newExecutorForDatabase)

	resp := makeRequest(t, server, "POST", "/db/comp_remote/tx", map[string]interface{}{
		"statements": []map[string]interface{}{},
	}, "Bearer "+token)
	// Composite explicit transactions open a fabric coordinator on BEGIN. Remote
	// participant auth forwarding is exercised in the lower-level multidb/cypher
	// tests; at the HTTP layer we only require the transaction open to succeed.
	require.Equal(t, http.StatusCreated, resp.Code, resp.Body.String())
	var txResp map[string]interface{}
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &txResp))
	require.NotNil(t, txResp["commit"])
}

func TestCompositeExplicitTx_SecondWriteShardErrorCode(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	dbManager, err := multidb.NewDatabaseManager(t.Context(), server.db.GetBaseStorageForManager(), &multidb.Config{
		DefaultDatabase: "nornic",
		SystemDatabase:  "system",
	})
	require.NoError(t, err)
	require.NoError(t, dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "shard_a"}))
	require.NoError(t, dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "shard_b"}))
	require.NoError(t, dbManager.CreateCompositeDatabase(t.Context(), "cmp_tx_code", []multidb.ConstituentRef{
		{Alias: "a", DatabaseName: "shard_a", Type: "local", AccessMode: "read_write"},
		{Alias: "b", DatabaseName: "shard_b", Type: "local", AccessMode: "read_write"},
	}))
	server.dbManager = dbManager
	server.txSessions = txsession.NewManager(30*time.Second, server.newExecutorForDatabase)

	// Open explicit transaction.
	openResp := makeRequest(t, server, "POST", "/db/cmp_tx_code/tx", map[string]interface{}{"statements": []map[string]interface{}{}}, "Bearer "+token)
	require.Equal(t, http.StatusCreated, openResp.Code, openResp.Body.String())
	var open map[string]interface{}
	require.NoError(t, json.Unmarshal(openResp.Body.Bytes(), &open))
	commitURL, _ := open["commit"].(string)
	require.NotEmpty(t, commitURL)
	txPath := strings.TrimSuffix(strings.TrimPrefix(commitURL, "http://localhost:7474"), "/commit")

	// First write on shard a succeeds.
	firstResp := makeRequest(t, server, "POST", txPath, map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CALL { USE cmp_tx_code.a CREATE (n:W {id:'1'}) RETURN count(n) AS c } RETURN c"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, firstResp.Code, firstResp.Body.String())

	// Second write on shard b in same tx fails with Neo4j's AccessMode
	// "Writing to more than one database per transaction is not allowed".
	secondResp := makeRequest(t, server, "POST", txPath, map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CALL { USE cmp_tx_code.b CREATE (n:W {id:'2'}) RETURN count(n) AS c } RETURN c"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, secondResp.Code, secondResp.Body.String())

	var body map[string]interface{}
	require.NoError(t, json.Unmarshal(secondResp.Body.Bytes(), &body))
	errs, ok := body["errors"].([]interface{})
	require.True(t, ok)
	require.NotEmpty(t, errs)
	firstErr, ok := errs[0].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "Neo.ClientError.Statement.AccessMode", firstErr["code"])
}

func TestDatabaseScopedExecutorAuthBranches(t *testing.T) {
	server, _ := setupTestServer(t)

	// Non-remote requests share the cached executor.
	execA, err := server.executorCache.Get("nornic")
	require.NoError(t, err)
	execB, err := server.executorCache.Get("nornic")
	require.NoError(t, err)
	require.Equal(t, execA, execB)
	require.False(t, server.databaseHasRemoteConstituent("nornic"))
	require.False(t, server.databaseHasRemoteConstituent("missing_db"))

	var tokenSeen string
	var remoteEngines []storage.Engine
	t.Cleanup(func() {
		for _, engine := range remoteEngines {
			_ = engine.Close()
		}
	})
	dbManager, err := multidb.NewDatabaseManager(t.Context(), server.db.GetBaseStorageForManager(), &multidb.Config{
		DefaultDatabase: "nornic",
		SystemDatabase:  "system",
		RemoteEngineFactory: func(_ multidb.ConstituentRef, authToken string) (storage.Engine, error) {
			tokenSeen = authToken
			engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			remoteEngines = append(remoteEngines, engine)
			return engine, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, dbManager.CreateCompositeDatabase(t.Context(), "comp_remote_exec", []multidb.ConstituentRef{
		{
			Alias:        "r1",
			DatabaseName: "remote_db",
			Type:         "remote",
			AccessMode:   "read",
			URI:          "http://remote.example",
		},
	}))
	server.dbManager = dbManager
	server.executors = make(map[string]*cypher.StorageExecutor)

	require.True(t, server.databaseHasRemoteConstituent("comp_remote_exec"))
	execRemote, err := cypher.NewDatabaseScopedExecutor(server.databaseScopedExecutorOptions("comp_remote_exec", "Bearer remote"))
	require.NoError(t, err)
	require.NotNil(t, execRemote)
	require.Equal(t, "Bearer remote", tokenSeen)

	// Factory error propagation path.
	badManager, err := multidb.NewDatabaseManager(t.Context(), server.db.GetBaseStorageForManager(), &multidb.Config{
		DefaultDatabase: "nornic",
		SystemDatabase:  "system",
		RemoteEngineFactory: func(_ multidb.ConstituentRef, _ string) (storage.Engine, error) {
			return nil, fmt.Errorf("remote dial failed")
		},
	})
	require.NoError(t, err)
	require.NoError(t, badManager.CreateCompositeDatabase(t.Context(), "comp_remote_bad", []multidb.ConstituentRef{
		{
			Alias:        "r1",
			DatabaseName: "remote_db",
			Type:         "remote",
			AccessMode:   "read",
			URI:          "http://remote.example",
		},
	}))
	server.dbManager = badManager
	_, err = cypher.NewDatabaseScopedExecutor(server.databaseScopedExecutorOptions("comp_remote_bad", "Bearer token"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "remote dial failed")
}

func TestExplicitTransactionWorkflow(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Step 1: Open transaction
	openResp := makeRequest(t, server, "POST", "/db/nornic/tx", map[string]interface{}{
		"statements": []map[string]interface{}{},
	}, "Bearer "+token)

	if openResp.Code != http.StatusCreated {
		t.Fatalf("failed to open transaction: %d", openResp.Code)
	}

	var openResult map[string]interface{}
	json.NewDecoder(openResp.Body).Decode(&openResult)

	commitURL := openResult["commit"].(string)
	// Extract transaction ID from commit URL
	parts := strings.Split(commitURL, "/")
	txID := parts[len(parts)-2] // Format: /db/nornic/tx/{txId}/commit

	// Step 2: Execute in transaction
	execResp := makeRequest(t, server, "POST", fmt.Sprintf("/db/nornic/tx/%s", txID), map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n) RETURN count(n) AS count"},
		},
	}, "Bearer "+token)

	if execResp.Code != http.StatusOK {
		t.Errorf("expected status 200 for execute, got %d: %s", execResp.Code, execResp.Body.String())
	}

	// Step 3: Commit transaction
	commitResp := makeRequest(t, server, "POST", fmt.Sprintf("/db/nornic/tx/%s/commit", txID), map[string]interface{}{
		"statements": []map[string]interface{}{},
	}, "Bearer "+token)

	if commitResp.Code != http.StatusOK {
		t.Errorf("expected status 200 for commit, got %d: %s", commitResp.Code, commitResp.Body.String())
	}
}

func TestRollbackTransaction(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Open transaction
	openResp := makeRequest(t, server, "POST", "/db/nornic/tx", map[string]interface{}{
		"statements": []map[string]interface{}{},
	}, "Bearer "+token)

	if openResp.Code != http.StatusCreated {
		t.Fatalf("failed to open transaction: %d", openResp.Code)
	}

	var openResult map[string]interface{}
	json.NewDecoder(openResp.Body).Decode(&openResult)

	commitURL := openResult["commit"].(string)
	parts := strings.Split(commitURL, "/")
	txID := parts[len(parts)-2]

	// Rollback transaction
	rollbackResp := makeRequest(t, server, "DELETE", fmt.Sprintf("/db/nornic/tx/%s", txID), nil, "Bearer "+token)

	if rollbackResp.Code != http.StatusOK {
		t.Errorf("expected status 200 for rollback, got %d", rollbackResp.Code)
	}
}

func TestExplicitTransactionRollbackRevertsCreate(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	openResp := makeRequest(t, server, "POST", "/db/nornic/tx", map[string]interface{}{
		"statements": []map[string]interface{}{},
	}, "Bearer "+token)
	require.Equal(t, http.StatusCreated, openResp.Code, openResp.Body.String())

	var openResult map[string]interface{}
	require.NoError(t, json.NewDecoder(openResp.Body).Decode(&openResult))
	commitURL, ok := openResult["commit"].(string)
	require.True(t, ok)
	parts := strings.Split(commitURL, "/")
	txID := parts[len(parts)-2]

	execResp := makeRequest(t, server, "POST", fmt.Sprintf("/db/nornic/tx/%s", txID), map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (n:Test {name: 'Transaction Test'})"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, execResp.Code, execResp.Body.String())
	assertTxResponseHasNoErrors(t, execResp)

	rollbackResp := makeRequest(t, server, "DELETE", fmt.Sprintf("/db/nornic/tx/%s", txID), nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, rollbackResp.Code, rollbackResp.Body.String())

	verifyResp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n:Test {name: 'Transaction Test'}) RETURN count(n) AS cnt"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, verifyResp.Code, verifyResp.Body.String())
	require.Equal(t, int64(0), extractCountFromTxResponse(t, verifyResp))
}

func TestExplicitTransactionRollbackRevertsNodeQuery(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Cleanup outside explicit transaction.
	cleanupResp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n:Test {name: 'Transaction Test'}) DETACH DELETE n"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, cleanupResp.Code, cleanupResp.Body.String())

	openResp := makeRequest(t, server, "POST", "/db/nornic/tx", map[string]interface{}{
		"statements": []map[string]interface{}{},
	}, "Bearer "+token)
	require.Equal(t, http.StatusCreated, openResp.Code, openResp.Body.String())

	var openResult map[string]interface{}
	require.NoError(t, json.NewDecoder(openResp.Body).Decode(&openResult))
	commitURL, ok := openResult["commit"].(string)
	require.True(t, ok)
	parts := strings.Split(commitURL, "/")
	txID := parts[len(parts)-2]

	execResp := makeRequest(t, server, "POST", fmt.Sprintf("/db/nornic/tx/%s", txID), map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (n:Test {name: 'Transaction Test'})"},
			{"statement": "MATCH (n:Test {name: 'Transaction Test'}) RETURN n"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, execResp.Code, execResp.Body.String())
	assertTxResponseHasNoErrors(t, execResp)

	rollbackResp := makeRequest(t, server, "DELETE", fmt.Sprintf("/db/nornic/tx/%s", txID), nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, rollbackResp.Code, rollbackResp.Body.String())

	verifyResp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n:Test {name: 'Transaction Test'}) RETURN n"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, verifyResp.Code, verifyResp.Body.String())
	require.Equal(t, 0, extractRowsLenFromTxResponse(t, verifyResp))
}

func TestImplicitTransactionSingleStatementRollsBackOnError(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{
				"statement": `
CREATE (n:ImplicitRollback {id: 1})
WITH n
CREAT (m:ImplicitRollback {id: 2})
RETURN n
`,
			},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())

	var txResp map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&txResp))
	errorsList, ok := txResp["errors"].([]interface{})
	require.True(t, ok)
	require.Greater(t, len(errorsList), 0)

	verifyResp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n:ImplicitRollback) RETURN count(n) AS cnt"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, verifyResp.Code, verifyResp.Body.String())
	require.Equal(t, int64(0), extractCountFromTxResponse(t, verifyResp))
}

func TestExplicitTransactionCommitPersistsCreate(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	openResp := makeRequest(t, server, "POST", "/db/nornic/tx", map[string]interface{}{
		"statements": []map[string]interface{}{},
	}, "Bearer "+token)
	require.Equal(t, http.StatusCreated, openResp.Code, openResp.Body.String())

	var openResult map[string]interface{}
	require.NoError(t, json.NewDecoder(openResp.Body).Decode(&openResult))
	commitURL, ok := openResult["commit"].(string)
	require.True(t, ok)
	parts := strings.Split(commitURL, "/")
	txID := parts[len(parts)-2]

	execResp := makeRequest(t, server, "POST", fmt.Sprintf("/db/nornic/tx/%s", txID), map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (n:TxCommitVerify {name: 'commit survives'})"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, execResp.Code, execResp.Body.String())
	assertTxResponseHasNoErrors(t, execResp)

	commitResp := makeRequest(t, server, "POST", fmt.Sprintf("/db/nornic/tx/%s/commit", txID), map[string]interface{}{
		"statements": []map[string]interface{}{},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, commitResp.Code, commitResp.Body.String())
	assertTxResponseHasNoErrors(t, commitResp)

	verifyResp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n:TxCommitVerify {name: 'commit survives'}) RETURN count(n) AS cnt"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, verifyResp.Code, verifyResp.Body.String())
	require.Equal(t, int64(1), extractCountFromTxResponse(t, verifyResp))

	cleanupResp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n:TxCommitVerify {name: 'commit survives'}) DETACH DELETE n"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, cleanupResp.Code, cleanupResp.Body.String())
	assertTxResponseHasNoErrors(t, cleanupResp)
}

// =============================================================================
// Query Endpoint Tests
// =============================================================================

func TestHandleQuery(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Use Neo4j-compatible endpoint for queries
	tests := []struct {
		name       string
		body       map[string]interface{}
		wantStatus int
	}{
		{
			name: "valid match query",
			body: map[string]interface{}{
				"statements": []map[string]interface{}{
					{"statement": "MATCH (n) RETURN n LIMIT 10"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "query with parameters",
			body: map[string]interface{}{
				"statements": []map[string]interface{}{
					{
						"statement":  "MATCH (n) WHERE n.name = $name RETURN n",
						"parameters": map[string]interface{}{"name": "test"},
					},
				},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", tt.body, "Bearer "+token)

			if resp.Code != tt.wantStatus {
				t.Errorf("expected status %d, got %d: %s", tt.wantStatus, resp.Code, resp.Body.String())
			}
		})
	}
}

// =============================================================================
// Node/Edge via Cypher Tests (Neo4j-compatible approach)
// =============================================================================

func TestNodesCRUDViaCypher(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Create a node via Cypher
	createResp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (n:Person {name: 'Test User'}) RETURN n"},
		},
	}, "Bearer "+token)

	if createResp.Code != http.StatusOK {
		t.Errorf("expected status 200 for create node, got %d: %s", createResp.Code, createResp.Body.String())
	}

	// Query nodes
	queryResp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n:Person) RETURN n"},
		},
	}, "Bearer "+token)

	if queryResp.Code != http.StatusOK {
		t.Errorf("expected status 200 for query, got %d", queryResp.Code)
	}
}

func TestEdgesCRUDViaCypher(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Create two nodes and a relationship via Cypher
	createResp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, r, b"},
		},
	}, "Bearer "+token)

	if createResp.Code != http.StatusOK {
		t.Errorf("expected status 200 for create, got %d: %s", createResp.Code, createResp.Body.String())
	}

	// Query relationships
	queryResp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (a)-[r:KNOWS]->(b) RETURN a.name, r, b.name"},
		},
	}, "Bearer "+token)

	if queryResp.Code != http.StatusOK {
		t.Errorf("expected status 200 for query, got %d", queryResp.Code)
	}
}

// =============================================================================
// Search Tests (NornicDB extension endpoints)
// =============================================================================

func TestHandleSearch(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Test search endpoint - should work even with empty database
	resp := makeRequest(t, server, "POST", "/nornicdb/search", map[string]interface{}{
		"query": "test query",
		"limit": 10,
	}, "Bearer "+token)

	// Should return 200 (success) - search service caching and index building should handle empty database
	if resp.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", resp.Code, resp.Body.String())
	}

	// Verify response structure
	var results []interface{}
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Results should be an array (may be empty if no data)
	if results == nil {
		t.Error("results should be an array, got nil")
	}

	// Test that search service is cached (second request should be faster)
	resp2 := makeRequest(t, server, "POST", "/nornicdb/search", map[string]interface{}{
		"query": "another query",
		"limit": 5,
	}, "Bearer "+token)

	if resp2.Code != http.StatusOK {
		t.Errorf("expected status 200 on second request, got %d: %s", resp2.Code, resp2.Body.String())
	}
}

func TestHandleSearchContinuationBeyondInitialLimit(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")
	otherToken := getAuthToken(t, auth, "reader")
	database := server.dbManager.DefaultDatabaseName()
	engine, err := server.dbManager.GetStorage(database)
	require.NoError(t, err)

	nodes := make([]*storage.Node, 5)
	for index := range nodes {
		nodes[index] = &storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("continuation-%d", index)),
			Labels: []string{"Document"},
			Properties: map[string]any{
				"content": fmt.Sprintf("alpha continuation document %d", index),
			},
		}
	}
	require.NoError(t, engine.BulkCreateNodes(nodes))

	rebuild := makeRequest(t, server, http.MethodPost, "/nornicdb/search/rebuild", map[string]any{"database": database}, "Bearer "+token)
	require.Equal(t, http.StatusOK, rebuild.Code, rebuild.Body.String())
	require.Eventually(t, func() bool {
		status := server.db.GetDatabaseSearchStatus(database)
		return status.Ready && !status.Building
	}, 5*time.Second, 50*time.Millisecond)

	start := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]any{
		"query": "alpha", "limit": 1, "n": 1, "database": database,
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, start.Code, start.Body.String())
	var first struct {
		Results  []map[string]any `json:"results"`
		QID      string           `json:"qid"`
		HasMore  bool             `json:"has_more"`
		Position uint64           `json:"position"`
	}
	require.NoError(t, json.NewDecoder(start.Body).Decode(&first))
	require.Len(t, first.Results, 1)
	require.NotEmpty(t, first.QID)
	require.True(t, first.HasMore)
	require.Equal(t, uint64(0), first.Position)

	foreignPull := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]any{
		"qid": first.QID, "n": 1, "database": database,
	}, "Bearer "+otherToken)
	require.Equal(t, http.StatusBadRequest, foreignPull.Code, foreignPull.Body.String())

	pull := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]any{
		"qid": first.QID, "n": 3, "database": database,
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, pull.Code, pull.Body.String())
	var second struct {
		Results  []map[string]any `json:"results"`
		QID      string           `json:"qid"`
		Position uint64           `json:"position"`
	}
	require.NoError(t, json.NewDecoder(pull.Body).Decode(&second))
	require.Len(t, second.Results, 3)
	require.Equal(t, uint64(1), second.Position)

	release := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]any{
		"qid": second.QID, "discard": true, "database": database,
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, release.Code, release.Body.String())
	var released struct {
		Released bool `json:"released"`
	}
	require.NoError(t, json.NewDecoder(release.Body).Decode(&released))
	require.True(t, released.Released)
}

func TestWriteSearchContinuationError(t *testing.T) {
	testCases := []struct {
		name          string
		err           error
		status        int
		retryable     bool
		requestStatus string
	}{
		{name: "saturated", err: resultstream.ErrCapacity, status: http.StatusServiceUnavailable, retryable: true, requestStatus: "continuation_saturated"},
		{name: "expired", err: resultstream.ErrExpiredQID, status: http.StatusGone, retryable: false, requestStatus: "continuation_gone"},
		{name: "gone", err: resultstream.ErrGoneQID, status: http.StatusGone, retryable: false, requestStatus: "continuation_gone"},
		{name: "invalid", err: resultstream.ErrInvalidQID, status: http.StatusBadRequest, retryable: false, requestStatus: "continuation_invalid"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			server := &Server{}
			response := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/nornicdb/search", nil)
			server.writeSearchContinuationError(response, request, testCase.err)
			require.Equal(t, testCase.status, response.Code)
			var body struct {
				Retryable     bool   `json:"retryable"`
				RequestStatus string `json:"request_status"`
			}
			require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
			require.Equal(t, testCase.retryable, body.Retryable)
			require.Equal(t, testCase.requestStatus, body.RequestStatus)
			if testCase.retryable {
				require.NotEmpty(t, response.Header().Get("Retry-After"))
			}
		})
	}
}

func TestHandleSearchIDContinuationGroupsAllPassages(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")
	database := server.dbManager.DefaultDatabaseName()
	engine, err := server.dbManager.GetStorage(database)
	require.NoError(t, err)

	for _, node := range []*storage.Node{
		{ID: "frame-c", Labels: []string{"Frame"}, Properties: map[string]any{"collection": "summer", "asset_id": "asset-b"}},
		{ID: "frame-b", Labels: []string{"Frame"}, Properties: map[string]any{"collection": "summer", "asset_id": "asset-a"}},
		{ID: "frame-a", Labels: []string{"Frame"}, Properties: map[string]any{"collection": "summer", "asset_id": "asset-a"}},
		{ID: "winter", Labels: []string{"Frame"}, Properties: map[string]any{"collection": "winter", "asset_id": "asset-c"}},
	} {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}

	start := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]any{
		"database": database,
		"mode":     "id",
		"group_by": "asset_id",
		"labels":   []string{"Frame"},
		"filters":  map[string][]string{"collection": {"summer"}},
		"n":        1,
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, start.Code, start.Body.String())
	var first struct {
		Results []struct {
			Node struct {
				ID string `json:"id"`
			} `json:"node"`
			GroupKey string `json:"group_key"`
			Phase    string `json:"phase"`
			Passages []struct {
				Node struct {
					ID string `json:"id"`
				} `json:"node"`
				Phase string `json:"phase"`
			} `json:"passages"`
		} `json:"results"`
		QID                 string `json:"qid"`
		Mode                string `json:"mode"`
		EligibleCount       int    `json:"eligible_count"`
		CollectionExhausted bool   `json:"collection_exhausted"`
		RankedPoolExhausted bool   `json:"ranked_pool_exhausted"`
	}
	require.NoError(t, json.NewDecoder(start.Body).Decode(&first))
	require.Len(t, first.Results, 1)
	require.Equal(t, "frame-a", first.Results[0].Node.ID)
	require.Equal(t, "asset-a", first.Results[0].GroupKey)
	require.Equal(t, "catalog", first.Results[0].Phase)
	require.Equal(t, []string{"frame-a", "frame-b"}, []string{
		first.Results[0].Passages[0].Node.ID,
		first.Results[0].Passages[1].Node.ID,
	})
	require.Equal(t, "id", first.Mode)
	require.Equal(t, 2, first.EligibleCount)
	require.True(t, first.RankedPoolExhausted)
	require.False(t, first.CollectionExhausted)
	require.NotEmpty(t, first.QID)

	pull := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]any{
		"database": database,
		"qid":      first.QID,
		"n":        1,
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, pull.Code, pull.Body.String())
	var last struct {
		Results []struct {
			GroupKey string `json:"group_key"`
		} `json:"results"`
		QID                 string `json:"qid"`
		Completion          string `json:"completion"`
		CollectionExhausted bool   `json:"collection_exhausted"`
	}
	require.NoError(t, json.NewDecoder(pull.Body).Decode(&last))
	require.Len(t, last.Results, 1)
	require.Equal(t, "asset-b", last.Results[0].GroupKey)
	require.Empty(t, last.QID)
	require.Equal(t, "eligible_population_exhausted", last.Completion)
	require.True(t, last.CollectionExhausted)
}

func TestSearchContinuationQIDIsSharedFromHTTPToCypher(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	database := server.dbManager.DefaultDatabaseName()
	engine, err := server.dbManager.GetStorage(database)
	require.NoError(t, err)
	for _, id := range []string{"cross-a", "cross-b"} {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"Document"}})
		require.NoError(t, err)
	}

	start := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]any{
		"database": database, "mode": "id", "n": 1,
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, start.Code, start.Body.String())
	var first struct {
		QID string `json:"qid"`
	}
	require.NoError(t, json.NewDecoder(start.Body).Decode(&first))
	require.NotEmpty(t, first.QID)

	claims, err := authenticator.ValidateToken(token)
	require.NoError(t, err)
	ctx := cypher.WithAuthenticatedPrincipal(context.Background(), auth.PrincipalID(claims))
	result, err := server.db.GetCypherExecutor().Execute(ctx, "USE "+database+" CALL db.retrieve($request)", map[string]interface{}{
		"request": map[string]interface{}{"qid": first.QID, "n": int64(1)},
	})
	require.NoError(t, err)
	page := result.Rows[0][0].(map[string]interface{})
	require.Len(t, page["results"], 1)
	require.Empty(t, page["qid"])
	require.True(t, page["collection_exhausted"].(bool))

	result, err = server.db.GetCypherExecutor().Execute(ctx, "USE "+database+" CALL db.retrieve($request)", map[string]interface{}{
		"request": map[string]interface{}{"mode": "id", "n": int64(1)},
	})
	require.NoError(t, err)
	page = result.Rows[0][0].(map[string]interface{})
	cypherQID := page["qid"].(string)
	require.NotEmpty(t, cypherQID)

	pull := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]any{
		"database": database, "qid": cypherQID, "n": 1,
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, pull.Code, pull.Body.String())
	var last struct {
		Results             []map[string]any `json:"results"`
		QID                 string           `json:"qid"`
		CollectionExhausted bool             `json:"collection_exhausted"`
	}
	require.NoError(t, json.NewDecoder(pull.Body).Decode(&last))
	require.Len(t, last.Results, 1)
	require.Empty(t, last.QID)
	require.True(t, last.CollectionExhausted)
}

func TestSearchContinuationQIDIsSharedAcrossDefaultHTTPAndCypherRequestsWithoutAuthentication(t *testing.T) {
	server, _ := setupTestServer(t)
	server.auth = nil
	database := server.dbManager.DefaultDatabaseName()
	engine, err := server.dbManager.GetStorage(database)
	require.NoError(t, err)
	for _, id := range []string{"anonymous-cross-a", "anonymous-cross-b", "anonymous-cross-c"} {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"Document"}})
		require.NoError(t, err)
	}

	handleAnonymousSearch := func(body map[string]any) *httptest.ResponseRecorder {
		encoded, encodeErr := json.Marshal(body)
		require.NoError(t, encodeErr)
		request := httptest.NewRequest(http.MethodPost, "/nornicdb/search", bytes.NewReader(encoded))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		server.handleSearch(response, request)
		return response
	}
	anonymous := cypher.WithAuthenticatedPrincipal(context.Background(), "anonymous")

	cypherStart, err := server.db.GetCypherExecutor().Execute(anonymous,
		"CALL db.retrieve({mode: 'id', n: 1})", nil)
	require.NoError(t, err)
	cypherPage := cypherStart.Rows[0][0].(map[string]interface{})
	cypherQID := cypherPage["qid"].(string)
	require.NotEmpty(t, cypherQID)

	httpPull := handleAnonymousSearch(map[string]any{"qid": cypherQID, "n": 1})
	require.Equal(t, http.StatusOK, httpPull.Code, httpPull.Body.String())

	httpStart := handleAnonymousSearch(map[string]any{"mode": "id", "n": 1})
	require.Equal(t, http.StatusOK, httpStart.Code, httpStart.Body.String())
	var httpPage struct {
		QID string `json:"qid"`
	}
	require.NoError(t, json.NewDecoder(httpStart.Body).Decode(&httpPage))
	require.NotEmpty(t, httpPage.QID)

	cypherPull, err := server.db.GetCypherExecutor().Execute(anonymous,
		"CALL db.retrieve($request)", map[string]interface{}{
			"request": map[string]interface{}{"qid": httpPage.QID, "n": int64(1)},
		})
	require.NoError(t, err)
	page := cypherPull.Rows[0][0].(map[string]interface{})
	require.Len(t, page["results"], 1)
}

// TestHandleSearch_FiltersParameter verifies that the `filters` field is accepted in the
// request body and that results are restricted to nodes matching the filter.
func TestHandleSearch_FiltersParameter(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Seed a few nodes with a "collection" property.
	dbName := server.dbManager.DefaultDatabaseName()
	eng, err := server.dbManager.GetStorage(dbName)
	require.NoError(t, err)

	nodes := []*storage.Node{
		{
			ID:     "chunk_user_a",
			Labels: []string{"Chunk"},
			Properties: map[string]any{
				"content":    "user_a exclusive document",
				"collection": []any{"user_a"},
			},
		},
		{
			ID:     "chunk_user_b",
			Labels: []string{"Chunk"},
			Properties: map[string]any{
				"content":    "user_b exclusive document",
				"collection": []any{"user_b"},
			},
		},
		{
			ID:     "chunk_shared",
			Labels: []string{"Chunk"},
			Properties: map[string]any{
				"content":    "shared document",
				"collection": []any{"user_a", "user_b"},
			},
		},
	}
	require.NoError(t, eng.BulkCreateNodes(nodes))

	// Trigger search index rebuild so the new nodes are indexed.
	rebuildResp := makeRequest(t, server, http.MethodPost, "/nornicdb/search/rebuild",
		map[string]interface{}{"database": dbName}, "Bearer "+token)
	require.Equal(t, http.StatusOK, rebuildResp.Code, rebuildResp.Body.String())
	require.Eventually(t, func() bool {
		st := server.db.GetDatabaseSearchStatus(dbName)
		return st.Ready && !st.Building
	}, 5*time.Second, 50*time.Millisecond)

	// Search with filter — should return only user_a nodes.
	resp := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]interface{}{
		"query": "document",
		"limit": 10,
		"filters": map[string]interface{}{
			"collection": []string{"user_a"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())

	// Response shape: [{"node": {"id":..., "properties": {...}}, "score": ...}, ...]
	var results []map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&results))

	for _, r := range results {
		nodeObj, ok := r["node"].(map[string]interface{})
		require.True(t, ok, "result missing 'node' field: %v", r)
		props, ok := nodeObj["properties"].(map[string]interface{})
		require.True(t, ok, "node missing 'properties' field: %v", nodeObj)
		coll := props["collection"]
		// The collection must include user_a (either scalar or array).
		matched := false
		switch v := coll.(type) {
		case string:
			matched = v == "user_a"
		case []interface{}:
			for _, elem := range v {
				if s, ok := elem.(string); ok && s == "user_a" {
					matched = true
					break
				}
			}
		}
		require.True(t, matched, "result %v has collection=%v — expected user_a membership", nodeObj["id"], coll)
	}

	// Search without filters — all three nodes are eligible.
	respAll := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]interface{}{
		"query": "document",
		"limit": 10,
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, respAll.Code, respAll.Body.String())

	var allResults []map[string]interface{}
	require.NoError(t, json.NewDecoder(respAll.Body).Decode(&allResults))
	require.Len(t, allResults, 3, "unfiltered search should return all 3 nodes")

	// Response projection is independent of candidate filtering. Exclusion wins
	// when a property appears in both caller-provided lists.
	projectedResp := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]interface{}{
		"query":              "document",
		"limit":              10,
		"include_properties": []string{"content", "collection"},
		"exclude_properties": []string{"collection"},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, projectedResp.Code, projectedResp.Body.String())
	var projectedResults []map[string]interface{}
	require.NoError(t, json.NewDecoder(projectedResp.Body).Decode(&projectedResults))
	require.Len(t, projectedResults, 3)
	for _, result := range projectedResults {
		nodeObject := result["node"].(map[string]interface{})
		properties := nodeObject["properties"].(map[string]interface{})
		require.Contains(t, properties, "content")
		require.NotContains(t, properties, "collection")
	}
}

func TestHandleSearch_ChunksLongQueriesForVectorSearch(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Simulate a local embedder that fails on long inputs (token buffer overflow, etc.).
	// The search endpoint should chunk the query and only embed chunk-sized strings.
	emb := &countingEmbedder{
		failIfTokensGreater: 512,
		dims:                1024,
	}
	server.db.SetEmbedder(emb)

	// Ensure vector search is actually usable; otherwise the handler correctly
	// short-circuits to BM25 and embedding is intentionally skipped.
	dbName := server.dbManager.DefaultDatabaseName()
	storageEngine, err := server.dbManager.GetStorage(dbName)
	require.NoError(t, err)
	searchSvc, err := server.db.GetOrCreateSearchService(dbName, storageEngine)
	require.NoError(t, err)
	seedVec := make([]float32, 1024)
	seedVec[0] = 1
	require.NoError(t, searchSvc.IndexNode(&storage.Node{
		ID:              storage.NodeID("seed-vector-node"),
		Labels:          []string{"Seed"},
		Properties:      map[string]interface{}{"name": "seed"},
		ChunkEmbeddings: [][]float32{seedVec},
	}))
	require.Greater(t, searchSvc.EmbeddingCount(), 0)

	longQuery := loadLargeDocQuery(t)
	resp := makeRequest(t, server, "POST", "/nornicdb/search", map[string]interface{}{
		"query": longQuery,
		"limit": 10,
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())

	emb.mu.Lock()
	calls := emb.calls
	maxTokens := emb.maxTokens
	emb.mu.Unlock()

	require.GreaterOrEqual(t, calls, 2, "expected query embedding to run on multiple chunks")
	require.LessOrEqual(t, maxTokens, 512, "expected no embedding call on chunks > 512 tokens")
}

func TestHandleSearch_SkipsEmbeddingWhenNoVectorsIndexed(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	emb := &countingEmbedder{dims: 1024}
	server.db.SetEmbedder(emb)

	dbName := server.dbManager.DefaultDatabaseName()
	storageEngine, err := server.dbManager.GetStorage(dbName)
	require.NoError(t, err)
	searchSvc, err := server.db.GetOrCreateSearchService(dbName, storageEngine)
	require.NoError(t, err)
	searchSvc.ClearVectorIndex()
	require.Equal(t, 0, searchSvc.EmbeddingCount())

	longQuery := strings.Repeat("a", 1200)
	resp := makeRequest(t, server, "POST", "/nornicdb/search", map[string]interface{}{
		"query": longQuery,
		"limit": 10,
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())

	emb.mu.Lock()
	calls := emb.calls
	emb.mu.Unlock()
	require.Equal(t, 0, calls, "expected no query embedding calls when vector index is empty")
}

func TestTxCommit_VectorQueryNodes_StringInput_UsesConfiguredEmbedder(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	emb := &countingEmbedder{dims: 1024}
	server.db.SetEmbedder(emb)

	dbName := server.dbManager.DefaultDatabaseName()
	storageEngine, err := server.dbManager.GetStorage(dbName)
	require.NoError(t, err)
	searchSvc, err := server.db.GetOrCreateSearchService(dbName, storageEngine)
	require.NoError(t, err)

	seedVec := make([]float32, 1024)
	seedVec[0] = float32(len("seed"))
	require.NoError(t, searchSvc.IndexNode(&storage.Node{
		ID:              storage.NodeID("seed-vector-node"),
		Labels:          []string{"Seed"},
		Properties:      map[string]interface{}{"name": "seed"},
		ChunkEmbeddings: [][]float32{seedVec},
	}))

	resp := makeRequest(t, server, "POST", "/db/"+dbName+"/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{
				"statement": "CALL db.index.vector.queryNodes('node_embedding_index', 5, 'seed') YIELD node, score RETURN node.id AS id, score LIMIT 1",
			},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())

	var payload map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	if errs, ok := payload["errors"].([]interface{}); ok {
		require.Len(t, errs, 0, "expected no cypher errors for string vector query")
	}

	emb.mu.Lock()
	calls := emb.calls
	emb.mu.Unlock()
	require.Greater(t, calls, 0, "expected query embedder to be called")
}

func TestHandleSearchRebuild(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Test search rebuild endpoint
	resp := makeRequest(t, server, "POST", "/nornicdb/search/rebuild", map[string]interface{}{
		"database": server.dbManager.DefaultDatabaseName(),
	}, "Bearer "+token)

	if resp.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", resp.Code, resp.Body.String())
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Verify response structure
	if result["success"] == nil {
		t.Error("missing 'success' field")
	}
	if result["database"] == nil {
		t.Error("missing 'database' field")
	}
	if result["message"] == nil {
		t.Error("missing 'message' field")
	}

	// Verify success is true
	success, ok := result["success"].(bool)
	if !ok {
		t.Errorf("'success' should be a boolean, got %T", result["success"])
	}
	if !success {
		t.Error("expected success to be true")
	}

	// Test rebuild without database parameter (should use default)
	resp2 := makeRequest(t, server, "POST", "/nornicdb/search/rebuild", nil, "Bearer "+token)
	if resp2.Code != http.StatusOK {
		t.Errorf("expected status 200 without database param, got %d: %s", resp2.Code, resp2.Body.String())
	}
}

func TestHandleSearchAdditionalDirectBranches(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Method not allowed.
	req := httptest.NewRequest(http.MethodGet, "/nornicdb/search", nil)
	rec := httptest.NewRecorder()
	server.handleSearch(rec, req)
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)

	// Invalid JSON.
	req = httptest.NewRequest(http.MethodPost, "/nornicdb/search", strings.NewReader("{"))
	rec = httptest.NewRecorder()
	server.handleSearch(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)

	// Forbidden (auth enabled + no claims).
	req = httptest.NewRequest(http.MethodPost, "/nornicdb/search", strings.NewReader(`{"query":"x"}`))
	rec = httptest.NewRecorder()
	server.handleSearch(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)

	// Search-not-ready branch.
	server.db.ResetSearchService(server.dbManager.DefaultDatabaseName())
	resp := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]interface{}{
		"query": "x",
	}, "Bearer "+token)
	require.Equal(t, http.StatusServiceUnavailable, resp.Code)
}

func TestHandleSearchRebuild_WriteForbiddenBranch(t *testing.T) {
	server, _ := setupTestServer(t)
	dbName := server.dbManager.DefaultDatabaseName()

	// Reader can access nornic but should not have write privilege for rebuild.
	req := httptest.NewRequest(http.MethodPost, "/nornicdb/search/rebuild", strings.NewReader(`{"database":"`+dbName+`"}`))
	req = req.WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{
		Username: "reader",
		Roles:    []string{"viewer"},
	}))
	rec := httptest.NewRecorder()
	server.handleSearchRebuild(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestHandleCommitTransaction_AdditionalBranches(t *testing.T) {
	server, _ := setupTestServer(t)
	dbName := server.dbManager.DefaultDatabaseName()

	// Open transaction as admin claim.
	openReq := httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx", strings.NewReader(`{"statements":[]}`))
	openReq = openReq.WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{
		Username: "admin",
		Roles:    []string{"admin"},
	}))
	openRec := httptest.NewRecorder()
	server.handleOpenTransaction(openRec, openReq, dbName)
	require.Equal(t, http.StatusCreated, openRec.Code)

	var openResp map[string]interface{}
	require.NoError(t, json.NewDecoder(openRec.Body).Decode(&openResp))
	commitURL, _ := openResp["commit"].(string)
	require.NotEmpty(t, commitURL)
	parts := strings.Split(commitURL, "/")
	txID := parts[len(parts)-2]
	require.NotEmpty(t, txID)

	// Commit with invalid final statement to exercise rollback-on-errors branch.
	commitReq := httptest.NewRequest(http.MethodPost, commitURL, strings.NewReader(`{"statements":[{"statement":"INVALID CYPHER"}]}`))
	commitReq = commitReq.WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{
		Username: "admin",
		Roles:    []string{"admin"},
	}))
	commitRec := httptest.NewRecorder()
	server.handleCommitTransaction(commitRec, commitReq, dbName, txID)
	require.Equal(t, http.StatusOK, commitRec.Code)

	var commitResp map[string]interface{}
	require.NoError(t, json.NewDecoder(commitRec.Body).Decode(&commitResp))
	if errs, ok := commitResp["errors"].([]interface{}); ok {
		require.NotEmpty(t, errs)
	}
}

func TestHandleSearch_LongChunkedBranches(t *testing.T) {
	server, _ := setupTestServer(t)
	server.auth = nil // Full DB access for branch-focused direct handler tests

	require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "translations"}))
	st, err := server.dbManager.GetStorage("translations")
	require.NoError(t, err)

	// Seed vectors so vectorSearchUsable=true.
	seed := make([]float32, 1024)
	seed[0] = 1
	_, _ = st.CreateNode(&storage.Node{
		ID:              "t_seed",
		Labels:          []string{"Doc"},
		Properties:      map[string]interface{}{"title": "seed"},
		ChunkEmbeddings: [][]float32{seed},
	})

	// Ensure search service exists and is ready for the database.
	svc, err := server.db.GetOrCreateSearchService("translations", st)
	require.NoError(t, err)
	require.NotNil(t, svc)
	// The DB owner schedules the initial build asynchronously. Calling
	// svc.BuildIndexes here would allow that queued build to start afterward
	// and invalidate the readiness just established by the manual build.
	require.Eventually(t, func() bool {
		return svc.IsReady() && svc.EmbeddingCount() > 0
	}, 10*time.Second, 20*time.Millisecond)

	// Use embedder so chunked vector path is exercised.
	server.db.SetEmbedder(&countingEmbedder{dims: 1024})
	server.config.Features = &nornicConfig.FeatureFlagsConfig{
		SearchRerankEnabled: true,
	}
	t.Setenv("NORNICDB_SEARCH_DIAG_TIMINGS", "true")

	// > 32 chunks to trigger maxQueryChunks cap and high limit to hit perChunkLimit > 100 path.
	longQuery := strings.Repeat("This is a long query block for chunking. ", 1200)
	body := map[string]interface{}{
		"database": "translations",
		"query":    longQuery,
		"labels":   []string{"Doc"},
		"limit":    1000,
	}
	// The search may answer 503 search_not_ready while the HNSW warm-up the
	// seeded vector started is still in flight; it answers 200 once ready.
	var resp *httptest.ResponseRecorder
	require.Eventually(t, func() bool {
		resp = makeRequest(t, server, http.MethodPost, "/nornicdb/search", body, "")
		if resp.Code == http.StatusServiceUnavailable {
			require.Contains(t, resp.Body.String(), "search_not_ready", "only a search still building may answer 503")
			return false
		}
		return true
	}, 10*time.Second, 20*time.Millisecond)
	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
}

func TestHandleImplicitTransaction_BranchMatrix(t *testing.T) {
	server, _ := setupTestServer(t)
	dbName := server.dbManager.DefaultDatabaseName()

	t.Run("invalid JSON", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx/commit", strings.NewReader("{"))
		rec := httptest.NewRecorder()
		server.handleImplicitTransaction(rec, req, dbName)
		require.Equal(t, http.StatusOK, rec.Code)
		var response TransactionResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		require.Len(t, response.Errors, 1)
		require.Equal(t, "Neo.ClientError.Request.InvalidFormat", response.Errors[0].Code)
	})

	t.Run("mutation denied without claims", func(t *testing.T) {
		body := `{"statements":[{"statement":"CREATE (n:Doc {name:'x'})"}]}`
		req := httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx/commit", strings.NewReader(body))
		rec := httptest.NewRecorder()
		server.handleImplicitTransaction(rec, req, dbName)
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("use missing database returns statement error with HTTP 200", func(t *testing.T) {
		body := `{"statements":[{"statement":":USE missing_db RETURN 1 AS n"}]}`
		req := httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx/commit", strings.NewReader(body))
		req = req.WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{
			Username: "admin",
			Roles:    []string{"admin"},
		}))
		rec := httptest.NewRecorder()
		server.handleImplicitTransaction(rec, req, dbName)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), "DatabaseNotFound")
	})

	t.Run("native USE statement is accepted", func(t *testing.T) {
		body := `{"statements":[{"statement":"USE nornic RETURN 1 AS n"}]}`
		req := httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx/commit", strings.NewReader(body))
		req = req.WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{
			Username: "admin",
			Roles:    []string{"admin"},
		}))
		rec := httptest.NewRecorder()
		server.handleImplicitTransaction(rec, req, dbName)
		require.Equal(t, http.StatusOK, rec.Code)
	})

	// An empty statement is Neo4j's SyntaxError and ends the request (#683).
	t.Run("comment-only statement is a syntax error", func(t *testing.T) {
		body := `{"statements":[{"statement":"// comment only","includeStats":true},{"statement":"RETURN 1 AS n","includeStats":true}]}`
		req := httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx/commit", strings.NewReader(body))
		req = req.WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{
			Username: "admin",
			Roles:    []string{"admin"},
		}))
		rec := httptest.NewRecorder()
		server.handleImplicitTransaction(rec, req, dbName)
		require.Equal(t, http.StatusOK, rec.Code)

		var payload TransactionResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&payload))
		require.Len(t, payload.Errors, 1)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", payload.Errors[0].Code)
		require.Empty(t, payload.Results)
	})
}

func TestHandleImplicitTransaction_AsyncWriteAccepted(t *testing.T) {
	tmpDir := t.TempDir()
	dbCfg := nornicdb.DefaultConfig()
	dbCfg.Memory.DecayEnabled = false
	dbCfg.Database.AsyncWritesEnabled = true
	db, err := nornicdb.Open(tmpDir, dbCfg)
	require.NoError(t, err)
	defer db.Close()

	authStorage := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = authStorage.Close() })
	authn, err := auth.NewAuthenticator(auth.AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("test-secret-key-for-testing-only-32b"),
	}, authStorage)
	require.NoError(t, err)
	_, _ = authn.CreateUser("admin", "password123", []auth.Role{auth.RoleAdmin})

	cfg := DefaultConfig()
	cfg.Port = 0
	srv, err := New(db, authn, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { stopTestServer(t, srv) })

	req := httptest.NewRequest(http.MethodPost, "/db/nornic/tx/commit", strings.NewReader(`{"statements":[{"statement":"CREATE (n:Doc {name:'async'})"}]}`))
	req = req.WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{
		Username: "admin",
		Roles:    []string{"admin"},
	}))
	rec := httptest.NewRecorder()
	srv.handleImplicitTransaction(rec, req, "nornic")
	require.Equal(t, http.StatusAccepted, rec.Code)
	require.Equal(t, "eventual", rec.Header().Get("X-NornicDB-Consistency"))
	var payload map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&payload))
	optimisticRaw, ok := payload["optimistic"].(map[string]interface{})
	require.True(t, ok, "expected optimistic metadata in async response")
	createdIDs, ok := optimisticRaw["createdNodeIds"].([]interface{})
	require.True(t, ok)
	require.NotEmpty(t, createdIDs)
}

func TestHandleImplicitTransaction_AsyncEnabledBuffersSetWriteReturnsAccepted(t *testing.T) {
	tmpDir := t.TempDir()
	dbCfg := nornicdb.DefaultConfig()
	dbCfg.Memory.DecayEnabled = false
	dbCfg.Database.AsyncWritesEnabled = true
	db, err := nornicdb.Open(tmpDir, dbCfg)
	require.NoError(t, err)
	defer db.Close()

	authStorage := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = authStorage.Close() })
	authn, err := auth.NewAuthenticator(auth.AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("test-secret-key-for-testing-only-32b"),
	}, authStorage)
	require.NoError(t, err)
	_, _ = authn.CreateUser("admin", "password123", []auth.Role{auth.RoleAdmin})

	cfg := DefaultConfig()
	cfg.Port = 0
	srv, err := New(db, authn, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { stopTestServer(t, srv) })

	req := httptest.NewRequest(http.MethodPost, "/db/nornic/tx/commit", strings.NewReader(`{"statements":[{"statement":"CREATE (n:Doc {name:'sync'}) SET n.updatedAt = datetime() RETURN n.name"}]}`))
	req = req.WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{
		Username: "admin",
		Roles:    []string{"admin"},
	}))
	rec := httptest.NewRecorder()
	srv.handleImplicitTransaction(rec, req, "nornic")

	// Every eligible write is acknowledged through the buffer when async
	// writes are enabled (the routing-layer eligibility of the removed
	// AsyncEngine is gone), so this mutation is accepted, not completed.
	require.Equal(t, http.StatusAccepted, rec.Code)
	require.Equal(t, "eventual", rec.Header().Get("X-NornicDB-Consistency"))

	var payload map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&payload))
	_, hasReceipt := payload["receipt"]
	require.True(t, hasReceipt, "accepted writes expose the same receipt metadata as sync")
}

func TestHandleImplicitTransaction_NoAuth_SkipsRBACWriteCheck(t *testing.T) {
	tmpDir := t.TempDir()
	dbCfg := nornicdb.DefaultConfig()
	dbCfg.Memory.DecayEnabled = false
	dbCfg.Database.AsyncWritesEnabled = false // Deterministic status: isolate RBAC behavior from async 202 semantics.
	db, err := nornicdb.Open(tmpDir, dbCfg)
	require.NoError(t, err)
	defer db.Close()

	cfg := DefaultConfig()
	cfg.Port = 0
	srv, err := New(db, nil, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })

	req := httptest.NewRequest(http.MethodPost, "/db/nornic/tx/commit", strings.NewReader(`{"statements":[{"statement":"CREATE (n:Doc {name:'open'}) RETURN n.name"}]}`))
	rec := httptest.NewRecorder()
	srv.handleImplicitTransaction(rec, req, "nornic")

	require.Equal(t, http.StatusOK, rec.Code)
	var payload TransactionResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&payload))
	require.Empty(t, payload.Errors, "writes should not be RBAC-forbidden when auth is disabled")
	require.NotEmpty(t, payload.Results)
}

func TestExecuteTxStatements_NoAuth_SkipsRBACWriteCheck(t *testing.T) {
	tmpDir := t.TempDir()
	dbCfg := nornicdb.DefaultConfig()
	dbCfg.Memory.DecayEnabled = false
	db, err := nornicdb.Open(tmpDir, dbCfg)
	require.NoError(t, err)
	defer db.Close()

	cfg := DefaultConfig()
	cfg.Port = 0
	srv, err := New(db, nil, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })

	dbName := srv.dbManager.DefaultDatabaseName()
	lease, err := srv.txSessions.OpenRequestForOwner(context.Background(), dbName, "")
	require.NoError(t, err)
	// End the request before the deferred DB.Close joins its retained work.
	defer func() { _ = lease.RollbackAndDelete(context.Background()); lease.Release() }()

	resp := &TransactionResponse{
		Results: make([]QueryResult, 0, 1),
		Errors:  make([]QueryError, 0, 1),
	}
	srv.executeTxStatements(context.Background(), "", nil, dbName, lease, []StatementRequest{
		{Statement: "CREATE (n:Doc {name:'tx-open'}) RETURN n.name"},
	}, resp)

	require.Empty(t, resp.Errors, "writes in tx session should not be RBAC-forbidden when auth is disabled")
	require.NotEmpty(t, resp.Results)
}

func TestHandleSimilar(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Get the default database storage to create test nodes with embeddings
	dbName := server.dbManager.DefaultDatabaseName()
	storageEngine, err := server.dbManager.GetStorage(dbName)
	require.NoError(t, err, "should get default database storage")

	// Create a target node with an embedding
	targetEmbedding := make([]float32, 1024)
	for i := range targetEmbedding {
		targetEmbedding[i] = float32(i) / 1024.0
	}
	targetNode := &storage.Node{
		ID:              storage.NodeID("target-node"),
		Labels:          []string{"Test"},
		Properties:      map[string]interface{}{"name": "Target Node"},
		ChunkEmbeddings: [][]float32{targetEmbedding},
	}
	_, err = storageEngine.CreateNode(targetNode)
	require.NoError(t, err, "should create target node")

	// Create a similar node with a similar embedding (slight variation)
	similarEmbedding := make([]float32, 1024)
	for i := range similarEmbedding {
		similarEmbedding[i] = float32(i+1) / 1024.0 // Slight variation
	}
	similarNode := &storage.Node{
		ID:              storage.NodeID("similar-node"),
		Labels:          []string{"Test"},
		Properties:      map[string]interface{}{"name": "Similar Node"},
		ChunkEmbeddings: [][]float32{similarEmbedding},
	}
	_, err = storageEngine.CreateNode(similarNode)
	require.NoError(t, err, "should create similar node")

	// Create a dissimilar node with a very different embedding
	dissimilarEmbedding := make([]float32, 1024)
	for i := range dissimilarEmbedding {
		dissimilarEmbedding[i] = float32(1024-i) / 1024.0 // Very different
	}
	dissimilarNode := &storage.Node{
		ID:              storage.NodeID("dissimilar-node"),
		Labels:          []string{"Test"},
		Properties:      map[string]interface{}{"name": "Dissimilar Node"},
		ChunkEmbeddings: [][]float32{dissimilarEmbedding},
	}
	_, err = storageEngine.CreateNode(dissimilarNode)
	require.NoError(t, err, "should create dissimilar node")

	// Test the similar endpoint
	resp := makeRequest(t, server, "POST", "/nornicdb/similar", map[string]interface{}{
		"node_id": "target-node",
		"limit":   5,
	}, "Bearer "+token)

	require.Equal(t, http.StatusOK, resp.Code, "should return 200 OK")

	// Verify response structure
	var results []map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&results)
	require.NoError(t, err, "should decode response as JSON array")

	// Should find at least the similar node (may also find dissimilar node)
	require.Greater(t, len(results), 0, "should return at least one result")

	// Verify result structure
	for _, result := range results {
		require.Contains(t, result, "node", "result should contain 'node' field")
		require.Contains(t, result, "score", "result should contain 'score' field")
		node, ok := result["node"].(map[string]interface{})
		require.True(t, ok, "node should be a map")
		require.Contains(t, node, "id", "node should have 'id' field")
		score, ok := result["score"].(float64)
		require.True(t, ok, "score should be a number")
		require.GreaterOrEqual(t, score, 0.0, "score should be non-negative")
		require.LessOrEqual(t, score, 1.0, "score should be at most 1.0")
	}

	// Test with non-existent node (should return 404)
	resp = makeRequest(t, server, "POST", "/nornicdb/similar", map[string]interface{}{
		"node_id": "non-existent-node",
		"limit":   5,
	}, "Bearer "+token)

	require.Equal(t, http.StatusNotFound, resp.Code, "should return 404 for non-existent node")

	// Test with node without embedding (should return 400)
	nodeWithoutEmbedding := &storage.Node{
		ID:              storage.NodeID("no-embedding-node"),
		Labels:          []string{"Test"},
		Properties:      map[string]interface{}{"name": "No Embedding"},
		ChunkEmbeddings: nil, // No embedding
	}
	_, err = storageEngine.CreateNode(nodeWithoutEmbedding)
	require.NoError(t, err, "should create node without embedding")

	resp = makeRequest(t, server, "POST", "/nornicdb/similar", map[string]interface{}{
		"node_id": "no-embedding-node",
		"limit":   5,
	}, "Bearer "+token)

	require.Equal(t, http.StatusBadRequest, resp.Code, "should return 400 for node without embedding")
}

// =============================================================================
// Schema Endpoint Tests (via Cypher - Neo4j compatible approach)
// =============================================================================

func TestSchemaViaCypher(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Get labels via CALL db.labels()
	labelsResp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CALL db.labels()"},
		},
	}, "Bearer "+token)

	if labelsResp.Code != http.StatusOK {
		t.Errorf("expected status 200 for labels query, got %d", labelsResp.Code)
	}

	// Get relationship types via CALL db.relationshipTypes()
	relTypesResp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CALL db.relationshipTypes()"},
		},
	}, "Bearer "+token)

	if relTypesResp.Code != http.StatusOK {
		t.Errorf("expected status 200 for relationship types query, got %d", relTypesResp.Code)
	}
}

func TestTransactionWithStatements(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Open transaction with initial statements
	openResp := makeRequest(t, server, "POST", "/db/nornic/tx", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n) RETURN count(n) as count"},
		},
	}, "Bearer "+token)

	if openResp.Code != http.StatusCreated {
		t.Errorf("expected status 201, got %d: %s", openResp.Code, openResp.Body.String())
	}

	// Check that results are included
	var result map[string]interface{}
	json.NewDecoder(openResp.Body).Decode(&result)

	results, ok := result["results"].([]interface{})
	if !ok || len(results) == 0 {
		t.Error("expected results from initial statement execution")
	}
}

func TestCommitTransactionWithStatements(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Open transaction
	openResp := makeRequest(t, server, "POST", "/db/nornic/tx", map[string]interface{}{
		"statements": []map[string]interface{}{},
	}, "Bearer "+token)

	var openResult map[string]interface{}
	json.NewDecoder(openResp.Body).Decode(&openResult)

	commitURL := openResult["commit"].(string)
	parts := strings.Split(commitURL, "/")
	txID := parts[len(parts)-2]

	// Commit with final statements
	commitResp := makeRequest(t, server, "POST", fmt.Sprintf("/db/nornic/tx/%s/commit", txID), map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n) RETURN count(n) as count"},
		},
	}, "Bearer "+token)

	if commitResp.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", commitResp.Code, commitResp.Body.String())
	}
}

func TestImplicitTransactionBadJSON(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Send malformed request (bad JSON) - this should give an error
	req := httptest.NewRequest("POST", "/db/nornic/tx/commit", strings.NewReader("not valid json"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code)
	var response TransactionResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.Len(t, response.Errors, 1)
	require.Equal(t, "Neo.ClientError.Request.InvalidFormat", response.Errors[0].Code)
}

func TestTransactionMethodNotAllowed(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// GET on tx should fail
	resp := makeRequest(t, server, "GET", "/db/nornic/tx", nil, "Bearer "+token)
	if resp.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", resp.Code)
	}
}

func TestSearchMethodNotAllowed(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "GET", "/nornicdb/search", nil, "Bearer "+token)
	if resp.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", resp.Code)
	}
}

func TestSimilarMethodNotAllowed(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "GET", "/nornicdb/similar", nil, "Bearer "+token)
	if resp.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", resp.Code)
	}
}

func TestSearchInvalidJSON(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	req := httptest.NewRequest("POST", "/nornicdb/search", strings.NewReader("invalid json"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 for invalid JSON, got %d", recorder.Code)
	}
}

func TestSimilarInvalidJSON(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	req := httptest.NewRequest("POST", "/nornicdb/similar", strings.NewReader("invalid json"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 for invalid JSON, got %d", recorder.Code)
	}
}

func TestRequestTimeoutMiddleware_TxRoute_UsesConfigAndOverride(t *testing.T) {
	// By default tx timeout has a safety floor, so a short handler should not time out
	// even when WriteTimeout is configured very small.
	s := &Server{config: &Config{WriteTimeout: 10 * time.Millisecond}}
	slowOK := s.requestTimeoutMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(25 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	req := httptest.NewRequest(http.MethodPost, "/db/nornic/tx/commit", nil)
	rr := httptest.NewRecorder()
	slowOK.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 for tx route without override, got %d body=%q", rr.Code, rr.Body.String())
	}

	// The execution timeout should return promptly before terminal COMMIT admission.
	t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "5ms")
	s2 := &Server{config: &Config{WriteTimeout: 10 * time.Second}}
	slowTimeout := s2.requestTimeoutMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, bounded := r.Context().Deadline()
		require.True(t, bounded)
		<-r.Context().Done()
		http.Error(w, r.Context().Err().Error(), http.StatusGatewayTimeout)
	}))
	req2 := httptest.NewRequest(http.MethodPost, "/db/nornic/tx/commit", nil)
	rr2 := httptest.NewRecorder()
	slowTimeout.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected pre-admission timeout response when execution deadline expires, got %d", rr2.Code)
	}
	if !strings.Contains(rr2.Body.String(), `"errors"`) {
		t.Fatalf("expected a transaction-shaped timeout response, got %q", rr2.Body.String())
	}
}

func TestRequestTimeoutMiddleware_TxRoute_TracksActiveTxRequests(t *testing.T) {
	s := &Server{config: &Config{WriteTimeout: 10 * time.Second}}
	started := make(chan struct{})
	release := make(chan struct{})
	mw := s.requestTimeoutMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/db/nornic/tx/commit", nil)
	rr := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		mw.ServeHTTP(rr, req)
		close(done)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("tx handler did not start")
	}
	require.Eventually(t, func() bool { return s.activeTxReqs.Load() == 1 }, time.Second, 10*time.Millisecond)

	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("tx handler did not complete")
	}
	require.Equal(t, int64(0), s.activeTxReqs.Load())
}

func TestTransactionHandlers_AdditionalErrorBranches(t *testing.T) {
	server, _ := setupTestServer(t)
	_ = server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "private"})

	adminCtx := context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{
		Username: "admin",
		Roles:    []string{"admin"},
	})

	req := httptest.NewRequest(http.MethodPost, "/db/missing_db/tx", strings.NewReader(`{"statements":[]}`)).WithContext(adminCtx)
	rec := httptest.NewRecorder()
	server.handleOpenTransaction(rec, req, "missing_db")
	require.Equal(t, http.StatusNotFound, rec.Code)

	req = httptest.NewRequest(http.MethodPost, "/db/private/tx", strings.NewReader(`{"statements":[]}`))
	rec = httptest.NewRecorder()
	server.handleOpenTransaction(rec, req, "private")
	require.Equal(t, http.StatusForbidden, rec.Code)

	req = httptest.NewRequest(http.MethodPost, "/db/nornic/tx/nope/commit", strings.NewReader(`{"statements":[]}`)).WithContext(adminCtx)
	rec = httptest.NewRecorder()
	server.handleCommitTransaction(rec, req, "nornic", "nope")
	require.Equal(t, http.StatusNotFound, rec.Code)

	req = httptest.NewRequest(http.MethodPost, "/db/private/tx/nope/commit", strings.NewReader(`{"statements":[]}`))
	rec = httptest.NewRecorder()
	server.handleCommitTransaction(rec, req, "private", "nope")
	require.Equal(t, http.StatusForbidden, rec.Code)

	req = httptest.NewRequest(http.MethodPost, "/db/nornic/tx/nope", strings.NewReader(`{"statements":[]}`)).WithContext(adminCtx)
	rec = httptest.NewRecorder()
	server.handleExecuteInTransaction(rec, req, "nornic", "nope")
	require.Equal(t, http.StatusNotFound, rec.Code)

	req = httptest.NewRequest(http.MethodDelete, "/db/private/tx/nope", nil)
	rec = httptest.NewRecorder()
	server.handleRollbackTransaction(rec, req, "private", "nope")
	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestNew_SearchRerankProviderBranches(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := nornicdb.Open(tmpDir, nornicdb.DefaultConfig())
	require.NoError(t, err)
	defer db.Close()

	t.Run("ollama provider defaults api url and model", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.EmbeddingEnabled = false
		cfg.MCPEnabled = false
		cfg.Features = &nornicConfig.FeatureFlagsConfig{
			SearchRerankEnabled:  true,
			SearchRerankProvider: "ollama",
		}
		s, err := New(db, nil, cfg)
		require.NoError(t, err)
		require.NotNil(t, s)
		t.Cleanup(func() { stopTestServer(t, s) })
	})

	t.Run("external provider with missing api url logs disabled path", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.EmbeddingEnabled = false
		cfg.MCPEnabled = false
		cfg.Features = &nornicConfig.FeatureFlagsConfig{
			SearchRerankEnabled:  true,
			SearchRerankProvider: "cohere",
		}
		s, err := New(db, nil, cfg)
		require.NoError(t, err)
		require.NotNil(t, s)
		t.Cleanup(func() { stopTestServer(t, s) })
	})

	t.Run("voyage provider uses native reranker defaults", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.EmbeddingEnabled = false
		cfg.MCPEnabled = false
		cfg.Features = &nornicConfig.FeatureFlagsConfig{
			SearchRerankEnabled:  true,
			SearchRerankProvider: "voyage",
			SearchRerankAPIKey:   "test-voyage-key",
		}
		s, err := New(db, nil, cfg)
		require.NoError(t, err)
		require.NotNil(t, s)
	})
}

// The rerank truncation setting reaches Voyage's request from db.rerank: the
// provider may shorten an over-long query by default, and doesn't with
// NORNICDB_SEARCH_RERANK_TRUNCATION=false (#907, I27).
func TestVoyageRerankTruncationSetting(t *testing.T) {
	for _, disable := range []bool{false, true} {
		var truncation *bool
		voyageAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var body struct {
				Truncation *bool `json:"truncation"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			truncation = body.Truncation
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"index": 0, "relevance_score": 0.9}}}))
		}))
		t.Cleanup(voyageAPI.Close)
		db, err := nornicdb.Open(t.TempDir(), nornicdb.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		t.Setenv("NORNICDB_SEARCH_RERANK_ENABLED", "true")
		t.Setenv("NORNICDB_SEARCH_RERANK_PROVIDER", "voyage")
		t.Setenv("NORNICDB_SEARCH_RERANK_API_URL", voyageAPI.URL)
		t.Setenv("NORNICDB_SEARCH_RERANK_API_KEY", "test-voyage-key")
		t.Setenv("NORNICDB_SEARCH_RERANK_TRUNCATION", fmt.Sprint(!disable))
		processConfig := nornicConfig.LoadFromEnv()
		cfg := DefaultConfig()
		cfg.EmbeddingEnabled = false
		cfg.MCPEnabled = false
		cfg.ProcessConfig = processConfig
		cfg.Features = &processConfig.Features
		s, err := New(db, nil, cfg)
		require.NoError(t, err)
		t.Cleanup(func() { stopTestServer(t, s) })
		_, err = db.ExecuteCypher(context.Background(), "CALL db.rerank({query: 'q', candidates: [{id: 'a', content: 'alpha'}]}) YIELD id RETURN id", nil)
		require.NoError(t, err)
		require.NotNil(t, truncation, "disable=%v", disable)
		require.Equal(t, !disable, *truncation)

		// Another database's search service gets its reranker from the
		// per-database resolver, with the same setting.
		truncation = nil
		svc, err := db.GetOrCreateSearchService("rerank_other", nil)
		require.NoError(t, err)
		_, err = svc.RerankCandidates(context.Background(), "q", []search.RerankCandidate{{ID: "a", Content: "alpha"}}, nil)
		require.NoError(t, err)
		require.NotNil(t, truncation, "per-database, disable=%v", disable)
		require.Equal(t, !disable, *truncation)
	}
}

func TestExplicitTransaction_OwnerIsolationAcrossCallers(t *testing.T) {
	server, _ := setupTestServer(t)
	dbName := server.dbManager.DefaultDatabaseName()

	ownerA := &auth.JWTClaims{Username: "admin", Roles: []string{"admin"}}
	ownerB := &auth.JWTClaims{Username: "other-admin", Roles: []string{"admin"}}

	openReq := httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx", strings.NewReader(`{"statements":[]}`))
	openReq = openReq.WithContext(context.WithValue(context.Background(), contextKeyClaims, ownerA))
	openRec := httptest.NewRecorder()
	server.handleOpenTransaction(openRec, openReq, dbName)
	require.Equal(t, http.StatusCreated, openRec.Code)

	var openPayload map[string]interface{}
	require.NoError(t, json.NewDecoder(openRec.Body).Decode(&openPayload))
	commitURL, _ := openPayload["commit"].(string)
	require.NotEmpty(t, commitURL)
	parts := strings.Split(commitURL, "/")
	txID := parts[len(parts)-2]
	require.NotEmpty(t, txID)

	execReq := httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx/"+txID, strings.NewReader(`{"statements":[{"statement":"RETURN 1 AS n"}]}`))
	execReq = execReq.WithContext(context.WithValue(context.Background(), contextKeyClaims, ownerB))
	execRec := httptest.NewRecorder()
	server.handleExecuteInTransaction(execRec, execReq, dbName, txID)
	require.Equal(t, http.StatusNotFound, execRec.Code)
}
