package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/textchunk"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Mock Database for Testing
// =============================================================================

// MockDB implements a minimal mock of nornicdb.DB for testing
type MockDB struct {
	nodes     map[string]*nornicdb.Node
	edges     map[string]*nornicdb.GraphEdge
	createErr error
	getErr    error
	searchErr error
}

func NewMockDB() *MockDB {
	return &MockDB{
		nodes: make(map[string]*nornicdb.Node),
		edges: make(map[string]*nornicdb.GraphEdge),
	}
}

func (m *MockDB) CreateNode(ctx context.Context, labels []string, properties map[string]interface{}) (*nornicdb.Node, error) {
	if m.createErr != nil {
		return nil, m.createErr
	}
	id := properties["id"]
	if id == nil {
		id = "mock-node-" + time.Now().Format("20060102150405.000000000")
	}
	node := &nornicdb.Node{
		ID:         id.(string),
		Labels:     labels,
		Properties: properties,
		CreatedAt:  time.Now(),
	}
	m.nodes[node.ID] = node
	return node, nil
}

func (m *MockDB) GetNode(ctx context.Context, id string) (*nornicdb.Node, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	if node, ok := m.nodes[id]; ok {
		return node, nil
	}
	return nil, nornicdb.ErrNotFound
}

func (m *MockDB) UpdateNode(ctx context.Context, id string, properties map[string]interface{}) (*nornicdb.Node, error) {
	if node, ok := m.nodes[id]; ok {
		for k, v := range properties {
			node.Properties[k] = v
		}
		return node, nil
	}
	return nil, nornicdb.ErrNotFound
}

func (m *MockDB) ListNodes(ctx context.Context, label string, limit, offset int) ([]*nornicdb.Node, error) {
	var result []*nornicdb.Node
	for _, n := range m.nodes {
		if label == "" || containsLabel(n.Labels, label) {
			result = append(result, n)
			if len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

func (m *MockDB) CreateEdge(ctx context.Context, source, target, edgeType string, properties map[string]interface{}) (*nornicdb.GraphEdge, error) {
	if m.createErr != nil {
		return nil, m.createErr
	}
	id := "mock-edge-" + time.Now().Format("20060102150405.000000000")
	edge := &nornicdb.GraphEdge{
		ID:         id,
		Source:     source,
		Target:     target,
		Type:       edgeType,
		Properties: properties,
		CreatedAt:  time.Now(),
	}
	m.edges[id] = edge
	return edge, nil
}

func (m *MockDB) Search(ctx context.Context, query string, labels []string, limit int) ([]*nornicdb.SearchResult, error) {
	if m.searchErr != nil {
		return nil, m.searchErr
	}
	return []*nornicdb.SearchResult{}, nil
}

func (m *MockDB) HybridSearch(ctx context.Context, query string, queryEmbedding []float32, labels []string, limit int) ([]*nornicdb.SearchResult, error) {
	if m.searchErr != nil {
		return nil, m.searchErr
	}
	return []*nornicdb.SearchResult{}, nil
}

func (m *MockDB) ExecuteCypher(ctx context.Context, query string, params map[string]interface{}) (*nornicdb.CypherResult, error) {
	return &nornicdb.CypherResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
	}, nil
}

// =============================================================================
// Server Configuration Tests
// =============================================================================

func TestDefaultServerConfig(t *testing.T) {
	config := DefaultServerConfig()

	if config.Address != "localhost" {
		t.Errorf("Expected address localhost, got %s", config.Address)
	}
	if config.Port != 7474 {
		t.Errorf("Expected port 7474, got %d", config.Port)
	}
	if config.ReadTimeout != 30*time.Second {
		t.Errorf("Expected read timeout 30s, got %v", config.ReadTimeout)
	}
}

func TestNewServer(t *testing.T) {
	server := NewServer(nil, nil)
	if server == nil {
		t.Fatal("NewServer() returned nil")
	}
	// Note: 6 handlers now - index/unindex removed (handled by the application layer)
	if len(server.handlers) != 6 {
		t.Errorf("Expected 6 handlers, got %d", len(server.handlers))
	}
}

// Mock Embedder
type mockEmbedder struct {
	embedCalled      bool
	embedBatchCalled bool
	batchTexts       []string
	embedding        []float32
}

func countTestTokens(text string) (int, error) {
	return len(strings.Fields(text)), nil
}

func chunkTestText(text string, maxTokens, overlap int) ([]string, error) {
	return textchunk.ChunkByTokenCount(text, maxTokens, overlap, countTestTokens)
}

func (m *mockEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	m.embedCalled = true
	if m.embedding != nil {
		return m.embedding, nil
	}
	return make([]float32, 1024), nil
}

func (m *mockEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	m.embedBatchCalled = true
	m.batchTexts = append([]string(nil), texts...)
	result := make([][]float32, len(texts))
	for i := range texts {
		if m.embedding != nil {
			result[i] = append([]float32(nil), m.embedding...)
		} else {
			result[i] = make([]float32, 1024)
		}
	}
	return result, nil
}

func (m *mockEmbedder) Model() string   { return "mock-embed" }
func (m *mockEmbedder) Dimensions() int { return 1024 }
func (m *mockEmbedder) Backend() string { return "cpu" } // Plan 04-05 D-06
func (m *mockEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return chunkTestText(text, maxTokens, overlap)
}

func TestSetEmbedder(t *testing.T) {
	server := NewServer(nil, nil)
	embedder := &mockEmbedder{}
	server.SetEmbedder(embedder)
	if !server.config.EmbeddingEnabled {
		t.Error("Embedding should be enabled after SetEmbedder")
	}
}

// =============================================================================
// HTTP Handler Tests
// =============================================================================

func TestHandleHealth(t *testing.T) {
	server := NewServer(nil, nil)
	server.started = time.Now()

	req := httptest.NewRequest("GET", "/mcp/health", nil)
	rec := httptest.NewRecorder()
	server.handleHealth(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", rec.Code)
	}

	var resp map[string]interface{}
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp["status"] != "healthy" {
		t.Errorf("Expected status=healthy")
	}
}

func TestHandleListTools(t *testing.T) {
	server := NewServer(nil, nil)

	req := httptest.NewRequest("GET", "/mcp/tools/list", nil)
	rec := httptest.NewRecorder()
	server.handleListTools(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", rec.Code)
	}

	var resp ListToolsResponse
	json.NewDecoder(rec.Body).Decode(&resp)
	// Note: 6 tools now - index/unindex removed (handled by the application layer)
	if len(resp.Tools) != 6 {
		t.Errorf("Expected 6 tools, got %d", len(resp.Tools))
	}
}

func TestHandleCallTool(t *testing.T) {
	server := NewServer(nil, nil)

	body := `{"name":"store","arguments":{"content":"test content"}}`
	req := httptest.NewRequest("POST", "/mcp/tools/call", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	server.handleCallTool(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", rec.Code)
	}
}

func TestHandleCallTool_MaxRequestSizeEnforced(t *testing.T) {
	cfg := DefaultServerConfig()
	cfg.MaxRequestSize = 10 // Intentionally tiny to force truncation
	server := NewServer(nil, cfg)

	body := `{"name":"store","arguments":{"content":"test content"}}`
	req := httptest.NewRequest("POST", "/mcp/tools/call", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	server.handleCallTool(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d", rec.Code)
	}
}

func TestHandleInitialize_MaxRequestSizeEnforced(t *testing.T) {
	cfg := DefaultServerConfig()
	cfg.MaxRequestSize = 10 // Intentionally tiny to force truncation
	server := NewServer(nil, cfg)

	body := `{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"x","version":"1"}}`
	req := httptest.NewRequest("POST", "/mcp/initialize", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	server.handleInitialize(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d", rec.Code)
	}
}

func TestHandleMCP_Initialize(t *testing.T) {
	server := NewServer(nil, nil)

	body := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`
	req := httptest.NewRequest("POST", "/mcp", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	server.handleMCP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", rec.Code)
	}
}

func TestHandleMCP_UnknownMethod(t *testing.T) {
	server := NewServer(nil, nil)

	body := `{"jsonrpc":"2.0","id":1,"method":"unknown","params":{}}`
	req := httptest.NewRequest("POST", "/mcp", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	server.handleMCP(rec, req)

	var resp map[string]interface{}
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp["error"] == nil {
		t.Error("Expected error for unknown method")
	}
}

func TestServerRouteAndWrapperHelpers(t *testing.T) {
	t.Run("register routes and serve http", func(t *testing.T) {
		server := NewServer(nil, nil)
		mux := http.NewServeMux()
		server.RegisterRoutes(mux)
		require.False(t, server.started.IsZero())

		req := httptest.NewRequest(http.MethodGet, "/mcp/health", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)

		rec = httptest.NewRecorder()
		server.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/does-not-exist", nil))
		require.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("call tool wrapper", func(t *testing.T) {
		server := NewServer(nil, nil)
		result, err := server.CallTool(context.Background(), ToolRecall, map[string]interface{}{"id": "node-1"})
		require.NoError(t, err)
		recallResult, ok := result.(RecallResult)
		require.True(t, ok)
		require.Equal(t, 1, recallResult.Count)

		_, err = server.CallTool(context.Background(), "missing-tool", nil)
		require.Error(t, err)
	})

	t.Run("start and stop lifecycle", func(t *testing.T) {
		cfg := DefaultServerConfig()
		cfg.Address = "127.0.0.1"
		cfg.Port = 0
		server := NewServer(nil, cfg)
		require.NoError(t, server.Start("127.0.0.1:0"))
		require.NoError(t, server.Stop(context.Background()))

		server.closed = true
		require.Error(t, server.Start("127.0.0.1:0"))
		require.NoError(t, server.Stop(context.Background()))
	})
}

func TestMCPHTTPAndJSONRPCBranches(t *testing.T) {
	server := NewServer(nil, nil)

	t.Run("handleMCP rejects non-post", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/mcp", nil)
		rec := httptest.NewRecorder()
		server.handleMCP(rec, req)
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
		require.Contains(t, rec.Body.String(), "POST required")
	})

	t.Run("handleMCP parse error", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewBufferString("{"))
		rec := httptest.NewRecorder()
		server.handleMCP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), "Parse error")
	})

	t.Run("handleMCP tools list", func(t *testing.T) {
		body := `{"jsonrpc":"2.0","id":7,"method":"tools/list","params":{}}`
		req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewBufferString(body))
		rec := httptest.NewRecorder()
		server.handleMCP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), `"tools"`)
	})

	t.Run("handleMCP tools call wraps tool errors", func(t *testing.T) {
		body := `{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"store","arguments":{"content":"persist me"}}}`
		req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewBufferString(body))
		rec := httptest.NewRecorder()
		server.handleMCP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), `"isError":true`)
		require.Contains(t, rec.Body.String(), "no database executor available")
	})

	t.Run("handleMCP tools call success", func(t *testing.T) {
		db, err := nornicdb.Open("", nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		dbServer := NewServer(db, nil)
		body := `{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"store","arguments":{"content":"json rpc store"}}}`
		req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewBufferString(body))
		rec := httptest.NewRecorder()
		dbServer.handleMCP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), `"jsonrpc":"2.0"`)
		require.Contains(t, rec.Body.String(), `json rpc store`)
		require.NotContains(t, rec.Body.String(), `"isError":true`)
	})

	t.Run("handleInitialize invalid json", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/mcp/initialize", bytes.NewBufferString("{"))
		rec := httptest.NewRecorder()
		server.handleInitialize(rec, req)
		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Contains(t, rec.Body.String(), "invalid request body")
	})

	t.Run("handleInitialize success", func(t *testing.T) {
		body := `{"protocolVersion":"2024-11-05","capabilities":{"tools":{}},"clientInfo":{"name":"tester","version":"1.0"}}`
		req := httptest.NewRequest(http.MethodPost, "/mcp/initialize", bytes.NewBufferString(body))
		rec := httptest.NewRecorder()
		server.handleInitialize(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), "NornicDB MCP Server")
	})

	t.Run("handleInitialize rejects non-post", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/mcp/initialize", nil)
		rec := httptest.NewRecorder()
		server.handleInitialize(rec, req)
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	})

	t.Run("handleListTools rejects unsupported method", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/mcp/tools/list", nil)
		rec := httptest.NewRecorder()
		server.handleListTools(rec, req)
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	})

	t.Run("handleCallTool invalid json", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/mcp/tools/call", bytes.NewBufferString("{"))
		rec := httptest.NewRecorder()
		server.handleCallTool(rec, req)
		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Contains(t, rec.Body.String(), "invalid request body")
	})

	t.Run("handleCallTool rejects non-post", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/mcp/tools/call", nil)
		rec := httptest.NewRecorder()
		server.handleCallTool(rec, req)
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
		require.Contains(t, rec.Body.String(), "POST required")
	})

	t.Run("handleCallTool succeeds with database", func(t *testing.T) {
		db, err := nornicdb.Open("", nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		dbServer := NewServer(db, nil)
		body := `{"name":"store","arguments":{"content":"http tool content","type":"Note"}}`
		req := httptest.NewRequest(http.MethodPost, "/mcp/tools/call", bytes.NewBufferString(body))
		rec := httptest.NewRecorder()
		dbServer.handleCallTool(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), `http tool content`)
		require.NotContains(t, rec.Body.String(), `"isError":true`)
	})

	t.Run("servehttp dispatches mcp routes", func(t *testing.T) {
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/mcp/tools/list", nil))
		require.Equal(t, http.StatusOK, rec.Code)

		rec = httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewBufferString(`{"jsonrpc":"2.0","id":9,"method":"initialize","params":{}}`))
		server.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), `"jsonrpc":"2.0"`)

		rec = httptest.NewRecorder()
		callReq := httptest.NewRequest(http.MethodPost, "/mcp/tools/call", bytes.NewBufferString(`{"name":"recall","arguments":{"id":"node-1"}}`))
		server.ServeHTTP(rec, callReq)
		require.Equal(t, http.StatusOK, rec.Code)

		rec = httptest.NewRecorder()
		server.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/mcp/health", nil))
		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), "healthy")
	})
}

func TestServerStorageAndExecutorHelpers(t *testing.T) {
	t.Run("database scoped executor unavailable", func(t *testing.T) {
		server := NewServer(nil, nil)
		var calledDB string
		server.SetDatabaseScopedExecutor(func(dbName string) (*cypher.StorageExecutor, func(context.Context, string) (*nornicdb.Node, error), error) {
			calledDB = dbName
			return nil, nil, nil
		})

		_, _, err := server.getExecutorAndGetNode(ContextWithDatabase(context.Background(), "tenant_a"))
		require.Error(t, err)
		require.Equal(t, "tenant_a", calledDB)
	})

	t.Run("database scoped storage and related nodes", func(t *testing.T) {
		server := NewServer(nil, nil)
		engine := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })

		_, err := engine.CreateNode(&storage.Node{ID: "tenant_a:1", Labels: []string{"Memory"}, Properties: map[string]interface{}{"title": "Root"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(&storage.Node{ID: "tenant_a:2", Labels: []string{"Task"}, Properties: map[string]interface{}{"name": "Neighbor"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(&storage.Node{ID: "tenant_a:3", Labels: []string{"Doc"}, Properties: map[string]interface{}{"title": "Incoming"}})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "tenant_a:e1", StartNode: "tenant_a:1", EndNode: "tenant_a:2", Type: "RELATES_TO"}))
		require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "tenant_a:e2", StartNode: "tenant_a:3", EndNode: "tenant_a:1", Type: "REFERENCES"}))

		var calledDB string
		server.SetDatabaseScopedStorage(func(dbName string) (storage.Engine, error) {
			calledDB = dbName
			return engine, nil
		})

		store, err := server.storageForContext(ContextWithDatabase(context.Background(), "tenant_a"))
		require.NoError(t, err)
		require.NotNil(t, store)
		require.Equal(t, "tenant_a", calledDB)

		related := server.getRelatedNodes(ContextWithDatabase(context.Background(), "tenant_a"), "tenant_a:1", 2)
		require.Len(t, related, 2)
		require.ElementsMatch(t, []string{"outgoing", "incoming"}, []string{related[0].Direction, related[1].Direction})
		require.ElementsMatch(t, []string{"Task", "Doc"}, []string{related[0].Type, related[1].Type})

		require.Nil(t, server.getRelatedNodes(context.Background(), "tenant_a:1", 1))
		require.Nil(t, server.getRelatedNodes(context.Background(), "", 2))
	})

	t.Run("default database fallback and direct db helpers", func(t *testing.T) {
		db, err := nornicdb.Open("", nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		server := NewServer(db, nil)
		exec := db.GetCypherExecutor()
		require.NotNil(t, exec)

		createResult, err := exec.Execute(context.Background(),
			"CREATE (n:Memory {title: 'helper node'}) RETURN elementId(n) AS id", nil)
		require.NoError(t, err)
		nodeID, _ := createResult.Rows[0][0].(string)

		gotExec, getNode, err := server.getExecutorAndGetNode(context.Background())
		require.NoError(t, err)
		require.NotNil(t, gotExec)
		require.NotNil(t, getNode)

		node, err := getNode(context.Background(), nodeID)
		require.NoError(t, err)
		require.Equal(t, "helper node", node.Properties["title"])

		var scopedExecDB string
		server.SetDatabaseScopedExecutor(func(dbName string) (*cypher.StorageExecutor, func(context.Context, string) (*nornicdb.Node, error), error) {
			scopedExecDB = dbName
			return gotExec, getNode, nil
		})
		_, _, err = server.getExecutorAndGetNode(context.Background())
		require.NoError(t, err)
		require.Equal(t, server.DefaultDatabaseName(), scopedExecDB)

		store, err := server.storageForContext(ContextWithDatabase(context.Background(), "tenant_b"))
		require.NoError(t, err)
		ns, ok := store.(*storage.NamespacedEngine)
		require.True(t, ok)
		require.Equal(t, "tenant_b", ns.Namespace())

		var scopedStorageDB string
		server.SetDatabaseScopedStorage(func(dbName string) (storage.Engine, error) {
			scopedStorageDB = dbName
			return store, nil
		})
		_, err = server.storageForContext(context.Background())
		require.NoError(t, err)
		require.Equal(t, server.DefaultDatabaseName(), scopedStorageDB)
	})

	t.Run("nil database storage returns nil", func(t *testing.T) {
		server := NewServer(nil, nil)
		store, err := server.storageForContext(context.Background())
		require.NoError(t, err)
		require.Nil(t, store)
	})
}

func TestServerUtilityHelpers(t *testing.T) {
	args := map[string]interface{}{"database": " tenant_a ", "db": "tenant_b"}
	require.Equal(t, "tenant_a", extractDatabaseArg(args))
	require.NotContains(t, args, "database")
	require.NotContains(t, args, "db")

	require.Equal(t, "4:nornicdb:node-1", normalizeNodeElementID("node-1"))
	require.Equal(t, "4:tenant:node-1", normalizeNodeElementID("4:tenant:node-1"))
	require.Equal(t, "node-1", localNodeIDFromAny("4:nornicdb:node-1"))
	require.Equal(t, "node-2", localNodeIDFromAny("4:tenant:node-2"))
	require.Equal(t, "", localNodeIDFromAny(" "))

	props := map[string]interface{}{"tags": []interface{}{"a", "b", 3}}
	require.Equal(t, []string{"a", "b"}, getStringSliceProp(props, "tags"))
	require.Equal(t, []string{"x", "y"}, getStringSliceProp(map[string]interface{}{"tags": []string{"x", "y"}}, "tags"))
	require.Nil(t, getStringSliceProp(nil, "tags"))

	out := toInterfaceMap(map[string]any{"score": 1, "title": "x"})
	require.Equal(t, map[string]interface{}{"score": 1, "title": "x"}, out)
	require.Nil(t, toInterfaceMap(nil))

	require.Equal(t, []string{"a", "b"}, getStringSlice(map[string]interface{}{"tags": []interface{}{"a", "b"}}, "tags"))
	require.Equal(t, 3.0, getFloat64(map[string]interface{}{"score": 3}, "score", 1.5))
	require.Equal(t, 1.5, getFloat64(map[string]interface{}{}, "score", 1.5))
	require.Equal(t, map[string]interface{}{"k": "v"}, getMap(map[string]interface{}{"meta": map[string]interface{}{"k": "v"}}, "meta"))
	require.Nil(t, getMap(map[string]interface{}{"meta": "bad"}, "meta"))
}

// =============================================================================
// Tool Handler Tests (without database - fallback mode)
// =============================================================================

func TestHandleStore_NoDB(t *testing.T) {
	server := NewServer(nil, nil)
	ctx := context.Background()

	// With no DB and no DatabaseScopedExecutor, store must fail (no silent fake ID).
	result, err := server.handleStore(ctx, map[string]interface{}{
		"content": "Test content",
	})
	if err == nil {
		t.Fatalf("handleStore() expected error when no database executor available, got result: %v", result)
	}
	if result != nil {
		t.Errorf("handleStore() expected nil result on error, got %v", result)
	}

	// Missing content
	_, err = server.handleStore(ctx, map[string]interface{}{})
	if err == nil {
		t.Error("Expected error for missing content")
	}
}

func TestHandleStore_WithDB(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	server := NewServer(db, nil)
	ctx := context.Background()

	result, err := server.handleStore(ctx, map[string]interface{}{
		"content": "First line\nSecond line should not become title",
		"type":    "Note",
		"tags":    []interface{}{"alpha", "beta"},
		"metadata": map[string]interface{}{
			"owner":     "alice",
			"embedding": []float32{1, 2, 3},
			"vector":    "drop-me",
		},
	})
	require.NoError(t, err)

	store := result.(StoreResult)
	require.NotEmpty(t, store.ID)
	require.Equal(t, "First line", store.Title)
	require.True(t, store.Embedded)

	node, err := db.GetNode(ctx, localNodeIDFromAny(store.ID))
	require.NoError(t, err)
	require.Contains(t, node.Labels, "Note")
	require.Equal(t, "First line", node.Properties["title"])
	require.Equal(t, "First line\nSecond line should not become title", node.Properties["content"])
	require.Equal(t, "alice", node.Properties["owner"])
	require.ElementsMatch(t, []string{"alpha", "beta"}, getStringSliceProp(toInterfaceMap(node.Properties), "tags"))
	_, hasEmbedding := node.Properties["embedding"]
	require.False(t, hasEmbedding)
	_, hasVector := node.Properties["vector"]
	require.False(t, hasVector)

	_, err = server.handleStore(ctx, map[string]interface{}{
		"content": "bad",
		"type":    "123bad",
	})
	require.ErrorContains(t, err, "invalid label")
}

func TestHandleRecall_NoDB(t *testing.T) {
	server := NewServer(nil, nil)
	ctx := context.Background()

	result, err := server.handleRecall(ctx, map[string]interface{}{
		"id": "node-123",
	})
	if err != nil {
		t.Fatalf("handleRecall() error = %v", err)
	}

	recallResult := result.(RecallResult)
	if recallResult.Count != 1 {
		t.Errorf("Expected count=1, got %d", recallResult.Count)
	}
}

func TestHandleRecall_WithDB(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	exec := db.GetCypherExecutor()
	require.NotNil(t, exec)

	ctx := context.Background()
	createNode := func(label, title, content, createdAt string, tags []string) string {
		result, err := exec.Execute(ctx,
			"CREATE (n:"+label+" {title: $title, content: $content, created_at: $createdAt, tags: $tags}) RETURN elementId(n) AS id",
			map[string]interface{}{
				"title":     title,
				"content":   content,
				"createdAt": createdAt,
				"tags":      tags,
			},
		)
		require.NoError(t, err)
		id, ok := result.Rows[0][0].(string)
		require.True(t, ok)
		return id
	}

	targetID := createNode("Memory", "Recent Memory", "important content", "2026-03-01T10:00:00Z", []string{"alpha", "beta"})
	_ = createNode("Task", "Old Task", "older content", "2025-01-01T10:00:00Z", []string{"beta"})

	server := NewServer(db, nil)

	t.Run("recalls node by id from database", func(t *testing.T) {
		result, err := server.handleRecall(ctx, map[string]interface{}{"id": targetID})
		require.NoError(t, err)

		recall := result.(RecallResult)
		require.Equal(t, 1, recall.Count)
		require.Len(t, recall.Nodes, 1)
		require.Equal(t, targetID, recall.Nodes[0].ID)
		require.Equal(t, "Memory", recall.Nodes[0].Type)
		require.Equal(t, "Recent Memory", recall.Nodes[0].Title)
		require.Equal(t, "important content", recall.Nodes[0].Content)
	})

	t.Run("returns error when requested id does not exist", func(t *testing.T) {
		_, err := server.handleRecall(ctx, map[string]interface{}{"id": "missing"})
		require.ErrorContains(t, err, "node not found")
	})

	t.Run("filters recalled nodes by type", func(t *testing.T) {
		result, err := server.handleRecall(ctx, map[string]interface{}{
			"type":  []interface{}{"Memory"},
			"limit": 5,
		})
		require.NoError(t, err)

		recall := result.(RecallResult)
		require.Equal(t, 1, recall.Count)
		require.Len(t, recall.Nodes, 1)
		require.Equal(t, targetID, recall.Nodes[0].ID)
		require.Equal(t, "Recent Memory", recall.Nodes[0].Title)
		require.Equal(t, "important content", recall.Nodes[0].Content)
	})

	t.Run("filters recalled nodes by tags", func(t *testing.T) {
		result, err := server.handleRecall(ctx, map[string]interface{}{
			"tags":  []interface{}{"alpha"},
			"limit": 5,
		})
		require.NoError(t, err)

		recall := result.(RecallResult)
		require.Equal(t, 1, recall.Count)
		require.Len(t, recall.Nodes, 1)
		require.Equal(t, targetID, recall.Nodes[0].ID)
	})

	t.Run("filters recalled nodes by since timestamp", func(t *testing.T) {
		result, err := server.handleRecall(ctx, map[string]interface{}{
			"since": "2026-01-01T00:00:00Z",
			"limit": 5,
		})
		require.NoError(t, err)

		recall := result.(RecallResult)
		require.Equal(t, 1, recall.Count)
		require.Len(t, recall.Nodes, 1)
		require.Equal(t, targetID, recall.Nodes[0].ID)
	})
}

func TestHandleDiscover_NoDB(t *testing.T) {
	server := NewServer(nil, nil)
	ctx := context.Background()

	result, err := server.handleDiscover(ctx, map[string]interface{}{
		"query": "test search",
	})
	if err != nil {
		t.Fatalf("handleDiscover() error = %v", err)
	}

	discoverResult := result.(DiscoverResult)
	if discoverResult.Method != "keyword" {
		t.Errorf("Expected method=keyword, got %s", discoverResult.Method)
	}

	// Missing query
	_, err = server.handleDiscover(ctx, map[string]interface{}{})
	if err == nil {
		t.Error("Expected error for missing query")
	}
}

func TestHandleDiscover_ChunksLongQueryForEmbedding(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	if err != nil {
		t.Fatalf("failed to open in-memory nornicdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	embedder := &mockEmbedder{}
	cfg := DefaultServerConfig()
	cfg.Embedder = embedder
	cfg.EmbeddingEnabled = true

	server := NewServer(db, cfg)
	ctx := context.Background()

	longQuery := strings.Repeat("a ", 650) // >512 tokens, should chunk
	_, err = server.handleDiscover(ctx, map[string]interface{}{
		"query": longQuery,
		"limit": 10,
	})
	if err != nil {
		t.Fatalf("handleDiscover() error = %v", err)
	}

	if !embedder.embedBatchCalled {
		t.Error("expected EmbedBatch to be called for chunked query embedding")
	}
	if embedder.embedCalled {
		t.Error("expected Embed (single) NOT to be called for chunked query embedding")
	}
	if len(embedder.batchTexts) <= 1 {
		t.Errorf("expected query to be chunked into multiple parts, got %d", len(embedder.batchTexts))
	}
	for i, c := range embedder.batchTexts {
		tokens, err := countTestTokens(c)
		if err != nil {
			t.Fatalf("countTestTokens: %v", err)
		}
		if tokens > 520 {
			t.Errorf("expected chunk %d to be <= ~512 tokens, got %d", i, tokens)
		}
	}

	result, err := server.handleDiscover(ctx, map[string]interface{}{
		"query": longQuery,
		"limit": 10,
	})
	if err != nil {
		t.Fatalf("second handleDiscover() error = %v", err)
	}
	discoverResult := result.(DiscoverResult)
	require.Equal(t, "vector", discoverResult.Method)
}

func TestHandleDiscover_WithDBKeywordResults(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	exec := db.GetCypherExecutor()
	require.NotNil(t, exec)
	ctx := context.Background()

	rootResult, err := exec.Execute(ctx,
		"CREATE (n:Memory {title: $title, content: $content}) RETURN elementId(n) AS id",
		map[string]interface{}{"title": "Alpha Root", "content": "alpha unique keyword content"},
	)
	require.NoError(t, err)
	rootID, _ := rootResult.Rows[0][0].(string)

	neighborResult, err := exec.Execute(ctx,
		"CREATE (n:Task {title: $title, content: $content}) RETURN elementId(n) AS id",
		map[string]interface{}{"title": "Neighbor Task", "content": "supporting context"},
	)
	require.NoError(t, err)
	neighborID, _ := neighborResult.Rows[0][0].(string)

	_, err = exec.Execute(ctx,
		"MATCH (a), (b) WHERE elementId(a) = $from AND elementId(b) = $to CREATE (a)-[:RELATES_TO]->(b)",
		map[string]interface{}{"from": rootID, "to": neighborID},
	)
	require.NoError(t, err)
	require.NoError(t, db.BuildSearchIndexes(context.Background()))

	server := NewServer(db, nil)
	result, err := server.handleDiscover(ctx, map[string]interface{}{
		"query": "alpha unique keyword",
		"depth": 2,
		"limit": 5,
	})
	require.NoError(t, err)

	discover := result.(DiscoverResult)
	require.Equal(t, "keyword", discover.Method)
	require.NotZero(t, discover.Total)
	require.NotEmpty(t, discover.Results)
	require.Equal(t, "Alpha Root", discover.Results[0].Title)
	require.Equal(t, "Memory", discover.Results[0].Type)
	require.NotEmpty(t, discover.Results[0].Related)
}

func TestHandleDiscover_VectorBranchWithManualEmbeddings(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	engine := db.GetStorage()
	vector := make([]float32, 1024)
	vector[0] = 1.0

	_, err = engine.CreateNode(&storage.Node{
		ID:     "vector-node",
		Labels: []string{"Memory"},
		Properties: map[string]interface{}{
			"title":   "Vector Match",
			"content": "manual embedded content",
		},
		ChunkEmbeddings: [][]float32{vector},
	})
	require.NoError(t, err)
	require.NoError(t, db.BuildSearchIndexes(context.Background()))

	embedder := &mockEmbedder{batchTexts: nil}
	serverCfg := DefaultServerConfig()
	serverCfg.Embedder = embedder
	serverCfg.EmbeddingEnabled = true
	server := NewServer(db, serverCfg)

	result, err := server.handleDiscover(context.Background(), map[string]interface{}{
		"query": "manual embedded content",
		"limit": 5,
		"depth": 0,
	})
	require.NoError(t, err)
	discover := result.(DiscoverResult)
	require.Equal(t, "vector", discover.Method)
}

// TestHandleDiscover_SimilarityIsCosineNotRRF is a regression test for the bug
// where `similarity` in the discover response was the outer RRF rank-decay
// score (~0.0164 at rank 1, 0.0091 at rank 50) instead of the underlying
// cosine score. The RRF rank score is by construction identical across
// queries (it only depends on rank position), making `min_similarity`
// thresholds useless.
//
// This test sets up two nodes with controlled embeddings: a "match" node
// whose embedding equals the query embedding (cosine = 1.0) and a "miss"
// node whose embedding is orthogonal (cosine = 0.0). It then asserts:
//
//  1. The returned `similarity` for the match is approximately 1.0 — i.e.
//     clearly NOT 0.0164 (which would mean RRF rank-1 score leaked through).
//  2. The match outranks the miss.
//  3. A `min_similarity=0.5` threshold drops the orthogonal node — the
//     threshold semantics only work if `similarity` is in cosine space.
func TestHandleDiscover_SimilarityIsCosineNotRRF(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	engine := db.GetStorage()

	matchVec := make([]float32, 1024)
	matchVec[0] = 1.0 // unit vector along axis 0
	missVec := make([]float32, 1024)
	missVec[1] = 1.0 // orthogonal to matchVec

	_, err = engine.CreateNode(&storage.Node{
		ID:     "match-node",
		Labels: []string{"Memory"},
		Properties: map[string]interface{}{
			"title":   "Match",
			"content": "cosine-similar to the query",
		},
		ChunkEmbeddings: [][]float32{matchVec},
	})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{
		ID:     "miss-node",
		Labels: []string{"Memory"},
		Properties: map[string]interface{}{
			"title":   "Miss",
			"content": "orthogonal to the query",
		},
		ChunkEmbeddings: [][]float32{missVec},
	})
	require.NoError(t, err)
	require.NoError(t, db.BuildSearchIndexes(context.Background()))

	// Embedder echoes matchVec for every query — query embedding == match-node embedding.
	embedder := &mockEmbedder{embedding: matchVec}
	serverCfg := DefaultServerConfig()
	serverCfg.Embedder = embedder
	serverCfg.EmbeddingEnabled = true
	server := NewServer(db, serverCfg)

	result, err := server.handleDiscover(context.Background(), map[string]interface{}{
		"query": "anything — embedder ignores the text",
		"limit": 5,
		"depth": 0,
	})
	require.NoError(t, err)
	discover := result.(DiscoverResult)
	require.Equal(t, "vector", discover.Method)
	require.NotEmpty(t, discover.Results, "expected match-node to appear in results")

	// Locate the match-node similarity in the response.
	var matchSim float64
	var matchFound bool
	for _, r := range discover.Results {
		if r.Title == "Match" {
			matchSim = r.Similarity
			matchFound = true
			break
		}
	}
	require.True(t, matchFound, "match-node not in results")

	// The smoking gun: the RRF rank-1 score is 1/(60+1) ≈ 0.01639. Cosine
	// of a unit vector with itself is 1.0. Anything in cosine territory
	// (≥ 0.5) proves we're surfacing the real similarity, not RRF.
	require.Greater(t, matchSim, 0.5,
		"match-node similarity should be cosine (~1.0), got %.4f — looks like RRF rank score leaked through", matchSim)

	// And min_similarity threshold semantics: a 0.5 threshold should drop
	// the orthogonal miss-node. If similarity were the RRF rank score
	// (≤ 0.0164), this threshold would drop *everything* including the match.
	thresholded, err := server.handleDiscover(context.Background(), map[string]interface{}{
		"query":          "anything — embedder ignores the text",
		"limit":          5,
		"min_similarity": 0.5,
		"depth":          0,
	})
	require.NoError(t, err)
	td := thresholded.(DiscoverResult)
	require.NotEmpty(t, td.Results, "min_similarity=0.5 dropped everything — similarity is not cosine")
	for _, r := range td.Results {
		require.GreaterOrEqual(t, r.Similarity, 0.5,
			"result %q below threshold (sim=%.4f) — min_similarity filter is broken", r.Title, r.Similarity)
		require.NotEqual(t, "Miss", r.Title, "orthogonal miss-node passed the 0.5 threshold")
	}
}

func TestHandleDiscover_StorageError(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	server := NewServer(db, nil)
	server.SetDatabaseScopedStorage(func(dbName string) (storage.Engine, error) {
		return nil, context.Canceled
	})

	_, err = server.handleDiscover(ContextWithDatabase(context.Background(), "tenant_a"), map[string]interface{}{
		"query": "alpha",
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestServerDefaultDatabaseName(t *testing.T) {
	var nilServer *Server
	require.Equal(t, "", nilServer.DefaultDatabaseName())

	server := NewServer(nil, nil)
	require.Equal(t, "", server.DefaultDatabaseName())

	db, err := nornicdb.Open("", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	server = NewServer(db, nil)
	require.Equal(t, "nornic", server.DefaultDatabaseName())
}

func TestResolveNodeForLink(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	exec := db.GetCypherExecutor()
	require.NotNil(t, exec)

	ctx := context.Background()
	createNode := func(title, externalID string) string {
		result, err := exec.Execute(ctx,
			"CREATE (n:Memory {title: $title, id: $externalID}) RETURN elementId(n) AS id",
			map[string]interface{}{"title": title, "externalID": externalID},
		)
		require.NoError(t, err)
		require.NotEmpty(t, result.Rows)
		id, ok := result.Rows[0][0].(string)
		require.True(t, ok)
		require.NotEmpty(t, id)
		return id
	}

	elementID := createNode("first", "external-1")
	otherElementID := createNode("second", "external-2")

	server := NewServer(db, nil)
	getNode := func(ctx context.Context, id string) (*nornicdb.Node, error) {
		return db.GetNode(ctx, localNodeIDFromAny(id))
	}

	t.Run("returns blank id as not found", func(t *testing.T) {
		node, resolvedID, err := server.resolveNodeForLink(ctx, exec, getNode, "   ")
		require.ErrorIs(t, err, nornicdb.ErrNotFound)
		require.Nil(t, node)
		require.Equal(t, "", resolvedID)
	})

	t.Run("prefers direct getNode by element id", func(t *testing.T) {
		node, resolvedID, err := server.resolveNodeForLink(ctx, exec, getNode, elementID)
		require.NoError(t, err)
		require.NotNil(t, node)
		require.Equal(t, elementID, resolvedID)
		require.Equal(t, "first", node.Properties["title"])
	})

	t.Run("falls back to internal id query", func(t *testing.T) {
		node, resolvedID, err := server.resolveNodeForLink(ctx, exec, nil, localNodeIDFromAny(otherElementID))
		require.NoError(t, err)
		require.NotNil(t, node)
		require.Equal(t, otherElementID, resolvedID)
		require.Equal(t, "second", node.Properties["title"])
	})

	t.Run("falls back to node id property", func(t *testing.T) {
		node, resolvedID, err := server.resolveNodeForLink(ctx, exec, nil, "external-1")
		require.NoError(t, err)
		require.NotNil(t, node)
		require.Equal(t, elementID, resolvedID)
		require.Equal(t, "external-1", node.Properties["id"])
	})

	t.Run("returns normalized id when node not found", func(t *testing.T) {
		node, resolvedID, err := server.resolveNodeForLink(ctx, exec, nil, "missing-id")
		require.ErrorIs(t, err, nornicdb.ErrNotFound)
		require.Nil(t, node)
		require.Equal(t, normalizeNodeElementID("missing-id"), resolvedID)
	})
}

func TestHandleLink_NoDB(t *testing.T) {
	server := NewServer(nil, nil)
	ctx := context.Background()

	result, err := server.handleLink(ctx, map[string]interface{}{
		"from":     "node-1",
		"to":       "node-2",
		"relation": "relates_to",
	})
	if err != nil {
		t.Fatalf("handleLink() error = %v", err)
	}

	linkResult := result.(LinkResult)
	if linkResult.EdgeID == "" {
		t.Error("Expected EdgeID")
	}

	// Missing from
	_, err = server.handleLink(ctx, map[string]interface{}{"to": "x", "relation": "relates_to"})
	if err == nil {
		t.Error("Expected error for missing from")
	}

	// Invalid relation (must be valid identifier; hyphen and leading digit are invalid)
	_, err = server.handleLink(ctx, map[string]interface{}{"from": "a", "to": "b", "relation": "has-hyphen"})
	if err == nil {
		t.Error("Expected error for invalid relation")
	}
	_, err = server.handleLink(ctx, map[string]interface{}{"from": "a", "to": "b", "relation": "123"})
	if err == nil {
		t.Error("Expected error for invalid relation (leading digit)")
	}
}

func TestHandleLink_WithDB(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	exec := db.GetCypherExecutor()
	require.NotNil(t, exec)
	ctx := context.Background()

	createNode := func(title, externalID string) string {
		result, err := exec.Execute(ctx,
			"CREATE (n:Memory {title: $title, id: $externalID}) RETURN elementId(n) AS id",
			map[string]interface{}{"title": title, "externalID": externalID},
		)
		require.NoError(t, err)
		id, ok := result.Rows[0][0].(string)
		require.True(t, ok)
		return id
	}

	fromID := createNode("Source Node", "source-ext")
	toID := createNode("Target Node", "target-ext")

	server := NewServer(db, nil)

	result, err := server.handleLink(ctx, map[string]interface{}{
		"from":     "source-ext",
		"to":       "target-ext",
		"relation": "relates_to",
		"strength": 0.75,
	})
	require.NoError(t, err)

	link := result.(LinkResult)
	require.NotEmpty(t, link.EdgeID)
	require.Equal(t, fromID, link.From.ID)
	require.Equal(t, "Memory", link.From.Type)
	require.Equal(t, "Source Node", link.From.Title)
	require.Equal(t, toID, link.To.ID)
	require.Equal(t, "Target Node", link.To.Title)

	edgeResult, err := exec.Execute(ctx,
		"MATCH (a)-[r:RELATES_TO]->(b) WHERE elementId(a) = $from AND elementId(b) = $to RETURN elementId(r), r.strength",
		map[string]interface{}{"from": fromID, "to": toID},
	)
	require.NoError(t, err)
	require.Len(t, edgeResult.Rows, 1)
	require.Equal(t, link.EdgeID, edgeResult.Rows[0][0])
	require.Equal(t, 0.75, edgeResult.Rows[0][1])

	_, err = server.handleLink(ctx, map[string]interface{}{
		"from":     "missing",
		"to":       "target-ext",
		"relation": "relates_to",
	})
	require.ErrorContains(t, err, "source node not found")

	_, err = server.handleLink(ctx, map[string]interface{}{
		"from":     "source-ext",
		"to":       "missing",
		"relation": "relates_to",
	})
	require.ErrorContains(t, err, "target node not found")
}

func TestHandleLink_ScopedExecutorError(t *testing.T) {
	server := NewServer(nil, nil)
	server.SetDatabaseScopedExecutor(func(dbName string) (*cypher.StorageExecutor, func(context.Context, string) (*nornicdb.Node, error), error) {
		return nil, nil, context.DeadlineExceeded
	})

	_, err := server.handleLink(ContextWithDatabase(context.Background(), "tenant_a"), map[string]interface{}{
		"from":     "a",
		"to":       "b",
		"relation": "relates_to",
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// Note: TestHandleIndex_NoDB and TestHandleUnindex_NoDB removed
// These handlers were removed - file indexing is handled by the application layer

func TestHandleTask_NoDB(t *testing.T) {
	server := NewServer(nil, nil)
	ctx := context.Background()

	result, err := server.handleTask(ctx, map[string]interface{}{
		"title": "Test Task",
	})
	if err != nil {
		t.Fatalf("handleTask() error = %v", err)
	}

	taskResult := result.(TaskResult)
	if taskResult.Task.ID == "" {
		t.Error("Expected task ID")
	}

	// Missing title
	_, err = server.handleTask(ctx, map[string]interface{}{})
	if err == nil {
		t.Error("Expected error for missing title")
	}

	result, err = server.handleTask(ctx, map[string]interface{}{
		"id":     "task-123",
		"status": "active",
	})
	require.NoError(t, err)
	taskResult = result.(TaskResult)
	require.Equal(t, normalizeNodeElementID("task-123"), taskResult.Task.ID)
	require.Equal(t, "active", taskResult.Task.Properties["status"])

	result, err = server.handleTask(ctx, map[string]interface{}{
		"id":     "task-123",
		"delete": true,
	})
	require.NoError(t, err)
	taskResult = result.(TaskResult)
	require.Equal(t, "Task deleted.", taskResult.NextAction)
}

func TestHandleTaskAndTasks_WithDB(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	server := NewServer(db, nil)
	ctx := context.Background()

	makeTask := func(args map[string]interface{}) TaskResult {
		result, err := server.handleTask(ctx, args)
		require.NoError(t, err)
		taskResult := result.(TaskResult)
		require.NotEmpty(t, taskResult.Task.ID)
		return taskResult
	}

	dependency := makeTask(map[string]interface{}{
		"title":    "Dependency",
		"status":   "active",
		"priority": "high",
		"assign":   "alice",
	})

	mainTask := makeTask(map[string]interface{}{
		"title":       "Main Task",
		"description": "needs dependency",
		"priority":    "medium",
		"assign":      "alice",
		"depends_on":  []interface{}{dependency.Task.ID},
	})
	require.Equal(t, "pending", mainTask.Task.Properties["status"])

	completedTask := makeTask(map[string]interface{}{
		"title":    "Completed Task",
		"status":   "done",
		"priority": "low",
		"assign":   "bob",
	})
	require.Equal(t, "completed", completedTask.Task.Properties["status"])

	updatedRaw, err := server.handleTask(ctx, map[string]interface{}{
		"id":          mainTask.Task.ID,
		"title":       "Main Task Updated",
		"description": "updated description",
		"priority":    "critical",
		"assign":      "carol",
	})
	require.NoError(t, err)
	updated := updatedRaw.(TaskResult)
	require.Equal(t, "Main Task Updated", updated.Task.Title)
	require.Equal(t, "updated description", updated.Task.Content)
	require.Equal(t, "active", updated.Task.Properties["status"])
	require.Equal(t, "critical", updated.Task.Properties["priority"])
	require.Equal(t, "carol", updated.Task.Properties["assigned_to"])

	completedRaw, err := server.handleTask(ctx, map[string]interface{}{
		"id":       mainTask.Task.ID,
		"complete": true,
	})
	require.NoError(t, err)
	completedMain := completedRaw.(TaskResult)
	require.Equal(t, "completed", completedMain.Task.Properties["status"])

	listRaw, err := server.handleTasks(ctx, map[string]interface{}{
		"status":      []interface{}{"done"},
		"assigned_to": "bob",
		"priority":    []interface{}{"low"},
		"limit":       10,
	})
	require.NoError(t, err)
	list := listRaw.(TasksResult)
	require.Len(t, list.Tasks, 1)
	require.Equal(t, completedTask.Task.ID, list.Tasks[0].ID)
	require.Equal(t, 1, list.Stats.Total)
	require.Equal(t, 1, list.Stats.ByStatus["completed"])
	require.Equal(t, 1, list.Stats.ByPriority["low"])

	unblockedRaw, err := server.handleTasks(ctx, map[string]interface{}{
		"unblocked_only": true,
		"limit":          10,
	})
	require.NoError(t, err)
	unblocked := unblockedRaw.(TasksResult)
	require.NotEmpty(t, unblocked.Tasks)
	for _, task := range unblocked.Tasks {
		require.NotEqual(t, mainTask.Task.ID, task.ID)
	}

	deletedRaw, err := server.handleTask(ctx, map[string]interface{}{
		"id":     dependency.Task.ID,
		"delete": true,
	})
	require.NoError(t, err)
	deleted := deletedRaw.(TaskResult)
	require.Equal(t, dependency.Task.ID, deleted.Task.ID)
	require.Equal(t, "Deleted 1 task(s).", deleted.NextAction)

	toggleTask := makeTask(map[string]interface{}{
		"title": "Toggle Task",
	})
	toggledRaw, err := server.handleTask(ctx, map[string]interface{}{
		"id": toggleTask.Task.ID,
	})
	require.NoError(t, err)
	toggled := toggledRaw.(TaskResult)
	require.Equal(t, "active", toggled.Task.Properties["status"])

	toggledRaw, err = server.handleTask(ctx, map[string]interface{}{
		"id": toggleTask.Task.ID,
	})
	require.NoError(t, err)
	toggled = toggledRaw.(TaskResult)
	require.Equal(t, "completed", toggled.Task.Properties["status"])

	fallbackServer := NewServer(nil, nil)
	fallbackRaw, err := fallbackServer.handleTask(ctx, map[string]interface{}{
		"id":       "task-fallback",
		"complete": true,
	})
	require.NoError(t, err)
	fallback := fallbackRaw.(TaskResult)
	require.Equal(t, "completed", fallback.Task.Properties["status"])

	_, err = server.handleTask(ctx, map[string]interface{}{
		"id": "missing-task",
	})
	require.ErrorContains(t, err, "task not found")

	_, err = server.handleTask(ctx, map[string]interface{}{
		"delete": true,
	})
	require.ErrorContains(t, err, "id is required for delete")
}

func TestTaskHasIncompleteDependencies(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	exec := db.GetCypherExecutor()
	require.NotNil(t, exec)
	ctx := context.Background()

	createTask := func(title, status string) string {
		result, err := exec.Execute(ctx,
			"CREATE (t:Task {title: $title, status: $status}) RETURN elementId(t) AS id",
			map[string]interface{}{"title": title, "status": status},
		)
		require.NoError(t, err)
		id, _ := result.Rows[0][0].(string)
		return id
	}

	blockedID := createTask("blocked", "pending")
	depID := createTask("dependency", "active")
	doneID := createTask("done", "completed")

	_, err = exec.Execute(ctx,
		"MATCH (a:Task), (b:Task) WHERE elementId(a) = $a AND elementId(b) = $b CREATE (a)-[:DEPENDS_ON]->(b)",
		map[string]interface{}{"a": blockedID, "b": depID},
	)
	require.NoError(t, err)

	blocked, err := taskHasIncompleteDependencies(ctx, exec, blockedID)
	require.NoError(t, err)
	require.True(t, blocked)

	blocked, err = taskHasIncompleteDependencies(ctx, exec, doneID)
	require.NoError(t, err)
	require.False(t, blocked)

	blocked, err = taskHasIncompleteDependencies(ctx, nil, blockedID)
	require.NoError(t, err)
	require.False(t, blocked)

	blocked, err = taskHasIncompleteDependencies(ctx, exec, "")
	require.NoError(t, err)
	require.False(t, blocked)
}

func TestHandleTasks_NoDB(t *testing.T) {
	server := NewServer(nil, nil)
	ctx := context.Background()

	result, err := server.handleTasks(ctx, map[string]interface{}{})
	if err != nil {
		t.Fatalf("handleTasks() error = %v", err)
	}

	tasksResult := result.(TasksResult)
	if tasksResult.Tasks == nil {
		t.Error("Expected Tasks to be initialized")
	}
}

// =============================================================================
// Utility Function Tests
// =============================================================================

func TestGetString(t *testing.T) {
	m := map[string]interface{}{"key": "value"}
	if getString(m, "key") != "value" {
		t.Error("Expected 'value'")
	}
	if getString(m, "missing") != "" {
		t.Error("Expected empty string")
	}
}

func TestGetInt(t *testing.T) {
	m := map[string]interface{}{"int": 42, "float64": float64(43)}
	if getInt(m, "int", 0) != 42 {
		t.Error("Expected 42")
	}
	if getInt(m, "float64", 0) != 43 {
		t.Error("Expected 43")
	}
	if getInt(m, "missing", 99) != 99 {
		t.Error("Expected default 99")
	}
}

func TestGetBool(t *testing.T) {
	m := map[string]interface{}{"true": true, "false": false}
	if !getBool(m, "true", false) {
		t.Error("Expected true")
	}
	if getBool(m, "false", true) {
		t.Error("Expected false")
	}
}

func TestTruncateString(t *testing.T) {
	if truncateString("hello", 10) != "hello" {
		t.Error("Expected unchanged string")
	}
	if truncateString("hello world", 5) != "he..." {
		t.Error("Expected truncated string")
	}
}

func TestGenerateTitle(t *testing.T) {
	if generateTitle("First line\nSecond", 100) != "First line" {
		t.Error("Expected first line")
	}
}

func TestGetLabelType(t *testing.T) {
	if getLabelType([]string{"Memory", "Node"}) != "Memory" {
		t.Error("Expected first label")
	}
	if getLabelType([]string{}) != "Node" {
		t.Error("Expected default Node")
	}
}

func TestGetStringProp(t *testing.T) {
	props := map[string]interface{}{"title": "Test", "count": 42}
	if getStringProp(props, "title") != "Test" {
		t.Error("Expected 'Test'")
	}
	if getStringProp(props, "count") != "" {
		t.Error("Expected empty for non-string")
	}
	if getStringProp(nil, "title") != "" {
		t.Error("Expected empty for nil map")
	}
}

func TestHasAnyTag(t *testing.T) {
	if !hasAnyTag([]string{"a", "b", "c"}, []string{"b", "d"}) {
		t.Error("Expected match for 'b'")
	}
	if hasAnyTag([]string{"a", "b"}, []string{"x", "y"}) {
		t.Error("Expected no match")
	}
}

func TestHasAllTags(t *testing.T) {
	require.True(t, hasAllTags([]string{"a", "b"}, nil))
	require.False(t, hasAllTags(nil, []string{"a"}))
	require.True(t, hasAllTags([]string{"a", "b", "c"}, []string{"a", "c"}))
	require.False(t, hasAllTags([]string{"a", "b"}, []string{"a", "z"}))
}

// =============================================================================
// CORS Tests
// =============================================================================

func TestCORSMiddleware(t *testing.T) {
	server := NewServer(nil, nil)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	wrapped := server.corsMiddleware(handler)

	req := httptest.NewRequest("OPTIONS", "/test", nil)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("Expected 204, got %d", rec.Code)
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("Expected CORS header")
	}
}

// =============================================================================
// Sanitize Properties Tests
// =============================================================================

func TestSanitizePropertiesForLLM(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]interface{}
		wantKeys []string
		skipKeys []string
	}{
		{
			name:     "nil input",
			input:    nil,
			wantKeys: nil,
		},
		{
			name: "removes embedding fields",
			input: map[string]interface{}{
				"title":                "Test",
				"content":              "Some content",
				"embedding":            make([]float32, 1024),
				"embedding_model":      "bge-m3",
				"embedding_dimensions": 1024,
				"has_embedding":        true,
				"embedded_at":          "2024-01-01",
			},
			wantKeys: []string{"title", "content"},
			skipKeys: []string{"embedding", "embedding_model", "embedding_dimensions", "has_embedding", "embedded_at"},
		},
		{
			name: "removes large float arrays",
			input: map[string]interface{}{
				"title":     "Test",
				"bigArray":  make([]float32, 500),
				"smallTags": []interface{}{"tag1", "tag2"},
			},
			wantKeys: []string{"title", "smallTags"},
			skipKeys: []string{"bigArray"},
		},
		{
			name: "keeps normal properties",
			input: map[string]interface{}{
				"title":     "Test Title",
				"content":   "Some content",
				"tags":      []interface{}{"a", "b"},
				"priority":  "high",
				"count":     42,
				"is_active": true,
			},
			wantKeys: []string{"title", "content", "tags", "priority", "count", "is_active"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizePropertiesForLLM(tt.input)

			if tt.input == nil {
				if result != nil {
					t.Error("Expected nil result for nil input")
				}
				return
			}

			// Check wanted keys are present
			for _, key := range tt.wantKeys {
				if _, ok := result[key]; !ok {
					t.Errorf("Expected key %q to be present", key)
				}
			}

			// Check skipped keys are absent
			for _, key := range tt.skipKeys {
				if _, ok := result[key]; ok {
					t.Errorf("Expected key %q to be filtered out", key)
				}
			}
		})
	}
}

// =============================================================================
// Store with Embedding Tests
// =============================================================================

func TestHandleStore_WithEmbedding(t *testing.T) {
	embedder := &mockEmbedder{}
	config := DefaultServerConfig()
	config.Embedder = embedder
	config.EmbeddingEnabled = true

	// With no DB (nil), store must fail; we no longer return a fake ID or call embedder.
	server := NewServer(nil, config)
	ctx := context.Background()

	_, err := server.handleStore(ctx, map[string]interface{}{
		"content": "Test content for embedding",
	})
	if err == nil {
		t.Fatal("handleStore() expected error when no database (nil db)")
	}
	// Embedder is not called when there is no executor (no persist path).
	if embedder.embedCalled {
		t.Error("Expected embedder not to be called when store fails (no DB)")
	}
}
