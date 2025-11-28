package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

// =============================================================================
// Mock Database for Testing
// =============================================================================

// MockDB implements a minimal mock of nornicdb.DB for testing
type MockDB struct {
	nodes      map[string]*nornicdb.Node
	edges      map[string]*nornicdb.GraphEdge
	createErr  error
	getErr     error
	searchErr  error
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

func containsLabel(labels []string, label string) bool {
	for _, l := range labels {
		if l == label {
			return true
		}
	}
	return false
}

// =============================================================================
// Server Configuration Tests
// =============================================================================

func TestDefaultServerConfig(t *testing.T) {
	config := DefaultServerConfig()

	if config.Address != "localhost" {
		t.Errorf("Expected address localhost, got %s", config.Address)
	}
	if config.Port != 9042 {
		t.Errorf("Expected port 9042, got %d", config.Port)
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
	if len(server.handlers) != 8 {
		t.Errorf("Expected 8 handlers, got %d", len(server.handlers))
	}
}

// Mock Embedder
type mockEmbedder struct {
	embedCalled bool
	embedding   []float32
}

func (m *mockEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	m.embedCalled = true
	if m.embedding != nil {
		return m.embedding, nil
	}
	return make([]float32, 1024), nil
}

func (m *mockEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	result := make([][]float32, len(texts))
	for i := range texts {
		result[i] = make([]float32, 1024)
	}
	return result, nil
}

func (m *mockEmbedder) Model() string   { return "mock-embed" }
func (m *mockEmbedder) Dimensions() int { return 1024 }

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
	if len(resp.Tools) != 8 {
		t.Errorf("Expected 8 tools, got %d", len(resp.Tools))
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

// =============================================================================
// Tool Handler Tests (without database - fallback mode)
// =============================================================================

func TestHandleStore_NoDB(t *testing.T) {
	server := NewServer(nil, nil)
	ctx := context.Background()

	result, err := server.handleStore(ctx, map[string]interface{}{
		"content": "Test content",
	})
	if err != nil {
		t.Fatalf("handleStore() error = %v", err)
	}

	storeResult := result.(StoreResult)
	if storeResult.ID == "" {
		t.Error("Expected ID in result")
	}

	// Missing content
	_, err = server.handleStore(ctx, map[string]interface{}{})
	if err == nil {
		t.Error("Expected error for missing content")
	}
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

	// Invalid relation
	_, err = server.handleLink(ctx, map[string]interface{}{"from": "a", "to": "b", "relation": "invalid"})
	if err == nil {
		t.Error("Expected error for invalid relation")
	}
}

func TestHandleIndex_NoDB(t *testing.T) {
	server := NewServer(nil, nil)
	ctx := context.Background()

	result, err := server.handleIndex(ctx, map[string]interface{}{
		"path": "/test/path",
	})
	if err != nil {
		t.Fatalf("handleIndex() error = %v", err)
	}

	indexResult := result.(IndexResult)
	if indexResult.WatchID == "" {
		t.Error("Expected WatchID")
	}

	// Missing path
	_, err = server.handleIndex(ctx, map[string]interface{}{})
	if err == nil {
		t.Error("Expected error for missing path")
	}
}

func TestHandleUnindex_NoDB(t *testing.T) {
	server := NewServer(nil, nil)
	ctx := context.Background()

	// First index
	indexResult, _ := server.handleIndex(ctx, map[string]interface{}{"path": "/test"})
	watchID := indexResult.(IndexResult).WatchID

	// Unindex
	_, err := server.handleUnindex(ctx, map[string]interface{}{"watch_id": watchID})
	if err != nil {
		t.Fatalf("handleUnindex() error = %v", err)
	}

	// Non-existent
	_, err = server.handleUnindex(ctx, map[string]interface{}{"watch_id": "non-existent"})
	if err == nil {
		t.Error("Expected error for non-existent watch")
	}
}

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
// Store with Embedding Tests
// =============================================================================

func TestHandleStore_WithEmbedding(t *testing.T) {
	embedder := &mockEmbedder{}
	config := DefaultServerConfig()
	config.Embedder = embedder
	config.EmbeddingEnabled = true

	server := NewServer(nil, config)
	ctx := context.Background()

	result, err := server.handleStore(ctx, map[string]interface{}{
		"content": "Test content for embedding",
	})
	if err != nil {
		t.Fatalf("handleStore() error = %v", err)
	}

	storeResult := result.(StoreResult)
	if !storeResult.Embedded {
		t.Error("Expected Embedded=true when embedder available")
	}
	if !embedder.embedCalled {
		t.Error("Expected embedder to be called")
	}
}
