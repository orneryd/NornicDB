package heimdall

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockDBRouter is a mock implementation of DatabaseRouter for testing.
type mockDBRouter struct{}

func (m *mockDBRouter) DefaultDatabaseName() string { return "nornic" }
func (m *mockDBRouter) ResolveDatabase(nameOrAlias string) (string, error) {
	if nameOrAlias == "" {
		return "nornic", nil
	}
	return nameOrAlias, nil
}
func (m *mockDBRouter) ListDatabases() []string { return []string{"nornic"} }

func (m *mockDBRouter) Query(ctx context.Context, database string, cypher string, params map[string]interface{}) ([]map[string]interface{}, error) {
	return []map[string]interface{}{{"count": int64(42)}}, nil
}

func (m *mockDBRouter) Stats(ctx context.Context, database string) (DatabaseStats, error) {
	return DatabaseStats{NodeCount: 100, RelationshipCount: 50}, nil
}

func (m *mockDBRouter) Discover(ctx context.Context, database string, query string, nodeTypes []string, limit int, depth int) (*DiscoverResult, error) {
	return &DiscoverResult{
		Results: []SearchResult{
			{
				ID:         "node-1",
				Type:       "TestNode",
				Title:      "Test Result",
				Similarity: 0.95,
			},
		},
		Method: "mock",
		Total:  1,
	}, nil
}

// mockMetricsReader is a mock implementation of MetricsReader for testing
type mockMetricsReader struct{}

func (m *mockMetricsReader) Runtime() RuntimeMetrics {
	return RuntimeMetrics{GoroutineCount: 10, MemoryAllocMB: 100, NumGC: 5}
}

// testHandler creates a handler with mock db and metrics for testing
func testHandler(manager *Manager, cfg Config) *Handler {
	return NewHandler(manager, cfg, &mockDBRouter{}, &mockMetricsReader{})
}

func TestNewHandler_Disabled(t *testing.T) {
	// When manager is nil (disabled), handler should be nil
	handler := NewHandler(nil, Config{}, nil, nil)
	assert.Nil(t, handler)
}

func TestNewHandler_Enabled(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)

	handler := testHandler(manager, manager.config)

	assert.NotNil(t, handler)
}

func TestHandler_Status(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	req := httptest.NewRequest(http.MethodGet, "/api/bifrost/status", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	var body map[string]interface{}
	err := json.NewDecoder(resp.Body).Decode(&body)
	require.NoError(t, err)

	assert.Equal(t, "ok", body["status"])
	assert.Equal(t, "test-model", body["model"])

	// Verify heimdall section
	heimdall, ok := body["heimdall"].(map[string]interface{})
	require.True(t, ok, "heimdall should be a map")
	assert.True(t, heimdall["enabled"].(bool))
	assert.NotNil(t, heimdall["stats"])

	// Verify bifrost section
	bifrost, ok := body["bifrost"].(map[string]interface{})
	require.True(t, ok, "bifrost should be a map")
	assert.NotNil(t, bifrost["enabled"])
}

func TestHandler_Status_MethodNotAllowed(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	req := httptest.NewRequest(http.MethodPost, "/api/bifrost/status", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}

func TestHandler_ChatCompletions(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	chatReq := ChatRequest{
		Model: "test-model",
		Messages: []ChatMessage{
			{Role: "user", Content: "Hello, check system health"},
		},
		Stream: false,
	}
	body, _ := json.Marshal(chatReq)

	req := httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var chatResp ChatResponse
	err := json.NewDecoder(resp.Body).Decode(&chatResp)
	require.NoError(t, err)

	assert.NotEmpty(t, chatResp.ID)
	assert.Equal(t, "test-model", chatResp.Model)
	require.Len(t, chatResp.Choices, 1)
	assert.Equal(t, "assistant", chatResp.Choices[0].Message.Role)
	assert.Contains(t, chatResp.Choices[0].Message.Content, "health")
}

func TestHandler_ChatCompletions_MethodNotAllowed(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	req := httptest.NewRequest(http.MethodGet, "/api/bifrost/chat/completions", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}

func TestHandler_ChatCompletions_InvalidBody(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	req := httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", strings.NewReader("invalid json"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestHandler_ChatCompletions_DefaultModel(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	// Don't specify model - should use config default
	chatReq := ChatRequest{
		Messages: []ChatMessage{
			{Role: "user", Content: "Hello"},
		},
	}
	body, _ := json.Marshal(chatReq)

	req := httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var chatResp ChatResponse
	err := json.NewDecoder(resp.Body).Decode(&chatResp)
	require.NoError(t, err)

	// Should use default model from config
	assert.Equal(t, "test-model", chatResp.Model)
}

func TestHandler_ChatCompletions_IgnoresRequestedModel(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	chatReq := ChatRequest{
		Model: "gpt-4o",
		Messages: []ChatMessage{
			{Role: "user", Content: "Hello"},
		},
	}
	body, _ := json.Marshal(chatReq)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var chatResp ChatResponse
	err := json.NewDecoder(resp.Body).Decode(&chatResp)
	require.NoError(t, err)
	assert.Equal(t, "test-model", chatResp.Model)
}

func TestHandler_ModelsEndpoint(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var payload map[string]interface{}
	err := json.NewDecoder(resp.Body).Decode(&payload)
	require.NoError(t, err)
	assert.Equal(t, "list", payload["object"])
	data, ok := payload["data"].([]interface{})
	require.True(t, ok)
	require.Len(t, data, 1)
	modelInfo, ok := data[0].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "test-model", modelInfo["id"])
}

func TestHandler_ChatCompletions_Streaming(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	chatReq := ChatRequest{
		Messages: []ChatMessage{
			{Role: "user", Content: "What can you help me with?"},
		},
		Stream: true,
	}
	body, _ := json.Marshal(chatReq)

	req := httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	// Read SSE events
	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	bodyStr := string(bodyBytes)

	// Should have SSE data events
	assert.Contains(t, bodyStr, "data:")
	// Should end with [DONE]
	assert.Contains(t, bodyStr, "[DONE]")
}

func TestHandler_NotFound(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	req := httptest.NewRequest(http.MethodGet, "/api/bifrost/unknown", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestHandler_ChatCompletions_SystemMessage(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	chatReq := ChatRequest{
		Messages: []ChatMessage{
			{Role: "system", Content: "You are Heimdall, the guardian of NornicDB."},
			{Role: "user", Content: "Who are you?"},
		},
	}
	body, _ := json.Marshal(chatReq)

	req := httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var chatResp ChatResponse
	err := json.NewDecoder(resp.Body).Decode(&chatResp)
	require.NoError(t, err)

	// Should have a response that mentions Heimdall
	assert.Contains(t, chatResp.Choices[0].Message.Content, "Heimdall")
}

func TestHandler_ChatCompletions_CustomParams(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	chatReq := ChatRequest{
		Messages: []ChatMessage{
			{Role: "user", Content: "Hello"},
		},
		MaxTokens:   256,
		Temperature: 0.8,
		TopP:        0.95,
	}
	body, _ := json.Marshal(chatReq)

	req := httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestHandler_ChatCompletions_MultiTurn(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	// Multi-turn conversation
	chatReq := ChatRequest{
		Messages: []ChatMessage{
			{Role: "system", Content: "You are a helpful assistant."},
			{Role: "user", Content: "What's the status?"},
			{Role: "assistant", Content: "System is healthy."},
			{Role: "user", Content: "Show me the detailed metrics and stats"},
		},
	}
	body, _ := json.Marshal(chatReq)

	req := httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var chatResp ChatResponse
	err := json.NewDecoder(resp.Body).Decode(&chatResp)
	require.NoError(t, err)

	// Should have a response (mock responds based on last user message)
	assert.NotEmpty(t, chatResp.Choices[0].Message.Content)
}

// Test SSE format
func TestHandler_SSEFormat(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	// Custom stream func for predictable output
	mockGen.streamFunc = func(ctx context.Context, prompt string, params GenerateParams, callback func(string) error) error {
		tokens := []string{"Hello", " ", "world", "!"}
		for _, token := range tokens {
			if err := callback(token); err != nil {
				return err
			}
		}
		return nil
	}

	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	chatReq := ChatRequest{
		Messages: []ChatMessage{{Role: "user", Content: "Hi"}},
		Stream:   true,
	}
	body, _ := json.Marshal(chatReq)

	req := httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", bytes.NewReader(body))
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	bodyBytes, _ := io.ReadAll(resp.Body)
	bodyStr := string(bodyBytes)

	// Verify SSE format
	assert.Contains(t, bodyStr, "data: {")
	assert.Contains(t, bodyStr, `"delta"`)
	assert.Contains(t, bodyStr, "data: [DONE]")
}

// Benchmark tests
func BenchmarkHandler_ChatCompletions(b *testing.B) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	chatReq := ChatRequest{
		Messages: []ChatMessage{
			{Role: "user", Content: "Hello"},
		},
	}
	body, _ := json.Marshal(chatReq)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", bytes.NewReader(body))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
	}
}

func BenchmarkHandler_Status(b *testing.B) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, "/api/bifrost/status", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
	}
}

// =============================================================================
// Bifrost Wiring Tests - Verify Handler <-> Bifrost <-> Heimdall connections
// =============================================================================

func TestHandler_BifrostBridge_Creation(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)

	// Config with Heimdall enabled - Bifrost should auto-enable
	cfg := manager.config
	cfg.Enabled = true
	cfg.BifrostEnabled = true

	handler := testHandler(manager, cfg)
	require.NotNil(t, handler)

	// Bifrost() should return a real Bifrost, not NoOp
	bridge := handler.Bifrost()
	require.NotNil(t, bridge)

	// Should not be connected initially
	assert.False(t, bridge.IsConnected())
	assert.Equal(t, 0, bridge.ConnectionCount())
}

func TestHandler_BifrostBridge_NoOp_WhenDisabled(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)

	// Config with Bifrost explicitly disabled
	cfg := manager.config
	cfg.Enabled = true
	cfg.BifrostEnabled = false

	handler := testHandler(manager, cfg)
	require.NotNil(t, handler)

	// Bifrost() should return NoOpBifrost
	bridge := handler.Bifrost()
	require.NotNil(t, bridge)

	// NoOp should always report not connected
	assert.False(t, bridge.IsConnected())
	assert.Equal(t, 0, bridge.ConnectionCount())

	// All methods should be no-ops (no error, no effect)
	assert.NoError(t, bridge.SendMessage("test"))
	assert.NoError(t, bridge.SendNotification("info", "title", "msg"))
	assert.NoError(t, bridge.Broadcast("test"))
}

func TestHandler_Events_Endpoint(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)

	cfg := manager.config
	cfg.Enabled = true
	cfg.BifrostEnabled = true

	handler := testHandler(manager, cfg)

	// Create a context with cancel for the SSE connection
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/api/bifrost/events", nil).WithContext(ctx)
	w := httptest.NewRecorder()

	// Run handler in goroutine since it blocks waiting for context
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(w, req)
		close(done)
	}()

	// Give it time to register and send initial message
	// Cancel immediately to unblock
	cancel()
	<-done

	resp := w.Result()
	defer resp.Body.Close()

	// Should have SSE headers
	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))
	assert.Equal(t, "no-cache", resp.Header.Get("Cache-Control"))

	// Should have initial connection message
	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "data:")
	assert.Contains(t, string(body), "connected")
}

func TestHandler_Events_Disabled(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)

	cfg := manager.config
	cfg.Enabled = true
	cfg.BifrostEnabled = false // Explicitly disabled

	handler := testHandler(manager, cfg)

	req := httptest.NewRequest(http.MethodGet, "/api/bifrost/events", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	// Should return error when Bifrost is disabled
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func TestHandler_Events_MethodNotAllowed(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)

	cfg := manager.config
	cfg.Enabled = true
	cfg.BifrostEnabled = true

	handler := testHandler(manager, cfg)

	// POST should not be allowed for events endpoint
	req := httptest.NewRequest(http.MethodPost, "/api/bifrost/events", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}

func TestHandler_Status_IncludesBifrostStats(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)

	cfg := manager.config
	cfg.Enabled = true
	cfg.BifrostEnabled = true

	handler := testHandler(manager, cfg)

	req := httptest.NewRequest(http.MethodGet, "/api/bifrost/status", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	var body map[string]interface{}
	err := json.NewDecoder(resp.Body).Decode(&body)
	require.NoError(t, err)

	// Verify bifrost section exists and has stats
	bifrost, ok := body["bifrost"].(map[string]interface{})
	require.True(t, ok, "bifrost section should exist")

	// Should show enabled and connection count
	assert.True(t, bifrost["enabled"].(bool), "Bifrost should be enabled")
	assert.Equal(t, float64(0), bifrost["connection_count"], "Should have 0 connections initially")
}

func TestBifrost_AutoEnabled_WithHeimdall(t *testing.T) {
	// Verify that when we use ConfigFromFeatureFlags with Heimdall enabled,
	// Bifrost is automatically enabled
	flags := &MockFeatureFlags{
		enabled: true,
	}

	cfg := ConfigFromFeatureFlags(flags)

	assert.True(t, cfg.Enabled, "Heimdall should be enabled")
	assert.True(t, cfg.BifrostEnabled, "Bifrost should auto-enable with Heimdall")
}

func TestBifrost_Disabled_WithHeimdallDisabled(t *testing.T) {
	// Verify that when Heimdall is disabled, Bifrost is also disabled
	flags := &MockFeatureFlags{
		enabled: false,
	}

	cfg := ConfigFromFeatureFlags(flags)

	assert.False(t, cfg.Enabled, "Heimdall should be disabled")
	assert.False(t, cfg.BifrostEnabled, "Bifrost should be disabled when Heimdall is disabled")
}

func TestHandler_Integration_BifrostToPlugin(t *testing.T) {
	// Test that the BifrostBridge can be accessed from Handler and passed to plugins
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)

	cfg := manager.config
	cfg.Enabled = true
	cfg.BifrostEnabled = true

	handler := testHandler(manager, cfg)
	require.NotNil(t, handler)

	// Get the Bifrost bridge
	bridge := handler.Bifrost()
	require.NotNil(t, bridge)

	// Verify the bridge can be used in SubsystemContext
	ctx := SubsystemContext{
		Config:  cfg,
		Bifrost: bridge,
	}

	// Plugin should be able to use the bridge
	assert.NotNil(t, ctx.Bifrost)
	assert.False(t, ctx.Bifrost.IsConnected())

	// Verify methods don't panic with no connections
	assert.NoError(t, ctx.Bifrost.SendMessage("test"))
	assert.NoError(t, ctx.Bifrost.SendNotification("info", "Test", "Message"))
	assert.NoError(t, ctx.Bifrost.Broadcast("announcement"))
}

// =============================================================================
// OpenAI API Compatibility Tests
// =============================================================================

func TestHandler_OpenAI_Compatibility_NonStreaming(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	chatReq := ChatRequest{
		Model: "test-model",
		Messages: []ChatMessage{
			{Role: "user", Content: "Hello"},
		},
	}
	body, _ := json.Marshal(chatReq)

	req := httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", bytes.NewReader(body))
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var response ChatResponse
	err := json.NewDecoder(resp.Body).Decode(&response)
	require.NoError(t, err)

	// OpenAI required fields
	assert.NotEmpty(t, response.ID, "ID is required by OpenAI API")
	assert.Equal(t, "chat.completion", response.Object, "object must be 'chat.completion' for non-streaming")
	assert.NotEmpty(t, response.Model, "model is required by OpenAI API")
	assert.NotZero(t, response.Created, "created timestamp is required by OpenAI API")
	assert.NotEmpty(t, response.Choices, "choices array is required by OpenAI API")

	// Verify choice structure
	choice := response.Choices[0]
	assert.Equal(t, 0, choice.Index, "first choice should have index 0")
	assert.NotNil(t, choice.Message, "message is required for non-streaming")
	assert.Equal(t, "assistant", choice.Message.Role, "role must be 'assistant'")
	assert.NotEmpty(t, choice.Message.Content, "content should not be empty")
	assert.Equal(t, "stop", choice.FinishReason, "finish_reason should be 'stop'")
}

func TestHandler_OpenAI_Compatibility_Streaming(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	chatReq := ChatRequest{
		Model:  "test-model",
		Stream: true,
		Messages: []ChatMessage{
			{Role: "user", Content: "Hello"},
		},
	}
	body, _ := json.Marshal(chatReq)

	req := httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", bytes.NewReader(body))
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	bodyBytes, _ := io.ReadAll(resp.Body)
	bodyStr := string(bodyBytes)

	// Verify SSE format
	assert.Contains(t, bodyStr, "data: {", "Should have SSE data prefix")
	assert.Contains(t, bodyStr, `"object":"chat.completion.chunk"`, "object must be 'chat.completion.chunk' for streaming")
	assert.Contains(t, bodyStr, `"delta"`, "streaming responses use delta not message")
	assert.Contains(t, bodyStr, "data: [DONE]", "stream should end with [DONE]")
}

func TestHandler_OpenAI_RequestFormat(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	// Test with all optional fields
	chatReq := ChatRequest{
		Model:       "test-model",
		Messages:    []ChatMessage{{Role: "user", Content: "Hello"}},
		MaxTokens:   100,
		Temperature: 0.7,
		TopP:        0.9,
		Stream:      false,
	}
	body, _ := json.Marshal(chatReq)

	req := httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", bytes.NewReader(body))
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestHandler_OpenAI_MessageRoles(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	// Test all OpenAI message roles
	chatReq := ChatRequest{
		Model: "test-model",
		Messages: []ChatMessage{
			{Role: "system", Content: "You are Heimdall, the guardian of NornicDB."},
			{Role: "user", Content: "Check system health"},
			{Role: "assistant", Content: "System is healthy."},
			{Role: "user", Content: "What else can you do?"},
		},
	}
	body, _ := json.Marshal(chatReq)

	req := httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", bytes.NewReader(body))
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var response ChatResponse
	json.NewDecoder(resp.Body).Decode(&response)

	// Response should be from assistant
	assert.Equal(t, "assistant", response.Choices[0].Message.Role)
}

func TestHandler_OpenAI_EmptyModel_UsesDefault(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	// Request without model - should use default
	chatReq := ChatRequest{
		Messages: []ChatMessage{
			{Role: "user", Content: "Hello"},
		},
	}
	body, _ := json.Marshal(chatReq)

	req := httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", bytes.NewReader(body))
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var response ChatResponse
	json.NewDecoder(resp.Body).Decode(&response)

	// Should use default model from config
	assert.NotEmpty(t, response.Model)
}

// TestHandler_TryParseAction tests action parsing from SLM responses
func TestHandler_TryParseAction(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	// Register a test action
	testActionExecuted := false
	RegisterBuiltinAction(ActionFunc{
		Name:        "heimdall_test_parse_test",
		Description: "Test action for parsing",
		Category:    "test",
		Handler: func(ctx ActionContext) (*ActionResult, error) {
			testActionExecuted = true
			return &ActionResult{
				Success: true,
				Message: "Test action executed",
			}, nil
		},
	})
	defer func() {
		// Clean up
		m := GetSubsystemManager()
		m.mu.Lock()
		delete(m.actions, "heimdall_test_parse_test")
		m.mu.Unlock()
	}()

	tests := []struct {
		name      string
		response  string
		wantParse bool
	}{
		{
			name:      "valid action JSON",
			response:  `{"action": "heimdall_test_parse_test", "params": {}}`,
			wantParse: true,
		},
		{
			name:      "action with params",
			response:  `{"action": "heimdall_test_parse_test", "params": {"key": "value"}}`,
			wantParse: true,
		},
		{
			name:      "conversational response",
			response:  "Hello! How can I help you today?",
			wantParse: false,
		},
		{
			name:      "unregistered action",
			response:  `{"action": "heimdall_unknown_action", "params": {}}`,
			wantParse: false,
		},
		{
			name:      "invalid JSON",
			response:  `{"action": incomplete`,
			wantParse: false,
		},
		{
			name:      "empty response",
			response:  "",
			wantParse: false,
		},
		{
			name:      "json with extra text",
			response:  `{"action": "heimdall_test_parse_test"} extra text`,
			wantParse: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _ := handler.tryParseAction(tt.response)
			if tt.wantParse {
				assert.NotNil(t, result)
				assert.Equal(t, "heimdall_test_parse_test", result.Action)
			} else {
				assert.Nil(t, result)
			}
		})
	}
	_ = testActionExecuted // Mark as used to silence linter
}

// TestHandler_ActionExecution tests that actions are executed from chat
func TestHandler_ActionExecution(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	// Register a test action
	actionResult := &ActionResult{
		Success: true,
		Message: "Hello from test action!",
		Data: map[string]interface{}{
			"greeting": "Hello",
		},
	}
	RegisterBuiltinAction(ActionFunc{
		Name:        "heimdall_test_hello_action",
		Description: "Say hello - test action",
		Category:    "test",
		Handler: func(ctx ActionContext) (*ActionResult, error) {
			return actionResult, nil
		},
	})
	defer func() {
		// Clean up
		m := GetSubsystemManager()
		m.mu.Lock()
		delete(m.actions, "heimdall_test_hello_action")
		m.mu.Unlock()
	}()

	// Mock generator: first round returns action JSON; after seeing tool result, return final answer (agentic loop)
	mockGen.generateFunc = func(ctx context.Context, prompt string, params GenerateParams) (string, error) {
		if strings.Contains(prompt, "Tool result") {
			return "Hello from test action!", nil
		}
		return `{"action": "heimdall_test_hello_action", "params": {}}`, nil
	}

	chatReq := ChatRequest{
		Messages: []ChatMessage{
			{Role: "user", Content: "say hello"},
		},
	}
	body, _ := json.Marshal(chatReq)

	req := httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", bytes.NewReader(body))
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var response ChatResponse
	err := json.NewDecoder(resp.Body).Decode(&response)
	require.NoError(t, err)

	// Response should contain the action result message
	assert.Contains(t, response.Choices[0].Message.Content, "Hello from test action!")
}

func TestHandler_OpenAI_V1ChatCompletions_PassThroughUnknownActionAsToolCall(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	mockGen.generateFunc = func(ctx context.Context, prompt string, params GenerateParams) (string, error) {
		return `{"action":"mcp__ide__getDiagnostics","params":{"uri":"file:///tmp/test.go"}}`, nil
	}
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	chatReq := ChatRequest{
		Model: "gpt-4o",
		Messages: []ChatMessage{
			{Role: "user", Content: "check diagnostics"},
		},
	}
	body, _ := json.Marshal(chatReq)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var chatResp ChatResponse
	err := json.NewDecoder(resp.Body).Decode(&chatResp)
	require.NoError(t, err)
	require.Len(t, chatResp.Choices, 1)
	require.NotNil(t, chatResp.Choices[0].Message)
	assert.Equal(t, "tool_calls", chatResp.Choices[0].FinishReason)
	require.Len(t, chatResp.Choices[0].Message.ToolCalls, 1)
	assert.Equal(t, "function", chatResp.Choices[0].Message.ToolCalls[0].Type)
	assert.Equal(t, "mcp__ide__getDiagnostics", chatResp.Choices[0].Message.ToolCalls[0].Function.Name)
	assert.JSONEq(t, `{"uri":"file:///tmp/test.go"}`, chatResp.Choices[0].Message.ToolCalls[0].Function.Arguments)
	assert.Equal(t, "", chatResp.Choices[0].Message.Content)
}

func TestHandler_OpenAI_V1Streaming_PassThroughUnknownActionAsToolCall(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	mockGen.streamFunc = func(ctx context.Context, prompt string, params GenerateParams, callback func(token string) error) error {
		for _, token := range []string{"{", `"action":"mcp__repo__search",`, `"params":{"query":"heimdall"}`, "}"} {
			if err := callback(token); err != nil {
				return err
			}
		}
		return nil
	}
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	chatReq := ChatRequest{
		Model:  "gpt-4o",
		Stream: true,
		Messages: []ChatMessage{
			{Role: "user", Content: "search repo"},
		},
	}
	body, _ := json.Marshal(chatReq)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	bodyBytes, _ := io.ReadAll(resp.Body)
	bodyStr := string(bodyBytes)
	assert.Contains(t, bodyStr, `"finish_reason":"tool_calls"`)
	assert.Contains(t, bodyStr, `"tool_calls":[{`)
	assert.Contains(t, bodyStr, `"name":"mcp__repo__search"`)
	assert.Contains(t, bodyStr, `"arguments":"{\"query\":\"heimdall\"}"`)
	assert.Contains(t, bodyStr, `data: [DONE]`)
	assert.NotContains(t, bodyStr, `Sorry, I don't know how to perform the action`)
}

func TestHandler_ChatCompletionRequest_StoresExternalToolsInPromptContext(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	mockGen.generateFunc = func(ctx context.Context, prompt string, params GenerateParams) (string, error) {
		return "tool-aware answer", nil
	}
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	chatReq := ChatRequest{
		Model: "gpt-4o",
		Messages: []ChatMessage{
			{Role: "user", Content: "use the external repo tool"},
		},
		Tools: []ChatToolDefinition{
			{
				Type: "function",
				Function: ChatToolDefinitionFunction{
					Name:        "mcp__repo__search",
					Description: "Search the repository graph",
					Parameters:  json.RawMessage(`{"type":"object","properties":{"query":{"type":"string"}}}`),
				},
			},
		},
	}
	body, _ := json.Marshal(chatReq)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var chatResp ChatResponse
	err := json.NewDecoder(resp.Body).Decode(&chatResp)
	require.NoError(t, err)
	assert.Equal(t, "tool-aware answer\n", chatResp.Choices[0].Message.Content)
}

// =============================================================================
// trimAgenticResponse / extractNonJSONAnswer (prompt-based agentic loop helpers)
// =============================================================================

// trimAgenticResponse extracts the first JSON object from the response, or the first paragraph if no JSON.
// Used by tests that validate prompt-based agentic response trimming.
func trimAgenticResponse(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if idx := strings.Index(s, "{"); idx >= 0 {
		if obj := extractFirstJSONObject(s[idx:]); obj != "" {
			return obj
		}
	}
	// No JSON: return first paragraph (up to first \n\n)
	if i := strings.Index(s, "\n\n"); i >= 0 {
		return strings.TrimSpace(s[:i])
	}
	return s
}

// extractNonJSONAnswer returns the prose answer part (before or after the first JSON object), or "" if only JSON.
func extractNonJSONAnswer(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	idx := strings.Index(s, "{")
	if idx < 0 {
		return s
	}
	before := strings.TrimSpace(s[:idx])
	obj := extractFirstJSONObject(s[idx:])
	if obj == "" {
		return s
	}
	after := strings.TrimSpace(s[idx+len(obj):])
	if before != "" {
		return before
	}
	return after
}

func TestTrimAgenticResponse(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty", "", ""},
		{"only json", `{"action": "heimdall_watcher_status", "params": {}}`, `{"action": "heimdall_watcher_status", "params": {}}`},
		{"thinking then json", "Okay, let me think...\n\n{\"action\": \"heimdall_watcher_discover\", \"params\": {\"query\": \"pharmacy\"}}", `{"action": "heimdall_watcher_discover", "params": {"query": "pharmacy"}}`},
		{"direct answer one line", "Your orders are at Pharmacy A and Pharmacy B.", "Your orders are at Pharmacy A and Pharmacy B."},
		{"direct answer multi-line", "Here is the answer.\n\nMore detail.", "Here is the answer."},
		{"json with trailing junk", `{"action":"x","params":{}} [{ "data": "..." }]`, `{"action":"x","params":{}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := trimAgenticResponse(tt.input)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestSanitizeAssistantResponse(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "keeps plain assistant reply",
			input:    "Hello! How can I assist you today?",
			expected: "Hello! How can I assist you today?\n",
		},
		{
			name:     "removes im template replay",
			input:    "Hello! How can I assist you today?<|im_end|> <|im_start|>user what actions can you perform?<|im_end|>",
			expected: "Hello! How can I assist you today?\n",
		},
		{
			name:     "preserves heimdall inline notification tail",
			input:    "Hello!\n[Heimdall]: 🔄 Repository Graph: Searching repository knowledge graph...",
			expected: "Hello!\n[Heimdall]: 🔄 Repository Graph: Searching repository knowledge graph...\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeAssistantResponse(tt.input)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestLooksLikeActionEnvelopePrefix(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "complete action json",
			input:    `{"action": "heimdall_watcher_status", "params": {}}`,
			expected: true,
		},
		{
			name:     "partial action json",
			input:    `{"action": "heimdall_watcher_hello", "params": {}`,
			expected: true,
		},
		{
			name:     "prefixed action json",
			input:    "Okay\n\n{" + `"action": "heimdall_watcher_query", "params": {`,
			expected: true,
		},
		{
			name:     "plain assistant text",
			input:    "Hello! How can I help?",
			expected: false,
		},
		{
			name:     "non action json",
			input:    `{"message": "hello"}`,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, looksLikeActionEnvelopePrefix(tt.input))
		})
	}
}

func TestHandler_OpenAI_V1Streaming_SuppressesHeimdallNotificationChunks(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	chatReq := ChatRequest{
		Model:  "test-model",
		Stream: true,
		Messages: []ChatMessage{
			{Role: "user", Content: "Hello"},
		},
	}
	body, _ := json.Marshal(chatReq)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	bodyBytes, _ := io.ReadAll(resp.Body)
	bodyStr := string(bodyBytes)

	assert.NotContains(t, bodyStr, `[Heimdall]:`, "OpenAI-compatible stream should contain only assistant content chunks")
	assert.Contains(t, bodyStr, `"object":"chat.completion.chunk"`)
	assert.Contains(t, bodyStr, `data: [DONE]`)
}

func TestExtractNonJSONAnswer(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty", "", ""},
		{"json then answer", `{"action": "none", "params": {}}

No additional context provided to answer the question about pharmacies.`, "No additional context provided to answer the question about pharmacies."},
		{"answer before json", "Your orders are at CVS Store #4521 and CVS Mail Order.\n{\"action\": \"none\", \"params\": {}}", "Your orders are at CVS Store #4521 and CVS Mail Order."},
		{"only json", `{"action": "none", "params": {}}`, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractNonJSONAnswer(tt.input)
			assert.Equal(t, tt.expected, got)
		})
	}
}

// =============================================================================
// Lifecycle Hook Tests
// =============================================================================

func TestPromptContext_BuildFinalPrompt(t *testing.T) {
	tests := []struct {
		name     string
		ctx      *PromptContext
		contains []string
	}{
		{
			name: "basic_prompt",
			ctx: &PromptContext{
				ActionPrompt: "- status: Get status\n",
				UserMessage:  "What's up?",
			},
			contains: []string{
				"You are Heimdall",
				"AVAILABLE ACTIONS:",
				"status: Get status",
				"reply with exactly one line",
			},
		},
		{
			name: "with_additional_instructions",
			ctx: &PromptContext{
				ActionPrompt:           "- status: Get status\n",
				AdditionalInstructions: "Be concise and helpful.",
			},
			contains: []string{
				"ADDITIONAL CONTEXT:",
				"Be concise and helpful.",
			},
		},
		{
			name: "with_examples",
			ctx: &PromptContext{
				ActionPrompt: "- status: Get status\n",
				Examples: []PromptExample{
					{UserSays: "check status", ActionJSON: `{"action": "status"}`},
				},
			},
			contains: []string{
				"EXAMPLES:",
				`User: "check status"`,
				`{"action": "status"}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.ctx.BuildFinalPrompt()
			for _, expected := range tt.contains {
				assert.Contains(t, result, expected)
			}
		})
	}
}

func TestPromptContext_ActionPromptImmutable(t *testing.T) {
	// Verify ActionPrompt is always first in the final prompt
	ctx := &PromptContext{
		ActionPrompt:           "- test_action: Test\n",
		AdditionalInstructions: "Additional context here",
		Examples: []PromptExample{
			{UserSays: "test", ActionJSON: `{"action": "test"}`},
		},
	}

	result := ctx.BuildFinalPrompt()

	// Find positions
	actionPos := strings.Index(result, "AVAILABLE ACTIONS:")
	contextPos := strings.Index(result, "ADDITIONAL CONTEXT:")
	examplesPos := strings.Index(result, "EXAMPLES:")

	// ActionPrompt section should come first (after header)
	assert.Less(t, actionPos, contextPos, "ActionPrompt should come before AdditionalContext")
	assert.Less(t, actionPos, examplesPos, "ActionPrompt should come before Examples")
}

func TestDefaultExamples(t *testing.T) {
	// Examples are no longer defined in the handler; plugins add them via PrePrompt hooks.
	// Prompt context starts with nil examples.
	var examples []PromptExample
	assert.Len(t, examples, 0, "Handler does not define default examples; plugins supply them")
}

func TestRequestLifecycle_Structure(t *testing.T) {
	lifecycle := &requestLifecycle{
		promptCtx: &PromptContext{
			RequestID:    "test-123",
			ActionPrompt: "- test: Test action\n",
			PluginData:   map[string]interface{}{"key": "value"},
		},
		requestID: "test-123",
		database:  &mockDBRouter{},
		metrics:   &mockMetricsReader{},
	}

	assert.Equal(t, "test-123", lifecycle.requestID)
	assert.NotNil(t, lifecycle.promptCtx)
	assert.NotNil(t, lifecycle.database)
	assert.NotNil(t, lifecycle.metrics)
}

func TestPreExecuteContext_Fields(t *testing.T) {
	ctx := &PreExecuteContext{
		RequestID:   "req-456",
		Action:      "heimdall_watcher_status",
		Params:      map[string]interface{}{"param1": "value1"},
		RawResponse: `{"action": "heimdall_watcher_status"}`,
		PluginData:  map[string]interface{}{"from_preprompt": true},
		Database:    &mockDBRouter{},
		Metrics:     &mockMetricsReader{},
	}

	assert.Equal(t, "req-456", ctx.RequestID)
	assert.Equal(t, "heimdall_watcher_status", ctx.Action)
	assert.Equal(t, "value1", ctx.Params["param1"])
	assert.NotNil(t, ctx.Database)
	assert.NotNil(t, ctx.Metrics)
}

func TestPreExecuteResult_Continue(t *testing.T) {
	tests := []struct {
		name     string
		result   PreExecuteResult
		expected bool
	}{
		{
			name:     "default_continue",
			result:   PreExecuteResult{Continue: true},
			expected: true,
		},
		{
			name:     "abort_execution",
			result:   PreExecuteResult{Continue: false, AbortMessage: "Validation failed"},
			expected: false,
		},
		{
			name: "modified_params",
			result: PreExecuteResult{
				Continue:       true,
				ModifiedParams: map[string]interface{}{"new_param": "new_value"},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.result.Continue)
		})
	}
}

func TestPostExecuteContext_Duration(t *testing.T) {
	ctx := &PostExecuteContext{
		RequestID: "req-789",
		Action:    "heimdall_watcher_query",
		Params:    map[string]interface{}{"cypher": "MATCH (n) RETURN n"},
		Result: &ActionResult{
			Success: true,
			Message: "Query completed",
			Data:    map[string]interface{}{"count": 42},
		},
		Duration:   100 * 1000000, // 100ms
		PluginData: map[string]interface{}{},
	}

	assert.Equal(t, "req-789", ctx.RequestID)
	assert.Equal(t, "heimdall_watcher_query", ctx.Action)
	assert.True(t, ctx.Result.Success)
}

func TestHandler_LifecycleFlow_NonStreaming(t *testing.T) {
	// Register a test action
	actionExecuted := false
	RegisterBuiltinAction(ActionFunc{
		Name:        "heimdall_test_lifecycle",
		Description: "Test lifecycle action",
		Category:    "test",
		Handler: func(ctx ActionContext) (*ActionResult, error) {
			actionExecuted = true
			return &ActionResult{
				Success: true,
				Message: "Lifecycle test complete",
			}, nil
		},
	})
	defer func() {
		m := GetSubsystemManager()
		m.mu.Lock()
		delete(m.actions, "heimdall_test_lifecycle")
		m.mu.Unlock()
	}()

	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	// Mock generator returns action
	mockGen.generateFunc = func(ctx context.Context, prompt string, params GenerateParams) (string, error) {
		return `{"action": "heimdall_test_lifecycle", "params": {}}`, nil
	}

	chatReq := ChatRequest{
		Messages: []ChatMessage{
			{Role: "user", Content: "run lifecycle test"},
		},
		Stream: false,
	}
	body, _ := json.Marshal(chatReq)

	req := httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", bytes.NewReader(body))
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Result().StatusCode)
	assert.True(t, actionExecuted, "Action should have been executed")
}

func TestHandler_LifecycleFlow_Streaming(t *testing.T) {
	// Register a test action
	actionExecuted := false
	RegisterBuiltinAction(ActionFunc{
		Name:        "heimdall_test_stream_lifecycle",
		Description: "Test streaming lifecycle",
		Category:    "test",
		Handler: func(ctx ActionContext) (*ActionResult, error) {
			actionExecuted = true
			return &ActionResult{
				Success: true,
				Message: "Stream lifecycle complete",
			}, nil
		},
	})
	defer func() {
		m := GetSubsystemManager()
		m.mu.Lock()
		delete(m.actions, "heimdall_test_stream_lifecycle")
		m.mu.Unlock()
	}()

	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)

	// Mock generator streams action
	mockGen.streamFunc = func(ctx context.Context, prompt string, params GenerateParams, callback func(string) error) error {
		callback(`{"action": `)
		callback(`"heimdall_test_stream_lifecycle", `)
		callback(`"params": {}}`)
		return nil
	}

	chatReq := ChatRequest{
		Messages: []ChatMessage{
			{Role: "user", Content: "run stream test"},
		},
		Stream: true,
	}
	body, _ := json.Marshal(chatReq)

	req := httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", bytes.NewReader(body))
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Result().StatusCode)
	assert.True(t, actionExecuted, "Action should have been executed in streaming mode")
}
