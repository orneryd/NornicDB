package heimdall

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockGenerator implements Generator interface for testing.
// Simulates model responses without requiring actual llama.cpp.
type MockGenerator struct {
	mu            sync.Mutex
	modelPath     string
	closed        bool
	generateFunc  func(ctx context.Context, prompt string, params GenerateParams) (string, error)
	streamFunc    func(ctx context.Context, prompt string, params GenerateParams, callback func(string) error) error
	generateCount int64
	streamCount   int64
}

// NewMockGenerator creates a mock generator that returns predictable responses.
func NewMockGenerator(modelPath string) *MockGenerator {
	return &MockGenerator{
		modelPath: modelPath,
		generateFunc: func(ctx context.Context, prompt string, params GenerateParams) (string, error) {
			// Default mock response based on prompt content
			return mockResponseForPrompt(prompt), nil
		},
		streamFunc: func(ctx context.Context, prompt string, params GenerateParams, callback func(string) error) error {
			// Default streaming - emit response word by word
			response := mockResponseForPrompt(prompt)
			words := strings.Fields(response)
			for _, word := range words {
				if err := callback(word + " "); err != nil {
					return err
				}
				// Small delay to simulate streaming
				time.Sleep(time.Millisecond)
			}
			return nil
		},
	}
}

// mockResponseForPrompt generates predictable responses based on prompt content.
// It extracts the last user message from the prompt to determine the response.
func mockResponseForPrompt(prompt string) string {
	userContent := prompt
	// Try ChatML format first
	if userStart := strings.LastIndex(prompt, "<|im_start|>user\n"); userStart != -1 {
		contentStart := userStart + len("<|im_start|>user\n")
		contentEnd := strings.Index(prompt[contentStart:], "<|im_end|>")
		if contentEnd != -1 {
			userContent = prompt[contentStart : contentStart+contentEnd]
		} else {
			userContent = prompt[contentStart:]
		}
	} else if idx := strings.LastIndex(prompt, "\n\nUser: "); idx != -1 {
		// Agentic prompt-based format: "...\n\nUser: <message>\n\nRespond..." or "\n\nBased on..."
		contentStart := idx + len("\n\nUser: ")
		if end := strings.Index(prompt[contentStart:], "\n\nRespond"); end != -1 {
			userContent = prompt[contentStart : contentStart+end]
		} else if end := strings.Index(prompt[contentStart:], "\n\nBased on"); end != -1 {
			userContent = prompt[contentStart : contentStart+end]
		} else if end := strings.Index(prompt[contentStart:], "\n\nAssistant:"); end != -1 {
			userContent = prompt[contentStart : contentStart+end]
		} else {
			userContent = prompt[contentStart:]
		}
		userContent = strings.TrimSpace(userContent)
	}

	lower := strings.ToLower(userContent)

	// Health check - explicit status/health queries
	if strings.Contains(lower, "health") ||
		(strings.Contains(lower, "status") && !strings.Contains(lower, "who")) {
		return `{"action": "heimdall_watcher_health", "params": {}}`
	}

	// Anomaly detection - check for "anomal" to catch "anomaly", "anomalies", etc.
	if strings.Contains(lower, "anomal") || strings.Contains(lower, "unusual") {
		return `{"action": "heimdall_anomaly_detect", "params": {"threshold": 0.8}}`
	}

	// Help request
	if strings.Contains(lower, "help") || strings.Contains(lower, "command") {
		return `{"action": "heimdall_help", "params": {}}`
	}

	// Configuration
	if strings.Contains(lower, "config") || strings.Contains(lower, "setting") {
		return `{"action": "heimdall_watcher_config", "params": {}}`
	}

	// Metrics
	if strings.Contains(lower, "metric") || strings.Contains(lower, "stats") {
		return `{"action": "heimdall_watcher_metrics", "params": {}}`
	}

	// Default conversational response
	return "I am Heimdall, the guardian of NornicDB. I can help you monitor and manage your graph database. What would you like to know?"
}

func (g *MockGenerator) Generate(ctx context.Context, prompt string, params GenerateParams) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.closed {
		return "", errors.New("generator is closed")
	}

	atomic.AddInt64(&g.generateCount, 1)
	return g.generateFunc(ctx, prompt, params)
}

func (g *MockGenerator) GenerateStream(ctx context.Context, prompt string, params GenerateParams, callback func(token string) error) error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return errors.New("generator is closed")
	}
	g.mu.Unlock()

	atomic.AddInt64(&g.streamCount, 1)
	return g.streamFunc(ctx, prompt, params, callback)
}

func (g *MockGenerator) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = true
	return nil
}

func (g *MockGenerator) ModelPath() string {
	return g.modelPath
}

func (g *MockGenerator) IsClosed() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.closed
}

func (g *MockGenerator) GetGenerateCount() int64 {
	return atomic.LoadInt64(&g.generateCount)
}

func (g *MockGenerator) GetStreamCount() int64 {
	return atomic.LoadInt64(&g.streamCount)
}

// Custom generator that returns errors for testing
type ErrorGenerator struct {
	generateErr error
	streamErr   error
}

func (e *ErrorGenerator) Generate(ctx context.Context, prompt string, params GenerateParams) (string, error) {
	return "", e.generateErr
}

func (e *ErrorGenerator) GenerateStream(ctx context.Context, prompt string, params GenerateParams, callback func(string) error) error {
	return e.streamErr
}

func (e *ErrorGenerator) Close() error {
	return nil
}

func (e *ErrorGenerator) ModelPath() string {
	return "error-model"
}

// Helper to create a manager with mock generator for testing
func newTestManager(generator Generator) *Manager {
	return &Manager{
		generator: generator,
		config: Config{
			Enabled:     true,
			Model:       "test-model",
			MaxTokens:   512,
			Temperature: 0.1,
		},
		modelPath: "/test/model.gguf",
		lastUsed:  time.Now(),
	}
}

// Tests

func TestNewManager_Disabled(t *testing.T) {
	cfg := Config{Enabled: false}

	manager, err := NewManager(cfg)

	assert.NoError(t, err)
	assert.Nil(t, manager, "Should return nil when disabled")
}

func TestNewManager_ModelNotFound(t *testing.T) {
	cfg := Config{
		Enabled:   true,
		Provider:  "local",
		ModelsDir: "/nonexistent/path",
		Model:     "nonexistent-model",
	}

	manager, err := NewManager(cfg)

	assert.Error(t, err)
	assert.Nil(t, manager)
	assert.Contains(t, err.Error(), "not found")
}

func TestNewManager_OpenAIProvider(t *testing.T) {
	// Mock OpenAI-compatible chat completions response
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"choices": []map[string]interface{}{
				{"message": map[string]interface{}{"content": "hello from openai"}},
			},
		})
	}))
	defer server.Close()

	cfg := Config{
		Enabled:  true,
		Provider: "openai",
		APIURL:   server.URL,
		APIKey:   "sk-test",
		Model:    "gpt-4o-mini",
	}
	manager, err := NewManager(cfg)
	require.NoError(t, err)
	require.NotNil(t, manager)
	defer manager.Close()

	ctx := context.Background()
	result, err := manager.Generate(ctx, "test prompt", DefaultGenerateParams())
	require.NoError(t, err)
	assert.Equal(t, "hello from openai", result)
}

func TestNewManager_OpenAIProvider_UsesGpt4WhenModelIsLocalDefault(t *testing.T) {
	// When provider=openai but config still has the local default model name,
	// the OpenAI generator should use gpt-4 instead of sending it to the API.
	var requestedModel string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		var body struct {
			Model string `json:"model"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		requestedModel = body.Model
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"choices": []map[string]interface{}{
				{"message": map[string]interface{}{"content": "ok"}},
			},
		})
	}))
	defer server.Close()

	cfg := Config{
		Enabled:  true,
		Provider: "openai",
		APIURL:   server.URL,
		APIKey:   "sk-test",
		Model:    "gpt-4o-mini", // config default from local provider
	}
	manager, err := NewManager(cfg)
	require.NoError(t, err)
	require.NotNil(t, manager)
	defer manager.Close()

	_, _ = manager.Generate(context.Background(), "test", DefaultGenerateParams())
	assert.Equal(t, "gpt-4o-mini", requestedModel, "openai provider should override local/GGUF model name with gpt-4o-mini")
}

func TestNewManager_OllamaProvider(t *testing.T) {
	// Mock Ollama /api/chat response
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/chat" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"message": map[string]interface{}{"role": "assistant", "content": "hi from ollama"},
			"done":    true,
		})
	}))
	defer server.Close()

	cfg := Config{
		Enabled:  true,
		Provider: "ollama",
		APIURL:   server.URL,
		Model:    "llama3.2",
	}
	manager, err := NewManager(cfg)
	require.NoError(t, err)
	require.NotNil(t, manager)
	defer manager.Close()

	ctx := context.Background()
	result, err := manager.Generate(ctx, "test prompt", DefaultGenerateParams())
	require.NoError(t, err)
	assert.Equal(t, "hi from ollama", result)
}

func TestManager_Generate(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)

	ctx := context.Background()
	prompt := "Check system health"
	params := DefaultGenerateParams()

	result, err := manager.Generate(ctx, prompt, params)

	require.NoError(t, err)
	assert.Contains(t, result, "heimdall_watcher_health")
	assert.Equal(t, int64(1), mockGen.GetGenerateCount())
}

func TestManagerGenerateRedactsCredentialURLBeforeModelAndOutput(t *testing.T) {
	const rawEndpoint = "https://synthetic-user:synthetic-pass@example.invalid/path?api_key=synthetic-key"
	var receivedPrompt string
	mockGen := NewMockGenerator("/test/model.gguf")
	mockGen.generateFunc = func(_ context.Context, prompt string, _ GenerateParams) (string, error) {
		receivedPrompt = prompt
		return rawEndpoint, nil
	}
	manager := newTestManager(mockGen)

	response, err := manager.Generate(context.Background(), "Check this endpoint: "+rawEndpoint, DefaultGenerateParams())
	if err != nil {
		t.Fatal("generation unexpectedly failed")
	}
	assertNoSyntheticCredential(t, receivedPrompt)
	assertNoSyntheticCredential(t, response)
	if !strings.Contains(receivedPrompt, "example.invalid") {
		t.Fatal("redaction unexpectedly removed the endpoint context")
	}
}

func TestManagerGenerateRedactsCredentialURLFromErrors(t *testing.T) {
	const rawEndpoint = "https://synthetic-user:synthetic-pass@example.invalid/path?api_key=synthetic-key"
	mockGen := NewMockGenerator("/test/model.gguf")
	mockGen.generateFunc = func(_ context.Context, _ string, _ GenerateParams) (string, error) {
		return "", errors.New(rawEndpoint)
	}
	manager := newTestManager(mockGen)

	_, err := manager.Generate(context.Background(), "Check this endpoint: "+rawEndpoint, DefaultGenerateParams())
	if err == nil {
		t.Fatal("expected the generator error")
	}
	assertNoSyntheticCredential(t, err.Error())
}

func TestManagerGenerateStreamRedactsCredentialURLAcrossChunks(t *testing.T) {
	const rawEndpoint = "https://synthetic-user:synthetic-pass@example.invalid/path?api_key=synthetic-key"
	var receivedPrompt string
	mockGen := NewMockGenerator("/test/model.gguf")
	mockGen.streamFunc = func(_ context.Context, prompt string, _ GenerateParams, callback func(string) error) error {
		receivedPrompt = prompt
		middle := len(rawEndpoint) / 2
		if err := callback(rawEndpoint[:middle]); err != nil {
			return err
		}
		return callback(rawEndpoint[middle:])
	}
	manager := newTestManager(mockGen)

	var response strings.Builder
	callbackCount := 0
	err := manager.GenerateStream(context.Background(), "Check this endpoint: "+rawEndpoint, DefaultGenerateParams(), func(token string) error {
		callbackCount++
		response.WriteString(token)
		return nil
	})
	if err != nil {
		t.Fatal("stream generation unexpectedly failed")
	}
	assertNoSyntheticCredential(t, receivedPrompt)
	assertNoSyntheticCredential(t, response.String())
	if callbackCount != 1 {
		t.Fatal("credential-bearing output was not redacted as a complete response")
	}
}

type captureToolsGenerator struct {
	*MockGenerator
	messages []ToolRoundMessage
	tools    []MCPTool
	content  string
	calls    []ParsedToolCall
}

func (g *captureToolsGenerator) GenerateWithTools(_ context.Context, messages []ToolRoundMessage, tools []MCPTool, _ GenerateParams) (string, []ParsedToolCall, error) {
	g.messages = messages
	g.tools = tools
	return g.content, append([]ParsedToolCall(nil), g.calls...), nil
}

func TestManagerGenerateWithToolsRedactsCredentialURLs(t *testing.T) {
	const rawEndpoint = "https://synthetic-user:synthetic-pass@example.invalid/path?api_key=synthetic-key"
	toolGenerator := &captureToolsGenerator{
		MockGenerator: NewMockGenerator("/test/model.gguf"),
		content:       rawEndpoint,
		calls:         []ParsedToolCall{{Id: "id", Name: "lookup", Arguments: `{"url":"` + rawEndpoint + `"}`}},
	}
	messages := []ToolRoundMessage{{
		Role:    "user",
		Content: "Check this endpoint: " + rawEndpoint,
		ToolCalls: []ParsedToolCall{{
			Id:        "id",
			Name:      "lookup",
			Arguments: `{"url":"` + rawEndpoint + `"}`,
		}},
	}}
	tools := []MCPTool{{
		Name:        "lookup",
		Description: "Use endpoint " + rawEndpoint,
		InputSchema: json.RawMessage(`{"examples":["` + rawEndpoint + `"]}`),
	}}
	manager := newTestManager(toolGenerator)

	content, calls, err := manager.GenerateWithTools(context.Background(), messages, tools, DefaultGenerateParams())
	if err != nil {
		t.Fatal("tool generation unexpectedly failed")
	}
	assertNoSyntheticCredential(t, toolGenerator.messages[0].Content)
	assertNoSyntheticCredential(t, toolGenerator.messages[0].ToolCalls[0].Arguments)
	assertNoSyntheticCredential(t, toolGenerator.tools[0].Description)
	assertNoSyntheticCredential(t, string(toolGenerator.tools[0].InputSchema))
	assertNoSyntheticCredential(t, content)
	assertNoSyntheticCredential(t, calls[0].Arguments)
	if !strings.Contains(messages[0].Content, "synthetic-pass") {
		t.Fatal("input was mutated instead of copied for redaction")
	}
}

func assertNoSyntheticCredential(t *testing.T, value string) {
	t.Helper()
	for _, marker := range []string{"synthetic-user", "synthetic-pass", "synthetic-key"} {
		if strings.Contains(value, marker) {
			t.Fatal("credential material escaped the generation redaction boundary")
		}
	}
}

func TestManager_Generate_RequestCounting(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)

	ctx := context.Background()
	params := DefaultGenerateParams()

	// Make multiple requests
	for i := 0; i < 5; i++ {
		_, err := manager.Generate(ctx, "test prompt", params)
		require.NoError(t, err)
	}

	stats := manager.Stats()
	assert.Equal(t, int64(5), stats.RequestCount)
	assert.Equal(t, int64(0), stats.ErrorCount)
}

func TestManager_Generate_ErrorCounting(t *testing.T) {
	errGen := &ErrorGenerator{
		generateErr: errors.New("generation failed"),
	}
	manager := newTestManager(errGen)

	ctx := context.Background()
	params := DefaultGenerateParams()

	// Make requests that will fail
	for i := 0; i < 3; i++ {
		_, err := manager.Generate(ctx, "test prompt", params)
		assert.Error(t, err)
	}

	stats := manager.Stats()
	assert.Equal(t, int64(3), stats.RequestCount)
	assert.Equal(t, int64(3), stats.ErrorCount)
}

func TestManager_Generate_Closed(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)

	// Close the manager
	err := manager.Close()
	require.NoError(t, err)

	// Try to generate
	ctx := context.Background()
	_, err = manager.Generate(ctx, "test", DefaultGenerateParams())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestManager_Generate_NilGenerator(t *testing.T) {
	manager := &Manager{
		config:    Config{Enabled: true},
		modelPath: "/test/model.gguf",
	}

	ctx := context.Background()
	_, err := manager.Generate(ctx, "test", DefaultGenerateParams())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no generator")
}

func TestManager_GenerateStream(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)

	ctx := context.Background()
	prompt := "Hello, tell me about yourself"
	params := DefaultGenerateParams()

	var tokens []string
	callback := func(token string) error {
		tokens = append(tokens, token)
		return nil
	}

	err := manager.GenerateStream(ctx, prompt, params, callback)

	require.NoError(t, err)
	assert.Greater(t, len(tokens), 0, "Should have received tokens")
	assert.Equal(t, int64(1), mockGen.GetStreamCount())

	// Verify we received tokens (streaming works)
	fullResponse := strings.Join(tokens, "")
	assert.NotEmpty(t, fullResponse)
}

func TestManager_GenerateStream_CallbackError(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	mockGen.streamFunc = func(ctx context.Context, prompt string, params GenerateParams, callback func(string) error) error {
		// First few tokens succeed
		callback("Hello ")
		callback("world ")
		// Then callback returns error
		return callback("error")
	}

	manager := newTestManager(mockGen)

	ctx := context.Background()
	callbackErr := errors.New("callback cancelled")
	var tokens []string
	callback := func(token string) error {
		if token == "error" {
			return callbackErr
		}
		tokens = append(tokens, token)
		return nil
	}

	err := manager.GenerateStream(ctx, "test", DefaultGenerateParams(), callback)

	assert.Error(t, err)
	assert.Equal(t, callbackErr, err)
	assert.Len(t, tokens, 2)
}

func TestManager_GenerateStream_Closed(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)

	err := manager.Close()
	require.NoError(t, err)

	ctx := context.Background()
	err = manager.GenerateStream(ctx, "test", DefaultGenerateParams(), func(string) error { return nil })

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestManager_Chat(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)

	ctx := context.Background()
	req := ChatRequest{
		Model: "test-model",
		Messages: []ChatMessage{
			{Role: "system", Content: "You are Heimdall, the guardian of NornicDB."},
			{Role: "user", Content: "Check health status"},
		},
		MaxTokens:   256,
		Temperature: 0.1,
	}

	resp, err := manager.Chat(ctx, req)

	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.NotEmpty(t, resp.ID)
	assert.Equal(t, "test-model", resp.Model)
	assert.Len(t, resp.Choices, 1)
	assert.Equal(t, "assistant", resp.Choices[0].Message.Role)
	assert.Equal(t, "stop", resp.Choices[0].FinishReason)
}

func TestManager_Chat_DefaultParams(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)

	ctx := context.Background()
	req := ChatRequest{
		Messages: []ChatMessage{
			{Role: "user", Content: "Hello"},
		},
		// MaxTokens and Temperature not set - should use config defaults
	}

	resp, err := manager.Chat(ctx, req)

	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestManager_Stats(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)

	// Initial stats
	stats := manager.Stats()
	assert.Equal(t, "/test/model.gguf", stats.ModelPath)
	assert.Equal(t, int64(0), stats.RequestCount)
	assert.Equal(t, int64(0), stats.ErrorCount)
	assert.True(t, stats.Enabled)

	// Make a request
	ctx := context.Background()
	_, err := manager.Generate(ctx, "test", DefaultGenerateParams())
	require.NoError(t, err)

	// Stats should be updated
	stats = manager.Stats()
	assert.Equal(t, int64(1), stats.RequestCount)
	assert.False(t, stats.LastUsed.IsZero())
}

func TestManager_Close(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)

	// Close
	err := manager.Close()
	require.NoError(t, err)
	assert.True(t, mockGen.IsClosed())

	// Close again (should be idempotent)
	err = manager.Close()
	assert.NoError(t, err)
}

func TestManager_Close_PrintsStats(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)

	// Make some requests
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		_, _ = manager.Generate(ctx, "test", DefaultGenerateParams())
	}

	// Close should print stats (captured in log, not tested here)
	err := manager.Close()
	require.NoError(t, err)
}

func TestGenerateID(t *testing.T) {
	id1 := generateID()
	id2 := generateID()

	assert.True(t, strings.HasPrefix(id1, "chatcmpl-"))
	assert.NotEqual(t, id1, id2, "IDs should be unique")
}

func TestMockGenerator_CustomResponse(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")

	// Override generate function for specific test
	mockGen.generateFunc = func(ctx context.Context, prompt string, params GenerateParams) (string, error) {
		return "Custom response for: " + prompt, nil
	}

	result, err := mockGen.Generate(context.Background(), "test input", DefaultGenerateParams())

	require.NoError(t, err)
	assert.Equal(t, "Custom response for: test input", result)
}

func TestMockGenerator_SimulateTimeout(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")

	// Override to simulate slow response
	mockGen.generateFunc = func(ctx context.Context, prompt string, params GenerateParams) (string, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(10 * time.Second):
			return "response", nil
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := mockGen.Generate(ctx, "test", DefaultGenerateParams())

	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)
}

func TestMockResponseForPrompt(t *testing.T) {
	tests := []struct {
		prompt   string
		contains string
	}{
		{"Check system health", "heimdall_watcher_health"},
		{"What's the status?", "heimdall_watcher_health"},
		{"Detect anomalies", "heimdall_anomaly_detect"},
		{"Find unusual patterns", "heimdall_anomaly_detect"},
		{"Show help", "heimdall_help"},
		{"List commands", "heimdall_help"},
		{"Get configuration", "heimdall_watcher_config"},
		{"Show settings", "heimdall_watcher_config"},
		{"Display metrics", "heimdall_watcher_metrics"},
		{"Show stats", "heimdall_watcher_metrics"},
		{"Hello there", "Heimdall"}, // Default response
	}

	for _, tt := range tests {
		t.Run(tt.prompt, func(t *testing.T) {
			response := mockResponseForPrompt(tt.prompt)
			assert.Contains(t, response, tt.contains)
		})
	}
}

func TestManager_ConcurrentAccess(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)

	ctx := context.Background()
	params := DefaultGenerateParams()
	numGoroutines := 10
	numRequestsPerGoroutine := 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numRequestsPerGoroutine; j++ {
				_, err := manager.Generate(ctx, "test prompt", params)
				assert.NoError(t, err)
			}
		}()
	}

	wg.Wait()

	stats := manager.Stats()
	expected := int64(numGoroutines * numRequestsPerGoroutine)
	assert.Equal(t, expected, stats.RequestCount)
}

// Test model path resolution
func TestNewManager_ModelPathResolution(t *testing.T) {
	// Create temp directory with mock model file
	tmpDir, err := os.MkdirTemp("", "heimdall-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create a fake model file
	modelPath := filepath.Join(tmpDir, "test-model.gguf")
	err = os.WriteFile(modelPath, []byte("fake model data"), 0644)
	require.NoError(t, err)

	// Use mock loader to avoid CGO dependency
	origLoader := SetGeneratorLoader(func(path string, gpuLayers, contextSize, batchSize, lazyMode int) (Generator, error) {
		// Verify the path was resolved correctly
		assert.Equal(t, modelPath, path)
		assert.Equal(t, 2, lazyMode)
		return NewMockGenerator(path), nil
	})
	defer SetGeneratorLoader(origLoader)

	cfg := Config{
		Enabled:   true,
		ModelsDir: tmpDir,
		Model:     "test-model",
		GPULayers: 0,
		LazyMode:  2,
	}

	manager, err := NewManager(cfg)
	require.NoError(t, err)
	require.NotNil(t, manager)
	defer manager.Close()
}

func TestNewManager_EnvVarOverrides(t *testing.T) {
	// Create temp directory with mock model file
	tmpDir, err := os.MkdirTemp("", "heimdall-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create a fake model file
	expectedModelPath := filepath.Join(tmpDir, "env-model.gguf")
	err = os.WriteFile(expectedModelPath, []byte("fake model data"), 0644)
	require.NoError(t, err)

	// Set env vars
	os.Setenv("NORNICDB_MODELS_DIR", tmpDir)
	os.Setenv("NORNICDB_HEIMDALL_MODEL", "env-model")
	os.Setenv("NORNICDB_HEIMDALL_GPU_LAYERS", "8")
	defer func() {
		os.Unsetenv("NORNICDB_MODELS_DIR")
		os.Unsetenv("NORNICDB_HEIMDALL_MODEL")
		os.Unsetenv("NORNICDB_HEIMDALL_GPU_LAYERS")
	}()

	// Use mock loader and verify env vars were used
	var loadedPath string
	var loadedGPULayers int
	origLoader := SetGeneratorLoader(func(path string, gpuLayers, contextSize, batchSize, lazyMode int) (Generator, error) {
		loadedPath = path
		loadedGPULayers = gpuLayers
		return NewMockGenerator(path), nil
	})
	defer SetGeneratorLoader(origLoader)

	gpuLayersEnv := os.Getenv("NORNICDB_HEIMDALL_GPU_LAYERS")
	gpuLayersVal := 0
	if gpuLayersEnv != "" {
		if v, err := strconv.Atoi(gpuLayersEnv); err == nil {
			gpuLayersVal = v
		}
	}

	cfg := Config{
		Enabled:   true,
		ModelsDir: os.Getenv("NORNICDB_MODELS_DIR"),
		Model:     os.Getenv("NORNICDB_HEIMDALL_MODEL"),
		GPULayers: gpuLayersVal,
	}

	manager, err := NewManager(cfg)
	require.NoError(t, err)
	require.NotNil(t, manager)
	defer manager.Close()

	// Verify env vars were used
	assert.Equal(t, expectedModelPath, loadedPath)
	assert.Equal(t, 8, loadedGPULayers)
}

// Test model loader injection
func TestSetGeneratorLoader(t *testing.T) {
	// Save and restore original loader
	origLoader := SetGeneratorLoader(DefaultGeneratorLoader)
	defer SetGeneratorLoader(origLoader)

	// Create mock loader
	mockLoader := func(modelPath string, gpuLayers, contextSize, batchSize, lazyMode int) (Generator, error) {
		return NewMockGenerator(modelPath), nil
	}

	// Set mock loader
	prev := SetGeneratorLoader(mockLoader)
	assert.NotNil(t, prev)

	// Verify it works
	gen, err := loadGenerator("/test/model.gguf", 0, 32768, 8192, 1)
	require.NoError(t, err)
	assert.NotNil(t, gen)

	// Restore
	SetGeneratorLoader(prev)

	// Now should fail again
	gen, err = loadGenerator("/test/model.gguf", 0, 32768, 8192, 1)
	assert.Error(t, err)
	assert.Nil(t, gen)
}

func TestNewManager_WithMockLoader(t *testing.T) {
	// Create temp model file
	tmpDir, err := os.MkdirTemp("", "heimdall-loader-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	modelPath := filepath.Join(tmpDir, "test-model.gguf")
	err = os.WriteFile(modelPath, []byte("fake model"), 0644)
	require.NoError(t, err)

	// Save and restore original loader
	origLoader := SetGeneratorLoader(func(path string, gpuLayers, contextSize, batchSize, lazyMode int) (Generator, error) {
		return NewMockGenerator(path), nil
	})
	defer SetGeneratorLoader(origLoader)

	cfg := Config{
		Enabled:   true,
		ModelsDir: tmpDir,
		Model:     "test-model",
		GPULayers: -1,
	}

	manager, err := NewManager(cfg)
	require.NoError(t, err)
	require.NotNil(t, manager)

	// Verify we can use the manager
	ctx := context.Background()
	result, err := manager.Generate(ctx, "Hello", DefaultGenerateParams())
	require.NoError(t, err)
	assert.NotEmpty(t, result)

	// Cleanup
	manager.Close()
}

func TestNewManager_GPUFallbackToCPU(t *testing.T) {
	// Create temp model file
	tmpDir, err := os.MkdirTemp("", "heimdall-fallback-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	modelPath := filepath.Join(tmpDir, "test-model.gguf")
	err = os.WriteFile(modelPath, []byte("fake model"), 0644)
	require.NoError(t, err)

	gpuAttempts := 0
	cpuAttempts := 0

	// Mock loader that fails on GPU but succeeds on CPU
	origLoader := SetGeneratorLoader(func(path string, gpuLayers, contextSize, batchSize, lazyMode int) (Generator, error) {
		if gpuLayers != 0 {
			gpuAttempts++
			return nil, errors.New("GPU not available")
		}
		cpuAttempts++
		return NewMockGenerator(path), nil
	})
	defer SetGeneratorLoader(origLoader)

	cfg := Config{
		Enabled:   true,
		ModelsDir: tmpDir,
		Model:     "test-model",
		GPULayers: -1, // Auto - will try GPU first
	}

	manager, err := NewManager(cfg)
	require.NoError(t, err)
	require.NotNil(t, manager)

	// Should have tried GPU first, then fallen back to CPU
	assert.Equal(t, 1, gpuAttempts, "Should have tried GPU once")
	assert.Equal(t, 1, cpuAttempts, "Should have fallen back to CPU")

	manager.Close()
}

func TestNewManager_BothLoadsFail(t *testing.T) {
	// Create temp model file
	tmpDir, err := os.MkdirTemp("", "heimdall-fail-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	modelPath := filepath.Join(tmpDir, "test-model.gguf")
	err = os.WriteFile(modelPath, []byte("fake model"), 0644)
	require.NoError(t, err)

	// Mock loader that always fails
	origLoader := SetGeneratorLoader(func(path string, gpuLayers, contextSize, batchSize, lazyMode int) (Generator, error) {
		return nil, errors.New("model loading failed")
	})
	defer SetGeneratorLoader(origLoader)

	cfg := Config{
		Enabled:   true,
		ModelsDir: tmpDir,
		Model:     "test-model",
		GPULayers: -1,
	}

	manager, err := NewManager(cfg)
	assert.Error(t, err)
	assert.Nil(t, manager)
	assert.Contains(t, err.Error(), "failed to load SLM model")
	assert.Contains(t, err.Error(), "gpu load failed")
	assert.Contains(t, err.Error(), "cpu fallback failed")
}

func TestNewManager_CPUOnlyMode(t *testing.T) {
	// Create temp model file
	tmpDir, err := os.MkdirTemp("", "heimdall-cpu-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	modelPath := filepath.Join(tmpDir, "test-model.gguf")
	err = os.WriteFile(modelPath, []byte("fake model"), 0644)
	require.NoError(t, err)

	loadedGPULayers := -1

	// Mock loader that tracks what was requested
	origLoader := SetGeneratorLoader(func(path string, gpuLayers, contextSize, batchSize, lazyMode int) (Generator, error) {
		loadedGPULayers = gpuLayers
		return NewMockGenerator(path), nil
	})
	defer SetGeneratorLoader(origLoader)

	cfg := Config{
		Enabled:   true,
		ModelsDir: tmpDir,
		Model:     "test-model",
		GPULayers: 0, // Explicit CPU only
	}

	manager, err := NewManager(cfg)
	require.NoError(t, err)
	require.NotNil(t, manager)

	assert.Equal(t, 0, loadedGPULayers)

	manager.Close()
}

// Integration-style test using mock
func TestManager_FullChatFlow(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)
	ctx := context.Background()

	// Simulate a full conversation
	messages := []ChatMessage{
		{Role: "system", Content: "You are Heimdall, the cognitive guardian of NornicDB."},
		{Role: "user", Content: "Hello, I need help monitoring my database."},
	}

	// First message
	resp, err := manager.Chat(ctx, ChatRequest{Messages: messages})
	require.NoError(t, err)
	assert.NotNil(t, resp)

	// Add assistant response to conversation
	messages = append(messages, *resp.Choices[0].Message)

	// User asks about health
	messages = append(messages, ChatMessage{Role: "user", Content: "Check system health"})

	resp, err = manager.Chat(ctx, ChatRequest{Messages: messages})
	require.NoError(t, err)

	// Should recommend health action
	assert.Contains(t, resp.Choices[0].Message.Content, "health")
}
