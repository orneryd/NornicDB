package heimdall

import (
	"context"
	"errors"
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
	// Extract the last user message from the ChatML format
	// Look for the last "<|im_start|>user\n" section
	userStart := strings.LastIndex(prompt, "<|im_start|>user\n")
	userContent := prompt
	if userStart != -1 {
		// Extract from after the user tag to the next tag or end
		contentStart := userStart + len("<|im_start|>user\n")
		contentEnd := strings.Index(prompt[contentStart:], "<|im_end|>")
		if contentEnd != -1 {
			userContent = prompt[contentStart : contentStart+contentEnd]
		} else {
			userContent = prompt[contentStart:]
		}
	}

	lower := strings.ToLower(userContent)

	// Health check - explicit status/health queries
	if strings.Contains(lower, "health") ||
		(strings.Contains(lower, "status") && !strings.Contains(lower, "who")) {
		return `{"action": "heimdall.watcher.health", "params": {}}`
	}

	// Anomaly detection - check for "anomal" to catch "anomaly", "anomalies", etc.
	if strings.Contains(lower, "anomal") || strings.Contains(lower, "unusual") {
		return `{"action": "heimdall.anomaly.detect", "params": {"threshold": 0.8}}`
	}

	// Help request
	if strings.Contains(lower, "help") || strings.Contains(lower, "command") {
		return `{"action": "heimdall.help", "params": {}}`
	}

	// Configuration
	if strings.Contains(lower, "config") || strings.Contains(lower, "setting") {
		return `{"action": "heimdall.watcher.config", "params": {}}`
	}

	// Metrics
	if strings.Contains(lower, "metric") || strings.Contains(lower, "stats") {
		return `{"action": "heimdall.watcher.metrics", "params": {}}`
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
		ModelsDir: "/nonexistent/path",
		Model:     "nonexistent-model",
	}

	manager, err := NewManager(cfg)

	assert.Error(t, err)
	assert.Nil(t, manager)
	assert.Contains(t, err.Error(), "not found")
}

func TestManager_Generate(t *testing.T) {
	mockGen := NewMockGenerator("/test/model.gguf")
	manager := newTestManager(mockGen)

	ctx := context.Background()
	prompt := "Check system health"
	params := DefaultGenerateParams()

	result, err := manager.Generate(ctx, prompt, params)

	require.NoError(t, err)
	assert.Contains(t, result, "heimdall.watcher.health")
	assert.Equal(t, int64(1), mockGen.GetGenerateCount())
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
		{"Check system health", "heimdall.watcher.health"},
		{"What's the status?", "heimdall.watcher.health"},
		{"Detect anomalies", "heimdall.anomaly.detect"},
		{"Find unusual patterns", "heimdall.anomaly.detect"},
		{"Show help", "heimdall.help"},
		{"List commands", "heimdall.help"},
		{"Get configuration", "heimdall.watcher.config"},
		{"Show settings", "heimdall.watcher.config"},
		{"Display metrics", "heimdall.watcher.metrics"},
		{"Show stats", "heimdall.watcher.metrics"},
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
	origLoader := SetGeneratorLoader(func(path string, gpuLayers, contextSize, batchSize int) (Generator, error) {
		// Verify the path was resolved correctly
		assert.Equal(t, modelPath, path)
		return NewMockGenerator(path), nil
	})
	defer SetGeneratorLoader(origLoader)

	cfg := Config{
		Enabled:   true,
		ModelsDir: tmpDir,
		Model:     "test-model",
		GPULayers: 0,
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
	origLoader := SetGeneratorLoader(func(path string, gpuLayers, contextSize, batchSize int) (Generator, error) {
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
	mockLoader := func(modelPath string, gpuLayers, contextSize, batchSize int) (Generator, error) {
		return NewMockGenerator(modelPath), nil
	}

	// Set mock loader
	prev := SetGeneratorLoader(mockLoader)
	assert.NotNil(t, prev)

	// Verify it works
	gen, err := loadGenerator("/test/model.gguf", 0, 32768, 8192)
	require.NoError(t, err)
	assert.NotNil(t, gen)

	// Restore
	SetGeneratorLoader(prev)

	// Now should fail again
	gen, err = loadGenerator("/test/model.gguf", 0, 32768, 8192)
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
	origLoader := SetGeneratorLoader(func(path string, gpuLayers, contextSize, batchSize int) (Generator, error) {
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
	origLoader := SetGeneratorLoader(func(path string, gpuLayers, contextSize, batchSize int) (Generator, error) {
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
	origLoader := SetGeneratorLoader(func(path string, gpuLayers, contextSize, batchSize int) (Generator, error) {
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
	origLoader := SetGeneratorLoader(func(path string, gpuLayers, contextSize, batchSize int) (Generator, error) {
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
