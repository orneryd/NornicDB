package heimdall

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Manager handles Heimdall SLM model loading and inference.
// Follows the same BYOM pattern as the embedding subsystem.
//
// Environment variables:
//   - NORNICDB_MODELS_DIR: Directory for .gguf files (default: /data/models)
//   - NORNICDB_HEIMDALL_MODEL: Model name (default: qwen2.5-1.5b-instruct-q4_k_m)
//   - NORNICDB_HEIMDALL_GPU_LAYERS: GPU layer offload (-1=auto, 0=CPU only)
//   - NORNICDB_HEIMDALL_ENABLED: Feature flag (default: false)
type Manager struct {
	mu        sync.RWMutex
	generator Generator
	config    Config
	modelPath string
	closed    bool

	// Stats
	requestCount int64
	errorCount   int64
	lastUsed     time.Time
}

// heimdallProviderFactories maps remote provider names to constructors.
// Populated by init() in generator provider files so scheduler
// does not reference those packages' constructors directly (avoids linter/IDE undefined errors).
var heimdallProviderFactories = make(map[string]func(Config) (Generator, error))

// RegisterHeimdallProvider registers a remote Heimdall provider. Called from generator_* init().
func RegisterHeimdallProvider(name string, factory func(Config) (Generator, error)) {
	heimdallProviderFactories[name] = factory
}

// NewManager creates an SLM manager using BYOM configuration.
// Returns nil if SLM feature is disabled.
//
// Heimdall provider selection:
//   - openai: Use OpenAI (or compatible) chat API; requires NORNICDB_HEIMDALL_API_KEY.
//   - ollama: Use Ollama /api/chat; NORNICDB_HEIMDALL_API_URL defaults to http://localhost:11434.
//   - vllm: Use vLLM's OpenAI-compatible API; NORNICDB_HEIMDALL_API_URL defaults to http://localhost:8000.
//   - litellm: Use a LiteLLM proxy (OpenAI-compatible gateway to 100+ providers);
//     NORNICDB_HEIMDALL_API_URL defaults to http://localhost:4000.
//   - local or empty: Load GGUF from NORNICDB_MODELS_DIR (BYOM).
func NewManager(cfg Config) (*Manager, error) {
	if !cfg.Enabled {
		return nil, nil // Feature disabled
	}

	provider := strings.ToLower(strings.TrimSpace(cfg.Provider))
	if provider == "" {
		provider = "local"
	}

	var generator Generator
	var modelPath string
	var err error

	if provider == "local" || provider == "" {
		generator, modelPath, err = loadLocalGenerator(cfg)
		if err != nil {
			return nil, err
		}
	} else if factory, ok := heimdallProviderFactories[provider]; ok {
		generator, err = factory(cfg)
		if err != nil {
			return nil, fmt.Errorf("Heimdall %s provider: %w", provider, err)
		}
		modelPath = generator.ModelPath()
		logHeimdallPrintf(slog.LevelInfo, "🛡️ Heimdall using %s: %s", provider, modelPath)
	} else {
		generator, modelPath, err = loadLocalGenerator(cfg)
		if err != nil {
			return nil, err
		}
	}

	// Best-effort GPU status logging (implementation-specific).
	// Some generator backends can report whether GPU acceleration is active.
	type gpuInfo interface {
		UsingGPU() bool
		GPULayers() int
	}
	if gi, ok := generator.(gpuInfo); ok {
		logHeimdallPrintf(slog.LevelInfo, "   Compute: GPU=%v (gpu_layers=%d)", gi.UsingGPU(), gi.GPULayers())
	} else {
		logHeimdallPrintf(slog.LevelInfo, "   Compute: GPU=unknown (generator does not report backend)")
	}

	// Log token budget allocation
	logHeimdallPrintf(slog.LevelInfo, "   Token budget: %dK context = %dK system + %dK user (multi-batch prefill)",
		MaxContextTokens()/1024, MaxSystemPromptTokens()/1024, MaxUserMessageTokens()/1024)

	return &Manager{
		generator: generator,
		config:    cfg,
		modelPath: modelPath,
		lastUsed:  time.Now(),
	}, nil
}

// GeneratorLoader is a function type for loading generators.
// This can be replaced for testing or by CGO implementation.
// Parameters:
//   - modelPath: Path to the GGUF model file
//   - gpuLayers: GPU layer offload (-1=auto, 0=CPU only)
//   - contextSize: Context window size (single-shot = 8192)
//   - batchSize: Batch processing size (match context for single-shot)
type GeneratorLoader func(modelPath string, gpuLayers, contextSize, batchSize int) (Generator, error)

// DefaultGeneratorLoader is the default loader (stub without CGO).
var DefaultGeneratorLoader GeneratorLoader = func(modelPath string, gpuLayers, contextSize, batchSize int) (Generator, error) {
	return nil, fmt.Errorf("SLM generation requires CGO build with localllm tag")
}

// generatorLoader is the active loader function.
// Can be overridden for testing via SetGeneratorLoader.
var generatorLoader GeneratorLoader = DefaultGeneratorLoader

// SetGeneratorLoader allows overriding the generator loader for testing.
// Returns the previous loader so it can be restored.
func SetGeneratorLoader(loader GeneratorLoader) GeneratorLoader {
	prev := generatorLoader
	generatorLoader = loader
	return prev
}

// loadGenerator creates a generator for the model using the active loader.
func loadGenerator(modelPath string, gpuLayers, contextSize, batchSize int) (Generator, error) {
	return generatorLoader(modelPath, gpuLayers, contextSize, batchSize)
}

// loadLocalGenerator resolves the GGUF model path and loads it via generatorLoader.
// Used when provider is "local" or empty.
func loadLocalGenerator(cfg Config) (Generator, string, error) {
	modelName := cfg.Model
	if modelName == "" {
		modelName = "qwen3-0.6b-instruct"
	}
	modelFile := modelName
	if !strings.HasSuffix(modelFile, ".gguf") {
		modelFile = modelFile + ".gguf"
	}

	modelsDir := cfg.ModelsDir
	if modelsDir == "" {
		candidates := []string{
			"/usr/local/var/nornicdb/models",
			"/app/models",
			"/data/models",
			"./models",
		}
		for _, dir := range candidates {
			fullPath := filepath.Join(dir, modelFile)
			if _, err := os.Stat(fullPath); err == nil {
				modelsDir = dir
				logHeimdallPrintf(slog.LevelInfo, "   Found Heimdall model at: %s", fullPath)
				break
			}
		}
		if modelsDir == "" {
			modelsDir = "/data/models"
		}
	}

	modelPath := filepath.Join(modelsDir, modelFile)
	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		return nil, "", fmt.Errorf("Heimdall model not found: %s (expected at %s)\n"+
			"  → Download a GGUF model and place it in the models directory\n"+
			"  → Or set NORNICDB_HEIMDALL_MODEL to point to an existing model\n"+
			"  → Or use provider ollama/openai (NORNICDB_HEIMDALL_PROVIDER=ollama or openai)",
			modelName, modelPath)
	}

	gpuLayers := cfg.GPULayers
	contextSize := cfg.ContextSize
	if contextSize == 0 {
		contextSize = 8192
	}
	batchSize := cfg.BatchSize
	if batchSize == 0 {
		batchSize = 2048
	}

	logHeimdallPrintf(slog.LevelInfo, "🛡️ Loading Heimdall model: %s", modelPath)
	logHeimdallPrintf(slog.LevelInfo, "   GPU layers: %d (-1 = auto, falls back to CPU if needed)", gpuLayers)
	logHeimdallPrintf(slog.LevelInfo, "   Context: %d tokens, Batch: %d tokens (single-shot mode)", contextSize, batchSize)

	generator, err := loadGenerator(modelPath, gpuLayers, contextSize, batchSize)
	if err != nil {
		gpuErr := err
		logHeimdallPrintf(slog.LevelWarn, "⚠️  GPU loading failed, trying CPU fallback: %v", err)
		generator, err = loadGenerator(modelPath, 0, contextSize, batchSize)
		if err != nil {
			return nil, "", fmt.Errorf("failed to load SLM model: gpu load failed: %v; cpu fallback failed: %w", gpuErr, err)
		}
		logHeimdallPrintf(slog.LevelInfo, "✅ SLM model loaded (gpu_layers=0)")
	} else {
		logHeimdallPrintf(slog.LevelInfo, "✅ SLM model loaded: %s", modelName)
	}
	return generator, modelPath, nil
}

// Generate produces a response for the given prompt.
func (m *Manager) Generate(ctx context.Context, prompt string, params GenerateParams) (string, error) {
	m.mu.RLock()
	if m.closed {
		m.mu.RUnlock()
		return "", fmt.Errorf("manager is closed")
	}
	gen := m.generator
	m.mu.RUnlock()

	if gen == nil {
		return "", fmt.Errorf("no generator loaded")
	}

	m.mu.Lock()
	m.requestCount++
	m.lastUsed = time.Now()
	m.mu.Unlock()

	result, err := gen.Generate(ctx, prompt, params)
	if err != nil {
		m.mu.Lock()
		m.errorCount++
		m.mu.Unlock()
		return "", err
	}

	return result, nil
}

// GenerateStream produces tokens via callback.
func (m *Manager) GenerateStream(ctx context.Context, prompt string, params GenerateParams, callback func(token string) error) error {
	m.mu.RLock()
	if m.closed {
		m.mu.RUnlock()
		return fmt.Errorf("manager is closed")
	}
	gen := m.generator
	m.mu.RUnlock()

	if gen == nil {
		return fmt.Errorf("no generator loaded")
	}

	m.mu.Lock()
	m.requestCount++
	m.lastUsed = time.Now()
	m.mu.Unlock()

	err := gen.GenerateStream(ctx, prompt, params, callback)
	if err != nil {
		m.mu.Lock()
		m.errorCount++
		m.mu.Unlock()
		return err
	}

	return nil
}

// SupportsTools returns true if the generator supports native tool/function calling
// (e.g. OpenAI, Ollama). When true, the handler uses GenerateWithTools and an agentic loop.
func (m *Manager) SupportsTools() bool {
	m.mu.RLock()
	gen := m.generator
	m.mu.RUnlock()
	if gen == nil {
		return false
	}
	_, ok := gen.(GeneratorWithTools)
	return ok
}

// GenerateWithTools runs one round of chat with tools (agentic loop). Only valid when SupportsTools() is true.
// Returns content and/or toolCalls; caller executes tools and calls again until no toolCalls.
func (m *Manager) GenerateWithTools(ctx context.Context, messages []ToolRoundMessage, tools []MCPTool, params GenerateParams) (content string, toolCalls []ParsedToolCall, err error) {
	m.mu.RLock()
	if m.closed {
		m.mu.RUnlock()
		return "", nil, fmt.Errorf("manager is closed")
	}
	gen := m.generator
	m.mu.RUnlock()

	if gen == nil {
		return "", nil, fmt.Errorf("no generator loaded")
	}
	gwt, ok := gen.(GeneratorWithTools)
	if !ok {
		return "", nil, fmt.Errorf("generator does not support tools")
	}

	m.mu.Lock()
	m.requestCount++
	m.lastUsed = time.Now()
	m.mu.Unlock()

	return gwt.GenerateWithTools(ctx, messages, tools, params)
}

// Chat handles chat completion requests.
func (m *Manager) Chat(ctx context.Context, req ChatRequest) (*ChatResponse, error) {
	prompt := BuildPrompt(req.Messages)

	params := GenerateParams{
		MaxTokens:   req.MaxTokens,
		Temperature: req.Temperature,
		TopP:        req.TopP,
		TopK:        20, // Qwen3 0.6B instruct best practice to reduce repetition
		StopTokens:  []string{"<|im_end|>", "<|endoftext|>", "</s>"},
	}
	if params.MaxTokens == 0 {
		params.MaxTokens = m.config.MaxTokens
	}
	if params.Temperature == 0 {
		params.Temperature = m.config.Temperature
	}

	response, err := m.Generate(ctx, prompt, params)
	if err != nil {
		return nil, err
	}

	return &ChatResponse{
		ID:      generateID(),
		Model:   m.config.Model,
		Created: time.Now().Unix(),
		Choices: []ChatChoice{
			{
				Index: 0,
				Message: &ChatMessage{
					Role:    "assistant",
					Content: response,
				},
				FinishReason: "stop",
			},
		},
	}, nil
}

// Stats returns current manager statistics.
type ManagerStats struct {
	ModelPath    string    `json:"model_path"`
	RequestCount int64     `json:"request_count"`
	ErrorCount   int64     `json:"error_count"`
	LastUsed     time.Time `json:"last_used"`
	Enabled      bool      `json:"enabled"`
}

func (m *Manager) Stats() ManagerStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return ManagerStats{
		ModelPath:    m.modelPath,
		RequestCount: m.requestCount,
		ErrorCount:   m.errorCount,
		LastUsed:     m.lastUsed,
		Enabled:      true,
	}
}

// Close releases all resources.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}
	m.closed = true

	if m.generator != nil {
		logHeimdallPrintf(slog.LevelInfo, "🧠 Closing SLM model")
		logHeimdallPrintf(slog.LevelInfo, "   Total requests: %d", m.requestCount)
		logHeimdallPrintf(slog.LevelInfo, "   Total errors: %d", m.errorCount)
		return m.generator.Close()
	}
	return nil
}

// ModelPath returns the path to the loaded model.
// This allows Manager to implement the Generator interface.
func (m *Manager) ModelPath() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.modelPath
}

// idCounter provides unique IDs even within the same nanosecond.
var idCounter uint64

// generateID creates a unique request ID using atomic counter for thread safety.
func generateID() string {
	// Use atomic operations for thread safety
	counter := atomic.AddUint64(&idCounter, 1)
	return fmt.Sprintf("chatcmpl-%d-%d", time.Now().UnixNano(), counter)
}
