package heimdall

import (
	"context"
	"fmt"
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

// NewManager creates an SLM manager using BYOM configuration.
// Returns nil if SLM feature is disabled.
func NewManager(cfg Config) (*Manager, error) {
	if !cfg.Enabled {
		return nil, nil // Feature disabled
	}

	// Get model name (fallback to default if not configured)
	modelName := cfg.Model
	if modelName == "" {
		modelName = "qwen2.5-0.5b-instruct"
	}
	// Only add .gguf if not already present
	modelFile := modelName
	if !strings.HasSuffix(modelFile, ".gguf") {
		modelFile = modelFile + ".gguf"
	}

	// Resolve model path - check where the actual model file exists
	modelsDir := cfg.ModelsDir
	if modelsDir == "" {
		// Check common model locations - look for the actual model file
		candidates := []string{
			"/usr/local/var/nornicdb/models", // macOS default (LaunchAgent)
			"/app/models",                    // Docker container (embedded models)
			"/data/models",                   // Mounted volume
			"./models",                       // Local development
		}
		for _, dir := range candidates {
			fullPath := filepath.Join(dir, modelFile)
			if _, err := os.Stat(fullPath); err == nil {
				modelsDir = dir
				fmt.Printf("   Found Heimdall model at: %s\n", fullPath)
				break
			}
		}
		if modelsDir == "" {
			modelsDir = "/data/models" // Final fallback for error message
		}
	}

	modelPath := filepath.Join(modelsDir, modelFile)

	// Check if model file exists
	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("Heimdall model not found: %s (expected at %s)\n"+
			"  → Download a GGUF model and place it in the models directory\n"+
			"  → Or set NORNICDB_HEIMDALL_MODEL to point to an existing model\n"+
			"  → Recommended: qwen2.5-0.5b-instruct (Apache 2.0 license)",
			modelName, modelPath)
	}

	// Get GPU layers config - defaults to auto (-1), falls back to CPU if needed
	gpuLayers := cfg.GPULayers
	if gpuLayers == 0 {
		gpuLayers = -1 // Auto
	}

	// Context and batch size - balanced for memory efficiency
	// Heimdall produces structured JSON (~500-2000 tokens max), so 8K context is ample.
	// Using 32K would allocate ~2.5GB GPU memory just for KV cache!
	// Note: Token budgets in types.go allow up to 16K, but actual usage rarely exceeds 4K.
	contextSize := cfg.ContextSize
	if contextSize == 0 {
		contextSize = 8192 // 8K is plenty for structured JSON output
	}
	batchSize := cfg.BatchSize
	if batchSize == 0 {
		batchSize = 2048 // Match typical prompt size
	}

	fmt.Printf("🛡️ Loading Heimdall model: %s\n", modelPath)
	fmt.Printf("   GPU layers: %d (-1 = auto, falls back to CPU if needed)\n", gpuLayers)
	fmt.Printf("   Context: %d tokens, Batch: %d tokens (single-shot mode)\n", contextSize, batchSize)

	// Load the model - this uses the stub for now, will be replaced with CGO impl
	generator, err := loadGenerator(modelPath, gpuLayers, contextSize, batchSize)
	if err != nil {
		// Try CPU fallback
		fmt.Printf("⚠️  GPU loading failed, trying CPU fallback: %v\n", err)
		generator, err = loadGenerator(modelPath, 0, contextSize, batchSize) // 0 = CPU only
		if err != nil {
			return nil, fmt.Errorf("failed to load SLM model: %w", err)
		}
		fmt.Printf("✅ SLM model loaded on CPU (slower but functional)\n")
	} else {
		fmt.Printf("✅ SLM model loaded: %s\n", modelName)
	}

	// Log token budget allocation
	fmt.Printf("   Token budget: %dK context = %dK system + %dK user (multi-batch prefill)\n",
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

// Chat handles chat completion requests.
func (m *Manager) Chat(ctx context.Context, req ChatRequest) (*ChatResponse, error) {
	prompt := BuildPrompt(req.Messages)

	params := GenerateParams{
		MaxTokens:   req.MaxTokens,
		Temperature: req.Temperature,
		TopP:        req.TopP,
		TopK:        40,
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
		fmt.Printf("🧠 Closing SLM model\n")
		fmt.Printf("   Total requests: %d\n", m.requestCount)
		fmt.Printf("   Total errors: %d\n", m.errorCount)
		return m.generator.Close()
	}
	return nil
}

// idCounter provides unique IDs even within the same nanosecond.
var idCounter uint64

// generateID creates a unique request ID using atomic counter for thread safety.
func generateID() string {
	// Use atomic operations for thread safety
	counter := atomic.AddUint64(&idCounter, 1)
	return fmt.Sprintf("chatcmpl-%d-%d", time.Now().UnixNano(), counter)
}
