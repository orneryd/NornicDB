//go:build localllm

// LocalGGUFEmbedder provides embedding generation using local GGUF models.
//
// This embedder runs models directly within NornicDB using llama.cpp,
// eliminating the need for external services like Ollama.
//
// Features:
//   - GPU acceleration (CUDA/Metal) with CPU fallback
//   - Memory-mapped model loading for low memory footprint
//   - Thread-safe concurrent embedding generation
//   - Crash resilience with panic recovery
//   - Model warmup to prevent GPU memory eviction
//
// Example:
//
//	config := &embed.Config{
//		Provider:   "local",
//		Model:      "bge-m3", // Resolves to /data/models/bge-m3.gguf
//		Dimensions: 1024,
//	}
//	embedder, err := embed.NewLocalGGUF(config)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer embedder.Close()
//
//	vec, err := embedder.Embed(ctx, "hello world")
package embed

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/localllm"
)

// LocalGGUFEmbedder implements Embedder using a local GGUF model via llama.cpp.
//
// This embedder provides GPU-accelerated embedding generation without
// external dependencies. Models are loaded from the configured models
// directory (default: /data/models/).
//
// Thread-safe: Can be used concurrently from multiple goroutines.
//
// Crash Resilience:
//   - Panics in CGO are caught and converted to errors
//   - Model warmup keeps GPU memory active to prevent eviction
//   - Detailed logging helps diagnose CUDA/Metal issues
type LocalGGUFEmbedder struct {
	model     *localllm.Model
	modelName string
	modelPath string

	// Crash resilience
	mu            sync.RWMutex
	closed        bool
	stopWarmup    chan struct{}
	warmupStopped chan struct{}

	// Stats for monitoring
	embedCount    atomic.Int64
	errorCount    atomic.Int64
	panicCount    atomic.Int64
	lastEmbedTime atomic.Int64 // Unix timestamp
}

// NewLocalGGUF creates an embedder using the existing Config pattern.
//
// Model resolution: config.Model → {NORNICDB_MODELS_DIR}/{model}.gguf
//
// Environment variables:
//   - NORNICDB_MODELS_DIR: Directory for .gguf files (default: /data/models)
//   - NORNICDB_EMBEDDING_GPU_LAYERS: GPU layer offload (-1=auto, 0=CPU, N=N layers)
//   - NORNICDB_EMBEDDING_WARMUP_INTERVAL: Warmup interval (default: 5m, 0=disabled)
//
// Example:
//
//	config := &embed.Config{
//		Provider:   "local",
//		Model:      "bge-m3",
//		Dimensions: 1024,
//	}
//	embedder, err := embed.NewLocalGGUF(config)
//	if err != nil {
//		// Model not found or failed to load
//		log.Fatal(err)
//	}
//	defer embedder.Close()
//
//	vec, _ := embedder.Embed(ctx, "semantic search")
//	fmt.Printf("Dimensions: %d\n", len(vec)) // 1024
func NewLocalGGUF(config *Config) (*LocalGGUFEmbedder, error) {
	// Resolve model path: model name → /data/models/{name}.gguf
	modelsDir := os.Getenv("NORNICDB_MODELS_DIR")
	if modelsDir == "" {
		modelsDir = "/data/models"
	}

	modelPath := filepath.Join(modelsDir, config.Model+".gguf")

	// Check if file exists
	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("model not found: %s (expected at %s)\n"+
			"  → Download a GGUF model (e.g., bge-m3) and place it in the models directory\n"+
			"  → Or set NORNICDB_MODELS_DIR to point to your models directory",
			config.Model, modelPath)
	}

	opts := localllm.DefaultOptions(modelPath)

	// Configure GPU layers from environment
	if gpuLayersStr := os.Getenv("NORNICDB_EMBEDDING_GPU_LAYERS"); gpuLayersStr != "" {
		if gpuLayers, err := strconv.Atoi(gpuLayersStr); err == nil {
			opts.GPULayers = gpuLayers
		}
	}

	fmt.Printf("🧠 Loading local embedding model: %s\n", modelPath)
	fmt.Printf("   GPU layers: %d (-1 = auto/all)\n", opts.GPULayers)

	model, err := localllm.LoadModel(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to load model %s: %w", modelPath, err)
	}

	// Verify dimensions match if specified
	if config.Dimensions > 0 && model.Dimensions() != config.Dimensions {
		model.Close()
		return nil, fmt.Errorf("dimension mismatch: model has %d, config expects %d",
			model.Dimensions(), config.Dimensions)
	}

	fmt.Printf("✅ Model loaded: %s (%d dimensions)\n", config.Model, model.Dimensions())

	embedder := &LocalGGUFEmbedder{
		model:         model,
		modelName:     config.Model,
		modelPath:     modelPath,
		stopWarmup:    make(chan struct{}),
		warmupStopped: make(chan struct{}),
	}

	// Start warmup goroutine to keep model in GPU memory
	warmupInterval := 5 * time.Minute // Default: warmup every 5 minutes
	if intervalStr := os.Getenv("NORNICDB_EMBEDDING_WARMUP_INTERVAL"); intervalStr != "" {
		if d, err := time.ParseDuration(intervalStr); err == nil {
			warmupInterval = d
		}
	}

	if warmupInterval > 0 {
		go embedder.warmupLoop(warmupInterval)
		fmt.Printf("🔥 Model warmup enabled: every %v\n", warmupInterval)
	}

	return embedder, nil
}

// warmupLoop periodically generates a dummy embedding to keep the model in GPU memory.
// This prevents GPU memory eviction which would require reloading from disk.
func (e *LocalGGUFEmbedder) warmupLoop(interval time.Duration) {
	defer close(e.warmupStopped)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopWarmup:
			return
		case <-ticker.C:
			// Check if we've had recent activity (no need to warmup if actively used)
			lastEmbed := time.Unix(e.lastEmbedTime.Load(), 0)
			if time.Since(lastEmbed) < interval/2 {
				continue // Model was recently used, skip warmup
			}

			// Generate a dummy embedding to keep model warm
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_, err := e.embedWithRecovery(ctx, "warmup")
			cancel()

			if err != nil {
				fmt.Printf("⚠️  Model warmup failed: %v\n", err)
			}
		}
	}
}

// embedWithRecovery wraps the embed call with panic recovery.
// CGO calls can panic on segfaults or other C-level errors.
// This prevents the entire process from crashing.
func (e *LocalGGUFEmbedder) embedWithRecovery(ctx context.Context, text string) (embedding []float32, err error) {
	// Recover from panics in CGO
	defer func() {
		if r := recover(); r != nil {
			e.panicCount.Add(1)

			// Capture stack trace for debugging
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			stackTrace := string(buf[:n])

			err = fmt.Errorf("PANIC in llama.cpp embedding (recovered): %v\nStack trace:\n%s", r, stackTrace)

			// Log detailed error for diagnostics
			fmt.Printf("🔴 EMBEDDING PANIC RECOVERED:\n")
			fmt.Printf("   Error: %v\n", r)
			fmt.Printf("   Text length: %d\n", len(text))
			fmt.Printf("   Model: %s\n", e.modelName)
			fmt.Printf("   Total panics: %d\n", e.panicCount.Load())
			fmt.Printf("   Stack trace:\n%s\n", stackTrace)
		}
	}()

	// Check if embedder is closed
	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return nil, fmt.Errorf("embedder is closed")
	}
	model := e.model
	e.mu.RUnlock()

	// Perform the actual embedding
	embedding, err = model.Embed(ctx, text)

	if err != nil {
		e.errorCount.Add(1)

		// Log CUDA/Metal specific errors for diagnostics
		errStr := err.Error()
		if containsGPUError(errStr) {
			fmt.Printf("🔴 GPU ERROR detected:\n")
			fmt.Printf("   Error: %v\n", err)
			fmt.Printf("   Text length: %d\n", len(text))
			fmt.Printf("   Model: %s\n", e.modelName)
			fmt.Printf("   Total errors: %d\n", e.errorCount.Load())
		}
	}

	return embedding, err
}

// containsGPUError checks if an error message indicates a GPU-related issue.
func containsGPUError(errStr string) bool {
	gpuKeywords := []string{
		"CUDA", "cuda", "GPU", "gpu",
		"Metal", "metal",
		"out of memory", "OOM",
		"device", "driver",
		"cublas", "cudnn",
		"allocation failed",
	}
	for _, kw := range gpuKeywords {
		if contains(errStr, kw) {
			return true
		}
	}
	return false
}

// contains is a simple string contains check (avoiding strings import for build tag compat)
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// Embed generates a normalized embedding for the given text.
//
// The returned vector is L2-normalized, suitable for cosine similarity.
//
// Crash Resilience:
//   - Panics in llama.cpp are caught and converted to errors
//   - Errors are logged with diagnostics for debugging
//
// Example:
//
//	vec, err := embedder.Embed(ctx, "graph database")
//	if err != nil {
//		return err
//	}
//	// Use vec for similarity search
func (e *LocalGGUFEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	e.embedCount.Add(1)
	e.lastEmbedTime.Store(time.Now().Unix())
	return e.embedWithRecovery(ctx, text)
}

// EmbedBatch generates embeddings for multiple texts.
//
// Each text is processed sequentially with crash recovery.
// If one text fails, processing continues with the others.
//
// Example:
//
//	texts := []string{"query 1", "query 2", "query 3"}
//	vecs, err := embedder.EmbedBatch(ctx, texts)
func (e *LocalGGUFEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	results := make([][]float32, len(texts))
	var firstErr error

	for i, t := range texts {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		emb, err := e.Embed(ctx, t)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("text %d: %w", i, err)
			}
			// Continue processing other texts even if one fails
			fmt.Printf("⚠️  Embedding failed for text %d (len=%d): %v\n", i, len(t), err)
			continue
		}
		results[i] = emb
	}

	// Return first error if any, but still return partial results
	return results, firstErr
}

// Dimensions returns the embedding vector dimension.
//
// Common values:
//   - BGE-M3: 1024
//   - E5-large: 1024
//   - Jina-v2-base-code: 768
func (e *LocalGGUFEmbedder) Dimensions() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.model == nil {
		return 0
	}
	return e.model.Dimensions()
}

// Model returns the model name (without path or extension).
func (e *LocalGGUFEmbedder) Model() string {
	return e.modelName
}

// Stats returns embedding statistics for monitoring.
type EmbedderStats struct {
	EmbedCount    int64     `json:"embed_count"`
	ErrorCount    int64     `json:"error_count"`
	PanicCount    int64     `json:"panic_count"`
	LastEmbedTime time.Time `json:"last_embed_time"`
	ModelName     string    `json:"model_name"`
	ModelPath     string    `json:"model_path"`
}

// Stats returns current embedder statistics for monitoring.
func (e *LocalGGUFEmbedder) Stats() EmbedderStats {
	lastEmbed := time.Unix(e.lastEmbedTime.Load(), 0)
	if e.lastEmbedTime.Load() == 0 {
		lastEmbed = time.Time{} // Zero time if never used
	}

	return EmbedderStats{
		EmbedCount:    e.embedCount.Load(),
		ErrorCount:    e.errorCount.Load(),
		PanicCount:    e.panicCount.Load(),
		LastEmbedTime: lastEmbed,
		ModelName:     e.modelName,
		ModelPath:     e.modelPath,
	}
}

// Close releases all resources associated with the embedder.
//
// After Close is called, the embedder must not be used.
func (e *LocalGGUFEmbedder) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}
	e.closed = true

	// Stop warmup goroutine
	close(e.stopWarmup)

	// Wait for warmup to finish (with timeout)
	select {
	case <-e.warmupStopped:
	case <-time.After(5 * time.Second):
		fmt.Printf("⚠️  Warmup goroutine did not stop in time\n")
	}

	// Close the model
	if e.model != nil {
		fmt.Printf("🧠 Closing embedding model: %s\n", e.modelName)
		fmt.Printf("   Total embeddings: %d\n", e.embedCount.Load())
		fmt.Printf("   Total errors: %d\n", e.errorCount.Load())
		fmt.Printf("   Total panics recovered: %d\n", e.panicCount.Load())
		return e.model.Close()
	}
	return nil
}
