//go:build windows || yzma

// Package localllm provides GPU-accelerated inference via yzma (no CGO required).
//
// This implementation uses yzma (github.com/hybridgroup/yzma) which provides
// purego FFI bindings to llama.cpp without requiring CGO compilation. This enables:
//
// - Zero CGO overhead: Just download prebuilt llama.cpp libraries
// - Easy Windows CUDA support: yzma handles DLL loading
// - Simpler builds: No C compiler needed
// - Easy updates: Re-run yzma install for new llama.cpp versions
// - Graceful fallback: Automatically falls back to CPU if GPU unavailable
//
// GPU Support (via yzma):
//   - Windows CUDA: yzma install --lib ./lib --processor cuda
//   - Linux CUDA: yzma install --lib ./lib --processor cuda
//   - macOS Metal: yzma install --lib ./lib (Metal auto-enabled)
//   - CPU fallback: Works on all platforms without GPU drivers
//
// Library Path:
//
// Set NORNICDB_LIB environment variable to specify where yzma libraries are:
//
//	export NORNICDB_LIB=/path/to/lib  # Linux/macOS
//	set NORNICDB_LIB=C:\path\to\lib   # Windows
//
// If not set, defaults to ./lib/llama
//
// Example:
//
//	opts := localllm.DefaultOptions("/models/bge-m3.gguf")
//	model, err := localllm.LoadModel(opts)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer model.Close()
//
//	embedding, err := model.Embed(ctx, "hello world")
//	// embedding is a normalized []float32
package localllm

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/hybridgroup/yzma/pkg/llama"
	"github.com/orneryd/nornicdb/pkg/math/vector"
)

// Backend information detected at init time
var (
	gpuAvailable  bool
	gpuDeviceName string
	backendInfo   string
	initOnce      sync.Once
	initErr       error
)

// BackendInfo contains information about the detected compute backend.
type BackendInfo struct {
	GPUAvailable  bool
	GPUDeviceName string
	SystemInfo    string
	DeviceCount   int
	Devices       []string
}

// GetBackendInfo returns information about the detected compute backend.
// This is useful for logging and debugging GPU detection.
func GetBackendInfo() BackendInfo {
	initOnce.Do(doInit)
	return BackendInfo{
		GPUAvailable:  gpuAvailable,
		GPUDeviceName: gpuDeviceName,
		SystemInfo:    backendInfo,
		DeviceCount:   int(llama.GGMLBackendDeviceCount()),
		Devices:       listDevices(),
	}
}

// listDevices returns a list of all available backend device names.
func listDevices() []string {
	count := llama.GGMLBackendDeviceCount()
	devices := make([]string, 0, count)
	for i := uint64(0); i < count; i++ {
		dev := llama.GGMLBackendDeviceGet(i)
		name := llama.GGMLBackendDeviceName(dev)
		if name != "" {
			devices = append(devices, name)
		}
	}
	return devices
}

// getExeDir returns the directory containing the current executable.
// This is used to find DLLs relative to the installed location.
func getExeDir() string {
	exe, err := os.Executable()
	if err != nil {
		return "."
	}
	return filepath.Dir(exe)
}

// doInit performs one-time initialization of the llama.cpp backend.
func doInit() {
	libPath := os.Getenv("NORNICDB_LIB")
	if libPath == "" {
		// Try multiple default locations for installed apps
		candidates := []string{
			"./lib/llama", // Development/portable
			filepath.Join(getExeDir(), "lib", "llama"),                           // Next to executable
			filepath.Join(getExeDir(), "lib"),                                    // Simpler layout
			filepath.Join(os.Getenv("PROGRAMFILES"), "NornicDB", "lib", "llama"), // Installed location
		}
		for _, candidate := range candidates {
			if _, err := os.Stat(filepath.Join(candidate, "ggml.dll")); err == nil {
				libPath = candidate
				break
			}
			// Also check for ggml-base.dll (some builds)
			if _, err := os.Stat(filepath.Join(candidate, "ggml-base.dll")); err == nil {
				libPath = candidate
				break
			}
		}
		if libPath == "" {
			libPath = "./lib/llama" // Fallback to default
		}
	}

	// Convert to absolute path for Windows DLL loading
	if absPath, err := filepath.Abs(libPath); err == nil {
		libPath = absPath
	}

	// On Windows, add libPath to PATH so dependent DLLs (CUDA, VC++, etc.) can be found
	// This must be done BEFORE calling llama.Load()
	if runtime.GOOS == "windows" {
		currentPath := os.Getenv("PATH")
		if !strings.Contains(currentPath, libPath) {
			os.Setenv("PATH", libPath+";"+currentPath)
		}
	}

	log.Printf("[localllm] Loading llama.cpp libraries from: %s", libPath)

	// Load llama.cpp libraries from libPath
	if err := llama.Load(libPath); err != nil {
		// Library load failed - this is a hard error
		initErr = fmt.Errorf("failed to load llama.cpp libraries from %s: %w", libPath, err)
		log.Printf("[localllm] WARNING: %v", initErr)
		return
	}

	// Initialize llama backend (GPU detection, etc.)
	llama.Init()

	// Detect GPU availability
	detectGPU()
}

// detectGPU probes for available GPU backends and sets global state.
func detectGPU() {
	// Check if GPU offload is supported by the loaded libraries
	gpuAvailable = llama.SupportsGpuOffload()

	// Get system info for debugging
	backendInfo = llama.PrintSystemInfo()

	// Enumerate devices to find GPU
	deviceCount := llama.GGMLBackendDeviceCount()
	for i := uint64(0); i < deviceCount; i++ {
		dev := llama.GGMLBackendDeviceGet(i)
		name := llama.GGMLBackendDeviceName(dev)

		// Look for GPU device (CUDA, Metal, Vulkan, etc.)
		nameLower := strings.ToLower(name)
		if strings.Contains(nameLower, "cuda") ||
			strings.Contains(nameLower, "metal") ||
			strings.Contains(nameLower, "vulkan") ||
			strings.Contains(nameLower, "hip") ||
			strings.Contains(nameLower, "gpu") {
			gpuDeviceName = name
			gpuAvailable = true
			break
		}
	}

	// Log detection results
	if gpuAvailable && gpuDeviceName != "" {
		log.Printf("[localllm] GPU detected: %s", gpuDeviceName)
	} else if gpuAvailable {
		log.Printf("[localllm] GPU offload supported (device detection inconclusive)")
	} else {
		log.Printf("[localllm] No GPU detected, using CPU-only mode")
	}
}

// Model wraps a GGUF model for embedding generation using yzma.
//
// Thread-safe: The Embed and EmbedBatch methods can be called concurrently.
// Each method call creates its own context for thread safety.
//
// GPU Fallback: If GPU is unavailable, the model automatically uses CPU.
type Model struct {
	modelPath string
	dims      int
	modelDesc string
	gpuLayers int32 // 0=CPU only, -1=auto (all layers to GPU if available)
	usingGPU  bool  // True if GPU is being used for this model
	mu        sync.Mutex
}

// Options configures model loading and inference.
//
// Fields:
//   - ModelPath: Path to .gguf model file
//   - ContextSize: Max context size for tokenization (default: 8192)
//   - BatchSize: Batch size for processing (default: 8192)
//   - Threads: CPU threads for inference (default: NumCPU/2, min 4)
//   - GPULayers: GPU layer offload (-1=auto/all, 0=CPU only, N=N layers)
//
// Graceful Fallback:
//
// When GPULayers is -1 (auto), the model will:
//  1. Try to use GPU if available
//  2. Fall back to CPU if GPU initialization fails
//  3. Log which backend is being used
//
// Set GPULayers to 0 to force CPU-only mode (no fallback needed).
type Options struct {
	ModelPath   string
	ContextSize int
	BatchSize   int
	Threads     int
	GPULayers   int
	Features    ContextFeatures
}

// DefaultOptions returns options optimized for embedding generation.
//
// GPU is enabled by default (-1 = auto-detect and use all layers).
// Set GPULayers to 0 to force CPU-only mode.
//
// Example:
//
//	opts := localllm.DefaultOptions("/models/bge-m3.gguf")
//	opts.GPULayers = 0 // Force CPU mode
//	model, err := localllm.LoadModel(opts)
func DefaultOptions(modelPath string) Options {
	// Optimal thread count for hybrid CPU/GPU workloads
	threads := runtime.NumCPU() / 2
	if threads < 4 {
		threads = 4
	}
	if threads > 8 {
		threads = 8 // Diminishing returns beyond 8 for embeddings
	}

	return Options{
		ModelPath:   modelPath,
		ContextSize: 8192, // Cap to bge-m3 context length
		BatchSize:   8192, // Matches context for efficient processing
		Threads:     threads,
		GPULayers:   -1, // Auto: offload all layers to GPU
		Features:    DefaultContextFeatures(),
	}
}

// DefaultRerankerOptions returns options for classifier-head GGUF rerankers.
func DefaultRerankerOptions(modelPath string) Options {
	opts := DefaultOptions(modelPath)
	opts.Features.PoolingType = 4
	return opts
}

// LoadModel loads a GGUF model for embedding generation.
//
// The model is loaded using yzma's FFI bindings. GPU acceleration is
// automatically available if yzma was installed with GPU support
// (e.g., yzma install --processor cuda).
//
// Graceful Fallback:
//
// If GPULayers is -1 (auto) and GPU is unavailable, the model will
// automatically fall back to CPU-only mode without error.
//
// Example:
//
//	opts := localllm.DefaultOptions("/models/bge-m3.gguf")
//	model, err := localllm.LoadModel(opts)
//	if err != nil {
//		log.Fatalf("Failed to load model: %v", err)
//	}
//	defer model.Close()
//
//	fmt.Printf("Model loaded: %d dimensions, GPU: %v\n", model.Dimensions(), model.UsingGPU())
func LoadModel(opts Options) (*Model, error) {
	// Ensure initialization is done
	initOnce.Do(doInit)
	if initErr != nil {
		return nil, initErr
	}

	// Determine GPU layers based on availability
	gpuLayers := int32(opts.GPULayers)
	usingGPU := false

	if opts.GPULayers == -1 {
		// Auto mode: use GPU if available, otherwise CPU
		if gpuAvailable {
			gpuLayers = -1 // All layers to GPU
			usingGPU = true
			log.Printf("[localllm] Loading model with GPU acceleration (%s)", gpuDeviceName)
		} else {
			gpuLayers = 0 // CPU only
			log.Printf("[localllm] Loading model in CPU-only mode (no GPU detected)")
		}
	} else if opts.GPULayers > 0 {
		// Specific GPU layer count requested
		if gpuAvailable {
			gpuLayers = int32(opts.GPULayers)
			usingGPU = true
			log.Printf("[localllm] Loading model with %d GPU layers", gpuLayers)
		} else {
			// GPU requested but not available - fall back gracefully
			gpuLayers = 0
			log.Printf("[localllm] WARNING: GPU layers requested but no GPU available, falling back to CPU")
		}
	} else {
		// CPU-only mode explicitly requested
		gpuLayers = 0
		log.Printf("[localllm] Loading model in CPU-only mode (explicitly requested)")
	}

	// Load model with appropriate GPU layer configuration
	modelParams := llama.ModelDefaultParams()
	modelParams.NGpuLayers = gpuLayers

	lmodel, err := llama.ModelLoadFromFile(opts.ModelPath, modelParams)
	if err != nil {
		// If GPU load failed, try CPU fallback
		if usingGPU {
			log.Printf("[localllm] GPU model load failed, attempting CPU fallback: %v", err)
			modelParams.NGpuLayers = 0
			lmodel, err = llama.ModelLoadFromFile(opts.ModelPath, modelParams)
			if err != nil {
				return nil, fmt.Errorf("failed to load model (GPU and CPU fallback both failed): %w", err)
			}
			usingGPU = false
			gpuLayers = 0
			log.Printf("[localllm] Successfully loaded model in CPU-only mode after GPU failure")
		} else {
			return nil, fmt.Errorf("failed to load model: %w", err)
		}
	}
	defer llama.ModelFree(lmodel)

	// Get embedding dimensions
	dims := int(llama.ModelNEmbd(lmodel))
	if dims == 0 {
		return nil, fmt.Errorf("failed to get embedding dimensions for model: %s", opts.ModelPath)
	}

	// Get model description
	modelDesc := llama.ModelDesc(lmodel)
	if modelDesc == "" {
		modelDesc = opts.ModelPath
	}

	return &Model{
		modelPath: opts.ModelPath,
		dims:      dims,
		modelDesc: modelDesc,
		gpuLayers: gpuLayers,
		usingGPU:  usingGPU,
	}, nil
}

// Embed generates a normalized embedding vector for the given text.
//
// The returned vector is L2-normalized (unit length), suitable for
// cosine similarity calculations.
//
// GPU Acceleration:
//
// If the model was loaded with GPU support (and GPU is available),
// the embedding computation will use the GPU automatically.
// If GPU fails during inference, it gracefully falls back to CPU.
//
// Example:
//
//	vec, err := model.Embed(ctx, "graph database")
//	if err != nil {
//		return err
//	}
//	fmt.Printf("Embedding: %d dimensions\n", len(vec))
func (m *Model) Embed(ctx context.Context, text string) ([]float32, error) {
	if text == "" {
		return nil, nil
	}

	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Load model with stored GPU configuration
	modelParams := llama.ModelDefaultParams()
	modelParams.NGpuLayers = m.gpuLayers

	model, err := llama.ModelLoadFromFile(m.modelPath, modelParams)
	if err != nil {
		// Try CPU fallback if GPU was being used
		if m.gpuLayers != 0 {
			modelParams.NGpuLayers = 0
			model, err = llama.ModelLoadFromFile(m.modelPath, modelParams)
			if err != nil {
				return nil, fmt.Errorf("failed to load model (GPU and CPU both failed): %w", err)
			}
			// Note: We don't update m.gpuLayers here to avoid race conditions
			// Each Embed call will retry GPU first
		} else {
			return nil, fmt.Errorf("failed to load model: %w", err)
		}
	}
	defer llama.ModelFree(model)

	// Create context for embeddings
	ctxParams := llama.ContextDefaultParams()
	ctxParams.Embeddings = 1 // Enable embeddings (uint8 bool)
	lctx, err := llama.InitFromModel(model, ctxParams)
	if err != nil {
		return nil, fmt.Errorf("failed to create context: %w", err)
	}
	defer llama.Free(lctx)

	// Get vocab
	vocab := llama.ModelGetVocab(model)

	// Tokenize
	tokens := llama.Tokenize(vocab, text, true, false)
	if len(tokens) == 0 {
		return nil, fmt.Errorf("text produced no tokens")
	}

	// Create batch
	batch := llama.BatchGetOne(tokens)
	// NOTE: BatchGetOne returns stack-allocated batch - do NOT call BatchFree
	// Only batches created via BatchInit need to be freed

	// Encode (for embeddings)
	if _, err := llama.Encode(lctx, batch); err != nil {
		return nil, fmt.Errorf("encoding failed: %w", err)
	}

	// Get embeddings (nOutputs=1 for single sequence, nEmbeddings=dims)
	emb, err := llama.GetEmbeddings(lctx, 1, m.dims)
	if err != nil {
		return nil, fmt.Errorf("failed to get embeddings: %w", err)
	}
	if len(emb) != m.dims {
		return nil, fmt.Errorf("embedding dimension mismatch: got %d, expected %d", len(emb), m.dims)
	}

	// Make a copy and normalize
	result := make([]float32, len(emb))
	copy(result, emb)
	vector.NormalizeInPlace(result)

	return result, nil
}

// EmbedBatch generates normalized embeddings for multiple texts.
//
// Each text is processed sequentially. For maximum throughput with many texts,
// consider parallel processing with multiple Model instances.
//
// Example:
//
//	texts := []string{"hello", "world", "test"}
//	vecs, err := model.EmbedBatch(ctx, texts)
//	if err != nil {
//		return err
//	}
//	for i, vec := range vecs {
//		fmt.Printf("Text %d: %d dims\n", i, len(vec))
//	}
func (m *Model) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	results := make([][]float32, len(texts))
	for i, text := range texts {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		vec, err := m.Embed(ctx, text)
		if err != nil {
			return nil, fmt.Errorf("embedding batch failed at index %d: %w", i, err)
		}
		results[i] = vec
	}
	return results, nil
}

// Dimensions returns the embedding vector dimensionality.
func (m *Model) Dimensions() int {
	return m.dims
}

// ModelDescription returns a human-readable description of the model.
func (m *Model) ModelDescription() string {
	return m.modelDesc
}

// UsingGPU returns true if the model is using GPU acceleration.
func (m *Model) UsingGPU() bool {
	return m.usingGPU
}

// GPULayers returns the number of layers offloaded to GPU.
// Returns -1 if all layers are on GPU, 0 if CPU-only.
func (m *Model) GPULayers() int {
	return int(m.gpuLayers)
}

// Close frees any resources associated with the model.
// No-op with yzma (libraries remain loaded for next model).
func (m *Model) Close() error {
	return nil
}

// ============================================================================
// RERANKER (Stage-2 search reranking)
// ============================================================================

// RerankerModel wraps a model for reranking (query, document) pairs.
//
// Note: yzma does not currently expose a dedicated reranker/logit API here,
// so we approximate relevance with embedding cosine similarity.
type RerankerModel struct {
	model *Model
}

// LoadRerankerModel loads a GGUF model for reranking.
func LoadRerankerModel(opts Options) (*RerankerModel, error) {
	model, err := LoadModel(opts)
	if err != nil {
		return nil, err
	}
	return &RerankerModel{model: model}, nil
}

// Score returns a relevance score in [0,1] for a (query, document) pair.
//
// Implementation detail:
//   - Computes normalized embeddings for query and document
//   - Uses cosine similarity
//   - Maps from [-1,1] to [0,1]
func (r *RerankerModel) Score(ctx context.Context, query, document string) (float32, error) {
	if r == nil || r.model == nil {
		return 0, fmt.Errorf("reranker model not loaded")
	}

	qVec, err := r.model.Embed(ctx, query)
	if err != nil {
		return 0, err
	}
	dVec, err := r.model.Embed(ctx, document)
	if err != nil {
		return 0, err
	}
	if len(qVec) == 0 || len(dVec) == 0 {
		return 0, fmt.Errorf("reranker returned empty embedding")
	}

	cos := vector.CosineSimilarity(qVec, dVec)
	if cos > 1 {
		cos = 1
	}
	if cos < -1 {
		cos = -1
	}

	return float32((cos + 1.0) / 2.0), nil
}

// Close releases reranker resources.
func (r *RerankerModel) Close() error {
	if r == nil || r.model == nil {
		return nil
	}
	return r.model.Close()
}

// ============================================================================
// TEXT GENERATION (for Heimdall SLM)
// ============================================================================

// GenerationModel wraps a GGUF model for text generation using yzma.
//
// GPU Fallback: If GPU is unavailable, the model automatically uses CPU.
type GenerationModel struct {
	modelPath   string
	contextSize uint32
	batchSize   uint32
	gpuLayers   int32
	usingGPU    bool
	mu          sync.Mutex
}

// GenerationOptions configures text generation model loading.
type GenerationOptions struct {
	ModelPath   string
	ContextSize int
	BatchSize   int
	Threads     int
	GPULayers   int
	Features    ContextFeatures
}

// DefaultGenerationOptions returns options optimized for text generation.
func DefaultGenerationOptions(modelPath string) GenerationOptions {
	threads := runtime.NumCPU() / 2
	if threads < 4 {
		threads = 4
	}
	if threads > 16 {
		threads = 16
	}

	return GenerationOptions{
		ModelPath:   modelPath,
		ContextSize: 2048,
		BatchSize:   512,
		Threads:     threads,
		GPULayers:   -1, // Auto: use GPU if available
		Features: ContextFeatures{
			CtxType:       0,  // LLAMA_CONTEXT_TYPE_DEFAULT
			PoolingType:   -1, // LLAMA_POOLING_TYPE_UNSPECIFIED
			AttentionType: 0,  // LLAMA_ATTENTION_TYPE_CAUSAL
			FlashAttn:     -1, // LLAMA_FLASH_ATTN_TYPE_AUTO
		},
	}
}

// LoadGenerationModel loads a GGUF model for text generation.
//
// Graceful Fallback:
//
// If GPULayers is -1 (auto) and GPU is unavailable, the model will
// automatically fall back to CPU-only mode without error.
//
// Example:
//
//	opts := localllm.DefaultGenerationOptions("/models/heimdall-7b.gguf")
//	model, err := localllm.LoadGenerationModel(opts)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer model.Close()
//
//	fmt.Printf("Model loaded, GPU: %v\n", model.UsingGPU())
func LoadGenerationModel(opts GenerationOptions) (*GenerationModel, error) {
	// Ensure initialization is done
	initOnce.Do(doInit)
	if initErr != nil {
		return nil, initErr
	}

	// Verify model exists
	if _, err := os.Stat(opts.ModelPath); err != nil {
		return nil, fmt.Errorf("model file not found: %w", err)
	}

	// Determine GPU layers based on availability
	gpuLayers := int32(opts.GPULayers)
	usingGPU := false

	if opts.GPULayers == -1 {
		if gpuAvailable {
			gpuLayers = -1
			usingGPU = true
			log.Printf("[localllm] Loading generation model with GPU acceleration")
		} else {
			gpuLayers = 0
			log.Printf("[localllm] Loading generation model in CPU-only mode")
		}
	} else if opts.GPULayers > 0 && gpuAvailable {
		gpuLayers = int32(opts.GPULayers)
		usingGPU = true
	} else {
		gpuLayers = 0
	}

	// Allow forcing CPU-only mode via environment for debugging (set to "1")
	if os.Getenv("NORNICDB_FORCE_CPU") == "1" {
		log.Printf("[localllm] FORCE_CPU=1 set: forcing generation model to CPU-only for debugging")
		gpuLayers = 0
		usingGPU = false
	}

	return &GenerationModel{
		modelPath:   opts.ModelPath,
		contextSize: uint32(opts.ContextSize),
		batchSize:   uint32(opts.BatchSize),
		gpuLayers:   gpuLayers,
		usingGPU:    usingGPU,
	}, nil
}

// GenerateParams configures text generation behavior.
type GenerateParams struct {
	MaxTokens   int32
	Temperature float32
	TopP        float32
	TopK        int32
}

// Generate generates text from a prompt.
//
// GPU Fallback: If GPU fails during inference, gracefully falls back to CPU.
//
// Example:
//
//	params := localllm.GenerateParams{
//		MaxTokens:   100,
//		Temperature: 0.7,
//		TopP:        0.9,
//		TopK:        40,
//	}
//	text, err := model.Generate(ctx, "Hello", params)
func (g *GenerationModel) Generate(ctx context.Context, prompt string, params GenerateParams) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Recover from panics originating in FFI/native code and log stacktrace
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[PANIC][localllm] Generate recovered panic: %v\n%s", r, debug.Stack())
		}
	}()

	// Load model with GPU configuration
	modelParams := llama.ModelDefaultParams()
	modelParams.NGpuLayers = g.gpuLayers

	model, err := llama.ModelLoadFromFile(g.modelPath, modelParams)
	if err != nil {
		// Try CPU fallback
		if g.gpuLayers != 0 {
			modelParams.NGpuLayers = 0
			model, err = llama.ModelLoadFromFile(g.modelPath, modelParams)
			if err != nil {
				return "", fmt.Errorf("failed to load model (GPU and CPU both failed): %w", err)
			}
		} else {
			return "", fmt.Errorf("failed to load model: %w", err)
		}
	}
	defer llama.ModelFree(model)

	// Create context for generation (not embeddings)
	ctxParams := llama.ContextDefaultParams()
	ctxParams.Embeddings = 0       // Disable embeddings (uint8 bool)
	ctxParams.NCtx = g.contextSize // Use configured context size
	ctxParams.NBatch = g.batchSize // Use configured batch size
	lctx, err := llama.InitFromModel(model, ctxParams)
	if err != nil {
		return "", fmt.Errorf("failed to create context: %w", err)
	}
	defer llama.Free(lctx)

	// Tokenize prompt
	vocab := llama.ModelGetVocab(model)
	tokens := llama.Tokenize(vocab, prompt, true, false)

	// Check if prompt fits in batch
	if len(tokens) > int(g.batchSize) {
		return "", fmt.Errorf("prompt too long: %d tokens exceeds batch size %d", len(tokens), g.batchSize)
	}

	// Setup sampler using yzma helper (ensures model-aware samplers are initialized)
	sp := llama.DefaultSamplerParams()
	sp.Temp = params.Temperature
	sp.TopK = params.TopK
	sp.TopP = params.TopP
	sampler := llama.NewSampler(model, llama.DefaultSamplers, sp)
	defer llama.SamplerFree(sampler)

	// Generate tokens
	var result string

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	batch := llama.BatchGetOne(tokens)
	if _, err := llama.Decode(lctx, batch); err != nil {
		return "", fmt.Errorf("prompt decode failed: %w", err)
	}
	// NOTE: BatchGetOne returns stack-allocated batch - do NOT call BatchFree

	runtime.ReadMemStats(&m)

	for i := int32(0); i < params.MaxTokens; i++ {
		// Sample next token from the last position in context
		token := llama.SamplerSample(sampler, lctx, -1)
		// Check for end-of-sequence
		if llama.VocabIsEOG(vocab, token) {
			break
		}

		// Convert token to string
		buf := make([]byte, 64)
		pieceLen := llama.TokenToPiece(vocab, token, buf, 0, true)
		if pieceLen > 0 {
			result += string(buf[:pieceLen])
		}

		// Decode ONLY the single new token for next iteration
		// This is the pattern from yzma examples: batch = llama.BatchGetOne([]llama.Token{token})
		singleBatch := llama.BatchGetOne([]llama.Token{token})
		if _, err := llama.Decode(lctx, singleBatch); err != nil {
			return result, fmt.Errorf("decode failed at token %d: %w", i, err)
		}
		// NOTE: BatchGetOne returns stack-allocated batch - do NOT call BatchFree

		// Check context cancellation
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}
	}

	return result, nil
}

// GenerateStream generates text and streams tokens via callback.
//
// GPU Fallback: If GPU fails during inference, gracefully falls back to CPU.
//
// Example:
//
//	err := model.GenerateStream(ctx, "Hello", params, func(token string) error {
//		fmt.Print(token)
//		return nil
//	})
func (g *GenerationModel) GenerateStream(ctx context.Context, prompt string, params GenerateParams, callback func(token string) error) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Recover from panics originating in FFI/native code and log stacktrace
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[PANIC][localllm] GenerateStream recovered panic: %v\n%s", r, debug.Stack())
		}
	}()

	// Load model with GPU configuration
	modelParams := llama.ModelDefaultParams()
	modelParams.NGpuLayers = g.gpuLayers

	model, err := llama.ModelLoadFromFile(g.modelPath, modelParams)
	if err != nil {
		// Try CPU fallback
		if g.gpuLayers != 0 {
			modelParams.NGpuLayers = 0
			model, err = llama.ModelLoadFromFile(g.modelPath, modelParams)
			if err != nil {
				return fmt.Errorf("failed to load model (GPU and CPU both failed): %w", err)
			}
		} else {
			return fmt.Errorf("failed to load model: %w", err)
		}
	}
	defer llama.ModelFree(model)

	// Create context
	ctxParams := llama.ContextDefaultParams()
	ctxParams.Embeddings = 0       // Disable embeddings
	ctxParams.NCtx = g.contextSize // Use configured context size
	ctxParams.NBatch = g.batchSize // Use configured batch size
	lctx, err := llama.InitFromModel(model, ctxParams)
	if err != nil {
		return fmt.Errorf("failed to create context: %w", err)
	}
	defer llama.Free(lctx)

	// Tokenize
	vocab := llama.ModelGetVocab(model)
	tokens := llama.Tokenize(vocab, prompt, true, false)

	// Check if prompt fits in batch
	if len(tokens) > int(g.batchSize) {
		return fmt.Errorf("prompt too long: %d tokens exceeds batch size %d", len(tokens), g.batchSize)
	}

	// Setup sampler using yzma helper (ensures model-aware samplers are initialized)
	sp := llama.DefaultSamplerParams()
	sp.Temp = params.Temperature
	sp.TopK = params.TopK
	sp.TopP = params.TopP
	sampler := llama.NewSampler(model, llama.DefaultSamplers, sp)
	defer llama.SamplerFree(sampler)

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	batch := llama.BatchGetOne(tokens)
	if _, err := llama.Decode(lctx, batch); err != nil {
		return fmt.Errorf("prompt decode failed: %w", err)
	}
	// NOTE: BatchGetOne returns stack-allocated batch - do NOT call BatchFree

	runtime.ReadMemStats(&m)

	for i := int32(0); i < params.MaxTokens; i++ {
		// Sample next token from the last position in context
		token := llama.SamplerSample(sampler, lctx, -1)
		if llama.VocabIsEOG(vocab, token) {
			break
		}

		buf := make([]byte, 64)
		pieceLen := llama.TokenToPiece(vocab, token, buf, 0, true)
		if pieceLen > 0 {
			tokenStr := string(buf[:pieceLen])
			if err := callback(tokenStr); err != nil {
				return err
			}
		}

		// Decode ONLY the single new token for next iteration
		// This is the pattern from yzma examples: batch = llama.BatchGetOne([]llama.Token{token})
		singleBatch := llama.BatchGetOne([]llama.Token{token})
		if _, err := llama.Decode(lctx, singleBatch); err != nil {
			return fmt.Errorf("decode failed at token %d: %w", i, err)
		}
		// NOTE: BatchGetOne returns stack-allocated batch - do NOT call BatchFree

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	return nil
}

// ModelPath returns the path to the model file.
func (g *GenerationModel) ModelPath() string {
	return g.modelPath
}

// UsingGPU returns true if the model is using GPU acceleration.
func (g *GenerationModel) UsingGPU() bool {
	return g.usingGPU
}

// GPULayers returns the number of layers offloaded to GPU.
func (g *GenerationModel) GPULayers() int {
	return int(g.gpuLayers)
}

// Close frees resources (no-op with yzma).
func (g *GenerationModel) Close() error {
	return nil
}
