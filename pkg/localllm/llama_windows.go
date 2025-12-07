//go:build cgo && windows

// Package localllm provides CGO bindings to llama.cpp for local GGUF model inference.
//
// This is the Windows implementation with automatic GPU detection.
//
// GPU Support (Dynamic Backend Loading):
//   - If ggml-cuda.dll is present in lib/llama, CUDA acceleration is used
//   - Falls back to CPU-only if CUDA DLL is not found
//   - No recompilation needed to switch between CPU/GPU
//
// CPU Optimizations:
//   - AVX/AVX2/FMA SIMD instructions
//   - Multi-threaded inference
//   - Memory-mapped model loading
//
// Build Requirements:
//   - MinGW-w64 (GCC for Windows)
//   - Pre-built llama.cpp libraries
//   - Optional: ggml-cuda.dll for GPU acceleration
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

/*
#cgo CFLAGS: -I${SRCDIR}/../../lib/llama

// Windows - link against MinGW-built llama.cpp libraries with dynamic backend support
// Libraries: All ggml/llama static libraries in correct order
#cgo LDFLAGS: -L${SRCDIR}/../../lib/llama/windows_amd64_cuda/build/src
#cgo LDFLAGS: -L${SRCDIR}/../../lib/llama/windows_amd64_cuda/build/ggml/src
#cgo LDFLAGS: -L${SRCDIR}/../../lib/llama/windows_amd64_cuda/build/common

// Link libraries in dependency order (most dependent first)
// Use -l:filename.a for files without lib prefix (ggml*.a)
#cgo LDFLAGS: -lcommon -lllama -l:ggml.a -l:ggml-cpu.a -l:ggml-base.a

// MinGW runtime libraries (OpenMP needed for parallel processing)
#cgo LDFLAGS: -lstdc++ -lgomp -static-libgcc -static-libstdc++ -lm -lpthread

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "llama.h"
#include "ggml.h"
#include "ggml-backend.h"

// Initialize backend once (handles GPU detection and dynamic backend loading)
static int initialized = 0;
static int cuda_loaded = 0;

void init_backend() {
    if (!initialized) {
        llama_backend_init();

        // Try to load GPU backends from lib/llama directory
        // This loads ggml-cuda.dll if present, enabling GPU acceleration
        // The function ggml_backend_load_all_from_path is available when GGML_BACKEND_DL=ON
        ggml_backend_load_all_from_path("lib/llama");

        // Check if CUDA backend is now available
        size_t backend_count = ggml_backend_dev_count();
        fprintf(stderr, "[localllm] Found %zu compute backend(s)\n", backend_count);

        for (size_t i = 0; i < backend_count; i++) {
            ggml_backend_dev_t dev = ggml_backend_dev_get(i);
            const char* name = ggml_backend_dev_name(dev);
            const char* desc = ggml_backend_dev_description(dev);
            fprintf(stderr, "[localllm] Backend %zu: %s - %s\n", i, name, desc);

            // Check if this is a CUDA backend
            if (name && (strstr(name, "CUDA") != NULL || strstr(name, "cuda") != NULL)) {
                cuda_loaded = 1;
            }
        }

        if (cuda_loaded) {
            fprintf(stderr, "[localllm] CUDA GPU acceleration enabled!\n");
        } else {
            fprintf(stderr, "[localllm] Using CPU backend\n");
        }

        initialized = 1;
    }
}

// Check if CUDA backend was loaded
int is_cuda_available() {
    return cuda_loaded;
}

// Get number of layers in model (for GPU offload calculation)
int get_n_layers(struct llama_model* model) {
    return llama_model_n_layer(model);
}

// Load model with optimal GPU settings
// n_gpu_layers: -1 = all layers on GPU, 0 = CPU only, N = N layers on GPU
struct llama_model* load_model(const char* path, int n_gpu_layers) {
    init_backend();
    struct llama_model_params params = llama_model_default_params();

    // Memory mapping for low memory usage
    params.use_mmap = 1;

    // Device selection - NULL means use all available devices
    // (new in b7285, explicit for clarity)
    params.devices = NULL;

    // GPU layer offloading
    // -1 means offload all layers (determined after loading)
    // For now, use a high number that will be clamped by llama.cpp
    if (n_gpu_layers < 0) {
        params.n_gpu_layers = 999;  // Will be clamped to actual layer count
    } else {
        params.n_gpu_layers = n_gpu_layers;
    }

    return llama_model_load_from_file(path, params);
}

// Create embedding context with CUDA optimizations
struct llama_context* create_context(struct llama_model* model, int n_ctx, int n_batch, int n_threads) {
    struct llama_context_params params = llama_context_default_params();

    // Context size for tokenization
    params.n_ctx = n_ctx;

    // Batch sizes for processing
    params.n_batch = n_batch;      // Logical batch size
    params.n_ubatch = n_batch;     // Physical batch size (same for embeddings)

    // CPU threading (used for CPU-only layers or fallback)
    params.n_threads = n_threads;
    params.n_threads_batch = n_threads;

    // Enable embeddings mode
    params.embeddings = 1;
    params.pooling_type = LLAMA_POOLING_TYPE_MEAN;

    // Set attention type for embedding models (non-causal, BERT-style)
    // Embedding models use bidirectional attention unlike causal LLMs
    params.attention_type = LLAMA_ATTENTION_TYPE_NON_CAUSAL;

    // Flash attention - major speedup on CUDA (b7285+ uses flash_attn_type enum)
    params.flash_attn_type = LLAMA_FLASH_ATTN_TYPE_ENABLED;

    // logits_all removed in newer llama.cpp - controlled per-batch now
    // We set batch.logits[i] = 1 in embed() function instead

    return llama_init_from_model(model, params);
}

// Tokenize using model's vocab
int tokenize(struct llama_model* model, const char* text, int text_len, int32_t* tokens, int max_tokens) {
    const struct llama_vocab* vocab = llama_model_get_vocab(model);
    // add_bos=true, special=true for proper embedding format
    return llama_tokenize(vocab, text, text_len, tokens, max_tokens, 1, 1);
}

// Generate embedding with GPU acceleration
int embed(struct llama_context* ctx, int32_t* tokens, int n_tokens, float* out, int n_embd) {
    // Clear memory before each embedding (not persistent for embeddings)
    // In b7285+, KV cache was replaced with memory API
    llama_memory_t mem = llama_get_memory(ctx);
    llama_memory_clear(mem, 1);

    // Create batch
    struct llama_batch batch = llama_batch_init(n_tokens, 0, 1);
    for (int i = 0; i < n_tokens; i++) {
        batch.token[i] = tokens[i];
        batch.pos[i] = i;
        batch.n_seq_id[i] = 1;
        batch.seq_id[i][0] = 0;
        batch.logits[i] = 1;  // Enable output for embedding extraction
    }
    batch.n_tokens = n_tokens;

    // Encode (for embedding models, use llama_encode not llama_decode)
    // llama_decode is for causal/generation models, llama_encode is for BERT-style models
    if (llama_encode(ctx, batch) != 0) {
        llama_batch_free(batch);
        return -1;
    }

    // Get pooled embedding
    float* embd = llama_get_embeddings_seq(ctx, 0);
    if (!embd) {
        llama_batch_free(batch);
        return -2;
    }

    // Copy to output
    memcpy(out, embd, n_embd * sizeof(float));
    llama_batch_free(batch);
    return 0;
}

// Get embedding dimensions
int get_n_embd(struct llama_model* model) {
    return llama_model_n_embd(model);
}

// Free resources
void free_ctx(struct llama_context* ctx) { if (ctx) llama_free(ctx); }
void free_model(struct llama_model* model) { if (model) llama_model_free(model); }
*/
import "C"

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"unsafe"

	"github.com/orneryd/nornicdb/pkg/math/vector"
)

// Model wraps a GGUF model for embedding generation.
//
// Thread-safe: The Embed and EmbedBatch methods can be called concurrently,
// but operations are serialized internally via mutex to prevent race conditions
// with the underlying C context.
type Model struct {
	model     *C.struct_llama_model
	ctx       *C.struct_llama_context
	dims      int
	modelDesc string
	mu        sync.Mutex
}

// Options configures model loading and inference.
//
// Fields:
//   - ModelPath: Path to .gguf model file
//   - ContextSize: Max context size for tokenization (default: 512)
//   - BatchSize: Batch size for processing (default: 512)
//   - Threads: CPU threads for inference (default: NumCPU/2, min 4)
//   - GPULayers: GPU layer offload (-1=auto/all, 0=CPU only, N=N layers)
type Options struct {
	ModelPath   string
	ContextSize int
	BatchSize   int
	Threads     int
	GPULayers   int
}

// DefaultOptions returns options optimized for embedding generation.
//
// GPU is enabled by default (-1 = auto-detect and use all layers).
// Set GPULayers to 0 to force CPU-only mode.
//
// For NVIDIA GPUs, this enables full CUDA acceleration with:
//   - Flash attention
//   - Full model offload to GPU
//   - Optimized tensor operations
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
		ContextSize: 512, // Enough for most embedding inputs
		BatchSize:   512, // Matches context for efficient processing
		Threads:     threads,
		GPULayers:   -1, // Auto: offload all layers to GPU
	}
}

// LoadModel loads a GGUF model for embedding generation.
//
// The model is memory-mapped for low memory footprint. GPU layers are
// automatically offloaded based on Options.GPULayers:
//   - -1: Auto-detect GPU and offload all layers (recommended)
//   - 0: CPU only (no GPU offload)
//   - N: Offload N layers to GPU
//
// CUDA Optimization (NVIDIA GPU):
//
// When running with CUDA support:
//   - All model layers are offloaded to GPU by default
//   - Flash attention is enabled for faster inference
//   - VRAM is used for tensor storage
//   - Typical speedup: 5-10x over CPU-only
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
//	fmt.Printf("Model loaded: %d dimensions\n", model.Dimensions())
func LoadModel(opts Options) (*Model, error) {
	cPath := C.CString(opts.ModelPath)
	defer C.free(unsafe.Pointer(cPath))

	model := C.load_model(cPath, C.int(opts.GPULayers))
	if model == nil {
		return nil, fmt.Errorf("failed to load model: %s", opts.ModelPath)
	}

	ctx := C.create_context(model, C.int(opts.ContextSize), C.int(opts.BatchSize), C.int(opts.Threads))
	if ctx == nil {
		C.free_model(model)
		return nil, fmt.Errorf("failed to create context for: %s", opts.ModelPath)
	}

	return &Model{
		model:     model,
		ctx:       ctx,
		dims:      int(C.get_n_embd(model)),
		modelDesc: opts.ModelPath, // Use path as description
	}, nil
}

// Embed generates a normalized embedding vector for the given text.
//
// The returned vector is L2-normalized (unit length), suitable for
// cosine similarity calculations.
//
// Concurrency:
//
// Operations are serialized via mutex because llama.cpp contexts are NOT
// thread-safe. The C.embed call holds the lock for the duration of GPU/CPU
// inference (~5-50ms depending on text length and hardware).
//
// For higher throughput under concurrent load, create multiple Model instances
// (each with its own GPU context). The GPU can process multiple contexts
// efficiently via kernel scheduling.
//
// GPU Acceleration:
//
// On NVIDIA GPUs with CUDA, the embedding is computed on the GPU:
//  1. Tokenization (CPU)
//  2. Model inference (GPU - flash attention enabled)
//  3. Pooling (GPU)
//  4. Normalization (CPU)
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

	// Lock required: llama.cpp contexts are not thread-safe.
	// For higher concurrency, use multiple Model instances.
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Tokenize
	cText := C.CString(text)
	defer C.free(unsafe.Pointer(cText))

	tokens := make([]C.int, 512)
	n := C.tokenize(m.model, cText, C.int(len(text)), &tokens[0], 512)
	if n < 0 {
		return nil, fmt.Errorf("tokenization failed for text of length %d", len(text))
	}
	if n == 0 {
		return nil, fmt.Errorf("text produced no tokens")
	}

	// Generate embedding (GPU-accelerated on CUDA)
	emb := make([]float32, m.dims)
	result := C.embed(m.ctx, (*C.int)(&tokens[0]), n, (*C.float)(&emb[0]), C.int(m.dims))
	if result != 0 {
		return nil, fmt.Errorf("embedding generation failed (code: %d)", result)
	}

	// Normalize to unit vector for cosine similarity
	vector.NormalizeInPlace(emb)
	return emb, nil
}

// EmbedBatch generates normalized embeddings for multiple texts.
//
// Each text is processed sequentially through the GPU. For maximum throughput
// with many texts, consider parallel processing with multiple Model instances.
//
// Note: True batch processing (multiple texts in single GPU kernel) would
// require llama.cpp changes. Current implementation is efficient for
// moderate batch sizes due to GPU kernel reuse.
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
	for i, t := range texts {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		emb, err := m.Embed(ctx, t)
		if err != nil {
			return nil, fmt.Errorf("text %d: %w", i, err)
		}
		results[i] = emb
	}
	return results, nil
}

// Dimensions returns the embedding vector size.
//
// This is determined by the model architecture:
//   - BGE-M3: 1024 dimensions
//   - E5-large: 1024 dimensions
//   - Jina-v2-base-code: 768 dimensions
func (m *Model) Dimensions() int { return m.dims }

// ModelDescription returns a human-readable description of the loaded model.
func (m *Model) ModelDescription() string { return m.modelDesc }

// Close releases all resources associated with the model.
//
// After Close is called, the Model must not be used.
// This properly releases GPU memory on CUDA.
func (m *Model) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ctx != nil {
		C.free_ctx(m.ctx)
		m.ctx = nil
	}
	if m.model != nil {
		C.free_model(m.model)
		m.model = nil
	}
	return nil
}
