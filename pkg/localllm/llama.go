//go:build cgo && !nolocalllm && (darwin || linux)

// Package localllm provides CGO bindings to llama.cpp for local GGUF model inference.
//
// This package enables NornicDB to run embedding models directly without external
// services like Ollama. It uses llama.cpp compiled as a static library with
// GPU acceleration (Metal on macOS, CUDA on Linux) and CPU fallback.
//
// Metal Optimizations (Apple Silicon):
//   - Flash attention for faster inference
//   - Full model GPU offload by default
//   - Unified memory utilization
//   - SIMD-optimized CPU fallback
//
// Features:
//   - GPU-first with automatic CPU fallback
//   - Memory-mapped model loading for low memory footprint
//   - Thread-safe embedding generation
//   - Batch embedding support
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

// Linux with CUDA (GPU primary) - includes CUDA driver (-lcuda), runtime (-lcudart), cuBLAS, and OpenMP (-lgomp)
#cgo linux,amd64,cuda LDFLAGS: -L${SRCDIR}/../../lib/llama -lllama_linux_amd64_cuda -L/usr/local/cuda/lib64 -lcudart -lcublas -lcuda -lgomp -lm -lstdc++ -lpthread
// Linux CPU fallback (ggml-cpu uses OpenMP => link libgomp)
#cgo linux,amd64,!cuda LDFLAGS: -L${SRCDIR}/../../lib/llama -lllama_linux_amd64 -lgomp -lm -lstdc++ -lpthread

#cgo linux,arm64 LDFLAGS: -L${SRCDIR}/../../lib/llama -lllama_linux_arm64 -lgomp -lm -lstdc++ -lpthread

// macOS with Metal (GPU primary on Apple Silicon)
// Set deployment target to macOS 26.0 to match llama library build (eliminates linker warnings)
#cgo darwin,arm64 LDFLAGS: -mmacosx-version-min=26.0 -L${SRCDIR}/../../lib/llama -lllama_darwin_arm64 -lm -lc++ -framework Accelerate -framework Metal -framework MetalPerformanceShaders -framework Foundation
#cgo darwin,amd64 LDFLAGS: -mmacosx-version-min=26.0 -L${SRCDIR}/../../lib/llama -lllama_darwin_amd64 -lm -lc++ -framework Accelerate

// Windows with dynamic backend loading (CUDA DLL loaded at runtime)
#cgo windows,amd64 LDFLAGS: -L${SRCDIR}/../../lib/llama -lllama_windows_amd64 -lm -lstdc++

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include "llama.h"

// Initialize backend once (handles GPU detection)
static int initialized = 0;

void init_backend() {
    if (!initialized) {
        // Keep backend init minimal for API compatibility across llama.cpp versions.
        // Verbose tensor-load logs are still suppressed via temporary stderr redirect
        // in load_model() below.
        llama_backend_init();

        #ifdef _WIN32
            // On Windows, load GPU backends (CUDA DLL) from lib directory
            // This enables GPU acceleration without linking CUDA at compile time
            ggml_backend_load_all_from_path("lib/llama");
        #endif

        initialized = 1;
    }
}

// Get number of layers in model (for GPU offload calculation)
int get_n_layers(struct llama_model* model) {
    return llama_model_n_layer(model);
}

// Load model with optimal GPU settings
// n_gpu_layers: -1 = all layers on GPU, 0 = CPU only, N = N layers on GPU
struct llama_model* load_model(const char* path, int n_gpu_layers) {
    init_backend();

    // Suppress verbose tensor loading logs by redirecting stderr to /dev/null (or NUL on Windows)
    // Save original stderr file descriptor
    int original_stderr = -1;
    FILE* dev_null = NULL;
    #ifdef _WIN32
        dev_null = fopen("NUL", "w");
    #else
        dev_null = fopen("/dev/null", "w");
    #endif
    if (dev_null) {
        original_stderr = dup(2);
        dup2(fileno(dev_null), 2);  // Redirect stderr (fd 2) to /dev/null or NUL
    }

    struct llama_model_params params = llama_model_default_params();

    // Memory mapping for low memory usage
    params.use_mmap = 1;

    // Device selection - NULL means use all available devices
    // (present in modern llama.cpp releases, explicit for clarity)
    params.devices = NULL;

    // GPU layer offloading
    // -1 means offload all layers (determined after loading)
    // For now, use a high number that will be clamped by llama.cpp
    if (n_gpu_layers < 0) {
        params.n_gpu_layers = 999;  // Will be clamped to actual layer count
    } else {
        params.n_gpu_layers = n_gpu_layers;
    }

    struct llama_model* model = llama_model_load_from_file(path, params);

    // Restore original stderr
    if (dev_null && original_stderr >= 0) {
        dup2(original_stderr, 2);  // Restore stderr
        close(original_stderr);
        fclose(dev_null);
    }

    return model;
}

// Create embedding context with Metal/GPU optimizations
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

    // Flash attention - major speedup on Metal and CUDA
    // Auto-detect best setting based on hardware and model
    params.flash_attn_type = LLAMA_FLASH_ATTN_TYPE_AUTO;

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
    // KV cache API renamed to "memory" in newer llama.cpp releases
    // Second arg (true) clears the data
    llama_memory_clear(llama_get_memory(ctx), 1);

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

// Get model training context size from metadata.
int get_n_ctx_train(struct llama_model* model) {
    return llama_model_n_ctx_train(model);
}

// Free resources
void free_ctx(struct llama_context* ctx) { if (ctx) llama_free(ctx); }
void free_model(struct llama_model* model) { if (model) llama_model_free(model); }

// ============================================================================
// TEXT GENERATION (for Heimdall SLM)
// ============================================================================

// Create context for text generation (different params than embeddings)
struct llama_context* create_gen_context(struct llama_model* model, int n_ctx, int n_batch, int n_threads) {
    struct llama_context_params params = llama_context_default_params();

    params.n_ctx = n_ctx;
    params.n_batch = n_batch;
    params.n_ubatch = n_batch;
    params.n_threads = n_threads;
    params.n_threads_batch = n_threads;

	// Generation mode: keep the context params as close as possible to
	// llama.cpp defaults and only flip the fields we actually need.
    params.embeddings = 0;
    params.pooling_type = LLAMA_POOLING_TYPE_UNSPECIFIED;
    params.attention_type = LLAMA_ATTENTION_TYPE_CAUSAL;

    // logits_all removed - controlled per-batch now

    return llama_init_from_model(model, params);
}

// Decode a batch of tokens (for generation)
int gen_decode(struct llama_context* ctx, int32_t* tokens, int n_tokens, int start_pos) {
    llama_memory_clear(llama_get_memory(ctx), 1);

    struct llama_batch batch = llama_batch_init(n_tokens, 0, 1);
    for (int i = 0; i < n_tokens; i++) {
        batch.token[i] = tokens[i];
        batch.pos[i] = start_pos + i;
        batch.n_seq_id[i] = 1;
        batch.seq_id[i][0] = 0;
        batch.logits[i] = (i == n_tokens - 1) ? 1 : 0;  // Only last token needs logits
    }
    batch.n_tokens = n_tokens;

    int result = llama_decode(ctx, batch);
    llama_batch_free(batch);
    return result;
}

// Decode single token (for autoregressive generation)
int gen_decode_token(struct llama_context* ctx, int32_t token, int pos) {
    struct llama_batch batch = llama_batch_init(1, 0, 1);
    batch.token[0] = token;
    batch.pos[0] = pos;
    batch.n_seq_id[0] = 1;
    batch.seq_id[0][0] = 0;
    batch.logits[0] = 1;
    batch.n_tokens = 1;

    int result = llama_decode(ctx, batch);
    llama_batch_free(batch);
    return result;
}

// Sample next token using sampler chain
int32_t sample_token(struct llama_context* ctx, struct llama_model* model, float temperature, float top_p, int top_k) {
    // Create sampler chain
    struct llama_sampler_chain_params sparams = llama_sampler_chain_default_params();
    struct llama_sampler* smpl = llama_sampler_chain_init(sparams);

    // Add samplers to chain: top-k -> top-p -> temperature -> dist
    if (top_k > 0) {
        llama_sampler_chain_add(smpl, llama_sampler_init_top_k(top_k));
    }
    if (top_p < 1.0f) {
        llama_sampler_chain_add(smpl, llama_sampler_init_top_p(top_p, 1));
    }
    if (temperature > 0.0f) {
        llama_sampler_chain_add(smpl, llama_sampler_init_temp(temperature));
    }
    llama_sampler_chain_add(smpl, llama_sampler_init_dist(42));

    // Sample from context (-1 means last token's logits)
    int32_t token = llama_sampler_sample(smpl, ctx, -1);

    llama_sampler_free(smpl);
    return token;
}

// Detokenize single token to string
int detokenize(struct llama_model* model, int32_t token, char* buf, int buf_size) {
    const struct llama_vocab* vocab = llama_model_get_vocab(model);
    return llama_token_to_piece(vocab, token, buf, buf_size, 0, 1);
}

// Check if token is EOS
int is_eos(struct llama_model* model, int32_t token) {
    const struct llama_vocab* vocab = llama_model_get_vocab(model);
    return llama_vocab_is_eog(vocab, token);
}

// ============================================================================
// ROBUSTNESS & ERROR HANDLING (using correct llama.cpp API)
// ============================================================================

// Helper to get vocab size using correct API
int get_vocab_size(struct llama_model* model) {
    const struct llama_vocab* vocab = llama_model_get_vocab(model);
    return llama_vocab_n_tokens(vocab);
}

// Check if context is healthy (not NULL, has valid state)
int ctx_is_healthy(struct llama_context* ctx) {
    if (!ctx) return 0;
    // Verify context has a valid n_ctx — this works regardless of
    // whether the KV cache is unified or per-layer (kv_unified=false).
    // llama_get_memory() returns NULL for non-unified caches, so we
    // cannot use it as a health check.
    return llama_n_ctx(ctx) > 0 ? 1 : 0;
}

// Check if model is healthy
int model_is_healthy(struct llama_model* model) {
    if (!model) return 0;
    int n_vocab = get_vocab_size(model);
    int n_embd = llama_model_n_embd(model);
    return (n_vocab > 0 && n_embd > 0) ? 1 : 0;
}

// Get context size
int get_ctx_size(struct llama_context* ctx) {
    return llama_n_ctx(ctx);
}

// Validate tokens before decode
int validate_tokens(struct llama_model* model, int32_t* tokens, int n_tokens) {
    if (!model || !tokens || n_tokens <= 0) return -1;
    int vocab_size = get_vocab_size(model);
    for (int i = 0; i < n_tokens; i++) {
        if (tokens[i] < 0 || tokens[i] >= vocab_size) {
            return i;  // Return index of invalid token
        }
    }
    return -1;  // All tokens valid
}

// Safe decode with validation
int safe_gen_decode(struct llama_context* ctx, struct llama_model* model,
                    int32_t* tokens, int n_tokens, int start_pos, char* error_buf, int error_buf_size) {
    if (!ctx) {
        snprintf(error_buf, error_buf_size, "context is NULL");
        return -100;
    }
    if (!model) {
        snprintf(error_buf, error_buf_size, "model is NULL");
        return -101;
    }
    if (!tokens) {
        snprintf(error_buf, error_buf_size, "tokens is NULL");
        return -102;
    }
    if (n_tokens <= 0) {
        snprintf(error_buf, error_buf_size, "n_tokens must be positive, got %d", n_tokens);
        return -103;
    }

    // Context size check
    int ctx_size = llama_n_ctx(ctx);
    if (start_pos + n_tokens > ctx_size) {
        snprintf(error_buf, error_buf_size,
            "tokens exceed context: pos=%d + n=%d > ctx=%d",
            start_pos, n_tokens, ctx_size);
        return -104;
    }

    // Token validation
    int invalid_idx = validate_tokens(model, tokens, n_tokens);
    if (invalid_idx >= 0) {
        int vocab_size = get_vocab_size(model);
        snprintf(error_buf, error_buf_size,
            "invalid token at index %d: value=%d, vocab_size=%d",
            invalid_idx, tokens[invalid_idx], vocab_size);
        return -105;
    }

    // Context health check
    if (!ctx_is_healthy(ctx)) {
        snprintf(error_buf, error_buf_size, "context health check failed");
        return -106;
    }

    // Clear memory for fresh decode
    llama_memory_clear(llama_get_memory(ctx), 1);

    // Create batch
    struct llama_batch batch = llama_batch_init(n_tokens, 0, 1);
    if (!batch.token) {
        snprintf(error_buf, error_buf_size, "failed to allocate batch");
        return -107;
    }

    for (int i = 0; i < n_tokens; i++) {
        batch.token[i] = tokens[i];
        batch.pos[i] = start_pos + i;
        batch.n_seq_id[i] = 1;
        batch.seq_id[i][0] = 0;
        batch.logits[i] = (i == n_tokens - 1) ? 1 : 0;
    }
    batch.n_tokens = n_tokens;

    // Actual decode
    int result = llama_decode(ctx, batch);
    llama_batch_free(batch);

    if (result != 0) {
        snprintf(error_buf, error_buf_size, "llama_decode returned %d", result);
        return result;
    }

    error_buf[0] = '\0';
    return 0;
}

// Safe single token decode
int safe_gen_decode_token(struct llama_context* ctx, struct llama_model* model,
                          int32_t token, int pos, char* error_buf, int error_buf_size) {
    if (!ctx) {
        snprintf(error_buf, error_buf_size, "context is NULL");
        return -100;
    }
    if (!model) {
        snprintf(error_buf, error_buf_size, "model is NULL");
        return -101;
    }

    // Validate token
    int vocab_size = get_vocab_size(model);
    if (token < 0 || token >= vocab_size) {
        snprintf(error_buf, error_buf_size,
            "invalid token %d (vocab_size=%d)", token, vocab_size);
        return -105;
    }

    // Check position
    int ctx_size = llama_n_ctx(ctx);
    if (pos >= ctx_size) {
        snprintf(error_buf, error_buf_size,
            "position %d exceeds context size %d", pos, ctx_size);
        return -104;
    }

    struct llama_batch batch = llama_batch_init(1, 0, 1);
    if (!batch.token) {
        snprintf(error_buf, error_buf_size, "failed to allocate batch");
        return -107;
    }

    batch.token[0] = token;
    batch.pos[0] = pos;
    batch.n_seq_id[0] = 1;
    batch.seq_id[0][0] = 0;
    batch.logits[0] = 1;
    batch.n_tokens = 1;

    int result = llama_decode(ctx, batch);
    llama_batch_free(batch);

    if (result != 0) {
        snprintf(error_buf, error_buf_size, "llama_decode returned %d", result);
        return result;
    }

    error_buf[0] = '\0';
    return 0;
}

// Get memory usage estimate (returns 0 if not available)
size_t get_model_memory_size(struct llama_model* model) {
    if (!model) return 0;
    // This is an approximation based on model parameters
    int n_vocab = get_vocab_size(model);
    int n_embd = llama_model_n_embd(model);
    int n_layer = llama_model_n_layer(model);
    // Rough estimate: vocab embeddings + layer params
    return (size_t)(n_vocab * n_embd * 4) + (size_t)(n_layer * n_embd * n_embd * 4 * 4);
}

// Reset context state (clear memory, etc.)
void reset_context(struct llama_context* ctx) {
    if (ctx) {
        llama_memory_clear(llama_get_memory(ctx), 1);
    }
}
*/
import "C"

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"strings"
	"sync"
	"unsafe"

	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/textchunk"
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
	maxTokens int
	modelDesc string
	mu        sync.Mutex
}

// Options configures model loading and inference.
//
// Fields:
//   - ModelPath: Path to .gguf model file
//   - ContextSize: Max context size for tokenization (default: auto from model cap, up to 8192 for embedding)
//   - BatchSize: Batch size for processing (default: match effective context size)
//   - Threads: CPU threads for inference (default: NumCPU/2, min 4)
//   - GPULayers: GPU layer offload (-1=auto/all, 0=CPU only, N=N layers)
type Options struct {
	ModelPath   string
	ContextSize int
	BatchSize   int
	Threads     int
	GPULayers   int
}

const (
	// defaultEmbeddingContextCap is the automatic context cap for embedding models.
	// bge-m3 GGUF supports 8192 tokens; we cap auto-defaults there and still clamp to model metadata.
	defaultEmbeddingContextCap = 8192
)

// modelLoadMu serializes llama.cpp model/context creation.
//
// Some backends (notably Metal init paths) are not robust when multiple models
// are initialized concurrently from separate goroutines during process startup
// (e.g. Heimdall + embedding model). Serializing load avoids startup crashes
// while keeping inference concurrency unchanged (per-model Embed already has
// its own mutex).
var modelLoadMu sync.Mutex

// DefaultOptions returns options optimized for embedding generation.
//
// GPU is enabled by default (-1 = auto-detect and use all layers).
// Set GPULayers to 0 to force CPU-only mode.
//
// For Apple Silicon, this enables full Metal GPU acceleration with:
//   - Flash attention
//   - Full model offload
//   - Unified memory optimization
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
		ContextSize: 0, // Auto: resolved from model metadata (capped by defaultEmbeddingContextCap)
		BatchSize:   0, // Auto: match effective context size
		Threads:     threads,
		GPULayers:   -1, // Auto: offload all layers to GPU
	}
}

func resolveEmbeddingContextAndBatch(opts Options, modelCtxTrain int) (ctxSize, batchSize int) {
	if opts.ContextSize > 0 {
		ctxSize = opts.ContextSize
	} else if modelCtxTrain > 0 {
		ctxSize = modelCtxTrain
		if ctxSize > defaultEmbeddingContextCap {
			ctxSize = defaultEmbeddingContextCap
		}
	} else {
		ctxSize = defaultEmbeddingContextCap
	}
	if modelCtxTrain > 0 && ctxSize > modelCtxTrain {
		ctxSize = modelCtxTrain
	}
	if ctxSize < 1 {
		ctxSize = 1
	}

	if opts.BatchSize > 0 {
		batchSize = opts.BatchSize
	} else {
		batchSize = ctxSize
	}
	if batchSize < 1 {
		batchSize = 1
	}
	if batchSize > ctxSize {
		batchSize = ctxSize
	}
	return ctxSize, batchSize
}

// LoadModel loads a GGUF model for embedding generation.
//
// The model is memory-mapped for low memory footprint. GPU layers are
// automatically offloaded based on Options.GPULayers:
//   - -1: Auto-detect GPU and offload all layers (recommended)
//   - 0: CPU only (no GPU offload)
//   - N: Offload N layers to GPU
//
// Metal Optimization (Apple Silicon):
//
// When running on Apple Silicon with Metal support compiled in:
//   - All model layers are offloaded to GPU by default
//   - Flash attention is enabled for faster inference
//   - Unified memory is utilized efficiently
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
	modelLoadMu.Lock()
	defer modelLoadMu.Unlock()

	cPath := C.CString(opts.ModelPath)
	defer C.free(unsafe.Pointer(cPath))

	model := C.load_model(cPath, C.int(opts.GPULayers))
	if model == nil {
		return nil, fmt.Errorf("failed to load model: %s", opts.ModelPath)
	}
	modelCtxTrain := int(C.get_n_ctx_train(model))
	ctxSize, batchSize := resolveEmbeddingContextAndBatch(opts, modelCtxTrain)
	ctx := C.create_context(model, C.int(ctxSize), C.int(batchSize), C.int(opts.Threads))
	if ctx == nil {
		C.free_model(model)
		return nil, fmt.Errorf("failed to create context for: %s", opts.ModelPath)
	}
	effectiveCtx := int(C.get_ctx_size(ctx))
	if effectiveCtx <= 0 {
		effectiveCtx = ctxSize
	}

	return &Model{
		model:     model,
		ctx:       ctx,
		dims:      int(C.get_n_embd(model)),
		maxTokens: effectiveCtx,
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
// On Apple Silicon with Metal, the embedding is computed on the GPU:
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
	tokenCap := m.maxTokens
	if tokenCap < 1 {
		tokenCap = defaultEmbeddingContextCap
	}
	tokens := make([]C.int, tokenCap)
	n := C.tokenize(m.model, cText, C.int(len(text)), &tokens[0], C.int(tokenCap))
	if n < 0 {
		required := -int(n)
		if required > tokenCap {
			return nil, fmt.Errorf("tokenization overflow: requires %d tokens, limit %d", required, tokenCap)
		}
		return nil, fmt.Errorf("tokenization failed for text of length %d", len(text))
	}
	if n == 0 {
		return nil, fmt.Errorf("text produced no tokens")
	}

	// Generate embedding (GPU-accelerated on Metal/CUDA)
	emb := make([]float32, m.dims)
	result := C.embed(m.ctx, (*C.int)(&tokens[0]), n, (*C.float)(&emb[0]), C.int(m.dims))
	if result != 0 {
		return nil, fmt.Errorf("embedding generation failed (code: %d)", result)
	}

	// Normalize to unit vector for cosine similarity
	vector.NormalizeInPlace(emb)
	return emb, nil
}

// CountTokens returns the exact tokenizer count for text using the model's vocab.
func (m *Model) CountTokens(text string) (int, error) {
	if text == "" {
		return 0, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	return m.countTokensLocked(text)
}

// ChunkText deterministically splits text using the model tokenizer so every
// returned chunk fits within the provided token cap.
func (m *Model) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	if text == "" {
		return []string{""}, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	return textchunk.ChunkByTokenCount(text, maxTokens, overlap, m.countTokensLocked)
}

func (m *Model) countTokensLocked(text string) (int, error) {
	cText := C.CString(text)
	defer C.free(unsafe.Pointer(cText))

	var scratch C.int
	n := C.tokenize(m.model, cText, C.int(len(text)), &scratch, 1)
	if n >= 0 {
		return int(n), nil
	}
	required := -int(n)
	if required > 0 {
		return required, nil
	}
	return 0, fmt.Errorf("tokenization failed for text of length %d", len(text))
}

// EmbedRaw returns the pooled output from the model without normalizing.
// Used by RerankerModel: when the GGUF outputs a single relevance logit (n_embd=1),
// emb[0] is the logit to pass through sigmoid. When n_embd>1 (e.g. 1024), the GGUF
// may be an embedding-style export without a classification head; emb[0] is then
// the first component of the [CLS] vector, which is not the true relevance score
// and can cluster around ~0.5 after sigmoid. Prefer a reranker GGUF that outputs
// 1 dimension (relevance logit).
func (m *Model) EmbedRaw(ctx context.Context, text string) ([]float32, error) {
	if text == "" {
		return nil, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	cText := C.CString(text)
	defer C.free(unsafe.Pointer(cText))
	tokenCap := m.maxTokens
	if tokenCap < 1 {
		tokenCap = defaultEmbeddingContextCap
	}
	tokens := make([]C.int, tokenCap)
	n := C.tokenize(m.model, cText, C.int(len(text)), &tokens[0], C.int(tokenCap))
	if n < 0 {
		required := -int(n)
		if required > tokenCap {
			return nil, fmt.Errorf("tokenization overflow: requires %d tokens, limit %d", required, tokenCap)
		}
		return nil, fmt.Errorf("tokenization failed for text of length %d", len(text))
	}
	if n == 0 {
		return nil, fmt.Errorf("text produced no tokens")
	}
	emb := make([]float32, m.dims)
	result := C.embed(m.ctx, (*C.int)(&tokens[0]), n, (*C.float)(&emb[0]), C.int(m.dims))
	if result != 0 {
		return nil, fmt.Errorf("embedding generation failed (code: %d)", result)
	}
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

// MaxTokens returns the effective tokenizer/input limit for this model context.
func (m *Model) MaxTokens() int { return m.maxTokens }

// ModelDescription returns a human-readable description of the loaded model.
func (m *Model) ModelDescription() string { return m.modelDesc }

// Close releases all resources associated with the model.
//
// After Close is called, the Model must not be used.
// This properly releases GPU memory on Metal/CUDA.
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

// ============================================================================
// RERANKER (BGE-style: query + document → single relevance score)
// ============================================================================
//
// RerankerModel uses the same BERT-style encoder as embedding models but is
// configured for classification/reranking: input "query \n\n document" is
// encoded and the model output (single dimension or first logit) is interpreted
// as a relevance score. Used for Stage-2 search reranking.

// RerankerModel wraps a GGUF model for reranking (query, document) pairs.
// It uses the embedding path: encode "query \n\n document" and treat the
// output as a relevance score (1-dim or first element with sigmoid).
type RerankerModel struct {
	model *Model
}

// LoadRerankerModel loads a GGUF reranker model (e.g. BGE-Reranker-v2-m3).
// Uses the same loader as embedding models; the model file must be a
// reranker/classification GGUF that outputs a single score per (query, doc) pair.
func LoadRerankerModel(opts Options) (*RerankerModel, error) {
	model, err := LoadModel(opts)
	if err != nil {
		return nil, err
	}
	return &RerankerModel{model: model}, nil
}

// Score returns a relevance score in [0, 1] for the (query, document) pair.
// Input format is "query \n\n document" (BGE-style). We use EmbedRaw (no
// normalization) so that when the GGUF outputs a single relevance logit (dims=1),
// we get the true logit; normalizing would turn it into ±1 and distort scores.
// Thread-safe: serialized via the underlying Model mutex.
func (r *RerankerModel) Score(ctx context.Context, query, document string) (float32, error) {
	if r == nil || r.model == nil {
		return 0, fmt.Errorf("reranker model not loaded")
	}
	combined := query + "\n\n" + document
	emb, err := r.model.EmbedRaw(ctx, combined)
	if err != nil {
		return 0, err
	}
	if len(emb) == 0 {
		return 0, fmt.Errorf("reranker returned empty embedding")
	}
	// Reranker GGUF should output 1 dim (relevance logit). If dims>1 we're using
	// the embedding-style pooled [CLS] and first component is not the true logit.
	logit := emb[0]
	score := sigmoid32(logit)
	if os.Getenv("NORNICDB_RERANK_DEBUG") == "1" {
		log.Printf("[rerank] dims=%d raw_logit=%.4f score=%.4f", len(emb), logit, score)
	}
	return score, nil
}

// sigmoid32 maps a logit to (0, 1) in a numerically stable way.
func sigmoid32(x float32) float32 {
	xf := float64(x)
	if xf >= 0 {
		return float32(1 / (1 + math.Exp(-xf)))
	}
	e := math.Exp(xf)
	return float32(e / (1 + e))
}

// Close releases the underlying model.
func (r *RerankerModel) Close() error {
	if r == nil || r.model == nil {
		return nil
	}
	return r.model.Close()
}

// ============================================================================
// TEXT GENERATION (for Heimdall SLM)
// ============================================================================

// GenerationModel wraps a GGUF model for text generation.
type GenerationModel struct {
	model       *C.struct_llama_model
	ctx         *C.struct_llama_context
	modelPath   string
	batchSize   int // Store for pre-flight validation
	contextSize int // Store for pre-flight validation
	mu          sync.Mutex
}

// GenerationOptions configures generation model loading.
type GenerationOptions struct {
	ModelPath   string
	ContextSize int // Max context window (default: 2048)
	BatchSize   int // Processing batch size (default: 512)
	Threads     int // CPU threads (default: NumCPU/2)
	GPULayers   int // GPU offload (-1=auto, 0=CPU)
}

// DefaultGenerationOptions returns sensible defaults for text generation.
func DefaultGenerationOptions(modelPath string) GenerationOptions {
	threads := runtime.NumCPU() / 2
	if threads < 4 {
		threads = 4
	}
	return GenerationOptions{
		ModelPath:   modelPath,
		ContextSize: 2048, // Larger context for conversations
		BatchSize:   512,
		Threads:     threads,
		GPULayers:   -1, // Auto GPU
	}
}

func resolveGenerationContextAndBatch(opts GenerationOptions, modelCtxTrain int) (ctxSize, batchSize int) {
	if opts.ContextSize > 0 {
		ctxSize = opts.ContextSize
	} else if modelCtxTrain > 0 {
		ctxSize = modelCtxTrain
	} else {
		ctxSize = 2048
	}
	if modelCtxTrain > 0 && ctxSize > modelCtxTrain {
		ctxSize = modelCtxTrain
	}
	if ctxSize < 1 {
		ctxSize = 1
	}

	if opts.BatchSize > 0 {
		batchSize = opts.BatchSize
	} else {
		batchSize = 512
	}
	if batchSize < 1 {
		batchSize = 1
	}
	if batchSize > ctxSize {
		batchSize = ctxSize
	}
	return ctxSize, batchSize
}

// LoadGenerationModel loads a GGUF model for text generation.
func LoadGenerationModel(opts GenerationOptions) (*GenerationModel, error) {
	modelLoadMu.Lock()
	defer modelLoadMu.Unlock()

	cPath := C.CString(opts.ModelPath)
	defer C.free(unsafe.Pointer(cPath))

	model := C.load_model(cPath, C.int(opts.GPULayers))
	if model == nil {
		return nil, fmt.Errorf("failed to load generation model: %s", opts.ModelPath)
	}

	modelCtxTrain := int(C.get_n_ctx_train(model))
	ctxSize, batchSize := resolveGenerationContextAndBatch(opts, modelCtxTrain)
	ctx := C.create_gen_context(model, C.int(ctxSize), C.int(batchSize), C.int(opts.Threads))
	if ctx == nil {
		C.free_model(model)
		return nil, fmt.Errorf("failed to create generation context: %s", opts.ModelPath)
	}

	return &GenerationModel{
		model:       model,
		ctx:         ctx,
		modelPath:   opts.ModelPath,
		batchSize:   batchSize,
		contextSize: ctxSize,
	}, nil
}

// GenerateParams configures text generation.
type GenerateParams struct {
	MaxTokens   int
	Temperature float32
	TopP        float32
	TopK        int
	StopTokens  []string
}

func longestStopTokenLength(stopTokens []string) int {
	longest := 0
	for _, stop := range stopTokens {
		if len(stop) > longest {
			longest = len(stop)
		}
	}
	return longest
}

func splitVisibleTextForStopTokens(candidate string, stopTokens []string, longestStopToken int) (visible string, remaining string, stop bool) {
	for _, stopToken := range stopTokens {
		if stopToken == "" {
			continue
		}
		if idx := strings.Index(candidate, stopToken); idx >= 0 {
			return candidate[:idx], "", true
		}
	}

	keepSuffix := 0
	maxCheck := len(candidate)
	if longestStopToken > 1 && maxCheck > longestStopToken-1 {
		maxCheck = longestStopToken - 1
	}
	for suffixLen := 1; suffixLen <= maxCheck; suffixLen++ {
		suffix := candidate[len(candidate)-suffixLen:]
		for _, stopToken := range stopTokens {
			if stopToken == "" || len(stopToken) <= suffixLen {
				continue
			}
			if strings.HasPrefix(stopToken, suffix) {
				if suffixLen > keepSuffix {
					keepSuffix = suffixLen
				}
				break
			}
		}
	}

	flushUpto := len(candidate) - keepSuffix
	if flushUpto <= 0 {
		return "", candidate, false
	}
	return candidate[:flushUpto], candidate[flushUpto:], false
}

// DefaultGenerateParams returns sensible defaults for structured output.
func DefaultGenerateParams() GenerateParams {
	return GenerateParams{
		MaxTokens:   512,
		Temperature: 0.1, // Low for deterministic JSON
		TopP:        0.9,
		TopK:        40,
		StopTokens:  []string{"<|im_end|>", "<|endoftext|>", "</s>"},
	}
}

// Generate produces a complete response for the prompt.
func (g *GenerationModel) Generate(ctx context.Context, prompt string, params GenerateParams) (string, error) {
	var result string
	err := g.GenerateStream(ctx, prompt, params, func(token string) error {
		result += token
		return nil
	})
	return result, err
}

// GenerateStream produces tokens via callback for streaming.
// Uses safe decode functions with signal handling and detailed error reporting.
func (g *GenerationModel) GenerateStream(ctx context.Context, prompt string, params GenerateParams, callback func(token string) error) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Pre-flight health check
	if g.model == nil || g.ctx == nil {
		return fmt.Errorf("model or context is nil - model may have been closed")
	}

	// Tokenize prompt
	cText := C.CString(prompt)
	defer C.free(unsafe.Pointer(cText))

	tokens := make([]C.int, params.MaxTokens+len(prompt))
	n := C.tokenize(g.model, cText, C.int(len(prompt)), &tokens[0], C.int(len(tokens)))
	if n < 0 {
		return fmt.Errorf("tokenization failed (result=%d)", n)
	}
	if n == 0 {
		return fmt.Errorf("prompt produced no tokens")
	}

	// Pre-flight validation: check token count against CONTEXT size (not batch)
	// Batch size just limits how many tokens we process per forward pass
	// We can process large prompts in multiple batches
	tokenCount := int(n)
	if tokenCount > g.contextSize {
		return fmt.Errorf("prompt too large: %d tokens exceeds context size of %d tokens. "+
			"Try a shorter message or increase NORNICDB_HEIMDALL_CONTEXT_SIZE",
			tokenCount, g.contextSize)
	}

	// Error buffer for detailed error messages from C
	errorBuf := make([]byte, 512)

	// Process prompt (prefill) - may require multiple batches for large prompts
	// Each batch adds to the KV cache, so the model "remembers" everything
	pos := 0
	remaining := tokenCount
	for remaining > 0 {
		// Process up to batchSize tokens at a time
		batchTokens := remaining
		if batchTokens > g.batchSize {
			batchTokens = g.batchSize
		}

		result := C.safe_gen_decode(
			g.ctx,
			g.model,
			(*C.int)(&tokens[pos]),
			C.int(batchTokens),
			C.int(pos), // Position in KV cache
			(*C.char)(unsafe.Pointer(&errorBuf[0])),
			C.int(len(errorBuf)),
		)
		if result != 0 {
			errMsg := C.GoString((*C.char)(unsafe.Pointer(&errorBuf[0])))
			return fmt.Errorf("prefill failed at position %d: %s (code=%d)", pos, errMsg, result)
		}

		pos += batchTokens
		remaining -= batchTokens
	}

	// Autoregressive generation
	genPos := tokenCount     // Start generation after all prompt tokens
	buf := make([]byte, 256) // Buffer for detokenization
	var emittedTail strings.Builder
	longestStopToken := longestStopTokenLength(params.StopTokens)

	for i := 0; i < params.MaxTokens; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Sample next token
		token := C.sample_token(g.ctx, g.model, C.float(params.Temperature), C.float(params.TopP), C.int(params.TopK))

		// Check for EOS
		if C.is_eos(g.model, token) != 0 {
			break
		}

		// Detokenize
		nBytes := C.detokenize(g.model, token, (*C.char)(unsafe.Pointer(&buf[0])), C.int(len(buf)))
		if nBytes > 0 {
			text := string(buf[:nBytes])
			emittedTail.WriteString(text)
			candidate := emittedTail.String()
			visible, remaining, stop := splitVisibleTextForStopTokens(candidate, params.StopTokens, longestStopToken)
			if stop {
				if visible != "" {
					if err := callback(visible); err != nil {
						return err
					}
				}
				return nil
			}
			if visible != "" {
				if err := callback(visible); err != nil {
					return err
				}
			}
			emittedTail.Reset()
			if remaining != "" {
				emittedTail.WriteString(remaining)
			}
		}

		// Decode the new token for next iteration with validation
		result := C.safe_gen_decode_token(
			g.ctx,
			g.model,
			token,
			C.int(genPos),
			(*C.char)(unsafe.Pointer(&errorBuf[0])),
			C.int(len(errorBuf)),
		)
		if result != 0 {
			errMsg := C.GoString((*C.char)(unsafe.Pointer(&errorBuf[0])))
			return fmt.Errorf("decode failed at position %d: %s (code=%d)", genPos, errMsg, result)
		}
		genPos++
	}

	if emittedTail.Len() > 0 {
		if err := callback(emittedTail.String()); err != nil {
			return err
		}
	}

	return nil
}

// ModelPath returns the loaded model path.
func (g *GenerationModel) ModelPath() string {
	return g.modelPath
}

// Close releases all resources.
func (g *GenerationModel) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.ctx != nil {
		C.free_ctx(g.ctx)
		g.ctx = nil
	}
	if g.model != nil {
		C.free_model(g.model)
		g.model = nil
	}
	return nil
}
