# Local GGUF Embedding Executor: Implementation Plan

> **Companion to**: `LOCAL_GGUF_EMBEDDING_FEASIBILITY.md`  
> **Scope**: Tight, performant GGUF embedding integration for NornicDB

---

## Overview

This plan adds a new `local` embedding provider that runs GGUF models directly within NornicDB. **External providers (Ollama, OpenAI) remain fully supported and unchanged.**

| Provider | Description | Status |
|----------|-------------|--------|
| `local` | **NEW** - Tightly coupled, runs in-process | This RFC |
| `ollama` | External Ollama server | Existing, unchanged |
| `openai` | OpenAI API | Existing, unchanged |

---

## Licensing

**BYOM (Bring Your Own Model)** - Licensing delineation is at the model file level.

| Component | License | Notes |
|-----------|---------|-------|
| llama.cpp | MIT | CGO static link - we're good |
| GGML | MIT | Via llama.cpp - we're good |
| NornicDB | MIT | Our code |
| **Model files** | **User's responsibility** | E5, BGE, etc. have their own licenses |

Users download their own `.gguf` files. We don't ship or recommend any specific model.

---

## Design Principles

1. **Backward compatible** - Existing `ollama` and `openai` configs unchanged
2. **BYOM** - User downloads/converts their own GGUF models
3. **Use existing config** - Same `NORNICDB_EMBEDDING_MODEL` env var, same pattern
4. **Simple model path** - Model name → `/data/models/{name}.gguf`
5. **Default to BGE-M3** - `bge-m3` instead of `mxbai-embed-large`
6. **GPU-first, CPU fallback** - Auto-detect CUDA/Metal, graceful fallback
7. **Tight CGO integration** - Direct llama.cpp bindings, no IPC/subprocess
8. **Low memory footprint** - mmap models, quantized weights, shared context

---

## Configuration (Matches Existing Pattern)

```bash
# NEW local mode (tightly coupled with database)
NORNICDB_EMBEDDING_PROVIDER=local
NORNICDB_EMBEDDING_MODEL=bge-m3                # Model name → /data/models/{name}.gguf
NORNICDB_EMBEDDING_DIMENSIONS=1024

# Existing providers still fully supported
NORNICDB_EMBEDDING_PROVIDER=ollama             # Uses external Ollama server
NORNICDB_EMBEDDING_PROVIDER=openai             # Uses OpenAI API

# GPU configuration (local mode only)
NORNICDB_EMBEDDING_GPU_LAYERS=-1               # -1 = auto (all to GPU if available)
                                               # 0 = CPU only
                                               # N = offload N layers to GPU
```

**Backward Compatibility:** Existing `ollama` and `openai` configurations work exactly as before.

---

## GPU Acceleration

Follows llama.cpp's GPU-first strategy:

```
Startup sequence (local mode):
1. Detect available GPU backend (CUDA → Metal → Vulkan → CPU)
2. If GPU found → offload all layers to GPU
3. If no GPU or NORNICDB_EMBEDDING_GPU_LAYERS=0 → use CPU with SIMD (AVX2/NEON)
```

| Backend | Platform | Detection |
|---------|----------|-----------|
| **CUDA** | Linux/Windows + NVIDIA | Primary, auto-detect |
| **Metal** | macOS Apple Silicon | Primary, auto-detect |
| **Vulkan** | Cross-platform | Fallback |
| **CPU** | All platforms | Always available |

### CGO Build Flags

```go
// Build with GPU support
#cgo linux,amd64 LDFLAGS: -L${SRCDIR}/../../lib/llama -lllama_linux_amd64_cuda -lcudart -lcublas
#cgo darwin,arm64 LDFLAGS: -L${SRCDIR}/../../lib/llama -lllama_darwin_arm64 -framework Metal -framework Accelerate
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     NornicDB Server                              │
├─────────────────────────────────────────────────────────────────┤
│  pkg/embed/                                                      │
│  ├── Embedder interface (existing)                               │
│  └── LocalGGUFEmbedder ──────────────────────────────────────┐  │
├──────────────────────────────────────────────────────────────┼──┤
│  pkg/localllm/                                               │  │
│  ┌───────────────────────────────────────────────────────────┼──┤
│  │                    Model (Go wrapper)                     │  │
│  │  • LoadModel() - mmap GGUF, create context                │  │
│  │  • Embed()     - tokenize → forward → pool → normalize    │  │
│  │  • Close()     - free resources                           │  │
│  └───────────────────────────────────────────────────────────┘  │
│                              │                                   │
│                      ┌───────▼───────┐                           │
│                      │  CGO Bridge   │                           │
│                      │  (llama.h)    │                           │
│                      └───────┬───────┘                           │
├──────────────────────────────┼──────────────────────────────────┤
│  lib/llama/ (vendored)       │                                   │
│  ├── llama.h + ggml.h        ▼                                   │
│  └── libllama.a ◄── Static library per platform                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Directory Structure

```
nornicdb/
├── pkg/
│   ├── embed/
│   │   ├── embed.go              # Embedder interface (unchanged)
│   │   ├── ollama.go             # OllamaEmbedder (unchanged)
│   │   ├── openai.go             # OpenAIEmbedder (unchanged)
│   │   └── local_gguf.go         # LocalGGUFEmbedder (NEW)
│   └── localllm/
│       ├── llama.go              # CGO bindings + Go wrapper
│       ├── llama_test.go
│       └── options.go            # Config structs
│
├── lib/
│   └── llama/                    # Vendored llama.cpp
│       ├── llama.h
│       ├── ggml.h
│       ├── libllama_linux_amd64.a       # CPU only
│       ├── libllama_linux_amd64_cuda.a  # With CUDA
│       ├── libllama_linux_arm64.a
│       ├── libllama_darwin_arm64.a      # With Metal
│       ├── libllama_darwin_amd64.a
│       └── libllama_windows_amd64.a     # With CUDA
│
└── scripts/
    └── build-llama.sh            # Build static libs
```

---

## Implementation

### Step 1: CGO Bindings

**File: `pkg/localllm/llama.go`**

```go
package localllm

/*
#cgo CFLAGS: -I${SRCDIR}/../../lib/llama

// Linux with CUDA (GPU primary)
#cgo linux,amd64,cuda LDFLAGS: -L${SRCDIR}/../../lib/llama -lllama_linux_amd64_cuda -lcudart -lcublas -lm -lstdc++ -lpthread
// Linux CPU fallback
#cgo linux,amd64,!cuda LDFLAGS: -L${SRCDIR}/../../lib/llama -lllama_linux_amd64 -lm -lstdc++ -lpthread

#cgo linux,arm64 LDFLAGS: -L${SRCDIR}/../../lib/llama -lllama_linux_arm64 -lm -lstdc++ -lpthread

// macOS with Metal (GPU primary on Apple Silicon)
#cgo darwin,arm64 LDFLAGS: -L${SRCDIR}/../../lib/llama -lllama_darwin_arm64 -lm -lc++ -framework Accelerate -framework Metal -framework MetalPerformanceShaders
#cgo darwin,amd64 LDFLAGS: -L${SRCDIR}/../../lib/llama -lllama_darwin_amd64 -lm -lc++ -framework Accelerate

// Windows with CUDA
#cgo windows,amd64 LDFLAGS: -L${SRCDIR}/../../lib/llama -lllama_windows_amd64 -lcudart -lcublas -lm -lstdc++

#include <stdlib.h>
#include <string.h>
#include "llama.h"

// Initialize backend once (handles GPU detection)
static int initialized = 0;
void init_backend() {
    if (!initialized) {
        llama_backend_init();
        initialized = 1;
    }
}

// Load model with mmap for low memory usage
llama_model* load_model(const char* path, int n_gpu_layers) {
    init_backend();
    struct llama_model_params params = llama_model_default_params();
    params.n_gpu_layers = n_gpu_layers;
    params.use_mmap = 1;
    return llama_load_model_from_file(path, params);
}

// Create embedding context (minimal memory)
llama_context* create_context(llama_model* model, int n_ctx, int n_batch, int n_threads) {
    struct llama_context_params params = llama_context_default_params();
    params.n_ctx = n_ctx;
    params.n_batch = n_batch;
    params.n_threads = n_threads;
    params.n_threads_batch = n_threads;
    params.embeddings = 1;
    params.pooling_type = LLAMA_POOLING_TYPE_MEAN;
    return llama_new_context_with_model(model, params);
}

// Tokenize using model's vocab
int tokenize(llama_model* model, const char* text, int text_len, int* tokens, int max_tokens) {
    return llama_tokenize(model, text, text_len, tokens, max_tokens, 1, 1);
}

// Generate embedding
int embed(llama_context* ctx, int* tokens, int n_tokens, float* out, int n_embd) {
    llama_kv_cache_clear(ctx);
    
    struct llama_batch batch = llama_batch_init(n_tokens, 0, 1);
    for (int i = 0; i < n_tokens; i++) {
        batch.token[i] = tokens[i];
        batch.pos[i] = i;
        batch.n_seq_id[i] = 1;
        batch.seq_id[i][0] = 0;
        batch.logits[i] = 0;
    }
    batch.n_tokens = n_tokens;
    
    if (llama_decode(ctx, batch) != 0) {
        llama_batch_free(batch);
        return -1;
    }
    
    float* embd = llama_get_embeddings_seq(ctx, 0);
    if (!embd) {
        llama_batch_free(batch);
        return -2;
    }
    
    memcpy(out, embd, n_embd * sizeof(float));
    llama_batch_free(batch);
    return 0;
}

int get_n_embd(llama_model* model) { return llama_n_embd(model); }
void free_ctx(llama_context* ctx) { if (ctx) llama_free(ctx); }
void free_model(llama_model* model) { if (model) llama_free_model(model); }
*/
import "C"

import (
    "context"
    "fmt"
    "math"
    "runtime"
    "sync"
    "unsafe"
)

// Model wraps a GGUF model for embedding generation
type Model struct {
    model *C.llama_model
    ctx   *C.llama_context
    dims  int
    mu    sync.Mutex
}

// Options configures model loading
type Options struct {
    ModelPath   string
    ContextSize int  // Default: 512
    BatchSize   int  // Default: 512
    Threads     int  // Default: NumCPU/2, capped at 8
    GPULayers   int  // Default: -1 (auto: all layers to GPU if available)
                     // 0 = CPU only, N = offload N layers
}

// DefaultOptions returns options optimized for GPU with CPU fallback
func DefaultOptions(modelPath string) Options {
    threads := runtime.NumCPU() / 2
    if threads < 1 {
        threads = 1
    }
    if threads > 8 {
        threads = 8
    }
    return Options{
        ModelPath:   modelPath,
        ContextSize: 512,
        BatchSize:   512,
        Threads:     threads,
        GPULayers:   -1, // Auto: use GPU if available, fallback to CPU
    }
}

// LoadModel loads a GGUF model
func LoadModel(opts Options) (*Model, error) {
    cPath := C.CString(opts.ModelPath)
    defer C.free(unsafe.Pointer(cPath))
    
    model := C.load_model(cPath, C.int(opts.GPULayers))
    if model == nil {
        return nil, fmt.Errorf("failed to load: %s", opts.ModelPath)
    }
    
    ctx := C.create_context(model, C.int(opts.ContextSize), C.int(opts.BatchSize), C.int(opts.Threads))
    if ctx == nil {
        C.free_model(model)
        return nil, fmt.Errorf("failed to create context")
    }
    
    return &Model{
        model: model,
        ctx:   ctx,
        dims:  int(C.get_n_embd(model)),
    }, nil
}

// Embed generates a normalized embedding
func (m *Model) Embed(ctx context.Context, text string) ([]float32, error) {
    if text == "" {
        return nil, nil
    }
    
    m.mu.Lock()
    defer m.mu.Unlock()
    
    // Tokenize
    cText := C.CString(text)
    defer C.free(unsafe.Pointer(cText))
    
    tokens := make([]C.int, 512)
    n := C.tokenize(m.model, cText, C.int(len(text)), &tokens[0], 512)
    if n < 0 {
        return nil, fmt.Errorf("tokenization failed")
    }
    
    // Embed
    emb := make([]float32, m.dims)
    if C.embed(m.ctx, (*C.int)(&tokens[0]), n, (*C.float)(&emb[0]), C.int(m.dims)) != 0 {
        return nil, fmt.Errorf("embedding failed")
    }
    
    // Normalize
    normalize(emb)
    return emb, nil
}

// EmbedBatch embeds multiple texts
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

// Dimensions returns embedding size
func (m *Model) Dimensions() int { return m.dims }

// Close frees resources
func (m *Model) Close() error {
    m.mu.Lock()
    defer m.mu.Unlock()
    C.free_ctx(m.ctx)
    C.free_model(m.model)
    return nil
}

func normalize(v []float32) {
    var sum float32
    for _, x := range v {
        sum += x * x
    }
    if sum == 0 {
        return
    }
    norm := float32(1.0 / math.Sqrt(float64(sum)))
    for i := range v {
        v[i] *= norm
    }
}
```

### Step 2: Embedder Integration

**File: `pkg/embed/local_gguf.go`**

```go
package embed

import (
    "context"
    "fmt"
    "os"
    "path/filepath"
    
    "github.com/orneryd/nornicdb/pkg/localllm"
)

// LocalGGUFEmbedder implements Embedder using a local GGUF model
type LocalGGUFEmbedder struct {
    model     *localllm.Model
    modelName string
    modelPath string
}

// NewLocalGGUF creates an embedder using the existing Config pattern.
// Model resolution: config.Model → /data/models/{model}.gguf
func NewLocalGGUF(config *Config) (*LocalGGUFEmbedder, error) {
    // Resolve model path: model name → /data/models/{name}.gguf
    modelsDir := os.Getenv("NORNICDB_MODELS_DIR")
    if modelsDir == "" {
        modelsDir = "/data/models"
    }
    
    modelPath := filepath.Join(modelsDir, config.Model+".gguf")
    
    // Check if file exists
    if _, err := os.Stat(modelPath); os.IsNotExist(err) {
        return nil, fmt.Errorf("model not found: %s (expected at %s)", config.Model, modelPath)
    }
    
    opts := localllm.DefaultOptions(modelPath)
    
    // Use config dimensions if provided, otherwise auto-detect
    if config.Dimensions > 0 {
        opts.ContextSize = 512 // Good default for embeddings
    }
    
    model, err := localllm.LoadModel(opts)
    if err != nil {
        return nil, fmt.Errorf("failed to load %s: %w", modelPath, err)
    }
    
    return &LocalGGUFEmbedder{
        model:     model,
        modelName: config.Model,
        modelPath: modelPath,
    }, nil
}

func (e *LocalGGUFEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
    return e.model.Embed(ctx, text)
}

func (e *LocalGGUFEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
    return e.model.EmbedBatch(ctx, texts)
}

func (e *LocalGGUFEmbedder) Dimensions() int { return e.model.Dimensions() }
func (e *LocalGGUFEmbedder) Model() string   { return e.modelName }
func (e *LocalGGUFEmbedder) Close() error    { return e.model.Close() }
```

### Step 3: Factory Update

**Update `NewEmbedder()` in `pkg/embed/embed.go`:**

```go
func NewEmbedder(config *Config) (Embedder, error) {
    switch config.Provider {
    case "local":
        return NewLocalGGUF(config)
    case "ollama":
        return NewOllama(config), nil
    case "openai":
        if config.APIKey == "" {
            return nil, fmt.Errorf("OpenAI requires an API key")
        }
        return NewOpenAI(config), nil
    default:
        return nil, fmt.Errorf("unknown provider: %s", config.Provider)
    }
}
```

### Step 4: Default Config Update

**Update defaults in `cmd/nornicdb/main.go`:**

```go
// Change default from mxbai to bge-m3
serveCmd.Flags().String("embedding-model", 
    getEnvStr("NORNICDB_EMBEDDING_MODEL", "bge-m3"), 
    "Embedding model name")
```

---

## Build System

### Build Script: `scripts/build-llama.sh`

```bash
#!/bin/bash
set -euo pipefail

VERSION="${1:-b4535}"
OUTDIR="lib/llama"
mkdir -p "$OUTDIR"

git clone --depth 1 --branch "$VERSION" https://github.com/ggerganov/llama.cpp.git /tmp/llama.cpp
cd /tmp/llama.cpp

OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)
[[ "$ARCH" == "x86_64" ]] && ARCH="amd64"
[[ "$ARCH" == "aarch64" ]] && ARCH="arm64"

CMAKE_ARGS="-DLLAMA_STATIC=ON -DBUILD_SHARED_LIBS=OFF -DLLAMA_BUILD_TESTS=OFF -DLLAMA_BUILD_EXAMPLES=OFF -DLLAMA_BUILD_SERVER=OFF"
[[ "$OS" == "darwin" && "$ARCH" == "arm64" ]] && CMAKE_ARGS="$CMAKE_ARGS -DLLAMA_METAL=ON"

cmake -B build $CMAKE_ARGS
cmake --build build --config Release -j$(nproc 2>/dev/null || sysctl -n hw.ncpu)

cp build/libllama.a "$OUTDIR/libllama_${OS}_${ARCH}.a"
cp llama.h ggml.h "$OUTDIR/"
echo "Built: libllama_${OS}_${ARCH}.a"
```

### GitHub Actions

```yaml
name: Build llama.cpp
on: workflow_dispatch

jobs:
  build:
    strategy:
      matrix:
        include:
          - os: ubuntu-latest
            lib: libllama_linux_amd64.a
          - os: macos-14
            lib: libllama_darwin_arm64.a
          - os: macos-13  
            lib: libllama_darwin_amd64.a
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4
      - run: ./scripts/build-llama.sh
      - uses: actions/upload-artifact@v4
        with:
          name: ${{ matrix.lib }}
          path: lib/llama/${{ matrix.lib }}
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NORNICDB_EMBEDDING_PROVIDER` | `ollama` | `local`, `ollama`, `openai` |
| `NORNICDB_EMBEDDING_MODEL` | `bge-m3` | Model name (looked up in models dir) |
| `NORNICDB_EMBEDDING_DIMENSIONS` | `1024` | Vector dimensions |
| `NORNICDB_MODELS_DIR` | `/data/models` | Directory for `.gguf` files |
| `NORNICDB_EMBEDDING_GPU_LAYERS` | `-1` | GPU offload: -1=auto, 0=CPU only, N=N layers |

**Note:** `NORNICDB_EMBEDDING_GPU_LAYERS` only applies to `local` provider. External providers (`ollama`, `openai`) manage their own GPU usage.

---

## Model Setup (User's Responsibility)

```bash
# Create models directory
mkdir -p /data/models

# Option 1: Download pre-converted GGUF (if available)
wget -O /data/models/bge-m3.gguf \
  https://huggingface.co/...some-community-conversion.../bge-m3.Q4_K_M.gguf

# Option 2: Convert from HuggingFace yourself
pip install llama-cpp-python
python -m llama_cpp.convert \
  --outfile /data/models/bge-m3.gguf \
  BAAI/bge-m3
```

**We don't ship models.** Users bring their own, handle their own licensing.

---

## Model Selection Guide

### Quick Reference

| Model | Best For | Context | Dims | License |
|-------|----------|---------|------|---------|
| **bge-m3** ⭐ | Long docs, code+docs hybrid (default) | 8192 | 1024 | MIT |
| **e5-large-v2** | Natural language search, multilingual | 512 | 1024 | MIT |
| **jina-embeddings-v2-base-code** | Pure code search | 8192 | 768 | Apache 2.0 |

### When to Use Which

**Use BGE-M3 (default) when:**
- Indexing entire files (handles 8K tokens)
- Code repositories with long functions
- Need hybrid retrieval (lexical + semantic)
- Want single model for code + docs

**Use E5 when:**
- Need 100+ language support
- Simpler dense-only retrieval preferred
- Working with shorter content (<512 tokens)
- Slightly lower memory footprint needed

**Use Jina Code when:**
- Pure code-to-code similarity
- "Find functions similar to this one"
- 30+ programming languages
- Don't need natural language understanding

### Practical Notes

1. **E5 and BGE-M3 both work for code** - they understand variable names, comments, and semantic patterns even though they weren't code-specialized

2. **Context length matters** - if your code files average >512 tokens, BGE-M3's 8K context is valuable

3. **Changing models = re-index everything** - embeddings from different models are incompatible

4. **Quantization is fine** - Q4_K_M loses ~2% quality but runs 3x faster, worth it for most use cases

---

## Performance Targets

| Hardware | Model | Latency | Memory |
|----------|-------|---------|--------|
| 4-core CPU | E5-base Q4 | <20ms | ~50MB (mmap) |
| 8-core CPU | E5-large Q4 | <30ms | ~100MB (mmap) |
| M2 (Metal) | E5-large Q4 | <10ms | ~100MB |

---

## Quick Start

```bash
# 1. Put your GGUF model in /data/models/
cp my-bge-model.gguf /data/models/bge-m3.gguf

# 2. Run with local provider (same config pattern as before!)
NORNICDB_EMBEDDING_PROVIDER=local \
NORNICDB_EMBEDDING_MODEL=bge-m3 \
nornicdb serve

# Or via CLI flags
nornicdb serve --embedding-provider=local --embedding-model=bge-m3
```

---

## Checklist

- [ ] Vendor llama.cpp headers (`lib/llama/`)
- [ ] Build static libs with GPU support (CUDA for Linux/Windows, Metal for macOS)
- [ ] Implement `pkg/localllm/llama.go` (CGO bindings with GPU detection)
- [ ] Implement `pkg/embed/local_gguf.go` (Embedder)
- [ ] Update `NewEmbedder()` factory to handle `local` provider
- [ ] **Ensure `ollama` and `openai` providers remain unchanged**
- [ ] Change default model to `bge-m3`
- [ ] Add `NORNICDB_MODELS_DIR` env var
- [ ] Add `NORNICDB_EMBEDDING_GPU_LAYERS` env var
- [ ] GPU auto-detection with graceful CPU fallback
- [ ] Tests + benchmarks (CPU and GPU)
- [ ] Docs update

---

## Migration from mxbai

For users already using `mxbai-embed-large` via Ollama:

```bash
# Before (Ollama) - STILL WORKS, NO CHANGES NEEDED
NORNICDB_EMBEDDING_PROVIDER=ollama
NORNICDB_EMBEDDING_MODEL=mxbai-embed-large

# After (Local GGUF) - NEW OPTION, put model in /data/models/
NORNICDB_EMBEDDING_PROVIDER=local
NORNICDB_EMBEDDING_MODEL=bge-m3   # or e5-large-v2, or keep mxbai if you have the GGUF
```

**Note:** Changing embedding models requires re-indexing. Embeddings from different models are not compatible.

---

*Version 2.0 - November 2024*
