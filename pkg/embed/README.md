# Embedding Package

## Overview

This package provides embedding generation clients for NornicDB's vector search capabilities.

## Production Status

### ✅ PRODUCTION READY

**File**: `embed.go`  
**Coverage**: 90%+  
**Status**: Fully tested and production ready

#### Features
- Ollama client (local open-source models)
- OpenAI client (cloud API)
- Provider abstraction via `Embedder` interface
- Comprehensive error handling
- Context cancellation support
- Batch processing

HTTP batches retain input order and split at the provider's request limits.
Ollama batches use `/api/embed` (the legacy `/api/embeddings` setting still
selects this endpoint for batches). Voyage text requests allow at most 1,000
inputs and use the model's aggregate token budget; OpenAI/Orca requests allow
at most 2,048 inputs and 300,000 tokens. UTF-8 bytes conservatively bound text
tokens; Voyage retrieval instructions are included in that budget. Multimodal
inputs containing images reserve the full per-input context budget because
remote image dimensions are unknown.

Returned usage is logged once per successful HTTP response at debug level
(`embedding provider usage`, with provider, model, inputs and total_tokens).
Voyage results also retain exactly attributable request totals in the existing
`DocumentChunkResult.TotalTokens`. Every request whose inputs all belong to one
document credits that document, even within a multi-document caller batch or
when it contains several chunks. A request spanning documents has no
provider-supplied per-document attribution, so its total is logged rather than
copied into document results. If a document participates in both kinds of
request, its result contains only its exactly attributable usage. No token
metric or public result/configuration field is added.

#### Usage Example

```go
import "github.com/orneryd/nornicdb/pkg/embed"

// Use local Ollama (recommended)
config := embed.DefaultOllamaConfig()
embedder := embed.NewOllama(config)

embedding, err := embedder.Embed(ctx, "graph database")
if err != nil {
    log.Fatal(err)
}

// Or use OpenAI
apiKey := os.Getenv("OPENAI_API_KEY")
config := embed.DefaultOpenAIConfig(apiKey)
embedder := embed.NewOpenAI(config)

// Batch processing
texts := []string{"memory", "storage", "database"}
embeddings, err := embedder.EmbedBatch(ctx, texts)
```


**See**: [AUTO_EMBED_STATUS.md](./AUTO_EMBED_STATUS.md) for details.

## Supported Providers

### Ollama (Local)

**Models**:
- `mxbai-embed-large` (1024 dimensions) - Recommended
- `nomic-embed-text` (768 dimensions)
- `all-minilm` (384 dimensions)

**Setup**:
```bash
$ ollama pull mxbai-embed-large
$ ollama serve
```

**Configuration**:
```go
config := embed.DefaultOllamaConfig()
// Default: http://localhost:11434, mxbai-embed-large, 1024 dims
embedder := embed.NewOllama(config)
```

### OpenAI (Cloud API)

**Models**:
- `text-embedding-3-small` (1536 dimensions) - $0.02/1M tokens
- `text-embedding-3-large` (3072 dimensions) - $0.13/1M tokens

**Setup**:
```bash
export OPENAI_API_KEY=sk-...
```

**Configuration**:
```go
apiKey := os.Getenv("OPENAI_API_KEY")
config := embed.DefaultOpenAIConfig(apiKey)
embedder := embed.NewOpenAI(config)
```

## Interface

All embedders implement the `Embedder` interface:

```go
type Embedder interface {
    Embed(ctx context.Context, text string) ([]float32, error)
    EmbedBatch(ctx context.Context, texts []string) ([][]float32, error)
    Dimensions() int
    Model() string
}
```

## Testing

```bash
# Run tests for production-ready features
cd nornicdb && go test ./pkg/embed/... -cover

# Expected coverage: ~90% (only embed.go is tested)
```

## Configuration Examples

### Custom Ollama Server

```go
config := &embed.Config{
    Provider:   "ollama",
    APIURL:     "http://192.168.1.100:11434",
    Model:      "nomic-embed-text",
    Dimensions: 768,
    Timeout:    60 * time.Second,
}
embedder := embed.NewOllama(config)
```

### Dynamic Provider Selection

```go
provider := os.Getenv("EMBEDDING_PROVIDER") // "ollama" or "openai"

var config *embed.Config
if provider == "openai" {
    apiKey := os.Getenv("OPENAI_API_KEY")
    config = embed.DefaultOpenAIConfig(apiKey)
} else {
    config = embed.DefaultOllamaConfig()
}

embedder, err := embed.NewEmbedder(context.Background(), config)
```

## Performance Considerations

### Ollama (Local)
- **Latency**: 10-50ms per embedding (CPU), 5-15ms (GPU)
- **Throughput**: 20-100 embeddings/sec depending on hardware
- **Cost**: Free (runs locally)
- **Privacy**: Complete (no data leaves your machine)

### OpenAI (Cloud)
- **Latency**: 100-300ms per request (network dependent)
- **Throughput**: Rate limited by API tier
- **Cost**: $0.02-$0.13 per 1M tokens
- **Privacy**: Data sent to OpenAI servers

### Batch Processing

Always use `EmbedBatch()` for multiple texts:

```go
// ❌ Inefficient (many API calls)
for _, text := range texts {
    emb, _ := embedder.Embed(ctx, text)
}

// ✅ Efficient (single API call for OpenAI, batched for Ollama)
embs, _ := embedder.EmbedBatch(ctx, texts)
```

## Error Handling

Common errors and how to handle them:

```go
embedding, err := embedder.Embed(ctx, text)
if err != nil {
    switch {
    case errors.Is(err, context.Canceled):
        // Context was cancelled
        return nil
    case errors.Is(err, context.DeadlineExceeded):
        // Request timed out
        return nil
    case strings.Contains(err.Error(), "401"):
        // Invalid API key (OpenAI)
        log.Fatal("Invalid API key")
    case strings.Contains(err.Error(), "429"):
        // Rate limited (OpenAI)
        time.Sleep(time.Second)
        // Retry...
    default:
        // Other error
        log.Printf("Embedding failed: %v", err)
    }
}
```

## See Also

- [AUTO_EMBED_STATUS.md](./AUTO_EMBED_STATUS.md) - Status of experimental features
- [embed.go](./embed.go) - Production-ready implementation
- [embed_test.go](./embed_test.go) - Comprehensive tests

---

**Production Status**: ✅ Core features ready (embed.go)  
**Test Coverage**: 90%+ (production features only)  
**Last Updated**: 2025-11-26
