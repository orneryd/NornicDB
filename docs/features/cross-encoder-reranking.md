# Cross-Encoder Reranking

**Two-Stage Retrieval for Higher Accuracy**

## Overview

Cross-encoder reranking is an optional Stage 2 retrieval step that improves search quality by re-scoring candidates with a more accurate (but slower) model. Reranking is **independent of Heimdall** and can use a **local GGUF model** (like embeddings) or an **external API** (Cohere, HuggingFace TEI, Ollama adapter), similar to how embeddings and Heimdall support multiple providers.

### How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│                     Two-Stage Retrieval                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Stage 1 (Fast)          Stage 2 (Accurate)                     │
│  ─────────────           ─────────────────                      │
│                                                                 │
│  ┌─────────────┐         ┌─────────────────┐                   │
│  │ Vector+BM25 │  ──→    │ Cross-Encoder   │  ──→  Top 10     │
│  │ RRF Fusion  │         │ Reranking       │       Results     │
│  └─────────────┘         └─────────────────┘                   │
│        ↓                        ↓                               │
│   100 candidates           Re-scored                            │
│   (fast lookup)            (query+doc together)                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Why Cross-Encoder?

**Bi-encoders** (embeddings) encode query and document separately:

```
query_embedding  = model.encode(query)
doc_embedding    = model.encode(document)  // Pre-computed!
score            = cosine(query_embedding, doc_embedding)
```

**Cross-encoders** encode them together:

```
score = model.cross_encode(query, document)  // Sees interaction!
```

The cross-encoder can capture fine-grained semantic relationships that bi-encoders miss, but it's O(N) vs O(log N).

## Server Configuration

When reranking is **enabled**, the server loads the configured reranker at startup (local GGUF asynchronously, external API immediately). Search requests then use Stage-2 reranking when `opts.RerankEnabled` is true (set from config for HTTP and gRPC search).

### Environment Variables

| Variable                          | Default     | Description                                                                                                                         |
| --------------------------------- | ----------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `NORNICDB_SEARCH_RERANK_ENABLED`  | `false`     | Enable Stage-2 reranking for vector/hybrid search                                                                                   |
| `NORNICDB_SEARCH_RERANK_PROVIDER` | `local`     | Backend: `local` (GGUF), `ollama`, `openai`, or `http`                                                                              |
| `NORNICDB_SEARCH_RERANK_MODEL`    | (see below) | For **local**: GGUF filename (e.g. `bge-reranker-v2-m3-Q4_K_M.gguf`). For **API**: model name/id (e.g. `rerank-english-v3.0`)       |
| `NORNICDB_SEARCH_RERANK_API_URL`  | (see below) | Rerank API endpoint for non-local providers (required when provider ≠ local; default for `ollama`: `http://localhost:11434/rerank`) |
| `NORNICDB_SEARCH_RERANK_API_KEY`  | (empty)     | API key for authenticated providers (e.g. Cohere, OpenAI)                                                                           |

Local GGUF rerankers default to llama.cpp rank pooling (`4`), non-causal
attention (`1`), and disabled flash attention (`0`). Override these with
`NORNICDB_RERANK_POOLING_TYPE`, `NORNICDB_RERANK_ATTENTION_TYPE`, and
`NORNICDB_RERANK_FLASH_ATTN` when a model requires different context settings.

Models directory for **local** provider: `NORNICDB_MODELS_DIR` (default `./models`). Download the default reranker with:

```bash
make download-bge-reranker
```

### YAML Configuration

```yaml
search_rerank:
  enabled: true
  provider: local # local | ollama | openai | http
  model: bge-reranker-v2-m3-Q4_K_M.gguf # GGUF filename (local) or API model name
  api_url: "" # For ollama/openai/http (e.g. https://api.cohere.ai/v1/rerank)
  api_key: "" # For Cohere, OpenAI, etc.
```

### Local GGUF (BGE-Reranker-v2-m3)

With `provider: local`, NornicDB loads a BGE-style reranker GGUF from the models directory (same pattern as the embedding model). Default model: `bge-reranker-v2-m3-Q4_K_M.gguf`.

```bash
# Download the default reranker model
make download-bge-reranker

# Enable and run (uses ./models by default)
export NORNICDB_SEARCH_RERANK_ENABLED=true
export NORNICDB_SEARCH_RERANK_PROVIDER=local
# Optional: NORNICDB_SEARCH_RERANK_MODEL=bge-reranker-v2-m3-Q4_K_M.gguf
# Optional: NORNICDB_MODELS_DIR=./models
./nornicdb serve
```

**Tip:** If you set env vars on the same line as the command (without `export`), every variable must be on one logical line using backslashes. Otherwise the shell runs each line as a separate command and only the last line’s vars are passed to `nornicdb`:

```bash
# ✅ Correct: all vars passed to nornicdb
NORNICDB_SEARCH_RERANK_ENABLED=true \
NORNICDB_SEARCH_RERANK_PROVIDER=local \
./bin/nornicdb serve

# ❌ Wrong: NORNICDB_SEARCH_RERANK_* are not passed (they run as separate commands)
NORNICDB_SEARCH_RERANK_ENABLED=true
NORNICDB_SEARCH_RERANK_PROVIDER=local
NORNICDB_HEIMDALL_ENABLED=true \
./bin/nornicdb serve
```

#### GGUF output dimension and quantization

- **Output dimension:** NornicDB requires the reranker GGUF to expose a **single relevance logit** through rank pooling. Model loading fails if the selected pooling mode does not return exactly one classifier output; NornicDB never substitutes the first coordinate of a pooled embedding. The logit is mapped through sigmoid to a score in [0,1].
- **Quantization:** Reranker GGUFs are commonly quantized (Q4_K_M, Q8_0, F16). Q4_K_M is typical for CPU/small GPU; Q8_0 or F16 for higher accuracy. The default `make download-bge-reranker` target uses a Q4_K_M variant.

#### Debugging flat or poor rerank scores

If every result has the same score (e.g. 0.49) or ordering is worse than with reranking off:

1. **Check model output dimension:** Set `NORNICDB_RERANK_DEBUG=1` and run a search. Logs show `dims=1`, `raw_logit`, and `score` per candidate. A non-scalar classifier head is rejected during model loading with its selected pooling type in the error.
2. **Fallback:** When the reranker produces nearly identical scores (range &lt; 0.05), NornicDB automatically falls back to RRF order and scores so search quality matches "reranking off" until you fix the model or config.

### External Providers (ollama / openai / http)

Use an HTTP rerank API (Cohere, HuggingFace TEI, or a custom/Ollama adapter). Set provider and API URL; for authenticated APIs, set the API key.

**Cohere:**

```bash
export NORNICDB_SEARCH_RERANK_ENABLED=true
export NORNICDB_SEARCH_RERANK_PROVIDER=http
export NORNICDB_SEARCH_RERANK_API_URL=https://api.cohere.ai/v1/rerank
export NORNICDB_SEARCH_RERANK_API_KEY=your_cohere_key
export NORNICDB_SEARCH_RERANK_MODEL=rerank-english-v3.0
```

**HuggingFace TEI (e.g. local container):**

```bash
export NORNICDB_SEARCH_RERANK_ENABLED=true
export NORNICDB_SEARCH_RERANK_PROVIDER=http
export NORNICDB_SEARCH_RERANK_API_URL=http://localhost:8080/rerank
export NORNICDB_SEARCH_RERANK_MODEL=cross-encoder/ms-marco-MiniLM-L-6-v2
```

**Ollama (if you run a rerank adapter on the Ollama port):**

```bash
export NORNICDB_SEARCH_RERANK_ENABLED=true
export NORNICDB_SEARCH_RERANK_PROVIDER=ollama
# NORNICDB_SEARCH_RERANK_API_URL defaults to http://localhost:11434/rerank
```

## Quick Start

### Enable via Search Options

```go
opts := search.DefaultSearchOptions()
opts.RerankEnabled = true
opts.RerankTopK = 100     // Rerank top 100 candidates
opts.RerankMinScore = 0.3 // Filter low-confidence results

results, err := svc.Search(ctx, query, embedding, opts)
```

### Configure the Reranker Programmatically

When you build the server yourself (e.g. tests or custom binary), you can set the reranker via `db.SetSearchReranker(...)`. For **HTTP/API** rerankers use `CrossEncoder`:

```go
// Configure cross-encoder (HTTP API) service
svc.SetReranker(search.NewCrossEncoder(&search.CrossEncoderConfig{
    Enabled:  true,
    APIURL:   "http://localhost:8081/rerank",
    Model:    "cross-encoder/ms-marco-MiniLM-L-6-v2",
    TopK:     100,
    Timeout:  30 * time.Second,
    MinScore: 0.0,
}))
```

For **local GGUF**, load the model with `localllm.DefaultRerankerOptions`, then pass it to `search.NewLocalReranker`. The options expose context size, batch size, threads, GPU layers, context type, pooling type, attention type, and flash attention. The standard server does this automatically when `NORNICDB_SEARCH_RERANK_ENABLED=true` and `NORNICDB_SEARCH_RERANK_PROVIDER=local`.

```go
opts := localllm.DefaultRerankerOptions("models/bge-reranker-v2-m3.gguf")
opts.Features.PoolingType = 4 // Override only when the model requires it.
model, err := localllm.LoadRerankerModel(opts)
```

## CrossEncoderConfig Options (HTTP/API)

| Option     | Default                                | Description                        |
| ---------- | -------------------------------------- | ---------------------------------- |
| `Enabled`  | `false`                                | Enable cross-encoder reranking     |
| `APIURL`   | `http://localhost:8081/rerank`         | Reranking service endpoint         |
| `APIKey`   | `""`                                   | Authentication token (if required) |
| `Model`    | `cross-encoder/ms-marco-MiniLM-L-6-v2` | Model name                         |
| `TopK`     | `100`                                  | How many candidates to rerank      |
| `Timeout`  | `30s`                                  | Request timeout                    |
| `MinScore` | `0.0`                                  | Minimum score threshold            |

## Supported Reranking Services

### Local GGUF (BGE-Reranker-v2-m3)

Configure via **server config** (env or YAML). No code required: set `NORNICDB_SEARCH_RERANK_ENABLED=true`, `NORNICDB_SEARCH_RERANK_PROVIDER=local`, and ensure the model is in `NORNICDB_MODELS_DIR` (e.g. `make download-bge-reranker`). See [Server Configuration](#server-configuration) above.

### Cohere Rerank API

```go
ce := search.NewCrossEncoder(&search.CrossEncoderConfig{
    Enabled: true,
    APIURL:  "https://api.cohere.ai/v1/rerank",
    APIKey:  "your-api-key",
    Model:   "rerank-english-v3.0",
})
```

### HuggingFace Text Embeddings Inference (TEI)

```bash
# Start TEI with reranking model
docker run -p 8081:80 ghcr.io/huggingface/text-embeddings-inference:latest \
    --model-id cross-encoder/ms-marco-MiniLM-L-6-v2
```

```go
ce := search.NewCrossEncoder(&search.CrossEncoderConfig{
    Enabled: true,
    APIURL:  "http://localhost:8081/rerank",
    Model:   "cross-encoder/ms-marco-MiniLM-L-6-v2",
})
```

### Local GGUF (in-process)

Use **server config** with `provider: local` and a GGUF path (e.g. BGE-Reranker-v2-m3). The server loads the model into memory like the embedding model. For an **external** HTTP service that runs a reranker (e.g. HuggingFace TEI or a custom adapter), use `provider: http` and set `api_url` to the rerank endpoint.

## Response Format

The cross-encoder integration supports multiple response formats:

### Cohere Format

```json
{
  "results": [
    { "index": 0, "relevance_score": 0.95 },
    { "index": 2, "relevance_score": 0.82 },
    { "index": 1, "relevance_score": 0.71 }
  ]
}
```

### HuggingFace TEI Format

```json
{
  "scores": [0.95, 0.71, 0.82]
}
```

### Simple Format

```json
{
  "rankings": [
    { "index": 0, "score": 0.95 },
    { "index": 2, "score": 0.82 }
  ]
}
```

## Performance Considerations

### Latency Trade-offs

| Method              | Latency   | Accuracy |
| ------------------- | --------- | -------- |
| Vector only         | ~5ms      | Good     |
| RRF Hybrid          | ~10ms     | Better   |
| RRF + Cross-Encoder | ~50-200ms | Best     |

### When to Use

✅ **Use cross-encoder when:**

- Accuracy is more important than latency
- Users are willing to wait for better results
- Search volume is low to moderate
- High-stakes decisions based on search

❌ **Skip cross-encoder when:**

- Low latency is critical (<50ms)
- High query volume (>1000 QPS)
- Results are "good enough" with bi-encoders
- Cost is a concern (API calls)

### Optimization Tips

1. **Limit TopK**: Rerank fewer candidates for faster response

   ```go
   opts.RerankTopK = 50  // Instead of 100
   ```

2. **Use MinScore**: Filter low-confidence results early

   ```go
   opts.RerankMinScore = 0.5  // Skip weak matches
   ```

3. **Batch Requests**: Cross-encoder processes all candidates in one call

4. **Cache Results**: For repeated queries, cache the reranked results

## Combining with MMR

Cross-encoder reranking can be combined with MMR diversification:

```go
opts := search.DefaultSearchOptions()
opts.MMREnabled = true
opts.MMRLambda = 0.7
opts.RerankEnabled = true

// Pipeline: Vector+BM25 → RRF → MMR → Cross-Encoder → Results
```

The search method will show: `rrf_hybrid+mmr+rerank`

## Monitoring

Check if any Stage-2 reranker is available (local GGUF or cross-encoder):

```go
if svc.RerankerAvailable(ctx) {
    log.Println("Reranker ready")
}
```

The search response includes the method used:

```json
{
  "search_method": "rrf_hybrid+rerank",
  "message": "RRF + Cross-Encoder Reranking"
}
```

## Error Handling

The cross-encoder gracefully falls back to original rankings on errors:

- API timeout → Use original RRF scores
- Server unavailable → Use original RRF scores
- Invalid response → Use original RRF scores

No error is returned to the caller - the search continues with best-effort results.

## Popular Reranker Models

| Model                                  | Provider        | Quality   | Speed   |
| -------------------------------------- | --------------- | --------- | ------- |
| `bge-reranker-v2-m3-Q4_K_M.gguf`       | Local (default) | Excellent | Medium  |
| `cross-encoder/ms-marco-MiniLM-L-6-v2` | HuggingFace TEI | Good      | Fast    |
| `cross-encoder/ms-marco-TinyBERT-L-6`  | HuggingFace TEI | Good      | Fastest |
| `BAAI/bge-reranker-base`               | TEI / HTTP      | Better    | Medium  |
| `Cohere rerank-english-v3.0`           | Cohere API      | Best      | API     |

## Related Documentation

- [Vector Search Guide](../user-guides/vector-search.md)
- [Hybrid Search Guide](../user-guides/hybrid-search.md)
- [RRF Algorithm](../user-guides/hybrid-search.md#rrf-algorithm)
- [Search Evaluation](../advanced/search-evaluation.md)

---

_Cross-Encoder Reranking v1.1 - January 2026 (local GGUF + external providers)_
