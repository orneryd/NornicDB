# Apple ML Embedding Server

## Overview

The NornicDB menu bar app now includes an **OpenAI-compatible embedding server** powered by Apple's on-device Natural Language framework. This provides privacy-first, zero-cost text embeddings without requiring external API calls or additional software.

## Features

✅ **OpenAI API Compatible** - Drop-in replacement for OpenAI embeddings endpoint  
✅ **Privacy-First** - All processing happens on-device, no data leaves your Mac  
✅ **Zero Cost** - No API fees, unlimited usage  
✅ **Fast** - Native Apple Silicon optimization  
✅ **Multi-Language** - Automatic language detection and support  
✅ **Easy Integration** - Works with existing OpenAI client libraries  

## Quick Start

### 1. Enable the Server

Open the NornicDB menu bar app and:
1. Click the NornicDB icon in the menu bar
2. Select **"Embedding Server Settings..."**
3. Click **"Start"** button
4. Server will start on port `11435` (configurable)

### 2. Test the Server

```bash
# Simple test
curl http://localhost:11435/v1/embeddings \
  -H "Content-Type: application/json" \
  -d '{
    "input": "Hello, world!",
    "model": "apple-ml-embeddings"
  }'
```

### 3. Use with OpenAI Client Libraries

#### Python

```python
from openai import OpenAI

# Point to local embedding server
client = OpenAI(
    api_key="not-needed",  # API key not required
    base_url="http://localhost:11435/v1"
)

# Generate embeddings
response = client.embeddings.create(
    input="Your text here",
    model="apple-ml-embeddings"
)

embedding = response.data[0].embedding
print(f"Embedding dimensions: {len(embedding)}")
```

#### JavaScript/TypeScript

```typescript
import OpenAI from 'openai';

const client = new OpenAI({
  apiKey: 'not-needed',
  baseURL: 'http://localhost:11435/v1'
});

const response = await client.embeddings.create({
  input: 'Your text here',
  model: 'apple-ml-embeddings'
});

const embedding = response.data[0].embedding;
console.log(`Embedding dimensions: ${embedding.length}`);
```

#### Go

```go
package main

import (
    "context"
    "fmt"
    openai "github.com/sashabaranov/go-openai"
)

func main() {
    config := openai.DefaultConfig("not-needed")
    config.BaseURL = "http://localhost:11435/v1"
    client := openai.NewClientWithConfig(config)
    
    resp, err := client.CreateEmbeddings(context.Background(), openai.EmbeddingRequest{
        Input: []string{"Your text here"},
        Model: "apple-ml-embeddings",
    })
    
    if err != nil {
        panic(err)
    }
    
    embedding := resp.Data[0].Embedding
    fmt.Printf("Embedding dimensions: %d\n", len(embedding))
}
```

## API Reference

### POST /v1/embeddings

Generate text embeddings (OpenAI-compatible).

**Request:**

```json
{
  "input": "text to embed",  // string or array of strings
  "model": "apple-ml-embeddings",
  "encoding_format": "float"  // optional
}
```

**Response:**

```json
{
  "object": "list",
  "data": [
    {
      "object": "embedding",
      "embedding": [0.1, 0.2, ...],
      "index": 0
    }
  ],
  "model": "apple-ml-embeddings-v3",
  "usage": {
    "prompt_tokens": 10,
    "total_tokens": 10
  }
}
```

### GET /health

Health check endpoint.

**Response:**

```json
{
  "status": "ok",
  "model": "apple-ml-embeddings-v3",
  "dimensions": 768,
  "requests_served": 42,
  "average_latency_ms": 15.3
}
```

### GET /

API information and available endpoints.

## Configuration

### Port Configuration

Default port: `11435` (to avoid conflicts with Ollama on `11434`)

To change the port:
1. Open **Embedding Server Settings**
2. Enter new port number (1024-65535)
3. Click outside the field to save
4. Restart the server for changes to take effect

Port is saved in `UserDefaults` and persists across app restarts.

### Auto-Start

Enable **"Start automatically on login"** to have the embedding server start when the menu bar app launches.

## Technical Details

### Embedding Models

The server uses Apple's `NLEmbedding` framework with the following models:

- **Sentence Embeddings v3** (default, macOS 13+)
  - Dimensions: 768
  - Best for: Semantic similarity, search
  - Language: Auto-detected

- **Sentence Embeddings v2** (fallback)
  - Dimensions: 512
  - Compatible with older macOS versions

- **Word Embeddings** (fallback)
  - Dimensions: 300
  - Used if sentence embeddings unavailable

### Performance

Typical performance on Apple Silicon:

- **Single embedding**: 10-20ms
- **Batch (10 texts)**: 50-100ms
- **Memory**: ~50MB
- **CPU**: Minimal (optimized for Apple Neural Engine)

### Language Support

Automatic language detection with support for:

- English
- Spanish
- French
- German
- Italian
- Portuguese
- And many more (full list via `NLEmbedding.supportedSentenceLanguages()`)

## Integration Examples

### Use with LangChain

```python
from langchain.embeddings.base import Embeddings
from openai import OpenAI

class AppleMLEmbeddings(Embeddings):
    def __init__(self):
        self.client = OpenAI(
            api_key="not-needed",
            base_url="http://localhost:11435/v1"
        )
    
    def embed_documents(self, texts):
        response = self.client.embeddings.create(
            input=texts,
            model="apple-ml-embeddings"
        )
        return [data.embedding for data in response.data]
    
    def embed_query(self, text):
        return self.embed_documents([text])[0]

# Use in LangChain
embeddings = AppleMLEmbeddings()
vector_store = FAISS.from_texts(texts, embeddings)
```

### Use with Semantic Search

```python
import numpy as np
from openai import OpenAI

client = OpenAI(
    api_key="not-needed",
    base_url="http://localhost:11435/v1"
)

def cosine_similarity(a, b):
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))

# Embed documents
documents = [
    "NornicDB is a graph database",
    "Python is a programming language",
    "Machine learning uses neural networks"
]

doc_embeddings = []
for doc in documents:
    response = client.embeddings.create(input=doc, model="apple-ml-embeddings")
    doc_embeddings.append(response.data[0].embedding)

# Search
query = "What is NornicDB?"
query_response = client.embeddings.create(input=query, model="apple-ml-embeddings")
query_embedding = query_response.data[0].embedding

# Find most similar
similarities = [cosine_similarity(query_embedding, doc_emb) for doc_emb in doc_embeddings]
best_match_idx = np.argmax(similarities)

print(f"Best match: {documents[best_match_idx]}")
print(f"Similarity: {similarities[best_match_idx]:.3f}")
```

### Use with NornicDB

```python
import nornicdb
from openai import OpenAI

# Initialize NornicDB
db = nornicdb.open("./data")

# Initialize embedding client
embeddings = OpenAI(
    api_key="not-needed",
    base_url="http://localhost:11435/v1"
)

# Add nodes with embeddings
def add_document(text, metadata):
    # Generate embedding
    response = embeddings.embeddings.create(
        input=text,
        model="apple-ml-embeddings"
    )
    embedding = response.data[0].embedding
    
    # Store in NornicDB
    db.execute("""
        CREATE (n:Document {
            text: $text,
            embedding: $embedding,
            metadata: $metadata
        })
    """, {"text": text, "embedding": embedding, "metadata": metadata})

# Semantic search
def search(query, top_k=5):
    # Generate query embedding
    response = embeddings.embeddings.create(
        input=query,
        model="apple-ml-embeddings"
    )
    query_embedding = response.data[0].embedding
    
    # Search in NornicDB
    results = db.execute("""
        MATCH (n:Document)
        WITH n, vector.similarity.cosine(n.embedding, $query_embedding) AS similarity
        WHERE similarity > 0.7
        RETURN n.text, similarity
        ORDER BY similarity DESC
        LIMIT $top_k
    """, {"query_embedding": query_embedding, "top_k": top_k})
    
    return results
```

## Troubleshooting

### Server Won't Start

**Error: "Apple ML embeddings not available on this system"**

- Requires macOS 12.0 or later
- Ensure NaturalLanguage framework is available
- Check System Preferences > Privacy > Analytics & Improvements

**Error: "Failed to create listener on port 11435"**

- Port may be in use by another application
- Change port in settings
- Check with: `lsof -i :11435`

### Slow Performance

- First request may be slower (model loading)
- Subsequent requests are cached and faster
- Batch requests for better throughput
- Check Activity Monitor for CPU/memory usage

### API Compatibility Issues

- Ensure you're using the correct endpoint: `/v1/embeddings`
- API key can be any value (not validated)
- Model name must be provided but can be any string

## Comparison with Other Solutions

| Feature | Apple ML Server | Ollama | OpenAI API |
|---------|----------------|--------|------------|
| **Cost** | Free | Free | $0.0001/1K tokens |
| **Privacy** | On-device | On-device | Cloud |
| **Setup** | Built-in | Requires install | API key needed |
| **Speed** | Fast (10-20ms) | Medium (50-100ms) | Varies (network) |
| **Dimensions** | 768 | 1024 (mxbai) | 1536 (ada-002) |
| **Languages** | Auto-detect | Model-dependent | Most languages |
| **Offline** | ✅ Yes | ✅ Yes | ❌ No |

## Security & Privacy

- **No Data Collection**: All processing happens on your Mac
- **No Network Calls**: Embeddings generated locally
- **No API Keys**: No authentication required
- **No Logging**: Request data is not persisted
- **Sandboxed**: Runs within menu bar app sandbox

## FAQ

**Q: Can I use this in production?**  
A: Yes, but consider that embedding quality may differ from OpenAI's models. Test thoroughly for your use case.

**Q: What's the embedding dimension?**  
A: Typically 768 for sentence embeddings v3, but check the `/health` endpoint for your system.

**Q: Can I use custom models?**  
A: Currently uses Apple's built-in models. Custom CoreML models may be supported in future versions.

**Q: Does this work on Intel Macs?**  
A: Yes, but performance is optimized for Apple Silicon.

**Q: Can I run multiple instances?**  
A: Yes, use different ports for each instance.

**Q: Is this compatible with Ollama?**  
A: Yes! Use port 11435 to avoid conflicts with Ollama (port 11434).

## Future Enhancements

Planned features:

- [ ] Custom CoreML model support
- [ ] Batch size optimization
- [ ] Response caching
- [ ] Prometheus metrics endpoint
- [ ] Docker container support
- [ ] Linux support (via CoreML Tools)

## Support

For issues or questions:

1. Check the menu bar app logs
2. Open an issue on GitHub
3. Join the NornicDB Discord community

## License

Part of NornicDB, licensed under the same terms as the main project.
