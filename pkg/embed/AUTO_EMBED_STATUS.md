# Auto-Embedding Status

## ⚠️ EXPERIMENTAL - DISABLED BY DEFAULT

**Status**: NOT PRODUCTION READY

The automatic embedding generation features in this package are **experimental** and have **not been fully tested**. They are **disabled by default** and should not be used in production environments.

## What is Auto-Embedding?

Auto-embedding refers to the automatic generation of vector embeddings for database nodes using LLM models (Ollama, OpenAI, etc.) with background processing, caching, and batch operations.

## Why is it Disabled?

1. **Not fully tested** - Test coverage is incomplete
2. **Integration not verified** - Database integration needs validation
3. **Performance not benchmarked** - Production performance unknown
4. **Error handling incomplete** - Edge cases not fully covered
5. **Resource management** - Memory and API usage not optimized

## What's Available?

### ✅ Production Ready (Enabled)
- `embed.go` - Basic embedding client interfaces
  - Ollama client (local models)
  - OpenAI client (API)
  - Provider abstraction
- **Coverage**: 90%+ for basic embedding functionality

### ⚠️ Experimental (Disabled)
- `auto_embed.go` - Automatic embedding generation
  - Background worker pools
  - LRU caching
  - Batch processing
  - Async callbacks
  - Node property extraction
- **Coverage**: 0% (tests disabled)
- **Status**: Code preserved for future development

## How to Enable (NOT RECOMMENDED)

If you need to test auto-embedding features in development:

```go
// Import the auto-embedding package
import "github.com/orneryd/nornicdb/pkg/embed"

// Create basic embedder
embedder := embed.NewOllama(nil)

// Create auto-embedder (EXPERIMENTAL)
config := embed.DefaultAutoEmbedConfig(embedder)
autoEmbedder := embed.NewAutoEmbedder(config)
defer autoEmbedder.Stop()

// Use at your own risk
embedding, _ := autoEmbedder.Embed(ctx, "test")
```

## Configuration

The following config options are **disabled by default**:

```yaml
# nornicdb.yaml
embeddings:
  auto_embed_enabled: false  # MUST be false in production
  provider: "none"            # Do not set unless testing
```

Environment variables:
```bash
# DO NOT SET THESE IN PRODUCTION
NORNICDB_AUTO_EMBED_ENABLED=false  # Default: false
NORNICDB_EMBEDDING_PROVIDER=none   # Default: none
```

## Roadmap

Before enabling auto-embedding in production:

- [ ] Complete test coverage (target: 90%)
- [ ] Integration tests with real database
- [ ] Performance benchmarks
- [ ] Memory profiling
- [ ] API rate limiting tests
- [ ] Error recovery testing
- [ ] Documentation review
- [ ] Security audit
- [ ] Production pilot

## For Developers

The code is kept in the repository for future development. If you're working on auto-embedding:

1. **DO NOT enable by default**
2. **Add comprehensive tests before production use**
3. **Benchmark performance impacts**
4. **Document all assumptions**
5. **Add feature flags for gradual rollout**

## Testing

Auto-embedding tests are currently **disabled**. To enable for development:

```bash
# Tests are disabled - do not run in CI/CD
# cd nornicdb && go test ./pkg/embed/... -tags experimental
```

---

**Last Updated**: 2025-11-26  
**Status**: DISABLED BY DEFAULT - NOT PRODUCTION READY
