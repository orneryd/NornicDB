# Apple Intelligence Integration for NornicDB

## Overview

NornicDB now supports **Apple Intelligence** for on-device, privacy-first text embeddings on macOS. This integration provides:

- ✅ **512-dimensional sentence embeddings** via Apple's NaturalLanguage framework
- ✅ **Zero-cost, on-device processing** (no API calls, no internet required)
- ✅ **OpenAI API-compatible** endpoint (`/v1/embeddings`)
- ✅ **Automatic configuration** - no code changes to NornicDB server needed
- ✅ **System tray toggle** - enable/disable with one click

## Architecture

The solution uses a **self-contained embedding server** that:

1. Runs locally on `http://localhost:11435`
2. Implements the OpenAI embeddings API format
3. Uses Apple's ML models for text embeddings
4. Auto-starts when "Use Apple Intelligence" is enabled

NornicDB is configured to treat this as an "OpenAI" provider, requiring **zero server-side code changes**.

## Files

### Core Components

- **`AppleMLEmbedder.swift`** - Apple NaturalLanguage framework wrapper
  - Generates 512-dimensional sentence embeddings
  - Supports multiple languages
  - Fallback handling for different macOS versions

- **`EmbeddingServer.swift`** - HTTP server for embeddings
  - OpenAI-compatible `/v1/embeddings` endpoint
  - Request buffering for proper HTTP parsing
  - Health check endpoint at `/health`

- **`EmbeddingServerTest.swift`** - Comprehensive test suite
  - Tests embeddings generation
  - Tests cosine similarity
  - Tests HTTP endpoints

### Integration Points

- **`NornicDBMenuBar.swift`** - Modified to:
  - Add `useAppleIntelligence` toggle in settings UI
  - Auto-start/stop embedding server based on settings
  - Configure NornicDB environment variables automatically

- **`Makefile`** - Updated to compile new Swift files

## Usage

### For End Users

1. Open **NornicDB Settings** from the menu bar
2. Go to **Features** tab
3. Enable **Embeddings**
4. Check **"Use Apple Intelligence"** (only visible if available)
5. Click **"Save & Restart"**

The embedding server will:
- Auto-start when the setting is enabled
- Auto-stop when disabled or app quits
- Run on port `11435` (configurable in code if needed)

### Configuration Details

When "Use Apple Intelligence" is enabled, NornicDB is automatically configured with:

```yaml
embedding:
  enabled: true
  provider: openai
  url: http://localhost:11435/v1/embeddings
  model: apple-ml-embeddings
  dimensions: 512
```

Environment variables set:
```bash
NORNICDB_EMBEDDING_ENABLED=true
NORNICDB_EMBEDDING_PROVIDER=openai
NORNICDB_EMBEDDING_API_URL=http://localhost:11435/v1/embeddings
NORNICDB_EMBEDDING_MODEL=apple-ml-embeddings
NORNICDB_EMBEDDING_DIMENSIONS=512
```

### Testing the Embedding Server

Run the standalone test:

```bash
cd macos/MenuBarApp
./test-embedding-server.sh
```

This will:
1. Test Apple ML availability
2. Generate embeddings
3. Test cosine similarity
4. Test HTTP endpoints
5. Verify OpenAI API compatibility

## API Compatibility

The embedding server is fully compatible with OpenAI's embeddings API:

### Request Format

```bash
curl http://localhost:11435/v1/embeddings \
  -H "Content-Type: application/json" \
  -d '{
    "input": "Your text here",
    "model": "apple-ml-embeddings"
  }'
```

### Response Format

```json
{
  "object": "list",
  "data": [
    {
      "object": "embedding",
      "embedding": [0.1, 0.2, ...],  // 512 floats
      "index": 0
    }
  ],
  "model": "apple-ml-sentence-v1",
  "usage": {
    "prompt_tokens": 4,
    "total_tokens": 4
  }
}
```

## Performance Characteristics

- **Dimensions**: 512 (vs 1024 for BGE-M3)
- **Speed**: Fast on-device inference (Apple Silicon optimized)
- **Memory**: Minimal (model is built into macOS)
- **Cost**: Zero (no API calls)
- **Privacy**: 100% on-device (no data leaves the machine)

## Advantages

✅ **No Setup Required** - Works out of the box on macOS 11+  
✅ **Zero Cost** - No API keys or subscriptions needed  
✅ **Privacy First** - All processing happens locally  
✅ **Fast** - Apple Silicon optimized  
✅ **Reliable** - No network dependencies  

## Limitations

⚠️ **macOS Only** - Requires Apple NaturalLanguage framework  
⚠️ **512 Dimensions** - Lower than some models (BGE-M3 is 1024)  
⚠️ **English Focused** - While multilingual, optimized for English  
⚠️ **Fixed Model** - Cannot upgrade without macOS updates  

## Troubleshooting

### Embedding server won't start

Check the logs:
```bash
tail -f /usr/local/var/log/nornicdb/stderr.log
```

Look for:
- `✅ Apple Intelligence embedding server auto-started`
- Or error messages

### Test if server is running

```bash
curl http://localhost:11435/health
```

Expected response:
```json
{
  "status": "ok",
  "model": "apple-ml-sentence-v1",
  "dimensions": 512,
  "requests_served": 0
}
```

### Embeddings not being generated

1. Check that embeddings are enabled in settings
2. Verify "Use Apple Intelligence" is checked
3. Restart the NornicDB server
4. Check that port 11435 is not in use by another process

### Reverting to local GGUF models

1. Uncheck "Use Apple Intelligence" in settings
2. Select your preferred GGUF model
3. Click "Save & Restart"

NornicDB will automatically reconfigure to use local embeddings.

## Development

### Building

The embedding server is compiled as part of the macOS menu bar app:

```bash
make macos-menubar
```

### Testing Changes

```bash
cd macos/MenuBarApp
./test-embedding-server.sh
```

### Adding Features

The server architecture is modular:

- **`AppleMLEmbedder`** - Handles ML model interaction
- **`EmbeddingServer`** - Handles HTTP server logic
- **`NornicDBMenuBar`** - Handles lifecycle management

## Future Enhancements

Potential improvements:

- [ ] Configurable port via UI
- [ ] Multiple model selection (if Apple adds more)
- [ ] Caching layer for repeated queries
- [ ] Batch processing optimization
- [ ] Server statistics dashboard

## References

- [Apple NaturalLanguage Documentation](https://developer.apple.com/documentation/naturallanguage)
- [OpenAI Embeddings API](https://platform.openai.com/docs/api-reference/embeddings)
- [EMBEDDING_SERVER.md](EMBEDDING_SERVER.md) - Detailed API documentation
