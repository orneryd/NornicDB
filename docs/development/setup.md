# Development Setup

**Get your development environment ready for NornicDB.**

## Prerequisites

- Go 1.21 or later
- Git
- Make (optional, for build shortcuts)
- Docker (optional, for containerized testing)
- curl (for downloading models)
- CGO-compatible compiler (for local LLM builds)

## Clone Repository

```bash
git clone https://github.com/orneryd/nornicdb.git
cd nornicdb
```

## Build from Source

### Native Binary

```bash
# Basic build (with UI)
make build

# Or without make:
go build -o bin/nornicdb ./cmd/nornicdb

# Headless build (no UI, smaller binary)
make build-headless

# With local LLM support
make build-localllm

# Headless with local LLM
make build-localllm-headless
```

### Docker Images

#### Model Downloads (Heimdall only)

Heimdall images require BGE-M3 embedding model (~400MB) and Qwen2.5-0.5B LLM (~350MB):

```bash
# Download both models automatically
make download-models

# Or download individually
make download-bge    # BGE-M3 embedding model
make download-qwen   # Qwen2.5-0.5B-Instruct LLM

# Check which models are present
make check-models
```

Models are downloaded from HuggingFace and saved to `models/` directory.

#### Build Docker Images

```bash
# ARM64 (Apple Silicon)
make build-arm64-metal                    # Base (BYOM)
make build-arm64-metal-bge                # With BGE embeddings
make build-arm64-metal-bge-heimdall       # With BGE + Heimdall AI (auto-downloads models)
make build-arm64-metal-headless           # Headless (no UI)

# AMD64 CUDA (NVIDIA GPU)
make build-amd64-cuda                     # Base (BYOM)
make build-amd64-cuda-bge                 # With BGE embeddings
make build-amd64-cuda-bge-heimdall        # With BGE + Heimdall AI (auto-downloads models)
make build-amd64-cuda-headless            # Headless (no UI)

# AMD64 CPU-only (no GPU, embeddings disabled)
make build-amd64-cpu                      # Minimal
make build-amd64-cpu-headless             # Minimal headless

# Build all variants for current architecture
make build-all
```

#### Deploy to Registry

```bash
# Deploy specific image (build + push)
make deploy-arm64-metal-bge-heimdall

# Deploy all variants for current architecture
make deploy-all
```

### Cross-Compilation

Build native binaries for other platforms from macOS:

```bash
# Linux servers
make cross-linux-amd64        # x86_64 (VPS, cloud)
make cross-linux-arm64        # ARM64 (Graviton, Jetson)

# Raspberry Pi
make cross-rpi                # Pi 4/5 (64-bit)
make cross-rpi32              # Pi 2/3 (32-bit)
make cross-rpi-zero           # Pi Zero (ARMv6)

# Windows
make cross-windows            # CPU-only build

# Build all platforms
make cross-all
```

Note: Cross-compiled binaries are pure Go (no CGO), so Metal/CUDA acceleration and local LLM support are not available.

## Run Tests

```bash
# All tests
go test ./...

# With coverage
go test -cover ./...

# Specific package
go test ./pkg/nornicdb/...
```

## Start Development Server

```bash
# Default settings (localhost:7474)
./bin/nornicdb serve --data-dir=./dev-data

# With auth disabled for easier development
./bin/nornicdb serve --data-dir=./dev-data --no-auth

# Custom ports
./bin/nornicdb serve --http-port=8080 --bolt-port=8687
```

## Environment Variables

### Core Settings

| Variable | Description | Default |
|----------|-------------|---------|
| `NORNICDB_DATA_DIR` | Data directory | `./data` |
| `NORNICDB_HTTP_PORT` | HTTP API port | `7474` |
| `NORNICDB_BOLT_PORT` | Bolt port | `7687` |
| `NORNICDB_NO_AUTH` | Disable auth | `false` |
| `NORNICDB_JWT_SECRET` | JWT secret | Required for auth |
| `NORNICDB_HEADLESS` | Disable web UI | `false` |

### Embedding & AI Features

| Variable | Description | Default |
|----------|-------------|---------|
| `NORNICDB_EMBEDDING_PROVIDER` | Embedding provider | `local` |
| `NORNICDB_EMBEDDING_MODEL` | Model path/name | `models/bge-m3.gguf` |
| `NORNICDB_HEIMDALL_ENABLED` | Enable Heimdall AI | `false` |
| `NORNICDB_HEIMDALL_MODEL` | Heimdall LLM model | `models/qwen2.5-0.5b-instruct-q4_k_m.gguf` |
| `NORNICDB_PLUGINS_DIR` | APOC plugins directory | `apoc/built-plugins` |

## IDE Setup

### VS Code

Recommended extensions:
- Go (official)
- Go Test Explorer
- GitLens

```json
// .vscode/settings.json
{
  "go.lintTool": "golangci-lint",
  "go.testFlags": ["-v"],
  "editor.formatOnSave": true
}
```

### GoLand

1. Open project root
2. Set GOPATH to project directory
3. Enable "Go Modules" in Preferences

## Project Structure

```
nornicdb/
├── cmd/nornicdb/     # CLI application
├── pkg/
│   ├── nornicdb/     # Core database
│   ├── server/       # HTTP server
│   ├── storage/      # Storage engines
│   ├── auth/         # Authentication
│   ├── audit/        # Audit logging
│   ├── gpu/          # GPU acceleration
│   └── heimdall/     # AI assistant
├── apoc/
│   ├── plugin-src/   # Plugin source code
│   └── built-plugins/ # Compiled plugins (.so)
├── docker/           # Docker files
├── models/           # LLM/embedding models
├── ui/               # Web UI (Svelte)
└── docs/             # Documentation
```

## APOC Plugin Development

NornicDB supports dynamic Go plugins (Linux/macOS only):

```bash
# Build APOC plugin
make plugins

# Run with plugins
NORNICDB_PLUGINS_DIR=apoc/built-plugins ./bin/nornicdb serve

# Clean plugins
make plugins-clean
```

See [APOC Plugin Guide](../features/plugin-system.md) for creating custom functions.

## Heimdall AI Assistant

Enable the built-in AI assistant for natural language database interactions:

```bash
# Ensure models are present
make check-models

# Download if missing
make download-models

# Run with Heimdall enabled
NORNICDB_HEIMDALL_ENABLED=true \
NORNICDB_HEIMDALL_MODEL=models/qwen2.5-0.5b-instruct-q4_k_m.gguf \
./bin/nornicdb serve
```

Access Bifrost (Heimdall UI) at [http://localhost:7474/bifrost](http://localhost:7474/bifrost)

See [Heimdall AI Assistant Guide](../user-guides/heimdall-ai-assistant.md) for details.

## Next Steps

- **[Testing Guide](testing.md)** - How to write tests
- **[Code Style](code-style.md)** - Code conventions
- **[Heimdall Plugins](../user-guides/heimdall-plugins.md)** - Create custom AI actions
- **[APOC Plugins](../features/plugin-system.md)** - Extend Cypher with custom functions
- **[Contributing](../CONTRIBUTING.md)** - Contribution process

