# NornicDB Build System
# ==============================================================================
# Cross-platform Docker image build and deployment
#
# Architectures:
#   - arm64-metal  : Apple Silicon with Metal acceleration
#   - amd64-cuda   : x86_64 with NVIDIA CUDA acceleration
#
# Each architecture has two variants:
#   - Base (BYOM)  : Bring Your Own Model - mount models at runtime
#   - BGE          : Pre-packaged with bge-m3.gguf embedding model
#
# Usage:
#   make build-arm64-metal          # Build base image (no model)
#   make build-arm64-metal-bge      # Build with embedded BGE model
#   make deploy-arm64-metal         # Build + Push base image
#   make deploy-arm64-metal-bge     # Build + Push BGE image
#   make deploy-all                 # Deploy both variants for current arch
# ==============================================================================

# Configuration
REGISTRY ?= timothyswt
VERSION ?= latest

# Docker build flags (use NO_CACHE=1 to force rebuild without cache)
# Usage: make build-arm64-metal NO_CACHE=1
ifdef NO_CACHE
    DOCKER_BUILD_FLAGS := --no-cache
else
    DOCKER_BUILD_FLAGS :=
endif

# Detect OS first (Windows doesn't have uname)
ifeq ($(OS),Windows_NT)
    HOST_OS := windows
    # Windows is always amd64 for our purposes (or override with HOST_ARCH=arm64)
    HOST_ARCH ?= amd64
    # Windows binaries need .exe extension
    BIN_EXT := .exe
else
    HOST_OS := $(shell uname -s | tr '[:upper:]' '[:lower:]')
    # Detect architecture: arm64 (Apple Silicon) or x86_64/amd64 (Intel/AMD)
    UNAME_M := $(shell uname -m)
    ifeq ($(UNAME_M),arm64)
        HOST_ARCH := arm64
    else ifeq ($(UNAME_M),aarch64)
        HOST_ARCH := arm64
    else
        HOST_ARCH := amd64
    endif
    # Unix binaries have no extension
    BIN_EXT :=
endif

# Image names: nornicdb-{architecture}[-{feature}]:latest
IMAGE_ARM64 := $(REGISTRY)/nornicdb-arm64-metal:$(VERSION)
IMAGE_ARM64_BGE := $(REGISTRY)/nornicdb-arm64-metal-bge:$(VERSION)
IMAGE_ARM64_BGE_HEIMDALL := $(REGISTRY)/nornicdb-arm64-metal-bge-heimdall:$(VERSION)
IMAGE_ARM64_HEADLESS := $(REGISTRY)/nornicdb-arm64-metal-headless:$(VERSION)
IMAGE_AMD64 := $(REGISTRY)/nornicdb-amd64-cuda:$(VERSION)
IMAGE_AMD64_BGE := $(REGISTRY)/nornicdb-amd64-cuda-bge:$(VERSION)
IMAGE_AMD64_BGE_HEIMDALL := $(REGISTRY)/nornicdb-amd64-cuda-bge-heimdall:$(VERSION)
IMAGE_AMD64_HEADLESS := $(REGISTRY)/nornicdb-amd64-cuda-headless:$(VERSION)
IMAGE_AMD64_CPU := $(REGISTRY)/nornicdb-amd64-cpu:$(VERSION)
IMAGE_AMD64_CPU_HEADLESS := $(REGISTRY)/nornicdb-amd64-cpu-headless:$(VERSION)
LLAMA_CUDA := $(REGISTRY)/llama-cuda-libs:b7285

# Dockerfiles
DOCKER_DIR := docker

# Model URLs and paths
MODELS_DIR := models
BGE_MODEL := $(MODELS_DIR)/bge-m3.gguf
QWEN_MODEL := $(MODELS_DIR)/qwen2.5-0.5b-instruct.gguf
BGE_URL := https://huggingface.co/gpustack/bge-m3-GGUF/resolve/main/bge-m3-Q4_K_M.gguf
QWEN_URL := https://huggingface.co/Qwen/Qwen2.5-0.5B-Instruct-GGUF/resolve/main/qwen2.5-0.5b-instruct-q4_k_m.gguf

.PHONY: build-arm64-metal build-arm64-metal-bge build-arm64-metal-bge-heimdall build-arm64-metal-headless
.PHONY: build-amd64-cuda build-amd64-cuda-bge build-amd64-cuda-bge-heimdall build-amd64-cuda-headless
.PHONY: build-amd64-cpu build-amd64-cpu-headless
.PHONY: build-all build-arm64-all build-amd64-all
.PHONY: push-arm64-metal push-arm64-metal-bge push-arm64-metal-bge-heimdall push-arm64-metal-headless
.PHONY: push-amd64-cuda push-amd64-cuda-bge push-amd64-cuda-bge-heimdall push-amd64-cuda-headless
.PHONY: push-amd64-cpu push-amd64-cpu-headless
.PHONY: deploy-arm64-metal deploy-arm64-metal-bge deploy-arm64-metal-bge-heimdall deploy-arm64-metal-headless
.PHONY: deploy-amd64-cuda deploy-amd64-cuda-bge deploy-amd64-cuda-bge-heimdall deploy-amd64-cuda-headless
.PHONY: deploy-amd64-cpu deploy-amd64-cpu-headless
.PHONY: deploy-all deploy-arm64-all deploy-amd64-all
.PHONY: build-llama-cuda push-llama-cuda deploy-llama-cuda
.PHONY: build build-ui build-binary build-localllm build-headless build-localllm-headless test clean images help macos-menubar macos-install macos-uninstall macos-all macos-clean macos-package macos-package-signed
.PHONY: download-models download-bge download-qwen check-models

# ==============================================================================
# Model Downloads (Heimdall prerequisites)
# ==============================================================================

# Create models directory if it doesn't exist
$(MODELS_DIR):
ifeq ($(HOST_OS),windows)
	@if not exist "$(MODELS_DIR)" mkdir "$(MODELS_DIR)"
else
	@mkdir -p $(MODELS_DIR)
endif

# Download BGE embedding model if missing
download-bge: $(MODELS_DIR)
ifeq ($(HOST_OS),windows)
	@if not exist "$(BGE_MODEL)" ( \
		echo =============================================================== && \
		echo  Downloading BGE-M3 embedding model... && \
		echo =============================================================== && \
		echo Source: $(BGE_URL) && \
		echo Target: $(BGE_MODEL) && \
		echo Size: ~400MB (this may take a few minutes) && \
		powershell -Command "Invoke-WebRequest -Uri '$(BGE_URL)' -OutFile '$(BGE_MODEL)'" && \
		echo Downloaded $(BGE_MODEL) \
	) else ( \
		echo BGE model already exists: $(BGE_MODEL) \
	)
else
	@if [ ! -f "$(BGE_MODEL)" ]; then \
		echo "==============================================================="; \
		echo " Downloading BGE-M3 embedding model..."; \
		echo "==============================================================="; \
		echo "Source: $(BGE_URL)"; \
		echo "Target: $(BGE_MODEL)"; \
		echo "Size: ~400MB (this may take a few minutes)"; \
		curl -L --progress-bar "$(BGE_URL)" -o "$(BGE_MODEL)"; \
		echo "Downloaded $(BGE_MODEL)"; \
	else \
		echo "BGE model already exists: $(BGE_MODEL)"; \
	fi
endif

# Download Qwen LLM model if missing
download-qwen: $(MODELS_DIR)
ifeq ($(HOST_OS),windows)
	@if not exist "$(QWEN_MODEL)" ( \
		echo =============================================================== && \
		echo  Downloading Qwen2.5-0.5B-Instruct model... && \
		echo =============================================================== && \
		echo Source: $(QWEN_URL) && \
		echo Target: $(QWEN_MODEL) && \
		echo Size: ~350MB (this may take a few minutes) && \
		powershell -Command "Invoke-WebRequest -Uri '$(QWEN_URL)' -OutFile '$(QWEN_MODEL)'" && \
		echo Downloaded $(QWEN_MODEL) \
	) else ( \
		echo Qwen model already exists: $(QWEN_MODEL) \
	)
else
	@if [ ! -f "$(QWEN_MODEL)" ]; then \
		echo "==============================================================="; \
		echo " Downloading Qwen2.5-0.5B-Instruct model..."; \
		echo "==============================================================="; \
		echo "Source: $(QWEN_URL)"; \
		echo "Target: $(QWEN_MODEL)"; \
		echo "Size: ~350MB (this may take a few minutes)"; \
		curl -L --progress-bar "$(QWEN_URL)" -o "$(QWEN_MODEL)"; \
		echo "Downloaded $(QWEN_MODEL)"; \
	else \
		echo "Qwen model already exists: $(QWEN_MODEL)"; \
	fi
endif

# Download both models if missing
download-models: download-bge download-qwen
	@echo ""
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ ✓ All Heimdall models ready                                  ║"
	@echo "╚══════════════════════════════════════════════════════════════╝"

# Check if models exist (without downloading)
check-models:
	@echo "Checking Heimdall models..."
ifeq ($(HOST_OS),windows)
	@if exist "$(BGE_MODEL)" ( \
		echo BGE model: $(BGE_MODEL) \
	) else ( \
		echo BGE model missing: $(BGE_MODEL) && \
		echo   Run: make download-bge \
	)
	@if exist "$(QWEN_MODEL)" ( \
		echo Qwen model: $(QWEN_MODEL) \
	) else ( \
		echo Qwen model missing: $(QWEN_MODEL) && \
		echo   Run: make download-qwen \
	)
else
	@if [ -f "$(BGE_MODEL)" ]; then \
		echo "✓ BGE model: $(BGE_MODEL)"; \
	else \
		echo "✗ BGE model missing: $(BGE_MODEL)"; \
		echo "  Run: make download-bge"; \
	fi
	@if [ -f "$(QWEN_MODEL)" ]; then \
		echo "✓ Qwen model: $(QWEN_MODEL)"; \
	else \
		echo "✗ Qwen model missing: $(QWEN_MODEL)"; \
		echo "  Run: make download-qwen"; \
	fi
endif

# ==============================================================================
# Build (local only, no push)
# ==============================================================================

build-arm64-metal:
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_ARM64) [BYOM]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --platform linux/arm64 -t $(IMAGE_ARM64) -f $(DOCKER_DIR)/Dockerfile.arm64-metal .

build-arm64-metal-bge: download-bge
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_ARM64_BGE) [with BGE model]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --platform linux/arm64 --build-arg EMBED_MODEL=true -t $(IMAGE_ARM64_BGE) -f $(DOCKER_DIR)/Dockerfile.arm64-metal .

build-arm64-metal-bge-heimdall: download-models
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_ARM64_BGE_HEIMDALL) [BGE + Heimdall]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --platform linux/arm64 -t $(IMAGE_ARM64_BGE_HEIMDALL) -f $(DOCKER_DIR)/Dockerfile.arm64-metal-heimdall .

build-arm64-metal-headless:
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_ARM64_HEADLESS) [headless, no UI]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --platform linux/arm64 --build-arg HEADLESS=true -t $(IMAGE_ARM64_HEADLESS) -f $(DOCKER_DIR)/Dockerfile.arm64-metal .

build-amd64-cuda:
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_AMD64) [BYOM]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --platform linux/amd64 -t $(IMAGE_AMD64) -f $(DOCKER_DIR)/Dockerfile.amd64-cuda .

build-amd64-cuda-bge: download-bge
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_AMD64_BGE) [with BGE model]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --platform linux/amd64 --build-arg EMBED_MODEL=true -t $(IMAGE_AMD64_BGE) -f $(DOCKER_DIR)/Dockerfile.amd64-cuda .

build-amd64-cuda-bge-heimdall: download-models
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_AMD64_BGE_HEIMDALL) [BGE + Heimdall]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --platform linux/amd64 -t $(IMAGE_AMD64_BGE_HEIMDALL) -f $(DOCKER_DIR)/Dockerfile.amd64-cuda-heimdall .

build-amd64-cuda-headless:
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_AMD64_HEADLESS) [headless, no UI]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --platform linux/amd64 --build-arg HEADLESS=true -t $(IMAGE_AMD64_HEADLESS) -f $(DOCKER_DIR)/Dockerfile.amd64-cuda .

build-amd64-cpu:
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_AMD64_CPU) [CPU-only, no embeddings]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --platform linux/amd64 -t $(IMAGE_AMD64_CPU) -f $(DOCKER_DIR)/Dockerfile.amd64-cpu .

build-amd64-cpu-headless:
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_AMD64_CPU_HEADLESS) [CPU-only, headless]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --platform linux/amd64 --build-arg HEADLESS=true -t $(IMAGE_AMD64_CPU_HEADLESS) -f $(DOCKER_DIR)/Dockerfile.amd64-cpu .

# Build both variants for an architecture
build-arm64-all: build-arm64-metal build-arm64-metal-bge build-arm64-metal-headless
	@echo "✓ Built all ARM64 Metal images"

build-amd64-all: build-amd64-cuda build-amd64-cuda-bge build-amd64-cuda-headless build-amd64-cpu build-amd64-cpu-headless
	@echo "✓ Built all AMD64 images (CUDA + CPU)"

# Build based on detected host architecture
build-all:
	@echo "Detected architecture: $(HOST_ARCH)"
ifeq ($(HOST_ARCH),arm64)
	@$(MAKE) build-arm64-all
else
	@$(MAKE) build-amd64-all
endif
	@echo "✓ All images for $(HOST_ARCH) built"

# ==============================================================================
# Push (registry only, assumes already built)
# ==============================================================================

push-arm64-metal:
	@echo "→ Pushing $(IMAGE_ARM64)"
	docker push $(IMAGE_ARM64)

push-arm64-metal-bge:
	@echo "→ Pushing $(IMAGE_ARM64_BGE)"
	docker push $(IMAGE_ARM64_BGE)

push-arm64-metal-bge-heimdall:
	@echo "→ Pushing $(IMAGE_ARM64_BGE_HEIMDALL)"
	docker push $(IMAGE_ARM64_BGE_HEIMDALL)

push-arm64-metal-headless:
	@echo "→ Pushing $(IMAGE_ARM64_HEADLESS)"
	docker push $(IMAGE_ARM64_HEADLESS)

push-amd64-cuda:
	@echo "→ Pushing $(IMAGE_AMD64)"
	docker push $(IMAGE_AMD64)

push-amd64-cuda-bge:
	@echo "→ Pushing $(IMAGE_AMD64_BGE)"
	docker push $(IMAGE_AMD64_BGE)

push-amd64-cuda-bge-heimdall:
	@echo "→ Pushing $(IMAGE_AMD64_BGE_HEIMDALL)"
	docker push $(IMAGE_AMD64_BGE_HEIMDALL)

push-amd64-cuda-headless:
	@echo "→ Pushing $(IMAGE_AMD64_HEADLESS)"
	docker push $(IMAGE_AMD64_HEADLESS)

push-amd64-cpu:
	@echo "→ Pushing $(IMAGE_AMD64_CPU)"
	docker push $(IMAGE_AMD64_CPU)

push-amd64-cpu-headless:
	@echo "→ Pushing $(IMAGE_AMD64_CPU_HEADLESS)"
	docker push $(IMAGE_AMD64_CPU_HEADLESS)

# ==============================================================================
# Deploy (Build + Push)
# ==============================================================================

deploy-arm64-metal: build-arm64-metal push-arm64-metal
	@echo "✓ Deployed $(IMAGE_ARM64)"

deploy-arm64-metal-bge: build-arm64-metal-bge push-arm64-metal-bge
	@echo "✓ Deployed $(IMAGE_ARM64_BGE)"

deploy-arm64-metal-bge-heimdall: build-arm64-metal-bge-heimdall push-arm64-metal-bge-heimdall
	@echo "✓ Deployed $(IMAGE_ARM64_BGE_HEIMDALL)"
	@echo "🛡️ Heimdall cognitive features enabled - access Bifrost at /bifrost"

deploy-arm64-metal-headless: build-arm64-metal-headless push-arm64-metal-headless
	@echo "✓ Deployed $(IMAGE_ARM64_HEADLESS)"

deploy-amd64-cuda: build-amd64-cuda push-amd64-cuda
	@echo "✓ Deployed $(IMAGE_AMD64)"

deploy-amd64-cuda-bge: build-amd64-cuda-bge push-amd64-cuda-bge
	@echo "✓ Deployed $(IMAGE_AMD64_BGE)"

deploy-amd64-cuda-bge-heimdall: build-amd64-cuda-bge-heimdall push-amd64-cuda-bge-heimdall
	@echo "✓ Deployed $(IMAGE_AMD64_BGE_HEIMDALL)"
	@echo "🛡️ Heimdall cognitive features enabled - access Bifrost at /bifrost"

deploy-amd64-cuda-headless: build-amd64-cuda-headless push-amd64-cuda-headless
	@echo "✓ Deployed $(IMAGE_AMD64_HEADLESS)"

deploy-amd64-cpu: build-amd64-cpu push-amd64-cpu
	@echo "✓ Deployed $(IMAGE_AMD64_CPU)"

deploy-amd64-cpu-headless: build-amd64-cpu-headless push-amd64-cpu-headless
	@echo "✓ Deployed $(IMAGE_AMD64_CPU_HEADLESS)"

# Deploy both variants for an architecture (including headless)
deploy-arm64-all: deploy-arm64-metal deploy-arm64-metal-bge deploy-arm64-metal-headless
	@echo "✓ Deployed all ARM64 Metal images"

deploy-amd64-all: deploy-amd64-cuda deploy-amd64-cuda-bge deploy-amd64-cuda-headless deploy-amd64-cpu deploy-amd64-cpu-headless
	@echo "✓ Deployed all AMD64 images (CUDA + CPU)"

# Deploy based on detected host architecture
deploy-all:
	@echo "Detected architecture: $(HOST_ARCH)"
ifeq ($(HOST_ARCH),arm64)
	@$(MAKE) deploy-arm64-all
else
	@$(MAKE) deploy-amd64-all
endif
	@echo "✓ All images for $(HOST_ARCH) deployed"

# ==============================================================================
# CUDA Prerequisite (one-time build, ~15 min)
# ==============================================================================

build-llama-cuda:
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building CUDA libs (one-time): $(LLAMA_CUDA)"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build --platform linux/amd64 -t $(LLAMA_CUDA) -f $(DOCKER_DIR)/Dockerfile.llama-cuda .

push-llama-cuda:
	@echo "→ Pushing $(LLAMA_CUDA)"
	docker push $(LLAMA_CUDA)

deploy-llama-cuda: build-llama-cuda push-llama-cuda
	@echo "✓ Deployed $(LLAMA_CUDA)"

# ==============================================================================
# Local Development (native binary, not Docker)
# ==============================================================================

# Build UI assets first
build-ui:
	@echo "Building UI assets..."
	@cd ui && npm install && npm run build
	@echo "✓ UI built successfully"

# Build NornicDB binary + APOC plugins (Windows: CPU-only, others: with localllm)
build: build-ui build-binary build-plugins-if-supported
	@echo "==============================================================="
	@echo " Build complete!"
	@echo "==============================================================="
ifeq ($(HOST_OS),windows)
	@echo ""
	@echo "Binary: bin\nornicdb.exe (CPU-only build)"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Quick start (no embeddings):"
	@echo "       .\bin\nornicdb.exe serve --no-auth"
	@echo ""
	@echo "  2. Connect with Neo4j drivers:"
	@echo "       bolt://localhost:7687"
	@echo "       (authentication disabled with --no-auth)"
	@echo ""
	@echo "  Note: This is a CPU-only build without embedding support."
	@echo "  For embeddings, use Docker: make build-amd64-cuda-bge-heimdall"
	@echo "  Or use external embedding provider (Ollama, OpenAI, etc.)"
	@echo ""
else
	@echo ""
	@echo "Binary: bin/nornicdb"
	@echo "Models: models/bge-m3.gguf"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Run the database:"
	@echo "       ./bin/nornicdb"
	@echo ""
	@echo "  2. Or run with custom config:"
	@echo "       ./bin/nornicdb --config nornicdb.yaml"
	@echo ""
	@echo "  3. Connect with Neo4j drivers:"
	@echo "       bolt://localhost:7687"
	@echo "       Username: admin"
	@echo "       Password: admin"
	@echo ""
endif

# Check and build llama.cpp library if not present or outdated
check-llama-lib:
ifeq ($(HOST_OS),windows)
	@echo "Windows: Skipping llama.cpp library check (CPU-only build)"
else
	@if [ ! -f lib/llama/libllama_$(HOST_OS)_$(HOST_ARCH).a ]; then \
		echo "⚠️  llama.cpp library not found, building..."; \
		./scripts/build-llama.sh; \
	elif ! nm lib/llama/libllama_$(HOST_OS)_$(HOST_ARCH).a 2>/dev/null | grep -q "llama_get_memory"; then \
		echo "⚠️  llama.cpp library outdated (missing llama_get_memory), rebuilding..."; \
		./scripts/build-llama.sh; \
	else \
		echo "✓ llama.cpp library up to date"; \
	fi
endif

build-binary: check-llama-lib
ifeq ($(HOST_OS),windows)
	@go build -o bin/nornicdb$(BIN_EXT) ./cmd/nornicdb
else
	CGO_ENABLED=1 go build -tags localllm -o bin/nornicdb$(BIN_EXT) ./cmd/nornicdb
endif

# Build plugins only if platform supports Go plugins (Linux/macOS, not Windows)
build-plugins-if-supported:
ifeq ($(OS),Windows_NT)
	@echo "Note: Go plugins not supported on Windows, skipping plugin build"
else
	@$(MAKE) plugins
endif

build-localllm: check-llama-lib build-plugins-if-supported
ifeq ($(HOST_OS),windows)
	@echo "Note: On Windows, build-localllm requires manual llama.cpp setup"
	@echo "Run: powershell -ExecutionPolicy Bypass -File scripts\\build-llama-cuda.ps1"
	@set CGO_ENABLED=1 && go build -tags "localllm" -o bin/nornicdb$(BIN_EXT) ./cmd/nornicdb
else
	CGO_ENABLED=1 go build -tags localllm -o bin/nornicdb$(BIN_EXT) ./cmd/nornicdb
endif

# Build without UI (headless mode)
build-headless: build-plugins-if-supported
	go build -tags noui -o bin/nornicdb-headless$(BIN_EXT) ./cmd/nornicdb

build-localllm-headless: check-llama-lib build-plugins-if-supported
ifeq ($(HOST_OS),windows)
	@echo "Note: On Windows, build-localllm-headless requires manual llama.cpp setup"
	@echo "Run: powershell -ExecutionPolicy Bypass -File scripts\\build-llama-cuda.ps1"
	@set CGO_ENABLED=1 && go build -tags "localllm noui" -o bin/nornicdb-headless$(BIN_EXT) ./cmd/nornicdb
else
	CGO_ENABLED=1 go build -tags "localllm noui" -o bin/nornicdb-headless$(BIN_EXT) ./cmd/nornicdb
endif

test:
ifeq ($(HOST_OS),windows)
	powershell -Command "$$env:GOMEMLIMIT='4GiB'; go test -p 1 -parallel 1 -timeout 30m ./..."
else
	go test -timeout 30m ./...
endif

# Test with limited parallelism (useful on Windows with memory constraints)
test-serial:
ifeq ($(HOST_OS),windows)
	powershell -Command "$$env:GOMEMLIMIT='4GiB'; go test -p 1 -parallel 1 -timeout 30m ./..."
else
	go test -p 1 -parallel 4 -timeout 30m ./...
endif
	

# Test a specific package
test-pkg:
	@echo "Usage: make test-pkg PKG=./pkg/cypher"
ifeq ($(HOST_OS),windows)
	powershell -Command "$$env:GOMEMLIMIT='4GiB'; go test -v -timeout 10m $(PKG)"
else
	go test -v -timeout 10m $(PKG)
endif

# ==============================================================================
# Cross-Compilation (native binaries for other platforms)
# ==============================================================================
# Build from macOS for: Linux servers, Raspberry Pi, Windows, etc.
# Note: CGO is disabled for cross-compilation (pure Go, no Metal/CUDA)

.PHONY: cross-linux-amd64 cross-linux-arm64 cross-rpi cross-rpi-zero cross-windows cross-all

# Linux x86_64 (standard servers, VPS, Docker hosts)
cross-linux-amd64:
	@echo "Building for Linux x86_64..."
ifeq ($(HOST_OS),windows)
	@set CGO_ENABLED=0 && set GOOS=linux && set GOARCH=amd64 && go build -o bin/nornicdb-linux-amd64 ./cmd/nornicdb
else
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/nornicdb-linux-amd64 ./cmd/nornicdb
endif
	@echo "bin/nornicdb-linux-amd64"

# Linux ARM64 (AWS Graviton, newer ARM servers, Jetson)
cross-linux-arm64:
	@echo "Building for Linux ARM64..."
ifeq ($(HOST_OS),windows)
	@set CGO_ENABLED=0 && set GOOS=linux && set GOARCH=arm64 && go build -o bin/nornicdb-linux-arm64 ./cmd/nornicdb
else
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o bin/nornicdb-linux-arm64 ./cmd/nornicdb
endif
	@echo "bin/nornicdb-linux-arm64"

# Raspberry Pi 4/5, Pi 3B+ 64-bit, Orange Pi, etc.
cross-rpi:
	@echo "Building for Raspberry Pi (64-bit ARM)..."
ifeq ($(HOST_OS),windows)
	@set CGO_ENABLED=0 && set GOOS=linux && set GOARCH=arm64 && go build -o bin/nornicdb-rpi64 ./cmd/nornicdb
else
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o bin/nornicdb-rpi64 ./cmd/nornicdb
endif
	@echo "bin/nornicdb-rpi64"

# Raspberry Pi 2/3/Zero 2 W (32-bit ARMv7)
cross-rpi32:
	@echo "Building for Raspberry Pi (32-bit ARMv7)..."
ifeq ($(HOST_OS),windows)
	@set CGO_ENABLED=0 && set GOOS=linux && set GOARCH=arm && set GOARM=7 && go build -o bin/nornicdb-rpi32 ./cmd/nornicdb
else
	CGO_ENABLED=0 GOOS=linux GOARCH=arm GOARM=7 go build -o bin/nornicdb-rpi32 ./cmd/nornicdb
endif
	@echo "bin/nornicdb-rpi32"

# Raspberry Pi 1/Zero/Zero W (ARMv6)
cross-rpi-zero:
	@echo "Building for Raspberry Pi Zero (ARMv6)..."
ifeq ($(HOST_OS),windows)
	@set CGO_ENABLED=0 && set GOOS=linux && set GOARCH=arm && set GOARM=6 && go build -o bin/nornicdb-rpi-zero ./cmd/nornicdb
else
	CGO_ENABLED=0 GOOS=linux GOARCH=arm GOARM=6 go build -o bin/nornicdb-rpi-zero ./cmd/nornicdb
endif
	@echo "bin/nornicdb-rpi-zero"

# Windows x86_64 (CPU only, no embeddings)
cross-windows:
	@echo "Building for Windows x86_64 (CPU-only)..."
ifeq ($(HOST_OS),windows)
	@set CGO_ENABLED=0 && set GOOS=windows && set GOARCH=amd64 && go build -o bin/nornicdb.exe ./cmd/nornicdb
else
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o bin/nornicdb.exe ./cmd/nornicdb
endif
	@echo "bin/nornicdb.exe"

# Windows native builds (must run on Windows)
# See: build.bat for all Windows variants
cross-windows-native:
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Windows Native Builds (run on Windows)                       ║"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "Variants available via build.bat:"
	@echo ""
	@echo "  CPU-only (no embeddings):"
	@echo "    build.bat cpu              Smallest (~15MB)"
	@echo ""
	@echo "  CPU + Local Embeddings:"
	@echo "    build.bat cpu-localllm     BYOM (~25MB)"
	@echo "    build.bat cpu-bge          With BGE model (~425MB)"
	@echo ""
	@echo "  CUDA + Local Embeddings (requires NVIDIA GPU):"
	@echo "    build.bat cuda             BYOM (~30MB)"
	@echo "    build.bat cuda-bge         With BGE model (~430MB)"
	@echo ""
	@echo "Prerequisites:"
	@echo "  - All: Go 1.23+"
	@echo "  - localllm/bge: Pre-built llama.cpp libs (build.bat download-libs)"
	@echo "  - cuda: CUDA Toolkit 12.x + VS2022"
	@echo "  - bge: BGE model file (build.bat download-model)"

# Build all cross-compilation targets (excludes Windows CUDA which needs native build)
cross-all: cross-linux-amd64 cross-linux-arm64 cross-rpi cross-rpi32 cross-rpi-zero cross-windows
	@echo ""
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Cross-compilation complete! Binaries in bin/                 ║"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	@ls -lh bin/nornicdb*

# ==============================================================================
# Utilities
# ==============================================================================

images:
	@echo "Host architecture: $(HOST_ARCH)"
	@echo ""
	@echo "ARM64 Metal images:"
	@echo "  $(IMAGE_ARM64)            [BYOM]"
	@echo "  $(IMAGE_ARM64_BGE)        [BGE embedded]"
	@echo "  $(IMAGE_ARM64_HEADLESS)   [headless, no UI]"
	@echo ""
	@echo "AMD64 CUDA images:"
	@echo "  $(IMAGE_AMD64)            [BYOM]"
	@echo "  $(IMAGE_AMD64_BGE)        [BGE embedded]"
	@echo "  $(IMAGE_AMD64_HEADLESS)   [headless, no UI]"
	@echo ""
	@echo "AMD64 CPU images (no GPU, embeddings disabled):"
	@echo "  $(IMAGE_AMD64_CPU)            [CPU-only]"
	@echo "  $(IMAGE_AMD64_CPU_HEADLESS)   [CPU-only, headless]"
	@echo ""
	@echo "CUDA prerequisite:"
	@echo "  $(LLAMA_CUDA)"

# ==============================================================================
# Plugin System (APOC + Heimdall)
# ==============================================================================
# Go plugins (.so files) that can be dynamically loaded at runtime.
# Note: Go plugins only work on Linux and macOS (not Windows).
# Plugins must be built with the same Go version as the main binary.

PLUGINS_DIR := apoc/built-plugins
HEIMDALL_PLUGINS_DIR := plugins/heimdall/built-plugins

# Check if plugins are supported on this platform
.PHONY: plugin-check
plugin-check:
ifeq ($(OS),Windows_NT)
	@echo "Error: Go plugins are not supported on Windows"
	@echo "Use static linking instead (functions are built into the binary)"
	@exit 1
else
	@echo "Platform $(shell uname -s) supports Go plugins"
endif

# Build all loadable plugins (APOC + Heimdall example)
.PHONY: plugins
plugins: plugin-check plugin-apoc plugin-heimdall-watcher
	@echo ""
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Plugins built successfully!                                  ║"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "APOC plugins:"
	@ls -lh $(PLUGINS_DIR)/*.so 2>/dev/null || echo "  (none)"
	@echo ""
	@echo "Heimdall plugins:"
	@ls -lh $(HEIMDALL_PLUGINS_DIR)/*.so 2>/dev/null || echo "  (none)"
	@echo ""
	@echo "To use copy-paste the following:"
	@echo "   NORNICDB_HEIMDALL_ENABLED=true \\"
	@echo "   NORNICDB_HEIMDALL_PLUGINS_DIR=$(HEIMDALL_PLUGINS_DIR) \\"
	@echo "   NORNICDB_PLUGINS_DIR=$(PLUGINS_DIR) \\"
	@echo "   NORNICDB_MODELS_DIR=$(MODELS_DIR) \\"
	@echo "   NORNICDB_EMBEDDING_PROVIDER=local \\"
	@echo "   NORNICDB_DATA_DIR=./data/test \\"
	@echo "   NORNICDB_KMEANS_CLUSTERING_ENABLED=true \\"
	@echo "   NORNICDB_EMBEDDING_PROVIDER=local \\"
	@echo "   ./bin/nornicdb serve --no-auth"

# Plugin source directory
PLUGINS_SRC_DIR := apoc/plugin-src

# Build APOC plugin (function count determined at runtime from plugin)
.PHONY: plugin-apoc
plugin-apoc: plugin-check
	@mkdir -p $(PLUGINS_DIR)
	@echo "Building APOC plugin..."
	cd $(PLUGINS_SRC_DIR)/apoc && go build -buildmode=plugin -o ../../../$(PLUGINS_DIR)/apoc.so apoc_plugin.go
	@echo "Built: $(PLUGINS_DIR)/apoc.so"

# Build Heimdall Watcher plugin (example plugin)
.PHONY: plugin-heimdall-watcher
plugin-heimdall-watcher: plugin-check
	@mkdir -p $(HEIMDALL_PLUGINS_DIR)
	@echo "Building Heimdall Watcher plugin..."
	cd plugins/heimdall/plugin-src/watcher && go build -buildmode=plugin -o ../../built-plugins/watcher.so watcher_plugin.go
	@echo "Built: $(HEIMDALL_PLUGINS_DIR)/watcher.so"

# Note: Heimdall core is built into the binary. Plugins extend functionality.
# Enable Heimdall with: NORNICDB_HEIMDALL_ENABLED=true
# Load plugins with: NORNICDB_HEIMDALL_PLUGINS_DIR=$(HEIMDALL_PLUGINS_DIR)

# Clean plugins
.PHONY: plugins-clean
plugins-clean:
	rm -rf $(PLUGINS_DIR) $(HEIMDALL_PLUGINS_DIR)
	@echo "Cleaned plugin build artifacts"

# List available plugins
.PHONY: plugins-list
plugins-list:
	@echo "Available Plugin Targets:"
	@echo ""
	@echo "  make plugins                    Build all plugins (APOC + Heimdall)"
	@echo "  make plugin-apoc                Build APOC plugin"
	@echo "  make plugin-heimdall-watcher    Build Heimdall Watcher plugin"
	@echo "  make plugins-clean              Remove built plugins"
	@echo ""
	@echo "Built APOC plugins:"
ifeq ($(HOST_OS),windows)
	@if exist "$(PLUGINS_DIR)" ( \
		dir /b $(PLUGINS_DIR)\*.dll 2>nul || echo   (none) \
	) else ( \
		echo   (none) \
	)
else
	@if [ -d "$(PLUGINS_DIR)" ]; then \
		ls -lh $(PLUGINS_DIR)/*.so 2>/dev/null || echo "  (none)"; \
	else \
		echo "  (none)"; \
	fi
endif
	@echo ""
	@echo "Built Heimdall plugins:"
ifeq ($(HOST_OS),windows)
	@if exist "$(HEIMDALL_PLUGINS_DIR)" ( \
		dir /b $(HEIMDALL_PLUGINS_DIR)\*.dll 2>nul || echo   (none) \
	) else ( \
		echo   (none) \
	)
else
	@if [ -d "$(HEIMDALL_PLUGINS_DIR)" ]; then \
		ls -lh $(HEIMDALL_PLUGINS_DIR)/*.so 2>/dev/null || echo "  (none)"; \
	else \
		echo "  (none)"; \
	fi
endif
	@echo ""
	@echo "To build all: make plugins"

clean:
	rm -rf bin/nornicdb bin/nornicdb-headless bin/nornicdb.exe \
		bin/nornicdb-linux-amd64 bin/nornicdb-linux-arm64 \
		bin/nornicdb-rpi64 bin/nornicdb-rpi32 bin/nornicdb-rpi-zero

help:
	@echo "NornicDB Build System (detected arch: $(HOST_ARCH), OS: $(HOST_OS))"
	@echo ""
	@echo "Model Downloads (Heimdall prerequisites):"
	@echo "  make download-models         Download both BGE + Qwen models"
	@echo "  make download-bge            Download BGE embedding model (~400MB)"
	@echo "  make download-qwen           Download Qwen LLM model (~350MB)"
	@echo "  make check-models            Check which models are present"
	@echo ""
	@echo "Local Development:"
ifeq ($(HOST_OS),windows)
	@echo "  make build                   Build UI + native binary (CPU-only, no embeddings)"
else
	@echo "  make build                   Build UI + native binary (with local embeddings)"
endif
	@echo "  make build-ui                Build UI assets only (ui/dist/)"
	@echo "  make build-binary            Build Go binary only (requires UI built)"
	@echo "  make build-headless          Build native binary without UI"
ifeq ($(HOST_OS),windows)
	@echo "  make build-localllm          Build with local LLM support (requires manual setup)"
else
	@echo "  make build-localllm          Build with local LLM support"
endif
	@echo "  make build-localllm-headless Build headless with local LLM"
	@echo ""
	@echo "Cross-Compilation (from macOS to other platforms):"
	@echo "  make cross-linux-amd64       Linux x86_64 (servers, VPS)"
	@echo "  make cross-linux-arm64       Linux ARM64 (Graviton, Jetson)"
	@echo "  make cross-rpi               Raspberry Pi 4/5 (64-bit)"
	@echo "  make cross-rpi32             Raspberry Pi 2/3/Zero 2 W (32-bit)"
	@echo "  make cross-rpi-zero          Raspberry Pi Zero/1 (ARMv6)"
	@echo "  make cross-windows           Windows x86_64 (CPU-only, cross-compile)"
	@echo "  make cross-windows-native    Windows builds (see all variants)"
	@echo "  make cross-all               Build ALL platforms (excl. Windows native)"
	@echo ""
	@echo "Docker Build (local only):"
	@echo "  make build-arm64-metal          Base image (BYOM)"
	@echo "  make build-arm64-metal-bge      With embedded BGE model"
	@echo "  make build-arm64-metal-headless Headless (no UI)"
	@echo "  make build-amd64-cuda           Base image (BYOM)"
	@echo "  make build-amd64-cuda-bge       With embedded BGE model"
	@echo "  make build-amd64-cuda-headless  Headless (no UI)"
	@echo "  make build-amd64-cpu            CPU-only (no GPU, no embeddings)"
	@echo "  make build-amd64-cpu-headless   CPU-only headless"
	@echo "  make build-arm64-all            Build all ARM64 variants"
	@echo "  make build-amd64-all            Build all AMD64 variants"
	@echo "  make build-all                  Build all variants for $(HOST_ARCH)"
	@echo ""
	@echo "Docker Deploy (build + push):"
	@echo "  make deploy-arm64-metal         Deploy base ARM64"
	@echo "  make deploy-arm64-metal-bge     Deploy ARM64 with BGE"
	@echo "  make deploy-arm64-metal-headless Deploy ARM64 headless"
	@echo "  make deploy-amd64-cuda          Deploy base AMD64"
	@echo "  make deploy-amd64-cuda-bge      Deploy AMD64 with BGE"
	@echo "  make deploy-amd64-cuda-headless Deploy AMD64 headless"
	@echo "  make deploy-amd64-cpu           Deploy AMD64 CPU-only"
	@echo "  make deploy-amd64-cpu-headless  Deploy AMD64 CPU-only headless"
	@echo "  make deploy-arm64-all           Deploy all ARM64 variants"
	@echo "  make deploy-amd64-all           Deploy all AMD64 variants"
	@echo "  make deploy-all                 Deploy all variants for $(HOST_ARCH)"
	@echo ""
	@echo "CUDA prereq (one-time, run on x86 machine):"
	@echo "  make build-llama-cuda"
	@echo "  make deploy-llama-cuda"
	@echo ""
	@echo "Headless mode:"
	@echo "  - Build:  -tags noui (excludes UI from binary)"
	@echo "  - Docker: --build-arg HEADLESS=true"
	@echo "  - Runtime: --headless flag or NORNICDB_HEADLESS=true"
	@echo ""
	@echo "CPU-only mode (amd64-cpu):"
	@echo "  - No CUDA/GPU support"
	@echo "  - Embeddings disabled by default (NORNICDB_EMBEDDING_PROVIDER=none)"
	@echo "  - Smallest image size"
	@echo ""
	@echo "Config: REGISTRY=name VERSION=tag make ..."

# ==============================================================================
# macOS Native Integration
# ==============================================================================
# Service installation, menu bar app, and distribution

.PHONY: macos-menubar macos-install macos-uninstall macos-all macos-clean

# Build the menu bar app (requires Xcode)
macos-menubar:
	@echo "Building macOS Menu Bar App..."
ifeq ($(HOST_OS),darwin)
	@echo "Architecture: $(HOST_ARCH)"
	@mkdir -p macos/build
	@cd macos/MenuBarApp && swiftc -o ../build/NornicDB NornicDBMenuBar.swift \
		-framework SwiftUI \
		-framework AppKit \
		-target $(HOST_ARCH)-apple-macos12.0 \
		-swift-version 5 \
		-parse-as-library
	@echo "Creating app bundle..."
	@mkdir -p macos/build/NornicDB.app/Contents/MacOS
	@mkdir -p macos/build/NornicDB.app/Contents/Resources
	@mv macos/build/NornicDB macos/build/NornicDB.app/Contents/MacOS/
	@echo "Generating app icon..."
	@if [ -f "macos/Assets/NornicDB.icns" ]; then \
		cp macos/Assets/NornicDB.icns macos/build/NornicDB.app/Contents/Resources/; \
		echo "  ✓ Using custom icon"; \
	elif command -v sips >/dev/null 2>&1 && [ -f "docs/assets/logos/nornicdb-logo.svg" ]; then \
		mkdir -p macos/build/temp.iconset; \
		for size in 16 32 128 256 512; do \
			qlmanage -t -s $$size -o macos/build/temp.iconset docs/assets/logos/nornicdb-logo.svg >/dev/null 2>&1 || true; \
		done; \
		if [ -f "macos/build/temp.iconset/nornicdb-logo.svg.png" ]; then \
			mv macos/build/temp.iconset/nornicdb-logo.svg.png macos/build/NornicDB.app/Contents/Resources/AppIcon.png; \
		fi; \
		rm -rf macos/build/temp.iconset; \
		echo "  ✓ Generated icon from SVG"; \
	else \
		echo "  ⚠ Using SF Symbol icon (install librsvg for custom icon)"; \
	fi
	@echo '<?xml version="1.0" encoding="UTF-8"?>' > macos/build/NornicDB.app/Contents/Info.plist
	@echo '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">' >> macos/build/NornicDB.app/Contents/Info.plist
	@echo '<plist version="1.0"><dict>' >> macos/build/NornicDB.app/Contents/Info.plist
	@echo '<key>CFBundleExecutable</key><string>NornicDB</string>' >> macos/build/NornicDB.app/Contents/Info.plist
	@echo '<key>CFBundleIdentifier</key><string>com.nornicdb.menubar</string>' >> macos/build/NornicDB.app/Contents/Info.plist
	@echo '<key>CFBundleName</key><string>NornicDB</string>' >> macos/build/NornicDB.app/Contents/Info.plist
	@echo '<key>CFBundleVersion</key><string>1.0.0</string>' >> macos/build/NornicDB.app/Contents/Info.plist
	@if [ -f "macos/build/NornicDB.app/Contents/Resources/NornicDB.icns" ]; then \
		echo '<key>CFBundleIconFile</key><string>NornicDB</string>' >> macos/build/NornicDB.app/Contents/Info.plist; \
	fi
	@echo '<key>LSUIElement</key><true/>' >> macos/build/NornicDB.app/Contents/Info.plist
	@echo '<key>NSHighResolutionCapable</key><true/>' >> macos/build/NornicDB.app/Contents/Info.plist
	@echo '</dict></plist>' >> macos/build/NornicDB.app/Contents/Info.plist
	@echo "✅ Menu bar app built: macos/build/NornicDB.app"
else
	@echo "❌ Menu bar app can only be built on macOS"
	@exit 1
endif

# Install NornicDB as a macOS service
macos-install:
	@echo "Installing NornicDB for macOS..."
ifeq ($(HOST_OS),darwin)
	@./macos/scripts/install.sh
else
	@echo "❌ macOS installation is only available on macOS"
	@exit 1
endif

# Uninstall NornicDB from macOS
macos-uninstall:
	@echo "Uninstalling NornicDB from macOS..."
ifeq ($(HOST_OS),darwin)
	@./macos/scripts/uninstall.sh
else
	@echo "❌ This uninstaller is for macOS only"
	@exit 1
endif

# Build everything and install (one command)
macos-all: build macos-menubar macos-install
	@echo "✅ NornicDB fully installed on macOS!"

# Clean macOS build artifacts
macos-clean:
	@echo "Cleaning macOS build artifacts..."
	@rm -rf macos/build
	@echo "✅ Cleaned"

# Create distributable .pkg installer (uses build-installer.sh)
macos-package: build macos-menubar
	@echo "Creating macOS package installer..."
ifeq ($(HOST_OS),darwin)
	@./macos/scripts/build-installer.sh
else
	@echo "❌ Package creation is only available on macOS"
	@exit 1
endif

# Create signed .pkg for distribution (requires Apple Developer account)
macos-package-signed: macos-package
	@echo "Signing package..."
ifeq ($(HOST_OS),darwin)
	@if [ -z "$(SIGN_IDENTITY)" ]; then \
		echo "❌ Error: SIGN_IDENTITY not set"; \
		echo "   Usage: make macos-package-signed SIGN_IDENTITY='Developer ID Installer: Your Name'"; \
		exit 1; \
	fi
	@productsign --sign "$(SIGN_IDENTITY)" \
		dist/NornicDB-*-$(ARCH).pkg \
		dist/NornicDB-*-$(ARCH)-signed.pkg
	@echo "✅ Signed package created"
else
	@echo "❌ Signing is only available on macOS"
	@exit 1
endif
