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
BUILD_COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
BUILD_TIME ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || echo unknown)
BUILD_LDFLAGS := -X github.com/orneryd/nornicdb/pkg/buildinfo.Commit=$(BUILD_COMMIT) -X github.com/orneryd/nornicdb/pkg/buildinfo.BuildTime=$(BUILD_TIME)
MACOS_APP_VERSION := $(shell if [ -n "$(VERSION)" ] && [ "$(VERSION)" != "latest" ]; then printf "%s" "$(VERSION)" | sed 's/^v//'; elif [ -f pkg/buildinfo/VERSION ]; then tr -d '[:space:]' < pkg/buildinfo/VERSION; else echo "0.0.0"; fi)

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
IMAGE_CPU_BGE := $(REGISTRY)/nornicdb-cpu-bge:$(VERSION)
IMAGE_CPU_BGE_HEADLESS := $(REGISTRY)/nornicdb-cpu-bge-headless:$(VERSION)
IMAGE_AMD64_VULKAN := $(REGISTRY)/nornicdb-amd64-vulkan:$(VERSION)
IMAGE_AMD64_VULKAN_BGE := $(REGISTRY)/nornicdb-amd64-vulkan-bge:$(VERSION)
IMAGE_AMD64_VULKAN_HEADLESS := $(REGISTRY)/nornicdb-amd64-vulkan-headless:$(VERSION)
LLAMA_VERSION ?= b9106
LLAMA_CPU := $(REGISTRY)/llama-cpu-libs:$(LLAMA_VERSION)
LLAMA_CUDA := $(REGISTRY)/llama-cuda-libs:$(LLAMA_VERSION)

# Dockerfiles
DOCKER_DIR := docker

# Model URLs and paths
MODELS_DIR := models
BGE_MODEL := $(MODELS_DIR)/bge-m3.gguf
QWEN_MODEL := $(MODELS_DIR)/qwen3-0.6b-instruct.gguf
# Reranker: download target uses Q4_K_M; Dockerfiles accept either bge-reranker-v2-m3.gguf or bge-reranker-v2-m3-Q4_K_M.gguf
BGE_RERANKER_MODEL := $(MODELS_DIR)/bge-reranker-v2-m3-Q4_K_M.gguf
BGE_URL := https://huggingface.co/gpustack/bge-m3-GGUF/resolve/main/bge-m3-Q4_K_M.gguf
QWEN_URL := https://huggingface.co/unsloth/Qwen3-0.6B-GGUF/resolve/main/Qwen3-0.6B-Q4_K_M.gguf
BGE_RERANKER_URL := https://huggingface.co/gpustack/bge-reranker-v2-m3-GGUF/resolve/main/bge-reranker-v2-m3-Q4_K_M.gguf

.PHONY: build-arm64-metal build-arm64-metal-bge build-arm64-metal-bge-heimdall build-arm64-metal-headless
.PHONY: build-amd64-cuda build-amd64-cuda-bge build-amd64-cuda-bge-heimdall build-amd64-cuda-headless
.PHONY: build-amd64-cpu build-amd64-cpu-headless
.PHONY: build-cpu-bge build-cpu-bge-headless
.PHONY: build-amd64-vulkan build-amd64-vulkan-bge build-amd64-vulkan-headless
.PHONY: build-all build-arm64-all build-amd64-all
.PHONY: push-arm64-metal push-arm64-metal-bge push-arm64-metal-bge-heimdall push-arm64-metal-headless
.PHONY: push-amd64-cuda push-amd64-cuda-bge push-amd64-cuda-bge-heimdall push-amd64-cuda-headless
.PHONY: push-amd64-cpu push-amd64-cpu-headless
.PHONY: push-cpu-bge push-cpu-bge-headless
.PHONY: deploy-arm64-metal deploy-arm64-metal-bge deploy-arm64-metal-bge-heimdall deploy-arm64-metal-headless
.PHONY: deploy-amd64-cuda deploy-amd64-cuda-bge deploy-amd64-cuda-bge-heimdall deploy-amd64-cuda-headless
.PHONY: deploy-amd64-cpu deploy-amd64-cpu-headless
.PHONY: deploy-cpu-bge deploy-cpu-bge-headless
.PHONY: deploy-amd64-vulkan deploy-amd64-vulkan-bge deploy-amd64-vulkan-headless
.PHONY: deploy-all deploy-arm64-all deploy-amd64-all
.PHONY: build-llama-cpu push-llama-cpu deploy-llama-cpu ensure-llama-cpu
.PHONY: build-llama-cuda push-llama-cuda deploy-llama-cuda ensure-llama-cuda
.PHONY: build build-ui build-binary build-localllm build-headless build-localllm-headless sync-version test lint-slog install-hooks clean images help macos-menubar macos-install macos-uninstall macos-all macos-clean macos-package macos-package-lite macos-package-full macos-package-all macos-package-signed
.PHONY: download-models download-bge download-qwen download-bge-reranker check-models
.PHONY: antlr-generate antlr-clean antlr-test antlr-test-full test-parsers

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
	@powershell -NoProfile -Command " \
		$$modelPath = '$(QWEN_MODEL)'; \
		$$url = '$(QWEN_URL)'; \
		function Test-GGUF([string]$$path) { \
			if (-not (Test-Path $$path)) { return $$false } \
			$$bytes = [System.IO.File]::ReadAllBytes($$path); \
			if ($$bytes.Length -lt 4) { return $$false } \
			return $$bytes[0] -eq 71 -and $$bytes[1] -eq 71 -and $$bytes[2] -eq 85 -and $$bytes[3] -eq 70; \
		} \
		if (-not (Test-GGUF $$modelPath)) { \
			if (Test-Path $$modelPath) { \
				Write-Host 'Removing invalid existing Qwen model:' $$modelPath; \
				Remove-Item $$modelPath -Force; \
			} \
			Write-Host '==============================================================='; \
			Write-Host ' Downloading qwen3-0.6b-Instruct model...'; \
			Write-Host '==============================================================='; \
			Write-Host 'Source:' $$url; \
			Write-Host 'Target:' $$modelPath; \
			Write-Host 'Size: ~350MB (this may take a few minutes)'; \
			Invoke-WebRequest -Uri $$url -OutFile $$modelPath; \
			if (-not (Test-GGUF $$modelPath)) { \
				if (Test-Path $$modelPath) { Remove-Item $$modelPath -Force } \
				throw 'Downloaded file is not a valid GGUF model'; \
			} \
			Write-Host 'Downloaded' $$modelPath; \
		} else { \
			Write-Host 'Qwen model already exists and is valid:' $$modelPath; \
		} \
	"
else
	@if [ ! -f "$(QWEN_MODEL)" ] || ! head -c 4 "$(QWEN_MODEL)" 2>/dev/null | grep -q '^GGUF$$'; then \
		if [ -f "$(QWEN_MODEL)" ]; then \
			echo "Removing invalid existing Qwen model: $(QWEN_MODEL)"; \
			rm -f "$(QWEN_MODEL)"; \
		fi; \
		echo "==============================================================="; \
		echo " Downloading qwen3-0.6b-Instruct model..."; \
		echo "==============================================================="; \
		echo "Source: $(QWEN_URL)"; \
		echo "Target: $(QWEN_MODEL)"; \
		echo "Size: ~350MB (this may take a few minutes)"; \
		curl -fL --progress-bar "$(QWEN_URL)" -o "$(QWEN_MODEL)"; \
		if ! head -c 4 "$(QWEN_MODEL)" 2>/dev/null | grep -q '^GGUF$$'; then \
			echo "Downloaded file is not a valid GGUF model: $(QWEN_MODEL)"; \
			rm -f "$(QWEN_MODEL)"; \
			exit 1; \
		fi; \
		echo "Downloaded $(QWEN_MODEL)"; \
	else \
		echo "Qwen model already exists and is valid: $(QWEN_MODEL)"; \
	fi
endif

# Download BGE-Reranker-v2-m3 for Stage-2 search reranking (NORNICDB_SEARCH_RERANK_ENABLED)
download-bge-reranker: $(MODELS_DIR)
ifeq ($(HOST_OS),windows)
	@if not exist "$(BGE_RERANKER_MODEL)" ( \
		echo =============================================================== && \
		echo  Downloading BGE-Reranker-v2-m3 model... && \
		echo =============================================================== && \
		echo Source: $(BGE_RERANKER_URL) && \
		echo Target: $(BGE_RERANKER_MODEL) && \
		powershell -Command "Invoke-WebRequest -Uri '$(BGE_RERANKER_URL)' -OutFile '$(BGE_RERANKER_MODEL)'" && \
		echo Downloaded $(BGE_RERANKER_MODEL) \
	) else ( \
		echo BGE-Reranker model already exists: $(BGE_RERANKER_MODEL) \
	)
else
	@if [ ! -f "$(BGE_RERANKER_MODEL)" ]; then \
		echo "==============================================================="; \
		echo " Downloading BGE-Reranker-v2-m3 model..."; \
		echo "==============================================================="; \
		echo "Source: $(BGE_RERANKER_URL)"; \
		echo "Target: $(BGE_RERANKER_MODEL)"; \
		curl -L --progress-bar "$(BGE_RERANKER_URL)" -o "$(BGE_RERANKER_MODEL)"; \
		echo "Downloaded $(BGE_RERANKER_MODEL)"; \
	else \
		echo "BGE-Reranker model already exists: $(BGE_RERANKER_MODEL)"; \
	fi
endif

# Download all models if missing (BGE + Qwen for Heimdall + BGE reranker for search)
download-models: download-bge download-qwen download-bge-reranker
	@echo ""
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ ✓ All Heimdall + search models ready                         ║"
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
	@if exist "$(BGE_RERANKER_MODEL)" (echo BGE-Reranker model: $(BGE_RERANKER_MODEL)) else (if exist "$(MODELS_DIR)\bge-reranker-v2-m3.gguf" (echo BGE-Reranker model: $(MODELS_DIR)\bge-reranker-v2-m3.gguf) else (echo BGE-Reranker model missing && echo   Run: make download-bge-reranker))
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
	@if [ -f "$(BGE_RERANKER_MODEL)" ] || [ -f "$(MODELS_DIR)/bge-reranker-v2-m3.gguf" ]; then \
		echo "✓ BGE-Reranker model: $(BGE_RERANKER_MODEL) or $(MODELS_DIR)/bge-reranker-v2-m3.gguf"; \
	else \
		echo "✗ BGE-Reranker model missing (need $(BGE_RERANKER_MODEL) or $(MODELS_DIR)/bge-reranker-v2-m3.gguf)"; \
		echo "  Run: make download-bge-reranker"; \
	fi
endif

# ==============================================================================
# Build (local only, no push)
# ==============================================================================

build-arm64-metal: ensure-llama-cpu
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_ARM64) [BYOM]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --platform linux/arm64 --build-arg LLAMA_CPU_IMAGE=$(LLAMA_CPU) -t $(IMAGE_ARM64) -f $(DOCKER_DIR)/Dockerfile.arm64-metal .

build-arm64-metal-bge: ensure-llama-cpu download-bge download-bge-reranker
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_ARM64_BGE) [with BGE model]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --platform linux/arm64 --build-arg LLAMA_CPU_IMAGE=$(LLAMA_CPU) --build-arg EMBED_MODEL=true -t $(IMAGE_ARM64_BGE) -f $(DOCKER_DIR)/Dockerfile.arm64-metal .

build-arm64-metal-bge-heimdall: ensure-llama-cpu download-models
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_ARM64_BGE_HEIMDALL) [BGE + Heimdall]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --platform linux/arm64 --build-arg LLAMA_CPU_IMAGE=$(LLAMA_CPU) -t $(IMAGE_ARM64_BGE_HEIMDALL) -f $(DOCKER_DIR)/Dockerfile.arm64-metal-heimdall .

build-arm64-metal-headless: ensure-llama-cpu
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_ARM64_HEADLESS) [headless, no UI]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --platform linux/arm64 --build-arg LLAMA_CPU_IMAGE=$(LLAMA_CPU) --build-arg HEADLESS=true -t $(IMAGE_ARM64_HEADLESS) -f $(DOCKER_DIR)/Dockerfile.arm64-metal .

build-amd64-cuda: ensure-llama-cuda
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_AMD64) [BYOM]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --platform linux/amd64 --build-arg LLAMA_CUDA_IMAGE=$(LLAMA_CUDA) -t $(IMAGE_AMD64) -f $(DOCKER_DIR)/Dockerfile.amd64-cuda .

build-amd64-cuda-bge: ensure-llama-cuda download-bge download-bge-reranker
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_AMD64_BGE) [with BGE model]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --platform linux/amd64 --build-arg LLAMA_CUDA_IMAGE=$(LLAMA_CUDA) --build-arg EMBED_MODEL=true -t $(IMAGE_AMD64_BGE) -f $(DOCKER_DIR)/Dockerfile.amd64-cuda .

build-amd64-cuda-bge-heimdall: ensure-llama-cuda download-models
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_AMD64_BGE_HEIMDALL) [BGE + Heimdall]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --platform linux/amd64 --build-arg LLAMA_CUDA_IMAGE=$(LLAMA_CUDA) -t $(IMAGE_AMD64_BGE_HEIMDALL) -f $(DOCKER_DIR)/Dockerfile.amd64-cuda-heimdall .

build-amd64-cuda-headless: ensure-llama-cuda
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_AMD64_HEADLESS) [headless, no UI]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --platform linux/amd64 --build-arg LLAMA_CUDA_IMAGE=$(LLAMA_CUDA) --build-arg HEADLESS=true -t $(IMAGE_AMD64_HEADLESS) -f $(DOCKER_DIR)/Dockerfile.amd64-cuda .

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

# CPU-only with BGE embeddings + reranker (cross-platform: works on both AMD64 and ARM64)
build-cpu-bge: ensure-llama-cpu download-bge download-bge-reranker
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_CPU_BGE) [CPU + BGE embeddings + reranker]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --build-arg LLAMA_CPU_IMAGE=$(LLAMA_CPU) -t $(IMAGE_CPU_BGE) -f $(DOCKER_DIR)/Dockerfile.cpu-bge .

build-cpu-bge-headless: ensure-llama-cpu download-bge download-bge-reranker
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_CPU_BGE_HEADLESS) [CPU + BGE + reranker, headless]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --build-arg LLAMA_CPU_IMAGE=$(LLAMA_CPU) --build-arg HEADLESS=true -t $(IMAGE_CPU_BGE_HEADLESS) -f $(DOCKER_DIR)/Dockerfile.cpu-bge .

# AMD64 Vulkan (any GPU: NVIDIA/AMD/Intel)
build-amd64-vulkan:
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_AMD64_VULKAN) [Vulkan GPU, BYOM]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --platform linux/amd64 -t $(IMAGE_AMD64_VULKAN) -f $(DOCKER_DIR)/Dockerfile.amd64-vulkan .

build-amd64-vulkan-bge: download-bge download-bge-reranker
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_AMD64_VULKAN_BGE) [Vulkan + BGE model]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --platform linux/amd64 --build-arg EMBED_MODEL=true -t $(IMAGE_AMD64_VULKAN_BGE) -f $(DOCKER_DIR)/Dockerfile.amd64-vulkan .

build-amd64-vulkan-headless:
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_AMD64_VULKAN_HEADLESS) [Vulkan, headless]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --platform linux/amd64 --build-arg HEADLESS=true -t $(IMAGE_AMD64_VULKAN_HEADLESS) -f $(DOCKER_DIR)/Dockerfile.amd64-vulkan .

# Build both variants for an architecture
build-arm64-all: build-arm64-metal build-arm64-metal-bge build-arm64-metal-headless
	@echo "✓ Built all ARM64 Metal images"

build-amd64-all: build-amd64-cuda build-amd64-cuda-bge build-amd64-cuda-headless build-amd64-cpu build-amd64-cpu-headless build-amd64-vulkan build-amd64-vulkan-bge build-amd64-vulkan-headless
	@echo "✓ Built all AMD64 images (CUDA + CPU + Vulkan)"

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

push-cpu-bge:
	@echo "→ Pushing $(IMAGE_CPU_BGE)"
	docker push $(IMAGE_CPU_BGE)

push-cpu-bge-headless:
	@echo "→ Pushing $(IMAGE_CPU_BGE_HEADLESS)"
	docker push $(IMAGE_CPU_BGE_HEADLESS)

push-amd64-vulkan:
	@echo "→ Pushing $(IMAGE_AMD64_VULKAN)"
	docker push $(IMAGE_AMD64_VULKAN)

push-amd64-vulkan-bge:
	@echo "→ Pushing $(IMAGE_AMD64_VULKAN_BGE)"
	docker push $(IMAGE_AMD64_VULKAN_BGE)

push-amd64-vulkan-headless:
	@echo "→ Pushing $(IMAGE_AMD64_VULKAN_HEADLESS)"
	docker push $(IMAGE_AMD64_VULKAN_HEADLESS)

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

deploy-cpu-bge: build-cpu-bge push-cpu-bge
	@echo "✓ Deployed $(IMAGE_CPU_BGE)"
	@echo "🧠 CPU-only with local BGE embeddings enabled"

deploy-cpu-bge-headless: build-cpu-bge-headless push-cpu-bge-headless
	@echo "✓ Deployed $(IMAGE_CPU_BGE_HEADLESS)"

deploy-amd64-vulkan: build-amd64-vulkan push-amd64-vulkan
	@echo "✓ Deployed $(IMAGE_AMD64_VULKAN)"

deploy-amd64-vulkan-bge: build-amd64-vulkan-bge push-amd64-vulkan-bge
	@echo "✓ Deployed $(IMAGE_AMD64_VULKAN_BGE)"

deploy-amd64-vulkan-headless: build-amd64-vulkan-headless push-amd64-vulkan-headless
	@echo "✓ Deployed $(IMAGE_AMD64_VULKAN_HEADLESS)"

# Deploy both variants for an architecture (including headless)
deploy-arm64-all: deploy-arm64-metal deploy-arm64-metal-bge deploy-arm64-metal-headless
	@echo "✓ Deployed all ARM64 Metal images"

deploy-amd64-all: deploy-amd64-cuda deploy-amd64-cuda-bge deploy-amd64-cuda-headless deploy-amd64-cpu deploy-amd64-cpu-headless deploy-amd64-vulkan deploy-amd64-vulkan-bge deploy-amd64-vulkan-headless
	@echo "✓ Deployed all AMD64 images (CUDA + CPU + Vulkan)"

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
# CPU/Metal prerequisite (one-time build)
# ==============================================================================

ensure-llama-cpu:
ifeq ($(HOST_OS),windows)
	@echo Checking CPU libs image: $(LLAMA_CPU)
	@docker image inspect "$(LLAMA_CPU)" >NUL 2>&1 && ( \
		echo Found local image $(LLAMA_CPU) \
	) || ( \
		echo Missing local image $(LLAMA_CPU); attempting to pull... && \
		docker pull "$(LLAMA_CPU)" >NUL 2>&1 && ( \
			echo Pulled $(LLAMA_CPU) \
		) || ( \
			echo Pull failed; building one-time prerequisite locally... && \
			$(MAKE) build-llama-cpu \
		) \
	)
else
	@echo "→ Checking CPU libs image: $(LLAMA_CPU)"
	@if docker image inspect "$(LLAMA_CPU)" > /dev/null 2>&1; then \
		echo "✓ Found local image $(LLAMA_CPU)"; \
	else \
		echo "→ Missing local image $(LLAMA_CPU); attempting to pull..."; \
		if docker pull "$(LLAMA_CPU)" > /dev/null 2>&1; then \
			echo "✓ Pulled $(LLAMA_CPU)"; \
		else \
			echo "→ Pull failed; building one-time prerequisite locally..."; \
			$(MAKE) build-llama-cpu; \
		fi; \
	fi
endif

build-llama-cpu:
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building CPU libs (one-time): $(LLAMA_CPU)"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --platform linux/$(HOST_ARCH) -t $(LLAMA_CPU) -f $(DOCKER_DIR)/Dockerfile.llama-cpu .

push-llama-cpu:
	@echo "→ Pushing $(LLAMA_CPU)"
	docker push $(LLAMA_CPU)

deploy-llama-cpu: build-llama-cpu push-llama-cpu
	@echo "✓ Deployed $(LLAMA_CPU)"

# ==============================================================================
# CUDA Prerequisite (one-time build, ~15 min)
# ==============================================================================

ensure-llama-cuda:
ifeq ($(HOST_OS),windows)
	@echo Checking CUDA libs image: $(LLAMA_CUDA)
	@docker image inspect "$(LLAMA_CUDA)" >NUL 2>&1 && ( \
		echo Found local image $(LLAMA_CUDA) \
	) || ( \
		echo Missing local image $(LLAMA_CUDA); attempting to pull... && \
		docker pull "$(LLAMA_CUDA)" >NUL 2>&1 && ( \
			echo Pulled $(LLAMA_CUDA) \
		) || ( \
			echo Pull failed; building one-time prerequisite locally... && \
			$(MAKE) build-llama-cuda \
		) \
	)
else
	@echo "→ Checking CUDA libs image: $(LLAMA_CUDA)"
	@if docker image inspect "$(LLAMA_CUDA)" > /dev/null 2>&1; then \
		echo "✓ Found local image $(LLAMA_CUDA)"; \
	else \
		echo "→ Missing local image $(LLAMA_CUDA); attempting to pull..."; \
		if docker pull "$(LLAMA_CUDA)" > /dev/null 2>&1; then \
			echo "✓ Pulled $(LLAMA_CUDA)"; \
		else \
			echo "→ Pull failed; building one-time prerequisite locally..."; \
			$(MAKE) build-llama-cuda; \
		fi; \
	fi
endif

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

sync-version:
ifeq ($(HOST_OS),windows)
	@powershell -NoProfile -ExecutionPolicy Bypass -Command "$$env:VERSION='$(VERSION)'; & '$(CURDIR)\scripts\sync-version.ps1'"
else
	@VERSION="$(VERSION)" sh ./scripts/sync-version.sh
endif

# Build UI assets first
build-ui:
	@echo "Building UI assets..."
	@cd ui && npm install && npm run build
	@echo "✓ UI built successfully"

# Build NornicDB binary + APOC plugins (Windows: GPU-enabled via yzma, others: with localllm)
build: build-ui build-binary build-plugins-if-supported
	@echo "==============================================================="
	@echo " Build complete!"
	@echo "==============================================================="
ifeq ($(HOST_OS),windows)
	@echo ""
	@echo "Binary: bin\nornicdb.exe (GPU-enabled build via yzma)"
	@echo "GPU Support: CUDA (NVIDIA), Vulkan (AMD/NVIDIA, build with Vulkan flags)"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Install CUDA libraries (if not done):"
	@echo "       .\scripts\setup-windows-cuda.bat"
	@echo ""
	@echo "  2. Set library path:"
	@echo "       $$env:NORNICDB_LIB = '.\lib\llama'"
	@echo ""
	@echo "  3. Run with GPU detection:"
	@echo "       .\bin\nornicdb.exe serve --no-auth"
	@echo "       .\bin\nornicdb.exe --check-gpu"
	@echo ""
	@echo "  4. Connect with Neo4j drivers:"
	@echo "       bolt://localhost:7687"
	@echo ""
	@echo "  Note: For local embeddings, download model:"
	@echo "       make download-bge"
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
	@echo "       Password: password"
	@echo ""
endif

# Check and build llama.cpp library if not present or outdated
check-llama-lib:
ifeq ($(HOST_OS),windows)
	@echo "Windows: Using yzma runtime GPU libraries (no compile-time linking)"
	@echo "  CUDA (NVIDIA):  Run scripts\setup-windows-cuda.bat"
	@echo "  Vulkan (AMD/Intel/NVIDIA): Run scripts\setup-windows-vulkan.bat"
	@echo ""
	@echo "  To force Vulkan build: set NORNICDB_GPU_BACKEND=vulkan before 'make build'"
else
	@if [ ! -f lib/llama/libllama_$(HOST_OS)_$(HOST_ARCH).a ]; then \
		echo "⚠️  llama.cpp library not found, building..."; \
		./scripts/build-llama.sh $(LLAMA_VERSION); \
	elif [ ! -f lib/llama/VERSION ] || [ "$$(cat lib/llama/VERSION 2>/dev/null)" != "$(LLAMA_VERSION)" ]; then \
		echo "⚠️  llama.cpp library version mismatch (have $$(cat lib/llama/VERSION 2>/dev/null || echo unknown), need $(LLAMA_VERSION)), rebuilding..."; \
		./scripts/build-llama.sh $(LLAMA_VERSION); \
	elif ! nm lib/llama/libllama_$(HOST_OS)_$(HOST_ARCH).a 2>/dev/null | grep -q "llama_get_memory"; then \
		echo "⚠️  llama.cpp library outdated (missing llama_get_memory), rebuilding..."; \
		./scripts/build-llama.sh $(LLAMA_VERSION); \
	else \
		echo "✓ llama.cpp library up to date ($(LLAMA_VERSION))"; \
	fi
endif

# Build OAuth provider for local testing
build-oauth-provider:
	@echo "Building OAuth provider..."
	go build -o bin/oauth-provider$(BIN_EXT) ./cmd/oauth-provider
	@echo "✓ Built: bin/oauth-provider$(BIN_EXT)"

build-swagger-ui:
	@echo "Building Swagger UI server..."
	go build -o bin/swagger-ui$(BIN_EXT) ./cmd/swagger-ui
	@echo "✓ Built: bin/swagger-ui$(BIN_EXT)"

build-binary: sync-version check-llama-lib
ifeq ($(HOST_OS),windows)
	@echo "Detecting GPU support..."
	@powershell -Command " \
		$$forceBackend = $$env:NORNICDB_GPU_BACKEND; \
		$$cuda = $$false; \
		$$vulkan = $$false; \
		if ($$forceBackend) { \
			Write-Host '  [**] Forced backend: '$$forceBackend -ForegroundColor Magenta; \
		} \
		if (Get-Command nvcc -ErrorAction SilentlyContinue) { \
			$$cudaVer = (nvcc --version 2>$$null | Select-String 'release' | ForEach-Object { $$_ -replace '.*release ([0-9.]+).*','$$1' }); \
			if ($$cudaVer) { \
				Write-Host '  [OK] CUDA Toolkit detected: v'$$cudaVer -ForegroundColor Green; \
				$$cuda = $$true; \
			} \
		} \
		if (-not $$cuda) { \
			if (Get-Command nvidia-smi -ErrorAction SilentlyContinue) { \
				Write-Host '  [!!] NVIDIA GPU detected but CUDA Toolkit not installed' -ForegroundColor Yellow; \
				Write-Host '       Install from: https://developer.nvidia.com/cuda-downloads' -ForegroundColor Yellow; \
			} else { \
				Write-Host '  [--] No NVIDIA GPU/CUDA detected' -ForegroundColor Gray; \
			} \
		} \
		if (Get-Command vulkaninfo -ErrorAction SilentlyContinue) { \
			Write-Host '  [OK] Vulkan SDK detected' -ForegroundColor Green; \
			$$vulkan = $$true; \
		} elseif (Test-Path 'C:\\VulkanSDK') { \
			Write-Host '  [OK] Vulkan SDK found at C:\\VulkanSDK' -ForegroundColor Green; \
			$$vulkan = $$true; \
		} else { \
			Write-Host '  [--] No Vulkan SDK detected' -ForegroundColor Gray; \
		} \
		Write-Host ''; \
		if ($$forceBackend -eq 'vulkan' -and $$vulkan) { \
			Write-Host 'Building with Vulkan support (forced)...' -ForegroundColor Cyan; \
			Write-Host 'GPU_BACKEND=vulkan' | Out-File -FilePath .gpu-backend -Encoding ascii; \
		} elseif ($$forceBackend -eq 'cuda' -and $$cuda) { \
			Write-Host 'Building with CUDA support (forced)...' -ForegroundColor Cyan; \
			Write-Host 'GPU_BACKEND=cuda' | Out-File -FilePath .gpu-backend -Encoding ascii; \
		} elseif ($$forceBackend -eq 'cpu') { \
			Write-Host 'Building CPU-only (forced)...' -ForegroundColor Yellow; \
			Write-Host 'GPU_BACKEND=cpu' | Out-File -FilePath .gpu-backend -Encoding ascii; \
		} elseif ($$cuda) { \
			Write-Host 'Building with CUDA support (NVIDIA GPU)...' -ForegroundColor Cyan; \
			Write-Host 'GPU_BACKEND=cuda' | Out-File -FilePath .gpu-backend -Encoding ascii; \
		} elseif ($$vulkan) { \
			Write-Host 'Building with Vulkan support (Universal GPU)...' -ForegroundColor Cyan; \
			Write-Host 'GPU_BACKEND=vulkan' | Out-File -FilePath .gpu-backend -Encoding ascii; \
		} else { \
			Write-Host 'Building CPU-only (no GPU acceleration)...' -ForegroundColor Yellow; \
			Write-Host 'GPU_BACKEND=cpu' | Out-File -FilePath .gpu-backend -Encoding ascii; \
		} \
	"
	@go build -ldflags "$(BUILD_LDFLAGS)" -o bin/nornicdb$(BIN_EXT) ./cmd/nornicdb
	@powershell -Command " \
		$$backend = (Get-Content .gpu-backend -ErrorAction SilentlyContinue) -replace 'GPU_BACKEND=',''; \
		Remove-Item .gpu-backend -ErrorAction SilentlyContinue; \
		Write-Host ''; \
		if ($$backend -eq 'cuda') { \
			Write-Host 'Next: Run scripts\\setup-windows-cuda.bat to install runtime libraries' -ForegroundColor Green; \
		} elseif ($$backend -eq 'vulkan') { \
			Write-Host 'Next: Run scripts\\setup-windows-vulkan.bat to install runtime libraries' -ForegroundColor Green; \
		} else { \
			Write-Host 'Note: GPU acceleration disabled. For better performance:' -ForegroundColor Yellow; \
			Write-Host '  NVIDIA: Install CUDA Toolkit from https://developer.nvidia.com/cuda-downloads' -ForegroundColor Yellow; \
			Write-Host '  AMD/Intel: Install Vulkan SDK from https://vulkan.lunarg.com/sdk/home' -ForegroundColor Yellow; \
		} \
	"
else
ifeq ($(HOST_OS),linux)
	CGO_ENABLED=1 CGO_LDFLAGS="-Wl,-no-pie" go build -ldflags "$(BUILD_LDFLAGS)" -tags localllm -o bin/nornicdb$(BIN_EXT) ./cmd/nornicdb
else
	CGO_ENABLED=1 go build -ldflags "$(BUILD_LDFLAGS)" -tags localllm -o bin/nornicdb$(BIN_EXT) ./cmd/nornicdb
endif
endif

# Build plugins only if platform supports Go plugins (Linux/macOS, not Windows)
build-plugins-if-supported:
ifeq ($(OS),Windows_NT)
	@echo "Note: Go plugins not supported on Windows, skipping plugin build"
else
	@$(MAKE) plugins
endif

build-localllm: sync-version check-llama-lib build-plugins-if-supported
ifeq ($(HOST_OS),windows)
	@echo "Note: On Windows, build-localllm requires manual llama.cpp setup"
	@echo "Run: powershell -ExecutionPolicy Bypass -File scripts\\build-llama-cuda.ps1"
	@set CGO_ENABLED=1 && go build -ldflags "$(BUILD_LDFLAGS)" -tags "localllm" -o bin/nornicdb$(BIN_EXT) ./cmd/nornicdb
else
ifeq ($(HOST_OS),linux)
	CGO_ENABLED=1 CGO_LDFLAGS="-Wl,-no-pie" go build -ldflags "$(BUILD_LDFLAGS)" -tags localllm -o bin/nornicdb$(BIN_EXT) ./cmd/nornicdb
else
	CGO_ENABLED=1 go build -ldflags "$(BUILD_LDFLAGS)" -tags localllm -o bin/nornicdb$(BIN_EXT) ./cmd/nornicdb
endif
endif

# Build without UI (headless mode)
build-headless: sync-version build-plugins-if-supported
	go build -ldflags "$(BUILD_LDFLAGS)" -tags noui -o bin/nornicdb-headless$(BIN_EXT) ./cmd/nornicdb

build-localllm-headless: sync-version check-llama-lib build-plugins-if-supported
ifeq ($(HOST_OS),windows)
	@echo "Note: On Windows, build-localllm-headless requires manual llama.cpp setup"
	@echo "Run: powershell -ExecutionPolicy Bypass -File scripts\\build-llama-cuda.ps1"
	@set CGO_ENABLED=1 && go build -ldflags "$(BUILD_LDFLAGS)" -tags "localllm noui" -o bin/nornicdb-headless$(BIN_EXT) ./cmd/nornicdb
else
ifeq ($(HOST_OS),linux)
	CGO_ENABLED=1 CGO_LDFLAGS="-Wl" go build -ldflags "$(BUILD_LDFLAGS)" -tags "localllm noui" -o bin/nornicdb-headless$(BIN_EXT) ./cmd/nornicdb
else
	CGO_ENABLED=1 go build -ldflags "$(BUILD_LDFLAGS)" -tags "localllm noui" -o bin/nornicdb-headless$(BIN_EXT) ./cmd/nornicdb
endif
endif

#
# Rejects two forbidden patterns in the four LOG-01 business packages
# (and cmd/nornicdb), so the slog migration cannot regress in CI:
#
#   1. `slog.Default()` — every business package MUST consume an injected
#      *slog.Logger (constructor injection per D-01); reaching into the
#      process-global default logger bypasses the redaction / mandatory-
#      fields / recovering handler stack.
#   2. `log.Printf` / `log.Println` / `fmt.Print` / `fmt.Println` /
#      `fmt.Printf` — the LOG-01 grep-zero contract; production logging
#      must flow through slog so the 4-layer handler stack applies.
#
# POSIX-portable grep only: `-RnE` (POSIX ERE), no `-P` (Perl regex —
# unsupported on BSD grep on macOS per W2 / Pitfall 5). The boundary
# pattern `(^|[^a-zA-Z_])` is the portable analog of `\b` (BSD grep
# treats `\b` inconsistently across versions).
#
# *_test.go is excluded — tests construct fixtures and may legitimately
# call into stdlib log helpers.
lint-slog:
	@! grep -RnE '(^|[^a-zA-Z_])slog\.Default\(' pkg/server pkg/cypher pkg/storage pkg/bolt cmd/ --include='*.go' 2>/dev/null \
		|| (echo "LOG-09 violation: slog.Default() forbidden in business packages — use injected *slog.Logger"; exit 1)
	@! grep -RnE '(^|[^a-zA-Z_])log\.(Printf|Println)|(^|[^a-zA-Z_])fmt\.(Print|Println|Printf)\(' pkg/server pkg/cypher pkg/storage pkg/bolt --include='*.go' --exclude='*_test.go' 2>/dev/null \
		|| (echo "LOG-01 violation: log.Printf|log.Println|fmt.Print|fmt.Println|fmt.Printf forbidden — use injected *slog.Logger"; exit 1)
	@echo "lint-slog: PASS (LOG-09 + LOG-01 gates clean)"

# pattern `(^|[^a-zA-Z_])` is the portable analog of `\b` (BSD grep treats
# `\b` inconsistently across versions; mirrors lint-slog precedent above).
#
# Path list excludes pkg/observability/ — that package legitimately
# constructs raw prometheus.* types (registry.go, etc.); the helper layer
# discipline applies to business-package CALLERS of pkg/observability,
# not pkg/observability itself.
#
# *_test.go is included — test fixtures that register raw *Vec to test
# the helper layer's behavior should also go through the helpers, OR live
# in pkg/observability/ where the lint does not scan.
lint-cardinality:
	@! grep -RnE '(^|[^a-zA-Z_])prometheus\.New(Counter|Gauge|Histogram|Summary)(Vec)?\(' \
		--include='*.go' \
		pkg/cypher pkg/storage pkg/bolt pkg/server pkg/replication pkg/auth pkg/embed pkg/search pkg/cache cmd/ \
		2>/dev/null \
		|| (echo "MET-04 violation: subsystems must register metrics via pkg/observability helpers (D-01 / Plan 03-02); see ADR §3.2 'Cardinality discipline' and pkg/observability/metrics.go"; exit 1)
	@echo "lint-cardinality: PASS (MET-04 helper-only registration enforced)"

test: lint-slog lint-cardinality
ifeq ($(HOST_OS),windows)
	powershell -Command "$$env:GOMEMLIMIT='4GiB'; go test -p 1 -parallel 1 -timeout 30m ./... 2>&1 | Select-String -Pattern '^(FAIL|ok|---\s+FAIL)'; exit $$LASTEXITCODE"
else
	# pipefail propagates `go test`'s real exit through the grep filter,
	# so a test failure still fails the make target even though grep
	# strips most of the noise. `go test ./...` always emits at least one
	# `ok` or `FAIL` line per package, so grep never returns 1 in
	# practice — pipefail's status is `go test`'s status.
	@bash -c 'set -o pipefail; go test -timeout 30m ./... 2>&1 | grep -E "^(FAIL|ok|---[[:space:]]+FAIL)"'
endif

install-hooks:
	bash scripts/install-git-hooks.sh

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

# Windows x86_64 (GPU-enabled via yzma: CUDA + Vulkan support)
cross-windows:
	@echo "Building for Windows x86_64 (GPU-enabled via yzma)..."
	@echo "  GPU Backends: CUDA (NVIDIA), Vulkan (AMD/NVIDIA, build with Vulkan flags)"
ifeq ($(HOST_OS),windows)
	@set CGO_ENABLED=0 && set GOOS=windows && set GOARCH=amd64 && go build -o bin/nornicdb.exe ./cmd/nornicdb
else
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o bin/nornicdb.exe ./cmd/nornicdb
endif
	@echo "bin/nornicdb.exe"
	@echo ""
	@echo "Note: GPU libraries installed separately via:"
	@echo "  scripts\setup-windows-cuda.bat    (NVIDIA CUDA)"
	@echo "  scripts\setup-windows-vulkan.bat  (AMD/NVIDIA Vulkan, build with Vulkan flags)"

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
	@echo "   NORNICDB_EMBEDDING_MODEL=bge-m3 \\"
	@echo "   NORNICDB_EMBEDDING_DIMENSIONS=1024 \\"
	@echo "   NORNICDB_DATA_DIR=./data/test \\"
	@echo "   NORNICDB_EMBEDDING_ENABLED=true \\"
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
	@echo "  make install-hooks           Install repository Git hooks"
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
	@echo "Version: $(MACOS_APP_VERSION)"
	@rm -rf macos/build 2>/dev/null || sudo rm -rf macos/build 2>/dev/null || true
	@mkdir -p macos/build
	@cd macos/MenuBarApp && swift build -c release --arch $(HOST_ARCH)
	@cp macos/MenuBarApp/.build/release/NornicDB macos/build/NornicDB
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
	@echo '<key>CFBundleShortVersionString</key><string>$(MACOS_APP_VERSION)</string>' >> macos/build/NornicDB.app/Contents/Info.plist
	@echo '<key>CFBundleVersion</key><string>$(MACOS_APP_VERSION)</string>' >> macos/build/NornicDB.app/Contents/Info.plist
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
	@rm -rf macos/build 2>/dev/null || sudo rm -rf macos/build 2>/dev/null || true
	@if [ -d "dist/installer" ]; then \
		echo "Removing installer artifacts (may require sudo)..."; \
		chmod -RN dist/installer 2>/dev/null || true; \
		chflags -R nouchg dist/installer 2>/dev/null || true; \
		xattr -rc dist/installer 2>/dev/null || true; \
		chmod -R u+rwX dist/installer 2>/dev/null || true; \
		rm -rf dist/installer 2>/dev/null || sudo rm -rf dist/installer; \
	fi
	@echo "✅ Cleaned"

# Create distributable .pkg installer
# Builds BOTH lite and full versions by default
macos-package: build macos-menubar plugins
	@echo "Creating macOS package installers (Lite + Full)..."
ifeq ($(HOST_OS),darwin)
	@VERSION="$(MACOS_APP_VERSION)" ./macos/scripts/build-installer.sh --both
else
	@echo "❌ Package creation is only available on macOS"
	@exit 1
endif

# Create LITE package only (no plugins, smaller download)
macos-package-lite: build macos-menubar
	@echo "Creating macOS package installer (Lite Edition)..."
ifeq ($(HOST_OS),darwin)
	@VERSION="$(MACOS_APP_VERSION)" ./macos/scripts/build-installer.sh --lite
else
	@echo "❌ Package creation is only available on macOS"
	@exit 1
endif

# Create FULL package only (with APOC + Heimdall plugins)
macos-package-full: build macos-menubar plugins
	@echo "Creating macOS package installer (Full Edition with plugins)..."
ifeq ($(HOST_OS),darwin)
	@VERSION="$(MACOS_APP_VERSION)" ./macos/scripts/build-installer.sh --full
else
	@echo "❌ Package creation is only available on macOS"
	@exit 1
endif

# Alias for backwards compatibility
macos-package-all: macos-package

# Create signed .pkg for distribution (requires Apple Developer account)
# Signs ALL unsigned packages (lite and full if they exist)
macos-package-signed: macos-package
	@echo "Signing package(s)..."
ifeq ($(HOST_OS),darwin)
	@if [ -z "$(SIGN_IDENTITY)" ]; then \
		echo "❌ Error: SIGN_IDENTITY not set"; \
		echo "   Usage: make macos-package-signed SIGN_IDENTITY='Developer ID Installer: Your Name'"; \
		exit 1; \
	fi
	@for pkg in dist/NornicDB-*-$(ARCH)-lite.pkg dist/NornicDB-*-$(ARCH)-full.pkg; do \
		if [ -f "$$pkg" ]; then \
			signed_pkg=$$(echo $$pkg | sed 's/\.pkg$$/-signed.pkg/'); \
			echo "Signing $$pkg -> $$signed_pkg"; \
			productsign --sign "$(SIGN_IDENTITY)" "$$pkg" "$$signed_pkg"; \
		fi; \
	done
	@echo "✅ Signed package(s) created"
else
	@echo "❌ Signing is only available on macOS"
	@exit 1
endif

# ==============================================================================
# ANTLR Parser Generation
# ==============================================================================
# Regenerate the ANTLR Cypher parser from grammar files (.g4)
# Requires: Java 11+ (for ANTLR tool)
# See: pkg/cypher/antlr/README.md for details

# Regenerate ANTLR parser from grammar files
antlr-generate:
	@echo "Regenerating ANTLR Cypher parser..."
	$(MAKE) -C pkg/cypher/antlr generate

# Run ENTIRE test suite with ANTLR parser active
# This validates that ANTLR parser works with all tests across the codebase
antlr-test:
	@echo "=============================================="
	@echo "Running ENTIRE test suite with ANTLR parser"
	@echo "=============================================="
	NORNICDB_PARSER=antlr go test -timeout 30m ./...
	@echo ""
	@echo "✅ Full test suite passed with ANTLR parser"

# Run FULL cypher test suite with ANTLR parser
# This validates that ANTLR parser works with all existing cypher tests
antlr-test-full:
	@echo "=============================================="
	@echo "Running FULL Cypher test suite with ANTLR parser"
	@echo "=============================================="
	NORNICDB_PARSER=antlr go test -timeout 30m ./pkg/cypher/...
	@echo ""
	@echo "✅ Full Cypher test suite passed with ANTLR parser"

# Run both parsers: first Nornic (default), then ANTLR
test-parsers:
	@echo "=============================================="
	@echo "Running Cypher tests with NORNIC parser (default)"
	@echo "=============================================="
	NORNICDB_PARSER=nornic go test -timeout 30m ./pkg/cypher/...
	@echo ""
	@echo "=============================================="
	@echo "Running Cypher tests with ANTLR parser"
	@echo "=============================================="
	NORNICDB_PARSER=antlr go test -timeout 30m ./pkg/cypher/...
	@echo ""
	@echo "✅ Both parsers passed all Cypher tests"

# Clean ANTLR generated files and JAR
antlr-clean:
	@echo "Cleaning ANTLR artifacts..."
	$(MAKE) -C pkg/cypher/antlr clean

# ==============================================================================
# Observability Performance Gates (Phase 12)
# ==============================================================================

bench-observability:
	@echo "Running observability hot-path benchmarks..."
	go test -bench BenchmarkObserve_Hot ./pkg/observability/... -benchmem -count=3

bench-cypher:
	@echo "Running cypher benchmarks..."
	go test -bench Benchmark ./pkg/cypher/... -benchmem -count=3 -timeout=120s

bench-bolt:
	@echo "Running bolt benchmarks..."
	go test -bench Benchmark ./pkg/bolt/... -benchmem -count=3 -timeout=60s

bench-all: bench-observability bench-cypher bench-bolt

perf-gates:
	@echo "Running performance gate tests..."
	go test -run "TestObservability_MemoryFloor|TestObservability_SpanAllocsPerOp|TestRedactingSpanProcessor_AllocsPerOp" ./pkg/observability/ -v -count=1

metrics-doc:
	@echo "Generating metrics reference..."
	go run ./cmd/metrics-doc-gen/ > docs/operations/metrics-reference.md
	@echo "Generated docs/operations/metrics-reference.md"
