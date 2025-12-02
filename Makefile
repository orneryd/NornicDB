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

# Detect architecture: arm64 (Apple Silicon) or x86_64/amd64 (Intel/AMD)
UNAME_M := $(shell uname -m)
ifeq ($(UNAME_M),arm64)
    HOST_ARCH := arm64
else ifeq ($(UNAME_M),aarch64)
    HOST_ARCH := arm64
else
    HOST_ARCH := amd64
endif

# Image names: nornicdb-{architecture}[-{feature}]:latest
IMAGE_ARM64 := $(REGISTRY)/nornicdb-arm64-metal:$(VERSION)
IMAGE_ARM64_BGE := $(REGISTRY)/nornicdb-arm64-metal-bge:$(VERSION)
IMAGE_ARM64_HEADLESS := $(REGISTRY)/nornicdb-arm64-metal-headless:$(VERSION)
IMAGE_AMD64 := $(REGISTRY)/nornicdb-amd64-cuda:$(VERSION)
IMAGE_AMD64_BGE := $(REGISTRY)/nornicdb-amd64-cuda-bge:$(VERSION)
IMAGE_AMD64_HEADLESS := $(REGISTRY)/nornicdb-amd64-cuda-headless:$(VERSION)
IMAGE_AMD64_CPU := $(REGISTRY)/nornicdb-amd64-cpu:$(VERSION)
IMAGE_AMD64_CPU_HEADLESS := $(REGISTRY)/nornicdb-amd64-cpu-headless:$(VERSION)
LLAMA_CUDA := $(REGISTRY)/llama-cuda-libs:b4785

# Dockerfiles
DOCKER_DIR := docker

.PHONY: build-arm64-metal build-arm64-metal-bge build-arm64-metal-headless
.PHONY: build-amd64-cuda build-amd64-cuda-bge build-amd64-cuda-headless
.PHONY: build-amd64-cpu build-amd64-cpu-headless
.PHONY: build-all build-arm64-all build-amd64-all
.PHONY: push-arm64-metal push-arm64-metal-bge push-arm64-metal-headless
.PHONY: push-amd64-cuda push-amd64-cuda-bge push-amd64-cuda-headless
.PHONY: push-amd64-cpu push-amd64-cpu-headless
.PHONY: deploy-arm64-metal deploy-arm64-metal-bge deploy-arm64-metal-headless
.PHONY: deploy-amd64-cuda deploy-amd64-cuda-bge deploy-amd64-cuda-headless
.PHONY: deploy-amd64-cpu deploy-amd64-cpu-headless
.PHONY: deploy-all deploy-arm64-all deploy-amd64-all
.PHONY: build-llama-cuda push-llama-cuda deploy-llama-cuda
.PHONY: build build-localllm build-headless build-localllm-headless test clean images help

# ==============================================================================
# Build (local only, no push)
# ==============================================================================

build-arm64-metal:
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_ARM64) [BYOM]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --platform linux/arm64 -t $(IMAGE_ARM64) -f $(DOCKER_DIR)/Dockerfile.arm64-metal .

build-arm64-metal-bge:
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_ARM64_BGE) [with BGE model]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --platform linux/arm64 --build-arg EMBED_MODEL=true -t $(IMAGE_ARM64_BGE) -f $(DOCKER_DIR)/Dockerfile.arm64-metal .

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

build-amd64-cuda-bge:
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║ Building: $(IMAGE_AMD64_BGE) [with BGE model]"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	docker build $(DOCKER_BUILD_FLAGS) --platform linux/amd64 --build-arg EMBED_MODEL=true -t $(IMAGE_AMD64_BGE) -f $(DOCKER_DIR)/Dockerfile.amd64-cuda .

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

push-arm64-metal-headless:
	@echo "→ Pushing $(IMAGE_ARM64_HEADLESS)"
	docker push $(IMAGE_ARM64_HEADLESS)

push-amd64-cuda:
	@echo "→ Pushing $(IMAGE_AMD64)"
	docker push $(IMAGE_AMD64)

push-amd64-cuda-bge:
	@echo "→ Pushing $(IMAGE_AMD64_BGE)"
	docker push $(IMAGE_AMD64_BGE)

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

deploy-arm64-metal-headless: build-arm64-metal-headless push-arm64-metal-headless
	@echo "✓ Deployed $(IMAGE_ARM64_HEADLESS)"

deploy-amd64-cuda: build-amd64-cuda push-amd64-cuda
	@echo "✓ Deployed $(IMAGE_AMD64)"

deploy-amd64-cuda-bge: build-amd64-cuda-bge push-amd64-cuda-bge
	@echo "✓ Deployed $(IMAGE_AMD64_BGE)"

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

build:
	go build -o bin/nornicdb ./cmd/nornicdb

build-localllm:
	CGO_ENABLED=1 go build -tags localllm -o bin/nornicdb ./cmd/nornicdb

# Build without UI (headless mode)
build-headless:
	go build -tags noui -o bin/nornicdb-headless ./cmd/nornicdb

build-localllm-headless:
	CGO_ENABLED=1 go build -tags "localllm noui" -o bin/nornicdb-headless ./cmd/nornicdb

test:
	go test ./...

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

clean:
	rm -rf bin/nornicdb bin/nornicdb-headless bin/nornicdb.exe

help:
	@echo "NornicDB Build System (detected arch: $(HOST_ARCH))"
	@echo ""
	@echo "Local Development:"
	@echo "  make build                   Build native binary with UI"
	@echo "  make build-headless          Build native binary without UI"
	@echo "  make build-localllm          Build with local LLM support"
	@echo "  make build-localllm-headless Build headless with local LLM"
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
