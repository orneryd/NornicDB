<!--
  DIY.md - Build instructions for NornicDB
  Auto-generated from Makefile targets
  Date: 2025-12-13
-->

# Build It Yourself — NornicDB

This file lists copy-pastable commands (Makefile targets and Docker/run examples) and what they produce. It covers Docker image builds, local/native builds, cross-compiles (including Raspberry Pi), downloads for embedding models, and useful utility targets.

> Tip: You can set `REGISTRY` and `VERSION` environment variables to change image names, e.g. `REGISTRY=mydockerid VERSION=1.0.6`.

---

## Prerequisites

- Go 1.23+
- Docker
- curl (for model downloads)
- GNU make
- For localllm/BGE: `llama.cpp` build & C toolchain (see `./scripts/build-llama.sh`)
- For CUDA images: NVIDIA drivers + CUDA Toolkit (12.x recommended)
- For Vulkan images: Vulkan runtime & drivers for your GPU (AMD/NVIDIA/Intel)

Model files (Makefile helpers):
- BGE: `models/bge-m3.gguf` — `make download-bge`
- Qwen: `models/qwen2.5-0.5b-instruct.gguf` — `make download-qwen`

---

## Common flags

- No cache: `NO_CACHE=1`
- Registry override: `REGISTRY=yourdockerid` (default in Makefile: `timothyswt`)
- Version/tag: `VERSION=latest` (default)

Example:
```bash
make build-amd64-cuda NO_CACHE=1 REGISTRY=myuser VERSION=1.0.6
```

---

## Docker image builds (what each target produces)

Note: Image names follow the Makefile variables. Replace `REGISTRY` and `VERSION` as needed.

### Apple Silicon (arm64, Metal)
- `make build-arm64-metal`
  - Produces Docker image: `$(REGISTRY)/nornicdb-arm64-metal:$(VERSION)`

- `make build-arm64-metal-bge`
  - Produces Docker image: `$(REGISTRY)/nornicdb-arm64-metal-bge:$(VERSION)`
  - Downloads BGE if missing (`make download-bge`).

- `make build-arm64-metal-bge-heimdall`
  - Produces Docker image: `$(REGISTRY)/nornicdb-arm64-metal-bge-heimdall:$(VERSION)`
  - Downloads Heimdall models (`make download-models`).

- `make build-arm64-metal-headless`
  - Produces Docker image: `$(REGISTRY)/nornicdb-arm64-metal-headless:$(VERSION)` (headless, no UI)

### NVIDIA GPU (amd64, CUDA)
- `make build-amd64-cuda`
  - Produces Docker image: `$(REGISTRY)/nornicdb-amd64-cuda:$(VERSION)` (BYOM/CUDA)

- `make build-amd64-cuda-bge`
  - Produces Docker image: `$(REGISTRY)/nornicdb-amd64-cuda-bge:$(VERSION)` (BGE embedded)

- `make build-amd64-cuda-bge-heimdall`
  - Produces Docker image: `$(REGISTRY)/nornicdb-amd64-cuda-bge-heimdall:$(VERSION)`

- `make build-amd64-cuda-headless`
  - Produces Docker image: `$(REGISTRY)/nornicdb-amd64-cuda-headless:$(VERSION)`

### AMD64 Vulkan (any GPU: NVIDIA/AMD/Intel)
- `make build-amd64-vulkan`
  - Produces Docker image: `$(REGISTRY)/nornicdb-amd64-vulkan:$(VERSION)` (BYOM/Vulkan)

- `make build-amd64-vulkan-bge`
  - Produces Docker image: `$(REGISTRY)/nornicdb-amd64-vulkan-bge:$(VERSION)` (Vulkan + BGE)

- `make build-amd64-vulkan-headless`
  - Produces Docker image: `$(REGISTRY)/nornicdb-amd64-vulkan-headless:$(VERSION)`

### CPU-only (amd64)
- `make build-amd64-cpu`
  - Produces Docker image: `$(REGISTRY)/nornicdb-amd64-cpu:$(VERSION)` (CPU only, embeddings disabled)

- `make build-amd64-cpu-headless`
  - Produces Docker image: `$(REGISTRY)/nornicdb-amd64-cpu-headless:$(VERSION)`

### Build multiple images
- `make build-arm64-all` — builds all `arm64-metal` variants (BYOM, BGE, headless)
- `make build-amd64-all` — builds CUDA + CPU + Vulkan variants
- `make build-all` — builds for detected host architecture (arm64 or amd64)

---

## Push / Deploy

After building an image, push with the corresponding `push-*` target, e.g.:

```bash
make push-amd64-vulkan-bge
```

Deploy (build + push) targets are available, e.g.:

```bash
make deploy-amd64-vulkan-bge
```

There are aggregate deploy targets:

- `make deploy-amd64-all`
- `make deploy-arm64-all`
- `make deploy-all` (uses detected host arch)

---

## Docker run examples

Replace `timothyswt` and `latest` with your `REGISTRY` and `VERSION`.

- BYOM (arm64 Metal):
```bash
docker pull timothyswt/nornicdb-arm64-metal:latest
docker run -d -p 7474:7474 -p 7687:7687 -v nornicdb-data:/data \
  timothyswt/nornicdb-arm64-metal:latest
```

- Vulkan + BGE (amd64):
```bash
docker pull timothyswt/nornicdb-amd64-vulkan-bge:latest
docker run -d -p 7474:7474 -p 7687:7687 -v nornicdb-data:/data \
  timothyswt/nornicdb-amd64-vulkan-bge:latest
```

- CUDA (amd64) with GPU access (Linux + NVIDIA):
```bash
docker pull timothyswt/nornicdb-amd64-cuda-bge:latest
docker run -d --gpus all -p 7474:7474 -p 7687:7687 -v nornicdb-data:/data \
  timothyswt/nornicdb-amd64-cuda-bge:latest
```

- Headless example:
```bash
docker pull timothyswt/nornicdb-amd64-vulkan-headless:latest
docker run -d -p 7474:7474 -p 7687:7687 -v nornicdb-data:/data \
  timothyswt/nornicdb-amd64-vulkan-headless:latest
```

---

## Local/native builds (binaries)

- `make build` — builds UI assets and native binary (calls `build-ui`, `build-binary`, `build-plugins-if-supported`).
  - Produces `bin/nornicdb` (or `bin/nornicdb.exe` on Windows).

- `make build-binary` — builds the Go binary (localllm support requires `lib/llama` present).
  - Output: `bin/nornicdb` or `bin/nornicdb.exe` depending on OS.

- `make build-headless` — builds headless binary: `bin/nornicdb-headless`.

- `make build-localllm` / `make build-localllm-headless` — builds with `localllm` tag (requires `lib/llama` and CGO enabled).

## Plugins

- `make plugins` — builds APOC + Heimdall plugins (produces plugins under `apoc/built-plugins` and `plugins/heimdall/built-plugins`).

---

## Cross-compilation (Raspberry Pi and others)

These produce native binaries under `bin/`:

- `make cross-linux-amd64`
  - Output: `bin/nornicdb-linux-amd64`

- `make cross-linux-arm64`
  - Output: `bin/nornicdb-linux-arm64`

- `make cross-rpi`
  - Output: `bin/nornicdb-rpi64` (Raspberry Pi 3B+/4/5 64-bit)

- `make cross-rpi32`
  - Output: `bin/nornicdb-rpi32` (32-bit ARMv7)

- `make cross-rpi-zero`
  - Output: `bin/nornicdb-rpi-zero` (ARMv6)

- `make cross-windows`
  - Output: `bin/nornicdb.exe` (cross-built for Windows x86_64)

Notes:
- Cross-compiles set `CGO_ENABLED=0` (pure Go). If you need CGO (for localllm/llama integration), build natively on the target device and install a C toolchain.
- For Raspberry Pi, to get GPU/local embeddings you generally need to build natively on-device and satisfy `llama.cpp` requirements.

---

## Model download helpers

- `make download-bge` — downloads the BGE embedding model to `models/bge-m3.gguf`.
- `make download-qwen` — downloads the Qwen LLM model.
- `make download-models` — downloads both BGE and Qwen.
- `make check-models` — checks for existence of models.

---

## Example: Full AMD64 Vulkan+BGE build+push+run

```bash
# set registry and version
export REGISTRY=timothyswt
export VERSION=latest

# download BGE model
make download-bge

# build the Vulkan + BGE image
make build-amd64-vulkan-bge

# push to registry
make push-amd64-vulkan-bge

# run
docker run -d --rm -p 7474:7474 -p 7687:7687 -v nornicdb-data:/data \
  ${REGISTRY}/nornicdb-amd64-vulkan-bge:${VERSION}
```

---

## Troubleshooting

- If Docker builds fail due to missing GPU libs, ensure the host has drivers installed and you expose devices/volumes expected by the Dockerfile.
- For CUDA images, use `--gpus all` on `docker run` for Linux hosts with NVIDIA.
- For Vulkan images, ensure the host has Vulkan runtime and exposes required device nodes; instructions vary by vendor and OS.
- If `make build-localllm` fails due to missing `lib/llama`, run `./scripts/build-llama.sh` or `make build-llama-cuda` as appropriate.

---

If you'd like, I can commit this file to the repo and open a PR, or add a short `docs/BUILD.md` entry instead. Tell me which you prefer.
