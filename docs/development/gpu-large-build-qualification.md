# Large GPU build qualification

This qualification follows the Metal buffer fixes in PR #1591. CUDA and Vulkan
retain inserted vectors in shards, so their resident-storage checks exercise
global vector IDs on both sides of 4 GiB as well as the allocation boundary.
The tests must use the requested GPU backend; CPU fallback does not qualify it.

## Run on a GPU host

Keep enough GPU memory free for at least 4.3 GiB of resident vectors plus query
buffers and driver overhead. Run large native builds and full Linux suites
sequentially when Docker has limited RAM; leave memory for the running database. The full construction fixture has 1,101,077 vectors
of 1,024 float32 coordinates. Its graph and original coordinates also use system
memory. Full construction can take hours on an RTX 2080 Ti.

PowerShell, with CUDA CGO libraries available:

```powershell
go test -tags cuda ./pkg/gpu/cuda ./pkg/gpu/vulkan -count=1
go test -tags cuda ./pkg/search -run '^Test(Cuda|Vulkan)HNSWBuildSmoke$' -count=1 -v
$env:NORNICDB_LARGE_GPU_TEST = '1'
go test -tags cuda ./pkg/search -run '^Test(Cuda|Vulkan)HNSWResidentBuildBeyondFourGiB$' -count=1 -v -timeout=30m
go test -tags cuda ./pkg/search -run '^TestCudaHNSWBuildBeyondFourGiB$' -count=1 -v -timeout=12h
go test ./pkg/search -run '^TestVulkanHNSWBuildBeyondFourGiB$' -count=1 -v -timeout=12h
Remove-Item Env:NORNICDB_LARGE_GPU_TEST
```

Vulkan uses the default pure-Go bridge and does not require the `cuda` tag.
Its unit suite includes a native subprocess that forces Go stack relocation
and garbage collection before pushing shader constants. This covers the
pointer lifetime through normalization, search, and both HNSW dispatches.
On a Metal Mac, the existing `NORNICDB_LARGE_METAL_TEST=1` tests use the same
construction fixture. No Metal run is implied by results collected on Windows.

The resident test holds more than 4 GiB concurrently and checks exact self
neighbors around the boundary. The construction tests additionally require
the intended backend, zero kernel errors, no CPU fallback, the full graph size,
and at least 95% self recall overall and beyond the boundary. A resident test
pass alone is not a full construction pass. The local 11 GiB card cannot
qualify a resident allocation beyond the separate 16 GiB offset boundary.

## Native inference dependencies

The upstream source pin is llama.cpp `v0.7.0`, commit
`620d2225cc436ab0b001e3200e590e3b286ef9ab`. Tracked headers must match the libraries.
Windows uses yzma `v1.30.0`, whose context ABI includes `MoeCacheSize`.

```powershell
docker build -f docker/Dockerfile.llama-cpu -t nornic-llama-cpu:v0.7.0-local .
docker build -f docker/Dockerfile.llama-cuda --build-arg LLAMA_BUILD_JOBS=2 -t nornic-llama-cuda:v0.7.0-local .
# For a local RTX 2080 Ti build, compile only its SM 75 architecture:
docker build -f docker/Dockerfile.llama-cuda --build-arg LLAMA_BUILD_JOBS=2 --build-arg LLAMA_CUDA_ARCHITECTURES=75 -t nornic-llama-cuda:v0.7.0-sm75-local .
```

CUDA library images disable host-native CPU instructions, as the CPU image does.
This avoids publishing an archive that requires the builder's CPU instruction set.
The CPU backend remains enabled, including models split between CPU and GPU.

### Existing model offload controls

For NornicDB's local GGUF provider, set
`NORNICDB_EMBEDDING_GPU_LAYERS` for embeddings and
`NORNICDB_HEIMDALL_GPU_LAYERS` for the assistant model:

| Value | Placement |
| --- | --- |
| `0` | CPU only |
| `12` (example) | Offload 12 model layers to GPU; remaining layers run on CPU |
| `-1` | Request automatic/full GPU offload; capacity depends on available VRAM |

These settings apply to local GGUF inference. Ollama and remote providers manage
their own model placement. CPU offload still needs enough system RAM, and GPU
layers plus inference buffers must fit available VRAM.

NornicDB preserves llama.cpp's default layer split across available GPUs.
llama.cpp controls model placement; the GPU SDK supplies execution and transfers.
Disabling host-native CPU instructions does not disable either CPU offload or
the default multi-GPU layer split. This qualification host has one RTX 2080 Ti,
so it does not establish multi-GPU correctness.

Explicit split modes, main-GPU selection, and per-GPU tensor ratios require
additional configuration and bindings across platforms. They are deferred to
1.5.0 rather than added to this GPU correctness and dependency update.

For BGE-M3 qualification with the default floating-point KV cache, pass
`--build-arg LLAMA_CUDA_FA_ALL_QUANTS=OFF` to omit unused quantized KV flash-attention
kernels and shorten a local build. Distribution builds retain all-quant support
by default. This option does not change the quantized GGUF model weights.

Application Dockerfiles consume the versioned `timothyswt/llama-*-libs` images.
Those new versioned images must be built and published before a clean remote
application build can consume them. A successful local builder image does not
establish that the remote tags exist.

Publish the CUDA prerequisite using the distribution defaults (all configured
CUDA architectures and quantized KV kernels), rather than tagging a local
SM75-only qualification image as the release artifact:

```powershell
docker build -f docker/Dockerfile.llama-cuda --build-arg LLAMA_BUILD_JOBS=2 -t timothyswt/llama-cuda-libs:v0.7.0 .
docker push timothyswt/llama-cuda-libs:v0.7.0
docker manifest inspect timothyswt/llama-cuda-libs:v0.7.0
```

The tag matches `LLAMA_VERSION` in the Makefile and the default
`LLAMA_CUDA_IMAGE` in both CUDA application Dockerfiles. After publishing, verify
an application build against the remote tag; record the registry digest with
the qualification results so CI's prerequisite can be identified exactly.

To build an application against the local GPU library without publishing it:

```powershell
docker build -f docker/Dockerfile.amd64-cuda --build-arg LLAMA_CUDA_IMAGE=nornic-llama-cuda:v0.7.0-sm75-local --build-arg HEADLESS=true --build-arg EMBED_MODEL=false -t nornicdb:v1.4.4-llama070-local .
```

Install matching Windows DLLs using the repository-pinned yzma CLI (the CUDA
and Vulkan `.bat` wrappers call this same installer):

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/setup-windows-llama.ps1 -Processor cuda
# CPU or Vulkan hosts use -Processor cpu or -Processor vulkan.
```

For actual Windows inference regression checks, point `NORNICDB_LIB` to the
directory containing the matching llama/ggml DLLs and `TEST_GGUF_MODEL` to a
BGE-M3 GGUF, then run `go test ./pkg/localllm -count=1 -v`. The integration test
checks finite, normalized, deterministic pooled embeddings. Missing model or
library settings skip this opt-in check and must be reported as a skip.

## Release verification

Run `make verify BASE=origin/release/v1.4.4` at the final commit. On Windows using
Git Bash, supply `SHELL=C:/Progra~1/Git/bin/bash.exe`. `GOFLAGS=-p=2` limits package
build concurrency on a host also running the CUDA builder and GPU qualification.
Preserve the verification summary and report the exact head/base, failures,
skips, and pending full-scale checks in the PR result comment.

## Mac handoff for PR #1671

The maintainer selected `make test` plus correction of test failures as this PR's
merge gate. Run the broader `make verify` and full GPU construction qualification
on the release branch. Consult the latest PR verification comment for the checked
commit and actual result; earlier successful package checks are not a complete
current-head pass.

Fetch and check out `fix/gpu-large-build-deps-v144`. To build and publish the CPU
prerequisite with the same platform set as the release workflow, run from the
repository root:

```bash
# Create this isolated builder once; omit if it already exists.
docker buildx create --name nornic-llama-publish --driver docker-container
docker buildx build --builder nornic-llama-publish \
  --platform linux/amd64,linux/arm64 \
  -f docker/Dockerfile.llama-cpu --build-arg LLAMA_BUILD_JOBS=2 \
  --provenance=false --push -t timothyswt/llama-cpu-libs:v0.7.0 .
docker buildx imagetools inspect timothyswt/llama-cpu-libs:v0.7.0
```

The native source builders check `lib/llama/REVISION` before exporting their
libraries. The Windows yzma installer requires the release manifest digest in
`lib/llama/YZMA_MANIFEST_SHA256`, rather than accepting an unavailable manifest.
The expected manifest pin comes from the upstream builder's v0.7.0 release notes.

The Windows CUDA distribution image is already built locally as
`timothyswt/llama-cuda-libs:v0.7.0` (image ID starts `c8611f2a8da0`), with the full
configured architecture set and all quantized-KV kernels. NornicDB and APOC
compiled against its libraries. Publication was handed to the maintainer after
repeated Docker registry DNS failures; a local tag is not proof of publication.

After both registry tags are available, record their registry digests. The CPU
digest must identify the manifest containing both AMD64 and ARM64. Update the
application prerequisites to immutable references and verify consumers against
those published references before considering the review's image-pin finding
fully resolved. Do not substitute the AMD64-only local CPU qualification image
for the multi-platform release tag.

Windows checks include the combined `noui,localllm,yzma` application build and
real BGE-M3 tokenizer chunking through yzma. CI now builds that tag combination.
The Windows symlink test skips only when the account lacks creation privilege;
the actual rejection assertion was also executed successfully on Linux.
