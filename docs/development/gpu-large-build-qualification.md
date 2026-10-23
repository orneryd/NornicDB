# Large GPU build qualification

This qualification follows the Metal buffer fixes in PR #1591. CUDA and Vulkan
retain inserted vectors in shards, so their resident-storage checks exercise
global vector IDs on both sides of 4 GiB as well as the allocation boundary.
The tests must use the requested GPU backend; CPU fallback does not qualify it.

## Run on a GPU host

Keep enough GPU memory free for at least 4.3 GiB of resident vectors plus query
buffers and driver overhead. The full construction fixture has 1,101,077 vectors
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
```

Application Dockerfiles consume the versioned `timothyswt/llama-*-libs` images.
Those new versioned images must be built and published before a clean remote
application build can consume them. A successful local builder image does not
establish that the remote tags exist.

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
