# GPU-Assisted HNSW Construction: Single-Pass Implementation Specification

## Objective

Implement GPU-assisted HNSW index construction in NornicDB so index build time decreases materially on GPU-capable hosts while preserving identical search correctness and deterministic recovery behavior.

This document defines one complete implementation target (no staged rollout).

## Scope

In scope:

- HNSW graph construction acceleration using GPU kernels where available.
- Runtime selection between GPU and CPU build paths.
- Deterministic on-disk serialization compatible with existing index load path.
- Full test, benchmark, and observability coverage.

Out of scope:

- Query-time HNSW search redesign.
- ANN algorithm replacement.
- Change of persisted HNSW format semantics.

## Current Constraints and Required Compatibility

- Existing HNSW behavior and recall characteristics must remain semantically equivalent.
- Existing persisted HNSW indexes must remain readable.
- GPU path must fall back to CPU automatically on unsupported hardware or kernel failure.
- Build must remain cancellable via context.

## Design Summary

Implement a hybrid builder:

1. GPU computes candidate neighborhoods and distance top-k for each insertion batch.
2. CPU performs final graph mutation/linking using the same invariants already enforced by current HNSW code.
3. Persisted graph is identical in logical structure guarantees to CPU builder.

Why hybrid:

- Distance evaluation and candidate generation are massively parallel and GPU-friendly.
- Final graph mutation requires ordered/invariant-preserving updates and is simpler to keep on CPU for correctness.

## Data Flow

1. BuildIndexes invokes HNSW builder with dataset vectors.
2. Builder checks GPU manager availability and backend readiness.
3. Vectors are chunked into build batches.
4. For each batch:
   - Transfer batch vectors and necessary graph frontier metadata to GPU.
   - GPU kernel computes candidate sets per node against entry/frontier pool.
   - GPU returns bounded candidate lists + distances.
   - CPU applies heuristic neighbor selection and bi-directional link updates.
5. After all batches, CPU finalizes layer consistency checks.
6. Persist index.

## Required Code Changes

### 1) HNSW builder integration surface

File targets:

- `pkg/search/hnsw_index.go`
- `pkg/search/search.go`

Add builder options:

- `UseGPUBuild bool`
- `GPUBuildBatchSize int`
- `GPUBuildCandidateK int`
- `GPUBuildDistancePrecision string` (`fp32` default)

Add capability query:

- `func (h *HNSWIndex) SupportsGPUBuild() bool`

### 2) GPU candidate generation module

New file:

- `pkg/search/hnsw_build_gpu.go`

New interface:

```go
type HNSWBuildAccelerator interface {
    Prepare(dim int, maxNodes int) error
    CandidateSearch(ctx context.Context, queries [][]float32, frontier [][]float32, topK int) (indices [][]int, distances [][]float32, err error)
    Close() error
}
```

Implementations:

- Metal-backed accelerator (darwin/arm64).
- CPU shim accelerator (same interface, used for deterministic parity tests and fallback).

### 3) Backend-specific kernel code

File targets:

- `pkg/search/hnsw_metal.go` (extend with build kernels)
- add shader/kernel source for batched L2/cosine scoring and top-k reduction.

Kernel contract:

- Input contiguous matrix for queries/frontier.
- Output fixed-width top-k indices + scores.
- Stable tie-break by source index for determinism.

### 4) Builder orchestration

File targets:

- `pkg/search/search.go` (`BuildIndexes` and HNSW construction path)

Behavior:

- Resolve build strategy:
  - default `NORNICDB_HNSW_BUILD_GPU_ENABLED=true`: attempt GPU-assisted build first when accelerator is available.
  - if GPU is disabled, unavailable, or fails at runtime -> fail-open to CPU build automatically.
- Emit strategy log line and metrics labels.
- Preserve context cancellation checks between batches.

### 5) Persistence compatibility (no new build fields)

File targets:

- Existing HNSW persistence/load path in `pkg/search`.

Requirements:

- Persistence format is unchanged.
- GPU-assisted build must emit the same persisted HNSW artifact format as CPU build.
- No compatibility inference logic is needed beyond existing load validation.
- No special legacy handling is required for GPU-assisted build because artifact format does not change.

## Correctness Requirements

- Neighbor constraints (`M`, layer rules, bidirectional edge consistency) must match existing invariants.
- For same input vectors and config, recall@k must match CPU baseline (no regression target).
- No disconnected-component regressions attributable to GPU candidate path.

Acceptance tolerances:

- Recall parity vs CPU baseline: no measurable regression (target delta = 0 for Recall@10 and Recall@100 in deterministic test setup).
- Index build failure rate: 0 under supported GPU conditions in CI/device test matrix.

## Performance Targets

On representative datasets:

- 1M vectors, dim 1024: build wall-clock reduction >= 35% vs CPU builder baseline.
- CPU utilization reduction during distance-heavy build segments >= 40%.
- Peak memory overhead increase <= 20% compared to CPU build path.

## Determinism and Reproducibility

- Candidate ordering must be stable for equal distances.
- Batch processing order must be fixed and documented.
- Randomized entrypoint logic (if any) must use explicit seed wiring in existing build-settings persistence.
- Rebuild replay determinism must come from existing settings + dataset ordering, not new HNSW build metadata fields.

## Observability

Add structured logs and counters:

- `hnsw_build_strategy`
- `hnsw_build_backend`
- `hnsw_build_batches_total`
- `hnsw_build_batch_duration_ms`
- `hnsw_build_gpu_kernel_errors_total`
- `hnsw_build_fallback_to_cpu_total`

Expose in existing metrics endpoint.

## Failure Handling

If GPU path fails at runtime (kernel init/run/transfer):

- Log explicit warning with failure code.
- Switch to CPU builder in-process for current build.
- Continue unless fatal CPU path error occurs.

No partial-corrupt index writes:

- only commit persisted HNSW artifacts after full successful finalization.

## Test Plan

### Unit tests

- GPU strategy selection and fallback logic.
- Stable tie-break ordering for equal-distance candidates.
- Persistence parity tests proving CPU-built and GPU-assisted-built indexes both load through the exact existing validator/loader path.

### Integration tests

- Build same dataset with CPU and GPU-assisted paths; compare graph invariants and recall.
- Cancellation mid-build leaves no committed partial index artifacts.
- Simulated GPU failure triggers CPU fallback and successful completion.

### Benchmarks

- Add benchmark set for build-only throughput:
  - dims: 1024
  - sizes: data stores in ./data to be used in benchmarking performance when we start the server we will measure the time difference. create a test harness for automated testing HNSW construction directly from an existing data file. currently, building HNSW on the existing dataset in ./data is ~10 minutes on CPU we can use that as a baseline.
- Record before/after stats in `docs/performance` artifact update.

## Configuration Surface

Environment variables (new):

- `NORNICDB_HNSW_BUILD_GPU_ENABLED` (bool, default `true`)
- `NORNICDB_HNSW_BUILD_GPU_BATCH_SIZE` (int, default `2048`)
- `NORNICDB_HNSW_BUILD_GPU_CANDIDATE_K` (int, default `128`)

Per-DB override keys (add to dbconfig allowed keys + resolver):

- `NORNICDB_HNSW_BUILD_GPU_ENABLED`
- `NORNICDB_HNSW_BUILD_GPU_BATCH_SIZE`
- `NORNICDB_HNSW_BUILD_GPU_CANDIDATE_K`

## Documentation Updates Required

Update:

- `docs/user-guides/search.md` with GPU-assisted build behavior and fallback semantics.
- `docs/operations/environment-variables.md` with defaults and valid ranges.
- `docs/performance/` benchmark comparison section.

## Completion Criteria

Implementation is complete when all are true:

1. GPU-assisted HNSW build runs automatically on supported GPU hosts.
2. CPU fallback is automatic and tested.
3. Recall/invariant tests show no regression versus CPU baseline.
4. Benchmarks show target improvement.
5. Metrics/logging expose strategy and fallback visibility.
6. Docs and env var reference are updated.
