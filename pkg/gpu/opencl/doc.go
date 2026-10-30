// Package opencl provides cross-platform GPU acceleration using OpenCL.
//
// This package implements GPU-accelerated vector similarity search using
// OpenCL, which provides cross-platform support for AMD, Intel, and NVIDIA GPUs.
//
// # Requirements
//
// For AMD GPUs on Linux:
//   - ROCm (Radeon Open Compute): https://rocm.docs.amd.com/
//   - Or AMD GPU drivers with OpenCL support
//
// For AMD GPUs on Windows:
//   - AMD Adrenalin drivers with OpenCL support
//
// For Intel GPUs:
//   - Intel oneAPI or Intel OpenCL runtime
//
// For NVIDIA GPUs (alternative to CUDA):
//   - NVIDIA drivers with OpenCL support
//
// # Build Tags
//
// This package is only compiled when the "opencl" build tag is present:
//
//	go build -tags opencl
//
// # Environment Variables
//
// Linux (AMD ROCm):
//
//	export LD_LIBRARY_PATH=/opt/rocm/opencl/lib:$LD_LIBRARY_PATH
//
// macOS (via Homebrew):
//
//	Note: macOS deprecated OpenCL in favor of Metal. Use Metal backend instead.
//
// Windows:
//
//	OpenCL drivers are typically included with GPU drivers.
//
// # Architecture
//
// The OpenCL backend uses:
//   - OpenCL 1.2 or later for compute operations
//   - Work groups optimized for GPU wavefront/warp sizes
//   - Coalesced memory access patterns for maximum bandwidth
//   - Local memory for reduction operations (top-k)
//
// # Performance Considerations
//
// OpenCL performance varies by vendor and driver:
//   - AMD GPUs: Excellent performance with ROCm drivers
//   - Intel GPUs: Good for integrated graphics, limited VRAM
//   - NVIDIA: Prefer CUDA for best performance on NVIDIA hardware
//
// Vector operations are parallelized across compute units:
//   - Cosine similarity: One work item per vector element
//   - Top-K search: Parallel reduction with local memory
//   - Normalization: Per-vector parallelization
//
// # Example
//
// Basic usage:
//
//	device, err := opencl.NewDevice(0) // First OpenCL device
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer device.Release()
//
//	// Create buffer with embeddings
//	buffer, err := device.NewBuffer(embeddings)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer buffer.Release()
//
//	// Search for similar vectors
//	results, err := device.Search(buffer, query, numVectors, dimensions, topK, normalized)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	for _, r := range results {
//	    fmt.Printf("Index: %d, Score: %.4f\n", r.Index, r.Score)
//	}
package opencl
