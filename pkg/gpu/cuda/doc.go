// Package cuda provides NVIDIA GPU acceleration for vector operations using CUDA.
//
// This package requires:
//   - NVIDIA GPU with CUDA Compute Capability 3.5+
//   - CUDA Toolkit 11.0+ installed
//   - cuBLAS library available
//
// The package provides GPU-accelerated:
//   - Vector normalization
//   - Cosine similarity computation
//   - Top-K selection
//
// Build Requirements:
//
// On Linux:
//   - Install CUDA Toolkit: https://developer.nvidia.com/cuda-downloads
//   - Set environment: export CUDA_HOME=/usr/local/cuda
//   - Ensure libcublas.so is in LD_LIBRARY_PATH
//
// On Windows:
//   - Install CUDA Toolkit from NVIDIA
//   - Visual Studio with C++ build tools
//   - CUDA_PATH environment variable set
//
// Build tags:
//   - Build with: go build -tags cuda
//   - Without CUDA: builds with stub implementations
//
// Example usage:
//
//	if cuda.IsAvailable() {
//	    device, err := cuda.NewDevice(0)
//	    if err != nil {
//	        log.Fatal(err)
//	    }
//	    defer device.Release()
//
//	    // Create buffer with embeddings
//	    buf, _ := device.NewBuffer(embeddings, cuda.MemoryDevice)
//
//	    // Perform similarity search
//	    results, _ := device.Search(buf, query, n, dims, k)
//	}
package cuda
