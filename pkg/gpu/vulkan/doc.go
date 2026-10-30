// Package vulkan provides cross-platform GPU acceleration using Vulkan Compute.
//
// This package implements GPU-accelerated vector similarity search using
// Vulkan's compute shaders, providing high-performance, cross-platform
// GPU acceleration for Windows, Linux, macOS (via MoltenVK), and Android.
//
// # Requirements
//
// For all platforms:
//   - Vulkan SDK: https://vulkan.lunarg.com/
//   - GPU with Vulkan 1.1+ support
//
// For Linux:
//
//	# Ubuntu/Debian
//	sudo apt install vulkan-tools libvulkan-dev vulkan-validationlayers
//
//	# Fedora
//	sudo dnf install vulkan-tools vulkan-loader-devel vulkan-validation-layers
//
// For Windows:
//   - Install Vulkan SDK from LunarG
//   - Set VULKAN_SDK environment variable
//
// For macOS (via MoltenVK):
//   - brew install molten-vk
//   - Or install via Vulkan SDK for macOS
//
// # Build Tags
//
// This package is only compiled when the "vulkan" build tag is present:
//
//	go build -tags vulkan
//
// # Environment Variables
//
// Linux:
//
//	export VULKAN_SDK=/path/to/vulkan/sdk
//	export LD_LIBRARY_PATH=$VULKAN_SDK/lib:$LD_LIBRARY_PATH
//
// Windows:
//
//	Set VULKAN_SDK to SDK installation path
//
// macOS:
//
//	export VULKAN_SDK=/path/to/vulkan/sdk
//	export VK_ICD_FILENAMES=$VULKAN_SDK/share/vulkan/icd.d/MoltenVK_icd.json
//
// # Architecture
//
// The Vulkan backend uses:
//   - Vulkan Compute Shaders (SPIR-V) for GPU operations
//   - Push constants for small uniform data (query vectors)
//   - Storage buffers for large data (embeddings, scores)
//   - Compute command buffers for GPU dispatch
//   - Descriptor sets for resource binding
//
// # Performance Considerations
//
// Vulkan provides:
//   - Low-level control for maximum performance
//   - Cross-platform GPU access (NVIDIA, AMD, Intel, Apple via MoltenVK)
//   - Explicit memory management for optimal GPU utilization
//   - Async compute for overlapping CPU/GPU work
//
// For best results:
//   - Use large batch sizes to amortize dispatch overhead
//   - Keep data on GPU across multiple searches
//   - Pre-normalize embeddings when possible
//
// # Example
//
// Basic usage:
//
//	device, err := vulkan.NewDevice(0)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer device.Release()
//
//	buffer, err := device.NewBuffer(embeddings)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer buffer.Release()
//
//	results, err := device.Search(buffer, query, numVectors, dimensions, topK, normalized)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	for _, r := range results {
//	    fmt.Printf("Index: %d, Score: %.4f\n", r.Index, r.Score)
//	}
package vulkan
