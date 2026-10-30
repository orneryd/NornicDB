// Package metal provides Metal GPU acceleration for macOS and Apple Silicon.
//
// This package implements GPU-accelerated vector similarity search using Apple's
// Metal API. It provides significant performance improvements for large-scale
// embedding searches on M1/M2/M3 Macs.
//
// Architecture:
//
//	┌─────────────────────────────────────┐
//	│           Go Application            │
//	│        (EmbeddingIndex API)         │
//	└─────────────┬───────────────────────┘
//	              │ CGO
//	┌─────────────▼───────────────────────┐
//	│         metal_bridge.go             │
//	│    (Go bindings via CGO)            │
//	└─────────────┬───────────────────────┘
//	              │ C ABI
//	┌─────────────▼───────────────────────┐
//	│         metal_bridge.m              │
//	│    (Objective-C Metal wrapper)      │
//	└─────────────┬───────────────────────┘
//	              │ Metal API
//	┌─────────────▼───────────────────────┐
//	│       Apple Silicon GPU             │
//	│    (M1/M2/M3 Neural Engine)         │
//	└─────────────────────────────────────┘
//
// Performance on Apple Silicon:
//   - M1 Pro: ~500K-1M embeddings/sec (1024-dim)
//   - M2 Max: ~1M-2M embeddings/sec
//   - M3 Max: ~2M-4M embeddings/sec
//
// Memory Requirements:
//   - Each embedding: dimensions × 4 bytes
//   - 1M embeddings @ 1024-dim = 4GB GPU memory
//   - Unified memory architecture allows efficient CPU-GPU sharing
//
// Usage:
//
//	device, err := metal.NewDevice()
//	if err != nil {
//		log.Fatal("Metal not available:", err)
//	}
//	defer device.Release()
//
//	// Create buffer for embeddings
//	buffer, err := device.NewBuffer(embeddings, metal.StorageShared)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Search
//	results, err := device.Search(buffer, query, 10, 0.7)
//
// Build Requirements:
//   - macOS 10.15+ or iOS 13+
//   - Xcode Command Line Tools
//   - CGO enabled (CGO_ENABLED=1)
//
// The package is only built on Darwin (macOS/iOS) due to Metal API availability.
package metal

// Build tags ensure this only compiles on macOS
// The actual CGO code is in metal_bridge.go with appropriate build constraints
