//go:build darwin && cgo && !nometal
// +build darwin,cgo,!nometal

package simd

/*
#cgo CFLAGS: -x objective-c -fobjc-arc
#cgo LDFLAGS: -framework Metal -framework MetalPerformanceShaders -framework Foundation

#include <stdlib.h>
#include <stdbool.h>

// Forward declarations
typedef void* MTLDevice;
typedef void* MTLBuffer;
typedef void* MTLCommandQueue;
typedef void* MTLComputePipelineState;
typedef void* MTLLibrary;

// Device management
MTLDevice simd_metal_create_device(void);
void simd_metal_release_device(MTLDevice device);
bool simd_metal_is_available(void);

// Buffer management  
MTLBuffer simd_metal_create_buffer(MTLDevice device, void* data, unsigned long size);
void simd_metal_release_buffer(MTLBuffer buffer);
void* simd_metal_buffer_contents(MTLBuffer buffer);

// Single-pair operations (for small batches)
float simd_metal_dot_product(MTLDevice device, float* a, float* b, unsigned int n);
float simd_metal_cosine_similarity(MTLDevice device, float* a, float* b, unsigned int n);
float simd_metal_euclidean_distance(MTLDevice device, float* a, float* b, unsigned int n);
float simd_metal_norm(MTLDevice device, float* v, unsigned int n);

// Batch operations (for large batches)
int simd_metal_batch_cosine_similarity(
    MTLDevice device,
    float* embeddings,
    float* query,
    float* scores,
    unsigned int num_vectors,
    unsigned int dimensions
);

int simd_metal_batch_dot_product(
    MTLDevice device,
    float* embeddings,
    float* query,
    float* results,
    unsigned int num_vectors,
    unsigned int dimensions
);

int simd_metal_batch_euclidean_distance(
    MTLDevice device,
    float* embeddings,
    float* query,
    float* distances,
    unsigned int num_vectors,
    unsigned int dimensions
);

int simd_metal_batch_normalize(
    MTLDevice device,
    float* vectors,
    unsigned int num_vectors,
    unsigned int dimensions
);

// Error handling
const char* simd_metal_last_error(void);
*/
import "C"

import (
	"errors"
	"runtime"
	"sync"
	"unsafe"
)

// Metal backend errors
var (
	ErrMetalNotAvailable = errors.New("simd/metal: Metal GPU not available")
	ErrMetalExecution    = errors.New("simd/metal: kernel execution failed")
)

// metalDevice holds the singleton Metal device
var (
	metalDevice     C.MTLDevice
	metalDeviceOnce sync.Once
	metalAvailable  bool
)

// initMetal initializes the Metal device (called once)
func initMetal() {
	metalDeviceOnce.Do(func() {
		if C.simd_metal_is_available() {
			metalDevice = C.simd_metal_create_device()
			metalAvailable = metalDevice != nil
		}
	})
}

// MetalAvailable returns true if Metal GPU acceleration is available
func MetalAvailable() bool {
	initMetal()
	return metalAvailable
}

// MetalDotProduct computes dot product using Metal GPU
// Falls back to CPU implementation for small vectors (< 4096 elements)
func MetalDotProduct(a, b []float32) float32 {
	n := len(a)
	if n == 0 || n != len(b) {
		return 0
	}

	// Use CPU for small vectors (GPU overhead not worth it)
	if n < 4096 || !MetalAvailable() {
		return DotProduct(a, b)
	}

	initMetal()
	result := C.simd_metal_dot_product(
		metalDevice,
		(*C.float)(unsafe.Pointer(&a[0])),
		(*C.float)(unsafe.Pointer(&b[0])),
		C.uint(n),
	)
	runtime.KeepAlive(a)
	runtime.KeepAlive(b)
	return float32(result)
}

// MetalCosineSimilarity computes cosine similarity using Metal GPU
// Falls back to CPU implementation for small vectors (< 4096 elements)
func MetalCosineSimilarity(a, b []float32) float32 {
	n := len(a)
	if n == 0 || n != len(b) {
		return 0
	}

	// Use CPU for small vectors
	if n < 4096 || !MetalAvailable() {
		return CosineSimilarity(a, b)
	}

	initMetal()
	result := C.simd_metal_cosine_similarity(
		metalDevice,
		(*C.float)(unsafe.Pointer(&a[0])),
		(*C.float)(unsafe.Pointer(&b[0])),
		C.uint(n),
	)
	runtime.KeepAlive(a)
	runtime.KeepAlive(b)
	return float32(result)
}

// MetalEuclideanDistance computes Euclidean distance using Metal GPU
// Falls back to CPU implementation for small vectors (< 4096 elements)
func MetalEuclideanDistance(a, b []float32) float32 {
	n := len(a)
	if n == 0 || n != len(b) {
		return 0
	}

	// Use CPU for small vectors
	if n < 4096 || !MetalAvailable() {
		return EuclideanDistance(a, b)
	}

	initMetal()
	result := C.simd_metal_euclidean_distance(
		metalDevice,
		(*C.float)(unsafe.Pointer(&a[0])),
		(*C.float)(unsafe.Pointer(&b[0])),
		C.uint(n),
	)
	runtime.KeepAlive(a)
	runtime.KeepAlive(b)
	return float32(result)
}

// MetalNorm computes vector norm using Metal GPU
// Falls back to CPU implementation for small vectors (< 4096 elements)
func MetalNorm(v []float32) float32 {
	n := len(v)
	if n == 0 {
		return 0
	}

	// Use CPU for small vectors
	if n < 4096 || !MetalAvailable() {
		return Norm(v)
	}

	initMetal()
	result := C.simd_metal_norm(
		metalDevice,
		(*C.float)(unsafe.Pointer(&v[0])),
		C.uint(n),
	)
	runtime.KeepAlive(v)
	return float32(result)
}

// BatchCosineSimilarityMetal computes cosine similarity between a query
// and a batch of embeddings using Metal GPU
//
// This is the most efficient way to search large embedding collections.
// For small batches (< 1000 vectors), CPU SIMD may be faster due to GPU overhead.
//
// Parameters:
//   - embeddings: Contiguous array of [num_vectors × dimensions] float32
//   - query: Single query vector of [dimensions] float32
//   - scores: Output array of [num_vectors] float32 similarity scores
//
// Returns error if Metal is not available or execution fails.
func BatchCosineSimilarityMetal(embeddings []float32, query []float32, scores []float32) error {
	if !MetalAvailable() {
		return ErrMetalNotAvailable
	}

	dimensions := len(query)
	if dimensions == 0 {
		return nil
	}

	numVectors := len(embeddings) / dimensions
	if numVectors == 0 || len(scores) < numVectors {
		return nil
	}

	initMetal()
	result := C.simd_metal_batch_cosine_similarity(
		metalDevice,
		(*C.float)(unsafe.Pointer(&embeddings[0])),
		(*C.float)(unsafe.Pointer(&query[0])),
		(*C.float)(unsafe.Pointer(&scores[0])),
		C.uint(numVectors),
		C.uint(dimensions),
	)

	runtime.KeepAlive(embeddings)
	runtime.KeepAlive(query)
	runtime.KeepAlive(scores)

	if result != 0 {
		return ErrMetalExecution
	}
	return nil
}

// BatchDotProductMetal computes dot product between a query and a batch of vectors
func BatchDotProductMetal(embeddings []float32, query []float32, results []float32) error {
	if !MetalAvailable() {
		return ErrMetalNotAvailable
	}

	dimensions := len(query)
	if dimensions == 0 {
		return nil
	}

	numVectors := len(embeddings) / dimensions
	if numVectors == 0 || len(results) < numVectors {
		return nil
	}

	initMetal()
	result := C.simd_metal_batch_dot_product(
		metalDevice,
		(*C.float)(unsafe.Pointer(&embeddings[0])),
		(*C.float)(unsafe.Pointer(&query[0])),
		(*C.float)(unsafe.Pointer(&results[0])),
		C.uint(numVectors),
		C.uint(dimensions),
	)

	runtime.KeepAlive(embeddings)
	runtime.KeepAlive(query)
	runtime.KeepAlive(results)

	if result != 0 {
		return ErrMetalExecution
	}
	return nil
}

// BatchEuclideanDistanceMetal computes Euclidean distance between a query and a batch of vectors
func BatchEuclideanDistanceMetal(embeddings []float32, query []float32, distances []float32) error {
	if !MetalAvailable() {
		return ErrMetalNotAvailable
	}

	dimensions := len(query)
	if dimensions == 0 {
		return nil
	}

	numVectors := len(embeddings) / dimensions
	if numVectors == 0 || len(distances) < numVectors {
		return nil
	}

	initMetal()
	result := C.simd_metal_batch_euclidean_distance(
		metalDevice,
		(*C.float)(unsafe.Pointer(&embeddings[0])),
		(*C.float)(unsafe.Pointer(&query[0])),
		(*C.float)(unsafe.Pointer(&distances[0])),
		C.uint(numVectors),
		C.uint(dimensions),
	)

	runtime.KeepAlive(embeddings)
	runtime.KeepAlive(query)
	runtime.KeepAlive(distances)

	if result != 0 {
		return ErrMetalExecution
	}
	return nil
}

// BatchNormalizeMetal normalizes a batch of vectors in-place using Metal GPU
func BatchNormalizeMetal(vectors []float32, numVectors, dimensions int) error {
	if !MetalAvailable() {
		return ErrMetalNotAvailable
	}

	if numVectors == 0 || dimensions == 0 || len(vectors) < numVectors*dimensions {
		return nil
	}

	initMetal()
	result := C.simd_metal_batch_normalize(
		metalDevice,
		(*C.float)(unsafe.Pointer(&vectors[0])),
		C.uint(numVectors),
		C.uint(dimensions),
	)

	runtime.KeepAlive(vectors)

	if result != 0 {
		return ErrMetalExecution
	}
	return nil
}
