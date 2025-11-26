//go:build darwin
// +build darwin

// Package metal provides Metal GPU acceleration for vector operations.
package metal

/*
#cgo CFLAGS: -x objective-c -fobjc-arc
#cgo LDFLAGS: -framework Metal -framework Foundation -framework CoreGraphics

#include <stdlib.h>
#include <stdbool.h>

// Forward declarations for Metal bridge functions
typedef void* MetalDevice;
typedef void* MetalBuffer;
typedef void* MetalCommandQueue;
typedef void* MetalComputePipeline;

// Device management
MetalDevice metal_create_device(void);
void metal_release_device(MetalDevice device);
bool metal_is_available(void);
const char* metal_device_name(MetalDevice device);
unsigned long metal_device_memory(MetalDevice device);

// Buffer management
MetalBuffer metal_create_buffer(MetalDevice device, void* data, unsigned long size, int storage_mode);
MetalBuffer metal_create_buffer_no_copy(MetalDevice device, void* data, unsigned long size, int storage_mode);
void metal_release_buffer(MetalBuffer buffer);
void* metal_buffer_contents(MetalBuffer buffer);
unsigned long metal_buffer_length(MetalBuffer buffer);
void metal_buffer_did_modify(MetalBuffer buffer, unsigned long start, unsigned long length);

// Compute operations
int metal_compute_cosine_similarity(
    MetalDevice device,
    MetalBuffer embeddings,
    MetalBuffer query,
    MetalBuffer scores,
    unsigned int n,
    unsigned int dimensions,
    bool normalized
);

int metal_compute_topk(
    MetalDevice device,
    MetalBuffer scores,
    MetalBuffer indices,
    MetalBuffer topk_scores,
    unsigned int n,
    unsigned int k
);

int metal_normalize_vectors(
    MetalDevice device,
    MetalBuffer vectors,
    unsigned int n,
    unsigned int dimensions
);

// Error handling
const char* metal_last_error(void);
void metal_clear_error(void);
*/
import "C"

import (
	"errors"
	"fmt"
	"sync"
	"unsafe"
)

// Errors
var (
	ErrMetalNotAvailable = errors.New("metal: Metal is not available on this system")
	ErrDeviceCreation    = errors.New("metal: failed to create Metal device")
	ErrBufferCreation    = errors.New("metal: failed to create buffer")
	ErrKernelExecution   = errors.New("metal: kernel execution failed")
	ErrInvalidBuffer     = errors.New("metal: invalid buffer")
)

// StorageMode defines how buffer memory is managed.
type StorageMode int

const (
	// StorageShared uses unified memory accessible by both CPU and GPU.
	// Best for Apple Silicon's unified memory architecture.
	StorageShared StorageMode = 0

	// StorageManaged requires explicit synchronization between CPU and GPU.
	// Use for Intel Macs with discrete GPUs.
	StorageManaged StorageMode = 1

	// StoragePrivate is GPU-only memory, highest performance.
	// Use when data doesn't need to be read back by CPU.
	StoragePrivate StorageMode = 2
)

// Device represents a Metal GPU device.
type Device struct {
	ptr    C.MetalDevice
	name   string
	memory uint64
	mu     sync.Mutex
}

// Buffer represents a Metal GPU buffer.
type Buffer struct {
	ptr    C.MetalBuffer
	size   uint64
	device *Device
}

// SearchResult holds a similarity search result.
type SearchResult struct {
	Index uint32
	Score float32
}

// IsAvailable checks if Metal is available on this system.
func IsAvailable() bool {
	return bool(C.metal_is_available())
}

// NewDevice creates a new Metal device (uses default GPU).
func NewDevice() (*Device, error) {
	if !IsAvailable() {
		return nil, ErrMetalNotAvailable
	}

	ptr := C.metal_create_device()
	if ptr == nil {
		errMsg := C.GoString(C.metal_last_error())
		C.metal_clear_error()
		return nil, fmt.Errorf("%w: %s", ErrDeviceCreation, errMsg)
	}

	return &Device{
		ptr:    ptr,
		name:   C.GoString(C.metal_device_name(ptr)),
		memory: uint64(C.metal_device_memory(ptr)),
	}, nil
}

// Release frees the Metal device resources.
func (d *Device) Release() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.ptr != nil {
		C.metal_release_device(d.ptr)
		d.ptr = nil
	}
}

// Name returns the GPU device name.
func (d *Device) Name() string {
	return d.name
}

// MemoryBytes returns the GPU memory size in bytes.
func (d *Device) MemoryBytes() uint64 {
	return d.memory
}

// MemoryMB returns the GPU memory size in megabytes.
func (d *Device) MemoryMB() int {
	return int(d.memory / (1024 * 1024))
}

// NewBuffer creates a new GPU buffer with copied data.
func (d *Device) NewBuffer(data []float32, mode StorageMode) (*Buffer, error) {
	if len(data) == 0 {
		return nil, errors.New("metal: cannot create empty buffer")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	size := C.ulong(len(data) * 4) // float32 = 4 bytes
	ptr := C.metal_create_buffer(
		d.ptr,
		unsafe.Pointer(&data[0]),
		size,
		C.int(mode),
	)

	if ptr == nil {
		errMsg := C.GoString(C.metal_last_error())
		C.metal_clear_error()
		return nil, fmt.Errorf("%w: %s", ErrBufferCreation, errMsg)
	}

	return &Buffer{
		ptr:    ptr,
		size:   uint64(size),
		device: d,
	}, nil
}

// NewBufferNoCopy creates a GPU buffer that shares memory with the provided slice.
// The slice must remain valid for the lifetime of the buffer.
// Only works with StorageShared on Apple Silicon.
func (d *Device) NewBufferNoCopy(data []float32, mode StorageMode) (*Buffer, error) {
	if len(data) == 0 {
		return nil, errors.New("metal: cannot create empty buffer")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	size := C.ulong(len(data) * 4)
	ptr := C.metal_create_buffer_no_copy(
		d.ptr,
		unsafe.Pointer(&data[0]),
		size,
		C.int(mode),
	)

	if ptr == nil {
		errMsg := C.GoString(C.metal_last_error())
		C.metal_clear_error()
		return nil, fmt.Errorf("%w: %s", ErrBufferCreation, errMsg)
	}

	return &Buffer{
		ptr:    ptr,
		size:   uint64(size),
		device: d,
	}, nil
}

// NewEmptyBuffer creates an uninitialized GPU buffer.
func (d *Device) NewEmptyBuffer(sizeBytes uint64, mode StorageMode) (*Buffer, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	ptr := C.metal_create_buffer(
		d.ptr,
		nil,
		C.ulong(sizeBytes),
		C.int(mode),
	)

	if ptr == nil {
		errMsg := C.GoString(C.metal_last_error())
		C.metal_clear_error()
		return nil, fmt.Errorf("%w: %s", ErrBufferCreation, errMsg)
	}

	return &Buffer{
		ptr:    ptr,
		size:   sizeBytes,
		device: d,
	}, nil
}

// Release frees the buffer resources.
func (b *Buffer) Release() {
	if b.ptr != nil {
		C.metal_release_buffer(b.ptr)
		b.ptr = nil
	}
}

// Size returns the buffer size in bytes.
func (b *Buffer) Size() uint64 {
	return b.size
}

// Contents returns a pointer to the buffer's CPU-accessible memory.
// Only valid for StorageShared and StorageManaged modes.
func (b *Buffer) Contents() unsafe.Pointer {
	return C.metal_buffer_contents(b.ptr)
}

// ReadFloat32 reads float32 values from the buffer.
func (b *Buffer) ReadFloat32(count int) []float32 {
	if count <= 0 || uint64(count*4) > b.size {
		return nil
	}

	contents := b.Contents()
	if contents == nil {
		return nil
	}

	// Create a Go slice that references the buffer memory
	result := make([]float32, count)
	src := (*[1 << 30]float32)(contents)[:count:count]
	copy(result, src)
	return result
}

// ReadUint32 reads uint32 values from the buffer.
func (b *Buffer) ReadUint32(count int) []uint32 {
	if count <= 0 || uint64(count*4) > b.size {
		return nil
	}

	contents := b.Contents()
	if contents == nil {
		return nil
	}

	result := make([]uint32, count)
	src := (*[1 << 30]uint32)(contents)[:count:count]
	copy(result, src)
	return result
}

// WriteFloat32 writes float32 values to the buffer.
func (b *Buffer) WriteFloat32(data []float32, offset int) error {
	if len(data) == 0 {
		return nil
	}

	if uint64((offset+len(data))*4) > b.size {
		return errors.New("metal: write exceeds buffer size")
	}

	contents := b.Contents()
	if contents == nil {
		return ErrInvalidBuffer
	}

	dst := (*[1 << 30]float32)(contents)[offset : offset+len(data) : offset+len(data)]
	copy(dst, data)

	// Notify Metal that buffer was modified
	C.metal_buffer_did_modify(b.ptr, C.ulong(offset*4), C.ulong(len(data)*4))

	return nil
}

// ComputeCosineSimilarity computes cosine similarity between query and all embeddings.
//
// Parameters:
//   - embeddings: Buffer containing n embeddings of 'dimensions' floats each
//   - query: Buffer containing a single query vector of 'dimensions' floats
//   - scores: Output buffer for n similarity scores
//   - n: Number of embeddings
//   - dimensions: Embedding dimensions
//   - normalized: If true, embeddings are pre-normalized (faster)
//
// Returns error if kernel execution fails.
func (d *Device) ComputeCosineSimilarity(
	embeddings, query, scores *Buffer,
	n, dimensions uint32,
	normalized bool,
) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	result := C.metal_compute_cosine_similarity(
		d.ptr,
		embeddings.ptr,
		query.ptr,
		scores.ptr,
		C.uint(n),
		C.uint(dimensions),
		C.bool(normalized),
	)

	if result != 0 {
		errMsg := C.GoString(C.metal_last_error())
		C.metal_clear_error()
		return fmt.Errorf("%w: %s", ErrKernelExecution, errMsg)
	}

	return nil
}

// ComputeTopK finds the k highest scoring indices.
//
// Parameters:
//   - scores: Buffer containing n similarity scores
//   - indices: Output buffer for k indices (uint32)
//   - topkScores: Output buffer for k scores (float32)
//   - n: Number of scores
//   - k: Number of top results to find
//
// Returns error if kernel execution fails.
func (d *Device) ComputeTopK(
	scores, indices, topkScores *Buffer,
	n, k uint32,
) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	result := C.metal_compute_topk(
		d.ptr,
		scores.ptr,
		indices.ptr,
		topkScores.ptr,
		C.uint(n),
		C.uint(k),
	)

	if result != 0 {
		errMsg := C.GoString(C.metal_last_error())
		C.metal_clear_error()
		return fmt.Errorf("%w: %s", ErrKernelExecution, errMsg)
	}

	return nil
}

// NormalizeVectors normalizes vectors in-place to unit length.
//
// Parameters:
//   - vectors: Buffer containing n vectors of 'dimensions' floats each
//   - n: Number of vectors
//   - dimensions: Vector dimensions
//
// After normalization, cosine similarity becomes a simple dot product.
func (d *Device) NormalizeVectors(vectors *Buffer, n, dimensions uint32) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	result := C.metal_normalize_vectors(
		d.ptr,
		vectors.ptr,
		C.uint(n),
		C.uint(dimensions),
	)

	if result != 0 {
		errMsg := C.GoString(C.metal_last_error())
		C.metal_clear_error()
		return fmt.Errorf("%w: %s", ErrKernelExecution, errMsg)
	}

	return nil
}

// Search performs a complete similarity search using GPU acceleration.
//
// This is a convenience function that:
// 1. Computes cosine similarity for all embeddings
// 2. Finds top-k most similar
// 3. Returns results
//
// Parameters:
//   - embeddings: GPU buffer with all embeddings (n × dimensions float32)
//   - query: Query vector (dimensions float32)
//   - n: Number of embeddings
//   - dimensions: Embedding dimensions
//   - k: Number of top results
//   - normalized: Whether embeddings are pre-normalized
//
// Returns top-k search results sorted by similarity (descending).
func (d *Device) Search(
	embeddings *Buffer,
	query []float32,
	n, dimensions uint32,
	k int,
	normalized bool,
) ([]SearchResult, error) {
	if k <= 0 {
		return nil, nil
	}
	if k > int(n) {
		k = int(n)
	}

	// Create temporary buffers
	queryBuf, err := d.NewBuffer(query, StorageShared)
	if err != nil {
		return nil, err
	}
	defer queryBuf.Release()

	scoresBuf, err := d.NewEmptyBuffer(uint64(n)*4, StorageShared)
	if err != nil {
		return nil, err
	}
	defer scoresBuf.Release()

	indicesBuf, err := d.NewEmptyBuffer(uint64(k)*4, StorageShared)
	if err != nil {
		return nil, err
	}
	defer indicesBuf.Release()

	topkScoresBuf, err := d.NewEmptyBuffer(uint64(k)*4, StorageShared)
	if err != nil {
		return nil, err
	}
	defer topkScoresBuf.Release()

	// Compute similarities
	if err := d.ComputeCosineSimilarity(embeddings, queryBuf, scoresBuf, n, dimensions, normalized); err != nil {
		return nil, err
	}

	// Find top-k
	if err := d.ComputeTopK(scoresBuf, indicesBuf, topkScoresBuf, n, uint32(k)); err != nil {
		return nil, err
	}

	// Read results
	indices := indicesBuf.ReadUint32(k)
	scores := topkScoresBuf.ReadFloat32(k)

	results := make([]SearchResult, k)
	for i := 0; i < k; i++ {
		results[i] = SearchResult{
			Index: indices[i],
			Score: scores[i],
		}
	}

	return results, nil
}
