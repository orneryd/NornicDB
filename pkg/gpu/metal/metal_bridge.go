//go:build darwin
// +build darwin

// Package metal provides Metal GPU acceleration for vector operations.
package metal

/*
#cgo CFLAGS: -x objective-c -fobjc-arc
#cgo LDFLAGS: -framework Metal -framework MetalPerformanceShaders -framework Foundation -framework CoreGraphics

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

// Memory tracking structs
typedef struct {
    unsigned long total_memory;
    unsigned long used_memory;
    unsigned long available_memory;
    unsigned long gpu_recommended;
    unsigned long current_allocated;
} MetalMemoryInfo;

typedef struct {
    char name[256];
    char architecture[64];
    int gpu_family;
    int max_threads_per_threadgroup;
    unsigned long max_buffer_length;
    bool supports_raytracing;
    bool supports_32bit_float_filtering;
    bool supports_32bit_msaa;
    bool is_low_power;
    bool is_headless;
    bool is_removable;
    bool has_unified_memory;
    int registry_id;
} MetalDeviceCapabilities;

// Memory tracking
void metal_get_memory_info(MetalDevice device, MetalMemoryInfo* info);
void metal_get_device_capabilities(MetalDevice device, MetalDeviceCapabilities* caps);

// Metal Performance Shaders (MPS)
bool metal_mps_is_supported(void);
int metal_mps_matrix_multiply(MetalDevice device, MetalBuffer a, MetalBuffer b, MetalBuffer c,
    unsigned int m, unsigned int n, unsigned int k, float alpha, float beta);
int metal_mps_matrix_multiply_bt(MetalDevice device, MetalBuffer a, MetalBuffer b, MetalBuffer c,
    unsigned int m, unsigned int n, unsigned int k);
int metal_mps_matrix_vector_multiply(MetalDevice device, MetalBuffer a, MetalBuffer x, MetalBuffer y,
    unsigned int m, unsigned int n, float alpha, float beta);
int metal_mps_batch_cosine_similarity(MetalDevice device, MetalBuffer embeddings, MetalBuffer query,
    MetalBuffer scores, unsigned int n, unsigned int dims);

int metal_hnsw_build_topk(
    MetalDevice device,
    MetalBuffer frontier,
    MetalBuffer queries,
    MetalBuffer scores,
    MetalBuffer indices,
    MetalBuffer topk_scores,
    unsigned int frontier_n,
    unsigned int query_n,
    unsigned int dimensions,
    unsigned int k
);
*/
import "C"

import (
	"errors"
	"fmt"
	"log"
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

// Float32View returns the buffer's first count float32 values as a slice of
// its shared memory, without copying. The slice is valid until the buffer is
// released or written by the GPU again.
func (b *Buffer) Float32View(count int) []float32 {
	if count <= 0 || uint64(count)*4 > b.size {
		return nil
	}
	contents := b.Contents()
	if contents == nil {
		return nil
	}
	return unsafe.Slice((*float32)(contents), count)
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

// HNSWBuildTopK computes batched cosine top-k for HNSW construction.
//
// The frontier and query buffers must contain normalized vectors in row-major
// layout: frontier is [frontierN x dimensions], queries is [queryN x dimensions].
// It returns compact row-major outputs with queryN*k indices and scores.
func (d *Device) HNSWBuildTopK(
	frontier, queries *Buffer,
	frontierN, queryN, dimensions uint32,
	k int,
) ([]uint32, []float32, error) {
	if k <= 0 || frontierN == 0 || queryN == 0 {
		return nil, nil, nil
	}
	if k > int(frontierN) {
		k = int(frontierN)
	}

	scoresBuf, err := d.NewEmptyBuffer(uint64(frontierN)*uint64(queryN)*4, StorageShared)
	if err != nil {
		return nil, nil, err
	}
	defer scoresBuf.Release()

	outCount := uint64(queryN) * uint64(k)
	indicesBuf, err := d.NewEmptyBuffer(outCount*4, StorageShared)
	if err != nil {
		return nil, nil, err
	}
	defer indicesBuf.Release()

	topkScoresBuf, err := d.NewEmptyBuffer(outCount*4, StorageShared)
	if err != nil {
		return nil, nil, err
	}
	defer topkScoresBuf.Release()

	d.mu.Lock()
	result := C.metal_hnsw_build_topk(
		d.ptr,
		frontier.ptr,
		queries.ptr,
		scoresBuf.ptr,
		indicesBuf.ptr,
		topkScoresBuf.ptr,
		C.uint(frontierN),
		C.uint(queryN),
		C.uint(dimensions),
		C.uint(k),
	)
	d.mu.Unlock()
	if result != 0 {
		errMsg := C.GoString(C.metal_last_error())
		C.metal_clear_error()
		return nil, nil, fmt.Errorf("%w: %s", ErrKernelExecution, errMsg)
	}

	return indicesBuf.ReadUint32(int(outCount)), topkScoresBuf.ReadFloat32(int(outCount)), nil
}

// =============================================================================
// Memory Tracking
// =============================================================================

// MemoryInfo contains GPU and system memory statistics.
type MemoryInfo struct {
	TotalMemory      uint64 // Total unified memory (bytes)
	UsedMemory       uint64 // Currently used (bytes)
	AvailableMemory  uint64 // Available (bytes)
	GPURecommended   uint64 // Recommended max GPU working set (bytes)
	CurrentAllocated uint64 // Currently allocated GPU buffers (bytes)
}

// GetMemoryInfo returns current memory statistics.
func (d *Device) GetMemoryInfo() MemoryInfo {
	var info C.MetalMemoryInfo
	C.metal_get_memory_info(d.ptr, &info)

	return MemoryInfo{
		TotalMemory:      uint64(info.total_memory),
		UsedMemory:       uint64(info.used_memory),
		AvailableMemory:  uint64(info.available_memory),
		GPURecommended:   uint64(info.gpu_recommended),
		CurrentAllocated: uint64(info.current_allocated),
	}
}

// DeviceCapabilities contains detailed GPU capabilities.
type DeviceCapabilities struct {
	Name                     string
	Architecture             string
	GPUFamily                int
	MaxThreadsPerThreadgroup int
	MaxBufferLength          uint64 // Can be very large (16GB+) on Apple Silicon
	IsLowPower               bool
	IsHeadless               bool
	HasUnifiedMemory         bool
	RegistryID               int
}

// GetCapabilities returns detailed device capabilities.
func (d *Device) GetCapabilities() DeviceCapabilities {
	var caps C.MetalDeviceCapabilities
	C.metal_get_device_capabilities(d.ptr, &caps)

	return DeviceCapabilities{
		Name:                     C.GoString(&caps.name[0]),
		Architecture:             C.GoString(&caps.architecture[0]),
		GPUFamily:                int(caps.gpu_family),
		MaxThreadsPerThreadgroup: int(caps.max_threads_per_threadgroup),
		MaxBufferLength:          uint64(caps.max_buffer_length),
		IsLowPower:               bool(caps.is_low_power),
		IsHeadless:               bool(caps.is_headless),
		HasUnifiedMemory:         bool(caps.has_unified_memory),
		RegistryID:               int(caps.registry_id),
	}
}

// GetCapabilitiesNoDevice returns capabilities without creating a device.
func GetCapabilitiesNoDevice() DeviceCapabilities {
	var caps C.MetalDeviceCapabilities
	C.metal_get_device_capabilities(nil, &caps)

	return DeviceCapabilities{
		Name:                     C.GoString(&caps.name[0]),
		Architecture:             C.GoString(&caps.architecture[0]),
		GPUFamily:                int(caps.gpu_family),
		MaxThreadsPerThreadgroup: int(caps.max_threads_per_threadgroup),
		MaxBufferLength:          uint64(caps.max_buffer_length),
		IsLowPower:               bool(caps.is_low_power),
		IsHeadless:               bool(caps.is_headless),
		HasUnifiedMemory:         bool(caps.has_unified_memory),
		RegistryID:               int(caps.registry_id),
	}
}

// PrintDeviceInfo logs detailed Metal device information.
// Call this on startup to show "Using Metal GPU: Apple M2 Max"
func PrintDeviceInfo() {
	if !IsAvailable() {
		log.Println("🔴 Metal GPU: Not available")
		return
	}

	caps := GetCapabilitiesNoDevice()

	log.Printf("🟢 Metal GPU: %s", caps.Name)
	if caps.Architecture != "" {
		log.Printf("   Architecture: %s (GPU Family %d)", caps.Architecture, caps.GPUFamily)
	}
	log.Printf("   Unified Memory: %v", caps.HasUnifiedMemory)
	log.Printf("   Max Threads/Group: %d", caps.MaxThreadsPerThreadgroup)
	log.Printf("   Max Buffer: %.1f GB", float64(caps.MaxBufferLength)/(1024*1024*1024))

	// Get memory info
	dev, err := NewDevice()
	if err == nil {
		memInfo := dev.GetMemoryInfo()
		log.Printf("   Total Memory: %.1f GB", float64(memInfo.TotalMemory)/(1024*1024*1024))
		log.Printf("   Available: %.1f GB", float64(memInfo.AvailableMemory)/(1024*1024*1024))
		log.Printf("   GPU Recommended: %.1f GB", float64(memInfo.GPURecommended)/(1024*1024*1024))
		dev.Release()
	}

	if MPSIsSupported() {
		log.Println("   MPS: ✅ Supported")
	}
}

// =============================================================================
// Metal Performance Shaders (MPS)
// =============================================================================

// MPSIsSupported returns true if Metal Performance Shaders are available.
func MPSIsSupported() bool {
	return bool(C.metal_mps_is_supported())
}

// MPSMatrixMultiply performs GPU-accelerated matrix multiplication.
// Computes: C = alpha * A * B + beta * C
//
// Parameters:
//   - a: Matrix A buffer (m × k floats)
//   - b: Matrix B buffer (k × n floats)
//   - c: Matrix C buffer (m × n floats, output)
//   - m, n, k: Matrix dimensions
//   - alpha, beta: Scaling factors
//
// This uses Metal Performance Shaders which are highly optimized
// for Apple Silicon's unified memory architecture.
func (d *Device) MPSMatrixMultiply(a, b, c *Buffer, m, n, k uint32, alpha, beta float32) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	result := C.metal_mps_matrix_multiply(
		d.ptr,
		a.ptr,
		b.ptr,
		c.ptr,
		C.uint(m),
		C.uint(n),
		C.uint(k),
		C.float(alpha),
		C.float(beta),
	)

	if result != 0 {
		errMsg := C.GoString(C.metal_last_error())
		C.metal_clear_error()
		return fmt.Errorf("MPS matrix multiply failed: %s", errMsg)
	}

	return nil
}

// MPSMatrixMultiplyTransposeB computes C = A × Bᵀ with Metal Performance
// Shaders: a is m × k, b is n × k and c (output) is m × n, all row-major, so
// c[i][j] is the dot product of row i of a and row j of b. It scores m query
// vectors against n stored vectors at once.
func (d *Device) MPSMatrixMultiplyTransposeB(a, b, c *Buffer, m, n, k uint32) error {
	if uint64(m)*uint64(k)*4 > a.size || uint64(n)*uint64(k)*4 > b.size || uint64(m)*uint64(n)*4 > c.size {
		return errors.New("metal: matrix exceeds buffer size")
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	if C.metal_mps_matrix_multiply_bt(d.ptr, a.ptr, b.ptr, c.ptr, C.uint(m), C.uint(n), C.uint(k)) != 0 {
		errMsg := C.GoString(C.metal_last_error())
		C.metal_clear_error()
		return fmt.Errorf("%w: %s", ErrKernelExecution, errMsg)
	}
	return nil
}

// MPSMatrixVectorMultiply performs GPU-accelerated matrix-vector multiplication.
// Computes: y = alpha * A * x + beta * y
//
// Parameters:
//   - a: Matrix A buffer (m × n floats)
//   - x: Vector x buffer (n floats)
//   - y: Vector y buffer (m floats, output)
//   - m, n: Matrix dimensions
//   - alpha, beta: Scaling factors
func (d *Device) MPSMatrixVectorMultiply(a, x, y *Buffer, m, n uint32, alpha, beta float32) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	result := C.metal_mps_matrix_vector_multiply(
		d.ptr,
		a.ptr,
		x.ptr,
		y.ptr,
		C.uint(m),
		C.uint(n),
		C.float(alpha),
		C.float(beta),
	)

	if result != 0 {
		errMsg := C.GoString(C.metal_last_error())
		C.metal_clear_error()
		return fmt.Errorf("MPS matrix-vector multiply failed: %s", errMsg)
	}

	return nil
}

// MPSBatchCosineSimilarity computes cosine similarities using MPS.
// More efficient than custom kernels for large batches (>1000 embeddings).
//
// Parameters:
//   - embeddings: Buffer with n embeddings (n × dims floats)
//   - query: Query vector buffer (dims floats)
//   - scores: Output buffer (n floats)
//   - n: Number of embeddings
//   - dims: Embedding dimensions
//
// Note: Assumes embeddings are pre-normalized for cosine similarity.
func (d *Device) MPSBatchCosineSimilarity(embeddings, query, scores *Buffer, n, dims uint32) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	result := C.metal_mps_batch_cosine_similarity(
		d.ptr,
		embeddings.ptr,
		query.ptr,
		scores.ptr,
		C.uint(n),
		C.uint(dims),
	)

	if result != 0 {
		errMsg := C.GoString(C.metal_last_error())
		C.metal_clear_error()
		return fmt.Errorf("MPS batch cosine similarity failed: %s", errMsg)
	}

	return nil
}
