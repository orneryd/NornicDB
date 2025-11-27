//go:build cuda && (linux || windows)
// +build cuda
// +build linux windows

// Package cuda provides NVIDIA GPU acceleration using CUDA and cuBLAS.
package cuda

/*
#cgo linux CFLAGS: -I/usr/local/cuda/include
#cgo linux LDFLAGS: -L/usr/local/cuda/lib64 -lcudart -lcublas -lcuda
#cgo windows CFLAGS: -I"C:/Program Files/NVIDIA GPU Computing Toolkit/CUDA/v13.0/include"
#cgo windows LDFLAGS: -L${SRCDIR}/../../../lib/cuda -lcudart -lcublas -lcuda

#include <cuda.h>
#include <cuda_runtime_api.h>
#include <cublas_v2.h>
#include <stdlib.h>
#include <string.h>

// Error handling
static char cuda_last_error[256] = {0};

void cuda_set_error(const char* msg) {
    strncpy(cuda_last_error, msg, sizeof(cuda_last_error) - 1);
}

const char* cuda_get_last_error() {
    return cuda_last_error;
}

void cuda_clear_error() {
    cuda_last_error[0] = 0;
}

// Device management
typedef struct {
    int device_id;
    cublasHandle_t cublas_handle;
    cudaStream_t stream;
} CudaDevice;

int cuda_get_device_count() {
    int count = 0;
    cudaError_t err = cudaGetDeviceCount(&count);
    if (err != cudaSuccess) {
        cuda_set_error(cudaGetErrorString(err));
        return -1;
    }
    return count;
}

int cuda_is_available() {
    return cuda_get_device_count() > 0 ? 1 : 0;
}

CudaDevice* cuda_create_device(int device_id) {
    cudaError_t err = cudaSetDevice(device_id);
    if (err != cudaSuccess) {
        cuda_set_error(cudaGetErrorString(err));
        return NULL;
    }

    CudaDevice* dev = (CudaDevice*)malloc(sizeof(CudaDevice));
    if (!dev) {
        cuda_set_error("Failed to allocate device struct");
        return NULL;
    }

    dev->device_id = device_id;

    // Create cuBLAS handle
    cublasStatus_t status = cublasCreate(&dev->cublas_handle);
    if (status != CUBLAS_STATUS_SUCCESS) {
        cuda_set_error("Failed to create cuBLAS handle");
        free(dev);
        return NULL;
    }

    // Create CUDA stream
    err = cudaStreamCreate(&dev->stream);
    if (err != cudaSuccess) {
        cuda_set_error(cudaGetErrorString(err));
        cublasDestroy(dev->cublas_handle);
        free(dev);
        return NULL;
    }

    // Associate stream with cuBLAS
    cublasSetStream(dev->cublas_handle, dev->stream);

    return dev;
}

void cuda_release_device(CudaDevice* dev) {
    if (dev) {
        if (dev->stream) cudaStreamDestroy(dev->stream);
        if (dev->cublas_handle) cublasDestroy(dev->cublas_handle);
        free(dev);
    }
}

const char* cuda_device_name(int device_id) {
    static char name[256];
    struct cudaDeviceProp prop;
    cudaError_t err = cudaGetDeviceProperties(&prop, device_id);
    if (err != cudaSuccess) {
        return "Unknown";
    }
    strncpy(name, prop.name, sizeof(name) - 1);
    return name;
}

size_t cuda_device_memory(int device_id) {
    struct cudaDeviceProp prop;
    cudaError_t err = cudaGetDeviceProperties(&prop, device_id);
    if (err != cudaSuccess) {
        return 0;
    }
    return prop.totalGlobalMem;
}

int cuda_device_compute_capability(int device_id) {
    struct cudaDeviceProp prop;
    cudaError_t err = cudaGetDeviceProperties(&prop, device_id);
    if (err != cudaSuccess) {
        return 0;
    }
    return prop.major * 10 + prop.minor;
}

// Buffer management
typedef struct {
    float* data;
    size_t size;
    int memory_type; // 0 = device, 1 = host pinned
} CudaBuffer;

CudaBuffer* cuda_create_buffer(CudaDevice* dev, float* host_data, size_t count, int memory_type) {
    CudaBuffer* buf = (CudaBuffer*)malloc(sizeof(CudaBuffer));
    if (!buf) {
        cuda_set_error("Failed to allocate buffer struct");
        return NULL;
    }

    buf->size = count * sizeof(float);
    buf->memory_type = memory_type;

    cudaError_t err;
    if (memory_type == 0) {
        // Device memory
        err = cudaMalloc((void**)&buf->data, buf->size);
        if (err != cudaSuccess) {
            cuda_set_error(cudaGetErrorString(err));
            free(buf);
            return NULL;
        }

        if (host_data) {
            err = cudaMemcpy(buf->data, host_data, buf->size, cudaMemcpyHostToDevice);
            if (err != cudaSuccess) {
                cuda_set_error(cudaGetErrorString(err));
                cudaFree(buf->data);
                free(buf);
                return NULL;
            }
        }
    } else {
        // Host pinned memory (for faster transfers)
        err = cudaMallocHost((void**)&buf->data, buf->size);
        if (err != cudaSuccess) {
            cuda_set_error(cudaGetErrorString(err));
            free(buf);
            return NULL;
        }

        if (host_data) {
            memcpy(buf->data, host_data, buf->size);
        }
    }

    return buf;
}

void cuda_release_buffer(CudaBuffer* buf) {
    if (buf) {
        if (buf->data) {
            if (buf->memory_type == 0) {
                cudaFree(buf->data);
            } else {
                cudaFreeHost(buf->data);
            }
        }
        free(buf);
    }
}

void* cuda_buffer_data(CudaBuffer* buf) {
    return buf ? buf->data : NULL;
}

size_t cuda_buffer_size(CudaBuffer* buf) {
    return buf ? buf->size : 0;
}

int cuda_buffer_copy_to_host(CudaBuffer* buf, float* host_data, size_t count) {
    if (!buf || !host_data) return -1;

    size_t copy_size = count * sizeof(float);
    if (copy_size > buf->size) copy_size = buf->size;

    cudaError_t err;
    if (buf->memory_type == 0) {
        err = cudaMemcpy(host_data, buf->data, copy_size, cudaMemcpyDeviceToHost);
    } else {
        memcpy(host_data, buf->data, copy_size);
        err = cudaSuccess;
    }

    if (err != cudaSuccess) {
        cuda_set_error(cudaGetErrorString(err));
        return -1;
    }
    return 0;
}

// Vector operations using cuBLAS

// Compute L2 norms for each vector (for normalization)
// vectors: n x dims matrix (row-major)
// norms: output array of n floats
int cuda_compute_norms(CudaDevice* dev, CudaBuffer* vectors, CudaBuffer* norms,
                       unsigned int n, unsigned int dims) {
    cublasStatus_t status;

    for (unsigned int i = 0; i < n; i++) {
        float* vec = vectors->data + i * dims;
        status = cublasSnrm2(dev->cublas_handle, dims, vec, 1, norms->data + i);
        if (status != CUBLAS_STATUS_SUCCESS) {
            cuda_set_error("cuBLAS norm computation failed");
            return -1;
        }
    }

    cudaStreamSynchronize(dev->stream);
    return 0;
}

// Normalize vectors in-place
int cuda_normalize_vectors(CudaDevice* dev, CudaBuffer* vectors,
                           unsigned int n, unsigned int dims) {
    // Allocate norms buffer
    CudaBuffer* norms = cuda_create_buffer(dev, NULL, n, 0);
    if (!norms) return -1;

    // Compute norms
    if (cuda_compute_norms(dev, vectors, norms, n, dims) != 0) {
        cuda_release_buffer(norms);
        return -1;
    }

    // Copy norms to host for scaling
    float* host_norms = (float*)malloc(n * sizeof(float));
    cuda_buffer_copy_to_host(norms, host_norms, n);

    // Scale each vector by 1/norm
    for (unsigned int i = 0; i < n; i++) {
        if (host_norms[i] > 1e-10f) {
            float scale = 1.0f / host_norms[i];
            cublasStatus_t status = cublasSscal(dev->cublas_handle, dims,
                                                 &scale, vectors->data + i * dims, 1);
            if (status != CUBLAS_STATUS_SUCCESS) {
                cuda_set_error("cuBLAS scale failed");
                free(host_norms);
                cuda_release_buffer(norms);
                return -1;
            }
        }
    }

    cudaStreamSynchronize(dev->stream);
    free(host_norms);
    cuda_release_buffer(norms);
    return 0;
}

// Compute cosine similarity: scores = embeddings @ query (all normalized)
// embeddings: n x dims (row-major on device)
// query: dims x 1 (column vector on device)
// scores: n x 1 output
int cuda_cosine_similarity(CudaDevice* dev, CudaBuffer* embeddings, CudaBuffer* query,
                           CudaBuffer* scores, unsigned int n, unsigned int dims,
                           int normalized) {
    // If not normalized, we'd need to normalize first
    // For now, assume normalized (dot product = cosine similarity)

    float alpha = 1.0f;
    float beta = 0.0f;

    // Matrix-vector multiply: scores = embeddings * query
    // embeddings is n x dims (row-major)
    // In cuBLAS (column-major), we treat it as dims x n and transpose
    cublasStatus_t status = cublasSgemv(dev->cublas_handle,
                                         CUBLAS_OP_T,  // Transpose because row-major
                                         dims, n,       // Matrix dimensions
                                         &alpha,
                                         embeddings->data, dims,  // A matrix
                                         query->data, 1,          // x vector
                                         &beta,
                                         scores->data, 1);        // y vector

    if (status != CUBLAS_STATUS_SUCCESS) {
        cuda_set_error("cuBLAS gemv failed");
        return -1;
    }

    cudaStreamSynchronize(dev->stream);
    return 0;
}

// Simple top-k selection (CPU implementation for now)
// For production, use thrust::sort or custom CUDA kernel
int cuda_topk(CudaDevice* dev, CudaBuffer* scores, unsigned int* out_indices,
              float* out_scores, unsigned int n, unsigned int k) {
    // Copy scores to host
    float* host_scores = (float*)malloc(n * sizeof(float));
    if (cuda_buffer_copy_to_host(scores, host_scores, n) != 0) {
        free(host_scores);
        return -1;
    }

    // Simple selection sort for top-k
    unsigned int* indices = (unsigned int*)malloc(n * sizeof(unsigned int));
    for (unsigned int i = 0; i < n; i++) indices[i] = i;

    for (unsigned int i = 0; i < k && i < n; i++) {
        unsigned int max_idx = i;
        for (unsigned int j = i + 1; j < n; j++) {
            if (host_scores[indices[j]] > host_scores[indices[max_idx]]) {
                max_idx = j;
            }
        }
        // Swap
        unsigned int tmp = indices[i];
        indices[i] = indices[max_idx];
        indices[max_idx] = tmp;
    }

    // Copy top-k results
    for (unsigned int i = 0; i < k && i < n; i++) {
        out_indices[i] = indices[i];
        out_scores[i] = host_scores[indices[i]];
    }

    free(host_scores);
    free(indices);
    return 0;
}
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
	ErrCUDANotAvailable = errors.New("cuda: CUDA is not available on this system")
	ErrDeviceCreation   = errors.New("cuda: failed to create CUDA device")
	ErrBufferCreation   = errors.New("cuda: failed to create buffer")
	ErrKernelExecution  = errors.New("cuda: kernel execution failed")
	ErrInvalidBuffer    = errors.New("cuda: invalid buffer")
)

// MemoryType defines how buffer memory is managed.
type MemoryType int

const (
	// MemoryDevice allocates memory on GPU device.
	MemoryDevice MemoryType = 0

	// MemoryPinned allocates page-locked host memory for faster transfers.
	MemoryPinned MemoryType = 1
)

// Device represents a CUDA GPU device.
type Device struct {
	ptr     *C.CudaDevice
	id      int
	name    string
	memory  uint64
	ccMajor int
	ccMinor int
	mu      sync.Mutex
}

// Buffer represents a CUDA memory buffer.
type Buffer struct {
	ptr    *C.CudaBuffer
	size   uint64
	device *Device
}

// SearchResult holds a similarity search result.
type SearchResult struct {
	Index uint32
	Score float32
}

// IsAvailable checks if CUDA is available on this system.
func IsAvailable() bool {
	return C.cuda_is_available() != 0
}

// DeviceCount returns the number of CUDA devices.
func DeviceCount() int {
	count := C.cuda_get_device_count()
	if count < 0 {
		return 0
	}
	return int(count)
}

// NewDevice creates a new CUDA device handle.
func NewDevice(deviceID int) (*Device, error) {
	if !IsAvailable() {
		return nil, ErrCUDANotAvailable
	}

	ptr := C.cuda_create_device(C.int(deviceID))
	if ptr == nil {
		errMsg := C.GoString(C.cuda_get_last_error())
		C.cuda_clear_error()
		return nil, fmt.Errorf("%w: %s", ErrDeviceCreation, errMsg)
	}

	cc := int(C.cuda_device_compute_capability(C.int(deviceID)))

	return &Device{
		ptr:     ptr,
		id:      deviceID,
		name:    C.GoString(C.cuda_device_name(C.int(deviceID))),
		memory:  uint64(C.cuda_device_memory(C.int(deviceID))),
		ccMajor: cc / 10,
		ccMinor: cc % 10,
	}, nil
}

// Release frees the CUDA device resources.
func (d *Device) Release() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.ptr != nil {
		C.cuda_release_device(d.ptr)
		d.ptr = nil
	}
}

// ID returns the device ID.
func (d *Device) ID() int {
	return d.id
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

// ComputeCapability returns the CUDA compute capability (major, minor).
func (d *Device) ComputeCapability() (int, int) {
	return d.ccMajor, d.ccMinor
}

// NewBuffer creates a new GPU buffer with data.
func (d *Device) NewBuffer(data []float32, memType MemoryType) (*Buffer, error) {
	if len(data) == 0 {
		return nil, errors.New("cuda: cannot create empty buffer")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	ptr := C.cuda_create_buffer(
		d.ptr,
		(*C.float)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		C.int(memType),
	)

	if ptr == nil {
		errMsg := C.GoString(C.cuda_get_last_error())
		C.cuda_clear_error()
		return nil, fmt.Errorf("%w: %s", ErrBufferCreation, errMsg)
	}

	return &Buffer{
		ptr:    ptr,
		size:   uint64(len(data) * 4),
		device: d,
	}, nil
}

// NewEmptyBuffer creates an uninitialized GPU buffer.
func (d *Device) NewEmptyBuffer(count uint64, memType MemoryType) (*Buffer, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	ptr := C.cuda_create_buffer(
		d.ptr,
		nil,
		C.size_t(count),
		C.int(memType),
	)

	if ptr == nil {
		errMsg := C.GoString(C.cuda_get_last_error())
		C.cuda_clear_error()
		return nil, fmt.Errorf("%w: %s", ErrBufferCreation, errMsg)
	}

	return &Buffer{
		ptr:    ptr,
		size:   count * 4,
		device: d,
	}, nil
}

// Release frees the buffer resources.
func (b *Buffer) Release() {
	if b.ptr != nil {
		C.cuda_release_buffer(b.ptr)
		b.ptr = nil
	}
}

// Size returns the buffer size in bytes.
func (b *Buffer) Size() uint64 {
	return b.size
}

// ReadFloat32 reads float32 values from the buffer.
func (b *Buffer) ReadFloat32(count int) []float32 {
	if count <= 0 || uint64(count*4) > b.size {
		return nil
	}

	result := make([]float32, count)
	ret := C.cuda_buffer_copy_to_host(b.ptr, (*C.float)(unsafe.Pointer(&result[0])), C.size_t(count))
	if ret != 0 {
		return nil
	}
	return result
}

// NormalizeVectors normalizes vectors in-place to unit length.
func (d *Device) NormalizeVectors(vectors *Buffer, n, dimensions uint32) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	ret := C.cuda_normalize_vectors(d.ptr, vectors.ptr, C.uint(n), C.uint(dimensions))
	if ret != 0 {
		errMsg := C.GoString(C.cuda_get_last_error())
		C.cuda_clear_error()
		return fmt.Errorf("%w: %s", ErrKernelExecution, errMsg)
	}
	return nil
}

// CosineSimilarity computes cosine similarity between query and all embeddings.
func (d *Device) CosineSimilarity(embeddings, query, scores *Buffer,
	n, dimensions uint32, normalized bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	normalizedInt := 0
	if normalized {
		normalizedInt = 1
	}

	ret := C.cuda_cosine_similarity(d.ptr, embeddings.ptr, query.ptr, scores.ptr,
		C.uint(n), C.uint(dimensions), C.int(normalizedInt))
	if ret != 0 {
		errMsg := C.GoString(C.cuda_get_last_error())
		C.cuda_clear_error()
		return fmt.Errorf("%w: %s", ErrKernelExecution, errMsg)
	}
	return nil
}

// TopK finds the k highest scoring indices.
func (d *Device) TopK(scores *Buffer, n, k uint32) ([]uint32, []float32, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	indices := make([]uint32, k)
	topkScores := make([]float32, k)

	ret := C.cuda_topk(d.ptr, scores.ptr,
		(*C.uint)(unsafe.Pointer(&indices[0])),
		(*C.float)(unsafe.Pointer(&topkScores[0])),
		C.uint(n), C.uint(k))
	if ret != 0 {
		errMsg := C.GoString(C.cuda_get_last_error())
		C.cuda_clear_error()
		return nil, nil, fmt.Errorf("%w: %s", ErrKernelExecution, errMsg)
	}

	return indices, topkScores, nil
}

// Search performs a complete similarity search.
func (d *Device) Search(embeddings *Buffer, query []float32, n, dimensions uint32, k int, normalized bool) ([]SearchResult, error) {
	if k <= 0 {
		return nil, nil
	}
	if k > int(n) {
		k = int(n)
	}

	// Create query buffer
	queryBuf, err := d.NewBuffer(query, MemoryDevice)
	if err != nil {
		return nil, err
	}
	defer queryBuf.Release()

	// Create scores buffer
	scoresBuf, err := d.NewEmptyBuffer(uint64(n), MemoryDevice)
	if err != nil {
		return nil, err
	}
	defer scoresBuf.Release()

	// Compute similarities
	if err := d.CosineSimilarity(embeddings, queryBuf, scoresBuf, n, dimensions, normalized); err != nil {
		return nil, err
	}

	// Find top-k
	indices, scores, err := d.TopK(scoresBuf, n, uint32(k))
	if err != nil {
		return nil, err
	}

	// Build results
	results := make([]SearchResult, k)
	for i := 0; i < k; i++ {
		results[i] = SearchResult{
			Index: indices[i],
			Score: scores[i],
		}
	}

	return results, nil
}

// HasGPUHardware returns true if CUDA GPU hardware is available.
func HasGPUHardware() bool {
	return IsAvailable()
}

// IsCUDACapable returns true if CUDA operations are available.
// In the real CUDA build, this is always true if IsAvailable() is true.
func IsCUDACapable() bool {
	return IsAvailable()
}

// GPUName returns the name of the first CUDA device.
func GPUName() string {
	if !IsAvailable() {
		return ""
	}
	device, err := NewDevice(0)
	if err != nil {
		return ""
	}
	defer device.Release()
	return device.Name()
}

// GPUMemoryMB returns the memory of the first CUDA device in MB.
func GPUMemoryMB() int {
	if !IsAvailable() {
		return 0
	}
	device, err := NewDevice(0)
	if err != nil {
		return 0
	}
	defer device.Release()
	return device.MemoryMB()
}
