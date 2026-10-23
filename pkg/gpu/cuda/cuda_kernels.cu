// CUDA kernels for GPU-accelerated vector similarity search.
// Compile with: nvcc -c -o cuda_kernels.o cuda_kernels.cu
// Or create fatbin: nvcc -fatbin -o cuda_kernels.fatbin cuda_kernels.cu

#include <cuda_runtime.h>
#include <cublas_v2.h>
#include <stdint.h>

extern "C" {

// ============================================================================
// Device Information
// ============================================================================

// Get number of CUDA devices
int cuda_device_count() {
    int count = 0;
    cudaError_t err = cudaGetDeviceCount(&count);
    if (err != cudaSuccess) {
        return 0;
    }
    return count;
}

// Check if CUDA is available
int cuda_is_available() {
    return cuda_device_count() > 0 ? 1 : 0;
}

// Get device name
int cuda_get_device_name(int device_id, char* name, int max_len) {
    cudaDeviceProp prop;
    cudaError_t err = cudaGetDeviceProperties(&prop, device_id);
    if (err != cudaSuccess) {
        return -1;
    }
    strncpy(name, prop.name, max_len - 1);
    name[max_len - 1] = '\0';
    return 0;
}

// Get device memory in bytes
uint64_t cuda_get_device_memory(int device_id) {
    cudaDeviceProp prop;
    cudaError_t err = cudaGetDeviceProperties(&prop, device_id);
    if (err != cudaSuccess) {
        return 0;
    }
    return (uint64_t)prop.totalGlobalMem;
}

// Get compute capability
int cuda_get_compute_capability(int device_id, int* major, int* minor) {
    cudaDeviceProp prop;
    cudaError_t err = cudaGetDeviceProperties(&prop, device_id);
    if (err != cudaSuccess) {
        return -1;
    }
    *major = prop.major;
    *minor = prop.minor;
    return 0;
}

// Set active device
int cuda_set_device(int device_id) {
    cudaError_t err = cudaSetDevice(device_id);
    return err == cudaSuccess ? 0 : -1;
}

// ============================================================================
// Memory Management
// ============================================================================

// Allocate device memory
void* cuda_malloc(uint64_t size) {
    void* ptr = NULL;
    cudaError_t err = cudaMalloc(&ptr, size);
    if (err != cudaSuccess) {
        return NULL;
    }
    return ptr;
}

// Allocate pinned (page-locked) host memory for faster transfers
void* cuda_malloc_host(uint64_t size) {
    void* ptr = NULL;
    cudaError_t err = cudaMallocHost(&ptr, size);
    if (err != cudaSuccess) {
        return NULL;
    }
    return ptr;
}

// Free device memory
void cuda_free(void* ptr) {
    if (ptr != NULL) {
        cudaFree(ptr);
    }
}

// Free pinned host memory
void cuda_free_host(void* ptr) {
    if (ptr != NULL) {
        cudaFreeHost(ptr);
    }
}

// Copy data from host to device
int cuda_memcpy_to_device(void* dst, const void* src, uint64_t size) {
    cudaError_t err = cudaMemcpy(dst, src, size, cudaMemcpyHostToDevice);
    return err == cudaSuccess ? 0 : -1;
}

// Copy data from device to host
int cuda_memcpy_to_host(void* dst, const void* src, uint64_t size) {
    cudaError_t err = cudaMemcpy(dst, src, size, cudaMemcpyDeviceToHost);
    return err == cudaSuccess ? 0 : -1;
}

// Async copy to device
int cuda_memcpy_to_device_async(void* dst, const void* src, uint64_t size, cudaStream_t stream) {
    cudaError_t err = cudaMemcpyAsync(dst, src, size, cudaMemcpyHostToDevice, stream);
    return err == cudaSuccess ? 0 : -1;
}

// Async copy to host
int cuda_memcpy_to_host_async(void* dst, const void* src, uint64_t size, cudaStream_t stream) {
    cudaError_t err = cudaMemcpyAsync(dst, src, size, cudaMemcpyDeviceToHost, stream);
    return err == cudaSuccess ? 0 : -1;
}

// ============================================================================
// Stream Management
// ============================================================================

cudaStream_t cuda_stream_create() {
    cudaStream_t stream;
    cudaError_t err = cudaStreamCreate(&stream);
    if (err != cudaSuccess) {
        return NULL;
    }
    return stream;
}

void cuda_stream_destroy(cudaStream_t stream) {
    if (stream != NULL) {
        cudaStreamDestroy(stream);
    }
}

int cuda_stream_synchronize(cudaStream_t stream) {
    cudaError_t err = cudaStreamSynchronize(stream);
    return err == cudaSuccess ? 0 : -1;
}

// ============================================================================
// cuBLAS Handle Management
// ============================================================================

cublasHandle_t cuda_cublas_create() {
    cublasHandle_t handle;
    cublasStatus_t status = cublasCreate(&handle);
    if (status != CUBLAS_STATUS_SUCCESS) {
        return NULL;
    }
    return handle;
}

void cuda_cublas_destroy(cublasHandle_t handle) {
    if (handle != NULL) {
        cublasDestroy(handle);
    }
}

int cuda_cublas_set_stream(cublasHandle_t handle, cudaStream_t stream) {
    cublasStatus_t status = cublasSetStream(handle, stream);
    return status == CUBLAS_STATUS_SUCCESS ? 0 : -1;
}

// ============================================================================
// Vector Operations Kernels
// ============================================================================

// Kernel: Compute L2 norm for each vector (for normalization)
__global__ void kernel_compute_norms(
    const float* vectors,
    float* norms,
    uint32_t n,
    uint32_t dimensions
) {
    uint32_t idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    
    float sum = 0.0f;
    const float* vec = vectors + (size_t)idx * dimensions;
    
    for (uint32_t d = 0; d < dimensions; d++) {
        float val = vec[d];
        sum += val * val;
    }
    
    norms[idx] = sqrtf(sum);
}

// Kernel: Normalize vectors in-place
__global__ void kernel_normalize_vectors(
    float* vectors,
    const float* norms,
    uint32_t n,
    uint32_t dimensions
) {
    uint32_t vec_idx = blockIdx.x;
    uint32_t dim_idx = threadIdx.x;
    
    if (vec_idx >= n) return;
    
    float norm = norms[vec_idx];
    if (norm < 1e-10f) norm = 1.0f; // Avoid division by zero
    
    // Each thread handles one dimension stride
    for (uint32_t d = dim_idx; d < dimensions; d += blockDim.x) {
        vectors[(size_t)vec_idx * dimensions + d] /= norm;
    }
}

// Wrapper function for normalize (internal kernel version)
static int cuda_normalize_vectors_kernel(
    float* d_vectors,
    uint32_t n,
    uint32_t dimensions,
    cudaStream_t stream
) {
    // Allocate temporary buffer for norms
    float* d_norms;
    cudaError_t err = cudaMalloc(&d_norms, n * sizeof(float));
    if (err != cudaSuccess) return -1;
    
    // Compute norms
    int threads_per_block = 256;
    int blocks = (n + threads_per_block - 1) / threads_per_block;
    kernel_compute_norms<<<blocks, threads_per_block, 0, stream>>>(
        d_vectors, d_norms, n, dimensions
    );
    
    // Normalize vectors
    threads_per_block = min(256, (int)dimensions);
    kernel_normalize_vectors<<<n, threads_per_block, 0, stream>>>(
        d_vectors, d_norms, n, dimensions
    );
    
    err = cudaGetLastError();
    cudaFree(d_norms);
    
    return err == cudaSuccess ? 0 : -1;
}

// ============================================================================
// Cosine Similarity Kernels
// ============================================================================

// Kernel: Compute cosine similarity between query and all vectors
// For pre-normalized vectors: similarity = dot product
__global__ void kernel_cosine_similarity_normalized(
    const float* embeddings,  // [n x dimensions]
    const float* query,       // [dimensions]
    float* scores,            // [n]
    uint32_t n,
    uint32_t dimensions
) {
    uint32_t idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    
    float dot = 0.0f;
    const float* vec = embeddings + (size_t)idx * dimensions;
    
    for (uint32_t d = 0; d < dimensions; d++) {
        dot += vec[d] * query[d];
    }
    
    scores[idx] = dot;
}

// Kernel: Compute cosine similarity with normalization
__global__ void kernel_cosine_similarity(
    const float* embeddings,  // [n x dimensions]
    const float* query,       // [dimensions]
    float* scores,            // [n]
    uint32_t n,
    uint32_t dimensions
) {
    uint32_t idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    
    float dot = 0.0f;
    float norm_e = 0.0f;
    float norm_q = 0.0f;
    
    const float* vec = embeddings + (size_t)idx * dimensions;
    
    for (uint32_t d = 0; d < dimensions; d++) {
        float e = vec[d];
        float q = query[d];
        dot += e * q;
        norm_e += e * e;
        norm_q += q * q;
    }
    
    float denom = sqrtf(norm_e) * sqrtf(norm_q);
    scores[idx] = (denom > 1e-10f) ? (dot / denom) : 0.0f;
}

// Wrapper for cosine similarity computation
int cuda_cosine_similarity(
    const float* d_embeddings,
    const float* d_query,
    float* d_scores,
    uint32_t n,
    uint32_t dimensions,
    int normalized,
    cudaStream_t stream
) {
    int threads_per_block = 256;
    int blocks = (n + threads_per_block - 1) / threads_per_block;
    
    if (normalized) {
        kernel_cosine_similarity_normalized<<<blocks, threads_per_block, 0, stream>>>(
            d_embeddings, d_query, d_scores, n, dimensions
        );
    } else {
        kernel_cosine_similarity<<<blocks, threads_per_block, 0, stream>>>(
            d_embeddings, d_query, d_scores, n, dimensions
        );
    }
    
    cudaError_t err = cudaGetLastError();
    return err == cudaSuccess ? 0 : -1;
}

// ============================================================================
// cuBLAS-based Similarity (faster for large dimensions)
// ============================================================================

// Use cuBLAS GEMV for batched dot products (more efficient for large dims)
int cuda_cosine_similarity_cublas(
    cublasHandle_t handle,
    const float* d_embeddings,  // [n x dimensions], row-major
    const float* d_query,       // [dimensions]
    float* d_scores,            // [n]
    uint32_t n,
    uint32_t dimensions
) {
    // For row-major storage, we use: scores = embeddings * query
    // cuBLAS uses column-major, so we compute: scores^T = query^T * embeddings^T
    // Which is equivalent to: scores = embeddings * query with proper transpose
    
    const float alpha = 1.0f;
    const float beta = 0.0f;
    
    // GEMV: y = alpha * A * x + beta * y
    // Here: scores = embeddings * query
    cublasStatus_t status = cublasSgemv(
        handle,
        CUBLAS_OP_T,           // Transpose embeddings (row-major to col-major)
        dimensions,            // rows of A (before transpose)
        n,                     // cols of A (before transpose) 
        &alpha,
        d_embeddings,          // A matrix
        dimensions,            // leading dimension
        d_query,               // x vector
        1,                     // incx
        &beta,
        d_scores,              // y vector (output)
        1                      // incy
    );
    
    return status == CUBLAS_STATUS_SUCCESS ? 0 : -1;
}

// ============================================================================
// Top-K Selection Kernels
// ============================================================================

// Simple top-k using insertion sort (good for small k)
__global__ void kernel_topk_simple(
    const float* scores,
    uint32_t* indices,
    float* top_scores,
    uint32_t n,
    uint32_t k
) {
    // Single thread computes top-k (for small k, this is efficient)
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    
    // Initialize with minimum values
    for (uint32_t i = 0; i < k; i++) {
        top_scores[i] = -1e30f;
        indices[i] = 0;
    }
    
    // Linear scan with insertion
    for (uint32_t i = 0; i < n; i++) {
        float score = scores[i];
        
        // Check if this score should be in top-k
        if (score > top_scores[k - 1]) {
            // Find insertion position
            uint32_t pos = k - 1;
            while (pos > 0 && score > top_scores[pos - 1]) {
                top_scores[pos] = top_scores[pos - 1];
                indices[pos] = indices[pos - 1];
                pos--;
            }
            top_scores[pos] = score;
            indices[pos] = i;
        }
    }
}

// Parallel top-k using per-block reduction (for larger n)
__global__ void kernel_topk_parallel(
    const float* scores,
    uint32_t* block_indices,   // [num_blocks * k]
    float* block_scores,       // [num_blocks * k]
    uint32_t n,
    uint32_t k
) {
    extern __shared__ char shared_mem[];
    float* s_scores = (float*)shared_mem;
    uint32_t* s_indices = (uint32_t*)(s_scores + k);
    
    uint32_t tid = threadIdx.x;
    uint32_t bid = blockIdx.x;
    uint32_t block_size = blockDim.x;
    uint32_t start = bid * block_size;
    
    // Initialize shared memory top-k
    if (tid < k) {
        s_scores[tid] = -1e30f;
        s_indices[tid] = 0;
    }
    __syncthreads();
    
    // Each thread processes its element
    uint32_t idx = start + tid;
    if (idx < n) {
        float score = scores[idx];
        
        // Atomic insert into shared top-k (simplified - uses critical section)
        for (int i = 0; i < k; i++) {
            float old = atomicMax((int*)&s_scores[i], __float_as_int(score));
            if (__int_as_float(old) < score) {
                // We successfully inserted, shift others down
                atomicExch(&s_indices[i], idx);
                break;
            }
        }
    }
    __syncthreads();
    
    // Write block results
    if (tid < k) {
        block_scores[bid * k + tid] = s_scores[tid];
        block_indices[bid * k + tid] = s_indices[tid];
    }
}

// Wrapper for top-k
int cuda_topk(
    const float* d_scores,
    uint32_t* d_indices,
    float* d_top_scores,
    uint32_t n,
    uint32_t k,
    cudaStream_t stream
) {
    // For simplicity, use the simple kernel (efficient for k <= 100)
    kernel_topk_simple<<<1, 1, 0, stream>>>(
        d_scores, d_indices, d_top_scores, n, k
    );
    
    cudaError_t err = cudaGetLastError();
    return err == cudaSuccess ? 0 : -1;
}

// ============================================================================
// Combined Search Operation
// ============================================================================

// Perform complete search: similarity + top-k
int cuda_search(
    cublasHandle_t handle,
    const float* d_embeddings,
    const float* d_query,
    uint32_t n,
    uint32_t dimensions,
    uint32_t k,
    int normalized,
    uint32_t* h_indices,      // Output: host memory
    float* h_scores,          // Output: host memory
    cudaStream_t stream
) {
    // Allocate temporary buffers
    float* d_scores;
    uint32_t* d_indices;
    float* d_top_scores;
    
    cudaError_t err;
    err = cudaMalloc(&d_scores, n * sizeof(float));
    if (err != cudaSuccess) return -1;
    
    err = cudaMalloc(&d_indices, k * sizeof(uint32_t));
    if (err != cudaSuccess) {
        cudaFree(d_scores);
        return -2;
    }
    
    err = cudaMalloc(&d_top_scores, k * sizeof(float));
    if (err != cudaSuccess) {
        cudaFree(d_scores);
        cudaFree(d_indices);
        return -3;
    }
    
    int result = 0;
    
    // Step 1: Compute similarities
    if (dimensions >= 256) {
        // Use cuBLAS for large dimensions
        result = cuda_cosine_similarity_cublas(
            handle, d_embeddings, d_query, d_scores, n, dimensions
        );
    } else {
        // Use custom kernel for small dimensions
        result = cuda_cosine_similarity(
            d_embeddings, d_query, d_scores, n, dimensions, normalized, stream
        );
    }
    
    if (result != 0) {
        cudaFree(d_scores);
        cudaFree(d_indices);
        cudaFree(d_top_scores);
        return -4;
    }
    
    // Step 2: Find top-k
    result = cuda_topk(d_scores, d_indices, d_top_scores, n, k, stream);
    if (result != 0) {
        cudaFree(d_scores);
        cudaFree(d_indices);
        cudaFree(d_top_scores);
        return -5;
    }
    
    // Step 3: Copy results to host
    err = cudaStreamSynchronize(stream);
    if (err != cudaSuccess) {
        cudaFree(d_scores);
        cudaFree(d_indices);
        cudaFree(d_top_scores);
        return -6;
    }
    
    cudaMemcpy(h_indices, d_indices, k * sizeof(uint32_t), cudaMemcpyDeviceToHost);
    cudaMemcpy(h_scores, d_top_scores, k * sizeof(float), cudaMemcpyDeviceToHost);
    
    // Cleanup
    cudaFree(d_scores);
    cudaFree(d_indices);
    cudaFree(d_top_scores);
    
    return 0;
}

// ============================================================================
// Error Handling
// ============================================================================

const char* cuda_get_last_error() {
    cudaError_t err = cudaGetLastError();
    return cudaGetErrorString(err);
}

void cuda_device_reset() {
    cudaDeviceReset();
}

void cuda_device_synchronize() {
    cudaDeviceSynchronize();
}

} // extern "C"
