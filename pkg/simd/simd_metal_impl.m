//go:build darwin && cgo && !nometal
// +build darwin,cgo,!nometal

// simd_metal_impl.m - Metal GPU implementation for SIMD vector operations
//
// This file implements the C functions declared in simd_metal_darwin.go
// for GPU-accelerated vector operations using Apple Metal.
//
// Build with: -tags metal

#import <Metal/Metal.h>
#import <Foundation/Foundation.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <math.h>

// Global error message storage
static char simd_g_error_message[1024] = {0};

static void simd_set_error(NSError* error, const char* context) {
    if (error) {
        snprintf(simd_g_error_message, sizeof(simd_g_error_message), "%s: %s",
                 context, [[error localizedDescription] UTF8String]);
    } else {
        snprintf(simd_g_error_message, sizeof(simd_g_error_message), "%s", context);
    }
}

// =============================================================================
// Metal Context for SIMD operations
// =============================================================================

typedef struct {
    id<MTLDevice> device;
    id<MTLCommandQueue> commandQueue;
    id<MTLLibrary> library;
    id<MTLComputePipelineState> dotProductKernel;
    id<MTLComputePipelineState> cosineSimilarityKernel;
    id<MTLComputePipelineState> euclideanDistanceKernel;
    id<MTLComputePipelineState> normKernel;
    id<MTLComputePipelineState> normalizeKernel;
    id<MTLComputePipelineState> batchCosineKernel;
    id<MTLComputePipelineState> batchDotKernel;
    id<MTLComputePipelineState> batchEuclideanKernel;
} SIMDMetalContext;

// Embedded Metal shaders for SIMD operations
static NSString* simd_shader_source = @R"(
#include <metal_stdlib>
using namespace metal;

// Parallel reduction helper - sum across threadgroup
kernel void reduce_sum(
    device const float* input [[buffer(0)]],
    device float* output [[buffer(1)]],
    constant uint& n [[buffer(2)]],
    threadgroup float* shared [[threadgroup(0)]],
    uint gid [[thread_position_in_grid]],
    uint lid [[thread_position_in_threadgroup]],
    uint tg_size [[threads_per_threadgroup]])
{
    float sum = 0.0f;
    for (uint i = gid; i < n; i += tg_size * 64) { // 64 threadgroups
        sum += input[i];
    }
    shared[lid] = sum;
    threadgroup_barrier(mem_flags::mem_threadgroup);
    
    // Reduce within threadgroup
    for (uint s = tg_size / 2; s > 0; s >>= 1) {
        if (lid < s) {
            shared[lid] += shared[lid + s];
        }
        threadgroup_barrier(mem_flags::mem_threadgroup);
    }
    
    if (lid == 0) {
        output[0] = shared[0];
    }
}

// Batch cosine similarity - each thread handles one vector
kernel void batch_cosine_similarity(
    device const float* embeddings [[buffer(0)]],
    device const float* query [[buffer(1)]],
    device float* scores [[buffer(2)]],
    constant uint& num_vectors [[buffer(3)]],
    constant uint& dimensions [[buffer(4)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= num_vectors) return;
    
    float dot = 0.0f;
    float normA = 0.0f;
    float normB = 0.0f;
    
    uint base = gid * dimensions;
    
    // Vectorized inner loop (Metal compiler will optimize this)
    for (uint i = 0; i < dimensions; i++) {
        float a = embeddings[base + i];
        float b = query[i];
        dot += a * b;
        normA += a * a;
        normB += b * b;
    }
    
    float denom = sqrt(normA * normB);
    scores[gid] = (denom > 0.0f) ? (dot / denom) : 0.0f;
}

// Batch dot product - each thread handles one vector
kernel void batch_dot_product(
    device const float* embeddings [[buffer(0)]],
    device const float* query [[buffer(1)]],
    device float* results [[buffer(2)]],
    constant uint& num_vectors [[buffer(3)]],
    constant uint& dimensions [[buffer(4)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= num_vectors) return;
    
    float dot = 0.0f;
    uint base = gid * dimensions;
    
    for (uint i = 0; i < dimensions; i++) {
        dot += embeddings[base + i] * query[i];
    }
    
    results[gid] = dot;
}

// Batch Euclidean distance - each thread handles one vector
kernel void batch_euclidean_distance(
    device const float* embeddings [[buffer(0)]],
    device const float* query [[buffer(1)]],
    device float* distances [[buffer(2)]],
    constant uint& num_vectors [[buffer(3)]],
    constant uint& dimensions [[buffer(4)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= num_vectors) return;
    
    float sum = 0.0f;
    uint base = gid * dimensions;
    
    for (uint i = 0; i < dimensions; i++) {
        float diff = embeddings[base + i] - query[i];
        sum += diff * diff;
    }
    
    distances[gid] = sqrt(sum);
}

// Batch normalize - each thread handles one vector
kernel void batch_normalize(
    device float* vectors [[buffer(0)]],
    constant uint& num_vectors [[buffer(1)]],
    constant uint& dimensions [[buffer(2)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= num_vectors) return;
    
    uint base = gid * dimensions;
    
    // Compute norm
    float sum = 0.0f;
    for (uint i = 0; i < dimensions; i++) {
        float v = vectors[base + i];
        sum += v * v;
    }
    
    float norm = sqrt(sum);
    if (norm == 0.0f) return;
    
    // Normalize in-place
    float inv_norm = 1.0f / norm;
    for (uint i = 0; i < dimensions; i++) {
        vectors[base + i] *= inv_norm;
    }
}
)";

// Global context (singleton pattern)
static SIMDMetalContext* g_simd_ctx = NULL;

// =============================================================================
// Public C API Implementation
// =============================================================================

bool simd_metal_is_available(void) {
    @autoreleasepool {
        id<MTLDevice> device = MTLCreateSystemDefaultDevice();
        return device != nil;
    }
}

void* simd_metal_create_device(void) {
    @autoreleasepool {
        if (g_simd_ctx != NULL) {
            return g_simd_ctx;
        }
        
        id<MTLDevice> device = MTLCreateSystemDefaultDevice();
        if (!device) {
            simd_set_error(nil, "No Metal device available");
            return NULL;
        }
        
        SIMDMetalContext* ctx = (SIMDMetalContext*)calloc(1, sizeof(SIMDMetalContext));
        if (!ctx) {
            simd_set_error(nil, "Failed to allocate context");
            return NULL;
        }
        
        ctx->device = device;
        ctx->commandQueue = [device newCommandQueue];
        if (!ctx->commandQueue) {
            simd_set_error(nil, "Failed to create command queue");
            free(ctx);
            return NULL;
        }
        
        // Compile shaders from source
        NSError* error = nil;
        MTLCompileOptions* options = [[MTLCompileOptions alloc] init];
        
        // Use mathMode for macOS 15+ or fallback to fastMathEnabled for older versions
        if (@available(macOS 15.0, *)) {
            options.mathMode = MTLMathModeFast;
        } else {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
            options.fastMathEnabled = YES;
#pragma clang diagnostic pop
        }
        
        ctx->library = [device newLibraryWithSource:simd_shader_source options:options error:&error];
        if (!ctx->library) {
            simd_set_error(error, "Failed to compile shaders");
            free(ctx);
            return NULL;
        }
        
        // Create pipeline states for batch operations
        id<MTLFunction> batchCosineFunc = [ctx->library newFunctionWithName:@"batch_cosine_similarity"];
        if (batchCosineFunc) {
            ctx->batchCosineKernel = [device newComputePipelineStateWithFunction:batchCosineFunc error:&error];
        }
        
        id<MTLFunction> batchDotFunc = [ctx->library newFunctionWithName:@"batch_dot_product"];
        if (batchDotFunc) {
            ctx->batchDotKernel = [device newComputePipelineStateWithFunction:batchDotFunc error:&error];
        }
        
        id<MTLFunction> batchEuclideanFunc = [ctx->library newFunctionWithName:@"batch_euclidean_distance"];
        if (batchEuclideanFunc) {
            ctx->batchEuclideanKernel = [device newComputePipelineStateWithFunction:batchEuclideanFunc error:&error];
        }
        
        id<MTLFunction> batchNormalizeFunc = [ctx->library newFunctionWithName:@"batch_normalize"];
        if (batchNormalizeFunc) {
            ctx->normalizeKernel = [device newComputePipelineStateWithFunction:batchNormalizeFunc error:&error];
        }
        
        g_simd_ctx = ctx;
        return ctx;
    }
}

void simd_metal_release_device(void* device) {
    // Context is singleton, don't release
}

void* simd_metal_create_buffer(void* device, void* data, unsigned long size) {
    @autoreleasepool {
        SIMDMetalContext* ctx = (SIMDMetalContext*)device;
        if (!ctx) return NULL;
        
        id<MTLBuffer> buffer;
        if (data) {
            buffer = [ctx->device newBufferWithBytes:data length:size options:MTLResourceStorageModeShared];
        } else {
            buffer = [ctx->device newBufferWithLength:size options:MTLResourceStorageModeShared];
        }
        return (__bridge_retained void*)buffer;
    }
}

void simd_metal_release_buffer(void* buffer) {
    @autoreleasepool {
        if (buffer) {
            id<MTLBuffer> mtlBuffer = (__bridge_transfer id<MTLBuffer>)buffer;
            mtlBuffer = nil;
        }
    }
}

void* simd_metal_buffer_contents(void* buffer) {
    @autoreleasepool {
        if (!buffer) return NULL;
        id<MTLBuffer> mtlBuffer = (__bridge id<MTLBuffer>)buffer;
        return [mtlBuffer contents];
    }
}

// =============================================================================
// Single-pair operations (fallback to CPU for small vectors)
// For single pairs, CPU SIMD is faster due to GPU dispatch overhead
// =============================================================================

float simd_metal_dot_product(void* device, float* a, float* b, unsigned int n) {
    // For single vector pairs, CPU is faster
    // This is a simple scalar fallback - real implementation uses CPU SIMD
    float sum = 0.0f;
    for (unsigned int i = 0; i < n; i++) {
        sum += a[i] * b[i];
    }
    return sum;
}

float simd_metal_cosine_similarity(void* device, float* a, float* b, unsigned int n) {
    float dot = 0.0f;
    float normA = 0.0f;
    float normB = 0.0f;
    
    for (unsigned int i = 0; i < n; i++) {
        dot += a[i] * b[i];
        normA += a[i] * a[i];
        normB += b[i] * b[i];
    }
    
    float denom = sqrtf(normA * normB);
    return (denom > 0.0f) ? (dot / denom) : 0.0f;
}

float simd_metal_euclidean_distance(void* device, float* a, float* b, unsigned int n) {
    float sum = 0.0f;
    for (unsigned int i = 0; i < n; i++) {
        float diff = a[i] - b[i];
        sum += diff * diff;
    }
    return sqrtf(sum);
}

float simd_metal_norm(void* device, float* v, unsigned int n) {
    float sum = 0.0f;
    for (unsigned int i = 0; i < n; i++) {
        sum += v[i] * v[i];
    }
    return sqrtf(sum);
}

// =============================================================================
// Batch operations - these are where GPU shines
// =============================================================================

int simd_metal_batch_cosine_similarity(
    void* device,
    float* embeddings,
    float* query,
    float* scores,
    unsigned int num_vectors,
    unsigned int dimensions)
{
    @autoreleasepool {
        SIMDMetalContext* ctx = (SIMDMetalContext*)device;
        if (!ctx || !ctx->batchCosineKernel) {
            simd_set_error(nil, "Metal context or kernel not initialized");
            return -1;
        }
        
        // Create buffers
        size_t embeddingsSize = num_vectors * dimensions * sizeof(float);
        size_t querySize = dimensions * sizeof(float);
        size_t scoresSize = num_vectors * sizeof(float);
        
        id<MTLBuffer> embeddingsBuffer = [ctx->device newBufferWithBytes:embeddings 
                                                                  length:embeddingsSize 
                                                                 options:MTLResourceStorageModeShared];
        id<MTLBuffer> queryBuffer = [ctx->device newBufferWithBytes:query 
                                                             length:querySize 
                                                            options:MTLResourceStorageModeShared];
        id<MTLBuffer> scoresBuffer = [ctx->device newBufferWithLength:scoresSize 
                                                              options:MTLResourceStorageModeShared];
        
        if (!embeddingsBuffer || !queryBuffer || !scoresBuffer) {
            simd_set_error(nil, "Failed to create buffers");
            return -1;
        }
        
        // Create command buffer and encoder
        id<MTLCommandBuffer> commandBuffer = [ctx->commandQueue commandBuffer];
        id<MTLComputeCommandEncoder> encoder = [commandBuffer computeCommandEncoder];
        
        [encoder setComputePipelineState:ctx->batchCosineKernel];
        [encoder setBuffer:embeddingsBuffer offset:0 atIndex:0];
        [encoder setBuffer:queryBuffer offset:0 atIndex:1];
        [encoder setBuffer:scoresBuffer offset:0 atIndex:2];
        [encoder setBytes:&num_vectors length:sizeof(uint) atIndex:3];
        [encoder setBytes:&dimensions length:sizeof(uint) atIndex:4];
        
        // Calculate thread configuration
        NSUInteger threadgroupSize = MIN(ctx->batchCosineKernel.maxTotalThreadsPerThreadgroup, 256);
        MTLSize threadgroups = MTLSizeMake((num_vectors + threadgroupSize - 1) / threadgroupSize, 1, 1);
        MTLSize threadsPerGroup = MTLSizeMake(threadgroupSize, 1, 1);
        
        [encoder dispatchThreadgroups:threadgroups threadsPerThreadgroup:threadsPerGroup];
        [encoder endEncoding];
        
        [commandBuffer commit];
        [commandBuffer waitUntilCompleted];
        
        if (commandBuffer.error) {
            simd_set_error(commandBuffer.error, "Kernel execution failed");
            return -1;
        }
        
        // Copy results back
        memcpy(scores, [scoresBuffer contents], scoresSize);
        
        return 0;
    }
}

int simd_metal_batch_dot_product(
    void* device,
    float* embeddings,
    float* query,
    float* results,
    unsigned int num_vectors,
    unsigned int dimensions)
{
    @autoreleasepool {
        SIMDMetalContext* ctx = (SIMDMetalContext*)device;
        if (!ctx || !ctx->batchDotKernel) {
            simd_set_error(nil, "Metal context or kernel not initialized");
            return -1;
        }
        
        size_t embeddingsSize = num_vectors * dimensions * sizeof(float);
        size_t querySize = dimensions * sizeof(float);
        size_t resultsSize = num_vectors * sizeof(float);
        
        id<MTLBuffer> embeddingsBuffer = [ctx->device newBufferWithBytes:embeddings 
                                                                  length:embeddingsSize 
                                                                 options:MTLResourceStorageModeShared];
        id<MTLBuffer> queryBuffer = [ctx->device newBufferWithBytes:query 
                                                             length:querySize 
                                                            options:MTLResourceStorageModeShared];
        id<MTLBuffer> resultsBuffer = [ctx->device newBufferWithLength:resultsSize 
                                                               options:MTLResourceStorageModeShared];
        
        if (!embeddingsBuffer || !queryBuffer || !resultsBuffer) {
            simd_set_error(nil, "Failed to create buffers");
            return -1;
        }
        
        id<MTLCommandBuffer> commandBuffer = [ctx->commandQueue commandBuffer];
        id<MTLComputeCommandEncoder> encoder = [commandBuffer computeCommandEncoder];
        
        [encoder setComputePipelineState:ctx->batchDotKernel];
        [encoder setBuffer:embeddingsBuffer offset:0 atIndex:0];
        [encoder setBuffer:queryBuffer offset:0 atIndex:1];
        [encoder setBuffer:resultsBuffer offset:0 atIndex:2];
        [encoder setBytes:&num_vectors length:sizeof(uint) atIndex:3];
        [encoder setBytes:&dimensions length:sizeof(uint) atIndex:4];
        
        NSUInteger threadgroupSize = MIN(ctx->batchDotKernel.maxTotalThreadsPerThreadgroup, 256);
        MTLSize threadgroups = MTLSizeMake((num_vectors + threadgroupSize - 1) / threadgroupSize, 1, 1);
        MTLSize threadsPerGroup = MTLSizeMake(threadgroupSize, 1, 1);
        
        [encoder dispatchThreadgroups:threadgroups threadsPerThreadgroup:threadsPerGroup];
        [encoder endEncoding];
        
        [commandBuffer commit];
        [commandBuffer waitUntilCompleted];
        
        if (commandBuffer.error) {
            simd_set_error(commandBuffer.error, "Kernel execution failed");
            return -1;
        }
        
        memcpy(results, [resultsBuffer contents], resultsSize);
        return 0;
    }
}

int simd_metal_batch_euclidean_distance(
    void* device,
    float* embeddings,
    float* query,
    float* distances,
    unsigned int num_vectors,
    unsigned int dimensions)
{
    @autoreleasepool {
        SIMDMetalContext* ctx = (SIMDMetalContext*)device;
        if (!ctx || !ctx->batchEuclideanKernel) {
            simd_set_error(nil, "Metal context or kernel not initialized");
            return -1;
        }
        
        size_t embeddingsSize = num_vectors * dimensions * sizeof(float);
        size_t querySize = dimensions * sizeof(float);
        size_t distancesSize = num_vectors * sizeof(float);
        
        id<MTLBuffer> embeddingsBuffer = [ctx->device newBufferWithBytes:embeddings 
                                                                  length:embeddingsSize 
                                                                 options:MTLResourceStorageModeShared];
        id<MTLBuffer> queryBuffer = [ctx->device newBufferWithBytes:query 
                                                             length:querySize 
                                                            options:MTLResourceStorageModeShared];
        id<MTLBuffer> distancesBuffer = [ctx->device newBufferWithLength:distancesSize 
                                                                 options:MTLResourceStorageModeShared];
        
        if (!embeddingsBuffer || !queryBuffer || !distancesBuffer) {
            simd_set_error(nil, "Failed to create buffers");
            return -1;
        }
        
        id<MTLCommandBuffer> commandBuffer = [ctx->commandQueue commandBuffer];
        id<MTLComputeCommandEncoder> encoder = [commandBuffer computeCommandEncoder];
        
        [encoder setComputePipelineState:ctx->batchEuclideanKernel];
        [encoder setBuffer:embeddingsBuffer offset:0 atIndex:0];
        [encoder setBuffer:queryBuffer offset:0 atIndex:1];
        [encoder setBuffer:distancesBuffer offset:0 atIndex:2];
        [encoder setBytes:&num_vectors length:sizeof(uint) atIndex:3];
        [encoder setBytes:&dimensions length:sizeof(uint) atIndex:4];
        
        NSUInteger threadgroupSize = MIN(ctx->batchEuclideanKernel.maxTotalThreadsPerThreadgroup, 256);
        MTLSize threadgroups = MTLSizeMake((num_vectors + threadgroupSize - 1) / threadgroupSize, 1, 1);
        MTLSize threadsPerGroup = MTLSizeMake(threadgroupSize, 1, 1);
        
        [encoder dispatchThreadgroups:threadgroups threadsPerThreadgroup:threadsPerGroup];
        [encoder endEncoding];
        
        [commandBuffer commit];
        [commandBuffer waitUntilCompleted];
        
        if (commandBuffer.error) {
            simd_set_error(commandBuffer.error, "Kernel execution failed");
            return -1;
        }
        
        memcpy(distances, [distancesBuffer contents], distancesSize);
        return 0;
    }
}

int simd_metal_batch_normalize(
    void* device,
    float* vectors,
    unsigned int num_vectors,
    unsigned int dimensions)
{
    @autoreleasepool {
        SIMDMetalContext* ctx = (SIMDMetalContext*)device;
        if (!ctx || !ctx->normalizeKernel) {
            simd_set_error(nil, "Metal context or kernel not initialized");
            return -1;
        }
        
        size_t vectorsSize = num_vectors * dimensions * sizeof(float);
        
        id<MTLBuffer> vectorsBuffer = [ctx->device newBufferWithBytes:vectors 
                                                               length:vectorsSize 
                                                              options:MTLResourceStorageModeShared];
        
        if (!vectorsBuffer) {
            simd_set_error(nil, "Failed to create buffer");
            return -1;
        }
        
        id<MTLCommandBuffer> commandBuffer = [ctx->commandQueue commandBuffer];
        id<MTLComputeCommandEncoder> encoder = [commandBuffer computeCommandEncoder];
        
        [encoder setComputePipelineState:ctx->normalizeKernel];
        [encoder setBuffer:vectorsBuffer offset:0 atIndex:0];
        [encoder setBytes:&num_vectors length:sizeof(uint) atIndex:1];
        [encoder setBytes:&dimensions length:sizeof(uint) atIndex:2];
        
        NSUInteger threadgroupSize = MIN(ctx->normalizeKernel.maxTotalThreadsPerThreadgroup, 256);
        MTLSize threadgroups = MTLSizeMake((num_vectors + threadgroupSize - 1) / threadgroupSize, 1, 1);
        MTLSize threadsPerGroup = MTLSizeMake(threadgroupSize, 1, 1);
        
        [encoder dispatchThreadgroups:threadgroups threadsPerThreadgroup:threadsPerGroup];
        [encoder endEncoding];
        
        [commandBuffer commit];
        [commandBuffer waitUntilCompleted];
        
        if (commandBuffer.error) {
            simd_set_error(commandBuffer.error, "Kernel execution failed");
            return -1;
        }
        
        // Copy results back (in-place modification)
        memcpy(vectors, [vectorsBuffer contents], vectorsSize);
        return 0;
    }
}

const char* simd_metal_last_error(void) {
    return simd_g_error_message;
}
