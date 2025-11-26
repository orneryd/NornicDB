// metal_bridge.m - Objective-C Metal API wrapper for CGO
//
// This file implements the C functions declared in metal_bridge.go
// that interface with Apple's Metal GPU compute API.

#import <Metal/Metal.h>
#import <Foundation/Foundation.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

// Global error message storage
static char g_error_message[1024] = {0};

static void set_error(NSError* error, const char* context) {
    if (error) {
        snprintf(g_error_message, sizeof(g_error_message), "%s: %s",
                 context, [[error localizedDescription] UTF8String]);
    } else {
        snprintf(g_error_message, sizeof(g_error_message), "%s", context);
    }
}

// =============================================================================
// Device Management
// =============================================================================

typedef struct {
    id<MTLDevice> device;
    id<MTLCommandQueue> commandQueue;
    id<MTLLibrary> library;
    id<MTLComputePipelineState> cosineNormalized;
    id<MTLComputePipelineState> cosineFull;
    id<MTLComputePipelineState> topkSimple;
    id<MTLComputePipelineState> topkSelect;
    id<MTLComputePipelineState> normalize;
} MetalContext;

void* metal_create_device(void) {
    @autoreleasepool {
        id<MTLDevice> device = MTLCreateSystemDefaultDevice();
        if (!device) {
            set_error(nil, "No Metal device available");
            return NULL;
        }
        
        MetalContext* ctx = (MetalContext*)calloc(1, sizeof(MetalContext));
        if (!ctx) {
            set_error(nil, "Failed to allocate context");
            return NULL;
        }
        
        ctx->device = device;
        ctx->commandQueue = [device newCommandQueue];
        if (!ctx->commandQueue) {
            set_error(nil, "Failed to create command queue");
            free(ctx);
            return NULL;
        }
        
        // Load shader library from metallib or compile from source
        NSError* error = nil;
        
        // Try to load pre-compiled metallib first
        NSString* libPath = [[NSBundle mainBundle] pathForResource:@"shaders" ofType:@"metallib"];
        if (libPath) {
            NSURL* libURL = [NSURL fileURLWithPath:libPath];
            ctx->library = [device newLibraryWithURL:libURL error:&error];
        }
        
        // If no metallib, compile from source
        if (!ctx->library) {
            // Embedded shader source (simplified version for initial implementation)
            NSString* shaderSource = @R"(
                #include <metal_stdlib>
                using namespace metal;
                
                kernel void cosine_similarity_normalized(
                    device const float* embeddings [[buffer(0)]],
                    device const float* query [[buffer(1)]],
                    device float* scores [[buffer(2)]],
                    constant uint& n [[buffer(3)]],
                    constant uint& dimensions [[buffer(4)]],
                    uint gid [[thread_position_in_grid]])
                {
                    if (gid >= n) return;
                    
                    float dot = 0.0f;
                    uint base = gid * dimensions;
                    
                    for (uint i = 0; i < dimensions; i++) {
                        dot += embeddings[base + i] * query[i];
                    }
                    
                    scores[gid] = dot;
                }
                
                kernel void cosine_similarity_full(
                    device const float* embeddings [[buffer(0)]],
                    device const float* query [[buffer(1)]],
                    device float* scores [[buffer(2)]],
                    constant uint& n [[buffer(3)]],
                    constant uint& dimensions [[buffer(4)]],
                    uint gid [[thread_position_in_grid]])
                {
                    if (gid >= n) return;
                    
                    float dot = 0.0f;
                    float normA = 0.0f;
                    float normB = 0.0f;
                    
                    uint base = gid * dimensions;
                    
                    for (uint i = 0; i < dimensions; i++) {
                        float a = embeddings[base + i];
                        float b = query[i];
                        dot += a * b;
                        normA += a * a;
                        normB += b * b;
                    }
                    
                    if (normA == 0.0f || normB == 0.0f) {
                        scores[gid] = 0.0f;
                        return;
                    }
                    
                    scores[gid] = dot / (sqrt(normA) * sqrt(normB));
                }
                
                kernel void topk_simple(
                    device const float* scores [[buffer(0)]],
                    device uint* topk_indices [[buffer(1)]],
                    device float* topk_scores [[buffer(2)]],
                    constant uint& n [[buffer(3)]],
                    constant uint& k [[buffer(4)]],
                    uint gid [[thread_position_in_grid]])
                {
                    if (gid != 0) return;
                    
                    for (uint i = 0; i < k; i++) {
                        topk_scores[i] = -2.0f;
                        topk_indices[i] = UINT_MAX;
                    }
                    
                    for (uint i = 0; i < n; i++) {
                        float score = scores[i];
                        
                        if (score > topk_scores[k-1]) {
                            uint pos = k - 1;
                            while (pos > 0 && score > topk_scores[pos-1]) {
                                topk_scores[pos] = topk_scores[pos-1];
                                topk_indices[pos] = topk_indices[pos-1];
                                pos--;
                            }
                            topk_scores[pos] = score;
                            topk_indices[pos] = i;
                        }
                    }
                }
                
                kernel void normalize_vectors(
                    device float* vectors [[buffer(0)]],
                    constant uint& n [[buffer(1)]],
                    constant uint& dimensions [[buffer(2)]],
                    uint gid [[thread_position_in_grid]])
                {
                    if (gid >= n) return;
                    
                    uint base = gid * dimensions;
                    
                    float sum_sq = 0.0f;
                    for (uint i = 0; i < dimensions; i++) {
                        float v = vectors[base + i];
                        sum_sq += v * v;
                    }
                    
                    if (sum_sq == 0.0f) return;
                    
                    float inv_norm = rsqrt(sum_sq);
                    
                    for (uint i = 0; i < dimensions; i++) {
                        vectors[base + i] *= inv_norm;
                    }
                }
            )";
            
            ctx->library = [device newLibraryWithSource:shaderSource options:nil error:&error];
            if (!ctx->library) {
                set_error(error, "Failed to compile shaders");
                free(ctx);
                return NULL;
            }
        }
        
        // Create compute pipelines
        id<MTLFunction> func;
        
        // Cosine similarity (normalized)
        func = [ctx->library newFunctionWithName:@"cosine_similarity_normalized"];
        if (func) {
            ctx->cosineNormalized = [device newComputePipelineStateWithFunction:func error:&error];
            if (!ctx->cosineNormalized) {
                set_error(error, "Failed to create cosine_normalized pipeline");
                free(ctx);
                return NULL;
            }
        }
        
        // Cosine similarity (full)
        func = [ctx->library newFunctionWithName:@"cosine_similarity_full"];
        if (func) {
            ctx->cosineFull = [device newComputePipelineStateWithFunction:func error:&error];
            if (!ctx->cosineFull) {
                set_error(error, "Failed to create cosine_full pipeline");
                free(ctx);
                return NULL;
            }
        }
        
        // Top-k simple
        func = [ctx->library newFunctionWithName:@"topk_simple"];
        if (func) {
            ctx->topkSimple = [device newComputePipelineStateWithFunction:func error:&error];
            if (!ctx->topkSimple) {
                set_error(error, "Failed to create topk_simple pipeline");
                free(ctx);
                return NULL;
            }
        }
        
        // Normalize vectors
        func = [ctx->library newFunctionWithName:@"normalize_vectors"];
        if (func) {
            ctx->normalize = [device newComputePipelineStateWithFunction:func error:&error];
            if (!ctx->normalize) {
                set_error(error, "Failed to create normalize pipeline");
                free(ctx);
                return NULL;
            }
        }
        
        return ctx;
    }
}

void metal_release_device(void* device) {
    if (!device) return;
    
    @autoreleasepool {
        MetalContext* ctx = (MetalContext*)device;
        ctx->device = nil;
        ctx->commandQueue = nil;
        ctx->library = nil;
        ctx->cosineNormalized = nil;
        ctx->cosineFull = nil;
        ctx->topkSimple = nil;
        ctx->topkSelect = nil;
        ctx->normalize = nil;
        free(ctx);
    }
}

bool metal_is_available(void) {
    @autoreleasepool {
        id<MTLDevice> device = MTLCreateSystemDefaultDevice();
        return device != nil;
    }
}

const char* metal_device_name(void* device) {
    if (!device) return "Unknown";
    
    @autoreleasepool {
        MetalContext* ctx = (MetalContext*)device;
        static char name[256];
        strncpy(name, [[ctx->device name] UTF8String], sizeof(name) - 1);
        return name;
    }
}

unsigned long metal_device_memory(void* device) {
    if (!device) return 0;
    
    @autoreleasepool {
        MetalContext* ctx = (MetalContext*)device;
        // On Apple Silicon, recommended max working set is good estimate
        // On Intel Macs, this returns actual VRAM
        if ([ctx->device respondsToSelector:@selector(recommendedMaxWorkingSetSize)]) {
            return [ctx->device recommendedMaxWorkingSetSize];
        }
        return 0;
    }
}

// =============================================================================
// Buffer Management
// =============================================================================

void* metal_create_buffer(void* device, void* data, unsigned long size, int storage_mode) {
    if (!device || size == 0) return NULL;
    
    @autoreleasepool {
        MetalContext* ctx = (MetalContext*)device;
        
        MTLResourceOptions options;
        switch (storage_mode) {
            case 0: // Shared
                options = MTLResourceStorageModeShared;
                break;
            case 1: // Managed
                options = MTLResourceStorageModeManaged;
                break;
            case 2: // Private
                options = MTLResourceStorageModePrivate;
                break;
            default:
                options = MTLResourceStorageModeShared;
        }
        
        id<MTLBuffer> buffer;
        if (data) {
            buffer = [ctx->device newBufferWithBytes:data length:size options:options];
        } else {
            buffer = [ctx->device newBufferWithLength:size options:options];
        }
        
        if (!buffer) {
            set_error(nil, "Failed to allocate buffer");
            return NULL;
        }
        
        return (__bridge_retained void*)buffer;
    }
}

void* metal_create_buffer_no_copy(void* device, void* data, unsigned long size, int storage_mode) {
    if (!device || !data || size == 0) return NULL;
    
    @autoreleasepool {
        MetalContext* ctx = (MetalContext*)device;
        
        // Only works with shared storage on Apple Silicon
        MTLResourceOptions options = MTLResourceStorageModeShared;
        
        id<MTLBuffer> buffer = [ctx->device newBufferWithBytesNoCopy:data
                                                              length:size
                                                             options:options
                                                         deallocator:nil];
        
        if (!buffer) {
            set_error(nil, "Failed to create no-copy buffer");
            return NULL;
        }
        
        return (__bridge_retained void*)buffer;
    }
}

void metal_release_buffer(void* buffer) {
    if (!buffer) return;
    
    @autoreleasepool {
        id<MTLBuffer> buf = (__bridge_transfer id<MTLBuffer>)buffer;
        buf = nil;
    }
}

void* metal_buffer_contents(void* buffer) {
    if (!buffer) return NULL;
    
    @autoreleasepool {
        id<MTLBuffer> buf = (__bridge id<MTLBuffer>)buffer;
        return [buf contents];
    }
}

unsigned long metal_buffer_length(void* buffer) {
    if (!buffer) return 0;
    
    @autoreleasepool {
        id<MTLBuffer> buf = (__bridge id<MTLBuffer>)buffer;
        return [buf length];
    }
}

void metal_buffer_did_modify(void* buffer, unsigned long start, unsigned long length) {
    if (!buffer) return;
    
    @autoreleasepool {
        id<MTLBuffer> buf = (__bridge id<MTLBuffer>)buffer;
        if ([buf respondsToSelector:@selector(didModifyRange:)]) {
            [buf didModifyRange:NSMakeRange(start, length)];
        }
    }
}

// =============================================================================
// Compute Operations
// =============================================================================

int metal_compute_cosine_similarity(
    void* device,
    void* embeddings_buf,
    void* query_buf,
    void* scores_buf,
    unsigned int n,
    unsigned int dimensions,
    bool normalized)
{
    if (!device || !embeddings_buf || !query_buf || !scores_buf) {
        set_error(nil, "Invalid parameters");
        return -1;
    }
    
    @autoreleasepool {
        MetalContext* ctx = (MetalContext*)device;
        id<MTLBuffer> embeddings = (__bridge id<MTLBuffer>)embeddings_buf;
        id<MTLBuffer> query = (__bridge id<MTLBuffer>)query_buf;
        id<MTLBuffer> scores = (__bridge id<MTLBuffer>)scores_buf;
        
        id<MTLComputePipelineState> pipeline = normalized ? ctx->cosineNormalized : ctx->cosineFull;
        if (!pipeline) {
            set_error(nil, "Pipeline not initialized");
            return -1;
        }
        
        id<MTLCommandBuffer> commandBuffer = [ctx->commandQueue commandBuffer];
        if (!commandBuffer) {
            set_error(nil, "Failed to create command buffer");
            return -1;
        }
        
        id<MTLComputeCommandEncoder> encoder = [commandBuffer computeCommandEncoder];
        if (!encoder) {
            set_error(nil, "Failed to create command encoder");
            return -1;
        }
        
        [encoder setComputePipelineState:pipeline];
        [encoder setBuffer:embeddings offset:0 atIndex:0];
        [encoder setBuffer:query offset:0 atIndex:1];
        [encoder setBuffer:scores offset:0 atIndex:2];
        [encoder setBytes:&n length:sizeof(n) atIndex:3];
        [encoder setBytes:&dimensions length:sizeof(dimensions) atIndex:4];
        
        // Calculate thread groups
        NSUInteger threadGroupSize = MIN(pipeline.maxTotalThreadsPerThreadgroup, 256);
        NSUInteger numGroups = (n + threadGroupSize - 1) / threadGroupSize;
        
        MTLSize gridSize = MTLSizeMake(n, 1, 1);
        MTLSize groupSize = MTLSizeMake(threadGroupSize, 1, 1);
        
        [encoder dispatchThreads:gridSize threadsPerThreadgroup:groupSize];
        [encoder endEncoding];
        
        [commandBuffer commit];
        [commandBuffer waitUntilCompleted];
        
        if (commandBuffer.error) {
            set_error(commandBuffer.error, "Kernel execution failed");
            return -1;
        }
        
        return 0;
    }
}

int metal_compute_topk(
    void* device,
    void* scores_buf,
    void* indices_buf,
    void* topk_scores_buf,
    unsigned int n,
    unsigned int k)
{
    if (!device || !scores_buf || !indices_buf || !topk_scores_buf) {
        set_error(nil, "Invalid parameters");
        return -1;
    }
    
    @autoreleasepool {
        MetalContext* ctx = (MetalContext*)device;
        id<MTLBuffer> scores = (__bridge id<MTLBuffer>)scores_buf;
        id<MTLBuffer> indices = (__bridge id<MTLBuffer>)indices_buf;
        id<MTLBuffer> topkScores = (__bridge id<MTLBuffer>)topk_scores_buf;
        
        id<MTLComputePipelineState> pipeline = ctx->topkSimple;
        if (!pipeline) {
            set_error(nil, "TopK pipeline not initialized");
            return -1;
        }
        
        id<MTLCommandBuffer> commandBuffer = [ctx->commandQueue commandBuffer];
        if (!commandBuffer) {
            set_error(nil, "Failed to create command buffer");
            return -1;
        }
        
        id<MTLComputeCommandEncoder> encoder = [commandBuffer computeCommandEncoder];
        if (!encoder) {
            set_error(nil, "Failed to create command encoder");
            return -1;
        }
        
        [encoder setComputePipelineState:pipeline];
        [encoder setBuffer:scores offset:0 atIndex:0];
        [encoder setBuffer:indices offset:0 atIndex:1];
        [encoder setBuffer:topkScores offset:0 atIndex:2];
        [encoder setBytes:&n length:sizeof(n) atIndex:3];
        [encoder setBytes:&k length:sizeof(k) atIndex:4];
        
        // topk_simple runs on single thread
        MTLSize gridSize = MTLSizeMake(1, 1, 1);
        MTLSize groupSize = MTLSizeMake(1, 1, 1);
        
        [encoder dispatchThreads:gridSize threadsPerThreadgroup:groupSize];
        [encoder endEncoding];
        
        [commandBuffer commit];
        [commandBuffer waitUntilCompleted];
        
        if (commandBuffer.error) {
            set_error(commandBuffer.error, "TopK kernel failed");
            return -1;
        }
        
        return 0;
    }
}

int metal_normalize_vectors(
    void* device,
    void* vectors_buf,
    unsigned int n,
    unsigned int dimensions)
{
    if (!device || !vectors_buf) {
        set_error(nil, "Invalid parameters");
        return -1;
    }
    
    @autoreleasepool {
        MetalContext* ctx = (MetalContext*)device;
        id<MTLBuffer> vectors = (__bridge id<MTLBuffer>)vectors_buf;
        
        id<MTLComputePipelineState> pipeline = ctx->normalize;
        if (!pipeline) {
            set_error(nil, "Normalize pipeline not initialized");
            return -1;
        }
        
        id<MTLCommandBuffer> commandBuffer = [ctx->commandQueue commandBuffer];
        if (!commandBuffer) {
            set_error(nil, "Failed to create command buffer");
            return -1;
        }
        
        id<MTLComputeCommandEncoder> encoder = [commandBuffer computeCommandEncoder];
        if (!encoder) {
            set_error(nil, "Failed to create command encoder");
            return -1;
        }
        
        [encoder setComputePipelineState:pipeline];
        [encoder setBuffer:vectors offset:0 atIndex:0];
        [encoder setBytes:&n length:sizeof(n) atIndex:1];
        [encoder setBytes:&dimensions length:sizeof(dimensions) atIndex:2];
        
        NSUInteger threadGroupSize = MIN(pipeline.maxTotalThreadsPerThreadgroup, 256);
        MTLSize gridSize = MTLSizeMake(n, 1, 1);
        MTLSize groupSize = MTLSizeMake(threadGroupSize, 1, 1);
        
        [encoder dispatchThreads:gridSize threadsPerThreadgroup:groupSize];
        [encoder endEncoding];
        
        [commandBuffer commit];
        [commandBuffer waitUntilCompleted];
        
        if (commandBuffer.error) {
            set_error(commandBuffer.error, "Normalize kernel failed");
            return -1;
        }
        
        return 0;
    }
}

// =============================================================================
// Error Handling
// =============================================================================

const char* metal_last_error(void) {
    return g_error_message;
}

void metal_clear_error(void) {
    g_error_message[0] = '\0';
}
