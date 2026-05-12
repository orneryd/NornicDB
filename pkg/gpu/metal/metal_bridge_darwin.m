// metal_bridge.m - Objective-C Metal API wrapper for CGO
//
// This file implements the C functions declared in metal_bridge.go
// that interface with Apple's Metal GPU compute API.
//
// Features:
//   - Metal GPU compute kernels for vector operations
//   - Metal Performance Shaders (MPS) for optimized matrix operations
//   - Memory tracking for unified memory management
//   - Device capability detection and logging

#import <Metal/Metal.h>
#import <MetalPerformanceShaders/MetalPerformanceShaders.h>
#import <Foundation/Foundation.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <mach/mach.h>
#include <sys/sysctl.h>

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
            NSString* shaderSource =
                @"#include <metal_stdlib>\n"
                @"using namespace metal;\n"
                @"\n"
                @"kernel void cosine_similarity_normalized(\n"
                @"    device const float* embeddings [[buffer(0)]],\n"
                @"    device const float* query [[buffer(1)]],\n"
                @"    device float* scores [[buffer(2)]],\n"
                @"    constant uint& n [[buffer(3)]],\n"
                @"    constant uint& dimensions [[buffer(4)]],\n"
                @"    uint gid [[thread_position_in_grid]])\n"
                @"{\n"
                @"    if (gid >= n) return;\n"
                @"    \n"
                @"    float dot = 0.0f;\n"
                @"    uint base = gid * dimensions;\n"
                @"    \n"
                @"    for (uint i = 0; i < dimensions; i++) {\n"
                @"        dot += embeddings[base + i] * query[i];\n"
                @"    }\n"
                @"    \n"
                @"    scores[gid] = dot;\n"
                @"}\n"
                @"\n"
                @"kernel void cosine_similarity_full(\n"
                @"    device const float* embeddings [[buffer(0)]],\n"
                @"    device const float* query [[buffer(1)]],\n"
                @"    device float* scores [[buffer(2)]],\n"
                @"    constant uint& n [[buffer(3)]],\n"
                @"    constant uint& dimensions [[buffer(4)]],\n"
                @"    uint gid [[thread_position_in_grid]])\n"
                @"{\n"
                @"    if (gid >= n) return;\n"
                @"    \n"
                @"    float dot = 0.0f;\n"
                @"    float normA = 0.0f;\n"
                @"    float normB = 0.0f;\n"
                @"    \n"
                @"    uint base = gid * dimensions;\n"
                @"    \n"
                @"    for (uint i = 0; i < dimensions; i++) {\n"
                @"        float a = embeddings[base + i];\n"
                @"        float b = query[i];\n"
                @"        dot += a * b;\n"
                @"        normA += a * a;\n"
                @"        normB += b * b;\n"
                @"    }\n"
                @"    \n"
                @"    if (normA == 0.0f || normB == 0.0f) {\n"
                @"        scores[gid] = 0.0f;\n"
                @"        return;\n"
                @"    }\n"
                @"    \n"
                @"    scores[gid] = dot / (sqrt(normA) * sqrt(normB));\n"
                @"}\n"
                @"\n"
                @"kernel void topk_simple(\n"
                @"    device const float* scores [[buffer(0)]],\n"
                @"    device uint* topk_indices [[buffer(1)]],\n"
                @"    device float* topk_scores [[buffer(2)]],\n"
                @"    constant uint& n [[buffer(3)]],\n"
                @"    constant uint& k [[buffer(4)]],\n"
                @"    uint gid [[thread_position_in_grid]])\n"
                @"{\n"
                @"    if (gid != 0) return;\n"
                @"    \n"
                @"    for (uint i = 0; i < k; i++) {\n"
                @"        topk_scores[i] = -2.0f;\n"
                @"        topk_indices[i] = UINT_MAX;\n"
                @"    }\n"
                @"    \n"
                @"    for (uint i = 0; i < n; i++) {\n"
                @"        float score = scores[i];\n"
                @"        \n"
                @"        if (score > topk_scores[k-1]) {\n"
                @"            uint pos = k - 1;\n"
                @"            while (pos > 0 && score > topk_scores[pos-1]) {\n"
                @"                topk_scores[pos] = topk_scores[pos-1];\n"
                @"                topk_indices[pos] = topk_indices[pos-1];\n"
                @"                pos--;\n"
                @"            }\n"
                @"            topk_scores[pos] = score;\n"
                @"            topk_indices[pos] = i;\n"
                @"        }\n"
                @"    }\n"
                @"}\n"
                @"\n"
                @"kernel void normalize_vectors(\n"
                @"    device float* vectors [[buffer(0)]],\n"
                @"    constant uint& n [[buffer(1)]],\n"
                @"    constant uint& dimensions [[buffer(2)]],\n"
                @"    uint gid [[thread_position_in_grid]])\n"
                @"{\n"
                @"    if (gid >= n) return;\n"
                @"    \n"
                @"    uint base = gid * dimensions;\n"
                @"    \n"
                @"    float sum_sq = 0.0f;\n"
                @"    for (uint i = 0; i < dimensions; i++) {\n"
                @"        float v = vectors[base + i];\n"
                @"        sum_sq += v * v;\n"
                @"    }\n"
                @"    \n"
                @"    if (sum_sq == 0.0f) return;\n"
                @"    \n"
                @"    float inv_norm = rsqrt(sum_sq);\n"
                @"    \n"
                @"    for (uint i = 0; i < dimensions; i++) {\n"
                @"        vectors[base + i] *= inv_norm;\n"
                @"    }\n"
                @"}\n";
            
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

// =============================================================================
// Memory Tracking & Device Info
// =============================================================================

typedef struct {
    unsigned long total_memory;        // Total unified memory (Apple Silicon)
    unsigned long used_memory;         // Currently used memory
    unsigned long available_memory;    // Available memory
    unsigned long gpu_recommended;     // Recommended max working set for GPU
    unsigned long current_allocated;   // Currently allocated GPU buffers
} MetalMemoryInfo;

// Get system memory info using Mach APIs
void metal_get_memory_info(void* device, MetalMemoryInfo* info) {
    if (!info) return;
    memset(info, 0, sizeof(MetalMemoryInfo));
    
    @autoreleasepool {
        // Get total physical memory
        int64_t total_mem;
        size_t size = sizeof(total_mem);
        if (sysctlbyname("hw.memsize", &total_mem, &size, NULL, 0) == 0) {
            info->total_memory = (unsigned long)total_mem;
        }
        
        // Get current memory usage from Mach
        mach_port_t host_port = mach_host_self();
        vm_size_t page_size;
        host_page_size(host_port, &page_size);
        
        vm_statistics64_data_t vm_stat;
        mach_msg_type_number_t count = HOST_VM_INFO64_COUNT;
        
        if (host_statistics64(host_port, HOST_VM_INFO64, (host_info64_t)&vm_stat, &count) == KERN_SUCCESS) {
            info->used_memory = (unsigned long)(vm_stat.active_count + vm_stat.wire_count) * page_size;
            info->available_memory = (unsigned long)(vm_stat.free_count + vm_stat.inactive_count) * page_size;
        }
        
        // Get GPU recommended working set
        if (device) {
            MetalContext* ctx = (MetalContext*)device;
            if ([ctx->device respondsToSelector:@selector(recommendedMaxWorkingSetSize)]) {
                info->gpu_recommended = (unsigned long)[ctx->device recommendedMaxWorkingSetSize];
            }
            if ([ctx->device respondsToSelector:@selector(currentAllocatedSize)]) {
                info->current_allocated = (unsigned long)[ctx->device currentAllocatedSize];
            }
        }
    }
}

// Get detailed device capabilities
typedef struct {
    char name[256];
    char architecture[64];
    int gpu_family;
    int max_threads_per_threadgroup;
    unsigned long max_buffer_length;  // Use unsigned long for 64-bit value
    bool supports_raytracing;
    bool supports_32bit_float_filtering;
    bool supports_32bit_msaa;
    bool is_low_power;
    bool is_headless;
    bool is_removable;
    bool has_unified_memory;
    int registry_id;
} MetalDeviceCapabilities;

void metal_get_device_capabilities(void* device, MetalDeviceCapabilities* caps) {
    if (!caps) return;
    memset(caps, 0, sizeof(MetalDeviceCapabilities));
    
    @autoreleasepool {
        MetalContext* ctx = (MetalContext*)device;
        if (!ctx) {
            // Get default device for capabilities check
            id<MTLDevice> dev = MTLCreateSystemDefaultDevice();
            if (!dev) return;
            
            strncpy(caps->name, [[dev name] UTF8String], sizeof(caps->name) - 1);
            caps->max_threads_per_threadgroup = (int)[dev maxThreadsPerThreadgroup].width;
            caps->max_buffer_length = (unsigned long)[dev maxBufferLength];
            caps->is_low_power = [dev isLowPower];
            caps->is_headless = [dev isHeadless];
            caps->has_unified_memory = [dev hasUnifiedMemory];
            caps->registry_id = (int)[dev registryID];
            
            // Detect Apple Silicon architecture
            if ([dev hasUnifiedMemory]) {
                // Check for specific GPU families (Apple Silicon)
                if (@available(macOS 13.0, *)) {
                    if ([dev supportsFamily:MTLGPUFamilyApple9]) {
                        strncpy(caps->architecture, "Apple M3/M3 Pro/M3 Max", sizeof(caps->architecture) - 1);
                        caps->gpu_family = 9;
                    } else if ([dev supportsFamily:MTLGPUFamilyApple8]) {
                        strncpy(caps->architecture, "Apple M2/M2 Pro/M2 Max/M2 Ultra", sizeof(caps->architecture) - 1);
                        caps->gpu_family = 8;
                    } else if ([dev supportsFamily:MTLGPUFamilyApple7]) {
                        strncpy(caps->architecture, "Apple M1/M1 Pro/M1 Max/M1 Ultra", sizeof(caps->architecture) - 1);
                        caps->gpu_family = 7;
                    }
                }
                if (caps->gpu_family == 0) {
                    strncpy(caps->architecture, "Apple Silicon", sizeof(caps->architecture) - 1);
                }
            } else {
                strncpy(caps->architecture, "Intel/Discrete GPU", sizeof(caps->architecture) - 1);
            }
            
            return;
        }
        
        strncpy(caps->name, [[ctx->device name] UTF8String], sizeof(caps->name) - 1);
        caps->max_threads_per_threadgroup = (int)[ctx->device maxThreadsPerThreadgroup].width;
        caps->max_buffer_length = (unsigned long)[ctx->device maxBufferLength];
        caps->is_low_power = [ctx->device isLowPower];
        caps->is_headless = [ctx->device isHeadless];
        caps->has_unified_memory = [ctx->device hasUnifiedMemory];
        caps->registry_id = (int)[ctx->device registryID];
        
        // Detect Apple Silicon
        if ([ctx->device hasUnifiedMemory]) {
            if (@available(macOS 13.0, *)) {
                if ([ctx->device supportsFamily:MTLGPUFamilyApple9]) {
                    strncpy(caps->architecture, "Apple M3/M3 Pro/M3 Max", sizeof(caps->architecture) - 1);
                    caps->gpu_family = 9;
                } else if ([ctx->device supportsFamily:MTLGPUFamilyApple8]) {
                    strncpy(caps->architecture, "Apple M2/M2 Pro/M2 Max/M2 Ultra", sizeof(caps->architecture) - 1);
                    caps->gpu_family = 8;
                } else if ([ctx->device supportsFamily:MTLGPUFamilyApple7]) {
                    strncpy(caps->architecture, "Apple M1/M1 Pro/M1 Max/M1 Ultra", sizeof(caps->architecture) - 1);
                    caps->gpu_family = 7;
                }
            }
            if (caps->gpu_family == 0) {
                strncpy(caps->architecture, "Apple Silicon", sizeof(caps->architecture) - 1);
            }
        } else {
            strncpy(caps->architecture, "Intel/Discrete GPU", sizeof(caps->architecture) - 1);
        }
    }
}

// =============================================================================
// Metal Performance Shaders (MPS) for Matrix Operations
// =============================================================================

// Matrix multiplication using MPS (optimized for Apple Silicon)
// Computes: C = alpha * A * B + beta * C
int metal_mps_matrix_multiply(
    void* device,
    void* a_buf,
    void* b_buf,
    void* c_buf,
    unsigned int m,      // rows of A and C
    unsigned int n,      // cols of B and C
    unsigned int k,      // cols of A, rows of B
    float alpha,
    float beta)
{
    if (!device || !a_buf || !b_buf || !c_buf) {
        set_error(nil, "Invalid parameters for MPS matrix multiply");
        return -1;
    }
    
    @autoreleasepool {
        MetalContext* ctx = (MetalContext*)device;
        id<MTLBuffer> bufA = (__bridge id<MTLBuffer>)a_buf;
        id<MTLBuffer> bufB = (__bridge id<MTLBuffer>)b_buf;
        id<MTLBuffer> bufC = (__bridge id<MTLBuffer>)c_buf;
        
        // Create MPS matrix descriptors
        MPSMatrixDescriptor* descA = [MPSMatrixDescriptor matrixDescriptorWithRows:m
                                                                           columns:k
                                                                          rowBytes:k * sizeof(float)
                                                                          dataType:MPSDataTypeFloat32];
        MPSMatrixDescriptor* descB = [MPSMatrixDescriptor matrixDescriptorWithRows:k
                                                                           columns:n
                                                                          rowBytes:n * sizeof(float)
                                                                          dataType:MPSDataTypeFloat32];
        MPSMatrixDescriptor* descC = [MPSMatrixDescriptor matrixDescriptorWithRows:m
                                                                           columns:n
                                                                          rowBytes:n * sizeof(float)
                                                                          dataType:MPSDataTypeFloat32];
        
        // Create MPS matrices
        MPSMatrix* matA = [[MPSMatrix alloc] initWithBuffer:bufA descriptor:descA];
        MPSMatrix* matB = [[MPSMatrix alloc] initWithBuffer:bufB descriptor:descB];
        MPSMatrix* matC = [[MPSMatrix alloc] initWithBuffer:bufC descriptor:descC];
        
        // Create matrix multiplication kernel
        MPSMatrixMultiplication* matMul = [[MPSMatrixMultiplication alloc] initWithDevice:ctx->device
                                                                            transposeLeft:NO
                                                                           transposeRight:NO
                                                                               resultRows:m
                                                                            resultColumns:n
                                                                          interiorColumns:k
                                                                                    alpha:alpha
                                                                                     beta:beta];
        
        // Execute
        id<MTLCommandBuffer> cmdBuf = [ctx->commandQueue commandBuffer];
        [matMul encodeToCommandBuffer:cmdBuf leftMatrix:matA rightMatrix:matB resultMatrix:matC];
        [cmdBuf commit];
        [cmdBuf waitUntilCompleted];
        
        if (cmdBuf.error) {
            set_error(cmdBuf.error, "MPS matrix multiplication failed");
            return -1;
        }
        
        return 0;
    }
}

// Matrix-vector multiplication using MPS
// Computes: y = alpha * A * x + beta * y
int metal_mps_matrix_vector_multiply(
    void* device,
    void* a_buf,      // Matrix A (m x n)
    void* x_buf,      // Vector x (n x 1)
    void* y_buf,      // Vector y (m x 1)
    unsigned int m,   // rows
    unsigned int n,   // cols
    float alpha,
    float beta)
{
    if (!device || !a_buf || !x_buf || !y_buf) {
        set_error(nil, "Invalid parameters for MPS matrix-vector multiply");
        return -1;
    }
    
    @autoreleasepool {
        MetalContext* ctx = (MetalContext*)device;
        id<MTLBuffer> bufA = (__bridge id<MTLBuffer>)a_buf;
        id<MTLBuffer> bufX = (__bridge id<MTLBuffer>)x_buf;
        id<MTLBuffer> bufY = (__bridge id<MTLBuffer>)y_buf;
        
        // Create MPS matrix/vector descriptors
        MPSMatrixDescriptor* descA = [MPSMatrixDescriptor matrixDescriptorWithRows:m
                                                                           columns:n
                                                                          rowBytes:n * sizeof(float)
                                                                          dataType:MPSDataTypeFloat32];
        MPSVectorDescriptor* descX = [MPSVectorDescriptor vectorDescriptorWithLength:n
                                                                            dataType:MPSDataTypeFloat32];
        MPSVectorDescriptor* descY = [MPSVectorDescriptor vectorDescriptorWithLength:m
                                                                            dataType:MPSDataTypeFloat32];
        
        MPSMatrix* matA = [[MPSMatrix alloc] initWithBuffer:bufA descriptor:descA];
        MPSVector* vecX = [[MPSVector alloc] initWithBuffer:bufX descriptor:descX];
        MPSVector* vecY = [[MPSVector alloc] initWithBuffer:bufY descriptor:descY];
        
        // Create matrix-vector multiplication kernel
        MPSMatrixVectorMultiplication* matVecMul = [[MPSMatrixVectorMultiplication alloc] initWithDevice:ctx->device
                                                                                              transpose:NO
                                                                                                   rows:m
                                                                                                columns:n
                                                                                                  alpha:alpha
                                                                                                   beta:beta];
        
        id<MTLCommandBuffer> cmdBuf = [ctx->commandQueue commandBuffer];
        [matVecMul encodeToCommandBuffer:cmdBuf inputMatrix:matA inputVector:vecX resultVector:vecY];
        [cmdBuf commit];
        [cmdBuf waitUntilCompleted];
        
        if (cmdBuf.error) {
            set_error(cmdBuf.error, "MPS matrix-vector multiplication failed");
            return -1;
        }
        
        return 0;
    }
}

// Batch cosine similarity using MPS (computes embeddings * query^T)
// More efficient than custom kernel for large batches
int metal_mps_batch_cosine_similarity(
    void* device,
    void* embeddings_buf,  // n x dims matrix
    void* query_buf,       // 1 x dims vector
    void* scores_buf,      // n x 1 output
    unsigned int n,
    unsigned int dims)
{
    if (!device || !embeddings_buf || !query_buf || !scores_buf) {
        set_error(nil, "Invalid parameters for MPS cosine similarity");
        return -1;
    }
    
    @autoreleasepool {
        MetalContext* ctx = (MetalContext*)device;
        id<MTLBuffer> embeddings = (__bridge id<MTLBuffer>)embeddings_buf;
        id<MTLBuffer> query = (__bridge id<MTLBuffer>)query_buf;
        id<MTLBuffer> scores = (__bridge id<MTLBuffer>)scores_buf;
        
        // embeddings (n x dims) * query^T (dims x 1) = scores (n x 1)
        MPSMatrixDescriptor* descE = [MPSMatrixDescriptor matrixDescriptorWithRows:n
                                                                           columns:dims
                                                                          rowBytes:dims * sizeof(float)
                                                                          dataType:MPSDataTypeFloat32];
        MPSVectorDescriptor* descQ = [MPSVectorDescriptor vectorDescriptorWithLength:dims
                                                                            dataType:MPSDataTypeFloat32];
        MPSVectorDescriptor* descS = [MPSVectorDescriptor vectorDescriptorWithLength:n
                                                                            dataType:MPSDataTypeFloat32];
        
        MPSMatrix* matE = [[MPSMatrix alloc] initWithBuffer:embeddings descriptor:descE];
        MPSVector* vecQ = [[MPSVector alloc] initWithBuffer:query descriptor:descQ];
        MPSVector* vecS = [[MPSVector alloc] initWithBuffer:scores descriptor:descS];
        
        // Matrix-vector multiplication gives us dot products
        MPSMatrixVectorMultiplication* dotProd = [[MPSMatrixVectorMultiplication alloc] initWithDevice:ctx->device
                                                                                            transpose:NO
                                                                                                 rows:n
                                                                                              columns:dims
                                                                                                alpha:1.0
                                                                                                 beta:0.0];
        
        id<MTLCommandBuffer> cmdBuf = [ctx->commandQueue commandBuffer];
        [dotProd encodeToCommandBuffer:cmdBuf inputMatrix:matE inputVector:vecQ resultVector:vecS];
        [cmdBuf commit];
        [cmdBuf waitUntilCompleted];
        
        if (cmdBuf.error) {
            set_error(cmdBuf.error, "MPS batch cosine similarity failed");
            return -1;
        }
        
        return 0;
    }
}

// Check if MPS is supported
bool metal_mps_is_supported(void) {
    @autoreleasepool {
        id<MTLDevice> device = MTLCreateSystemDefaultDevice();
        if (!device) return false;
        
        // MPS is supported on all Metal-capable devices on macOS 10.13+
        return true;
    }
}
