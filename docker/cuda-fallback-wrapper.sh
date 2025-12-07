#!/bin/sh
# CUDA Fallback Wrapper
# Detects GPU availability and handles libcuda.so.1 missing gracefully

# Create a stub libcuda.so.1 if GPU is not available
if ! nvidia-smi >/dev/null 2>&1; then
    echo "No GPU detected - creating CUDA stub library for graceful fallback"
    
    # Create minimal stub library directory
    mkdir -p /tmp/cuda-stub
    
    # Create comprehensive stub with all commonly required CUDA driver API functions
    cat > /tmp/cuda-stub/stub.c << 'STUBEOF'
// CUDA Driver API stubs - returns error codes to indicate no GPU
typedef int CUresult;
#define CUDA_ERROR_NO_DEVICE 100

// Initialization
CUresult cuInit(unsigned int flags) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuDriverGetVersion(int *version) { if(version) *version = 0; return 0; }

// Device Management
CUresult cuDeviceGetCount(int *count) { if(count) *count = 0; return 0; }
CUresult cuDeviceGet(void *dev, int ordinal) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuDeviceGetName(char *name, int len, void *dev) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuDeviceGetAttribute(int *pi, int attrib, void *dev) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuDeviceTotalMem(unsigned long long *bytes, void *dev) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuDeviceGetUuid(void *uuid, void *dev) { return CUDA_ERROR_NO_DEVICE; }

// Context Management
CUresult cuCtxCreate(void **ctx, unsigned int flags, void *dev) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuCtxDestroy(void *ctx) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuCtxGetCurrent(void **ctx) { if(ctx) *ctx = 0; return 0; }
CUresult cuCtxSetCurrent(void *ctx) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuCtxSynchronize(void) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuCtxPushCurrent(void *ctx) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuCtxPopCurrent(void **ctx) { return CUDA_ERROR_NO_DEVICE; }

// Memory Management
CUresult cuMemAlloc(void **ptr, unsigned long long size) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuMemAllocManaged(void **ptr, unsigned long long size, unsigned int flags) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuMemFree(void *ptr) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuMemcpyHtoD(void *dst, const void *src, unsigned long long size) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuMemcpyDtoH(void *dst, void *src, unsigned long long size) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuMemcpyDtoD(void *dst, void *src, unsigned long long size) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuMemsetD8(void *ptr, unsigned char value, unsigned long long count) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuMemsetD32(void *ptr, unsigned int value, unsigned long long count) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuMemAddressFree(void *ptr, unsigned long long size) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuMemAddressReserve(void **ptr, unsigned long long size, unsigned long long alignment, void *addr, unsigned long long flags) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuMemCreate(void *handle, unsigned long long size, void *prop, unsigned long long flags) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuMemMap(void *ptr, unsigned long long size, unsigned long long offset, void *handle, unsigned long long flags) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuMemUnmap(void *ptr, unsigned long long size) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuMemRelease(void *handle) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuMemGetAllocationGranularity(unsigned long long *granularity, void *prop, int option) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuMemSetAccess(void *ptr, unsigned long long size, void *desc, unsigned long long count) { return CUDA_ERROR_NO_DEVICE; }

// Module/Kernel Management
CUresult cuModuleLoad(void **module, const char *fname) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuModuleLoadData(void **module, const void *image) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuModuleUnload(void *module) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuModuleGetFunction(void **func, void *module, const char *name) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuLaunchKernel(void *f, unsigned int gx, unsigned int gy, unsigned int gz, unsigned int bx, unsigned int by, unsigned int bz, unsigned int sharedMem, void *stream, void **params, void **extra) { return CUDA_ERROR_NO_DEVICE; }

// Stream Management
CUresult cuStreamCreate(void **stream, unsigned int flags) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuStreamDestroy(void *stream) { return CUDA_ERROR_NO_DEVICE; }
CUresult cuStreamSynchronize(void *stream) { return CUDA_ERROR_NO_DEVICE; }

// Error Handling
CUresult cuGetErrorString(CUresult error, const char **str) { if(str) *str = "CUDA not available"; return 0; }
CUresult cuGetErrorName(CUresult error, const char **str) { if(str) *str = "CUDA_ERROR_NO_DEVICE"; return 0; }
STUBEOF
    
    gcc -shared -fPIC -o /tmp/cuda-stub/libcuda.so.1 /tmp/cuda-stub/stub.c 2>/dev/null || {
        echo "Could not create CUDA stub - local embeddings will be disabled"
        export NORNICDB_EMBEDDING_PROVIDER=none
    }
    
    # Add stub to library path (prepend so it's found first)
    export LD_LIBRARY_PATH="/tmp/cuda-stub:$LD_LIBRARY_PATH"
    
    # Disable GPU in environment
    export NORNICDB_EMBEDDING_GPU_LAYERS=0
    export NORNICDB_GPU_ENABLED=false
    echo "CUDA stub created - running in CPU-only mode"
fi

# Execute the real entrypoint
exec /app/entrypoint-real.sh "$@"