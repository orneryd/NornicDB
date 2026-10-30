//go:build opencl && (linux || windows || darwin)
// +build opencl
// +build linux windows darwin

// Package opencl provides cross-platform GPU acceleration using OpenCL.
package opencl

/*
#cgo linux CFLAGS: -I/opt/rocm/include -I/usr/include
#cgo linux LDFLAGS: -L/opt/rocm/lib -L/usr/lib/x86_64-linux-gnu -lOpenCL
#cgo darwin CFLAGS: -framework OpenCL
#cgo darwin LDFLAGS: -framework OpenCL
#cgo windows LDFLAGS: -lOpenCL

#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// Error handling
static char opencl_last_error[256] = {0};

void opencl_set_error(const char* msg) {
    strncpy(opencl_last_error, msg, sizeof(opencl_last_error) - 1);
}

const char* opencl_get_last_error() {
    return opencl_last_error;
}

void opencl_clear_error() {
    opencl_last_error[0] = 0;
}

const char* opencl_error_string(cl_int error) {
    switch (error) {
        case CL_SUCCESS: return "CL_SUCCESS";
        case CL_DEVICE_NOT_FOUND: return "CL_DEVICE_NOT_FOUND";
        case CL_DEVICE_NOT_AVAILABLE: return "CL_DEVICE_NOT_AVAILABLE";
        case CL_COMPILER_NOT_AVAILABLE: return "CL_COMPILER_NOT_AVAILABLE";
        case CL_MEM_OBJECT_ALLOCATION_FAILURE: return "CL_MEM_OBJECT_ALLOCATION_FAILURE";
        case CL_OUT_OF_RESOURCES: return "CL_OUT_OF_RESOURCES";
        case CL_OUT_OF_HOST_MEMORY: return "CL_OUT_OF_HOST_MEMORY";
        case CL_BUILD_PROGRAM_FAILURE: return "CL_BUILD_PROGRAM_FAILURE";
        case CL_INVALID_VALUE: return "CL_INVALID_VALUE";
        case CL_INVALID_DEVICE_TYPE: return "CL_INVALID_DEVICE_TYPE";
        case CL_INVALID_PLATFORM: return "CL_INVALID_PLATFORM";
        case CL_INVALID_DEVICE: return "CL_INVALID_DEVICE";
        case CL_INVALID_CONTEXT: return "CL_INVALID_CONTEXT";
        case CL_INVALID_QUEUE_PROPERTIES: return "CL_INVALID_QUEUE_PROPERTIES";
        case CL_INVALID_COMMAND_QUEUE: return "CL_INVALID_COMMAND_QUEUE";
        case CL_INVALID_HOST_PTR: return "CL_INVALID_HOST_PTR";
        case CL_INVALID_MEM_OBJECT: return "CL_INVALID_MEM_OBJECT";
        case CL_INVALID_BINARY: return "CL_INVALID_BINARY";
        case CL_INVALID_BUILD_OPTIONS: return "CL_INVALID_BUILD_OPTIONS";
        case CL_INVALID_PROGRAM: return "CL_INVALID_PROGRAM";
        case CL_INVALID_PROGRAM_EXECUTABLE: return "CL_INVALID_PROGRAM_EXECUTABLE";
        case CL_INVALID_KERNEL_NAME: return "CL_INVALID_KERNEL_NAME";
        case CL_INVALID_KERNEL_DEFINITION: return "CL_INVALID_KERNEL_DEFINITION";
        case CL_INVALID_KERNEL: return "CL_INVALID_KERNEL";
        case CL_INVALID_ARG_INDEX: return "CL_INVALID_ARG_INDEX";
        case CL_INVALID_ARG_VALUE: return "CL_INVALID_ARG_VALUE";
        case CL_INVALID_ARG_SIZE: return "CL_INVALID_ARG_SIZE";
        case CL_INVALID_KERNEL_ARGS: return "CL_INVALID_KERNEL_ARGS";
        case CL_INVALID_WORK_DIMENSION: return "CL_INVALID_WORK_DIMENSION";
        case CL_INVALID_WORK_GROUP_SIZE: return "CL_INVALID_WORK_GROUP_SIZE";
        case CL_INVALID_WORK_ITEM_SIZE: return "CL_INVALID_WORK_ITEM_SIZE";
        case CL_INVALID_GLOBAL_OFFSET: return "CL_INVALID_GLOBAL_OFFSET";
        default: return "Unknown OpenCL error";
    }
}

// OpenCL kernel source code
static const char* kernel_source =
"__kernel void cosine_similarity_normalized(\n"
"    __global const float* embeddings,\n"
"    __global const float* query,\n"
"    __global float* scores,\n"
"    const unsigned int n,\n"
"    const unsigned int dims\n"
") {\n"
"    unsigned int idx = get_global_id(0);\n"
"    if (idx >= n) return;\n"
"    \n"
"    float dot = 0.0f;\n"
"    __global const float* vec = embeddings + idx * dims;\n"
"    \n"
"    for (unsigned int d = 0; d < dims; d++) {\n"
"        dot += vec[d] * query[d];\n"
"    }\n"
"    \n"
"    scores[idx] = dot;\n"
"}\n"
"\n"
"__kernel void cosine_similarity(\n"
"    __global const float* embeddings,\n"
"    __global const float* query,\n"
"    __global float* scores,\n"
"    const unsigned int n,\n"
"    const unsigned int dims\n"
") {\n"
"    unsigned int idx = get_global_id(0);\n"
"    if (idx >= n) return;\n"
"    \n"
"    float dot = 0.0f;\n"
"    float norm_e = 0.0f;\n"
"    float norm_q = 0.0f;\n"
"    \n"
"    __global const float* vec = embeddings + idx * dims;\n"
"    \n"
"    for (unsigned int d = 0; d < dims; d++) {\n"
"        float e = vec[d];\n"
"        float q = query[d];\n"
"        dot += e * q;\n"
"        norm_e += e * e;\n"
"        norm_q += q * q;\n"
"    }\n"
"    \n"
"    float denom = sqrt(norm_e) * sqrt(norm_q);\n"
"    scores[idx] = (denom > 1e-10f) ? (dot / denom) : 0.0f;\n"
"}\n"
"\n"
"__kernel void compute_norms(\n"
"    __global const float* vectors,\n"
"    __global float* norms,\n"
"    const unsigned int n,\n"
"    const unsigned int dims\n"
") {\n"
"    unsigned int idx = get_global_id(0);\n"
"    if (idx >= n) return;\n"
"    \n"
"    float sum = 0.0f;\n"
"    __global const float* vec = vectors + idx * dims;\n"
"    \n"
"    for (unsigned int d = 0; d < dims; d++) {\n"
"        float val = vec[d];\n"
"        sum += val * val;\n"
"    }\n"
"    \n"
"    norms[idx] = sqrt(sum);\n"
"}\n"
"\n"
"__kernel void normalize_vectors(\n"
"    __global float* vectors,\n"
"    __global const float* norms,\n"
"    const unsigned int n,\n"
"    const unsigned int dims\n"
") {\n"
"    unsigned int vec_idx = get_global_id(0);\n"
"    if (vec_idx >= n) return;\n"
"    \n"
"    float norm = norms[vec_idx];\n"
"    if (norm < 1e-10f) norm = 1.0f;\n"
"    \n"
"    __global float* vec = vectors + vec_idx * dims;\n"
"    for (unsigned int d = 0; d < dims; d++) {\n"
"        vec[d] /= norm;\n"
"    }\n"
"}\n";

// Device structure
typedef struct {
    cl_platform_id platform;
    cl_device_id device;
    cl_context context;
    cl_command_queue queue;
    cl_program program;
    cl_kernel kernel_cosine_normalized;
    cl_kernel kernel_cosine;
    cl_kernel kernel_norms;
    cl_kernel kernel_normalize;
    int device_id;
} OpenCLDevice;

// Get number of GPU devices across all platforms
int opencl_get_device_count() {
    cl_uint num_platforms;
    cl_int err = clGetPlatformIDs(0, NULL, &num_platforms);
    if (err != CL_SUCCESS || num_platforms == 0) {
        return 0;
    }

    cl_platform_id* platforms = (cl_platform_id*)malloc(num_platforms * sizeof(cl_platform_id));
    clGetPlatformIDs(num_platforms, platforms, NULL);

    int total_devices = 0;
    for (cl_uint i = 0; i < num_platforms; i++) {
        cl_uint num_devices;
        err = clGetDeviceIDs(platforms[i], CL_DEVICE_TYPE_GPU, 0, NULL, &num_devices);
        if (err == CL_SUCCESS) {
            total_devices += num_devices;
        }
    }

    free(platforms);
    return total_devices;
}

int opencl_is_available() {
    return opencl_get_device_count() > 0 ? 1 : 0;
}

// Get Nth GPU device across all platforms
int opencl_get_device_by_index(int index, cl_platform_id* out_platform, cl_device_id* out_device) {
    cl_uint num_platforms;
    cl_int err = clGetPlatformIDs(0, NULL, &num_platforms);
    if (err != CL_SUCCESS || num_platforms == 0) {
        return -1;
    }

    cl_platform_id* platforms = (cl_platform_id*)malloc(num_platforms * sizeof(cl_platform_id));
    clGetPlatformIDs(num_platforms, platforms, NULL);

    int current_index = 0;
    for (cl_uint i = 0; i < num_platforms; i++) {
        cl_uint num_devices;
        err = clGetDeviceIDs(platforms[i], CL_DEVICE_TYPE_GPU, 0, NULL, &num_devices);
        if (err != CL_SUCCESS) continue;

        if (index < current_index + (int)num_devices) {
            cl_device_id* devices = (cl_device_id*)malloc(num_devices * sizeof(cl_device_id));
            clGetDeviceIDs(platforms[i], CL_DEVICE_TYPE_GPU, num_devices, devices, NULL);
            *out_platform = platforms[i];
            *out_device = devices[index - current_index];
            free(devices);
            free(platforms);
            return 0;
        }
        current_index += num_devices;
    }

    free(platforms);
    return -1;
}

OpenCLDevice* opencl_create_device(int device_id) {
    OpenCLDevice* dev = (OpenCLDevice*)malloc(sizeof(OpenCLDevice));
    if (!dev) {
        opencl_set_error("Failed to allocate device struct");
        return NULL;
    }
    memset(dev, 0, sizeof(OpenCLDevice));
    dev->device_id = device_id;

    // Get device
    if (opencl_get_device_by_index(device_id, &dev->platform, &dev->device) != 0) {
        opencl_set_error("Device not found");
        free(dev);
        return NULL;
    }

    cl_int err;

    // Create context
    dev->context = clCreateContext(NULL, 1, &dev->device, NULL, NULL, &err);
    if (err != CL_SUCCESS) {
        char msg[256];
        snprintf(msg, sizeof(msg), "Failed to create context: %s", opencl_error_string(err));
        opencl_set_error(msg);
        free(dev);
        return NULL;
    }

    // Create command queue
    dev->queue = clCreateCommandQueue(dev->context, dev->device, 0, &err);
    if (err != CL_SUCCESS) {
        char msg[256];
        snprintf(msg, sizeof(msg), "Failed to create command queue: %s", opencl_error_string(err));
        opencl_set_error(msg);
        clReleaseContext(dev->context);
        free(dev);
        return NULL;
    }

    // Create program from source
    size_t source_len = strlen(kernel_source);
    dev->program = clCreateProgramWithSource(dev->context, 1, &kernel_source, &source_len, &err);
    if (err != CL_SUCCESS) {
        char msg[256];
        snprintf(msg, sizeof(msg), "Failed to create program: %s", opencl_error_string(err));
        opencl_set_error(msg);
        clReleaseCommandQueue(dev->queue);
        clReleaseContext(dev->context);
        free(dev);
        return NULL;
    }

    // Build program
    err = clBuildProgram(dev->program, 1, &dev->device, "-cl-fast-relaxed-math", NULL, NULL);
    if (err != CL_SUCCESS) {
        // Get build log
        size_t log_size;
        clGetProgramBuildInfo(dev->program, dev->device, CL_PROGRAM_BUILD_LOG, 0, NULL, &log_size);
        char* log = (char*)malloc(log_size + 1);
        clGetProgramBuildInfo(dev->program, dev->device, CL_PROGRAM_BUILD_LOG, log_size, log, NULL);
        log[log_size] = '\0';

        char msg[256];
        snprintf(msg, sizeof(msg), "Failed to build program: %s", log);
        opencl_set_error(msg);

        free(log);
        clReleaseProgram(dev->program);
        clReleaseCommandQueue(dev->queue);
        clReleaseContext(dev->context);
        free(dev);
        return NULL;
    }

    // Create kernels
    dev->kernel_cosine_normalized = clCreateKernel(dev->program, "cosine_similarity_normalized", &err);
    if (err != CL_SUCCESS) {
        opencl_set_error("Failed to create kernel: cosine_similarity_normalized");
        clReleaseProgram(dev->program);
        clReleaseCommandQueue(dev->queue);
        clReleaseContext(dev->context);
        free(dev);
        return NULL;
    }

    dev->kernel_cosine = clCreateKernel(dev->program, "cosine_similarity", &err);
    if (err != CL_SUCCESS) {
        opencl_set_error("Failed to create kernel: cosine_similarity");
        clReleaseKernel(dev->kernel_cosine_normalized);
        clReleaseProgram(dev->program);
        clReleaseCommandQueue(dev->queue);
        clReleaseContext(dev->context);
        free(dev);
        return NULL;
    }

    dev->kernel_norms = clCreateKernel(dev->program, "compute_norms", &err);
    if (err != CL_SUCCESS) {
        opencl_set_error("Failed to create kernel: compute_norms");
        clReleaseKernel(dev->kernel_cosine);
        clReleaseKernel(dev->kernel_cosine_normalized);
        clReleaseProgram(dev->program);
        clReleaseCommandQueue(dev->queue);
        clReleaseContext(dev->context);
        free(dev);
        return NULL;
    }

    dev->kernel_normalize = clCreateKernel(dev->program, "normalize_vectors", &err);
    if (err != CL_SUCCESS) {
        opencl_set_error("Failed to create kernel: normalize_vectors");
        clReleaseKernel(dev->kernel_norms);
        clReleaseKernel(dev->kernel_cosine);
        clReleaseKernel(dev->kernel_cosine_normalized);
        clReleaseProgram(dev->program);
        clReleaseCommandQueue(dev->queue);
        clReleaseContext(dev->context);
        free(dev);
        return NULL;
    }

    return dev;
}

void opencl_release_device(OpenCLDevice* dev) {
    if (dev) {
        if (dev->kernel_normalize) clReleaseKernel(dev->kernel_normalize);
        if (dev->kernel_norms) clReleaseKernel(dev->kernel_norms);
        if (dev->kernel_cosine) clReleaseKernel(dev->kernel_cosine);
        if (dev->kernel_cosine_normalized) clReleaseKernel(dev->kernel_cosine_normalized);
        if (dev->program) clReleaseProgram(dev->program);
        if (dev->queue) clReleaseCommandQueue(dev->queue);
        if (dev->context) clReleaseContext(dev->context);
        free(dev);
    }
}

const char* opencl_device_name(OpenCLDevice* dev) {
    static char name[256];
    cl_int err = clGetDeviceInfo(dev->device, CL_DEVICE_NAME, sizeof(name), name, NULL);
    if (err != CL_SUCCESS) {
        return "Unknown";
    }
    return name;
}

const char* opencl_device_vendor(OpenCLDevice* dev) {
    static char vendor[256];
    cl_int err = clGetDeviceInfo(dev->device, CL_DEVICE_VENDOR, sizeof(vendor), vendor, NULL);
    if (err != CL_SUCCESS) {
        return "Unknown";
    }
    return vendor;
}

size_t opencl_device_memory(OpenCLDevice* dev) {
    cl_ulong mem_size;
    cl_int err = clGetDeviceInfo(dev->device, CL_DEVICE_GLOBAL_MEM_SIZE, sizeof(mem_size), &mem_size, NULL);
    if (err != CL_SUCCESS) {
        return 0;
    }
    return (size_t)mem_size;
}

size_t opencl_max_work_group_size(OpenCLDevice* dev) {
    size_t max_size;
    cl_int err = clGetDeviceInfo(dev->device, CL_DEVICE_MAX_WORK_GROUP_SIZE, sizeof(max_size), &max_size, NULL);
    if (err != CL_SUCCESS) {
        return 256; // Default fallback
    }
    return max_size;
}

// Buffer management
typedef struct {
    cl_mem mem;
    size_t size;
    OpenCLDevice* device;
} OpenCLBuffer;

OpenCLBuffer* opencl_create_buffer(OpenCLDevice* dev, float* host_data, size_t count) {
    OpenCLBuffer* buf = (OpenCLBuffer*)malloc(sizeof(OpenCLBuffer));
    if (!buf) {
        opencl_set_error("Failed to allocate buffer struct");
        return NULL;
    }

    buf->size = count * sizeof(float);
    buf->device = dev;

    cl_int err;
    cl_mem_flags flags = CL_MEM_READ_WRITE;
    if (host_data) {
        flags |= CL_MEM_COPY_HOST_PTR;
    }

    buf->mem = clCreateBuffer(dev->context, flags, buf->size, host_data, &err);
    if (err != CL_SUCCESS) {
        char msg[256];
        snprintf(msg, sizeof(msg), "Failed to create buffer: %s", opencl_error_string(err));
        opencl_set_error(msg);
        free(buf);
        return NULL;
    }

    return buf;
}

void opencl_release_buffer(OpenCLBuffer* buf) {
    if (buf) {
        if (buf->mem) clReleaseMemObject(buf->mem);
        free(buf);
    }
}

size_t opencl_buffer_size(OpenCLBuffer* buf) {
    return buf ? buf->size : 0;
}

int opencl_buffer_copy_to_host(OpenCLBuffer* buf, float* host_data, size_t count) {
    if (!buf || !host_data) return -1;

    size_t copy_size = count * sizeof(float);
    if (copy_size > buf->size) copy_size = buf->size;

    cl_int err = clEnqueueReadBuffer(buf->device->queue, buf->mem, CL_TRUE, 0, copy_size, host_data, 0, NULL, NULL);
    if (err != CL_SUCCESS) {
        char msg[256];
        snprintf(msg, sizeof(msg), "Failed to read buffer: %s", opencl_error_string(err));
        opencl_set_error(msg);
        return -1;
    }
    return 0;
}

// Vector operations

int opencl_normalize_vectors(OpenCLDevice* dev, OpenCLBuffer* vectors, unsigned int n, unsigned int dims) {
    cl_int err;

    // Create norms buffer
    OpenCLBuffer* norms = opencl_create_buffer(dev, NULL, n);
    if (!norms) return -1;

    // Compute norms
    err = clSetKernelArg(dev->kernel_norms, 0, sizeof(cl_mem), &vectors->mem);
    err |= clSetKernelArg(dev->kernel_norms, 1, sizeof(cl_mem), &norms->mem);
    err |= clSetKernelArg(dev->kernel_norms, 2, sizeof(unsigned int), &n);
    err |= clSetKernelArg(dev->kernel_norms, 3, sizeof(unsigned int), &dims);
    if (err != CL_SUCCESS) {
        opencl_release_buffer(norms);
        return -1;
    }

    size_t global_size = n;
    err = clEnqueueNDRangeKernel(dev->queue, dev->kernel_norms, 1, NULL, &global_size, NULL, 0, NULL, NULL);
    if (err != CL_SUCCESS) {
        opencl_release_buffer(norms);
        return -1;
    }

    // Normalize vectors
    err = clSetKernelArg(dev->kernel_normalize, 0, sizeof(cl_mem), &vectors->mem);
    err |= clSetKernelArg(dev->kernel_normalize, 1, sizeof(cl_mem), &norms->mem);
    err |= clSetKernelArg(dev->kernel_normalize, 2, sizeof(unsigned int), &n);
    err |= clSetKernelArg(dev->kernel_normalize, 3, sizeof(unsigned int), &dims);
    if (err != CL_SUCCESS) {
        opencl_release_buffer(norms);
        return -1;
    }

    err = clEnqueueNDRangeKernel(dev->queue, dev->kernel_normalize, 1, NULL, &global_size, NULL, 0, NULL, NULL);
    if (err != CL_SUCCESS) {
        opencl_release_buffer(norms);
        return -1;
    }

    clFinish(dev->queue);
    opencl_release_buffer(norms);
    return 0;
}

int opencl_cosine_similarity(OpenCLDevice* dev, OpenCLBuffer* embeddings, OpenCLBuffer* query,
                              OpenCLBuffer* scores, unsigned int n, unsigned int dims, int normalized) {
    cl_int err;
    cl_kernel kernel = normalized ? dev->kernel_cosine_normalized : dev->kernel_cosine;

    err = clSetKernelArg(kernel, 0, sizeof(cl_mem), &embeddings->mem);
    err |= clSetKernelArg(kernel, 1, sizeof(cl_mem), &query->mem);
    err |= clSetKernelArg(kernel, 2, sizeof(cl_mem), &scores->mem);
    err |= clSetKernelArg(kernel, 3, sizeof(unsigned int), &n);
    err |= clSetKernelArg(kernel, 4, sizeof(unsigned int), &dims);
    if (err != CL_SUCCESS) {
        char msg[256];
        snprintf(msg, sizeof(msg), "Failed to set kernel args: %s", opencl_error_string(err));
        opencl_set_error(msg);
        return -1;
    }

    size_t global_size = n;
    err = clEnqueueNDRangeKernel(dev->queue, kernel, 1, NULL, &global_size, NULL, 0, NULL, NULL);
    if (err != CL_SUCCESS) {
        char msg[256];
        snprintf(msg, sizeof(msg), "Failed to enqueue kernel: %s", opencl_error_string(err));
        opencl_set_error(msg);
        return -1;
    }

    clFinish(dev->queue);
    return 0;
}

int opencl_topk(OpenCLDevice* dev, OpenCLBuffer* scores, unsigned int* out_indices,
                float* out_scores, unsigned int n, unsigned int k) {
    // Copy scores to host (CPU top-k for simplicity - can be optimized with GPU radix sort)
    float* host_scores = (float*)malloc(n * sizeof(float));
    if (opencl_buffer_copy_to_host(scores, host_scores, n) != 0) {
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
	ErrOpenCLNotAvailable = errors.New("opencl: OpenCL is not available on this system")
	ErrDeviceCreation     = errors.New("opencl: failed to create OpenCL device")
	ErrBufferCreation     = errors.New("opencl: failed to create buffer")
	ErrKernelExecution    = errors.New("opencl: kernel execution failed")
	ErrInvalidBuffer      = errors.New("opencl: invalid buffer")
)

// Device represents an OpenCL GPU device.
type Device struct {
	ptr    *C.OpenCLDevice
	id     int
	name   string
	vendor string
	memory uint64
	mu     sync.Mutex
}

// Buffer represents an OpenCL memory buffer.
type Buffer struct {
	ptr    *C.OpenCLBuffer
	size   uint64
	device *Device
}

// SearchResult holds a similarity search result.
type SearchResult struct {
	Index uint32
	Score float32
}

// IsAvailable checks if OpenCL is available on this system.
func IsAvailable() bool {
	return C.opencl_is_available() != 0
}

// DeviceCount returns the number of OpenCL GPU devices.
func DeviceCount() int {
	count := C.opencl_get_device_count()
	if count < 0 {
		return 0
	}
	return int(count)
}

// NewDevice creates a new OpenCL device handle.
func NewDevice(deviceID int) (*Device, error) {
	if !IsAvailable() {
		return nil, ErrOpenCLNotAvailable
	}

	ptr := C.opencl_create_device(C.int(deviceID))
	if ptr == nil {
		errMsg := C.GoString(C.opencl_get_last_error())
		C.opencl_clear_error()
		return nil, fmt.Errorf("%w: %s", ErrDeviceCreation, errMsg)
	}

	return &Device{
		ptr:    ptr,
		id:     deviceID,
		name:   C.GoString(C.opencl_device_name(ptr)),
		vendor: C.GoString(C.opencl_device_vendor(ptr)),
		memory: uint64(C.opencl_device_memory(ptr)),
	}, nil
}

// Release frees the OpenCL device resources.
func (d *Device) Release() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.ptr != nil {
		C.opencl_release_device(d.ptr)
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

// Vendor returns the GPU vendor name.
func (d *Device) Vendor() string {
	return d.vendor
}

// MemoryBytes returns the GPU memory size in bytes.
func (d *Device) MemoryBytes() uint64 {
	return d.memory
}

// MemoryMB returns the GPU memory size in megabytes.
func (d *Device) MemoryMB() int {
	return int(d.memory / (1024 * 1024))
}

// NewBuffer creates a new GPU buffer with data.
func (d *Device) NewBuffer(data []float32) (*Buffer, error) {
	if len(data) == 0 {
		return nil, errors.New("opencl: cannot create empty buffer")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	ptr := C.opencl_create_buffer(
		d.ptr,
		(*C.float)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	)

	if ptr == nil {
		errMsg := C.GoString(C.opencl_get_last_error())
		C.opencl_clear_error()
		return nil, fmt.Errorf("%w: %s", ErrBufferCreation, errMsg)
	}

	return &Buffer{
		ptr:    ptr,
		size:   uint64(len(data) * 4),
		device: d,
	}, nil
}

// NewEmptyBuffer creates an uninitialized GPU buffer.
func (d *Device) NewEmptyBuffer(count uint64) (*Buffer, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	ptr := C.opencl_create_buffer(
		d.ptr,
		nil,
		C.size_t(count),
	)

	if ptr == nil {
		errMsg := C.GoString(C.opencl_get_last_error())
		C.opencl_clear_error()
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
		C.opencl_release_buffer(b.ptr)
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
	ret := C.opencl_buffer_copy_to_host(b.ptr, (*C.float)(unsafe.Pointer(&result[0])), C.size_t(count))
	if ret != 0 {
		return nil
	}
	return result
}

// NormalizeVectors normalizes vectors in-place to unit length.
func (d *Device) NormalizeVectors(vectors *Buffer, n, dimensions uint32) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	ret := C.opencl_normalize_vectors(d.ptr, vectors.ptr, C.uint(n), C.uint(dimensions))
	if ret != 0 {
		errMsg := C.GoString(C.opencl_get_last_error())
		C.opencl_clear_error()
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

	ret := C.opencl_cosine_similarity(d.ptr, embeddings.ptr, query.ptr, scores.ptr,
		C.uint(n), C.uint(dimensions), C.int(normalizedInt))
	if ret != 0 {
		errMsg := C.GoString(C.opencl_get_last_error())
		C.opencl_clear_error()
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

	ret := C.opencl_topk(d.ptr, scores.ptr,
		(*C.uint)(unsafe.Pointer(&indices[0])),
		(*C.float)(unsafe.Pointer(&topkScores[0])),
		C.uint(n), C.uint(k))
	if ret != 0 {
		errMsg := C.GoString(C.opencl_get_last_error())
		C.opencl_clear_error()
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
	queryBuf, err := d.NewBuffer(query)
	if err != nil {
		return nil, err
	}
	defer queryBuf.Release()

	// Create scores buffer
	scoresBuf, err := d.NewEmptyBuffer(uint64(n))
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
