//go:build cgovulkan && (linux || windows || darwin)

// Package vulkan provides cross-platform GPU acceleration using Vulkan Compute.
//
// Build Requirements:
//
//	Set CGO_CFLAGS and CGO_LDFLAGS environment variables before building:
//
//	macOS with MoltenVK:
//	  export CGO_CFLAGS="-I/path/to/vulkan-sdk/include"
//	  export CGO_LDFLAGS="-L/path/to/vulkan-sdk/lib -lvulkan"
//
//	Linux:
//	  export CGO_CFLAGS="-I$VULKAN_SDK/include"
//	  export CGO_LDFLAGS="-L$VULKAN_SDK/lib -lvulkan"
package vulkan

/*
#cgo linux LDFLAGS: -lvulkan
#cgo darwin LDFLAGS: -lvulkan
#cgo windows LDFLAGS: -lvulkan-1

#include <vulkan/vulkan.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

// Error handling
static char vulkan_last_error[512] = {0};

void vulkan_set_error(const char* msg) {
    strncpy(vulkan_last_error, msg, sizeof(vulkan_last_error) - 1);
}

const char* vulkan_get_last_error() {
    return vulkan_last_error;
}

void vulkan_clear_error() {
    vulkan_last_error[0] = 0;
}

const char* vulkan_result_string(VkResult result) {
    switch (result) {
        case VK_SUCCESS: return "VK_SUCCESS";
        case VK_NOT_READY: return "VK_NOT_READY";
        case VK_TIMEOUT: return "VK_TIMEOUT";
        case VK_ERROR_OUT_OF_HOST_MEMORY: return "VK_ERROR_OUT_OF_HOST_MEMORY";
        case VK_ERROR_OUT_OF_DEVICE_MEMORY: return "VK_ERROR_OUT_OF_DEVICE_MEMORY";
        case VK_ERROR_INITIALIZATION_FAILED: return "VK_ERROR_INITIALIZATION_FAILED";
        case VK_ERROR_DEVICE_LOST: return "VK_ERROR_DEVICE_LOST";
        case VK_ERROR_MEMORY_MAP_FAILED: return "VK_ERROR_MEMORY_MAP_FAILED";
        case VK_ERROR_LAYER_NOT_PRESENT: return "VK_ERROR_LAYER_NOT_PRESENT";
        case VK_ERROR_EXTENSION_NOT_PRESENT: return "VK_ERROR_EXTENSION_NOT_PRESENT";
        case VK_ERROR_FEATURE_NOT_PRESENT: return "VK_ERROR_FEATURE_NOT_PRESENT";
        case VK_ERROR_INCOMPATIBLE_DRIVER: return "VK_ERROR_INCOMPATIBLE_DRIVER";
        case VK_ERROR_TOO_MANY_OBJECTS: return "VK_ERROR_TOO_MANY_OBJECTS";
        case VK_ERROR_FORMAT_NOT_SUPPORTED: return "VK_ERROR_FORMAT_NOT_SUPPORTED";
        default: return "Unknown Vulkan error";
    }
}

// SPIR-V shader code for cosine similarity (pre-compiled)
// This is the compiled SPIR-V bytecode for the compute shader
// Equivalent GLSL:
//
// #version 450
// layout(local_size_x = 256) in;
//
// layout(set = 0, binding = 0) readonly buffer Embeddings { float embeddings[]; };
// layout(set = 0, binding = 1) readonly buffer Query { float query[]; };
// layout(set = 0, binding = 2) writeonly buffer Scores { float scores[]; };
//
// layout(push_constant) uniform PushConstants {
//     uint n;
//     uint dims;
//     uint normalized;
// } pc;
//
// void main() {
//     uint idx = gl_GlobalInvocationID.x;
//     if (idx >= pc.n) return;
//
//     float dot = 0.0;
//     float norm_e = 0.0;
//     float norm_q = 0.0;
//
//     uint base = idx * pc.dims;
//     for (uint d = 0; d < pc.dims; d++) {
//         float e = embeddings[base + d];
//         float q = query[d];
//         dot += e * q;
//         if (pc.normalized == 0) {
//             norm_e += e * e;
//             norm_q += q * q;
//         }
//     }
//
//     if (pc.normalized != 0) {
//         scores[idx] = dot;
//     } else {
//         float denom = sqrt(norm_e) * sqrt(norm_q);
//         scores[idx] = denom > 1e-10 ? dot / denom : 0.0;
//     }
// }

// Pre-compiled SPIR-V for cosine similarity shader
// Generated from: pkg/gpu/vulkan/shaders/cosine_similarity.comp
// Compile with: glslc -fshader-stage=compute -O cosine_similarity.comp -o cosine_similarity.spv
// Total size: 2380 bytes (595 uint32_t words)
static const uint32_t cosine_similarity_spirv[] = {
    0x07230203, 0x00010000, 0x000d000b, 0x00000099, 0x00000000, 0x00020011, 0x00000001, 0x0006000b,
    0x00000001, 0x4c534c47, 0x6474732e, 0x3035342e, 0x00000000, 0x0003000e, 0x00000000, 0x00000001,
    0x0006000f, 0x00000005, 0x00000004, 0x6e69616d, 0x00000000, 0x0000000b, 0x00060010, 0x00000004,
    0x00000011, 0x00000100, 0x00000001, 0x00000001, 0x00040047, 0x0000000b, 0x0000000b, 0x0000001c,
    0x00030047, 0x00000015, 0x00000002, 0x00050048, 0x00000015, 0x00000000, 0x00000023, 0x00000000,
    0x00050048, 0x00000015, 0x00000001, 0x00000023, 0x00000004, 0x00050048, 0x00000015, 0x00000002,
    0x00000023, 0x00000008, 0x00040047, 0x00000039, 0x00000006, 0x00000004, 0x00030047, 0x0000003a,
    0x00000003, 0x00040048, 0x0000003a, 0x00000000, 0x00000018, 0x00050048, 0x0000003a, 0x00000000,
    0x00000023, 0x00000000, 0x00030047, 0x0000003c, 0x00000018, 0x00040047, 0x0000003c, 0x00000021,
    0x00000000, 0x00040047, 0x0000003c, 0x00000022, 0x00000000, 0x00040047, 0x00000044, 0x00000006,
    0x00000004, 0x00030047, 0x00000045, 0x00000003, 0x00040048, 0x00000045, 0x00000000, 0x00000018,
    0x00050048, 0x00000045, 0x00000000, 0x00000023, 0x00000000, 0x00030047, 0x00000047, 0x00000018,
    0x00040047, 0x00000047, 0x00000021, 0x00000001, 0x00040047, 0x00000047, 0x00000022, 0x00000000,
    0x00040047, 0x0000007b, 0x00000006, 0x00000004, 0x00030047, 0x0000007c, 0x00000003, 0x00040048,
    0x0000007c, 0x00000000, 0x00000019, 0x00050048, 0x0000007c, 0x00000000, 0x00000023, 0x00000000,
    0x00030047, 0x0000007e, 0x00000019, 0x00040047, 0x0000007e, 0x00000021, 0x00000002, 0x00040047,
    0x0000007e, 0x00000022, 0x00000000, 0x00040047, 0x00000084, 0x0000000b, 0x00000019, 0x00020013,
    0x00000002, 0x00030021, 0x00000003, 0x00000002, 0x00040015, 0x00000006, 0x00000020, 0x00000000,
    0x00040017, 0x00000009, 0x00000006, 0x00000003, 0x00040020, 0x0000000a, 0x00000001, 0x00000009,
    0x0004003b, 0x0000000a, 0x0000000b, 0x00000001, 0x0004002b, 0x00000006, 0x0000000c, 0x00000000,
    0x00040020, 0x0000000d, 0x00000001, 0x00000006, 0x0005001e, 0x00000015, 0x00000006, 0x00000006,
    0x00000006, 0x00040020, 0x00000016, 0x00000009, 0x00000015, 0x0004003b, 0x00000016, 0x00000017,
    0x00000009, 0x00040015, 0x00000018, 0x00000020, 0x00000001, 0x0004002b, 0x00000018, 0x00000019,
    0x00000000, 0x00040020, 0x0000001a, 0x00000009, 0x00000006, 0x00020014, 0x0000001d, 0x00030016,
    0x00000022, 0x00000020, 0x0004002b, 0x00000022, 0x00000025, 0x00000000, 0x0004002b, 0x00000018,
    0x0000002a, 0x00000001, 0x0003001d, 0x00000039, 0x00000022, 0x0003001e, 0x0000003a, 0x00000039,
    0x00040020, 0x0000003b, 0x00000002, 0x0000003a, 0x0004003b, 0x0000003b, 0x0000003c, 0x00000002,
    0x00040020, 0x00000040, 0x00000002, 0x00000022, 0x0003001d, 0x00000044, 0x00000022, 0x0003001e,
    0x00000045, 0x00000044, 0x00040020, 0x00000046, 0x00000002, 0x00000045, 0x0004003b, 0x00000046,
    0x00000047, 0x00000002, 0x0004002b, 0x00000018, 0x00000050, 0x00000002, 0x0004002b, 0x00000022,
    0x00000071, 0x2edbe6ff, 0x0003001d, 0x0000007b, 0x00000022, 0x0003001e, 0x0000007c, 0x0000007b,
    0x00040020, 0x0000007d, 0x00000002, 0x0000007c, 0x0004003b, 0x0000007d, 0x0000007e, 0x00000002,
    0x0004002b, 0x00000006, 0x00000082, 0x00000100, 0x0004002b, 0x00000006, 0x00000083, 0x00000001,
    0x0006002c, 0x00000009, 0x00000084, 0x00000082, 0x00000083, 0x00000083, 0x00050036, 0x00000002,
    0x00000004, 0x00000000, 0x00000003, 0x000200f8, 0x00000005, 0x000300f7, 0x0000008a, 0x00000000,
    0x000300fb, 0x0000000c, 0x0000008b, 0x000200f8, 0x0000008b, 0x00050041, 0x0000000d, 0x0000000e,
    0x0000000b, 0x0000000c, 0x0004003d, 0x00000006, 0x0000000f, 0x0000000e, 0x00050041, 0x0000001a,
    0x0000001b, 0x00000017, 0x00000019, 0x0004003d, 0x00000006, 0x0000001c, 0x0000001b, 0x000500ae,
    0x0000001d, 0x0000001e, 0x0000000f, 0x0000001c, 0x000300f7, 0x00000020, 0x00000000, 0x000400fa,
    0x0000001e, 0x0000001f, 0x00000020, 0x000200f8, 0x0000001f, 0x000200f9, 0x0000008a, 0x000200f8,
    0x00000020, 0x00050041, 0x0000001a, 0x0000002b, 0x00000017, 0x0000002a, 0x0004003d, 0x00000006,
    0x0000002c, 0x0000002b, 0x00050084, 0x00000006, 0x0000002d, 0x0000000f, 0x0000002c, 0x000200f9,
    0x0000002f, 0x000200f8, 0x0000002f, 0x000700f5, 0x00000022, 0x00000093, 0x00000025, 0x00000020,
    0x0000004f, 0x00000032, 0x000700f5, 0x00000022, 0x00000092, 0x00000025, 0x00000020, 0x00000098,
    0x00000032, 0x000700f5, 0x00000022, 0x00000091, 0x00000025, 0x00000020, 0x00000097, 0x00000032,
    0x000700f5, 0x00000006, 0x00000090, 0x0000000c, 0x00000020, 0x00000061, 0x00000032, 0x000500b0,
    0x0000001d, 0x00000037, 0x00000090, 0x0000002c, 0x000400f6, 0x00000031, 0x00000032, 0x00000000,
    0x000400fa, 0x00000037, 0x00000030, 0x00000031, 0x000200f8, 0x00000030, 0x00050080, 0x00000006,
    0x0000003f, 0x0000002d, 0x00000090, 0x00060041, 0x00000040, 0x00000041, 0x0000003c, 0x00000019,
    0x0000003f, 0x0004003d, 0x00000022, 0x00000042, 0x00000041, 0x00060041, 0x00000040, 0x00000049,
    0x00000047, 0x00000019, 0x00000090, 0x0004003d, 0x00000022, 0x0000004a, 0x00000049, 0x00050085,
    0x00000022, 0x0000004d, 0x00000042, 0x0000004a, 0x00050081, 0x00000022, 0x0000004f, 0x00000093,
    0x0000004d, 0x00050041, 0x0000001a, 0x00000051, 0x00000017, 0x00000050, 0x0004003d, 0x00000006,
    0x00000052, 0x00000051, 0x000500aa, 0x0000001d, 0x00000053, 0x00000052, 0x0000000c, 0x000300f7,
    0x00000055, 0x00000000, 0x000400fa, 0x00000053, 0x00000054, 0x00000055, 0x000200f8, 0x00000054,
    0x00050085, 0x00000022, 0x00000058, 0x00000042, 0x00000042, 0x00050081, 0x00000022, 0x0000005a,
    0x00000091, 0x00000058, 0x00050085, 0x00000022, 0x0000005d, 0x0000004a, 0x0000004a, 0x00050081,
    0x00000022, 0x0000005f, 0x00000092, 0x0000005d, 0x000200f9, 0x00000055, 0x000200f8, 0x00000055,
    0x000700f5, 0x00000022, 0x00000098, 0x00000092, 0x00000030, 0x0000005f, 0x00000054, 0x000700f5,
    0x00000022, 0x00000097, 0x00000091, 0x00000030, 0x0000005a, 0x00000054, 0x000200f9, 0x00000032,
    0x000200f8, 0x00000032, 0x00050080, 0x00000006, 0x00000061, 0x00000090, 0x0000002a, 0x000200f9,
    0x0000002f, 0x000200f8, 0x00000031, 0x00050041, 0x0000001a, 0x00000062, 0x00000017, 0x00000050,
    0x0004003d, 0x00000006, 0x00000063, 0x00000062, 0x000500ab, 0x0000001d, 0x00000064, 0x00000063,
    0x0000000c, 0x000300f7, 0x00000066, 0x00000000, 0x000400fa, 0x00000064, 0x00000065, 0x00000069,
    0x000200f8, 0x00000065, 0x000200f9, 0x00000066, 0x000200f8, 0x00000069, 0x0006000c, 0x00000022,
    0x0000006c, 0x00000001, 0x0000001f, 0x00000091, 0x0006000c, 0x00000022, 0x0000006e, 0x00000001,
    0x0000001f, 0x00000092, 0x00050085, 0x00000022, 0x0000006f, 0x0000006c, 0x0000006e, 0x000500ba,
    0x0000001d, 0x00000072, 0x0000006f, 0x00000071, 0x000300f7, 0x00000075, 0x00000000, 0x000400fa,
    0x00000072, 0x00000074, 0x00000079, 0x000200f8, 0x00000074, 0x00050088, 0x00000022, 0x00000078,
    0x00000093, 0x0000006f, 0x000200f9, 0x00000075, 0x000200f8, 0x00000079, 0x000200f9, 0x00000075,
    0x000200f8, 0x00000075, 0x000700f5, 0x00000022, 0x00000094, 0x00000078, 0x00000074, 0x00000025,
    0x00000079, 0x000200f9, 0x00000066, 0x000200f8, 0x00000066, 0x000700f5, 0x00000022, 0x00000095,
    0x00000093, 0x00000065, 0x00000094, 0x00000075, 0x00060041, 0x00000040, 0x00000081, 0x0000007e,
    0x00000019, 0x0000000f, 0x0003003e, 0x00000081, 0x00000095, 0x000200f9, 0x0000008a, 0x000200f8,
    0x0000008a, 0x000100fd, 0x00010038,
};

static const size_t cosine_similarity_spirv_size = sizeof(cosine_similarity_spirv);

// Device structure
typedef struct {
    VkInstance instance;
    VkPhysicalDevice physical_device;
    VkDevice device;
    VkQueue compute_queue;
    uint32_t compute_queue_family;
    VkCommandPool command_pool;
    VkDescriptorPool descriptor_pool;
    VkPipelineLayout pipeline_layout;
    VkPipeline cosine_pipeline;
    VkPipeline topk_pipeline;
    VkPipeline normalize_pipeline;
    VkDescriptorSetLayout descriptor_set_layout;
    int device_id;
    char device_name[256];
    uint64_t device_memory;
} VulkanDevice;

// Check if Vulkan is available
int vulkan_is_available() {
    VkInstance instance;
    VkApplicationInfo app_info = {
        .sType = VK_STRUCTURE_TYPE_APPLICATION_INFO,
        .pApplicationName = "NornicDB",
        .applicationVersion = VK_MAKE_VERSION(1, 0, 0),
        .pEngineName = "NornicDB GPU",
        .engineVersion = VK_MAKE_VERSION(1, 0, 0),
        .apiVersion = VK_API_VERSION_1_1
    };

    VkInstanceCreateInfo create_info = {
        .sType = VK_STRUCTURE_TYPE_INSTANCE_CREATE_INFO,
        .pApplicationInfo = &app_info
    };

    VkResult result = vkCreateInstance(&create_info, NULL, &instance);
    if (result != VK_SUCCESS) {
        return 0;
    }

    uint32_t device_count = 0;
    vkEnumeratePhysicalDevices(instance, &device_count, NULL);

    vkDestroyInstance(instance, NULL);
    return device_count > 0 ? 1 : 0;
}

// Get device count
int vulkan_get_device_count() {
    VkInstance instance;
    VkApplicationInfo app_info = {
        .sType = VK_STRUCTURE_TYPE_APPLICATION_INFO,
        .pApplicationName = "NornicDB",
        .applicationVersion = VK_MAKE_VERSION(1, 0, 0),
        .apiVersion = VK_API_VERSION_1_1
    };

    VkInstanceCreateInfo create_info = {
        .sType = VK_STRUCTURE_TYPE_INSTANCE_CREATE_INFO,
        .pApplicationInfo = &app_info
    };

    if (vkCreateInstance(&create_info, NULL, &instance) != VK_SUCCESS) {
        return 0;
    }

    uint32_t device_count = 0;
    vkEnumeratePhysicalDevices(instance, &device_count, NULL);

    vkDestroyInstance(instance, NULL);
    return (int)device_count;
}

// Find compute queue family
int vulkan_find_compute_queue_family(VkPhysicalDevice physical_device) {
    uint32_t queue_family_count = 0;
    vkGetPhysicalDeviceQueueFamilyProperties(physical_device, &queue_family_count, NULL);

    VkQueueFamilyProperties* queue_families = malloc(queue_family_count * sizeof(VkQueueFamilyProperties));
    vkGetPhysicalDeviceQueueFamilyProperties(physical_device, &queue_family_count, queue_families);

    int compute_family = -1;
    for (uint32_t i = 0; i < queue_family_count; i++) {
        if (queue_families[i].queueFlags & VK_QUEUE_COMPUTE_BIT) {
            compute_family = i;
            break;
        }
    }

    free(queue_families);
    return compute_family;
}

// Create Vulkan device
VulkanDevice* vulkan_create_device(int device_id) {
    VulkanDevice* dev = (VulkanDevice*)calloc(1, sizeof(VulkanDevice));
    if (!dev) {
        vulkan_set_error("Failed to allocate device struct");
        return NULL;
    }
    dev->device_id = device_id;

    // Create instance
    VkApplicationInfo app_info = {
        .sType = VK_STRUCTURE_TYPE_APPLICATION_INFO,
        .pApplicationName = "NornicDB",
        .applicationVersion = VK_MAKE_VERSION(1, 0, 0),
        .pEngineName = "NornicDB GPU",
        .engineVersion = VK_MAKE_VERSION(1, 0, 0),
        .apiVersion = VK_API_VERSION_1_1
    };

    VkInstanceCreateInfo instance_info = {
        .sType = VK_STRUCTURE_TYPE_INSTANCE_CREATE_INFO,
        .pApplicationInfo = &app_info
    };

    VkResult result = vkCreateInstance(&instance_info, NULL, &dev->instance);
    if (result != VK_SUCCESS) {
        char msg[256];
        snprintf(msg, sizeof(msg), "Failed to create Vulkan instance: %s", vulkan_result_string(result));
        vulkan_set_error(msg);
        free(dev);
        return NULL;
    }

    // Enumerate physical devices
    uint32_t device_count = 0;
    vkEnumeratePhysicalDevices(dev->instance, &device_count, NULL);
    if (device_count == 0 || device_id >= (int)device_count) {
        vulkan_set_error("No suitable GPU found or invalid device ID");
        vkDestroyInstance(dev->instance, NULL);
        free(dev);
        return NULL;
    }

    VkPhysicalDevice* physical_devices = malloc(device_count * sizeof(VkPhysicalDevice));
    vkEnumeratePhysicalDevices(dev->instance, &device_count, physical_devices);
    dev->physical_device = physical_devices[device_id];
    free(physical_devices);

    // Get device properties
    VkPhysicalDeviceProperties properties;
    vkGetPhysicalDeviceProperties(dev->physical_device, &properties);
    strncpy(dev->device_name, properties.deviceName, sizeof(dev->device_name) - 1);

    // Get device memory
    VkPhysicalDeviceMemoryProperties mem_properties;
    vkGetPhysicalDeviceMemoryProperties(dev->physical_device, &mem_properties);
    for (uint32_t i = 0; i < mem_properties.memoryHeapCount; i++) {
        if (mem_properties.memoryHeaps[i].flags & VK_MEMORY_HEAP_DEVICE_LOCAL_BIT) {
            dev->device_memory = mem_properties.memoryHeaps[i].size;
            break;
        }
    }

    // Find compute queue family
    dev->compute_queue_family = vulkan_find_compute_queue_family(dev->physical_device);
    if (dev->compute_queue_family < 0) {
        vulkan_set_error("No compute queue family found");
        vkDestroyInstance(dev->instance, NULL);
        free(dev);
        return NULL;
    }

    // Create logical device
    float queue_priority = 1.0f;
    VkDeviceQueueCreateInfo queue_info = {
        .sType = VK_STRUCTURE_TYPE_DEVICE_QUEUE_CREATE_INFO,
        .queueFamilyIndex = dev->compute_queue_family,
        .queueCount = 1,
        .pQueuePriorities = &queue_priority
    };

    VkPhysicalDeviceFeatures device_features = {0};

    VkDeviceCreateInfo device_info = {
        .sType = VK_STRUCTURE_TYPE_DEVICE_CREATE_INFO,
        .queueCreateInfoCount = 1,
        .pQueueCreateInfos = &queue_info,
        .pEnabledFeatures = &device_features
    };

    result = vkCreateDevice(dev->physical_device, &device_info, NULL, &dev->device);
    if (result != VK_SUCCESS) {
        char msg[256];
        snprintf(msg, sizeof(msg), "Failed to create logical device: %s", vulkan_result_string(result));
        vulkan_set_error(msg);
        vkDestroyInstance(dev->instance, NULL);
        free(dev);
        return NULL;
    }

    // Get compute queue
    vkGetDeviceQueue(dev->device, dev->compute_queue_family, 0, &dev->compute_queue);

    // Create command pool
    VkCommandPoolCreateInfo pool_info = {
        .sType = VK_STRUCTURE_TYPE_COMMAND_POOL_CREATE_INFO,
        .queueFamilyIndex = dev->compute_queue_family,
        .flags = VK_COMMAND_POOL_CREATE_RESET_COMMAND_BUFFER_BIT
    };

    result = vkCreateCommandPool(dev->device, &pool_info, NULL, &dev->command_pool);
    if (result != VK_SUCCESS) {
        vulkan_set_error("Failed to create command pool");
        vkDestroyDevice(dev->device, NULL);
        vkDestroyInstance(dev->instance, NULL);
        free(dev);
        return NULL;
    }

    // Create descriptor pool
    VkDescriptorPoolSize pool_sizes[] = {
        { VK_DESCRIPTOR_TYPE_STORAGE_BUFFER, 100 }
    };

    VkDescriptorPoolCreateInfo desc_pool_info = {
        .sType = VK_STRUCTURE_TYPE_DESCRIPTOR_POOL_CREATE_INFO,
        .maxSets = 100,
        .poolSizeCount = 1,
        .pPoolSizes = pool_sizes
    };

    result = vkCreateDescriptorPool(dev->device, &desc_pool_info, NULL, &dev->descriptor_pool);
    if (result != VK_SUCCESS) {
        vulkan_set_error("Failed to create descriptor pool");
        vkDestroyCommandPool(dev->device, dev->command_pool, NULL);
        vkDestroyDevice(dev->device, NULL);
        vkDestroyInstance(dev->instance, NULL);
        free(dev);
        return NULL;
    }

    // Create descriptor set layout
    VkDescriptorSetLayoutBinding bindings[] = {
        { .binding = 0, .descriptorType = VK_DESCRIPTOR_TYPE_STORAGE_BUFFER, .descriptorCount = 1, .stageFlags = VK_SHADER_STAGE_COMPUTE_BIT },
        { .binding = 1, .descriptorType = VK_DESCRIPTOR_TYPE_STORAGE_BUFFER, .descriptorCount = 1, .stageFlags = VK_SHADER_STAGE_COMPUTE_BIT },
        { .binding = 2, .descriptorType = VK_DESCRIPTOR_TYPE_STORAGE_BUFFER, .descriptorCount = 1, .stageFlags = VK_SHADER_STAGE_COMPUTE_BIT }
    };

    VkDescriptorSetLayoutCreateInfo layout_info = {
        .sType = VK_STRUCTURE_TYPE_DESCRIPTOR_SET_LAYOUT_CREATE_INFO,
        .bindingCount = 3,
        .pBindings = bindings
    };

    result = vkCreateDescriptorSetLayout(dev->device, &layout_info, NULL, &dev->descriptor_set_layout);
    if (result != VK_SUCCESS) {
        vulkan_set_error("Failed to create descriptor set layout");
        vkDestroyDescriptorPool(dev->device, dev->descriptor_pool, NULL);
        vkDestroyCommandPool(dev->device, dev->command_pool, NULL);
        vkDestroyDevice(dev->device, NULL);
        vkDestroyInstance(dev->instance, NULL);
        free(dev);
        return NULL;
    }

    // Create pipeline layout with push constants
    VkPushConstantRange push_constant_range = {
        .stageFlags = VK_SHADER_STAGE_COMPUTE_BIT,
        .offset = 0,
        .size = 12 // 3 uint32: n, dims, normalized
    };

    VkPipelineLayoutCreateInfo pipeline_layout_info = {
        .sType = VK_STRUCTURE_TYPE_PIPELINE_LAYOUT_CREATE_INFO,
        .setLayoutCount = 1,
        .pSetLayouts = &dev->descriptor_set_layout,
        .pushConstantRangeCount = 1,
        .pPushConstantRanges = &push_constant_range
    };

    result = vkCreatePipelineLayout(dev->device, &pipeline_layout_info, NULL, &dev->pipeline_layout);
    if (result != VK_SUCCESS) {
        vulkan_set_error("Failed to create pipeline layout");
        vkDestroyDescriptorSetLayout(dev->device, dev->descriptor_set_layout, NULL);
        vkDestroyDescriptorPool(dev->device, dev->descriptor_pool, NULL);
        vkDestroyCommandPool(dev->device, dev->command_pool, NULL);
        vkDestroyDevice(dev->device, NULL);
        vkDestroyInstance(dev->instance, NULL);
        free(dev);
        return NULL;
    }

    // Note: Compute pipelines would be created here with actual SPIR-V shaders
    // For now, we'll create them lazily or use fallback CPU paths

    return dev;
}

void vulkan_release_device(VulkanDevice* dev) {
    if (!dev) return;

    if (dev->cosine_pipeline) vkDestroyPipeline(dev->device, dev->cosine_pipeline, NULL);
    if (dev->topk_pipeline) vkDestroyPipeline(dev->device, dev->topk_pipeline, NULL);
    if (dev->normalize_pipeline) vkDestroyPipeline(dev->device, dev->normalize_pipeline, NULL);
    if (dev->pipeline_layout) vkDestroyPipelineLayout(dev->device, dev->pipeline_layout, NULL);
    if (dev->descriptor_set_layout) vkDestroyDescriptorSetLayout(dev->device, dev->descriptor_set_layout, NULL);
    if (dev->descriptor_pool) vkDestroyDescriptorPool(dev->device, dev->descriptor_pool, NULL);
    if (dev->command_pool) vkDestroyCommandPool(dev->device, dev->command_pool, NULL);
    if (dev->device) vkDestroyDevice(dev->device, NULL);
    if (dev->instance) vkDestroyInstance(dev->instance, NULL);

    free(dev);
}

const char* vulkan_device_name(VulkanDevice* dev) {
    return dev ? dev->device_name : "Unknown";
}

uint64_t vulkan_device_memory(VulkanDevice* dev) {
    return dev ? dev->device_memory : 0;
}

// Buffer structure
typedef struct {
    VkBuffer buffer;
    VkDeviceMemory memory;
    VkDeviceSize size;
    VulkanDevice* device;
    void* mapped;
} VulkanBuffer;

// Find suitable memory type
uint32_t vulkan_find_memory_type(VulkanDevice* dev, uint32_t type_filter, VkMemoryPropertyFlags properties) {
    VkPhysicalDeviceMemoryProperties mem_properties;
    vkGetPhysicalDeviceMemoryProperties(dev->physical_device, &mem_properties);

    for (uint32_t i = 0; i < mem_properties.memoryTypeCount; i++) {
        if ((type_filter & (1 << i)) &&
            (mem_properties.memoryTypes[i].propertyFlags & properties) == properties) {
            return i;
        }
    }
    return UINT32_MAX;
}

VulkanBuffer* vulkan_create_buffer(VulkanDevice* dev, float* host_data, size_t count) {
    VulkanBuffer* buf = (VulkanBuffer*)calloc(1, sizeof(VulkanBuffer));
    if (!buf) {
        vulkan_set_error("Failed to allocate buffer struct");
        return NULL;
    }

    buf->size = count * sizeof(float);
    buf->device = dev;

    // Create buffer
    VkBufferCreateInfo buffer_info = {
        .sType = VK_STRUCTURE_TYPE_BUFFER_CREATE_INFO,
        .size = buf->size,
        .usage = VK_BUFFER_USAGE_STORAGE_BUFFER_BIT | VK_BUFFER_USAGE_TRANSFER_DST_BIT | VK_BUFFER_USAGE_TRANSFER_SRC_BIT,
        .sharingMode = VK_SHARING_MODE_EXCLUSIVE
    };

    VkResult result = vkCreateBuffer(dev->device, &buffer_info, NULL, &buf->buffer);
    if (result != VK_SUCCESS) {
        char msg[256];
        snprintf(msg, sizeof(msg), "Failed to create buffer: %s", vulkan_result_string(result));
        vulkan_set_error(msg);
        free(buf);
        return NULL;
    }

    // Get memory requirements
    VkMemoryRequirements mem_requirements;
    vkGetBufferMemoryRequirements(dev->device, buf->buffer, &mem_requirements);

    // Allocate memory (host visible for easy access)
    VkMemoryPropertyFlags properties = VK_MEMORY_PROPERTY_HOST_VISIBLE_BIT | VK_MEMORY_PROPERTY_HOST_COHERENT_BIT;
    uint32_t memory_type = vulkan_find_memory_type(dev, mem_requirements.memoryTypeBits, properties);
    if (memory_type == UINT32_MAX) {
        vulkan_set_error("Failed to find suitable memory type");
        vkDestroyBuffer(dev->device, buf->buffer, NULL);
        free(buf);
        return NULL;
    }

    VkMemoryAllocateInfo alloc_info = {
        .sType = VK_STRUCTURE_TYPE_MEMORY_ALLOCATE_INFO,
        .allocationSize = mem_requirements.size,
        .memoryTypeIndex = memory_type
    };

    result = vkAllocateMemory(dev->device, &alloc_info, NULL, &buf->memory);
    if (result != VK_SUCCESS) {
        vulkan_set_error("Failed to allocate buffer memory");
        vkDestroyBuffer(dev->device, buf->buffer, NULL);
        free(buf);
        return NULL;
    }

    vkBindBufferMemory(dev->device, buf->buffer, buf->memory, 0);

    // Map and copy data if provided
    if (host_data) {
        void* data;
        vkMapMemory(dev->device, buf->memory, 0, buf->size, 0, &data);
        memcpy(data, host_data, buf->size);
        vkUnmapMemory(dev->device, buf->memory);
    }

    return buf;
}

void vulkan_release_buffer(VulkanBuffer* buf) {
    if (!buf) return;

    if (buf->mapped) {
        vkUnmapMemory(buf->device->device, buf->memory);
    }
    if (buf->buffer) vkDestroyBuffer(buf->device->device, buf->buffer, NULL);
    if (buf->memory) vkFreeMemory(buf->device->device, buf->memory, NULL);

    free(buf);
}

size_t vulkan_buffer_size(VulkanBuffer* buf) {
    return buf ? (size_t)buf->size : 0;
}

int vulkan_buffer_copy_to_host(VulkanBuffer* buf, float* host_data, size_t count) {
    if (!buf || !host_data) return -1;

    size_t copy_size = count * sizeof(float);
    if (copy_size > buf->size) copy_size = buf->size;

    void* data;
    VkResult result = vkMapMemory(buf->device->device, buf->memory, 0, copy_size, 0, &data);
    if (result != VK_SUCCESS) {
        vulkan_set_error("Failed to map buffer memory");
        return -1;
    }

    memcpy(host_data, data, copy_size);
    vkUnmapMemory(buf->device->device, buf->memory);

    return 0;
}

// Compute operations (simplified - would use actual compute shaders in production)

int vulkan_normalize_vectors(VulkanDevice* dev, VulkanBuffer* vectors, uint32_t n, uint32_t dims) {
    // CPU fallback for now - would dispatch compute shader in production
    float* data = (float*)malloc(n * dims * sizeof(float));
    if (vulkan_buffer_copy_to_host(vectors, data, n * dims) != 0) {
        free(data);
        return -1;
    }

    for (uint32_t i = 0; i < n; i++) {
        float* vec = data + i * dims;
        float norm = 0.0f;
        for (uint32_t d = 0; d < dims; d++) {
            norm += vec[d] * vec[d];
        }
        norm = sqrtf(norm);
        if (norm > 1e-10f) {
            for (uint32_t d = 0; d < dims; d++) {
                vec[d] /= norm;
            }
        }
    }

    // Write back
    void* mapped;
    vkMapMemory(dev->device, vectors->memory, 0, vectors->size, 0, &mapped);
    memcpy(mapped, data, n * dims * sizeof(float));
    vkUnmapMemory(dev->device, vectors->memory);

    free(data);
    return 0;
}

int vulkan_cosine_similarity(VulkanDevice* dev, VulkanBuffer* embeddings, VulkanBuffer* query,
                              VulkanBuffer* scores, uint32_t n, uint32_t dims, int normalized) {
    // CPU fallback - would dispatch compute shader in production
    float* emb_data = (float*)malloc(n * dims * sizeof(float));
    float* query_data = (float*)malloc(dims * sizeof(float));
    float* score_data = (float*)malloc(n * sizeof(float));

    if (vulkan_buffer_copy_to_host(embeddings, emb_data, n * dims) != 0 ||
        vulkan_buffer_copy_to_host(query, query_data, dims) != 0) {
        free(emb_data);
        free(query_data);
        free(score_data);
        return -1;
    }

    for (uint32_t i = 0; i < n; i++) {
        float* vec = emb_data + i * dims;
        float dot = 0.0f;
        float norm_e = 0.0f;
        float norm_q = 0.0f;

        for (uint32_t d = 0; d < dims; d++) {
            dot += vec[d] * query_data[d];
            if (!normalized) {
                norm_e += vec[d] * vec[d];
                norm_q += query_data[d] * query_data[d];
            }
        }

        if (normalized) {
            score_data[i] = dot;
        } else {
            float denom = sqrtf(norm_e) * sqrtf(norm_q);
            score_data[i] = (denom > 1e-10f) ? dot / denom : 0.0f;
        }
    }

    // Write scores
    void* mapped;
    vkMapMemory(dev->device, scores->memory, 0, n * sizeof(float), 0, &mapped);
    memcpy(mapped, score_data, n * sizeof(float));
    vkUnmapMemory(dev->device, scores->memory);

    free(emb_data);
    free(query_data);
    free(score_data);
    return 0;
}

int vulkan_topk(VulkanDevice* dev, VulkanBuffer* scores, uint32_t* out_indices,
                float* out_scores, uint32_t n, uint32_t k) {
    // CPU implementation for top-k
    float* score_data = (float*)malloc(n * sizeof(float));
    if (vulkan_buffer_copy_to_host(scores, score_data, n) != 0) {
        free(score_data);
        return -1;
    }

    // Simple selection sort for top-k
    uint32_t* indices = (uint32_t*)malloc(n * sizeof(uint32_t));
    for (uint32_t i = 0; i < n; i++) indices[i] = i;

    for (uint32_t i = 0; i < k && i < n; i++) {
        uint32_t max_idx = i;
        for (uint32_t j = i + 1; j < n; j++) {
            if (score_data[indices[j]] > score_data[indices[max_idx]]) {
                max_idx = j;
            }
        }
        uint32_t tmp = indices[i];
        indices[i] = indices[max_idx];
        indices[max_idx] = tmp;
    }

    for (uint32_t i = 0; i < k && i < n; i++) {
        out_indices[i] = indices[i];
        out_scores[i] = score_data[indices[i]];
    }

    free(score_data);
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
	ErrVulkanNotAvailable = errors.New("vulkan: Vulkan is not available on this system")
	ErrDeviceCreation     = errors.New("vulkan: failed to create Vulkan device")
	ErrBufferCreation     = errors.New("vulkan: failed to create buffer")
	ErrKernelExecution    = errors.New("vulkan: kernel execution failed")
	ErrInvalidBuffer      = errors.New("vulkan: invalid buffer")
)

// Device represents a Vulkan GPU device.
type Device struct {
	ptr    *C.VulkanDevice
	id     int
	name   string
	memory uint64
	mu     sync.Mutex
}

// Buffer represents a Vulkan memory buffer.
type Buffer struct {
	ptr    *C.VulkanBuffer
	size   uint64
	device *Device
}

// SearchResult holds a similarity search result.
type SearchResult struct {
	Index uint32
	Score float32
}

// IsAvailable checks if Vulkan is available on this system.
func IsAvailable() bool {
	return C.vulkan_is_available() != 0
}

// DeviceCount returns the number of Vulkan GPU devices.
func DeviceCount() int {
	count := C.vulkan_get_device_count()
	if count < 0 {
		return 0
	}
	return int(count)
}

// NewDevice creates a new Vulkan device handle.
func NewDevice(deviceID int) (*Device, error) {
	if !IsAvailable() {
		return nil, ErrVulkanNotAvailable
	}

	ptr := C.vulkan_create_device(C.int(deviceID))
	if ptr == nil {
		errMsg := C.GoString(C.vulkan_get_last_error())
		C.vulkan_clear_error()
		return nil, fmt.Errorf("%w: %s", ErrDeviceCreation, errMsg)
	}

	return &Device{
		ptr:    ptr,
		id:     deviceID,
		name:   C.GoString(C.vulkan_device_name(ptr)),
		memory: uint64(C.vulkan_device_memory(ptr)),
	}, nil
}

// Release frees the Vulkan device resources.
func (d *Device) Release() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.ptr != nil {
		C.vulkan_release_device(d.ptr)
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

// NewBuffer creates a new GPU buffer with data.
func (d *Device) NewBuffer(data []float32) (*Buffer, error) {
	if len(data) == 0 {
		return nil, errors.New("vulkan: cannot create empty buffer")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	ptr := C.vulkan_create_buffer(
		d.ptr,
		(*C.float)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	)

	if ptr == nil {
		errMsg := C.GoString(C.vulkan_get_last_error())
		C.vulkan_clear_error()
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

	ptr := C.vulkan_create_buffer(
		d.ptr,
		nil,
		C.size_t(count),
	)

	if ptr == nil {
		errMsg := C.GoString(C.vulkan_get_last_error())
		C.vulkan_clear_error()
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
		C.vulkan_release_buffer(b.ptr)
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
	ret := C.vulkan_buffer_copy_to_host(b.ptr, (*C.float)(unsafe.Pointer(&result[0])), C.size_t(count))
	if ret != 0 {
		return nil
	}
	return result
}

// NormalizeVectors normalizes vectors in-place to unit length.
func (d *Device) NormalizeVectors(vectors *Buffer, n, dimensions uint32) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	ret := C.vulkan_normalize_vectors(d.ptr, vectors.ptr, C.uint(n), C.uint(dimensions))
	if ret != 0 {
		errMsg := C.GoString(C.vulkan_get_last_error())
		C.vulkan_clear_error()
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

	ret := C.vulkan_cosine_similarity(d.ptr, embeddings.ptr, query.ptr, scores.ptr,
		C.uint(n), C.uint(dimensions), C.int(normalizedInt))
	if ret != 0 {
		errMsg := C.GoString(C.vulkan_get_last_error())
		C.vulkan_clear_error()
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

	ret := C.vulkan_topk(d.ptr, scores.ptr,
		(*C.uint)(unsafe.Pointer(&indices[0])),
		(*C.float)(unsafe.Pointer(&topkScores[0])),
		C.uint(n), C.uint(k))
	if ret != 0 {
		errMsg := C.GoString(C.vulkan_get_last_error())
		C.vulkan_clear_error()
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
