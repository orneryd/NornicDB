//go:build !cgovulkan && (linux || darwin)

// Package vulkan provides cross-platform GPU acceleration using Vulkan Compute.
// This file handles Unix-like systems (Linux, macOS/MoltenVK).
package vulkan

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/ebitengine/purego"
)

// Library search paths and names for Unix systems
func getLibraryPaths() ([]string, []string) {
	var libNames []string
	var searchPaths []string

	switch runtime.GOOS {
	case "linux":
		libNames = []string{"libvulkan.so.1", "libvulkan.so"}
		searchPaths = []string{
			"/usr/lib/x86_64-linux-gnu",
			"/usr/lib64",
			"/usr/lib",
			"/usr/local/lib",
		}
		if vulkanSDK := os.Getenv("VULKAN_SDK"); vulkanSDK != "" {
			searchPaths = append([]string{filepath.Join(vulkanSDK, "lib")}, searchPaths...)
		}
	case "darwin":
		libNames = []string{"libvulkan.dylib", "libvulkan.1.dylib", "libMoltenVK.dylib"}
		searchPaths = []string{
			"/usr/local/lib",
			"/opt/homebrew/lib",
		}
		if vulkanSDK := os.Getenv("VULKAN_SDK"); vulkanSDK != "" {
			searchPaths = append([]string{filepath.Join(vulkanSDK, "lib")}, searchPaths...)
		}
	}

	return libNames, searchPaths
}

// loadLibrary loads the Vulkan library on Unix systems
func loadLibrary() (uintptr, error) {
	libNames, searchPaths := getLibraryPaths()

	// Try each combination
	for _, libName := range libNames {
		// Try bare name first (system will search LD_LIBRARY_PATH/DYLD_LIBRARY_PATH)
		if lib, err := purego.Dlopen(libName, purego.RTLD_NOW|purego.RTLD_GLOBAL); err == nil {
			return lib, nil
		}

		// Try explicit paths
		for _, path := range searchPaths {
			fullPath := filepath.Join(path, libName)
			if _, err := os.Stat(fullPath); err == nil {
				if lib, err := purego.Dlopen(fullPath, purego.RTLD_NOW|purego.RTLD_GLOBAL); err == nil {
					return lib, nil
				}
			}
		}
	}

	return 0, fmt.Errorf("vulkan library not found (tried: %v in paths: %v)", libNames, searchPaths)
}

// registerFunctions registers Vulkan function pointers using purego
func registerFunctions(lib uintptr) {
	purego.RegisterLibFunc(&vkCreateInstance, lib, "vkCreateInstance")
	purego.RegisterLibFunc(&vkDestroyInstance, lib, "vkDestroyInstance")
	purego.RegisterLibFunc(&vkEnumeratePhysicalDevices, lib, "vkEnumeratePhysicalDevices")
	purego.RegisterLibFunc(&vkGetPhysicalDeviceProperties, lib, "vkGetPhysicalDeviceProperties")
	purego.RegisterLibFunc(&vkGetPhysicalDeviceMemoryProperties, lib, "vkGetPhysicalDeviceMemoryProperties")
	purego.RegisterLibFunc(&vkGetPhysicalDeviceQueueFamilyProperties, lib, "vkGetPhysicalDeviceQueueFamilyProperties")
	purego.RegisterLibFunc(&vkCreateDevice, lib, "vkCreateDevice")
	purego.RegisterLibFunc(&vkDestroyDevice, lib, "vkDestroyDevice")
	purego.RegisterLibFunc(&vkGetDeviceQueue, lib, "vkGetDeviceQueue")
	purego.RegisterLibFunc(&vkCreateBuffer, lib, "vkCreateBuffer")
	purego.RegisterLibFunc(&vkDestroyBuffer, lib, "vkDestroyBuffer")
	purego.RegisterLibFunc(&vkGetBufferMemoryRequirements, lib, "vkGetBufferMemoryRequirements")
	purego.RegisterLibFunc(&vkAllocateMemory, lib, "vkAllocateMemory")
	purego.RegisterLibFunc(&vkFreeMemory, lib, "vkFreeMemory")
	purego.RegisterLibFunc(&vkBindBufferMemory, lib, "vkBindBufferMemory")
	purego.RegisterLibFunc(&vkMapMemory, lib, "vkMapMemory")
	purego.RegisterLibFunc(&vkUnmapMemory, lib, "vkUnmapMemory")
	purego.RegisterLibFunc(&vkCreateCommandPool, lib, "vkCreateCommandPool")
	purego.RegisterLibFunc(&vkDestroyCommandPool, lib, "vkDestroyCommandPool")
	purego.RegisterLibFunc(&vkDeviceWaitIdle, lib, "vkDeviceWaitIdle")
}

// registerComputeFunctions registers additional Vulkan functions for compute shaders
func registerComputeFunctions(lib uintptr) {
	purego.RegisterLibFunc(&vkCreateShaderModule, lib, "vkCreateShaderModule")
	purego.RegisterLibFunc(&vkDestroyShaderModule, lib, "vkDestroyShaderModule")
	purego.RegisterLibFunc(&vkCreateDescriptorSetLayout, lib, "vkCreateDescriptorSetLayout")
	purego.RegisterLibFunc(&vkDestroyDescriptorSetLayout, lib, "vkDestroyDescriptorSetLayout")
	purego.RegisterLibFunc(&vkCreatePipelineLayout, lib, "vkCreatePipelineLayout")
	purego.RegisterLibFunc(&vkDestroyPipelineLayout, lib, "vkDestroyPipelineLayout")
	purego.RegisterLibFunc(&vkCreateComputePipelines, lib, "vkCreateComputePipelines")
	purego.RegisterLibFunc(&vkDestroyPipeline, lib, "vkDestroyPipeline")
	purego.RegisterLibFunc(&vkCreateDescriptorPool, lib, "vkCreateDescriptorPool")
	purego.RegisterLibFunc(&vkDestroyDescriptorPool, lib, "vkDestroyDescriptorPool")
	purego.RegisterLibFunc(&vkResetDescriptorPool, lib, "vkResetDescriptorPool")
	purego.RegisterLibFunc(&vkAllocateDescriptorSets, lib, "vkAllocateDescriptorSets")
	purego.RegisterLibFunc(&vkUpdateDescriptorSets, lib, "vkUpdateDescriptorSets")
	purego.RegisterLibFunc(&vkAllocateCommandBuffers, lib, "vkAllocateCommandBuffers")
	purego.RegisterLibFunc(&vkFreeCommandBuffers, lib, "vkFreeCommandBuffers")
	purego.RegisterLibFunc(&vkBeginCommandBuffer, lib, "vkBeginCommandBuffer")
	purego.RegisterLibFunc(&vkEndCommandBuffer, lib, "vkEndCommandBuffer")
	purego.RegisterLibFunc(&vkCmdBindPipeline, lib, "vkCmdBindPipeline")
	purego.RegisterLibFunc(&vkCmdBindDescriptorSets, lib, "vkCmdBindDescriptorSets")
	purego.RegisterLibFunc(&vkCmdPushConstants, lib, "vkCmdPushConstants")
	purego.RegisterLibFunc(&vkCmdDispatch, lib, "vkCmdDispatch")
	purego.RegisterLibFunc(&vkCmdPipelineBarrier, lib, "vkCmdPipelineBarrier")
	purego.RegisterLibFunc(&vkQueueSubmit, lib, "vkQueueSubmit")
	purego.RegisterLibFunc(&vkQueueWaitIdle, lib, "vkQueueWaitIdle")
}
