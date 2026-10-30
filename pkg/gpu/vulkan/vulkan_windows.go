//go:build !cgovulkan && windows

// Package vulkan provides cross-platform GPU acceleration using Vulkan Compute.
// This file handles Windows systems using golang.org/x/sys/windows.
package vulkan

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"

	"github.com/ebitengine/purego"
)

// Library search paths and names for Windows
func getLibraryPaths() ([]string, []string) {
	libNames := []string{"vulkan-1.dll"}
	searchPaths := []string{
		// System paths (Windows will search these automatically)
		"",
		// Vulkan SDK
		filepath.Join(os.Getenv("VULKAN_SDK"), "Bin"),
		// System32
		filepath.Join(os.Getenv("SystemRoot"), "System32"),
	}

	return libNames, searchPaths
}

// loadLibrary loads the Vulkan library on Windows
func loadLibrary() (uintptr, error) {
	libNames, searchPaths := getLibraryPaths()

	// Try each combination
	for _, libName := range libNames {
		// Try bare name first (Windows will search PATH and system directories)
		if lib, err := syscall.LoadDLL(libName); err == nil {
			return uintptr(lib.Handle), nil
		}

		// Try explicit paths
		for _, path := range searchPaths {
			if path == "" {
				continue
			}
			fullPath := filepath.Join(path, libName)
			if _, err := os.Stat(fullPath); err == nil {
				if lib, err := syscall.LoadDLL(fullPath); err == nil {
					return uintptr(lib.Handle), nil
				}
			}
		}
	}

	return 0, fmt.Errorf("vulkan library not found (tried: %v)", libNames)
}

// getProcAddress gets a function address from the loaded DLL
func getProcAddress(lib uintptr, name string) uintptr {
	dll := &syscall.DLL{Handle: syscall.Handle(lib)}
	proc, err := dll.FindProc(name)
	if err != nil {
		return 0
	}
	return proc.Addr()
}

// registerFunctions registers Vulkan function pointers on Windows
func registerFunctions(lib uintptr) {
	// On Windows, we use purego.RegisterFunc with addresses from GetProcAddress
	purego.RegisterFunc(&vkCreateInstance, getProcAddress(lib, "vkCreateInstance"))
	purego.RegisterFunc(&vkDestroyInstance, getProcAddress(lib, "vkDestroyInstance"))
	purego.RegisterFunc(&vkEnumeratePhysicalDevices, getProcAddress(lib, "vkEnumeratePhysicalDevices"))
	purego.RegisterFunc(&vkGetPhysicalDeviceProperties, getProcAddress(lib, "vkGetPhysicalDeviceProperties"))
	purego.RegisterFunc(&vkGetPhysicalDeviceMemoryProperties, getProcAddress(lib, "vkGetPhysicalDeviceMemoryProperties"))
	purego.RegisterFunc(&vkGetPhysicalDeviceQueueFamilyProperties, getProcAddress(lib, "vkGetPhysicalDeviceQueueFamilyProperties"))
	purego.RegisterFunc(&vkCreateDevice, getProcAddress(lib, "vkCreateDevice"))
	purego.RegisterFunc(&vkDestroyDevice, getProcAddress(lib, "vkDestroyDevice"))
	purego.RegisterFunc(&vkGetDeviceQueue, getProcAddress(lib, "vkGetDeviceQueue"))
	purego.RegisterFunc(&vkCreateBuffer, getProcAddress(lib, "vkCreateBuffer"))
	purego.RegisterFunc(&vkDestroyBuffer, getProcAddress(lib, "vkDestroyBuffer"))
	purego.RegisterFunc(&vkGetBufferMemoryRequirements, getProcAddress(lib, "vkGetBufferMemoryRequirements"))
	purego.RegisterFunc(&vkAllocateMemory, getProcAddress(lib, "vkAllocateMemory"))
	purego.RegisterFunc(&vkFreeMemory, getProcAddress(lib, "vkFreeMemory"))
	purego.RegisterFunc(&vkBindBufferMemory, getProcAddress(lib, "vkBindBufferMemory"))
	purego.RegisterFunc(&vkMapMemory, getProcAddress(lib, "vkMapMemory"))
	purego.RegisterFunc(&vkUnmapMemory, getProcAddress(lib, "vkUnmapMemory"))
	purego.RegisterFunc(&vkCreateCommandPool, getProcAddress(lib, "vkCreateCommandPool"))
	purego.RegisterFunc(&vkDestroyCommandPool, getProcAddress(lib, "vkDestroyCommandPool"))
	purego.RegisterFunc(&vkDeviceWaitIdle, getProcAddress(lib, "vkDeviceWaitIdle"))
}

// registerComputeFunctions registers additional Vulkan functions for compute shaders
func registerComputeFunctions(lib uintptr) {
	purego.RegisterFunc(&vkCreateShaderModule, getProcAddress(lib, "vkCreateShaderModule"))
	purego.RegisterFunc(&vkDestroyShaderModule, getProcAddress(lib, "vkDestroyShaderModule"))
	purego.RegisterFunc(&vkCreateDescriptorSetLayout, getProcAddress(lib, "vkCreateDescriptorSetLayout"))
	purego.RegisterFunc(&vkDestroyDescriptorSetLayout, getProcAddress(lib, "vkDestroyDescriptorSetLayout"))
	purego.RegisterFunc(&vkCreatePipelineLayout, getProcAddress(lib, "vkCreatePipelineLayout"))
	purego.RegisterFunc(&vkDestroyPipelineLayout, getProcAddress(lib, "vkDestroyPipelineLayout"))
	purego.RegisterFunc(&vkCreateComputePipelines, getProcAddress(lib, "vkCreateComputePipelines"))
	purego.RegisterFunc(&vkDestroyPipeline, getProcAddress(lib, "vkDestroyPipeline"))
	purego.RegisterFunc(&vkCreateDescriptorPool, getProcAddress(lib, "vkCreateDescriptorPool"))
	purego.RegisterFunc(&vkDestroyDescriptorPool, getProcAddress(lib, "vkDestroyDescriptorPool"))
	purego.RegisterFunc(&vkResetDescriptorPool, getProcAddress(lib, "vkResetDescriptorPool"))
	purego.RegisterFunc(&vkAllocateDescriptorSets, getProcAddress(lib, "vkAllocateDescriptorSets"))
	purego.RegisterFunc(&vkUpdateDescriptorSets, getProcAddress(lib, "vkUpdateDescriptorSets"))
	purego.RegisterFunc(&vkAllocateCommandBuffers, getProcAddress(lib, "vkAllocateCommandBuffers"))
	purego.RegisterFunc(&vkFreeCommandBuffers, getProcAddress(lib, "vkFreeCommandBuffers"))
	purego.RegisterFunc(&vkBeginCommandBuffer, getProcAddress(lib, "vkBeginCommandBuffer"))
	purego.RegisterFunc(&vkEndCommandBuffer, getProcAddress(lib, "vkEndCommandBuffer"))
	purego.RegisterFunc(&vkCmdBindPipeline, getProcAddress(lib, "vkCmdBindPipeline"))
	purego.RegisterFunc(&vkCmdBindDescriptorSets, getProcAddress(lib, "vkCmdBindDescriptorSets"))
	purego.RegisterFunc(&vkCmdPushConstants, getProcAddress(lib, "vkCmdPushConstants"))
	purego.RegisterFunc(&vkCmdDispatch, getProcAddress(lib, "vkCmdDispatch"))
	purego.RegisterFunc(&vkCmdPipelineBarrier, getProcAddress(lib, "vkCmdPipelineBarrier"))
	purego.RegisterFunc(&vkQueueSubmit, getProcAddress(lib, "vkQueueSubmit"))
	purego.RegisterFunc(&vkQueueWaitIdle, getProcAddress(lib, "vkQueueWaitIdle"))
}

// Ensure unsafe is used (for type conversions in main file)
var _ = unsafe.Pointer(nil)
