//go:build !cgovulkan

package vulkan

import (
	"math"
	"testing"
	"unsafe"
)

func TestFindComputeQueueFamily(t *testing.T) {
	original := vkGetPhysicalDeviceQueueFamilyProperties
	defer func() { vkGetPhysicalDeviceQueueFamilyProperties = original }()

	queueFamilies := []VkQueueFamilyProperties{
		{QueueFlags: 0},
		{QueueFlags: VK_QUEUE_COMPUTE_BIT},
		{QueueFlags: VK_QUEUE_COMPUTE_BIT | 0x1},
	}

	vkGetPhysicalDeviceQueueFamilyProperties = func(_ VkPhysicalDevice, count *uint32, props *VkQueueFamilyProperties) {
		if props == nil {
			*count = uint32(len(queueFamilies))
			return
		}
		slice := unsafe.Slice(props, len(queueFamilies))
		copy(slice, queueFamilies)
	}

	if got := findComputeQueueFamily(0); got != 1 {
		t.Fatalf("findComputeQueueFamily() = %d, want 1", got)
	}

	queueFamilies = nil
	vkGetPhysicalDeviceQueueFamilyProperties = func(_ VkPhysicalDevice, count *uint32, props *VkQueueFamilyProperties) {
		if props == nil {
			*count = 0
		}
	}
	if got := findComputeQueueFamily(0); got != -1 {
		t.Fatalf("findComputeQueueFamily() empty = %d, want -1", got)
	}

	queueFamilies = []VkQueueFamilyProperties{{QueueFlags: 0}, {QueueFlags: 0x1}}
	vkGetPhysicalDeviceQueueFamilyProperties = func(_ VkPhysicalDevice, count *uint32, props *VkQueueFamilyProperties) {
		if props == nil {
			*count = uint32(len(queueFamilies))
			return
		}
		slice := unsafe.Slice(props, len(queueFamilies))
		copy(slice, queueFamilies)
	}
	if got := findComputeQueueFamily(0); got != -1 {
		t.Fatalf("findComputeQueueFamily() no compute queue = %d, want -1", got)
	}
}

func TestDeviceFindMemoryType(t *testing.T) {
	original := vkGetPhysicalDeviceMemoryProperties
	defer func() { vkGetPhysicalDeviceMemoryProperties = original }()

	vkGetPhysicalDeviceMemoryProperties = func(_ VkPhysicalDevice, props *VkPhysicalDeviceMemoryProperties) {
		props.MemoryTypeCount = 3
		props.MemoryTypes[0] = VkMemoryType{PropertyFlags: VK_MEMORY_PROPERTY_HOST_VISIBLE_BIT}
		props.MemoryTypes[1] = VkMemoryType{PropertyFlags: VK_MEMORY_PROPERTY_HOST_VISIBLE_BIT | VK_MEMORY_PROPERTY_HOST_COHERENT_BIT}
		props.MemoryTypes[2] = VkMemoryType{PropertyFlags: VK_MEMORY_PROPERTY_HOST_COHERENT_BIT}
	}

	device := &Device{}
	if got, ok := device.findMemoryType(0b111, VK_MEMORY_PROPERTY_HOST_VISIBLE_BIT|VK_MEMORY_PROPERTY_HOST_COHERENT_BIT); !ok || got != 1 {
		t.Fatalf("findMemoryType() = (%d,%v), want (1,true)", got, ok)
	}
	if _, ok := device.findMemoryType(0b001, VK_MEMORY_PROPERTY_HOST_VISIBLE_BIT|VK_MEMORY_PROPERTY_HOST_COHERENT_BIT); ok {
		t.Fatal("findMemoryType() unexpectedly found unsupported property combination")
	}
}

func TestSqrt64(t *testing.T) {
	tests := []struct {
		name string
		in   float64
		want float64
	}{
		{name: "negative", in: -1, want: 0},
		{name: "zero", in: 0, want: 0},
		{name: "one", in: 1, want: 1},
		{name: "perfect square", in: 81, want: 9},
		{name: "fraction", in: 0.25, want: 0.5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sqrt64(tt.in)
			if math.Abs(got-tt.want) > 1e-6 {
				t.Fatalf("sqrt64(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestDeviceMemoryMB(t *testing.T) {
	device := &Device{memory: 3 * 1024 * 1024 * 1024}
	if got := device.MemoryMB(); got != 3072 {
		t.Fatalf("MemoryMB() = %d, want 3072", got)
	}
}

func TestBufferGuardPaths(t *testing.T) {
	buffer := &Buffer{size: 8}
	if got := buffer.ReadFloat32(0); got != nil {
		t.Fatalf("ReadFloat32(0) = %#v, want nil", got)
	}
	if got := buffer.ReadFloat32(3); got != nil {
		t.Fatalf("ReadFloat32(3) = %#v, want nil because it exceeds size", got)
	}
	if got := buffer.ReadUint32(0); got != nil {
		t.Fatalf("ReadUint32(0) = %#v, want nil", got)
	}
	if got := buffer.ReadUint32(3); got != nil {
		t.Fatalf("ReadUint32(3) = %#v, want nil because it exceeds size", got)
	}
	if err := buffer.writeFloat32([]float32{1, 2, 3}); err == nil {
		t.Fatal("writeFloat32 oversized write returned nil error")
	}
}
