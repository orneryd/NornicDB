//go:build !cgovulkan

// Package vulkan provides cross-platform GPU acceleration using Vulkan Compute.
//
// This implementation uses purego for FFI to dynamically load the Vulkan library,
// enabling GPU acceleration without CGO compilation. This is similar to how
// yzma provides llama.cpp bindings.
//
// Supported Platforms:
//   - Windows: Loads vulkan-1.dll (included with NVIDIA/AMD drivers)
//   - Linux: Loads libvulkan.so.1 (from Vulkan SDK or mesa)
//   - macOS: Loads libvulkan.dylib (from MoltenVK or Vulkan SDK)
//
// The library is automatically detected from standard locations:
//   - System PATH
//   - GPU driver directories
//   - Vulkan SDK installation
package vulkan

import (
	"errors"
	"fmt"
	"sync"
	"unsafe"
)

// Vulkan constants
const (
	VK_SUCCESS                     = 0
	VK_NOT_READY                   = 1
	VK_TIMEOUT                     = 2
	VK_ERROR_OUT_OF_HOST_MEMORY    = -1
	VK_ERROR_OUT_OF_DEVICE_MEMORY  = -2
	VK_ERROR_INITIALIZATION_FAILED = -3
	VK_ERROR_DEVICE_LOST           = -4

	VK_STRUCTURE_TYPE_APPLICATION_INFO         = 0
	VK_STRUCTURE_TYPE_INSTANCE_CREATE_INFO     = 1
	VK_STRUCTURE_TYPE_DEVICE_QUEUE_CREATE_INFO = 2
	VK_STRUCTURE_TYPE_DEVICE_CREATE_INFO       = 3
	VK_STRUCTURE_TYPE_BUFFER_CREATE_INFO       = 12
	VK_STRUCTURE_TYPE_MEMORY_ALLOCATE_INFO     = 5
	VK_STRUCTURE_TYPE_COMMAND_POOL_CREATE_INFO = 39

	VK_API_VERSION_1_1 = uint32(0x00401000) // Version 1.1.0

	VK_QUEUE_COMPUTE_BIT = 0x00000002

	VK_BUFFER_USAGE_STORAGE_BUFFER_BIT = 0x00000020
	VK_BUFFER_USAGE_TRANSFER_SRC_BIT   = 0x00000001
	VK_BUFFER_USAGE_TRANSFER_DST_BIT   = 0x00000002

	VK_SHARING_MODE_EXCLUSIVE = 0

	VK_MEMORY_PROPERTY_HOST_VISIBLE_BIT  = 0x00000002
	VK_MEMORY_PROPERTY_HOST_COHERENT_BIT = 0x00000004
	VK_MEMORY_HEAP_DEVICE_LOCAL_BIT      = 0x00000001

	VK_COMMAND_POOL_CREATE_RESET_COMMAND_BUFFER_BIT = 0x00000002
)

// Vulkan type aliases
type VkInstance uintptr
type VkPhysicalDevice uintptr
type VkDevice uintptr
type VkQueue uintptr
type VkBuffer uintptr
type VkDeviceMemory uintptr
type VkCommandPool uintptr
type VkDeviceSize uint64
type VkResult int32

// VkApplicationInfo structure
type VkApplicationInfo struct {
	SType              uint32
	PNext              uintptr
	PApplicationName   uintptr
	ApplicationVersion uint32
	PEngineName        uintptr
	EngineVersion      uint32
	ApiVersion         uint32
}

// VkInstanceCreateInfo structure
type VkInstanceCreateInfo struct {
	SType                   uint32
	PNext                   uintptr
	Flags                   uint32
	PApplicationInfo        *VkApplicationInfo
	EnabledLayerCount       uint32
	PpEnabledLayerNames     uintptr
	EnabledExtensionCount   uint32
	PpEnabledExtensionNames uintptr
}

// VkPhysicalDeviceProperties structure
type VkPhysicalDeviceProperties struct {
	ApiVersion        uint32
	DriverVersion     uint32
	VendorID          uint32
	DeviceID          uint32
	DeviceType        uint32
	DeviceName        [256]byte
	PipelineCacheUUID [16]byte
	Limits            [512]byte // VkPhysicalDeviceLimits is large
	SparseProperties  [20]byte
}

// VkPhysicalDeviceMemoryProperties structure
type VkPhysicalDeviceMemoryProperties struct {
	MemoryTypeCount uint32
	MemoryTypes     [32]VkMemoryType
	MemoryHeapCount uint32
	MemoryHeaps     [16]VkMemoryHeap
}

// VkMemoryType structure
type VkMemoryType struct {
	PropertyFlags uint32
	HeapIndex     uint32
}

// VkMemoryHeap structure
type VkMemoryHeap struct {
	Size  VkDeviceSize
	Flags uint32
}

// VkQueueFamilyProperties structure
type VkQueueFamilyProperties struct {
	QueueFlags                  uint32
	QueueCount                  uint32
	TimestampValidBits          uint32
	MinImageTransferGranularity [3]uint32
}

// VkDeviceQueueCreateInfo structure
type VkDeviceQueueCreateInfo struct {
	SType            uint32
	PNext            uintptr
	Flags            uint32
	QueueFamilyIndex uint32
	QueueCount       uint32
	PQueuePriorities *float32
}

// VkDeviceCreateInfo structure
type VkDeviceCreateInfo struct {
	SType                   uint32
	PNext                   uintptr
	Flags                   uint32
	QueueCreateInfoCount    uint32
	PQueueCreateInfos       *VkDeviceQueueCreateInfo
	EnabledLayerCount       uint32
	PpEnabledLayerNames     uintptr
	EnabledExtensionCount   uint32
	PpEnabledExtensionNames uintptr
	PEnabledFeatures        uintptr
}

// VkBufferCreateInfo structure
type VkBufferCreateInfo struct {
	SType                 uint32
	PNext                 uintptr
	Flags                 uint32
	Size                  VkDeviceSize
	Usage                 uint32
	SharingMode           uint32
	QueueFamilyIndexCount uint32
	PQueueFamilyIndices   *uint32
}

// VkMemoryRequirements structure
type VkMemoryRequirements struct {
	Size           VkDeviceSize
	Alignment      VkDeviceSize
	MemoryTypeBits uint32
}

// VkMemoryAllocateInfo structure
type VkMemoryAllocateInfo struct {
	SType           uint32
	PNext           uintptr
	AllocationSize  VkDeviceSize
	MemoryTypeIndex uint32
}

// VkCommandPoolCreateInfo structure
type VkCommandPoolCreateInfo struct {
	SType            uint32
	PNext            uintptr
	Flags            uint32
	QueueFamilyIndex uint32
}

// Vulkan function pointers (set by platform-specific code)
var (
	vulkanLib uintptr
	vulkanMu  sync.Mutex
	vulkanErr error

	// Instance functions
	vkCreateInstance                         func(pCreateInfo *VkInstanceCreateInfo, pAllocator uintptr, pInstance *VkInstance) VkResult
	vkDestroyInstance                        func(instance VkInstance, pAllocator uintptr)
	vkEnumeratePhysicalDevices               func(instance VkInstance, pPhysicalDeviceCount *uint32, pPhysicalDevices *VkPhysicalDevice) VkResult
	vkGetPhysicalDeviceProperties            func(physicalDevice VkPhysicalDevice, pProperties *VkPhysicalDeviceProperties)
	vkGetPhysicalDeviceMemoryProperties      func(physicalDevice VkPhysicalDevice, pMemoryProperties *VkPhysicalDeviceMemoryProperties)
	vkGetPhysicalDeviceQueueFamilyProperties func(physicalDevice VkPhysicalDevice, pQueueFamilyPropertyCount *uint32, pQueueFamilyProperties *VkQueueFamilyProperties)
	vkCreateDevice                           func(physicalDevice VkPhysicalDevice, pCreateInfo *VkDeviceCreateInfo, pAllocator uintptr, pDevice *VkDevice) VkResult
	vkDestroyDevice                          func(device VkDevice, pAllocator uintptr)
	vkGetDeviceQueue                         func(device VkDevice, queueFamilyIndex uint32, queueIndex uint32, pQueue *VkQueue)
	vkCreateBuffer                           func(device VkDevice, pCreateInfo *VkBufferCreateInfo, pAllocator uintptr, pBuffer *VkBuffer) VkResult
	vkDestroyBuffer                          func(device VkDevice, buffer VkBuffer, pAllocator uintptr)
	vkGetBufferMemoryRequirements            func(device VkDevice, buffer VkBuffer, pMemoryRequirements *VkMemoryRequirements)
	vkAllocateMemory                         func(device VkDevice, pAllocateInfo *VkMemoryAllocateInfo, pAllocator uintptr, pMemory *VkDeviceMemory) VkResult
	vkFreeMemory                             func(device VkDevice, memory VkDeviceMemory, pAllocator uintptr)
	vkBindBufferMemory                       func(device VkDevice, buffer VkBuffer, memory VkDeviceMemory, memoryOffset VkDeviceSize) VkResult
	vkMapMemory                              func(device VkDevice, memory VkDeviceMemory, offset VkDeviceSize, size VkDeviceSize, flags uint32, ppData *uintptr) VkResult
	vkUnmapMemory                            func(device VkDevice, memory VkDeviceMemory)
	vkCreateCommandPool                      func(device VkDevice, pCreateInfo *VkCommandPoolCreateInfo, pAllocator uintptr, pCommandPool *VkCommandPool) VkResult
	vkDestroyCommandPool                     func(device VkDevice, commandPool VkCommandPool, pAllocator uintptr)
	vkDeviceWaitIdle                         func(device VkDevice) VkResult
)

// Errors
var (
	ErrVulkanNotAvailable = errors.New("vulkan: Vulkan is not available (library not found)")
	ErrDeviceCreation     = errors.New("vulkan: failed to create Vulkan device")
	ErrBufferCreation     = errors.New("vulkan: failed to create buffer")
	ErrKernelExecution    = errors.New("vulkan: kernel execution failed")
	ErrInvalidBuffer      = errors.New("vulkan: invalid buffer")
)

// initVulkan initializes the Vulkan library
func initVulkan() error {
	vulkanMu.Lock()
	defer vulkanMu.Unlock()

	if vulkanLib != 0 {
		return nil // Already initialized
	}

	if vulkanErr != nil {
		return vulkanErr // Previously failed
	}

	lib, err := loadLibrary()
	if err != nil {
		vulkanErr = err
		return err
	}
	vulkanLib = lib

	// Load function pointers (platform-specific)
	registerFunctions(lib)

	return nil
}

// Device represents a Vulkan GPU device.
type Device struct {
	instance       VkInstance
	physicalDevice VkPhysicalDevice
	device         VkDevice
	computeQueue   VkQueue
	commandPool    VkCommandPool
	queueFamily    uint32
	id             int
	name           string
	memory         uint64
	mu             sync.Mutex
}

// Buffer represents a Vulkan memory buffer.
type Buffer struct {
	buffer VkBuffer
	memory VkDeviceMemory
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
	if err := initVulkan(); err != nil {
		return false
	}

	// Try to create a temporary instance
	appName := []byte("NornicDB\x00")
	engineName := []byte("NornicDB GPU\x00")

	appInfo := VkApplicationInfo{
		SType:              VK_STRUCTURE_TYPE_APPLICATION_INFO,
		PApplicationName:   uintptr(unsafe.Pointer(&appName[0])),
		ApplicationVersion: 0x00010000,
		PEngineName:        uintptr(unsafe.Pointer(&engineName[0])),
		EngineVersion:      0x00010000,
		ApiVersion:         VK_API_VERSION_1_1,
	}

	createInfo := VkInstanceCreateInfo{
		SType:            VK_STRUCTURE_TYPE_INSTANCE_CREATE_INFO,
		PApplicationInfo: &appInfo,
	}

	var instance VkInstance
	result := vkCreateInstance(&createInfo, 0, &instance)
	if result != VK_SUCCESS {
		return false
	}

	var deviceCount uint32
	vkEnumeratePhysicalDevices(instance, &deviceCount, nil)

	vkDestroyInstance(instance, 0)

	return deviceCount > 0
}

// DeviceCount returns the number of Vulkan GPU devices.
func DeviceCount() int {
	if err := initVulkan(); err != nil {
		return 0
	}

	appName := []byte("NornicDB\x00")
	engineName := []byte("NornicDB GPU\x00")

	appInfo := VkApplicationInfo{
		SType:              VK_STRUCTURE_TYPE_APPLICATION_INFO,
		PApplicationName:   uintptr(unsafe.Pointer(&appName[0])),
		ApplicationVersion: 0x00010000,
		PEngineName:        uintptr(unsafe.Pointer(&engineName[0])),
		EngineVersion:      0x00010000,
		ApiVersion:         VK_API_VERSION_1_1,
	}

	createInfo := VkInstanceCreateInfo{
		SType:            VK_STRUCTURE_TYPE_INSTANCE_CREATE_INFO,
		PApplicationInfo: &appInfo,
	}

	var instance VkInstance
	if vkCreateInstance(&createInfo, 0, &instance) != VK_SUCCESS {
		return 0
	}
	defer vkDestroyInstance(instance, 0)

	var deviceCount uint32
	vkEnumeratePhysicalDevices(instance, &deviceCount, nil)

	return int(deviceCount)
}

// findComputeQueueFamily finds a queue family that supports compute operations
func findComputeQueueFamily(physicalDevice VkPhysicalDevice) int32 {
	var queueFamilyCount uint32
	vkGetPhysicalDeviceQueueFamilyProperties(physicalDevice, &queueFamilyCount, nil)

	if queueFamilyCount == 0 {
		return -1
	}

	queueFamilies := make([]VkQueueFamilyProperties, queueFamilyCount)
	vkGetPhysicalDeviceQueueFamilyProperties(physicalDevice, &queueFamilyCount, &queueFamilies[0])

	for i := uint32(0); i < queueFamilyCount; i++ {
		if queueFamilies[i].QueueFlags&VK_QUEUE_COMPUTE_BIT != 0 {
			return int32(i)
		}
	}

	return -1
}

// NewDevice creates a new Vulkan device handle.
func NewDevice(deviceID int) (*Device, error) {
	if err := initVulkan(); err != nil {
		return nil, ErrVulkanNotAvailable
	}

	appName := []byte("NornicDB\x00")
	engineName := []byte("NornicDB GPU\x00")

	appInfo := VkApplicationInfo{
		SType:              VK_STRUCTURE_TYPE_APPLICATION_INFO,
		PApplicationName:   uintptr(unsafe.Pointer(&appName[0])),
		ApplicationVersion: 0x00010000,
		PEngineName:        uintptr(unsafe.Pointer(&engineName[0])),
		EngineVersion:      0x00010000,
		ApiVersion:         VK_API_VERSION_1_1,
	}

	createInfo := VkInstanceCreateInfo{
		SType:            VK_STRUCTURE_TYPE_INSTANCE_CREATE_INFO,
		PApplicationInfo: &appInfo,
	}

	var instance VkInstance
	if result := vkCreateInstance(&createInfo, 0, &instance); result != VK_SUCCESS {
		return nil, fmt.Errorf("%w: failed to create instance (code %d)", ErrDeviceCreation, result)
	}

	// Enumerate physical devices
	var deviceCount uint32
	vkEnumeratePhysicalDevices(instance, &deviceCount, nil)
	if deviceCount == 0 || deviceID >= int(deviceCount) {
		vkDestroyInstance(instance, 0)
		return nil, fmt.Errorf("%w: no suitable GPU found or invalid device ID", ErrDeviceCreation)
	}

	physicalDevices := make([]VkPhysicalDevice, deviceCount)
	vkEnumeratePhysicalDevices(instance, &deviceCount, &physicalDevices[0])
	physicalDevice := physicalDevices[deviceID]

	// Get device properties
	var properties VkPhysicalDeviceProperties
	vkGetPhysicalDeviceProperties(physicalDevice, &properties)

	// Extract device name (null-terminated string)
	var deviceName string
	for i, b := range properties.DeviceName {
		if b == 0 {
			deviceName = string(properties.DeviceName[:i])
			break
		}
	}

	// Get device memory
	var memProperties VkPhysicalDeviceMemoryProperties
	vkGetPhysicalDeviceMemoryProperties(physicalDevice, &memProperties)

	var deviceMemory uint64
	for i := uint32(0); i < memProperties.MemoryHeapCount; i++ {
		if memProperties.MemoryHeaps[i].Flags&VK_MEMORY_HEAP_DEVICE_LOCAL_BIT != 0 {
			deviceMemory = uint64(memProperties.MemoryHeaps[i].Size)
			break
		}
	}

	// Find compute queue family
	computeFamily := findComputeQueueFamily(physicalDevice)
	if computeFamily < 0 {
		vkDestroyInstance(instance, 0)
		return nil, fmt.Errorf("%w: no compute queue family found", ErrDeviceCreation)
	}

	// Create logical device
	queuePriority := float32(1.0)
	queueCreateInfo := VkDeviceQueueCreateInfo{
		SType:            VK_STRUCTURE_TYPE_DEVICE_QUEUE_CREATE_INFO,
		QueueFamilyIndex: uint32(computeFamily),
		QueueCount:       1,
		PQueuePriorities: &queuePriority,
	}

	deviceCreateInfo := VkDeviceCreateInfo{
		SType:                VK_STRUCTURE_TYPE_DEVICE_CREATE_INFO,
		QueueCreateInfoCount: 1,
		PQueueCreateInfos:    &queueCreateInfo,
	}

	var device VkDevice
	if result := vkCreateDevice(physicalDevice, &deviceCreateInfo, 0, &device); result != VK_SUCCESS {
		vkDestroyInstance(instance, 0)
		return nil, fmt.Errorf("%w: failed to create logical device (code %d)", ErrDeviceCreation, result)
	}

	// Get compute queue
	var computeQueue VkQueue
	vkGetDeviceQueue(device, uint32(computeFamily), 0, &computeQueue)

	// Create command pool
	poolCreateInfo := VkCommandPoolCreateInfo{
		SType:            VK_STRUCTURE_TYPE_COMMAND_POOL_CREATE_INFO,
		Flags:            VK_COMMAND_POOL_CREATE_RESET_COMMAND_BUFFER_BIT,
		QueueFamilyIndex: uint32(computeFamily),
	}

	var commandPool VkCommandPool
	if result := vkCreateCommandPool(device, &poolCreateInfo, 0, &commandPool); result != VK_SUCCESS {
		vkDestroyDevice(device, 0)
		vkDestroyInstance(instance, 0)
		return nil, fmt.Errorf("%w: failed to create command pool (code %d)", ErrDeviceCreation, result)
	}

	return &Device{
		instance:       instance,
		physicalDevice: physicalDevice,
		device:         device,
		computeQueue:   computeQueue,
		commandPool:    commandPool,
		queueFamily:    uint32(computeFamily),
		id:             deviceID,
		name:           deviceName,
		memory:         deviceMemory,
	}, nil
}

// Release frees the Vulkan device resources.
func (d *Device) Release() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.device != 0 {
		vkDeviceWaitIdle(d.device)
		if d.commandPool != 0 {
			vkDestroyCommandPool(d.device, d.commandPool, 0)
		}
		vkDestroyDevice(d.device, 0)
	}
	if d.instance != 0 {
		vkDestroyInstance(d.instance, 0)
	}

	d.device = 0
	d.instance = 0
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

// findMemoryType finds a suitable memory type for allocation
func (d *Device) findMemoryType(typeFilter uint32, properties uint32) (uint32, bool) {
	var memProperties VkPhysicalDeviceMemoryProperties
	vkGetPhysicalDeviceMemoryProperties(d.physicalDevice, &memProperties)

	for i := uint32(0); i < memProperties.MemoryTypeCount; i++ {
		if (typeFilter&(1<<i)) != 0 &&
			(memProperties.MemoryTypes[i].PropertyFlags&properties) == properties {
			return i, true
		}
	}
	return 0, false
}

// NewBuffer creates a new GPU buffer with data.
func (d *Device) NewBuffer(data []float32) (*Buffer, error) {
	// Check if device is initialized
	if d.device == 0 {
		return nil, ErrVulkanNotAvailable
	}

	if len(data) == 0 {
		return nil, errors.New("vulkan: cannot create empty buffer")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	size := VkDeviceSize(len(data) * 4)

	// Create buffer
	bufferInfo := VkBufferCreateInfo{
		SType:       VK_STRUCTURE_TYPE_BUFFER_CREATE_INFO,
		Size:        size,
		Usage:       VK_BUFFER_USAGE_STORAGE_BUFFER_BIT | VK_BUFFER_USAGE_TRANSFER_SRC_BIT | VK_BUFFER_USAGE_TRANSFER_DST_BIT,
		SharingMode: VK_SHARING_MODE_EXCLUSIVE,
	}

	var buffer VkBuffer
	if result := vkCreateBuffer(d.device, &bufferInfo, 0, &buffer); result != VK_SUCCESS {
		return nil, fmt.Errorf("%w: failed to create buffer (code %d)", ErrBufferCreation, result)
	}

	// Get memory requirements
	var memReqs VkMemoryRequirements
	vkGetBufferMemoryRequirements(d.device, buffer, &memReqs)

	// Find suitable memory type (host visible for easy access)
	memTypeIndex, found := d.findMemoryType(memReqs.MemoryTypeBits,
		VK_MEMORY_PROPERTY_HOST_VISIBLE_BIT|VK_MEMORY_PROPERTY_HOST_COHERENT_BIT)
	if !found {
		vkDestroyBuffer(d.device, buffer, 0)
		return nil, fmt.Errorf("%w: no suitable memory type found", ErrBufferCreation)
	}

	// Allocate memory
	allocInfo := VkMemoryAllocateInfo{
		SType:           VK_STRUCTURE_TYPE_MEMORY_ALLOCATE_INFO,
		AllocationSize:  memReqs.Size,
		MemoryTypeIndex: memTypeIndex,
	}

	var memory VkDeviceMemory
	if result := vkAllocateMemory(d.device, &allocInfo, 0, &memory); result != VK_SUCCESS {
		vkDestroyBuffer(d.device, buffer, 0)
		return nil, fmt.Errorf("%w: failed to allocate memory (code %d)", ErrBufferCreation, result)
	}

	// Bind buffer to memory
	if result := vkBindBufferMemory(d.device, buffer, memory, 0); result != VK_SUCCESS {
		vkFreeMemory(d.device, memory, 0)
		vkDestroyBuffer(d.device, buffer, 0)
		return nil, fmt.Errorf("%w: failed to bind buffer memory (code %d)", ErrBufferCreation, result)
	}

	// Map and copy data
	var mappedPtr uintptr
	if result := vkMapMemory(d.device, memory, 0, size, 0, &mappedPtr); result != VK_SUCCESS {
		vkFreeMemory(d.device, memory, 0)
		vkDestroyBuffer(d.device, buffer, 0)
		return nil, fmt.Errorf("%w: failed to map memory (code %d)", ErrBufferCreation, result)
	}

	// Copy data to mapped memory
	dst := unsafe.Slice((*float32)(unsafe.Pointer(mappedPtr)), len(data))
	copy(dst, data)

	vkUnmapMemory(d.device, memory)

	return &Buffer{
		buffer: buffer,
		memory: memory,
		size:   uint64(size),
		device: d,
	}, nil
}

// NewEmptyBuffer creates an uninitialized GPU buffer.
func (d *Device) NewEmptyBuffer(count uint64) (*Buffer, error) {
	// Check if device is initialized
	if d.device == 0 {
		return nil, ErrVulkanNotAvailable
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	size := VkDeviceSize(count * 4)

	bufferInfo := VkBufferCreateInfo{
		SType:       VK_STRUCTURE_TYPE_BUFFER_CREATE_INFO,
		Size:        size,
		Usage:       VK_BUFFER_USAGE_STORAGE_BUFFER_BIT | VK_BUFFER_USAGE_TRANSFER_SRC_BIT | VK_BUFFER_USAGE_TRANSFER_DST_BIT,
		SharingMode: VK_SHARING_MODE_EXCLUSIVE,
	}

	var buffer VkBuffer
	if result := vkCreateBuffer(d.device, &bufferInfo, 0, &buffer); result != VK_SUCCESS {
		return nil, fmt.Errorf("%w: failed to create buffer (code %d)", ErrBufferCreation, result)
	}

	var memReqs VkMemoryRequirements
	vkGetBufferMemoryRequirements(d.device, buffer, &memReqs)

	memTypeIndex, found := d.findMemoryType(memReqs.MemoryTypeBits,
		VK_MEMORY_PROPERTY_HOST_VISIBLE_BIT|VK_MEMORY_PROPERTY_HOST_COHERENT_BIT)
	if !found {
		vkDestroyBuffer(d.device, buffer, 0)
		return nil, fmt.Errorf("%w: no suitable memory type found", ErrBufferCreation)
	}

	allocInfo := VkMemoryAllocateInfo{
		SType:           VK_STRUCTURE_TYPE_MEMORY_ALLOCATE_INFO,
		AllocationSize:  memReqs.Size,
		MemoryTypeIndex: memTypeIndex,
	}

	var memory VkDeviceMemory
	if result := vkAllocateMemory(d.device, &allocInfo, 0, &memory); result != VK_SUCCESS {
		vkDestroyBuffer(d.device, buffer, 0)
		return nil, fmt.Errorf("%w: failed to allocate memory (code %d)", ErrBufferCreation, result)
	}

	if result := vkBindBufferMemory(d.device, buffer, memory, 0); result != VK_SUCCESS {
		vkFreeMemory(d.device, memory, 0)
		vkDestroyBuffer(d.device, buffer, 0)
		return nil, fmt.Errorf("%w: failed to bind buffer memory (code %d)", ErrBufferCreation, result)
	}

	return &Buffer{
		buffer: buffer,
		memory: memory,
		size:   uint64(size),
		device: d,
	}, nil
}

// Release frees the buffer resources.
func (b *Buffer) Release() {
	if b.device == nil {
		return
	}
	b.device.mu.Lock()
	defer b.device.mu.Unlock()

	if b.buffer != 0 {
		vkDestroyBuffer(b.device.device, b.buffer, 0)
	}
	if b.memory != 0 {
		vkFreeMemory(b.device.device, b.memory, 0)
	}
	b.buffer = 0
	b.memory = 0
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

	b.device.mu.Lock()
	defer b.device.mu.Unlock()

	var mappedPtr uintptr
	if result := vkMapMemory(b.device.device, b.memory, 0, VkDeviceSize(count*4), 0, &mappedPtr); result != VK_SUCCESS {
		return nil
	}

	result := make([]float32, count)
	src := unsafe.Slice((*float32)(unsafe.Pointer(mappedPtr)), count)
	copy(result, src)

	vkUnmapMemory(b.device.device, b.memory)

	return result
}

// ReadUint32 reads uint32 values from the buffer.
func (b *Buffer) ReadUint32(count int) []uint32 {
	if count <= 0 || uint64(count*4) > b.size {
		return nil
	}

	b.device.mu.Lock()
	defer b.device.mu.Unlock()

	var mappedPtr uintptr
	if result := vkMapMemory(b.device.device, b.memory, 0, VkDeviceSize(count*4), 0, &mappedPtr); result != VK_SUCCESS {
		return nil
	}

	result := make([]uint32, count)
	src := unsafe.Slice((*uint32)(unsafe.Pointer(mappedPtr)), count)
	copy(result, src)

	vkUnmapMemory(b.device.device, b.memory)

	return result
}

// writeFloat32 writes float32 values to the buffer
func (b *Buffer) writeFloat32(data []float32) error {
	if uint64(len(data)*4) > b.size {
		return errors.New("vulkan: data exceeds buffer size")
	}

	b.device.mu.Lock()
	defer b.device.mu.Unlock()

	var mappedPtr uintptr
	if result := vkMapMemory(b.device.device, b.memory, 0, VkDeviceSize(len(data)*4), 0, &mappedPtr); result != VK_SUCCESS {
		return fmt.Errorf("vulkan: failed to map memory (code %d)", result)
	}

	dst := unsafe.Slice((*float32)(unsafe.Pointer(mappedPtr)), len(data))
	copy(dst, data)

	vkUnmapMemory(b.device.device, b.memory)

	return nil
}

// NormalizeVectors normalizes vectors in-place to unit length.
// This is a CPU implementation as compute shaders require more setup.
func (d *Device) NormalizeVectors(vectors *Buffer, n, dimensions uint32) error {
	// Check if device is initialized
	if d.device == 0 {
		return ErrVulkanNotAvailable
	}

	data := vectors.ReadFloat32(int(n * dimensions))
	if data == nil {
		return ErrInvalidBuffer
	}

	// Normalize each vector
	for i := uint32(0); i < n; i++ {
		vec := data[i*dimensions : (i+1)*dimensions]
		var norm float32
		for _, v := range vec {
			norm += v * v
		}
		if norm > 1e-10 {
			norm = float32(1.0 / sqrt64(float64(norm)))
			for j := range vec {
				vec[j] *= norm
			}
		}
	}

	return vectors.writeFloat32(data)
}

// sqrt64 computes square root using Newton's method
func sqrt64(x float64) float64 {
	if x <= 0 {
		return 0
	}
	z := x
	for i := 0; i < 10; i++ {
		z = (z + x/z) / 2
	}
	return z
}

// CosineSimilarity computes cosine similarity between query and all embeddings.
// This is a CPU implementation that uses GPU memory buffers.
func (d *Device) CosineSimilarity(embeddings, query, scores *Buffer,
	n, dimensions uint32, normalized bool) error {
	// Check if device is initialized
	if d.device == 0 {
		return ErrVulkanNotAvailable
	}

	embData := embeddings.ReadFloat32(int(n * dimensions))
	queryData := query.ReadFloat32(int(dimensions))
	if embData == nil || queryData == nil {
		return ErrInvalidBuffer
	}

	scoreData := make([]float32, n)

	for i := uint32(0); i < n; i++ {
		vec := embData[i*dimensions : (i+1)*dimensions]
		var dot, normE, normQ float32

		for dd := uint32(0); dd < dimensions; dd++ {
			dot += vec[dd] * queryData[dd]
			if !normalized {
				normE += vec[dd] * vec[dd]
				normQ += queryData[dd] * queryData[dd]
			}
		}

		if normalized {
			scoreData[i] = dot
		} else {
			denom := float32(sqrt64(float64(normE)) * sqrt64(float64(normQ)))
			if denom > 1e-10 {
				scoreData[i] = dot / denom
			}
		}
	}

	return scores.writeFloat32(scoreData)
}

// TopK finds the k highest scoring indices.
func (d *Device) TopK(scores *Buffer, n, k uint32) ([]uint32, []float32, error) {
	// Check if device is initialized
	if d.device == 0 {
		return nil, nil, ErrVulkanNotAvailable
	}

	scoreData := scores.ReadFloat32(int(n))
	if scoreData == nil {
		return nil, nil, ErrInvalidBuffer
	}

	// Simple selection sort for top-k
	indices := make([]uint32, n)
	for i := uint32(0); i < n; i++ {
		indices[i] = i
	}

	for i := uint32(0); i < k && i < n; i++ {
		maxIdx := i
		for j := i + 1; j < n; j++ {
			if scoreData[indices[j]] > scoreData[indices[maxIdx]] {
				maxIdx = j
			}
		}
		indices[i], indices[maxIdx] = indices[maxIdx], indices[i]
	}

	resultIndices := make([]uint32, k)
	resultScores := make([]float32, k)
	for i := uint32(0); i < k; i++ {
		resultIndices[i] = indices[i]
		resultScores[i] = scoreData[indices[i]]
	}

	return resultIndices, resultScores, nil
}

// Search performs a complete similarity search.
func (d *Device) Search(embeddings *Buffer, query []float32, n, dimensions uint32, k int, normalized bool) ([]SearchResult, error) {
	// Check if device is initialized
	if d.device == 0 {
		return nil, ErrVulkanNotAvailable
	}

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
	indices, topScores, err := d.TopK(scoresBuf, n, uint32(k))
	if err != nil {
		return nil, err
	}

	// Build results
	results := make([]SearchResult, k)
	for i := 0; i < k; i++ {
		results[i] = SearchResult{
			Index: indices[i],
			Score: topScores[i],
		}
	}

	return results, nil
}
