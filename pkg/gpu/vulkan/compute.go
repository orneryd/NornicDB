//go:build !cgovulkan

// Package vulkan provides GPU-accelerated compute operations using Vulkan.
//
// This file contains the compute pipeline infrastructure that executes
// SPIR-V shaders on the GPU for similarity search operations.

package vulkan

import (
	_ "embed"
	"errors"
	"fmt"
	"sync"
	"unsafe"
)

// Embedded SPIR-V shader binaries compiled from GLSL
var (
	//go:embed shaders/cosine_similarity.spv
	cosineSimilaritySPV []byte

	//go:embed shaders/normalize.spv
	normalizeSPV []byte

	//go:embed shaders/topk.spv
	topkSPV []byte

	//go:embed shaders/topk_full.spv
	topkFullSPV []byte
)

// Additional Vulkan constants for compute pipelines
const (
	VK_STRUCTURE_TYPE_SHADER_MODULE_CREATE_INFO         = 16
	VK_STRUCTURE_TYPE_PIPELINE_SHADER_STAGE_CREATE_INFO = 18
	VK_STRUCTURE_TYPE_COMPUTE_PIPELINE_CREATE_INFO      = 29
	VK_STRUCTURE_TYPE_PIPELINE_LAYOUT_CREATE_INFO       = 30
	VK_STRUCTURE_TYPE_DESCRIPTOR_SET_LAYOUT_CREATE_INFO = 32
	VK_STRUCTURE_TYPE_DESCRIPTOR_POOL_CREATE_INFO       = 33
	VK_STRUCTURE_TYPE_DESCRIPTOR_SET_ALLOCATE_INFO      = 34
	VK_STRUCTURE_TYPE_WRITE_DESCRIPTOR_SET              = 35
	VK_STRUCTURE_TYPE_COMMAND_BUFFER_ALLOCATE_INFO      = 40
	VK_STRUCTURE_TYPE_COMMAND_BUFFER_BEGIN_INFO         = 42
	VK_STRUCTURE_TYPE_SUBMIT_INFO                       = 4

	VK_SHADER_STAGE_COMPUTE_BIT = 0x00000020

	VK_DESCRIPTOR_TYPE_STORAGE_BUFFER = 7

	VK_COMMAND_BUFFER_LEVEL_PRIMARY = 0

	VK_COMMAND_BUFFER_USAGE_ONE_TIME_SUBMIT_BIT = 0x00000001

	VK_PIPELINE_BIND_POINT_COMPUTE = 1
)

// Vulkan handle types for compute
type VkShaderModule uintptr
type VkDescriptorSetLayout uintptr
type VkPipelineLayout uintptr
type VkPipeline uintptr
type VkDescriptorPool uintptr
type VkDescriptorSet uintptr
type VkCommandBuffer uintptr
type VkFence uintptr

// VkShaderModuleCreateInfo structure
type VkShaderModuleCreateInfo struct {
	SType    uint32
	PNext    uintptr
	Flags    uint32
	CodeSize uintptr
	PCode    *uint32
}

// VkDescriptorSetLayoutBinding structure
type VkDescriptorSetLayoutBinding struct {
	Binding            uint32
	DescriptorType     uint32
	DescriptorCount    uint32
	StageFlags         uint32
	PImmutableSamplers uintptr
}

// VkDescriptorSetLayoutCreateInfo structure
type VkDescriptorSetLayoutCreateInfo struct {
	SType        uint32
	PNext        uintptr
	Flags        uint32
	BindingCount uint32
	PBindings    *VkDescriptorSetLayoutBinding
}

// VkPushConstantRange structure
type VkPushConstantRange struct {
	StageFlags uint32
	Offset     uint32
	Size       uint32
}

// VkPipelineLayoutCreateInfo structure
type VkPipelineLayoutCreateInfo struct {
	SType                  uint32
	PNext                  uintptr
	Flags                  uint32
	SetLayoutCount         uint32
	PSetLayouts            *VkDescriptorSetLayout
	PushConstantRangeCount uint32
	PPushConstantRanges    *VkPushConstantRange
}

// VkPipelineShaderStageCreateInfo structure
type VkPipelineShaderStageCreateInfo struct {
	SType               uint32
	PNext               uintptr
	Flags               uint32
	Stage               uint32
	Module              VkShaderModule
	PName               uintptr
	PSpecializationInfo uintptr
}

// VkComputePipelineCreateInfo structure
type VkComputePipelineCreateInfo struct {
	SType              uint32
	PNext              uintptr
	Flags              uint32
	Stage              VkPipelineShaderStageCreateInfo
	Layout             VkPipelineLayout
	BasePipelineHandle VkPipeline
	BasePipelineIndex  int32
}

// VkDescriptorPoolSize structure
type VkDescriptorPoolSize struct {
	Type            uint32
	DescriptorCount uint32
}

// VkDescriptorPoolCreateInfo structure
type VkDescriptorPoolCreateInfo struct {
	SType         uint32
	PNext         uintptr
	Flags         uint32
	MaxSets       uint32
	PoolSizeCount uint32
	PPoolSizes    *VkDescriptorPoolSize
}

// VkDescriptorSetAllocateInfo structure
type VkDescriptorSetAllocateInfo struct {
	SType              uint32
	PNext              uintptr
	DescriptorPool     VkDescriptorPool
	DescriptorSetCount uint32
	PSetLayouts        *VkDescriptorSetLayout
}

// VkDescriptorBufferInfo structure
type VkDescriptorBufferInfo struct {
	Buffer VkBuffer
	Offset VkDeviceSize
	Range  VkDeviceSize
}

// VkWriteDescriptorSet structure
type VkWriteDescriptorSet struct {
	SType            uint32
	PNext            uintptr
	DstSet           VkDescriptorSet
	DstBinding       uint32
	DstArrayElement  uint32
	DescriptorCount  uint32
	DescriptorType   uint32
	PImageInfo       uintptr
	PBufferInfo      *VkDescriptorBufferInfo
	PTexelBufferView uintptr
}

// VkCommandBufferAllocateInfo structure
type VkCommandBufferAllocateInfo struct {
	SType              uint32
	PNext              uintptr
	CommandPool        VkCommandPool
	Level              uint32
	CommandBufferCount uint32
}

// VkCommandBufferBeginInfo structure
type VkCommandBufferBeginInfo struct {
	SType            uint32
	PNext            uintptr
	Flags            uint32
	PInheritanceInfo uintptr
}

// VkSubmitInfo structure
type VkSubmitInfo struct {
	SType                uint32
	PNext                uintptr
	WaitSemaphoreCount   uint32
	PWaitSemaphores      uintptr
	PWaitDstStageMask    uintptr
	CommandBufferCount   uint32
	PCommandBuffers      *VkCommandBuffer
	SignalSemaphoreCount uint32
	PSignalSemaphores    uintptr
}

// Additional Vulkan function pointers for compute
var (
	vkCreateShaderModule         func(device VkDevice, pCreateInfo *VkShaderModuleCreateInfo, pAllocator uintptr, pShaderModule *VkShaderModule) VkResult
	vkDestroyShaderModule        func(device VkDevice, shaderModule VkShaderModule, pAllocator uintptr)
	vkCreateDescriptorSetLayout  func(device VkDevice, pCreateInfo *VkDescriptorSetLayoutCreateInfo, pAllocator uintptr, pSetLayout *VkDescriptorSetLayout) VkResult
	vkDestroyDescriptorSetLayout func(device VkDevice, descriptorSetLayout VkDescriptorSetLayout, pAllocator uintptr)
	vkCreatePipelineLayout       func(device VkDevice, pCreateInfo *VkPipelineLayoutCreateInfo, pAllocator uintptr, pPipelineLayout *VkPipelineLayout) VkResult
	vkDestroyPipelineLayout      func(device VkDevice, pipelineLayout VkPipelineLayout, pAllocator uintptr)
	vkCreateComputePipelines     func(device VkDevice, pipelineCache uintptr, createInfoCount uint32, pCreateInfos *VkComputePipelineCreateInfo, pAllocator uintptr, pPipelines *VkPipeline) VkResult
	vkDestroyPipeline            func(device VkDevice, pipeline VkPipeline, pAllocator uintptr)
	vkCreateDescriptorPool       func(device VkDevice, pCreateInfo *VkDescriptorPoolCreateInfo, pAllocator uintptr, pDescriptorPool *VkDescriptorPool) VkResult
	vkDestroyDescriptorPool      func(device VkDevice, descriptorPool VkDescriptorPool, pAllocator uintptr)
	vkResetDescriptorPool        func(device VkDevice, descriptorPool VkDescriptorPool, flags uint32) VkResult
	vkAllocateDescriptorSets     func(device VkDevice, pAllocateInfo *VkDescriptorSetAllocateInfo, pDescriptorSets *VkDescriptorSet) VkResult
	vkUpdateDescriptorSets       func(device VkDevice, descriptorWriteCount uint32, pDescriptorWrites *VkWriteDescriptorSet, descriptorCopyCount uint32, pDescriptorCopies uintptr)
	vkAllocateCommandBuffers     func(device VkDevice, pAllocateInfo *VkCommandBufferAllocateInfo, pCommandBuffers *VkCommandBuffer) VkResult
	vkFreeCommandBuffers         func(device VkDevice, commandPool VkCommandPool, commandBufferCount uint32, pCommandBuffers *VkCommandBuffer)
	vkBeginCommandBuffer         func(commandBuffer VkCommandBuffer, pBeginInfo *VkCommandBufferBeginInfo) VkResult
	vkEndCommandBuffer           func(commandBuffer VkCommandBuffer) VkResult
	vkCmdBindPipeline            func(commandBuffer VkCommandBuffer, pipelineBindPoint uint32, pipeline VkPipeline)
	vkCmdBindDescriptorSets      func(commandBuffer VkCommandBuffer, pipelineBindPoint uint32, layout VkPipelineLayout, firstSet uint32, descriptorSetCount uint32, pDescriptorSets *VkDescriptorSet, dynamicOffsetCount uint32, pDynamicOffsets uintptr)
	vkCmdPushConstants           func(commandBuffer VkCommandBuffer, layout VkPipelineLayout, stageFlags uint32, offset uint32, size uint32, pValues uintptr)
	vkCmdDispatch                func(commandBuffer VkCommandBuffer, groupCountX uint32, groupCountY uint32, groupCountZ uint32)
	vkQueueSubmit                func(queue VkQueue, submitCount uint32, pSubmits *VkSubmitInfo, fence VkFence) VkResult
	vkQueueWaitIdle              func(queue VkQueue) VkResult

	computeFunctionsRegistered bool
	computeFunctionsMu         sync.Mutex
)

// ComputePipeline represents a compiled compute shader pipeline
type ComputePipeline struct {
	device           *Device
	shaderModule     VkShaderModule
	descriptorLayout VkDescriptorSetLayout
	pipelineLayout   VkPipelineLayout
	pipeline         VkPipeline
	descriptorPool   VkDescriptorPool
	pushConstantSize uint32
}

// PipelineType identifies which shader to use
type PipelineType int

const (
	PipelineCosineSimilarity PipelineType = iota
	PipelineNormalize
	PipelineTopK
	PipelineTopKFull
)

// ComputeContext holds resources for compute operations
type ComputeContext struct {
	device    *Device
	pipelines map[PipelineType]*ComputePipeline
	mu        sync.Mutex
}

// NewComputeContext creates a new compute context with shader pipelines
func (d *Device) NewComputeContext() (*ComputeContext, error) {
	computeFunctionsMu.Lock()
	if !computeFunctionsRegistered {
		registerComputeFunctions(vulkanLib)
		computeFunctionsRegistered = true
	}
	computeFunctionsMu.Unlock()

	ctx := &ComputeContext{
		device:    d,
		pipelines: make(map[PipelineType]*ComputePipeline),
	}

	// Create cosine similarity pipeline (3 buffers: embeddings, query, scores)
	cosPipeline, err := d.createPipeline(cosineSimilaritySPV, 3, 12) // 3 uint32 push constants
	if err != nil {
		return nil, fmt.Errorf("failed to create cosine similarity pipeline: %w", err)
	}
	ctx.pipelines[PipelineCosineSimilarity] = cosPipeline

	// Create normalize pipeline (1 buffer: vectors)
	normPipeline, err := d.createPipeline(normalizeSPV, 1, 8) // 2 uint32 push constants
	if err != nil {
		cosPipeline.Release()
		return nil, fmt.Errorf("failed to create normalize pipeline: %w", err)
	}
	ctx.pipelines[PipelineNormalize] = normPipeline

	// Create top-k pipeline (3 buffers: scores, indices, top_scores)
	topkPipeline, err := d.createPipeline(topkFullSPV, 3, 8) // 2 uint32 push constants
	if err != nil {
		cosPipeline.Release()
		normPipeline.Release()
		return nil, fmt.Errorf("failed to create topk pipeline: %w", err)
	}
	ctx.pipelines[PipelineTopK] = topkPipeline

	return ctx, nil
}

// createPipeline creates a compute pipeline from SPIR-V code
func (d *Device) createPipeline(spirv []byte, numBuffers int, pushConstantSize uint32) (*ComputePipeline, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Create shader module
	moduleInfo := VkShaderModuleCreateInfo{
		SType:    VK_STRUCTURE_TYPE_SHADER_MODULE_CREATE_INFO,
		CodeSize: uintptr(len(spirv)),
		PCode:    (*uint32)(unsafe.Pointer(&spirv[0])),
	}

	var shaderModule VkShaderModule
	if result := vkCreateShaderModule(d.device, &moduleInfo, 0, &shaderModule); result != VK_SUCCESS {
		return nil, fmt.Errorf("failed to create shader module: %d", result)
	}

	// Create descriptor set layout
	bindings := make([]VkDescriptorSetLayoutBinding, numBuffers)
	for i := 0; i < numBuffers; i++ {
		bindings[i] = VkDescriptorSetLayoutBinding{
			Binding:         uint32(i),
			DescriptorType:  VK_DESCRIPTOR_TYPE_STORAGE_BUFFER,
			DescriptorCount: 1,
			StageFlags:      VK_SHADER_STAGE_COMPUTE_BIT,
		}
	}

	layoutInfo := VkDescriptorSetLayoutCreateInfo{
		SType:        VK_STRUCTURE_TYPE_DESCRIPTOR_SET_LAYOUT_CREATE_INFO,
		BindingCount: uint32(numBuffers),
		PBindings:    &bindings[0],
	}

	var descriptorLayout VkDescriptorSetLayout
	if result := vkCreateDescriptorSetLayout(d.device, &layoutInfo, 0, &descriptorLayout); result != VK_SUCCESS {
		vkDestroyShaderModule(d.device, shaderModule, 0)
		return nil, fmt.Errorf("failed to create descriptor set layout: %d", result)
	}

	// Create pipeline layout with push constants
	pushConstantRange := VkPushConstantRange{
		StageFlags: VK_SHADER_STAGE_COMPUTE_BIT,
		Offset:     0,
		Size:       pushConstantSize,
	}

	pipelineLayoutInfo := VkPipelineLayoutCreateInfo{
		SType:                  VK_STRUCTURE_TYPE_PIPELINE_LAYOUT_CREATE_INFO,
		SetLayoutCount:         1,
		PSetLayouts:            &descriptorLayout,
		PushConstantRangeCount: 1,
		PPushConstantRanges:    &pushConstantRange,
	}

	var pipelineLayout VkPipelineLayout
	if result := vkCreatePipelineLayout(d.device, &pipelineLayoutInfo, 0, &pipelineLayout); result != VK_SUCCESS {
		vkDestroyDescriptorSetLayout(d.device, descriptorLayout, 0)
		vkDestroyShaderModule(d.device, shaderModule, 0)
		return nil, fmt.Errorf("failed to create pipeline layout: %d", result)
	}

	// Create compute pipeline
	mainName := []byte("main\x00")
	pipelineInfo := VkComputePipelineCreateInfo{
		SType: VK_STRUCTURE_TYPE_COMPUTE_PIPELINE_CREATE_INFO,
		Stage: VkPipelineShaderStageCreateInfo{
			SType:  VK_STRUCTURE_TYPE_PIPELINE_SHADER_STAGE_CREATE_INFO,
			Stage:  VK_SHADER_STAGE_COMPUTE_BIT,
			Module: shaderModule,
			PName:  uintptr(unsafe.Pointer(&mainName[0])),
		},
		Layout: pipelineLayout,
	}

	var pipeline VkPipeline
	if result := vkCreateComputePipelines(d.device, 0, 1, &pipelineInfo, 0, &pipeline); result != VK_SUCCESS {
		vkDestroyPipelineLayout(d.device, pipelineLayout, 0)
		vkDestroyDescriptorSetLayout(d.device, descriptorLayout, 0)
		vkDestroyShaderModule(d.device, shaderModule, 0)
		return nil, fmt.Errorf("failed to create compute pipeline: %d", result)
	}

	// Create descriptor pool
	poolSize := VkDescriptorPoolSize{
		Type:            VK_DESCRIPTOR_TYPE_STORAGE_BUFFER,
		DescriptorCount: uint32(numBuffers * 16), // Allow multiple sets
	}

	poolInfo := VkDescriptorPoolCreateInfo{
		SType:         VK_STRUCTURE_TYPE_DESCRIPTOR_POOL_CREATE_INFO,
		MaxSets:       16,
		PoolSizeCount: 1,
		PPoolSizes:    &poolSize,
	}

	var descriptorPool VkDescriptorPool
	if result := vkCreateDescriptorPool(d.device, &poolInfo, 0, &descriptorPool); result != VK_SUCCESS {
		vkDestroyPipeline(d.device, pipeline, 0)
		vkDestroyPipelineLayout(d.device, pipelineLayout, 0)
		vkDestroyDescriptorSetLayout(d.device, descriptorLayout, 0)
		vkDestroyShaderModule(d.device, shaderModule, 0)
		return nil, fmt.Errorf("failed to create descriptor pool: %d", result)
	}

	return &ComputePipeline{
		device:           d,
		shaderModule:     shaderModule,
		descriptorLayout: descriptorLayout,
		pipelineLayout:   pipelineLayout,
		pipeline:         pipeline,
		descriptorPool:   descriptorPool,
		pushConstantSize: pushConstantSize,
	}, nil
}

// Release frees pipeline resources
func (p *ComputePipeline) Release() {
	if p.device == nil {
		return
	}
	p.device.mu.Lock()
	defer p.device.mu.Unlock()

	if p.descriptorPool != 0 {
		vkDestroyDescriptorPool(p.device.device, p.descriptorPool, 0)
	}
	if p.pipeline != 0 {
		vkDestroyPipeline(p.device.device, p.pipeline, 0)
	}
	if p.pipelineLayout != 0 {
		vkDestroyPipelineLayout(p.device.device, p.pipelineLayout, 0)
	}
	if p.descriptorLayout != 0 {
		vkDestroyDescriptorSetLayout(p.device.device, p.descriptorLayout, 0)
	}
	if p.shaderModule != 0 {
		vkDestroyShaderModule(p.device.device, p.shaderModule, 0)
	}
}

// Release frees all compute context resources
func (c *ComputeContext) Release() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, p := range c.pipelines {
		p.Release()
	}
	c.pipelines = nil
}

// dispatchCompute executes a compute shader
func (c *ComputeContext) dispatchCompute(pipelineType PipelineType, buffers []*Buffer, pushConstants []uint32, workgroupsX, workgroupsY, workgroupsZ uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	pipeline, ok := c.pipelines[pipelineType]
	if !ok {
		return errors.New("pipeline not found")
	}

	c.device.mu.Lock()
	defer c.device.mu.Unlock()

	// Reset descriptor pool to reclaim all descriptor sets
	// This allows us to reuse the pool without running out of sets
	if result := vkResetDescriptorPool(c.device.device, pipeline.descriptorPool, 0); result != VK_SUCCESS {
		return fmt.Errorf("failed to reset descriptor pool: %d", result)
	}

	// Allocate descriptor set
	allocInfo := VkDescriptorSetAllocateInfo{
		SType:              VK_STRUCTURE_TYPE_DESCRIPTOR_SET_ALLOCATE_INFO,
		DescriptorPool:     pipeline.descriptorPool,
		DescriptorSetCount: 1,
		PSetLayouts:        &pipeline.descriptorLayout,
	}

	var descriptorSet VkDescriptorSet
	if result := vkAllocateDescriptorSets(c.device.device, &allocInfo, &descriptorSet); result != VK_SUCCESS {
		return fmt.Errorf("failed to allocate descriptor set: %d", result)
	}

	// Update descriptor set with buffer bindings
	writes := make([]VkWriteDescriptorSet, len(buffers))
	bufferInfos := make([]VkDescriptorBufferInfo, len(buffers))

	for i, buf := range buffers {
		bufferInfos[i] = VkDescriptorBufferInfo{
			Buffer: buf.buffer,
			Offset: 0,
			Range:  VkDeviceSize(buf.size),
		}
		writes[i] = VkWriteDescriptorSet{
			SType:           VK_STRUCTURE_TYPE_WRITE_DESCRIPTOR_SET,
			DstSet:          descriptorSet,
			DstBinding:      uint32(i),
			DescriptorCount: 1,
			DescriptorType:  VK_DESCRIPTOR_TYPE_STORAGE_BUFFER,
			PBufferInfo:     &bufferInfos[i],
		}
	}

	vkUpdateDescriptorSets(c.device.device, uint32(len(writes)), &writes[0], 0, 0)

	// Allocate command buffer
	cmdAllocInfo := VkCommandBufferAllocateInfo{
		SType:              VK_STRUCTURE_TYPE_COMMAND_BUFFER_ALLOCATE_INFO,
		CommandPool:        c.device.commandPool,
		Level:              VK_COMMAND_BUFFER_LEVEL_PRIMARY,
		CommandBufferCount: 1,
	}

	var commandBuffer VkCommandBuffer
	if result := vkAllocateCommandBuffers(c.device.device, &cmdAllocInfo, &commandBuffer); result != VK_SUCCESS {
		return fmt.Errorf("failed to allocate command buffer: %d", result)
	}
	defer vkFreeCommandBuffers(c.device.device, c.device.commandPool, 1, &commandBuffer)

	// Begin command buffer
	beginInfo := VkCommandBufferBeginInfo{
		SType: VK_STRUCTURE_TYPE_COMMAND_BUFFER_BEGIN_INFO,
		Flags: VK_COMMAND_BUFFER_USAGE_ONE_TIME_SUBMIT_BIT,
	}

	if result := vkBeginCommandBuffer(commandBuffer, &beginInfo); result != VK_SUCCESS {
		return fmt.Errorf("failed to begin command buffer: %d", result)
	}

	// Bind pipeline
	vkCmdBindPipeline(commandBuffer, VK_PIPELINE_BIND_POINT_COMPUTE, pipeline.pipeline)

	// Bind descriptor set
	vkCmdBindDescriptorSets(commandBuffer, VK_PIPELINE_BIND_POINT_COMPUTE, pipeline.pipelineLayout, 0, 1, &descriptorSet, 0, 0)

	// Push constants
	if len(pushConstants) > 0 {
		vkCmdPushConstants(commandBuffer, pipeline.pipelineLayout, VK_SHADER_STAGE_COMPUTE_BIT, 0, uint32(len(pushConstants)*4), uintptr(unsafe.Pointer(&pushConstants[0])))
	}

	// Dispatch compute
	vkCmdDispatch(commandBuffer, workgroupsX, workgroupsY, workgroupsZ)

	// End command buffer
	if result := vkEndCommandBuffer(commandBuffer); result != VK_SUCCESS {
		return fmt.Errorf("failed to end command buffer: %d", result)
	}

	// Submit to queue
	submitInfo := VkSubmitInfo{
		SType:              VK_STRUCTURE_TYPE_SUBMIT_INFO,
		CommandBufferCount: 1,
		PCommandBuffers:    &commandBuffer,
	}

	if result := vkQueueSubmit(c.device.computeQueue, 1, &submitInfo, 0); result != VK_SUCCESS {
		return fmt.Errorf("failed to submit to queue: %d", result)
	}

	// Wait for completion
	if result := vkQueueWaitIdle(c.device.computeQueue); result != VK_SUCCESS {
		return fmt.Errorf("failed to wait for queue: %d", result)
	}

	return nil
}

// CosineSimilarityGPU computes cosine similarity using GPU compute shader
func (c *ComputeContext) CosineSimilarityGPU(embeddings, query, scores *Buffer, n, dimensions uint32, normalized bool) error {
	normalizedFlag := uint32(0)
	if normalized {
		normalizedFlag = 1
	}

	pushConstants := []uint32{n, dimensions, normalizedFlag}

	// Each workgroup processes 256 embeddings
	workgroups := (n + 255) / 256

	return c.dispatchCompute(PipelineCosineSimilarity, []*Buffer{embeddings, query, scores}, pushConstants, workgroups, 1, 1)
}

// NormalizeVectorsGPU normalizes vectors using GPU compute shader
func (c *ComputeContext) NormalizeVectorsGPU(vectors *Buffer, n, dimensions uint32) error {
	pushConstants := []uint32{n, dimensions}

	// Each workgroup normalizes one vector
	return c.dispatchCompute(PipelineNormalize, []*Buffer{vectors}, pushConstants, n, 1, 1)
}

// TopKGPU finds top-k scores using GPU compute shader.
// Uses an iterative reduction algorithm that works for any k value.
// Performance is optimal for k <= 64 (typical for similarity search).
// For larger k, the algorithm still works but may be slower (O(k×n) complexity).
func (c *ComputeContext) TopKGPU(scores, topIndices, topScores *Buffer, n, k uint32) error {
	pushConstants := []uint32{n, k}

	// Single workgroup with 256 threads - each thread strides through all n elements
	// This handles any n value efficiently via parallel striding
	return c.dispatchCompute(PipelineTopK, []*Buffer{scores, topIndices, topScores}, pushConstants, 1, 1, 1)
}

// SearchGPU performs complete similarity search on GPU
func (c *ComputeContext) SearchGPU(embeddings *Buffer, query []float32, n, dimensions uint32, k int, normalized bool) ([]SearchResult, error) {
	if k <= 0 {
		return nil, nil
	}
	if k > int(n) {
		k = int(n)
	}

	// Create query buffer
	queryBuf, err := c.device.NewBuffer(query)
	if err != nil {
		return nil, err
	}
	defer queryBuf.Release()

	// Create scores buffer
	scoresBuf, err := c.device.NewEmptyBuffer(uint64(n))
	if err != nil {
		return nil, err
	}
	defer scoresBuf.Release()

	// Create top-k output buffers
	topIndicesBuf, err := c.device.NewEmptyBuffer(uint64(k))
	if err != nil {
		return nil, err
	}
	defer topIndicesBuf.Release()

	topScoresBuf, err := c.device.NewEmptyBuffer(uint64(k))
	if err != nil {
		return nil, err
	}
	defer topScoresBuf.Release()

	// Run cosine similarity on GPU
	if err := c.CosineSimilarityGPU(embeddings, queryBuf, scoresBuf, n, dimensions, normalized); err != nil {
		return nil, fmt.Errorf("cosine similarity failed: %w", err)
	}

	// Run top-k on GPU
	if err := c.TopKGPU(scoresBuf, topIndicesBuf, topScoresBuf, n, uint32(k)); err != nil {
		return nil, fmt.Errorf("top-k failed: %w", err)
	}

	// Read results back - indices are uint32, scores are float32
	indices := topIndicesBuf.ReadUint32(k)
	scores := topScoresBuf.ReadFloat32(k)

	if indices == nil || scores == nil {
		return nil, errors.New("failed to read results")
	}

	results := make([]SearchResult, k)
	for i := 0; i < k; i++ {
		results[i] = SearchResult{
			Index: indices[i],
			Score: scores[i],
		}
	}

	return results, nil
}
