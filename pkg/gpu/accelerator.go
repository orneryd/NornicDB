// Package gpu provides GPU acceleration for NornicDB vector operations.
// This file provides the high-level accelerator that integrates with EmbeddingIndex.
package gpu

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/orneryd/nornicdb/pkg/gpu/cuda"
	"github.com/orneryd/nornicdb/pkg/gpu/metal"
	"github.com/orneryd/nornicdb/pkg/gpu/opencl"
	"github.com/orneryd/nornicdb/pkg/gpu/vulkan"
)

// Accelerator provides GPU-accelerated vector operations.
// It automatically selects the best available backend (Metal on macOS,
// OpenCL/CUDA on other platforms).
//
// Usage:
//
//	accel, err := gpu.NewAccelerator(nil)
//	if err != nil {
//		// Fall back to CPU
//	}
//	defer accel.Release()
//
//	// Create GPU-backed embedding index
//	index := accel.NewEmbeddingIndex(1024)
//	index.Add("doc-1", embedding1)
//	index.SyncToGPU()
//	results, _ := index.Search(query, 10)
type Accelerator struct {
	backend Backend
	config  *Config

	// Metal-specific (macOS)
	metalDevice *metal.Device

	// CUDA-specific (NVIDIA)
	cudaDevice *cuda.Device

	// OpenCL-specific (AMD, Intel, cross-platform)
	openclDevice *opencl.Device

	// Vulkan-specific (cross-platform compute)
	vulkanDevice *vulkan.Device

	// Stats
	mu    sync.RWMutex
	stats AcceleratorStats
}

// AcceleratorStats tracks GPU usage statistics.
type AcceleratorStats struct {
	SearchesGPU      int64
	SearchesCPU      int64
	BytesUploaded    int64
	BytesDownloaded  int64
	KernelExecutions int64
}

// NewAccelerator creates a new GPU accelerator with auto-detection.
//
// The accelerator automatically detects and initializes the best available
// GPU backend for the current platform:
//   - macOS: Metal (Apple Silicon optimized)
//   - Linux/Windows: OpenCL or CUDA (when implemented)
//
// If no GPU is available and config.FallbackOnError is true (default),
// the accelerator runs in CPU-only mode.
func NewAccelerator(config *Config) (*Accelerator, error) {
	if config == nil {
		config = DefaultConfig()
	}

	accel := &Accelerator{
		config:  config,
		backend: BackendNone,
	}

	if !config.Enabled {
		return accel, nil
	}

	// Try to initialize GPU backend
	if err := accel.initBackend(config.PreferredBackend); err != nil {
		if config.FallbackOnError {
			// Fall back to CPU mode
			return accel, nil
		}
		return nil, err
	}

	return accel, nil
}

// initBackend initializes the appropriate GPU backend.
func (a *Accelerator) initBackend(preferred Backend) error {
	// Determine backends to try
	var backends []Backend

	if preferred != BackendNone {
		backends = append(backends, preferred)
	}

	// Auto-detect based on platform
	switch runtime.GOOS {
	case "darwin":
		backends = append(backends, BackendMetal)
	case "linux", "windows":
		backends = append(backends, BackendOpenCL, BackendCUDA, BackendVulkan)
	}

	// Try each backend
	for _, backend := range backends {
		if err := a.tryBackend(backend); err == nil {
			return nil
		}
	}

	return ErrGPUNotAvailable
}

// tryBackend attempts to initialize a specific backend.
func (a *Accelerator) tryBackend(backend Backend) error {
	switch backend {
	case BackendMetal:
		return a.initMetal()
	case BackendOpenCL:
		return a.initOpenCL()
	case BackendCUDA:
		return a.initCUDA()
	case BackendVulkan:
		return a.initVulkan()
	default:
		return ErrGPUNotAvailable
	}
}

// initMetal initializes the Metal backend (macOS only).
func (a *Accelerator) initMetal() error {
	if !metal.IsAvailable() {
		return ErrGPUNotAvailable
	}

	device, err := metal.NewDevice()
	if err != nil {
		return err
	}

	a.metalDevice = device
	a.backend = BackendMetal
	return nil
}

// initOpenCL initializes the OpenCL backend.
func (a *Accelerator) initOpenCL() error {
	if !opencl.IsAvailable() {
		return ErrGPUNotAvailable
	}

	device, err := opencl.NewDevice(0)
	if err != nil {
		return err
	}

	a.openclDevice = device
	a.backend = BackendOpenCL
	return nil
}

// initCUDA initializes the CUDA backend.
func (a *Accelerator) initCUDA() error {
	if !cuda.IsAvailable() {
		return ErrGPUNotAvailable
	}

	device, err := cuda.NewDevice(0)
	if err != nil {
		return err
	}

	a.cudaDevice = device
	a.backend = BackendCUDA
	return nil
}

// initVulkan initializes the Vulkan compute backend.
func (a *Accelerator) initVulkan() error {
	if !vulkan.IsAvailable() {
		return ErrGPUNotAvailable
	}

	device, err := vulkan.NewDevice(0)
	if err != nil {
		return err
	}

	a.vulkanDevice = device
	a.backend = BackendVulkan
	return nil
}

// Release frees all GPU resources.
func (a *Accelerator) Release() {
	if a.metalDevice != nil {
		a.metalDevice.Release()
		a.metalDevice = nil
	}
	if a.cudaDevice != nil {
		a.cudaDevice.Release()
		a.cudaDevice = nil
	}
	if a.openclDevice != nil {
		a.openclDevice.Release()
		a.openclDevice = nil
	}
	if a.vulkanDevice != nil {
		a.vulkanDevice.Release()
		a.vulkanDevice = nil
	}
	a.backend = BackendNone
}

// IsEnabled returns whether GPU acceleration is active.
func (a *Accelerator) IsEnabled() bool {
	return a.backend != BackendNone
}

// Backend returns the active GPU backend.
func (a *Accelerator) Backend() Backend {
	return a.backend
}

// DeviceName returns the GPU device name.
func (a *Accelerator) DeviceName() string {
	switch a.backend {
	case BackendMetal:
		if a.metalDevice != nil {
			return a.metalDevice.Name()
		}
	case BackendCUDA:
		if a.cudaDevice != nil {
			return a.cudaDevice.Name()
		}
	case BackendOpenCL:
		if a.openclDevice != nil {
			return a.openclDevice.Name()
		}
	case BackendVulkan:
		if a.vulkanDevice != nil {
			return a.vulkanDevice.Name()
		}
	}
	return "CPU"
}

// DeviceMemoryMB returns the GPU memory in megabytes.
func (a *Accelerator) DeviceMemoryMB() int {
	switch a.backend {
	case BackendMetal:
		if a.metalDevice != nil {
			return a.metalDevice.MemoryMB()
		}
	case BackendCUDA:
		if a.cudaDevice != nil {
			return a.cudaDevice.MemoryMB()
		}
	case BackendOpenCL:
		if a.openclDevice != nil {
			return a.openclDevice.MemoryMB()
		}
	case BackendVulkan:
		if a.vulkanDevice != nil {
			return a.vulkanDevice.MemoryMB()
		}
	}
	return 0
}

// Stats returns GPU usage statistics.
func (a *Accelerator) Stats() AcceleratorStats {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.stats
}

// GPUEmbeddingIndex provides GPU-accelerated embedding storage and search.
type GPUEmbeddingIndex struct {
	accel      *Accelerator
	dimensions int

	// CPU-side data
	nodeIDs   []string
	idToIndex map[string]int
	cpuData   []float32 // Flat array: [vec0..., vec1..., vec2...]

	// GPU-side data (Metal)
	metalBuffer *metal.Buffer

	// GPU-side data (CUDA)
	cudaBuffer *cuda.Buffer

	// GPU-side data (OpenCL)
	openclBuffer *opencl.Buffer

	// GPU-side data (Vulkan)
	vulkanBuffer *vulkan.Buffer

	gpuSynced bool

	// Stats
	searchesGPU int64
	searchesCPU int64

	mu sync.RWMutex
}

// NewGPUEmbeddingIndex creates a new GPU-accelerated embedding index.
func (a *Accelerator) NewGPUEmbeddingIndex(dimensions int) *GPUEmbeddingIndex {
	return &GPUEmbeddingIndex{
		accel:      a,
		dimensions: dimensions,
		nodeIDs:    make([]string, 0, 10000),
		idToIndex:  make(map[string]int, 10000),
		cpuData:    make([]float32, 0, 10000*dimensions),
	}
}

// Add inserts or updates an embedding.
func (idx *GPUEmbeddingIndex) Add(nodeID string, embedding []float32) error {
	if len(embedding) != idx.dimensions {
		return ErrInvalidDimensions
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	if i, exists := idx.idToIndex[nodeID]; exists {
		// Update existing
		copy(idx.cpuData[i*idx.dimensions:], embedding)
	} else {
		// Add new
		idx.nodeIDs = append(idx.nodeIDs, nodeID)
		idx.idToIndex[nodeID] = len(idx.nodeIDs) - 1
		idx.cpuData = append(idx.cpuData, embedding...)
	}

	idx.gpuSynced = false
	return nil
}

// AddBatch adds multiple embeddings efficiently.
func (idx *GPUEmbeddingIndex) AddBatch(nodeIDs []string, embeddings [][]float32) error {
	if len(nodeIDs) != len(embeddings) {
		return fmt.Errorf("nodeIDs and embeddings length mismatch")
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	for i, nodeID := range nodeIDs {
		if len(embeddings[i]) != idx.dimensions {
			return ErrInvalidDimensions
		}

		if j, exists := idx.idToIndex[nodeID]; exists {
			copy(idx.cpuData[j*idx.dimensions:], embeddings[i])
		} else {
			idx.nodeIDs = append(idx.nodeIDs, nodeID)
			idx.idToIndex[nodeID] = len(idx.nodeIDs) - 1
			idx.cpuData = append(idx.cpuData, embeddings[i]...)
		}
	}

	idx.gpuSynced = false
	return nil
}

// Remove deletes an embedding by nodeID.
func (idx *GPUEmbeddingIndex) Remove(nodeID string) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	i, exists := idx.idToIndex[nodeID]
	if !exists {
		return false
	}

	// Swap with last
	lastIdx := len(idx.nodeIDs) - 1
	if i != lastIdx {
		lastNodeID := idx.nodeIDs[lastIdx]
		idx.nodeIDs[i] = lastNodeID
		idx.idToIndex[lastNodeID] = i

		srcStart := lastIdx * idx.dimensions
		dstStart := i * idx.dimensions
		copy(idx.cpuData[dstStart:dstStart+idx.dimensions],
			idx.cpuData[srcStart:srcStart+idx.dimensions])
	}

	idx.nodeIDs[lastIdx] = ""
	idx.nodeIDs = idx.nodeIDs[:lastIdx]
	idx.cpuData = idx.cpuData[:lastIdx*idx.dimensions]
	delete(idx.idToIndex, nodeID)

	idx.gpuSynced = false
	return true
}

// SyncToGPU uploads embeddings to GPU memory.
func (idx *GPUEmbeddingIndex) SyncToGPU() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.accel.IsEnabled() {
		return ErrGPUDisabled
	}

	if len(idx.cpuData) == 0 {
		idx.gpuSynced = true
		return nil
	}

	switch idx.accel.backend {
	case BackendMetal:
		return idx.syncToMetal()
	case BackendCUDA:
		return idx.syncToCUDA()
	case BackendOpenCL:
		return idx.syncToOpenCL()
	case BackendVulkan:
		return idx.syncToVulkan()
	default:
		return ErrGPUNotAvailable
	}
}

// syncToMetal uploads data to Metal GPU buffer.
func (idx *GPUEmbeddingIndex) syncToMetal() error {
	// Release old buffer
	if idx.metalBuffer != nil {
		idx.metalBuffer.Release()
		idx.metalBuffer = nil
	}

	// Create new buffer with embeddings
	buffer, err := idx.accel.metalDevice.NewBuffer(idx.cpuData, metal.StorageShared)
	if err != nil {
		return err
	}

	idx.metalBuffer = buffer
	idx.gpuSynced = true

	// Update stats
	idx.accel.mu.Lock()
	idx.accel.stats.BytesUploaded += int64(len(idx.cpuData) * 4)
	idx.accel.mu.Unlock()

	return nil
}

// syncToCUDA uploads data to CUDA GPU buffer.
func (idx *GPUEmbeddingIndex) syncToCUDA() error {
	// Release old buffer
	if idx.cudaBuffer != nil {
		idx.cudaBuffer.Release()
		idx.cudaBuffer = nil
	}

	// Create new buffer with embeddings
	buffer, err := idx.accel.cudaDevice.NewBuffer(idx.cpuData, cuda.MemoryDevice)
	if err != nil {
		return err
	}

	idx.cudaBuffer = buffer
	idx.gpuSynced = true

	// Update stats
	idx.accel.mu.Lock()
	idx.accel.stats.BytesUploaded += int64(len(idx.cpuData) * 4)
	idx.accel.mu.Unlock()

	return nil
}

// syncToOpenCL uploads data to OpenCL GPU buffer.
func (idx *GPUEmbeddingIndex) syncToOpenCL() error {
	// Release old buffer
	if idx.openclBuffer != nil {
		idx.openclBuffer.Release()
		idx.openclBuffer = nil
	}

	// Create new buffer with embeddings
	buffer, err := idx.accel.openclDevice.NewBuffer(idx.cpuData)
	if err != nil {
		return err
	}

	idx.openclBuffer = buffer
	idx.gpuSynced = true

	// Update stats
	idx.accel.mu.Lock()
	idx.accel.stats.BytesUploaded += int64(len(idx.cpuData) * 4)
	idx.accel.mu.Unlock()

	return nil
}

// syncToVulkan uploads data to Vulkan GPU buffer.
func (idx *GPUEmbeddingIndex) syncToVulkan() error {
	// Release old buffer
	if idx.vulkanBuffer != nil {
		idx.vulkanBuffer.Release()
		idx.vulkanBuffer = nil
	}

	// Create new buffer with embeddings
	buffer, err := idx.accel.vulkanDevice.NewBuffer(idx.cpuData)
	if err != nil {
		return err
	}

	idx.vulkanBuffer = buffer
	idx.gpuSynced = true

	// Update stats
	idx.accel.mu.Lock()
	idx.accel.stats.BytesUploaded += int64(len(idx.cpuData) * 4)
	idx.accel.mu.Unlock()

	return nil
}

// Search finds the k most similar embeddings.
func (idx *GPUEmbeddingIndex) Search(query []float32, k int) ([]SearchResult, error) {
	if len(query) != idx.dimensions {
		return nil, ErrInvalidDimensions
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	n := len(idx.nodeIDs)
	if n == 0 {
		return nil, nil
	}
	if k > n {
		k = n
	}

	// Use GPU if available and synced
	if idx.accel.IsEnabled() && idx.gpuSynced {
		return idx.searchGPU(query, k)
	}

	return idx.searchCPU(query, k)
}

// searchGPU performs GPU-accelerated search.
func (idx *GPUEmbeddingIndex) searchGPU(query []float32, k int) ([]SearchResult, error) {
	idx.searchesGPU++

	switch idx.accel.backend {
	case BackendMetal:
		return idx.searchMetal(query, k)
	case BackendCUDA:
		return idx.searchCUDA(query, k)
	case BackendOpenCL:
		return idx.searchOpenCL(query, k)
	case BackendVulkan:
		return idx.searchVulkan(query, k)
	default:
		// Fallback to CPU
		return idx.searchCPU(query, k)
	}
}

// searchMetal performs search using Metal GPU.
func (idx *GPUEmbeddingIndex) searchMetal(query []float32, k int) ([]SearchResult, error) {
	if idx.metalBuffer == nil {
		return idx.searchCPU(query, k)
	}

	n := uint32(len(idx.nodeIDs))

	results, err := idx.accel.metalDevice.Search(
		idx.metalBuffer,
		query,
		n,
		uint32(idx.dimensions),
		k,
		true, // normalized
	)

	if err != nil {
		// Fallback to CPU on GPU error
		return idx.searchCPU(query, k)
	}

	// Update stats
	idx.accel.mu.Lock()
	idx.accel.stats.SearchesGPU++
	idx.accel.stats.KernelExecutions += 2 // similarity + topk
	idx.accel.mu.Unlock()

	// Convert to SearchResult with nodeIDs
	output := make([]SearchResult, len(results))
	for i, r := range results {
		if int(r.Index) < len(idx.nodeIDs) {
			output[i] = SearchResult{
				ID:       idx.nodeIDs[r.Index],
				Score:    r.Score,
				Distance: 1 - r.Score,
			}
		}
	}

	return output, nil
}

// searchCUDA performs search using CUDA GPU.
func (idx *GPUEmbeddingIndex) searchCUDA(query []float32, k int) ([]SearchResult, error) {
	if idx.cudaBuffer == nil {
		return idx.searchCPU(query, k)
	}

	n := uint32(len(idx.nodeIDs))

	results, err := idx.accel.cudaDevice.Search(
		idx.cudaBuffer,
		query,
		n,
		uint32(idx.dimensions),
		k,
		true, // normalized
	)

	if err != nil {
		// Fallback to CPU on GPU error
		return idx.searchCPU(query, k)
	}

	// Update stats
	idx.accel.mu.Lock()
	idx.accel.stats.SearchesGPU++
	idx.accel.stats.KernelExecutions += 2 // similarity + topk
	idx.accel.mu.Unlock()

	// Convert to SearchResult with nodeIDs
	output := make([]SearchResult, len(results))
	for i, r := range results {
		if int(r.Index) < len(idx.nodeIDs) {
			output[i] = SearchResult{
				ID:       idx.nodeIDs[r.Index],
				Score:    r.Score,
				Distance: 1 - r.Score,
			}
		}
	}

	return output, nil
}

// searchOpenCL performs search using OpenCL GPU.
func (idx *GPUEmbeddingIndex) searchOpenCL(query []float32, k int) ([]SearchResult, error) {
	if idx.openclBuffer == nil {
		return idx.searchCPU(query, k)
	}

	n := uint32(len(idx.nodeIDs))

	results, err := idx.accel.openclDevice.Search(
		idx.openclBuffer,
		query,
		n,
		uint32(idx.dimensions),
		k,
		true, // normalized
	)

	if err != nil {
		// Fallback to CPU on GPU error
		return idx.searchCPU(query, k)
	}

	// Update stats
	idx.accel.mu.Lock()
	idx.accel.stats.SearchesGPU++
	idx.accel.stats.KernelExecutions += 2 // similarity + topk
	idx.accel.mu.Unlock()

	// Convert to SearchResult with nodeIDs
	output := make([]SearchResult, len(results))
	for i, r := range results {
		if int(r.Index) < len(idx.nodeIDs) {
			output[i] = SearchResult{
				ID:       idx.nodeIDs[r.Index],
				Score:    r.Score,
				Distance: 1 - r.Score,
			}
		}
	}

	return output, nil
}

// searchVulkan performs search using Vulkan compute GPU.
func (idx *GPUEmbeddingIndex) searchVulkan(query []float32, k int) ([]SearchResult, error) {
	if idx.vulkanBuffer == nil {
		return idx.searchCPU(query, k)
	}

	n := uint32(len(idx.nodeIDs))

	results, err := idx.accel.vulkanDevice.Search(
		idx.vulkanBuffer,
		query,
		n,
		uint32(idx.dimensions),
		k,
		true, // normalized
	)

	if err != nil {
		// Fallback to CPU on GPU error
		return idx.searchCPU(query, k)
	}

	// Update stats
	idx.accel.mu.Lock()
	idx.accel.stats.SearchesGPU++
	idx.accel.stats.KernelExecutions += 2 // similarity + topk
	idx.accel.mu.Unlock()

	// Convert to SearchResult with nodeIDs
	output := make([]SearchResult, len(results))
	for i, r := range results {
		if int(r.Index) < len(idx.nodeIDs) {
			output[i] = SearchResult{
				ID:       idx.nodeIDs[r.Index],
				Score:    r.Score,
				Distance: 1 - r.Score,
			}
		}
	}

	return output, nil
}

// searchCPU performs CPU-based search (fallback).
func (idx *GPUEmbeddingIndex) searchCPU(query []float32, k int) ([]SearchResult, error) {
	idx.searchesCPU++

	idx.accel.mu.Lock()
	idx.accel.stats.SearchesCPU++
	idx.accel.mu.Unlock()

	n := len(idx.nodeIDs)

	// Compute all similarities
	scores := make([]float32, n)
	for i := 0; i < n; i++ {
		start := i * idx.dimensions
		end := start + idx.dimensions
		scores[i] = cosineSimilarityFlat(query, idx.cpuData[start:end])
	}

	// Find top-k
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}
	partialSort(indices, scores, k)

	// Build results
	results := make([]SearchResult, k)
	for i := 0; i < k; i++ {
		vecIdx := indices[i]
		results[i] = SearchResult{
			ID:       idx.nodeIDs[vecIdx],
			Score:    scores[vecIdx],
			Distance: 1 - scores[vecIdx],
		}
	}

	return results, nil
}

// Count returns the number of embeddings.
func (idx *GPUEmbeddingIndex) Count() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.nodeIDs)
}

// IsGPUSynced returns whether GPU buffer is up-to-date.
func (idx *GPUEmbeddingIndex) IsGPUSynced() bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.gpuSynced
}

// Release frees GPU resources.
func (idx *GPUEmbeddingIndex) Release() {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.metalBuffer != nil {
		idx.metalBuffer.Release()
		idx.metalBuffer = nil
	}
	if idx.cudaBuffer != nil {
		idx.cudaBuffer.Release()
		idx.cudaBuffer = nil
	}
	if idx.openclBuffer != nil {
		idx.openclBuffer.Release()
		idx.openclBuffer = nil
	}
	if idx.vulkanBuffer != nil {
		idx.vulkanBuffer.Release()
		idx.vulkanBuffer = nil
	}
}

// GPUEmbeddingIndexStats holds index statistics.
type GPUEmbeddingIndexStats struct {
	Count       int
	Dimensions  int
	GPUSynced   bool
	SearchesGPU int64
	SearchesCPU int64
	MemoryMB    float64
}

// Stats returns index statistics.
func (idx *GPUEmbeddingIndex) Stats() GPUEmbeddingIndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	bytesPerEmbed := idx.dimensions*4 + 32 // float32 + string overhead
	totalBytes := len(idx.nodeIDs) * bytesPerEmbed

	return GPUEmbeddingIndexStats{
		Count:       len(idx.nodeIDs),
		Dimensions:  idx.dimensions,
		GPUSynced:   idx.gpuSynced,
		SearchesGPU: idx.searchesGPU,
		SearchesCPU: idx.searchesCPU,
		MemoryMB:    float64(totalBytes) / (1024 * 1024),
	}
}
