//go:build !darwin
// +build !darwin

// Package metal provides Metal GPU acceleration for macOS and Apple Silicon.
// This file provides stubs for non-Darwin systems.
package metal

import "errors"

// Errors
var (
	ErrMetalNotAvailable = errors.New("metal: Metal is only available on macOS")
	ErrDeviceCreation    = errors.New("metal: failed to create Metal device")
	ErrBufferCreation    = errors.New("metal: failed to create buffer")
	ErrKernelExecution   = errors.New("metal: kernel execution failed")
	ErrInvalidBuffer     = errors.New("metal: invalid buffer")
)

// StorageMode defines how buffer memory is managed.
type StorageMode int

const (
	StorageShared  StorageMode = 0
	StorageManaged StorageMode = 1
	StoragePrivate StorageMode = 2
)

// Device represents a Metal GPU device (stub for non-Darwin).
type Device struct{}

// Buffer represents a Metal GPU buffer (stub for non-Darwin).
type Buffer struct{}

// SearchResult holds a similarity search result.
type SearchResult struct {
	Index uint32
	Score float32
}

// IsAvailable checks if Metal is available (always false on non-Darwin).
func IsAvailable() bool {
	return false
}

// NewDevice creates a new Metal device (not available on non-Darwin).
func NewDevice() (*Device, error) {
	return nil, ErrMetalNotAvailable
}

// Release frees the Metal device resources.
func (d *Device) Release() {}

// Name returns the GPU device name.
func (d *Device) Name() string { return "" }

// MemoryBytes returns the GPU memory size in bytes.
func (d *Device) MemoryBytes() uint64 { return 0 }

// MemoryMB returns the GPU memory size in megabytes.
func (d *Device) MemoryMB() int { return 0 }

// NewBuffer creates a new GPU buffer with copied data.
func (d *Device) NewBuffer(data []float32, mode StorageMode) (*Buffer, error) {
	return nil, ErrMetalNotAvailable
}

// NewBufferNoCopy creates a GPU buffer that shares memory.
func (d *Device) NewBufferNoCopy(data []float32, mode StorageMode) (*Buffer, error) {
	return nil, ErrMetalNotAvailable
}

// NewEmptyBuffer creates an uninitialized GPU buffer.
func (d *Device) NewEmptyBuffer(sizeBytes uint64, mode StorageMode) (*Buffer, error) {
	return nil, ErrMetalNotAvailable
}

// Release frees the buffer resources.
func (b *Buffer) Release() {}

// Size returns the buffer size in bytes.
func (b *Buffer) Size() uint64 { return 0 }

// ReadFloat32 reads float32 values from the buffer (stub).
func (b *Buffer) ReadFloat32(count int) []float32 { return nil }

// Float32View returns the buffer's contents as a slice (stub).
func (b *Buffer) Float32View(count int) []float32 { return nil }

// WriteFloat32 writes float32 values into the buffer (stub).
func (b *Buffer) WriteFloat32(data []float32, offset int) error { return ErrMetalNotAvailable }

// ComputeCosineSimilarity computes cosine similarity (stub).
func (d *Device) ComputeCosineSimilarity(embeddings, query, scores *Buffer, n, dimensions uint32, normalized bool) error {
	return ErrMetalNotAvailable
}

// ComputeTopK finds the k highest scoring indices (stub).
func (d *Device) ComputeTopK(scores, indices, topkScores *Buffer, n, k uint32) error {
	return ErrMetalNotAvailable
}

// NormalizeVectors normalizes vectors in-place (stub).
func (d *Device) NormalizeVectors(vectors *Buffer, n, dimensions uint32) error {
	return ErrMetalNotAvailable
}

// Search performs a complete similarity search (stub).
func (d *Device) Search(embeddings *Buffer, query []float32, n, dimensions uint32, k int, normalized bool) ([]SearchResult, error) {
	return nil, ErrMetalNotAvailable
}

// HNSWBuildTopK computes batched HNSW construction candidates (stub).
func (d *Device) HNSWBuildTopK(frontier, queries *Buffer, frontierN, queryN, dimensions uint32, k int) ([]uint32, []float32, error) {
	return nil, nil, ErrMetalNotAvailable
}

// =============================================================================
// Memory Tracking (stubs)
// =============================================================================

// MemoryInfo contains GPU and system memory statistics.
type MemoryInfo struct {
	TotalMemory      uint64
	UsedMemory       uint64
	AvailableMemory  uint64
	GPURecommended   uint64
	CurrentAllocated uint64
}

// GetMemoryInfo returns current memory statistics (stub).
func (d *Device) GetMemoryInfo() MemoryInfo {
	return MemoryInfo{}
}

// DeviceCapabilities contains detailed GPU capabilities.
type DeviceCapabilities struct {
	Name                     string
	Architecture             string
	GPUFamily                int
	MaxThreadsPerThreadgroup int
	MaxBufferLength          uint64 // Can be very large (16GB+) on Apple Silicon
	IsLowPower               bool
	IsHeadless               bool
	HasUnifiedMemory         bool
	RegistryID               int
}

// GetCapabilities returns detailed device capabilities (stub).
func (d *Device) GetCapabilities() DeviceCapabilities {
	return DeviceCapabilities{}
}

// GetCapabilitiesNoDevice returns capabilities without creating a device (stub).
func GetCapabilitiesNoDevice() DeviceCapabilities {
	return DeviceCapabilities{}
}

// PrintDeviceInfo logs detailed Metal device information (stub).
func PrintDeviceInfo() {
	// No-op on non-Darwin
}

// =============================================================================
// Metal Performance Shaders (stubs)
// =============================================================================

// MPSIsSupported returns true if Metal Performance Shaders are available.
func MPSIsSupported() bool {
	return false
}

// MPSMatrixMultiply performs GPU-accelerated matrix multiplication (stub).
func (d *Device) MPSMatrixMultiply(a, b, c *Buffer, m, n, k uint32, alpha, beta float32) error {
	return ErrMetalNotAvailable
}

// MPSMatrixMultiplyTransposeB computes C = A × Bᵀ (stub).
func (d *Device) MPSMatrixMultiplyTransposeB(a, b, c *Buffer, m, n, k uint32) error {
	return ErrMetalNotAvailable
}

// MPSMatrixVectorMultiply performs GPU-accelerated matrix-vector multiplication (stub).
func (d *Device) MPSMatrixVectorMultiply(a, x, y *Buffer, m, n uint32, alpha, beta float32) error {
	return ErrMetalNotAvailable
}

// MPSBatchCosineSimilarity computes cosine similarities using MPS (stub).
func (d *Device) MPSBatchCosineSimilarity(embeddings, query, scores *Buffer, n, dims uint32) error {
	return ErrMetalNotAvailable
}
