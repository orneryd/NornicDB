//go:build !opencl
// +build !opencl

// Package opencl provides cross-platform GPU acceleration using OpenCL.
// This is a stub implementation for systems without OpenCL support.
package opencl

import (
	"errors"
)

// Errors
var (
	ErrOpenCLNotAvailable = errors.New("opencl: OpenCL is not available (build without opencl tag)")
	ErrDeviceCreation     = errors.New("opencl: failed to create OpenCL device")
	ErrBufferCreation     = errors.New("opencl: failed to create buffer")
	ErrKernelExecution    = errors.New("opencl: kernel execution failed")
	ErrInvalidBuffer      = errors.New("opencl: invalid buffer")
)

// Device represents an OpenCL GPU device (stub).
type Device struct{}

// Buffer represents an OpenCL memory buffer (stub).
type Buffer struct{}

// SearchResult holds a similarity search result.
type SearchResult struct {
	Index uint32
	Score float32
}

// IsAvailable returns false on systems without OpenCL.
func IsAvailable() bool {
	return false
}

// DeviceCount returns 0 on systems without OpenCL.
func DeviceCount() int {
	return 0
}

// NewDevice returns an error on systems without OpenCL.
func NewDevice(deviceID int) (*Device, error) {
	return nil, ErrOpenCLNotAvailable
}

// Release is a no-op stub.
func (d *Device) Release() {}

// ID returns 0.
func (d *Device) ID() int { return 0 }

// Name returns empty string.
func (d *Device) Name() string { return "" }

// Vendor returns empty string.
func (d *Device) Vendor() string { return "" }

// MemoryBytes returns 0.
func (d *Device) MemoryBytes() uint64 { return 0 }

// MemoryMB returns 0.
func (d *Device) MemoryMB() int { return 0 }

// NewBuffer returns an error.
func (d *Device) NewBuffer(data []float32) (*Buffer, error) {
	return nil, ErrOpenCLNotAvailable
}

// NewEmptyBuffer returns an error.
func (d *Device) NewEmptyBuffer(count uint64) (*Buffer, error) {
	return nil, ErrOpenCLNotAvailable
}

// Release is a no-op stub.
func (b *Buffer) Release() {}

// Size returns 0.
func (b *Buffer) Size() uint64 { return 0 }

// ReadFloat32 returns nil.
func (b *Buffer) ReadFloat32(count int) []float32 { return nil }

// NormalizeVectors returns an error.
func (d *Device) NormalizeVectors(vectors *Buffer, n, dimensions uint32) error {
	return ErrOpenCLNotAvailable
}

// CosineSimilarity returns an error.
func (d *Device) CosineSimilarity(embeddings, query, scores *Buffer, n, dimensions uint32, normalized bool) error {
	return ErrOpenCLNotAvailable
}

// TopK returns an error.
func (d *Device) TopK(scores *Buffer, n, k uint32) ([]uint32, []float32, error) {
	return nil, nil, ErrOpenCLNotAvailable
}

// Search returns an error.
func (d *Device) Search(embeddings *Buffer, query []float32, n, dimensions uint32, k int, normalized bool) ([]SearchResult, error) {
	return nil, ErrOpenCLNotAvailable
}
