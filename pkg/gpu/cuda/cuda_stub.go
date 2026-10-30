//go:build !cuda || !(linux || windows)
// +build !cuda !linux,!windows

// Package cuda provides NVIDIA GPU acceleration using CUDA.
// This is a stub implementation for systems without CUDA support.
// It includes runtime detection via nvidia-smi for informational purposes.
package cuda

import (
	"errors"
	"fmt"
	"os/exec"
	"runtime"
	"strings"
	"sync"
)

// Errors
var (
	ErrCUDANotAvailable = errors.New("cuda: CUDA is not available (build without cuda tag or unsupported platform)")
	ErrDeviceCreation   = errors.New("cuda: failed to create CUDA device")
	ErrBufferCreation   = errors.New("cuda: failed to create buffer")
	ErrKernelExecution  = errors.New("cuda: kernel execution failed")
	ErrInvalidBuffer    = errors.New("cuda: invalid buffer")
)

// Runtime GPU detection cache
var (
	gpuDetected     bool
	gpuDetectedOnce sync.Once
	gpuName         string
	gpuMemoryMB     int
)

// detectGPURuntime checks for NVIDIA GPU using nvidia-smi (no CUDA required)
func detectGPURuntime() {
	gpuDetectedOnce.Do(func() {
		if runtime.GOOS != "linux" && runtime.GOOS != "windows" {
			return
		}

		// Try nvidia-smi to detect GPU
		cmd := exec.Command("nvidia-smi", "--query-gpu=name,memory.total", "--format=csv,noheader,nounits")
		output, err := cmd.Output()
		if err != nil {
			return
		}

		lines := strings.TrimSpace(string(output))
		if lines == "" {
			return
		}

		// Parse first GPU: "NVIDIA GeForce RTX 3080, 10240"
		parts := strings.Split(lines, ",")
		if len(parts) >= 1 {
			gpuName = strings.TrimSpace(parts[0])
			gpuDetected = true
		}
		if len(parts) >= 2 {
			// Parse memory (nvidia-smi returns in MiB)
			var mem int
			if _, err := fmt.Sscanf(strings.TrimSpace(parts[1]), "%d", &mem); err == nil {
				gpuMemoryMB = mem
			}
		}
	})
}

// MemoryType defines how buffer memory is managed.
type MemoryType int

const (
	MemoryDevice MemoryType = 0
	MemoryPinned MemoryType = 1
)

// Device represents a CUDA GPU device (stub).
type Device struct{}

// Buffer represents a CUDA memory buffer (stub).
type Buffer struct{}

// SearchResult holds a similarity search result.
type SearchResult struct {
	Index uint32
	Score float32
}

// IsAvailable checks if CUDA/GPU is available.
// In stub mode, we detect GPU via nvidia-smi but can't use it for acceleration.
// This allows informative logging about GPU presence.
func IsAvailable() bool {
	detectGPURuntime()
	return gpuDetected
}

// HasGPUHardware returns true if NVIDIA GPU hardware is detected.
// This is separate from CUDA availability - GPU may be present but CUDA not usable.
func HasGPUHardware() bool {
	detectGPURuntime()
	return gpuDetected
}

// GPUName returns the detected GPU name, or empty string if none.
func GPUName() string {
	detectGPURuntime()
	return gpuName
}

// GPUMemoryMB returns the detected GPU memory in MB, or 0 if none.
func GPUMemoryMB() int {
	detectGPURuntime()
	return gpuMemoryMB
}

// IsCUDACapable returns false in stub mode - GPU is detected but CUDA ops unavailable.
func IsCUDACapable() bool {
	return false
}

// DeviceCount returns 1 if GPU detected, 0 otherwise.
// Note: In stub mode, device can't actually be used for CUDA operations.
func DeviceCount() int {
	detectGPURuntime()
	if gpuDetected {
		return 1
	}
	return 0
}

// NewDevice returns an error on systems without CUDA.
// Even with GPU detected, CUDA operations require the cuda build tag.
func NewDevice(deviceID int) (*Device, error) {
	detectGPURuntime()
	if gpuDetected {
		return nil, fmt.Errorf("%w: GPU '%s' detected but binary built without CUDA support (use -tags cuda)", ErrCUDANotAvailable, gpuName)
	}
	return nil, ErrCUDANotAvailable
}

// Release is a no-op stub.
func (d *Device) Release() {}

// ID returns 0.
func (d *Device) ID() int { return 0 }

// Name returns empty string.
func (d *Device) Name() string { return "" }

// MemoryBytes returns 0.
func (d *Device) MemoryBytes() uint64 { return 0 }

// MemoryMB returns 0.
func (d *Device) MemoryMB() int { return 0 }

// ComputeCapability returns 0, 0.
func (d *Device) ComputeCapability() (int, int) { return 0, 0 }

// NewBuffer returns an error.
func (d *Device) NewBuffer(data []float32, memType MemoryType) (*Buffer, error) {
	return nil, ErrCUDANotAvailable
}

// NewEmptyBuffer returns an error.
func (d *Device) NewEmptyBuffer(count uint64, memType MemoryType) (*Buffer, error) {
	return nil, ErrCUDANotAvailable
}

// Release is a no-op stub.
func (b *Buffer) Release() {}

// Size returns 0.
func (b *Buffer) Size() uint64 { return 0 }

// ReadFloat32 returns nil.
func (b *Buffer) ReadFloat32(count int) []float32 { return nil }

// NormalizeVectors returns an error.
func (d *Device) NormalizeVectors(vectors *Buffer, n, dimensions uint32) error {
	return ErrCUDANotAvailable
}

// CosineSimilarity returns an error.
func (d *Device) CosineSimilarity(embeddings, query, scores *Buffer, n, dimensions uint32, normalized bool) error {
	return ErrCUDANotAvailable
}

// TopK returns an error.
func (d *Device) TopK(scores *Buffer, n, k uint32) ([]uint32, []float32, error) {
	return nil, nil, ErrCUDANotAvailable
}

// Search returns an error.
func (d *Device) Search(embeddings *Buffer, query []float32, n, dimensions uint32, k int, normalized bool) ([]SearchResult, error) {
	return nil, ErrCUDANotAvailable
}
