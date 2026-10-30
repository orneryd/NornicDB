//go:build !cuda || !(linux || windows)
// +build !cuda !linux,!windows

package cuda

import (
	"errors"
	"testing"
)

func TestIsAvailableStub(t *testing.T) {
	// In stub mode, IsAvailable() returns true if GPU hardware is detected via nvidia-smi.
	// This is intentional - it allows informative error messages about needing CUDA build.
	// We can't test a specific value since it depends on hardware presence.
	available := IsAvailable()
	t.Logf("GPU hardware detected (stub mode): %v", available)

	// IsCUDACapable should always be false in stub mode (no CUDA ops available)
	if IsCUDACapable() {
		t.Error("IsCUDACapable() should return false in stub mode")
	}
}

func TestDeviceCountStub(t *testing.T) {
	// DeviceCount returns 1 if GPU detected, 0 otherwise (in stub mode)
	count := DeviceCount()
	t.Logf("Device count (stub mode): %d", count)

	// Verify consistency with IsAvailable
	if IsAvailable() && count == 0 {
		t.Error("IsAvailable() is true but DeviceCount() is 0")
	}
	if !IsAvailable() && count > 0 {
		t.Error("IsAvailable() is false but DeviceCount() > 0")
	}
}

func TestNewDeviceStub(t *testing.T) {
	device, err := NewDevice(0)

	// Device should always be nil in stub mode (can't create CUDA device)
	if device != nil {
		t.Error("NewDevice() should return nil device in stub mode")
	}

	// Error should wrap ErrCUDANotAvailable (may include extra context about detected GPU)
	if err == nil {
		t.Error("NewDevice() should return an error in stub mode")
	} else if !errors.Is(err, ErrCUDANotAvailable) {
		t.Errorf("NewDevice() error should wrap ErrCUDANotAvailable, got: %v", err)
	}
}

func TestDeviceMethodsStub(t *testing.T) {
	var device Device

	// These should not panic
	device.Release()

	if device.ID() != 0 {
		t.Error("ID() should return 0")
	}
	if device.Name() != "" {
		t.Error("Name() should return empty string")
	}
	if device.MemoryBytes() != 0 {
		t.Error("MemoryBytes() should return 0")
	}
	if device.MemoryMB() != 0 {
		t.Error("MemoryMB() should return 0")
	}

	major, minor := device.ComputeCapability()
	if major != 0 || minor != 0 {
		t.Error("ComputeCapability() should return 0, 0")
	}
}

func TestBufferMethodsStub(t *testing.T) {
	var buffer Buffer

	// These should not panic
	buffer.Release()

	if buffer.Size() != 0 {
		t.Error("Size() should return 0")
	}
	if buffer.ReadFloat32(10) != nil {
		t.Error("ReadFloat32() should return nil")
	}
}

func TestDeviceBufferCreationStub(t *testing.T) {
	var device Device

	_, err := device.NewBuffer([]float32{1.0}, MemoryDevice)
	if err != ErrCUDANotAvailable {
		t.Errorf("NewBuffer() error = %v, want ErrCUDANotAvailable", err)
	}

	_, err = device.NewEmptyBuffer(100, MemoryDevice)
	if err != ErrCUDANotAvailable {
		t.Errorf("NewEmptyBuffer() error = %v, want ErrCUDANotAvailable", err)
	}
}

func TestDeviceOperationsStub(t *testing.T) {
	var device Device
	var buffer Buffer

	err := device.NormalizeVectors(&buffer, 10, 3)
	if err != ErrCUDANotAvailable {
		t.Errorf("NormalizeVectors() error = %v, want ErrCUDANotAvailable", err)
	}

	err = device.CosineSimilarity(&buffer, &buffer, &buffer, 10, 3, true)
	if err != ErrCUDANotAvailable {
		t.Errorf("CosineSimilarity() error = %v, want ErrCUDANotAvailable", err)
	}

	_, _, err = device.TopK(&buffer, 10, 5)
	if err != ErrCUDANotAvailable {
		t.Errorf("TopK() error = %v, want ErrCUDANotAvailable", err)
	}

	_, err = device.Search(&buffer, []float32{1.0}, 10, 1, 5, true)
	if err != ErrCUDANotAvailable {
		t.Errorf("Search() error = %v, want ErrCUDANotAvailable", err)
	}
}

func TestMemoryTypeConstants(t *testing.T) {
	if MemoryDevice != 0 {
		t.Error("MemoryDevice should be 0")
	}
	if MemoryPinned != 1 {
		t.Error("MemoryPinned should be 1")
	}
}

func TestErrorVariables(t *testing.T) {
	if ErrCUDANotAvailable == nil {
		t.Error("ErrCUDANotAvailable should not be nil")
	}
	if ErrDeviceCreation == nil {
		t.Error("ErrDeviceCreation should not be nil")
	}
	if ErrBufferCreation == nil {
		t.Error("ErrBufferCreation should not be nil")
	}
	if ErrKernelExecution == nil {
		t.Error("ErrKernelExecution should not be nil")
	}
	if ErrInvalidBuffer == nil {
		t.Error("ErrInvalidBuffer should not be nil")
	}
}
