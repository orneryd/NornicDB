//go:build !cgovulkan

package vulkan

import (
	"errors"
	"testing"
)

func TestDeviceMethodsStub(t *testing.T) {
	var device Device

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
}

func TestBufferMethodsStub(t *testing.T) {
	var buffer Buffer

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

	_, err := device.NewBuffer([]float32{1.0})
	if !errors.Is(err, ErrVulkanNotAvailable) && !errors.Is(err, ErrDeviceCreation) {
		t.Errorf("NewBuffer() error = %v, want ErrVulkanNotAvailable or ErrDeviceCreation", err)
	}

	_, err = device.NewEmptyBuffer(100)
	if !errors.Is(err, ErrVulkanNotAvailable) && !errors.Is(err, ErrDeviceCreation) {
		t.Errorf("NewEmptyBuffer() error = %v, want ErrVulkanNotAvailable or ErrDeviceCreation", err)
	}
}

func TestDeviceOperationsStub(t *testing.T) {
	var device Device
	var buffer Buffer

	err := device.NormalizeVectors(&buffer, 10, 3)
	if !errors.Is(err, ErrVulkanNotAvailable) && !errors.Is(err, ErrDeviceCreation) {
		t.Errorf("NormalizeVectors() error = %v, want ErrVulkanNotAvailable or ErrDeviceCreation", err)
	}

	err = device.CosineSimilarity(&buffer, &buffer, &buffer, 10, 3, true)
	if !errors.Is(err, ErrVulkanNotAvailable) && !errors.Is(err, ErrDeviceCreation) {
		t.Errorf("CosineSimilarity() error = %v, want ErrVulkanNotAvailable or ErrDeviceCreation", err)
	}

	_, _, err = device.TopK(&buffer, 10, 5)
	if !errors.Is(err, ErrVulkanNotAvailable) && !errors.Is(err, ErrDeviceCreation) {
		t.Errorf("TopK() error = %v, want ErrVulkanNotAvailable or ErrDeviceCreation", err)
	}

	_, err = device.Search(&buffer, []float32{1.0}, 10, 1, 5, true)
	if !errors.Is(err, ErrVulkanNotAvailable) && !errors.Is(err, ErrDeviceCreation) {
		t.Errorf("Search() error = %v, want ErrVulkanNotAvailable or ErrDeviceCreation", err)
	}
}

func TestErrorVariables(t *testing.T) {
	if ErrVulkanNotAvailable == nil {
		t.Error("ErrVulkanNotAvailable should not be nil")
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
