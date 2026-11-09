//go:build !cgovulkan

package search

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func requireLargeVulkanTest(t *testing.T) largeGPUAccelerator {
	t.Helper()
	if os.Getenv("NORNICDB_LARGE_GPU_TEST") != "1" {
		t.Skip("set NORNICDB_LARGE_GPU_TEST=1; requires a real GPU and several GiB")
	}
	a, err := NewVulkanHNSWBuildAccelerator()
	require.NoError(t, err, "explicit hardware qualification must use the requested GPU")
	return a
}
func TestVulkanHNSWResidentBuildBeyondFourGiB(t *testing.T) {
	a := requireLargeVulkanTest(t)
	defer a.Close()
	checkLargeGPUResidentBuildBeyondFourGiB(t, a)
}
func TestVulkanHNSWBuildBeyondFourGiB(t *testing.T) {
	checkLargeGPUHNSWBuildBeyondFourGiB(t, requireLargeVulkanTest(t), "Vulkan")
}

func TestVulkanHNSWBuildSmoke(t *testing.T) {
	a, err := NewVulkanHNSWBuildAccelerator()
	if err != nil {
		t.Skipf("Vulkan hardware unavailable: %v", err)
	}
	checkGPUHNSWBuild(t, a, "Vulkan", 8192)
}
