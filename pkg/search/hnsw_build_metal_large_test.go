//go:build darwin && cgo && !nometal

package search

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func requireLargeMetalTest(t *testing.T) *MetalHNSWBuildAccelerator {
	t.Helper()
	if os.Getenv("NORNICDB_LARGE_METAL_TEST") != "1" {
		t.Skip("set NORNICDB_LARGE_METAL_TEST=1 to run (allocates several GiB)")
	}
	accel, err := NewMetalHNSWBuildAccelerator()
	if err != nil {
		t.Skipf("Metal not available: %v", err)
	}
	return accel
}

func TestMetalHNSWResidentBuildBeyondFourGiB(t *testing.T) {
	a := requireLargeMetalTest(t)
	defer a.Close()
	checkLargeGPUResidentBuildBeyondFourGiB(t, a)
	require.Equal(t, largeGPUBoundaryNodes+8192, a.residentN)
	require.Greater(t, a.residentCap*largeGPUTestDim, 1<<30)
}
func TestMetalHNSWBuildBeyondFourGiB(t *testing.T) {
	checkLargeGPUHNSWBuildBeyondFourGiB(t, requireLargeMetalTest(t), "Metal")
}
