//go:build cuda && (linux || windows)

package search

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func requireLargeCudaTest(t *testing.T) largeGPUAccelerator {
	t.Helper()
	if os.Getenv("NORNICDB_LARGE_GPU_TEST") != "1" {
		t.Skip("set NORNICDB_LARGE_GPU_TEST=1; requires a real GPU and several GiB")
	}
	a, err := NewCudaHNSWBuildAccelerator()
	require.NoError(t, err, "explicit hardware qualification must use the requested GPU")
	return a
}
func TestCudaHNSWResidentBuildBeyondFourGiB(t *testing.T) {
	a := requireLargeCudaTest(t)
	defer a.Close()
	checkLargeGPUResidentBuildBeyondFourGiB(t, a)
}
func TestCudaHNSWBuildBeyondFourGiB(t *testing.T) {
	checkLargeGPUHNSWBuildBeyondFourGiB(t, requireLargeCudaTest(t), "Cuda")
}

func TestCudaHNSWBuildSmoke(t *testing.T) {
	a, err := NewCudaHNSWBuildAccelerator()
	if err != nil {
		t.Skipf("Cuda hardware unavailable: %v", err)
	}
	checkGPUHNSWBuild(t, a, "Cuda", 8192)
}
