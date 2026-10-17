package main

import (
	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestServeGPUBackendSelection(t *testing.T) {
	config := gpuConfigForBackend("cpu")
	require.False(t, config.Enabled)
	manager, err := gpu.NewManager(config)
	require.NoError(t, err)
	require.False(t, manager.IsEnabled())
	for name, backend := range map[string]gpu.Backend{"cuda": gpu.BackendCUDA, "vulkan": gpu.BackendVulkan, "metal": gpu.BackendMetal, "opencl": gpu.BackendOpenCL} {
		config := gpuConfigForBackend(name)
		require.True(t, config.Enabled)
		require.True(t, config.FallbackOnError)
		require.Equal(t, backend, config.PreferredBackend)
	}
	require.True(t, gpuConfigForBackend("").Enabled)
}
