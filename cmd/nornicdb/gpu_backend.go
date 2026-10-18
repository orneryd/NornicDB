package main

import (
	"runtime"
	"strings"

	"github.com/orneryd/nornicdb/pkg/gpu"
)

// gpuConfigForBackend resolves the serve command's GPU policy. CPU benchmarks
// must disable the manager as well as individual vector acceleration paths.
func gpuConfigForBackend(backend string) *gpu.Config {
	config := gpu.DefaultConfig()
	config.Enabled = true
	config.FallbackOnError = true
	switch strings.ToLower(backend) {
	case "cpu":
		config.Enabled = false
	case "vulkan":
		config.PreferredBackend = gpu.BackendVulkan
	case "cuda":
		config.PreferredBackend = gpu.BackendCUDA
	case "metal":
		config.PreferredBackend = gpu.BackendMetal
	case "opencl":
		config.PreferredBackend = gpu.BackendOpenCL
	default:
		if runtime.GOOS == "darwin" {
			config.PreferredBackend = gpu.BackendMetal
		}
	}
	return config
}
