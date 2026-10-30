//go:build cgovulkan

package search

import (
	"context"
	"errors"
)

// ErrVulkanNotAvailableForBuild is returned when Vulkan HNSW build acceleration
// is not available in CGo Vulkan mode.
var ErrVulkanNotAvailableForBuild = errors.New("vulkan: Vulkan HNSW build acceleration not available in cgovulkan mode")

// VulkanHNSWBuildAccelerator is a stub used when building with cgovulkan tag.
type VulkanHNSWBuildAccelerator struct{}

// NewVulkanHNSWBuildAccelerator always returns an error in cgovulkan mode.
func NewVulkanHNSWBuildAccelerator() (*VulkanHNSWBuildAccelerator, error) {
	return nil, ErrVulkanNotAvailableForBuild
}

func (a *VulkanHNSWBuildAccelerator) Prepare(int, int) error { return ErrVulkanNotAvailableForBuild }
func (a *VulkanHNSWBuildAccelerator) CandidateSearch(context.Context, [][]float32, [][]float32, int) ([][]int, [][]float32, error) {
	return nil, nil, ErrVulkanNotAvailableForBuild
}
func (a *VulkanHNSWBuildAccelerator) Close() error { return nil }
