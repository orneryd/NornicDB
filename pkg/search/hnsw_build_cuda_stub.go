//go:build !cuda || !(linux || windows)

package search

import (
	"context"
	"errors"
)

// ErrCUDANotAvailableForBuild is returned when CUDA HNSW build acceleration
// is not available on this platform or build configuration.
var ErrCUDANotAvailableForBuild = errors.New("cuda: CUDA HNSW build acceleration not available (build without cuda tag or unsupported platform)")

// CudaHNSWBuildAccelerator is a stub used when CUDA is not available.
type CudaHNSWBuildAccelerator struct{}

// NewCudaHNSWBuildAccelerator always returns an error in stub mode.
func NewCudaHNSWBuildAccelerator() (*CudaHNSWBuildAccelerator, error) {
	return nil, ErrCUDANotAvailableForBuild
}

func (a *CudaHNSWBuildAccelerator) Prepare(int, int) error { return ErrCUDANotAvailableForBuild }
func (a *CudaHNSWBuildAccelerator) CandidateSearch(context.Context, [][]float32, [][]float32, int) ([][]int, [][]float32, error) {
	return nil, nil, ErrCUDANotAvailableForBuild
}
func (a *CudaHNSWBuildAccelerator) Close() error { return nil }
