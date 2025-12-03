//go:build !cgo || nolocalllm || (!darwin && !linux && !(windows && cuda))

// Package localllm provides CGO bindings to llama.cpp for local GGUF model inference.
//
// This is a stub implementation for platforms without CGO or llama.cpp support.
// To use local GGUF models, build with CGO enabled on a supported platform
// (Linux, macOS, Windows with CUDA) and ensure the llama.cpp static library is available.
package localllm

import (
	"context"
	"errors"
	"runtime"
)

var errNotSupported = errors.New("local GGUF embeddings not supported: build with CGO on linux/darwin/windows (with -tags=cuda for Windows)")

// Model wraps a GGUF model for embedding generation.
// This is a stub that returns errors on unsupported platforms.
type Model struct{}

// Options configures model loading and inference.
type Options struct {
	ModelPath   string
	ContextSize int
	BatchSize   int
	Threads     int
	GPULayers   int
}

// DefaultOptions returns options optimized for embedding generation.
func DefaultOptions(modelPath string) Options {
	threads := runtime.NumCPU() / 2
	if threads < 1 {
		threads = 1
	}
	if threads > 8 {
		threads = 8
	}
	return Options{
		ModelPath:   modelPath,
		ContextSize: 512,
		BatchSize:   512,
		Threads:     threads,
		GPULayers:   -1,
	}
}

// LoadModel returns an error on unsupported platforms.
func LoadModel(opts Options) (*Model, error) {
	return nil, errNotSupported
}

// Embed returns an error on unsupported platforms.
func (m *Model) Embed(ctx context.Context, text string) ([]float32, error) {
	return nil, errNotSupported
}

// EmbedBatch returns an error on unsupported platforms.
func (m *Model) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	return nil, errNotSupported
}

// Dimensions returns 0 on unsupported platforms.
func (m *Model) Dimensions() int { return 0 }

// Close is a no-op on unsupported platforms.
func (m *Model) Close() error { return nil }
