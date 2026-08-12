//go:build (!cgo && !windows && !yzma) || nolocalllm

// Package localllm provides CGO bindings to llama.cpp for local GGUF model inference.
//
// This is a stub implementation for platforms without CGO or llama.cpp support.
// To use local GGUF models, build with CGO enabled on a supported platform
// (Linux, macOS, Windows) and ensure the llama.cpp static library is available.
package localllm

import (
	"context"
	"errors"
	"runtime"
)

var errNotSupported = errors.New("local GGUF embeddings not supported: build with CGO on linux/darwin/windows (with -tags=cuda for Windows)")

const defaultEmbeddingContextCap = 8192

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
	Features    ContextFeatures
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
		ContextSize: 0,
		BatchSize:   0,
		Threads:     threads,
		GPULayers:   -1,
		Features:    DefaultContextFeatures(),
	}
}

// DefaultRerankerOptions returns options for classifier-head GGUF rerankers.
func DefaultRerankerOptions(modelPath string) Options {
	opts := DefaultOptions(modelPath)
	opts.Features.PoolingType = 4
	return opts
}

// LoadModel returns an error on unsupported platforms.
func LoadModel(opts Options) (*Model, error) {
	return nil, errNotSupported
}

// Embed returns an error on unsupported platforms.
func (m *Model) Embed(ctx context.Context, text string) ([]float32, error) {
	return nil, errNotSupported
}

// EmbedRaw returns an error on unsupported platforms.
func (m *Model) EmbedRaw(ctx context.Context, text string) ([]float32, error) {
	return nil, errNotSupported
}

// EmbedBatch returns an error on unsupported platforms.
func (m *Model) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	return nil, errNotSupported
}

// Dimensions returns 0 on unsupported platforms.
func (m *Model) Dimensions() int { return 0 }

// MaxTokens returns the default capability cap on unsupported platforms.
func (m *Model) MaxTokens() int { return defaultEmbeddingContextCap }

// Close is a no-op on unsupported platforms.
func (m *Model) Close() error { return nil }

// RerankerModel wraps a GGUF model for reranking; stub returns errors.
type RerankerModel struct{}

// LoadRerankerModel returns an error on unsupported platforms.
func LoadRerankerModel(opts Options) (*RerankerModel, error) {
	return nil, errNotSupported
}

// Score returns an error on unsupported platforms.
func (r *RerankerModel) Score(ctx context.Context, query, document string) (float32, error) {
	return 0, errNotSupported
}

// Close is a no-op on unsupported platforms.
func (r *RerankerModel) Close() error { return nil }
