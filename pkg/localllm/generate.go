//go:build (!cgo && !windows && !yzma) || nolocalllm

// Package localllm - Generation API Stub
//
// This file provides stub types for text generation when CGO is not available.
// The actual CGO implementation is in llama.go.
//
// Generation support enables NornicDB to run reasoning SLMs alongside
// embedding models for cognitive database capabilities.
//
// When CGO is enabled, use LoadGenerationModel to load a reasoning model:
//
//	opts := localllm.DefaultGenerationOptions("/models/qwen3-0.6b.gguf")
//	model, err := localllm.LoadGenerationModel(opts)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer model.Close()
//
//	response, err := model.Generate(ctx, prompt, DefaultGenerateParams())
package localllm

import (
	"context"
	"fmt"
)

// GenerationModel wraps a GGUF model for text generation (reasoning tasks).
// This is a stub - actual implementation requires CGO.
type GenerationModel struct {
	modelPath  string
	maxContext int
}

// GenerateParams configures text generation behavior.
type GenerateParams struct {
	MaxTokens   int      // Maximum tokens to generate (default: 256)
	Temperature float32  // Sampling temperature (default: 0.1 for structured output)
	TopP        float32  // Nucleus sampling (default: 0.9)
	TopK        int      // Top-K sampling (default: 40)
	StopTokens  []string // Stop generation on these strings
}

// DefaultGenerateParams returns parameters optimized for structured JSON output.
func DefaultGenerateParams() GenerateParams {
	return GenerateParams{
		MaxTokens:   256,
		Temperature: 0.1,
		TopP:        0.9,
		TopK:        40,
		StopTokens:  []string{"<|im_end|>", "<|endoftext|>", "</s>"},
	}
}

// GenerationOptions configures model loading for text generation.
type GenerationOptions struct {
	ModelPath   string
	ContextSize int // Max context window (default: 2048)
	BatchSize   int
	Threads     int
	GPULayers   int
	LazyMode    int
	Features    ContextFeatures
}

// DefaultGenerationOptions returns options optimized for reasoning SLMs.
func DefaultGenerationOptions(modelPath string) GenerationOptions {
	return GenerationOptions{
		ModelPath:   modelPath,
		ContextSize: 2048,
		BatchSize:   512,
		Threads:     4,
		GPULayers:   -1,
		LazyMode:    LazyModeAuto,
		Features: ContextFeatures{
			CtxType:       0,  // LLAMA_CONTEXT_TYPE_DEFAULT
			PoolingType:   -1, // LLAMA_POOLING_TYPE_UNSPECIFIED
			AttentionType: 0,  // LLAMA_ATTENTION_TYPE_CAUSAL
			FlashAttn:     -1, // LLAMA_FLASH_ATTN_TYPE_AUTO
		},
	}
}

// LoadGenerationModel is a stub - requires CGO build.
func LoadGenerationModel(opts GenerationOptions) (*GenerationModel, error) {
	return nil, fmt.Errorf("generation model requires CGO build (use -tags cgo)")
}

// Generate is a stub - requires CGO build.
func (m *GenerationModel) Generate(ctx context.Context, prompt string, params GenerateParams) (string, error) {
	return "", fmt.Errorf("generation requires CGO build")
}

// GenerateStream is a stub - requires CGO build.
func (m *GenerationModel) GenerateStream(ctx context.Context, prompt string, params GenerateParams, callback func(token string) error) error {
	return fmt.Errorf("generation requires CGO build")
}

// Close is a stub.
func (m *GenerationModel) Close() error {
	return nil
}

// ModelPath returns the path to the loaded model file.
func (m *GenerationModel) ModelPath() string {
	return m.modelPath
}
