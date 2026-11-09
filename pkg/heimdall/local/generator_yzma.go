//go:build (windows || yzma) && !nolocalllm

// Local inference backend for the Heimdall host.
// This file provides the yzma-enabled generator for Windows GPU support.
package local

import (
	"context"
	"os"
	"strconv"

	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/localllm"
)

func init() {
	// Register the yzma-enabled generator loader for Windows
	heimdall.SetGeneratorLoader(yzmaGeneratorLoader)
}

// yzmaGeneratorLoader loads a generation model using yzma bindings (Windows GPU).
func yzmaGeneratorLoader(modelPath string, gpuLayers, contextSize, batchSize, lazyMode int) (heimdall.Generator, error) {
	opts := localllm.DefaultGenerationOptions(modelPath)
	opts.GPULayers = gpuLayers
	opts.ContextSize = contextSize
	opts.BatchSize = batchSize
	opts.LazyMode = lazyMode

	// Apply Heimdall-specific context features from env
	if v, ok := yzmaEnvInt("NORNICDB_HEIMDALL_CTX_TYPE"); ok && v != 0 {
		opts.Features.CtxType = v
	}
	if v, ok := yzmaEnvInt("NORNICDB_HEIMDALL_POOLING_TYPE"); ok && v != 0 {
		opts.Features.PoolingType = v
	}
	if v, ok := yzmaEnvInt("NORNICDB_HEIMDALL_ATTENTION_TYPE"); ok && v != 0 {
		opts.Features.AttentionType = v
	}
	if v, ok := yzmaEnvInt("NORNICDB_HEIMDALL_FLASH_ATTN"); ok {
		opts.Features.FlashAttn = v
	}
	if v, ok := yzmaEnvInt("NORNICDB_HEIMDALL_LAZY_MODE"); ok && v >= localllm.LazyModeOff && v <= localllm.LazyModeOn {
		opts.LazyMode = v
	}

	model, err := localllm.LoadGenerationModel(opts)
	if err != nil {
		return nil, err
	}

	return &yzmaGenerator{model: model}, nil
}

// yzmaEnvInt reads an env var as int.
func yzmaEnvInt(key string) (int, bool) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return 0, false
	}
	i, err := strconv.Atoi(v)
	return i, err == nil
}

// yzmaGenerator wraps localllm.GenerationModel to implement Generator interface.
type yzmaGenerator struct {
	model *localllm.GenerationModel
}

func (g *yzmaGenerator) Generate(ctx context.Context, prompt string, params heimdall.GenerateParams) (string, error) {
	llamaParams := localllm.GenerateParams{
		MaxTokens:   int32(params.MaxTokens),
		Temperature: params.Temperature,
		TopP:        params.TopP,
		TopK:        int32(params.TopK),
	}
	return g.model.Generate(ctx, prompt, llamaParams)
}

func (g *yzmaGenerator) GenerateStream(ctx context.Context, prompt string, params heimdall.GenerateParams, callback func(token string) error) error {
	llamaParams := localllm.GenerateParams{
		MaxTokens:   int32(params.MaxTokens),
		Temperature: params.Temperature,
		TopP:        params.TopP,
		TopK:        int32(params.TopK),
	}
	return g.model.GenerateStream(ctx, prompt, llamaParams, callback)
}

func (g *yzmaGenerator) Close() error {
	return g.model.Close()
}

func (g *yzmaGenerator) ModelPath() string {
	return g.model.ModelPath()
}
