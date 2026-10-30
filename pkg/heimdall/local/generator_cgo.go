//go:build cgo && !nolocalllm && (darwin || linux) && !yzma

// Local inference backend for the Heimdall host.
// This file provides the CGO-enabled generator using localllm.
package local

import (
	"context"
	"os"
	"strconv"

	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/localllm"
)

func init() {
	// Register the CGO-enabled generator loader
	heimdall.SetGeneratorLoader(cgoGeneratorLoader)
}

// cgoGeneratorLoader loads a generation model using localllm CGO bindings.
func cgoGeneratorLoader(modelPath string, gpuLayers, contextSize, batchSize, lazyMode int) (heimdall.Generator, error) {
	opts := localllm.DefaultGenerationOptions(modelPath)
	opts.GPULayers = gpuLayers
	opts.ContextSize = contextSize
	opts.BatchSize = batchSize
	opts.LazyMode = lazyMode

	// Apply Heimdall-specific context features from env
	if v, ok := envInt("NORNICDB_HEIMDALL_CTX_TYPE"); ok && v != 0 {
		opts.Features.CtxType = v
	}
	if v, ok := envInt("NORNICDB_HEIMDALL_POOLING_TYPE"); ok && v != 0 {
		opts.Features.PoolingType = v
	}
	if v, ok := envInt("NORNICDB_HEIMDALL_ATTENTION_TYPE"); ok && v != 0 {
		opts.Features.AttentionType = v
	}
	if v, ok := envInt("NORNICDB_HEIMDALL_FLASH_ATTN"); ok {
		opts.Features.FlashAttn = v
	}
	if v, ok := envInt("NORNICDB_HEIMDALL_LAZY_MODE"); ok && v >= localllm.LazyModeOff && v <= localllm.LazyModeOn {
		opts.LazyMode = v
	}

	model, err := localllm.LoadGenerationModel(opts)
	if err != nil {
		return nil, err
	}

	return &cgoGenerator{model: model}, nil
}

// envInt reads an env var as int.
func envInt(key string) (int, bool) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return 0, false
	}
	i, err := strconv.Atoi(v)
	return i, err == nil
}

// cgoGenerator wraps localllm.GenerationModel to implement Generator interface.
type cgoGenerator struct {
	model *localllm.GenerationModel
}

func (g *cgoGenerator) Generate(ctx context.Context, prompt string, params heimdall.GenerateParams) (string, error) {
	llamaParams := localllm.GenerateParams{
		MaxTokens:   params.MaxTokens,
		Temperature: params.Temperature,
		TopP:        params.TopP,
		TopK:        params.TopK,
		StopTokens:  params.StopTokens,
	}
	return g.model.Generate(ctx, prompt, llamaParams)
}

func (g *cgoGenerator) GenerateStream(ctx context.Context, prompt string, params heimdall.GenerateParams, callback func(token string) error) error {
	llamaParams := localllm.GenerateParams{
		MaxTokens:   params.MaxTokens,
		Temperature: params.Temperature,
		TopP:        params.TopP,
		TopK:        params.TopK,
		StopTokens:  params.StopTokens,
	}
	return g.model.GenerateStream(ctx, prompt, llamaParams, callback)
}

func (g *cgoGenerator) Close() error {
	return g.model.Close()
}

func (g *cgoGenerator) ModelPath() string {
	return g.model.ModelPath()
}
