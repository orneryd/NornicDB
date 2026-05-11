//go:build !localllm

package embed

import (
	"context"
	"errors"
	"time"

	"github.com/orneryd/nornicdb/pkg/observability"
)

// LocalGGUFEmbedder is a stub for when localllm build tag is not set.
// Build with -tags=localllm to enable local GGUF embedding support.
type LocalGGUFEmbedder struct{}

var errLocalLLMNotBuilt = errors.New("local GGUF embeddings not available: build with -tags=localllm and llama.cpp library")

// NewLocalGGUF returns an error when localllm is not built in.
// To enable local GGUF embedding support:
//  1. Build llama.cpp: ./scripts/build-llama.sh
//  2. Build with tag: go build -tags=localllm ./cmd/nornicdb
func NewLocalGGUF(config *Config) (*LocalGGUFEmbedder, error) {
	return nil, errLocalLLMNotBuilt
}

// Embed returns an error (stub).
func (e *LocalGGUFEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	return nil, errLocalLLMNotBuilt
}

// EmbedBatch returns an error (stub).
func (e *LocalGGUFEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	return nil, errLocalLLMNotBuilt
}

// ChunkText returns an error (stub).
func (e *LocalGGUFEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return nil, errLocalLLMNotBuilt
}

// Dimensions returns 0 (stub).
func (e *LocalGGUFEmbedder) Dimensions() int {
	return 0
}

// Model returns empty string (stub).
func (e *LocalGGUFEmbedder) Model() string {
	return ""
}

// Backend returns the build-tag-derived backend label per Plan 04-05 D-06.
// On the !localllm path the value is whichever build_tag matrix file
// (backend_default.go / backend_metal.go / backend_cuda.go /
// backend_vulkan.go) was selected. The stub still returns a real value so
// the closed enum {gpu,cpu,cuda,metal,vulkan} holds for embedder probes
// even in nolocalllm builds.
func (e *LocalGGUFEmbedder) Backend() string {
	return localGGUFBackend
}

// EmbedderStats holds embedding statistics (stub).
type EmbedderStats struct {
	EmbedCount    int64     `json:"embed_count"`
	ErrorCount    int64     `json:"error_count"`
	PanicCount    int64     `json:"panic_count"`
	LastEmbedTime time.Time `json:"last_embed_time"`
	ModelName     string    `json:"model_name"`
	ModelPath     string    `json:"model_path"`
}

// Stats returns empty stats (stub).
func (e *LocalGGUFEmbedder) Stats() EmbedderStats {
	return EmbedderStats{}
}

// Close is a no-op (stub).
func (e *LocalGGUFEmbedder) Close() error {
	return nil
}

// AttachMetrics is a no-op (stub). Plan 04-05-03 D-09 — symmetric with the
// production type so cmd/nornicdb wiring can call AttachMetrics without
// build-tag branching.
func (e *LocalGGUFEmbedder) AttachMetrics(m *observability.EmbedMetrics) {}
