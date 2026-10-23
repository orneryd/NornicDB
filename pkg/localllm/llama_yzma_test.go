//go:build !nolocalllm && (windows || yzma)

package localllm

import (
	"context"
	"math"
	"os"
	"testing"
)

// This exercises the native FFI layouts and pooled output API together. A
// synthetic vector cannot detect a stale llama.cpp ContextParams layout.
func TestYzmaPooledEmbeddingIntegration(t *testing.T) {
	path := os.Getenv("TEST_GGUF_MODEL")
	if path == "" {
		t.Skip("set TEST_GGUF_MODEL and NORNICDB_LIB to test native embedding inference")
	}
	opts := DefaultOptions(path)
	opts.GPULayers = 0
	opts.ContextSize = 128
	opts.BatchSize = 128
	model, err := LoadModel(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer model.Close()
	vectors, err := model.EmbedBatch(context.Background(), []string{"GPU buffer boundaries", "GPU buffer boundaries"})
	if err != nil {
		t.Fatal(err)
	}
	if model.Dimensions() <= 0 || len(vectors) != 2 {
		t.Fatal("missing embedding results")
	}
	for _, v := range vectors {
		if len(v) != model.Dimensions() {
			t.Fatalf("embedding dimensions: got %d, want %d", len(v), model.Dimensions())
		}
		var norm float64
		for _, x := range v {
			if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
				t.Fatal("non-finite embedding")
			}
			norm += float64(x) * float64(x)
		}
		if math.Abs(norm-1) > 0.01 {
			t.Fatalf("expected normalized pooled output, squared norm = %g", norm)
		}
	}
	for i, x := range vectors[0] {
		if math.Abs(float64(x-vectors[1][i])) > 1e-4 {
			t.Fatal("identical input produced different embeddings")
		}
	}
}
