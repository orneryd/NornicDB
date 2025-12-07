package localllm

import (
	"context"
	"os"
	"runtime"
	"testing"
)

// skipOnConstrainedEnv skips tests in memory-constrained environments
func skipOnConstrainedEnv(t testing.TB) {
	t.Helper()
	if os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		t.Skip("Skipping model loading test in CI environment")
	}
	if runtime.GOOS == "windows" {
		t.Skip("Skipping model loading test on Windows due to memory constraints")
	}
}

// TestDefaultOptions verifies default options are reasonable
func TestDefaultOptions(t *testing.T) {
	opts := DefaultOptions("/tmp/test.gguf")

	if opts.ModelPath != "/tmp/test.gguf" {
		t.Errorf("ModelPath = %q, want /tmp/test.gguf", opts.ModelPath)
	}
	if opts.ContextSize != 512 {
		t.Errorf("ContextSize = %d, want 512", opts.ContextSize)
	}
	if opts.BatchSize != 512 {
		t.Errorf("BatchSize = %d, want 512", opts.BatchSize)
	}
	if opts.Threads < 1 {
		t.Errorf("Threads = %d, want >= 1", opts.Threads)
	}
	if opts.Threads > 8 {
		t.Errorf("Threads = %d, want <= 8", opts.Threads)
	}
	if opts.GPULayers != -1 {
		t.Errorf("GPULayers = %d, want -1 (auto)", opts.GPULayers)
	}
}

// TestLoadModel_FileNotFound verifies error on missing model file
func TestLoadModel_FileNotFound(t *testing.T) {
	t.Skip("Skipping: requires llama.cpp static library")

	opts := DefaultOptions("/nonexistent/model.gguf")
	_, err := LoadModel(opts)
	if err == nil {
		t.Error("Expected error for non-existent model file")
	}
}

// TestModel_Integration is an integration test requiring actual model
func TestModel_Integration(t *testing.T) {
	skipOnConstrainedEnv(t)
	modelPath := os.Getenv("TEST_GGUF_MODEL")
	if modelPath == "" {
		t.Skip("Skipping: TEST_GGUF_MODEL not set")
	}

	opts := DefaultOptions(modelPath)
	opts.GPULayers = 0 // Force CPU for CI

	model, err := LoadModel(opts)
	if err != nil {
		t.Fatalf("LoadModel failed: %v", err)
	}
	defer model.Close()

	t.Logf("Model loaded: %d dimensions", model.Dimensions())

	// Test single embedding
	ctx := context.Background()
	vec, err := model.Embed(ctx, "hello world")
	if err != nil {
		t.Fatalf("Embed failed: %v", err)
	}

	if len(vec) != model.Dimensions() {
		t.Errorf("Embedding length = %d, want %d", len(vec), model.Dimensions())
	}

	// Verify normalization
	var sumSq float32
	for _, v := range vec {
		sumSq += v * v
	}
	if sumSq < 0.99 || sumSq > 1.01 {
		t.Errorf("Embedding not normalized: sum of squares = %f", sumSq)
	}
}

// TestModel_BatchEmbedding tests batch embedding
func TestModel_BatchEmbedding(t *testing.T) {
	skipOnConstrainedEnv(t)
	modelPath := os.Getenv("TEST_GGUF_MODEL")
	if modelPath == "" {
		t.Skip("Skipping: TEST_GGUF_MODEL not set")
	}

	opts := DefaultOptions(modelPath)
	opts.GPULayers = 0

	model, err := LoadModel(opts)
	if err != nil {
		t.Fatalf("LoadModel failed: %v", err)
	}
	defer model.Close()

	texts := []string{"hello", "world", "test"}
	ctx := context.Background()

	vecs, err := model.EmbedBatch(ctx, texts)
	if err != nil {
		t.Fatalf("EmbedBatch failed: %v", err)
	}

	if len(vecs) != len(texts) {
		t.Errorf("Got %d embeddings, want %d", len(vecs), len(texts))
	}
}

// BenchmarkEmbed measures embedding performance
func BenchmarkEmbed(b *testing.B) {
	skipOnConstrainedEnv(b)
	modelPath := os.Getenv("TEST_GGUF_MODEL")
	if modelPath == "" {
		b.Skip("Skipping: TEST_GGUF_MODEL not set")
	}

	opts := DefaultOptions(modelPath)
	model, err := LoadModel(opts)
	if err != nil {
		b.Fatalf("LoadModel failed: %v", err)
	}
	defer model.Close()

	ctx := context.Background()
	text := "The quick brown fox jumps over the lazy dog"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := model.Embed(ctx, text)
		if err != nil {
			b.Fatal(err)
		}
	}
}
