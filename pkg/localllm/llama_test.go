package localllm

import (
	"context"
	"math"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/textchunk"
)

func wordTokenCount(text string) (int, error) {
	fields := strings.Fields(text)
	return len(fields), nil
}

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
	if opts.ContextSize != 0 {
		t.Errorf("ContextSize = %d, want 0 (auto)", opts.ContextSize)
	}
	if opts.BatchSize != 0 {
		t.Errorf("BatchSize = %d, want 0 (auto)", opts.BatchSize)
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
	if opts.Features.FlashAttn != 0 {
		t.Errorf("Features.FlashAttn = %d, want 0 (disabled)", opts.Features.FlashAttn)
	}
}

func TestDefaultRerankerOptions(t *testing.T) {
	opts := DefaultRerankerOptions("/tmp/reranker.gguf")
	if opts.ModelPath != "/tmp/reranker.gguf" {
		t.Fatalf("ModelPath = %q", opts.ModelPath)
	}
	if opts.Features.PoolingType != 4 {
		t.Fatalf("PoolingType = %d, want rank pooling (4)", opts.Features.PoolingType)
	}
	if opts.Features.AttentionType != 1 {
		t.Fatalf("AttentionType = %d, want non-causal (1)", opts.Features.AttentionType)
	}

	opts.Features.PoolingType = 3
	if opts.Features.PoolingType != 3 {
		t.Fatal("reranker pooling override was not retained")
	}
}

func TestResolveEmbeddingContextAndBatch(t *testing.T) {
	tests := []struct {
		name      string
		opts      Options
		trainCtx  int
		wantCtx   int
		wantBatch int
	}{
		{
			name:      "auto uses train context capped",
			opts:      Options{},
			trainCtx:  8192,
			wantCtx:   8192,
			wantBatch: 8192,
		},
		{
			name:      "auto clamps to smaller model context",
			opts:      Options{},
			trainCtx:  4096,
			wantCtx:   4096,
			wantBatch: 4096,
		},
		{
			name:      "explicit context clamps to model context",
			opts:      Options{ContextSize: 12000, BatchSize: 12000},
			trainCtx:  8192,
			wantCtx:   8192,
			wantBatch: 8192,
		},
		{
			name:      "batch clamps to effective context",
			opts:      Options{ContextSize: 6000, BatchSize: 7000},
			trainCtx:  0,
			wantCtx:   6000,
			wantBatch: 6000,
		},
		{
			name:      "fallback when train context unknown",
			opts:      Options{},
			trainCtx:  0,
			wantCtx:   defaultEmbeddingContextCap,
			wantBatch: defaultEmbeddingContextCap,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCtx, gotBatch := resolveEmbeddingContextAndBatch(tt.opts, tt.trainCtx)
			if gotCtx != tt.wantCtx {
				t.Fatalf("ctx=%d want=%d", gotCtx, tt.wantCtx)
			}
			if gotBatch != tt.wantBatch {
				t.Fatalf("batch=%d want=%d", gotBatch, tt.wantBatch)
			}
		})
	}
}

func TestDefaultGenerationOptions(t *testing.T) {
	opts := DefaultGenerationOptions("/tmp/gemma.gguf")

	if opts.ModelPath != "/tmp/gemma.gguf" {
		t.Fatalf("ModelPath = %q, want /tmp/gemma.gguf", opts.ModelPath)
	}
	if opts.ContextSize != 2048 {
		t.Fatalf("ContextSize = %d, want 2048", opts.ContextSize)
	}
	if opts.BatchSize != 512 {
		t.Fatalf("BatchSize = %d, want 512", opts.BatchSize)
	}
	if opts.Threads < 4 {
		t.Fatalf("Threads = %d, want >= 4", opts.Threads)
	}
	if opts.GPULayers != -1 {
		t.Fatalf("GPULayers = %d, want -1", opts.GPULayers)
	}
}

func TestSplitVisibleTextForStopTokens(t *testing.T) {
	stopTokens := []string{"<|im_end|>", "</s>"}
	longest := longestStopTokenLength(stopTokens)

	t.Run("holds back suffix that may become a stop token", func(t *testing.T) {
		visible, remaining, stop := splitVisibleTextForStopTokens("Hello<|im_", stopTokens, longest)
		if stop {
			t.Fatal("expected stop=false")
		}
		if visible != "Hello" {
			t.Fatalf("visible=%q want=%q", visible, "Hello")
		}
		if remaining != "<|im_" {
			t.Fatalf("remaining=%q want=%q", remaining, "<|im_")
		}
	})

	t.Run("stops when full marker completes across chunks", func(t *testing.T) {
		visible, remaining, stop := splitVisibleTextForStopTokens("<|im_end|> trailing", stopTokens, longest)
		if !stop {
			t.Fatal("expected stop=true")
		}
		if visible != "" {
			t.Fatalf("visible=%q want empty", visible)
		}
		if remaining != "" {
			t.Fatalf("remaining=%q want empty", remaining)
		}
	})

	t.Run("preserves text before completed stop marker", func(t *testing.T) {
		visible, remaining, stop := splitVisibleTextForStopTokens("Hello<|im_end|>more", stopTokens, longest)
		if !stop {
			t.Fatal("expected stop=true")
		}
		if visible != "Hello" {
			t.Fatalf("visible=%q want=%q", visible, "Hello")
		}
		if remaining != "" {
			t.Fatalf("remaining=%q want empty", remaining)
		}
	})
}

func TestResolveGenerationContextAndBatch(t *testing.T) {
	tests := []struct {
		name      string
		opts      GenerationOptions
		trainCtx  int
		wantCtx   int
		wantBatch int
	}{
		{
			name:      "defaults use train context and default batch",
			opts:      GenerationOptions{},
			trainCtx:  8192,
			wantCtx:   8192,
			wantBatch: 512,
		},
		{
			name:      "defaults fall back when train context unknown",
			opts:      GenerationOptions{},
			trainCtx:  0,
			wantCtx:   2048,
			wantBatch: 512,
		},
		{
			name:      "explicit context clamps to model context",
			opts:      GenerationOptions{ContextSize: 12000, BatchSize: 1024},
			trainCtx:  8192,
			wantCtx:   8192,
			wantBatch: 1024,
		},
		{
			name:      "batch clamps to effective context",
			opts:      GenerationOptions{ContextSize: 384, BatchSize: 1024},
			trainCtx:  0,
			wantCtx:   384,
			wantBatch: 384,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCtx, gotBatch := resolveGenerationContextAndBatch(tt.opts, tt.trainCtx)
			if gotCtx != tt.wantCtx {
				t.Fatalf("ctx=%d want=%d", gotCtx, tt.wantCtx)
			}
			if gotBatch != tt.wantBatch {
				t.Fatalf("batch=%d want=%d", gotBatch, tt.wantBatch)
			}
		})
	}
}

func TestModelMethodsThatDoNotRequireLoadedNativeModel(t *testing.T) {
	model := &Model{dims: 3, maxTokens: 0, modelDesc: "fixture.gguf"}
	ctx := context.Background()

	vec, err := model.Embed(ctx, "")
	if err != nil {
		t.Fatalf("Embed empty returned error: %v", err)
	}
	if vec != nil {
		t.Fatalf("Embed empty = %v, want nil", vec)
	}

	raw, err := model.EmbedRaw(ctx, "")
	if err != nil {
		t.Fatalf("EmbedRaw empty returned error: %v", err)
	}
	if raw != nil {
		t.Fatalf("EmbedRaw empty = %v, want nil", raw)
	}

	count, err := model.CountTokens("")
	if err != nil {
		t.Fatalf("CountTokens empty returned error: %v", err)
	}
	if count != 0 {
		t.Fatalf("CountTokens empty = %d, want 0", count)
	}

	chunks, err := model.ChunkText("", 10, 2)
	if err != nil {
		t.Fatalf("ChunkText empty returned error: %v", err)
	}
	if len(chunks) != 1 || chunks[0] != "" {
		t.Fatalf("ChunkText empty = %#v, want single empty chunk", chunks)
	}

	batch, err := model.EmbedBatch(ctx, []string{""})
	if err != nil {
		t.Fatalf("EmbedBatch empty text returned error: %v", err)
	}
	if len(batch) != 1 || batch[0] != nil {
		t.Fatalf("EmbedBatch empty = %#v, want one nil vector", batch)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := model.Embed(cancelled, "hello"); err != context.Canceled {
		t.Fatalf("Embed cancelled error = %v, want context.Canceled", err)
	}
	if _, err := model.EmbedRaw(cancelled, "hello"); err != context.Canceled {
		t.Fatalf("EmbedRaw cancelled error = %v, want context.Canceled", err)
	}
	if _, err := model.EmbedBatch(cancelled, []string{"hello"}); err != context.Canceled {
		t.Fatalf("EmbedBatch cancelled error = %v, want context.Canceled", err)
	}

	if model.Dimensions() != 3 {
		t.Fatalf("Dimensions = %d, want 3", model.Dimensions())
	}
	if model.MaxTokens() != 0 {
		t.Fatalf("MaxTokens = %d, want 0", model.MaxTokens())
	}
	if model.ModelDescription() != "fixture.gguf" {
		t.Fatalf("ModelDescription = %q", model.ModelDescription())
	}
	if err := model.Close(); err != nil {
		t.Fatalf("Close nil native handles returned error: %v", err)
	}
}

func TestRerankerModelSafeBranches(t *testing.T) {
	ctx := context.Background()
	var nilReranker *RerankerModel
	if _, err := nilReranker.Score(ctx, "query", "doc"); err == nil {
		t.Fatal("expected nil reranker score error")
	}
	if err := nilReranker.Close(); err != nil {
		t.Fatalf("nil reranker close returned error: %v", err)
	}

	r := &RerankerModel{}
	if _, err := r.Score(ctx, "query", "doc"); err == nil {
		t.Fatal("expected unloaded reranker score error")
	}
	if err := r.Close(); err != nil {
		t.Fatalf("unloaded reranker close returned error: %v", err)
	}

	if got := sigmoid32(0); math.Abs(float64(got-0.5)) > 1e-6 {
		t.Fatalf("sigmoid32(0) = %f, want 0.5", got)
	}
	if got := sigmoid32(2); got <= 0.5 || got >= 1 {
		t.Fatalf("sigmoid32(2) = %f, want (0.5, 1)", got)
	}
	if got := sigmoid32(-2); got <= 0 || got >= 0.5 {
		t.Fatalf("sigmoid32(-2) = %f, want (0, 0.5)", got)
	}

	score, err := rerankerScoreFromOutput([]float32{0})
	if err != nil || math.Abs(float64(score-0.5)) > 1e-6 {
		t.Fatalf("rerankerScoreFromOutput scalar = (%f, %v), want (0.5, nil)", score, err)
	}
	if _, err := rerankerScoreFromOutput(nil); err == nil {
		t.Fatal("expected empty reranker output error")
	}
	if _, err := rerankerScoreFromOutput([]float32{0.1, 0.2}); err == nil {
		t.Fatal("expected non-scalar reranker output error")
	}
	if got := modelOutputDimensions(4, 1024, 1); got != 1 {
		t.Fatalf("rank model output dimensions = %d, want 1", got)
	}
	if got := modelOutputDimensions(1, 1024, 1); got != 1024 {
		t.Fatalf("embedding model output dimensions = %d, want 1024", got)
	}
	if got := formatRerankPrompt("Query: {query}\nDocument: {document}", "what?", "answer"); got != "Query: what?\nDocument: answer" {
		t.Fatalf("formatRerankPrompt = %q", got)
	}
}

func TestGenerationModelSafeBranches(t *testing.T) {
	params := DefaultGenerateParams()
	if params.MaxTokens != 512 {
		t.Fatalf("MaxTokens = %d, want 512", params.MaxTokens)
	}
	if len(params.StopTokens) == 0 {
		t.Fatal("expected default stop tokens")
	}

	model := &GenerationModel{modelPath: "fixture.gguf", batchSize: 4, contextSize: 8}
	if model.ModelPath() != "fixture.gguf" {
		t.Fatalf("ModelPath = %q", model.ModelPath())
	}
	if _, err := model.Generate(context.Background(), "prompt", params); err == nil {
		t.Fatal("expected Generate to fail for nil native handles")
	}
	if err := model.GenerateStream(context.Background(), "prompt", params, func(string) error { return nil }); err == nil {
		t.Fatal("expected GenerateStream to fail for nil native handles")
	}
	if err := model.Close(); err != nil {
		t.Fatalf("Close nil native generation handles returned error: %v", err)
	}
}

func TestNativeLoadFailureBranches(t *testing.T) {
	missingEmbedding := DefaultOptions("/definitely/missing/embedding-model.gguf")
	if model, err := LoadModel(missingEmbedding); err == nil || model != nil {
		t.Fatalf("LoadModel missing = (%v, %v), want nil error", model, err)
	}

	if reranker, err := LoadRerankerModel(missingEmbedding); err == nil || reranker != nil {
		t.Fatalf("LoadRerankerModel missing = (%v, %v), want nil error", reranker, err)
	}

	missingGeneration := DefaultGenerationOptions("/definitely/missing/generation-model.gguf")
	if model, err := LoadGenerationModel(missingGeneration); err == nil || model != nil {
		t.Fatalf("LoadGenerationModel missing = (%v, %v), want nil error", model, err)
	}
}

func TestChunkTextByTokenCount_DeterministicLimits(t *testing.T) {
	text := strings.Join([]string{
		"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
	}, " ")

	chunks, err := textchunk.ChunkByTokenCount(text, 4, 1, wordTokenCount)
	if err != nil {
		t.Fatalf("chunkTextByTokenCount failed: %v", err)
	}
	if len(chunks) < 3 {
		t.Fatalf("expected multiple chunks, got %v", chunks)
	}
	for i, chunk := range chunks {
		tok, err := wordTokenCount(chunk)
		if err != nil {
			t.Fatalf("count tokens for chunk %d: %v", i, err)
		}
		if tok > 4 {
			t.Fatalf("chunk %d exceeds token cap: got %d tokens in %q", i, tok, chunk)
		}
	}
	if chunks[0] != "one two three four" {
		t.Fatalf("unexpected first chunk: %q", chunks[0])
	}
	if chunks[1] != "four five six seven" {
		t.Fatalf("unexpected overlapped second chunk: %q", chunks[1])
	}
}

func TestChunkTextByTokenCount_OverlapClamped(t *testing.T) {
	text := "alpha beta gamma delta"
	chunks, err := textchunk.ChunkByTokenCount(text, 2, 5, wordTokenCount)
	if err != nil {
		t.Fatalf("chunkTextByTokenCount failed: %v", err)
	}
	if len(chunks) == 0 {
		t.Fatal("expected chunks")
	}
	for i, chunk := range chunks {
		tok, err := wordTokenCount(chunk)
		if err != nil {
			t.Fatalf("count tokens for chunk %d: %v", i, err)
		}
		if tok > 2 {
			t.Fatalf("chunk %d exceeds clamped cap: got %d tokens in %q", i, tok, chunk)
		}
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
	if model.MaxTokens() < 1 {
		t.Fatalf("MaxTokens = %d, want > 0", model.MaxTokens())
	}

	// Verify normalization
	var sumSq float32
	for _, v := range vec {
		sumSq += v * v
	}
	if sumSq < 0.99 || sumSq > 1.01 {
		t.Errorf("Embedding not normalized: sum of squares = %f", sumSq)
	}

	// Regression guard: verify we can tokenize/embed beyond legacy 512-token limit
	// when the model context supports it.
	if model.MaxTokens() > 640 {
		longText := strings.Repeat("embedding token ", 640)
		if _, err := model.Embed(ctx, longText); err != nil {
			t.Fatalf("Embed longText failed with MaxTokens=%d: %v", model.MaxTokens(), err)
		}
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
