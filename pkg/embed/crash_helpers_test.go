//go:build !localllm

package embed

import (
	"testing"
	"time"
)

// Tests for crash handling helper functions that work without localllm build tag

func TestContainsGPUError(t *testing.T) {
	tests := []struct {
		name     string
		errStr   string
		expected bool
	}{
		{"CUDA error", "CUDA error: out of memory", true},
		{"cuda lowercase", "cuda allocation failed", true},
		{"GPU uppercase", "GPU memory exhausted", true},
		{"gpu lowercase", "gpu device not found", true},
		{"Metal error", "Metal: command buffer error", true},
		{"metal lowercase", "metal shader compilation failed", true},
		{"OOM", "out of memory", true},
		{"device error", "device driver version mismatch", true},
		{"cublas error", "cublas initialization failed", true},
		{"cudnn error", "cudnn: invalid handle", true},
		{"allocation", "allocation failed for tensor", true},
		{"driver error", "driver not loaded", true},
		{"regular error", "file not found", false},
		{"network error", "connection refused", false},
		{"timeout", "context deadline exceeded", false},
		{"empty string", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := containsGPUErrorStub(tt.errStr)
			if result != tt.expected {
				t.Errorf("containsGPUError(%q) = %v, want %v", tt.errStr, result, tt.expected)
			}
		})
	}
}

// containsGPUErrorStub mirrors the logic from local_gguf.go for testing without localllm tag
func containsGPUErrorStub(errStr string) bool {
	gpuKeywords := []string{
		"CUDA", "cuda", "GPU", "gpu",
		"Metal", "metal",
		"out of memory", "OOM",
		"device", "driver",
		"cublas", "cudnn",
		"allocation failed",
	}
	for _, kw := range gpuKeywords {
		if containsStub(errStr, kw) {
			return true
		}
	}
	return false
}

// containsStub mirrors the logic from local_gguf.go
func containsStub(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestContainsHelper(t *testing.T) {
	tests := []struct {
		s      string
		substr string
		want   bool
	}{
		{"hello world", "world", true},
		{"hello world", "hello", true},
		{"hello world", "lo wo", true},
		{"hello world", "foo", false},
		{"hello", "hello world", false},
		{"", "foo", false},
		{"foo", "", true},
		{"", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.s+"_"+tt.substr, func(t *testing.T) {
			got := containsStub(tt.s, tt.substr)
			if got != tt.want {
				t.Errorf("contains(%q, %q) = %v, want %v", tt.s, tt.substr, got, tt.want)
			}
		})
	}
}

func TestEmbedderStatsStruct(t *testing.T) {
	// Test that the EmbedderStats struct has the expected fields
	stats := EmbedderStats{
		EmbedCount:    100,
		ErrorCount:    5,
		PanicCount:    1,
		LastEmbedTime: time.Now(),
		ModelName:     "test-model",
		ModelPath:     "/test/path/model.gguf",
	}

	if stats.EmbedCount != 100 {
		t.Errorf("EmbedCount = %d, want 100", stats.EmbedCount)
	}
	if stats.ErrorCount != 5 {
		t.Errorf("ErrorCount = %d, want 5", stats.ErrorCount)
	}
	if stats.PanicCount != 1 {
		t.Errorf("PanicCount = %d, want 1", stats.PanicCount)
	}
	if stats.ModelName != "test-model" {
		t.Errorf("ModelName = %q, want %q", stats.ModelName, "test-model")
	}
	if stats.ModelPath != "/test/path/model.gguf" {
		t.Errorf("ModelPath = %q, want %q", stats.ModelPath, "/test/path/model.gguf")
	}
}

func TestLocalGGUFStubMethods(t *testing.T) {
	embedder := &LocalGGUFEmbedder{}

	// Test stub methods return expected values
	if dims := embedder.Dimensions(); dims != 0 {
		t.Errorf("Dimensions() = %d, want 0", dims)
	}

	if model := embedder.Model(); model != "" {
		t.Errorf("Model() = %q, want empty string", model)
	}

	stats := embedder.Stats()
	if stats.EmbedCount != 0 {
		t.Errorf("Stats().EmbedCount = %d, want 0", stats.EmbedCount)
	}

	if err := embedder.Close(); err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}
}

func TestNewLocalGGUFReturnsError(t *testing.T) {
	config := &Config{
		Provider:   "local",
		Model:      "test-model",
		Dimensions: 1024,
	}

	_, err := NewLocalGGUF(config)
	if err == nil {
		t.Error("expected error when localllm not built, got nil")
	}

	if err != errLocalLLMNotBuilt {
		t.Errorf("expected errLocalLLMNotBuilt, got: %v", err)
	}
}
