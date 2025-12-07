//go:build cgo && (darwin || linux) && !windows

package heimdall

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// skipOnConstrainedEnv skips tests in memory-constrained CI environments
func skipOnConstrainedEnv(t *testing.T) {
	t.Helper()
	if os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		t.Skip("Skipping model loading test in CI environment")
	}
	if runtime.GOOS == "windows" {
		t.Skip("Skipping model loading test on Windows due to memory constraints")
	}
}

// TestCGOGeneratorLoader_Registered verifies the CGO loader is registered via init()
func TestCGOGeneratorLoader_Registered(t *testing.T) {
	// The CGO loader should be registered, not the default stub
	// Check by seeing if it returns a different error than the stub

	// First, save current loader
	originalLoader := generatorLoader
	defer func() { generatorLoader = originalLoader }()

	// The default stub returns "SLM generation requires CGO build"
	// The CGO loader should return a different error (file not found)
	_, err := generatorLoader("/nonexistent/model.gguf", 0, 8192, 8192)

	// If CGO loader is registered, error should be about file not found, not about CGO
	if err != nil {
		assert.NotContains(t, err.Error(), "requires CGO build",
			"CGO loader should be registered, not the stub")
	}
}

// TestCGOGenerator_LoadModel tests loading a real model if available
func TestCGOGenerator_LoadModel(t *testing.T) {
	skipOnConstrainedEnv(t)
	// Skip if no model available
	modelPath := findTestModel(t)
	if modelPath == "" {
		t.Skip("No test model available - set NORNICDB_MODELS_DIR or place a .gguf model in ./models")
	}

	// Try to load with CGO
	generator, err := cgoGeneratorLoader(modelPath, 0, 32768, 8192) // CPU only for test
	if err != nil {
		t.Skipf("Could not load model (may be incompatible): %v", err)
	}
	require.NotNil(t, generator)
	defer generator.Close()

	assert.Equal(t, modelPath, generator.ModelPath())
}

// TestCGOGenerator_Generate tests text generation if model available
func TestCGOGenerator_Generate(t *testing.T) {
	skipOnConstrainedEnv(t)
	modelPath := findTestModel(t)
	if modelPath == "" {
		t.Skip("No test model available")
	}

	generator, err := cgoGeneratorLoader(modelPath, 0, 32768, 8192)
	if err != nil {
		t.Skipf("Could not load model: %v", err)
	}
	defer generator.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	params := GenerateParams{
		MaxTokens:   50, // Small for fast test
		Temperature: 0.1,
		TopP:        0.9,
		TopK:        40,
		StopTokens:  []string{"<|im_end|>"},
	}

	// Simple prompt
	response, err := generator.Generate(ctx, "Say hello in one word:", params)
	require.NoError(t, err)
	assert.NotEmpty(t, response, "Should generate some response")
	t.Logf("Generated response: %s", response)
}

// TestCGOGenerator_GenerateStream tests streaming generation
func TestCGOGenerator_GenerateStream(t *testing.T) {
	skipOnConstrainedEnv(t)
	modelPath := findTestModel(t)
	if modelPath == "" {
		t.Skip("No test model available")
	}

	generator, err := cgoGeneratorLoader(modelPath, 0, 32768, 8192)
	if err != nil {
		t.Skipf("Could not load model: %v", err)
	}
	defer generator.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	params := GenerateParams{
		MaxTokens:   20,
		Temperature: 0.1,
		TopP:        0.9,
		TopK:        40,
	}

	var tokens []string
	err = generator.GenerateStream(ctx, "Count: 1, 2,", params, func(token string) error {
		tokens = append(tokens, token)
		return nil
	})
	require.NoError(t, err)
	assert.NotEmpty(t, tokens, "Should receive tokens via callback")
	t.Logf("Received %d tokens", len(tokens))
}

// TestCGOGenerator_ContextCancellation tests that generation respects context
func TestCGOGenerator_ContextCancellation(t *testing.T) {
	skipOnConstrainedEnv(t)
	modelPath := findTestModel(t)
	if modelPath == "" {
		t.Skip("No test model available")
	}

	generator, err := cgoGeneratorLoader(modelPath, 0, 32768, 8192)
	if err != nil {
		t.Skipf("Could not load model: %v", err)
	}
	defer generator.Close()

	// Cancel immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	params := GenerateParams{
		MaxTokens:   100,
		Temperature: 0.1,
	}

	_, err = generator.Generate(ctx, "Tell me a long story:", params)
	assert.ErrorIs(t, err, context.Canceled)
}

// findTestModel looks for a GGUF model to use for testing
func findTestModel(t *testing.T) string {
	// Check env var first
	if dir := os.Getenv("NORNICDB_MODELS_DIR"); dir != "" {
		if model := findGGUFInDir(dir); model != "" {
			return model
		}
	}

	// Check common locations
	candidates := []string{
		"./models",
		"../models",
		"../../models",
		"/app/models",
		"/data/models",
	}

	for _, dir := range candidates {
		if model := findGGUFInDir(dir); model != "" {
			return model
		}
	}

	return ""
}

func findGGUFInDir(dir string) string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return ""
	}

	// Prefer instruction-tuned models for testing
	preferred := []string{
		"qwen2.5-0.5b-instruct.gguf",
		"qwen2.5-1.5b-instruct.gguf",
		"tinyllama-1.1b.gguf",
	}

	for _, name := range preferred {
		path := filepath.Join(dir, name)
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	// Fall back to any GGUF that's not an embedding model
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".gguf" {
			// Skip embedding models
			if entry.Name() == "bge-m3.gguf" {
				continue
			}
			return filepath.Join(dir, entry.Name())
		}
	}

	return ""
}
