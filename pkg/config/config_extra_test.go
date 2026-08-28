package config

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeTelemetryListen(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty", in: "", want: ""},
		{name: "numeric", in: "9090", want: ":9090"},
		{name: "numeric with spaces", in: " 8081 ", want: ":8081"},
		{name: "already prefixed", in: ":9090", want: ":9090"},
		{name: "host and port", in: "127.0.0.1:9090", want: "127.0.0.1:9090"},
		{name: "nondigit", in: "abc", want: "abc"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, normalizeTelemetryListen(tc.in))
		})
	}
}

func TestGetParserType_DefaultWhenParserValueUnset(t *testing.T) {
	prev := GetParserType()
	defer SetParserType(prev)

	// Simulate startup state before init has stored parser type.
	parserType = atomic.Value{}
	assert.Equal(t, ParserTypeNornic, GetParserType())
}

func TestApplyEnvVars_FlashAttentionAllowsZeroOverride(t *testing.T) {
	t.Setenv("NORNICDB_EMBEDDING_FLASH_ATTN", "0")
	t.Setenv("NORNICDB_HEIMDALL_FLASH_ATTN", "0")
	t.Setenv("NORNICDB_RERANK_FLASH_ATTN", "0")

	cfg := LoadDefaults()
	cfg.Memory.EmbeddingFlashAttn = -1
	cfg.Features.HeimdallFlashAttn = -1
	cfg.Features.RerankFlashAttn = -1

	require.NoError(t, applyEnvVars(cfg))
	assert.Equal(t, 0, cfg.Memory.EmbeddingFlashAttn)
	assert.Equal(t, 0, cfg.Features.HeimdallFlashAttn)
	assert.Equal(t, 0, cfg.Features.RerankFlashAttn)
}

func TestLoadFromEnv_BM25Properties(t *testing.T) {
	t.Setenv("NORNICDB_SEARCH_BM25_PROPERTIES", " title, text, ,summary ")

	cfg := LoadFromEnv()

	require.Equal(t, []string{"title", "text", "summary"}, cfg.Memory.SearchBM25Properties)
}

func TestLoadFromFile_BM25Properties(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nornicdb.yaml")
	require.NoError(t, os.WriteFile(path, []byte("search:\n  bm25_properties: [title, text]\n"), 0o600))

	cfg, err := LoadFromFile(path)

	require.NoError(t, err)
	require.Equal(t, []string{"title", "text"}, cfg.Memory.SearchBM25Properties)
}

// ============================================================================
// FeatureFlagsConfig Heimdall getters
// ============================================================================

func TestFeatureFlagsConfig_HeimdallGetters(t *testing.T) {
	f := &FeatureFlagsConfig{
		HeimdallEnabled:  true,
		HeimdallModel:    "llama3",
		HeimdallProvider: "ollama",
		HeimdallAPIURL:   "http://localhost:11434",
		HeimdallAPIKey:   "my-secret-key",
	}
	assert.True(t, f.GetHeimdallEnabled())
	assert.Equal(t, "llama3", f.GetHeimdallModel())
	assert.Equal(t, "ollama", f.GetHeimdallProvider())
	assert.Equal(t, "http://localhost:11434", f.GetHeimdallAPIURL())
	assert.Equal(t, "my-secret-key", f.GetHeimdallAPIKey())
}

func TestFeatureFlagsConfig_HeimdallGetters_Defaults(t *testing.T) {
	f := &FeatureFlagsConfig{}
	assert.False(t, f.GetHeimdallEnabled())
	assert.Equal(t, "", f.GetHeimdallModel())
	assert.Equal(t, "", f.GetHeimdallProvider())
	assert.Equal(t, "", f.GetHeimdallAPIURL())
	assert.Equal(t, "", f.GetHeimdallAPIKey())
}

func TestFeatureFlagsConfig_AdditionalHeimdallGetters(t *testing.T) {
	f := &FeatureFlagsConfig{
		HeimdallGPULayers:        7,
		HeimdallContextSize:      4096,
		HeimdallBatchSize:        512,
		HeimdallMaxTokens:        256,
		HeimdallTemperature:      0.25,
		HeimdallAnomalyDetection: true,
		HeimdallRuntimeDiagnosis: true,
		HeimdallMemoryCuration:   true,
		HeimdallMaxContextTokens: 5000,
		HeimdallMaxSystemTokens:  3000,
		HeimdallMaxUserTokens:    1000,
	}

	assert.Equal(t, 7, f.GetHeimdallGPULayers())
	assert.Equal(t, 4096, f.GetHeimdallContextSize())
	assert.Equal(t, 512, f.GetHeimdallBatchSize())
	assert.Equal(t, 256, f.GetHeimdallMaxTokens())
	assert.Equal(t, float32(0.25), f.GetHeimdallTemperature())
	assert.True(t, f.GetHeimdallAnomalyDetection())
	assert.True(t, f.GetHeimdallRuntimeDiagnosis())
	assert.True(t, f.GetHeimdallMemoryCuration())
	assert.Equal(t, 5000, f.GetHeimdallMaxContextTokens())
	assert.Equal(t, 3000, f.GetHeimdallMaxSystemTokens())
	assert.Equal(t, 1000, f.GetHeimdallMaxUserTokens())
}

// ============================================================================
// FindConfigFile – env override path
// ============================================================================

func TestFindConfigFile_EnvOverride(t *testing.T) {
	// Create a temp file to act as a real config
	tmp, err := os.CreateTemp("", "nornicdb-config-*.yaml")
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	tmp.Close()

	t.Setenv("NORNICDB_CONFIG", tmp.Name())
	result := FindConfigFile()
	assert.Equal(t, tmp.Name(), result)
}

func TestFindConfigFile_NoEnv_ReturnsString(t *testing.T) {
	// Without a real file, FindConfigFile returns "" or a candidate path
	os.Unsetenv("NORNICDB_CONFIG")
	result := FindConfigFile()
	// Just ensure it doesn't panic and returns a string
	assert.IsType(t, "", result)
}

// ============================================================================
// ApplyEnvVars – basic smoke test
// ============================================================================

func TestApplyEnvVars_NoEnvVars(t *testing.T) {
	cfg := LoadDefaults()
	err := ApplyEnvVars(cfg)
	assert.NoError(t, err)
}

func TestApplyEnvVars_WithSomeEnvVars(t *testing.T) {
	cfg := LoadDefaults()
	t.Setenv("NORNICDB_PORT", "7688")
	t.Setenv("NORNICDB_HOST", "testhost")
	err := ApplyEnvVars(cfg)
	assert.NoError(t, err)
}

// ============================================================================
// GetParserType – feature_flags.go
// ============================================================================

func TestGetParserType_Default(t *testing.T) {
	result := GetParserType()
	assert.IsType(t, "", result)
}

func TestGetParserType_SetParserTypeFallbacks(t *testing.T) {
	prev := GetParserType()
	defer SetParserType(prev)

	SetParserType("antlr")
	assert.Equal(t, ParserTypeANTLR, GetParserType())

	SetParserType("unexpected")
	assert.Equal(t, ParserTypeNornic, GetParserType())
}
