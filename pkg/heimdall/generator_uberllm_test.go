package heimdall

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUberLLMProviderRegistered(t *testing.T) {
	_, ok := heimdallProviderFactories["uberllm"]
	assert.True(t, ok, "uberllm provider should be registered")
}

func TestNewUberLLMGeneratorRequiresAPIKey(t *testing.T) {
	_, err := newOpenAIGenerator(Config{Provider: "uberllm"})
	require.ErrorContains(t, err, "uberllm provider requires NORNICDB_HEIMDALL_API_KEY")
}

func TestNewUberLLMGeneratorDefaults(t *testing.T) {
	gen, err := newOpenAIGenerator(Config{Provider: "uberllm", APIKey: "ull-test", Model: "qwen3-0.6b-instruct.gguf"})
	require.NoError(t, err)
	oag := gen.(*openAIGenerator)
	assert.Equal(t, "https://api.uberllm.dev", oag.baseURL)
	assert.Equal(t, "openai/gpt-4o-mini", oag.model)
	assert.Equal(t, "uberllm:openai/gpt-4o-mini", gen.ModelPath())
}

func TestNewUberLLMGeneratorAcceptsV1BaseURL(t *testing.T) {
	gen, err := newOpenAIGenerator(Config{Provider: "uberllm", APIKey: "ull-test", APIURL: "https://api.uberllm.dev/v1/", Model: "deepseek/deepseek-v4.1-flash"})
	require.NoError(t, err)
	oag := gen.(*openAIGenerator)
	assert.Equal(t, "https://api.uberllm.dev", oag.baseURL)
	assert.Equal(t, "deepseek/deepseek-v4.1-flash", oag.model)
}

// TestUberLLMGeneratorRoundTrip drives Generate against a stub OpenAI-compatible
// server, asserting the request path, Authorization header and vendor/model id.
func TestUberLLMGeneratorRoundTrip(t *testing.T) {
	var gotPath, gotAuth, gotModel string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		body, _ := io.ReadAll(r.Body)
		var req map[string]any
		_ = json.Unmarshal(body, &req)
		gotModel, _ = req["model"].(string)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"UBERLLM_OK"},"finish_reason":"stop"}]}`))
	}))
	defer srv.Close()

	gen, err := newOpenAIGenerator(Config{Provider: "uberllm", Model: "deepseek/deepseek-v4.1-flash", APIURL: srv.URL + "/v1", APIKey: "ull-test"})
	require.NoError(t, err)

	out, err := gen.Generate(context.Background(), "ping", GenerateParams{MaxTokens: 16})
	require.NoError(t, err)

	assert.Equal(t, "UBERLLM_OK", out)
	assert.Equal(t, "/v1/chat/completions", gotPath)
	assert.Equal(t, "Bearer ull-test", gotAuth)
	assert.Equal(t, "deepseek/deepseek-v4.1-flash", gotModel)
}
