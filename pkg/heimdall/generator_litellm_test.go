package heimdall

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLiteLLMProviderRegistered(t *testing.T) {
	_, ok := heimdallProviderFactories["litellm"]
	assert.True(t, ok, "litellm provider should be registered")
}

func TestNewLiteLLMGeneratorDefaults(t *testing.T) {
	gen, err := newLiteLLMGenerator(Config{Model: "gpt-4o"})
	require.NoError(t, err)

	g, ok := gen.(*liteLLMGenerator)
	require.True(t, ok, "litellm generator wraps openAIGenerator")
	assert.Equal(t, defaultLiteLLMBaseURL, g.baseURL, "defaults to the local LiteLLM proxy")
	assert.Equal(t, "gpt-4o", g.model, "model/alias is used as-is")
	assert.Equal(t, "EMPTY", g.apiKey, "empty key falls back to a placeholder")
	assert.Equal(t, "litellm:gpt-4o", g.ModelPath())
}

func TestNewLiteLLMGeneratorOverrides(t *testing.T) {
	gen, err := newLiteLLMGenerator(Config{
		Model:  "anthropic/claude-sonnet-4-20250514",
		APIURL: "http://gateway.internal:4000/", // trailing slash trimmed
		APIKey: "sk-litellm-master",
	})
	require.NoError(t, err)

	g := gen.(*liteLLMGenerator)
	assert.Equal(t, "http://gateway.internal:4000", g.baseURL)
	assert.Equal(t, "anthropic/claude-sonnet-4-20250514", g.model)
	assert.Equal(t, "sk-litellm-master", g.apiKey)
}

func TestNewLiteLLMGeneratorRequiresModel(t *testing.T) {
	_, err := newLiteLLMGenerator(Config{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires NORNICDB_HEIMDALL_MODEL")
}

// TestLiteLLMGeneratorRoundTrip drives Generate against a stub OpenAI-compatible
// server (as a LiteLLM proxy would present), asserting the request shape and the
// Authorization header, and that the response is parsed back.
func TestLiteLLMGeneratorRoundTrip(t *testing.T) {
	var gotPath, gotAuth, gotModel string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		body, _ := io.ReadAll(r.Body)
		var req map[string]any
		_ = json.Unmarshal(body, &req)
		gotModel, _ = req["model"].(string)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"LITELLM_OK"},"finish_reason":"stop"}]}`))
	}))
	defer srv.Close()

	gen, err := newLiteLLMGenerator(Config{Model: "gpt-4o", APIURL: srv.URL, APIKey: "sk-test"})
	require.NoError(t, err)

	out, err := gen.Generate(context.Background(), "ping", GenerateParams{MaxTokens: 16})
	require.NoError(t, err)

	assert.Equal(t, "LITELLM_OK", out)
	assert.Equal(t, "/v1/chat/completions", gotPath)
	assert.Equal(t, "Bearer sk-test", gotAuth)
	assert.Equal(t, "gpt-4o", gotModel)
	assert.True(t, strings.HasPrefix(gen.ModelPath(), "litellm:"))
}
