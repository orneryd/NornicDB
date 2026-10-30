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

func TestNewLiteLLMGeneratorRequiresModel(t *testing.T) {
	_, err := newOpenAIGenerator(Config{Provider: "litellm"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires NORNICDB_HEIMDALL_MODEL")
}

// TestLiteLLMProductionConfigModelPolicy covers both the rejected absent model
// and a preserved gateway alias through the same feature-flag construction path.
func TestLiteLLMProductionConfigModelPolicy(t *testing.T) {
	for _, model := range []string{"", "team-chat"} {
		name := "requires_explicit_model"
		if model != "" {
			name = "preserves_alias"
		}
		t.Run(name, func(t *testing.T) {
			flags := &MockFeatureFlags{enabled: true, provider: "litellm", model: model}
			cfg := ConfigFromFeatureFlags(flags)
			require.Equal(t, model, cfg.Model)
			gen, err := newOpenAIGenerator(cfg)
			if model == "" {
				require.ErrorContains(t, err, "requires NORNICDB_HEIMDALL_MODEL")
				return
			}
			require.NoError(t, err)
			require.IsType(t, &openAIGenerator{}, gen)
			require.Equal(t, model, gen.(*openAIGenerator).model)
		})
	}
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

	gen, err := newOpenAIGenerator(Config{Provider: "litellm", Model: "gpt-4o", APIURL: srv.URL, APIKey: "sk-test"})
	require.NoError(t, err)

	out, err := gen.Generate(context.Background(), "ping", GenerateParams{MaxTokens: 16})
	require.NoError(t, err)

	assert.Equal(t, "LITELLM_OK", out)
	assert.Equal(t, "/v1/chat/completions", gotPath)
	assert.Equal(t, "Bearer sk-test", gotAuth)
	assert.Equal(t, "gpt-4o", gotModel)
	assert.True(t, strings.HasPrefix(gen.ModelPath(), "litellm:"))
}
