package heimdall

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOpenAICompatibleProviderConstructionContracts(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cfg    Config
		url    string
		key    string
		model  string
		prefix string
	}{
		{"orca_defaults", Config{Provider: "orca", APIKey: "test-key"}, "https://api.orcarouter.ai", "test-key", "orcarouter/auto", "orca"},
		{"orca_overrides", Config{Provider: "orca", APIURL: "https://gateway.example/v1/", APIKey: "custom-key", Model: "anthropic/claude-sonnet-4.6"}, "https://gateway.example", "custom-key", "anthropic/claude-sonnet-4.6", "orca"},
		{"litellm_defaults", Config{Provider: "litellm", Model: "gpt-4o"}, defaultLiteLLMBaseURL, "EMPTY", "gpt-4o", "litellm"},
		{"litellm_overrides", Config{Provider: "litellm", Model: "anthropic/claude-sonnet-4-20250514", APIURL: "http://gateway.internal:4000/", APIKey: "sk-litellm-master"}, "http://gateway.internal:4000", "sk-litellm-master", "anthropic/claude-sonnet-4-20250514", "litellm"},
		{"direct_openai", Config{APIKey: "key"}, defaultOpenAIBaseURL, "key", defaultOpenAIModel, "openai"},
		{"normalized_openai", Config{Provider: " OPENAI ", APIKey: "key", Model: " local.gguf "}, defaultOpenAIBaseURL, "key", defaultOpenAIModel, "openai"},
		{"vllm", Config{Provider: " VLLM ", Model: " local.gguf "}, defaultVLLMBaseURL, "EMPTY", "local.gguf", "openai"},
		{"litellm", Config{Provider: " LITELLM ", Model: " local.gguf "}, defaultLiteLLMBaseURL, "EMPTY", "local.gguf", "litellm"},
		{"orca", Config{Provider: " ORCA ", APIKey: "key", Model: "local.gguf"}, defaultOrcaBaseURL, "key", defaultOrcaModel, "orca"},
		{"orca_path", Config{Provider: "orca", APIKey: "key", APIURL: " https://gateway.test/proxy/v1/ "}, "https://gateway.test/proxy", "key", defaultOrcaModel, "orca"},
		{"orca_unparseable_url_retained", Config{Provider: "orca", APIKey: "key", APIURL: "https://gateway.test/%zz"}, "https://gateway.test/%zz", "key", defaultOrcaModel, "orca"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			generator, err := newOpenAIGenerator(tc.cfg)
			require.NoError(t, err)
			require.IsType(t, &openAIGenerator{}, generator)
			g := generator.(*openAIGenerator)
			require.Equal(t, tc.url, g.baseURL)
			require.Equal(t, tc.key, g.apiKey)
			require.Equal(t, tc.model, g.model)
			require.Equal(t, tc.prefix+":"+tc.model, g.ModelPath())
			require.Equal(t, 120*time.Second, g.client.Timeout)
			require.NotNil(t, g.complete)
			g.baseURL, g.apiKey = "https://injected.test", "injected-key"
			url, key := g.credentials()
			require.Equal(t, g.baseURL, url)
			require.Equal(t, g.apiKey, key)
		})
	}
	require.Equal(t, "openai:model", (&openAIGenerator{model: "model"}).ModelPath())
}

func TestOpenAICompatibleProviderValidationOrder(t *testing.T) {
	for _, provider := range []string{"openai", "vllm", "litellm", "orca"} {
		t.Run(provider, func(t *testing.T) {
			_, err := newOpenAIGenerator(Config{Provider: provider, APIURL: "https://gateway.test/%zz"})
			if provider == "orca" {
				require.ErrorContains(t, err, "requires NORNICDB_HEIMDALL_API_KEY")
			} else {
				require.ErrorContains(t, err, provider+" provider URL:")
			}
		})
	}
	_, err := newOpenAIGenerator(Config{Provider: "orca", APIKey: " \t "})
	require.ErrorContains(t, err, "requires NORNICDB_HEIMDALL_API_KEY")
}
