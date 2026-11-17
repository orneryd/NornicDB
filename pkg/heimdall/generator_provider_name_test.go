package heimdall

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenAICompatibleNamesAndRoutesAcrossGenerationModes(t *testing.T) {
	paths := []struct{ base, expected string }{
		{"", "/v1/chat/completions"}, {"/", "/v1/chat/completions"},
		{"/v1", "/v1/chat/completions"}, {"/v1/", "/v1/chat/completions"},
		{"/proxy", "/proxy/v1/chat/completions"}, {"/proxy/v1/", "/proxy/v1/chat/completions"},
		{"/tenant%2Fname/v1/", "/tenant%2Fname/v1/chat/completions"},
	}
	const query = "api_key=first%2Bvalue&region=eu&api_key=second%20value"
	for _, provider := range []string{"openai", "orca", "vllm", "litellm"} {
		for _, path := range paths {
			for _, mode := range []string{"plain", "stream", "tools"} {
				t.Run(provider+path.base+"/"+mode, func(t *testing.T) {
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						assert.Equal(t, path.expected, r.URL.EscapedPath())
						assert.Equal(t, query, r.URL.RawQuery)
						assert.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))
						var body map[string]interface{}
						if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&body)) {
							return
						}
						assert.Equal(t, "vendor/model-id", body["model"])
						assert.Equal(t, mode == "stream", body["stream"] == true)
						if mode == "tools" {
							assert.NotEmpty(t, body["tools"])
						}
						if mode == "stream" {
							fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"OK\"}}]}\n\ndata: [DONE]\n\n")
							return
						}
						fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"OK","tool_calls":[{"id":"call-1","type":"function","function":{"name":"inspect","arguments":"{}"}}]}}]}`)
					}))
					defer server.Close()
					manager, err := NewManager(Config{Enabled: true, Provider: provider, ProviderName: " UberLLM ", APIURL: server.URL + path.base + "?" + query, APIKey: "test-key", Model: "vendor/model-id"})
					require.NoError(t, err)
					defer manager.Close()
					require.Equal(t, "UberLLM", manager.ProviderName())
					require.Equal(t, "UberLLM:vendor/model-id", manager.ModelPath())
					require.Equal(t, provider, manager.config.identity.protocol)
					var content string
					switch mode {
					case "plain":
						content, err = manager.Generate(t.Context(), "probe", GenerateParams{})
					case "stream":
						err = manager.GenerateStream(t.Context(), "probe", GenerateParams{}, func(token string) error { content += token; return nil })
					case "tools":
						var calls []ParsedToolCall
						content, calls, err = manager.GenerateWithTools(t.Context(), []ToolRoundMessage{{Role: "user", Content: "probe"}}, []MCPTool{{Name: "inspect", InputSchema: json.RawMessage(`{"type":"object"}`)}}, GenerateParams{})
						require.Len(t, calls, 1)
						require.Equal(t, "inspect", calls[0].Name)
					}
					require.NoError(t, err)
					require.Equal(t, "OK", content)
				})
			}
		}
	}
}

func TestProviderNameErrorsUseConfiguredIdentityAndRedactCredentials(t *testing.T) {
	for _, mode := range []string{"plain", "stream", "tools"} {
		for _, failure := range []string{"status", "decode", "empty"} {
			if mode == "stream" && failure != "status" {
				continue
			}
			t.Run(mode+"/"+failure, func(t *testing.T) {
				const secret = "synthetic-provider-name-secret"
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch failure {
					case "status":
						w.WriteHeader(502)
						fmt.Fprint(w, "rejected "+secret)
					case "decode":
						fmt.Fprint(w, "invalid JSON "+secret)
					case "empty":
						fmt.Fprint(w, `{"choices":[]}`)
					}
				}))
				defer server.Close()
				manager, err := NewManager(Config{Enabled: true, Provider: "openai", ProviderName: "UberLLM", APIKey: secret, APIURL: server.URL + "/proxy/v1?api_key=" + secret, Model: "vendor/model"})
				require.NoError(t, err)
				defer manager.Close()
				switch mode {
				case "plain":
					_, err = manager.Generate(t.Context(), "probe", GenerateParams{})
				case "stream":
					err = manager.GenerateStream(t.Context(), "probe", GenerateParams{}, func(string) error { return nil })
				case "tools":
					_, _, err = manager.GenerateWithTools(t.Context(), nil, nil, GenerateParams{})
				}
				require.Error(t, err)
				require.Contains(t, err.Error(), "UberLLM")
				require.NotContains(t, err.Error(), "openai")
				testfixture.RequireMarkersAbsent(t, err.Error(), secret)
			})
		}
	}
	_, err := NewManager(Config{Enabled: true, Provider: "openai", ProviderName: "UberLLM"})
	require.ErrorContains(t, err, "UberLLM provider requires NORNICDB_HEIMDALL_API_KEY")
	require.NotContains(t, err.Error(), "openai")
}

func TestProviderNameConfigAndLogging(t *testing.T) {
	t.Setenv("NORNICDB_HEIMDALL_PROVIDER_NAME", "")
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("heimdall:\n  enabled: true\n  provider: openai\n  provider_name: UberLLM\n  api_key: test-key\n  api_url: https://example.invalid/v1\n  model: vendor/model\n"), 0600))
	loaded, err := config.LoadFromFile(path)
	require.NoError(t, err)
	require.Equal(t, "UberLLM", loaded.Features.GetHeimdallProviderName())
	cfg := ConfigFromFeatureFlags(&loaded.Features)
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
	defer slog.SetDefault(previous)
	manager, err := NewManager(cfg)
	require.NoError(t, err)
	defer manager.Close()
	require.Contains(t, output.String(), "UberLLM:vendor/model")
	require.Equal(t, "NORNICDB_HEIMDALL_PROVIDER_NAME", manager.ProviderNameSource())
	t.Setenv("NORNICDB_HEIMDALL_PROVIDER_NAME", "My Gateway")
	loaded, err = config.LoadFromFile(path)
	require.NoError(t, err)
	require.Equal(t, "My Gateway", loaded.Features.HeimdallProviderName)
	require.Equal(t, "openai", loaded.Features.HeimdallProvider)
	require.Equal(t, "vendor/model", loaded.Features.HeimdallModel)
}

func TestProviderNameUnsetPreservesLegacyConstruction(t *testing.T) {
	for _, name := range []string{"", " \t "} {
		manager, err := NewManager(Config{Enabled: true, Provider: "vllm", ProviderName: name, Model: "served-model"})
		require.NoError(t, err)
		require.Equal(t, "openai:served-model", manager.ModelPath())
		require.Equal(t, "vllm", manager.ProviderName())
		require.Equal(t, "NORNICDB_HEIMDALL_PROVIDER", manager.ProviderNameSource())
		require.NoError(t, manager.Close())
	}
	_, registered := heimdallProviderFactories["uberllm"]
	require.False(t, registered)
}

func TestProviderNameDoesNotSelectProviderPolicy(t *testing.T) {
	for _, protocol := range []string{"openai", "orca", "vllm", "litellm"} {
		t.Run(protocol, func(t *testing.T) {
			cfg := Config{Enabled: true, Provider: protocol, APIKey: "test-key", Model: "vendor/model"}
			original, err := NewManager(cfg)
			require.NoError(t, err)
			defer original.Close()
			cfg.ProviderName = "Orca" // A label cannot select Orca's policy.
			renamed, err := NewManager(cfg)
			require.NoError(t, err)
			defer renamed.Close()
			before, after := original.generator.(*openAIGenerator), renamed.generator.(*openAIGenerator)
			require.Equal(t, before.baseURL, after.baseURL)
			require.Equal(t, before.apiKey, after.apiKey)
			require.Equal(t, before.model, after.model)
			require.Equal(t, before.client.Timeout, after.client.Timeout)
			require.Equal(t, "Orca:vendor/model", renamed.ModelPath())
		})
	}
}

func TestProviderNameDelegatingFactoryResolvesItsUpdatedConfig(t *testing.T) {
	const protocol = "provider-name-delegation"
	old, existed := heimdallProviderFactories[protocol]
	t.Cleanup(func() {
		if existed {
			heimdallProviderFactories[protocol] = old
		} else {
			delete(heimdallProviderFactories, protocol)
		}
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/chat/completions", r.URL.Path)
		assert.Equal(t, "Bearer delegated-key", r.Header.Get("Authorization"))
		fmt.Fprint(w, `{"choices":[{"message":{"content":"OK"}}]}`)
	}))
	defer server.Close()
	calls := 0
	RegisterHeimdallProvider(protocol, func(cfg Config) (Generator, error) {
		calls++
		if calls > 1 {
			return nil, fmt.Errorf("delegation selected its original factory again")
		}
		cfg.Provider, cfg.ProviderName = "openai", "UberLLM"
		cfg.APIURL, cfg.APIKey = server.URL+"/v1", "delegated-key"
		return NewManager(cfg)
	})
	manager, err := NewManager(Config{Enabled: true, Provider: protocol, Model: "vendor/model"})
	require.NoError(t, err)
	defer manager.Close()
	content, err := manager.Generate(t.Context(), "probe", GenerateParams{})
	require.NoError(t, err)
	require.Equal(t, "OK", content)
	require.Equal(t, 1, calls)
	require.Equal(t, "UberLLM:vendor/model", manager.ModelPath())
}
