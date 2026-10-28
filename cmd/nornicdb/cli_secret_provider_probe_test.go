package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/voyage"
)

// Mock upstreams reflect only the synthetic credential this test sent them.
// No public provider is contacted and returned error text is never logged.
func TestCLISecretProviderErrors(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		credential := r.Header.Get("Authorization")
		if _, password, ok := r.BasicAuth(); ok {
			credential = password
		}
		if credential == "" {
			_ = r.ParseForm()
			credential = r.Form.Get("client_secret")
			if credential == "" {
				credential = r.Form.Get("refresh_token")
			}
		}
		if credential != cliSecretProbeEnv && credential != "Bearer "+cliSecretProbeEnv {
			t.Error("upstream did not receive the configured runtime credential (values withheld)")
		}
		fmt.Fprintf(w, `{"error":%q,"errors":[{"code":"Neo.ClientError.Security.Unauthorized","message":%q}]}`, credential, credential)
	}))
	defer upstream.Close()
	check := func(t *testing.T, err error) {
		t.Helper()
		if err == nil {
			t.Fatal("mock rejection produced no error")
		}
		assertCLISecretAbsent(t, "upstream error", []byte(err.Error()))
	}
	for _, provider := range []string{"openai", "orca", "voyage", "ollama"} {
		t.Run("embedding-"+provider, func(t *testing.T) {
			apiURL := upstream.URL
			if provider == "ollama" {
				apiURL = strings.Replace(apiURL, "://", "://probe:"+cliSecretProbeEnv+"@", 1)
			}
			client := newCLISecretProbeEmbedder(t, provider, apiURL, cliSecretProbeEnv)
			_, err := client.Embed(context.Background(), "probe")
			check(t, err)
		})
	}
	t.Run("voyage-rerank", func(t *testing.T) {
		client, err := voyage.NewClient(voyage.Config{APIKey: cliSecretProbeEnv, BaseURL: upstream.URL, MaxRetries: -1, Timeout: time.Second})
		if err != nil {
			t.Fatal("synthetic Voyage client could not initialize")
		}
		_, err = client.Rerank(context.Background(), "probe", []string{"document"}, voyage.RerankOptions{Model: "probe"})
		check(t, err)
	})
	for _, provider := range []string{"openai", "orca", "vllm", "litellm", "ollama"} {
		t.Run("heimdall-"+provider, func(t *testing.T) {
			apiURL := upstream.URL
			if provider == "ollama" {
				apiURL = strings.Replace(apiURL, "://", "://probe:"+cliSecretProbeEnv+"@", 1)
			}
			manager, err := heimdall.NewManager(heimdall.Config{Enabled: true, Provider: provider, APIKey: cliSecretProbeEnv, APIURL: apiURL, Model: "probe"})
			if err != nil {
				t.Fatal("synthetic Heimdall client could not initialize")
			}
			defer manager.Close()
			_, err = manager.Generate(context.Background(), "probe", heimdall.GenerateParams{})
			check(t, err)
			err = manager.GenerateStream(context.Background(), "probe", heimdall.GenerateParams{}, func(string) error { return nil })
			check(t, err)
			_, _, err = manager.GenerateWithTools(context.Background(), nil, nil, heimdall.GenerateParams{})
			check(t, err)
		})
	}
	t.Run("oauth", func(t *testing.T) {
		t.Setenv("NORNICDB_AUTH_PROVIDER", "oauth")
		t.Setenv("NORNICDB_OAUTH_ISSUER", upstream.URL)
		t.Setenv("NORNICDB_OAUTH_CLIENT_ID", "probe")
		t.Setenv("NORNICDB_OAUTH_CLIENT_SECRET", cliSecretProbeEnv)
		t.Setenv("NORNICDB_OAUTH_CALLBACK_URL", "http://127.0.0.1/callback")
		manager := auth.NewOAuthManager(nil)
		_, err := manager.ExchangeCode("synthetic-code")
		check(t, err)
		_, err = manager.GetUserInfo(cliSecretProbeEnv)
		check(t, err)
		err = manager.RefreshOAuthToken(&auth.User{}, cliSecretProbeFlag)
		check(t, err)
	})
	t.Run("remote-basic-auth", func(t *testing.T) {
		engine, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: upstream.URL, Database: "probe", User: "probe", Password: cliSecretProbeEnv})
		if err != nil {
			t.Fatal("synthetic remote client could not initialize")
		}
		defer engine.Close()
		_, _, err = engine.QueryCypher(context.Background(), "RETURN 1", nil, storage.RemoteAccessRead)
		check(t, err)
	})
}

func TestCLISecretProviderOpaqueCredential(t *testing.T) {
	credential := "https://" + cliSecretProbeEnv + ".invalid/key?value=\"&"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+credential {
			t.Error("runtime credential changed (value withheld)")
		}
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, `{"error":%q}`, r.Header.Get("Authorization"))
	}))
	defer upstream.Close()
	for _, provider := range []string{"openai", "voyage"} {
		t.Run(provider, func(t *testing.T) {
			client := newCLISecretProbeEmbedder(t, provider, upstream.URL, credential)
			_, err := client.Embed(context.Background(), "probe")
			if err == nil {
				t.Fatal("mock rejection produced no error")
			}
			assertCLISecretAbsent(t, "opaque credential error", []byte(err.Error()))
		})
	}
}

// newCLISecretProbeEmbedder retains the synthetic provider construction contract.
func newCLISecretProbeEmbedder(t *testing.T, provider, apiURL, credential string) embed.Embedder {
	t.Helper()
	client, err := embed.NewEmbedder(context.Background(), &embed.Config{Provider: provider, APIKey: credential, APIURL: apiURL, APIPath: "/v1/embeddings", Model: "probe", Dimensions: 3, Timeout: time.Second})
	if err != nil {
		t.Fatal("synthetic provider could not initialize")
	}
	return client
}
