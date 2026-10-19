package observability_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/observability"
)

// A new canonical URL-only name is deliberately unknown to every consumer.
// This guard fails if one keeps a private credential list or loses config/path
// context. Fixed expected outputs ensure changing the classifier breaks it.
func TestCredentialURLConsumerGuard(t *testing.T) {
	previous := observability.DefaultRedactKeys
	observability.DefaultRedactKeys = append(append([]string(nil), previous...), "url_query_fixture_seal")
	t.Cleanup(func() { observability.DefaultRedactKeys = previous })
	const secret = "synthetic-canonical-url-871429"
	for _, field := range []string{"api_key", "X-Amz-Signature", "fixture_seal"} {
		t.Run(field, func(t *testing.T) {
			raw := "https://example.invalid/proxy?model=public&" + field + "=" + secret
			for _, endpoint := range []string{raw, strings.Replace(raw, "?", "#", 1), "https://fixture:" + secret + "@example.invalid/?model=public"} {
				cause := errors.New("reflected " + secret)
				outputs := []string{
					observability.RedactURL(endpoint), observability.DisplayValue("endpoint", endpoint),
					observability.RedactText(endpoint), observability.DisplayFlagDefault("embedding-url", endpoint),
					strings.Join(observability.DisplayArgs([]string{"serve", "--embedding-url=" + endpoint}), " "),
					observability.RedactError(cause, observability.ContextCredentials("CREATE ALIAS remote AT '"+endpoint+"'")...).Error(),
				}
				for _, output := range outputs {
					if strings.Contains(output, secret) {
						t.Error("URL consumer bypassed canonical owner; capture withheld")
					}
				}
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			data := "embedding:\n  api_url: " + raw + "\nserver:\n  bolt_port: " + secret + "\n"
			if err := os.WriteFile(path, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := config.LoadFromFile(path); err == nil || strings.Contains(err.Error(), secret[:7]) {
				t.Error("config diagnostic bypassed URL credential context; capture withheld")
			}
			for _, provider := range []string{"ollama", "openai", "orca"} {
				for _, route := range []string{"/proxy/embedding", "/proxy/api/embeddings", "/proxy/api/embed"} {
					for _, status := range []int{401, 503} {
						for _, batch := range []bool{false, true} {
							t.Run(fmt.Sprintf("%s/%s/%d/batch_%t", provider, route, status, batch), func(t *testing.T) {
								wantRoute := route
								if provider == "ollama" && batch && strings.HasSuffix(route, "/api/embeddings") {
									wantRoute = strings.TrimSuffix(route, "/embeddings") + "/embed"
								}
								upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
									if r.URL.Path != wantRoute || r.URL.Query().Get(field) != secret || r.URL.Query().Get("api-version") != "public" {
										t.Error("runtime endpoint changed")
									}
									w.WriteHeader(status)
									fmt.Fprint(w, "provider refused "+r.URL.Query().Get(field))
								}))
								defer upstream.Close()
								client, err := embed.NewEmbedder(&embed.Config{Provider: provider, APIURL: upstream.URL, APIPath: route + "?" + field + "=" + url.QueryEscape(secret) + "&api-version=public", APIKey: "synthetic-separate-header-918723", Model: "probe", Dimensions: 3, Timeout: time.Second})
								if err != nil {
									t.Fatal("synthetic provider initialization failed")
								}
								if batch {
									_, err = client.EmbedBatch(context.Background(), []string{"one", "two"})
								} else {
									_, err = client.Embed(context.Background(), "one")
								}
								var remote *embed.ProviderError
								if !errors.As(err, &remote) {
									t.Fatal("expected typed provider rejection")
								}
								if remote.StatusCode != status || remote.Retryable() != (status >= 500) || strings.Contains(remote.Body, secret) || strings.Contains(err.Error(), secret) {
									t.Error("provider diagnostic lost redaction or status contract; capture withheld")
								}
							})
						}
					}
				}
			}
		})
	}
}
