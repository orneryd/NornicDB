package embed

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// Both credentials reach the provider. Every byte of its reflected credential
// span must be projected, including when two owned matches overlap.
func TestCredentialPublicOverlappingProviderCredentials(t *testing.T) {
	const short = "synthetic-credential-871429"
	cases := []struct {
		name, header, query string
		offset              bool
	}{
		{"header_prefix", short, short + "-rotated-317", false},
		{"query_prefix_control", short + "-rotated-317", short, false},
		{"offset_overlap", "synthetic-left-shared", "shared-right-871429", true},
	}
	for _, provider := range []string{"ollama", "openai", "orca"} {
		for _, batch := range []bool{false, true} {
			for _, item := range cases {
				for _, status := range []int{http.StatusUnauthorized, http.StatusServiceUnavailable} {
					t.Run(fmt.Sprintf("%s/%s/status_%d/batch_%t", provider, item.name, status, batch), func(t *testing.T) {
						upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							header := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
							if provider == "ollama" {
								username, password, ok := r.BasicAuth()
								if !ok || username != "synthetic-basic-user" { t.Error("provider did not receive configured Basic auth") }
								header = password
							}
							query := r.URL.Query().Get("api_key")
							if r.URL.Path != "/proxy/embedding" || header != item.header || query != item.query {
								t.Error("provider did not receive both configured credentials")
							}
							echo := header
							if item.offset {
								echo = header + strings.TrimPrefix(query, "shared")
							} else if len(query) > len(header) {
								echo = query
							}
							w.WriteHeader(status)
							fmt.Fprint(w, "provider refused "+echo)
						}))
						defer upstream.Close()
						apiURL, apiKey := upstream.URL, item.header
						if provider == "ollama" {
							endpoint, err := url.Parse(apiURL)
							if err != nil { t.Fatal(err) }
							endpoint.User = url.UserPassword("synthetic-basic-user", item.header)
							apiURL, apiKey = endpoint.String(), ""
						}
						client, err := NewEmbedder(&Config{
							Provider: provider, APIURL: apiURL,
							APIPath: "/proxy/embedding?api_key=" + url.QueryEscape(item.query),
							APIKey:  apiKey, Model: "probe", Dimensions: 3, Timeout: 5 * time.Second,
						})
						if err != nil {
							t.Fatal("could not initialize synthetic provider")
						}
						if batch {
							_, err = client.EmbedBatch(context.Background(), []string{"one", "two"})
						} else {
							_, err = client.Embed(context.Background(), "one")
						}
						var remote *ProviderError
						if !errors.As(err, &remote) {
							t.Fatal("expected typed provider rejection")
						}
						wantProvider := provider
						if provider == "orca" { wantProvider = "openai" } // Existing develop transport identity.
						if remote.Provider != wantProvider || remote.StatusCode != status || remote.Retryable() != (status >= 500) {
							t.Error("provider identity, status or retry classification changed")
						}
						if remote.Body != "provider refused <REDACTED>" {
							t.Error("part of an owned credential span remains in the provider diagnostic")
						}
					})
				}
			}
		}
	}
}
