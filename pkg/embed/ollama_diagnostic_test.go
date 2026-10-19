package embed

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Only this test's synthetic credential is reflected
// by a disposable local test server on TrueNAS. Never log the returned text.
func TestOllamaDiagnosticBoundary(t *testing.T) {
	const credential = "review-synthetic-ollama-password"
	for _, status := range []int{http.StatusUnauthorized, http.StatusServiceUnavailable} {
		for _, path := range []string{"/api/embeddings", "/api/embed"} {
			for _, batch := range []bool{false, true} {
				t.Run(fmt.Sprintf("status_%d%s/batch_%t", status, path, batch), func(t *testing.T) {
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						_, password, ok := r.BasicAuth()
						if !ok || password != credential {
							t.Error("upstream did not receive configured credential")
						}
						w.WriteHeader(status)
						fmt.Fprint(w, "upstream rejected "+password)
					}))
					defer upstream.Close()
					apiURL := strings.Replace(upstream.URL, "://", "://probe:"+credential+"@", 1)
					client, err := NewEmbedder(&Config{Provider: "ollama", APIURL: apiURL, APIPath: path, Model: "probe", Dimensions: 3, Timeout: time.Second})
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
					if remote.Provider != "ollama" || remote.StatusCode != status || remote.Retryable() != (status >= 500) {
						t.Error("provider/status/retry contract changed")
					}
					if strings.Contains(remote.Body, credential) || strings.Contains(err.Error(), credential) {
						t.Error("configured credential remains in provider diagnostic")
					}
				})
			}
		}
	}
}

type ollamaDiagnosticTransport func(*http.Request) (*http.Response, error)

func (f ollamaDiagnosticTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestOllamaDiagnosticTransportError(t *testing.T) {
	const credential = "test-ollama-transport-password"
	for _, path := range []string{"/api/embeddings", "/api/embed"} {
		for _, batch := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/batch_%t", path, batch), func(t *testing.T) {
				cause := fmt.Errorf("synthetic transport rejected %s", credential)
				client := NewOllama(&Config{APIURL: "http://probe:" + credential + "@provider.invalid", APIPath: path, Model: "probe", Dimensions: 3})
				client.client.Transport = ollamaDiagnosticTransport(func(*http.Request) (*http.Response, error) {
					return nil, cause
				})
				var err error
				if batch {
					_, err = client.EmbedBatch(context.Background(), []string{"one", "two"})
				} else {
					_, err = client.Embed(context.Background(), "one")
				}
				if !errors.Is(err, cause) {
					t.Fatal("transport error identity was lost")
				}
				if strings.Contains(err.Error(), credential) {
					t.Fatal("configured credential remains in transport diagnostic")
				}
			})
		}
	}
}
