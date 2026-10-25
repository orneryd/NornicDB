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

// A passwordless Basic-auth token is supplied in URL userinfo. The slash is
// URL-escaped but is a legal byte in the actual HTTP Basic username.
func TestCredentialPublicURLUsernameCredential(t *testing.T) {
	for index, credential := range []string{"synthetic-username-control-871429", "synthetic/token-username-871429"} {
		for _, status := range []int{http.StatusUnauthorized, http.StatusServiceUnavailable} {
			for _, batch := range []bool{false, true} {
				t.Run(fmt.Sprintf("credential_%d/status_%d/batch_%t", index, status, batch), func(t *testing.T) {
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						username, password, ok := r.BasicAuth()
						if !ok || username != credential || password != "" {
							t.Error("provider did not receive configured Basic-auth token")
						}
						w.WriteHeader(status)
						fmt.Fprint(w, "provider refused "+username)
					}))
					defer upstream.Close()
					endpoint, err := url.Parse(upstream.URL)
					if err != nil {
						t.Fatal(err)
					}
					endpoint.User = url.User(credential)
					client, err := NewEmbedder(context.Background(), &Config{Provider: "ollama", APIURL: endpoint.String(), APIPath: "/api/embeddings", Model: "probe", Dimensions: 3, Timeout: 5 * time.Second})
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
					if remote.StatusCode != status || remote.Retryable() != (status >= 500) {
						t.Error("status or retry classification changed")
					}
					if strings.Contains(remote.Body, credential) || strings.Contains(err.Error(), credential) {
						t.Error("decoded URL username credential remains in provider diagnostic")
					}
				})
			}
		}
	}
}
