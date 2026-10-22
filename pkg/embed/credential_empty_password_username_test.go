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

// This sends exactly the same Basic-auth username and empty password as the
// ASCII control in TestCredentialPublicURLUsernameCredential, but spells an explicit
// empty password in the configured URL (token:@host rather than token@host).
func TestCredentialPublicEmptyPasswordUsernameCredential(t *testing.T) {
	const credential = "synthetic-username-control-871429"
	for _, status := range []int{http.StatusUnauthorized, http.StatusServiceUnavailable} {
		for _, batch := range []bool{false, true} {
			t.Run(fmt.Sprintf("status_%d/batch_%t", status, batch), func(t *testing.T) {
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					username, password, ok := r.BasicAuth()
					if !ok || username != credential || password != "" {
						t.Error("provider did not receive the configured Basic-auth token")
					}
					w.WriteHeader(status)
					fmt.Fprint(w, "provider refused "+username)
				}))
				defer upstream.Close()
				endpoint, err := url.Parse(upstream.URL)
				if err != nil {
					t.Fatal(err)
				}
				endpoint.User = url.UserPassword(credential, "")
				client, err := NewEmbedder(&Config{Provider: "ollama", APIURL: endpoint.String(), APIPath: "/api/embeddings", Model: "probe", Dimensions: 3, Timeout: 5 * time.Second})
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
					t.Error("username credential with explicit empty password remains in provider diagnostic")
				}
			})
		}
	}
}
