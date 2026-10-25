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

func TestProviderErrorRetainsIdentityAndRedactsBody(t *testing.T) {
	for _, provider := range []string{"openai", "orca"} {
		t.Run(provider, func(t *testing.T) {
			const key = "test-provider-private-key"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprint(w, "provider rejected "+key)
			}))
			defer server.Close()
			e, err := NewEmbedder(context.Background(), &Config{Provider: provider, APIURL: server.URL, APIKey: key, Dimensions: 2, Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			_, err = e.EmbedBatch(context.Background(), []string{"one", "two"})
			var remote *ProviderError
			if !errors.As(err, &remote) {
				t.Fatalf("expected ProviderError, got %v", err)
			}
			if remote.Provider != provider || !remote.Retryable() {
				t.Fatalf("provider/retry contract: %+v", remote)
			}
			if strings.Contains(remote.Body, key) || strings.Contains(err.Error(), key) {
				t.Fatal("configured credential remains in provider error")
			}
		})
	}
}
