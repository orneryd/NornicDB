package embed

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/observability"
	voyageapi "github.com/orneryd/nornicdb/pkg/voyage"
)

type credentialErrorTransport struct{}

func (credentialErrorTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("synthetic provider failure for %s", request.URL.String())
}

func TestCredentialedEmbeddingClientErrorsAreRedacted(t *testing.T) {
	credential := strings.Join([]string{"synthetic", "embedding", "credential"}, "-")
	client := &http.Client{Transport: credentialErrorTransport{}}
	for _, provider := range []string{"ollama", "openai", "orca", "voyage"} {
		t.Run(provider, func(t *testing.T) {
			config := &Config{
				Provider: provider, APIURL: "https://provider.example.test/proxy?api_key=" + credential,
				APIKey: "header-" + credential, Model: "route-test", Dimensions: 1, Timeout: time.Second,
			}
			embedder, err := NewEmbedder(context.Background(), config)
			if err != nil {
				t.Fatal("provider construction failed")
			}
			switch typed := embedder.(type) {
			case *OllamaEmbedder:
				typed.client = client
			case *OpenAIEmbedder:
				typed.client = client
			case *VoyageEmbedder:
				typed.client, err = voyageapi.NewClient(voyageapi.Config{
					APIKey: config.APIKey, BaseURL: config.APIURL, HTTPClient: client, MaxRetries: -1,
				})
				if err != nil {
					t.Fatal("voyage client construction failed")
				}
			default:
				t.Fatal("unexpected provider implementation")
			}

			_, err = embedder.Embed(context.Background(), "probe")
			assertProviderCredentialRedacted(t, err, credential)
		})
	}
}

func assertProviderCredentialRedacted(t *testing.T, err error, credential string) {
	t.Helper()
	if err == nil {
		t.Fatal("transport failure was not returned")
	}
	if strings.Contains(err.Error(), credential) {
		t.Fatal("provider error exposed the synthetic credential")
	}
	if !strings.Contains(err.Error(), observability.RedactedPlaceholder()) {
		t.Fatal("provider error did not retain the redaction marker")
	}
}
