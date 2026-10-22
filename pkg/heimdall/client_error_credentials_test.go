package heimdall

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
)

type credentialedGeneratorFailureTransport struct{}

func (credentialedGeneratorFailureTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("synthetic generator failure for %s", request.URL.String())
}

func TestCredentialedHeimdallClientErrorsAreRedacted(t *testing.T) {
	credential := strings.Join([]string{"synthetic", "heimdall", "credential"}, "-")
	client := &http.Client{Transport: credentialedGeneratorFailureTransport{}}
	for _, provider := range []string{"ollama", "openai", "orca", "vllm", "litellm"} {
		t.Run(provider, func(t *testing.T) {
			manager, err := NewManager(Config{
				Enabled: true, Provider: provider, Model: "route-test", APIKey: "header-" + credential,
				APIURL: "https://provider.example.test/proxy?api_key=" + credential,
			})
			if err != nil {
				t.Fatal("provider construction failed")
			}
			t.Cleanup(func() { _ = manager.Close() })
			switch generator := manager.generator.(type) {
			case *ollamaGenerator:
				generator.client = client
			case *openAIGenerator:
				generator.client = client
			case *orcaGenerator:
				generator.client = client
			case *liteLLMGenerator:
				generator.client = client
			default:
				t.Fatal("unexpected provider implementation")
			}

			_, err = manager.Generate(context.Background(), "probe", GenerateParams{})
			if err == nil {
				t.Fatal("transport failure was not returned")
			}
			if strings.Contains(err.Error(), credential) {
				t.Fatal("Heimdall error exposed the synthetic credential")
			}
			if !strings.Contains(err.Error(), observability.RedactedPlaceholder()) {
				t.Fatal("Heimdall error did not retain the redaction marker")
			}
		})
	}
}
