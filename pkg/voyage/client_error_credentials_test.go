package voyage

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
)

type credentialFailureRoundTripper struct{}

func (credentialFailureRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("synthetic voyage failure for %s", request.URL.String())
}

func TestCredentialedVoyageClientErrorsAreRedacted(t *testing.T) {
	credential := strings.Join([]string{"synthetic", "voyage", "credential"}, "-")
	client, err := NewClient(Config{
		APIKey:  "header-" + credential,
		BaseURL: "https://provider.example.test/proxy?api_key=" + credential,
		HTTPClient: &http.Client{
			Transport: credentialFailureRoundTripper{},
		},
		MaxRetries: -1,
	})
	if err != nil {
		t.Fatal("voyage client construction failed")
	}

	_, err = client.Rerank(context.Background(), "probe", []string{"document"}, RerankOptions{})
	if err == nil {
		t.Fatal("transport failure was not returned")
	}
	if strings.Contains(err.Error(), credential) {
		t.Fatal("voyage error exposed the synthetic credential")
	}
	if !strings.Contains(err.Error(), observability.RedactedPlaceholder()) {
		t.Fatal("voyage error did not retain the redaction marker")
	}
}
