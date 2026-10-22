package search

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
)

type credentialedRerankFailureTransport struct{}

func (credentialedRerankFailureTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("synthetic rerank failure for %s", request.URL.String())
}

func TestCredentialedRerankClientErrorsAreRedacted(t *testing.T) {
	credential := strings.Join([]string{"synthetic", "rerank", "credential"}, "-")
	reranker := NewCrossEncoder(&CrossEncoderConfig{
		Enabled: true, APIURL: "https://provider.example.test/rerank?api_key=" + credential,
		APIKey: "header-" + credential, Model: "route-test", TopK: 1,
	})
	reranker.client = &http.Client{Transport: credentialedRerankFailureTransport{}}

	_, err := reranker.Rerank(context.Background(), "probe", []RerankCandidate{{ID: "one", Content: "document"}})
	if err == nil {
		t.Fatal("transport failure was not returned")
	}
	if strings.Contains(err.Error(), credential) {
		t.Fatal("rerank error exposed the synthetic credential")
	}
	if !strings.Contains(err.Error(), observability.RedactedPlaceholder()) {
		t.Fatal("rerank error did not retain the redaction marker")
	}
}
