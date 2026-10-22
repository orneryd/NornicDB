package search

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
)

type secretRerankTransport func(*http.Request) (*http.Response, error)

func (f secretRerankTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestCrossEncoderSecretError(t *testing.T) {
	const credential = "synthetic-rerank-credential-314159"
	cause := errors.New("transport reflected " + credential)
	ce := NewCrossEncoder(&CrossEncoderConfig{Enabled: true, APIURL: "http://probe:" + credential + "@example.test/rerank?api_key=" + credential, APIKey: credential})
	ce.client = &http.Client{Transport: secretRerankTransport(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") != "Bearer "+credential {
			t.Error("runtime credential changed (value withheld)")
		}
		return nil, cause
	})}
	_, err := ce.Rerank(context.Background(), "query", []RerankCandidate{{ID: "probe", Content: "document"}})
	if err == nil || !errors.Is(err, cause) {
		t.Fatal("reranker transport cause lost")
	}
	var localized *localization.LocalizedError
	if !errors.As(err, &localized) {
		t.Fatal("localized identity lost")
	}
	if strings.Contains(err.Error(), credential) || strings.Contains(localized.Error(), credential) {
		t.Error("reranker error disclosed credential (details withheld)")
	}
}
