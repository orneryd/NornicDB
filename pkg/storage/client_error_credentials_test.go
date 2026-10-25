package storage

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/observability"
)

func TestCredentialedRemoteHTTPClientErrorsAreRedacted(t *testing.T) {
	credential := strings.Join([]string{"synthetic", "remote", "credential"}, "-")
	engine, err := NewRemoteEngine(RemoteEngineConfig{
		URI: "https://provider.example.test/proxy?api_key=" + credential, Database: "nornic",
		AuthToken: "bearer-" + credential,
		HTTPClient: &http.Client{
			Transport: testfixture.RoundTripperFunc(func(request *http.Request) (*http.Response, error) {
				return nil, fmt.Errorf("synthetic remote failure for %s", request.URL.String())
			}),
		},
	})
	if err != nil {
		t.Fatal("remote engine construction failed")
	}
	t.Cleanup(func() { _ = engine.Close() })

	_, _, err = engine.QueryCypher(context.Background(), "RETURN 1", nil)
	if err == nil {
		t.Fatal("transport failure was not returned")
	}
	if strings.Contains(err.Error(), credential) {
		t.Fatal("remote error exposed the synthetic credential")
	}
	if !strings.Contains(err.Error(), observability.RedactedPlaceholder()) {
		t.Fatal("remote error did not retain the redaction marker")
	}

	followupEngine, err := NewRemoteEngine(RemoteEngineConfig{
		URI: "https://provider.example.test/proxy?api_key=" + credential, Database: "nornic",
		HTTPClient: &http.Client{Transport: testfixture.RoundTripperFunc(func(request *http.Request) (*http.Response, error) {
			if strings.HasSuffix(request.URL.Path, "/tx") {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body: io.NopCloser(strings.NewReader(
						`{"commit":"https://provider.example.test/db/nornic/tx/one/commit","errors":[]}`,
					)),
					Request: request,
				}, nil
			}
			return nil, fmt.Errorf("synthetic follow-up failure for %s", request.URL.String())
		})},
	})
	if err != nil {
		t.Fatal("follow-up engine construction failed")
	}
	t.Cleanup(func() { _ = followupEngine.Close() })
	tx, err := followupEngine.BeginCypherTx(context.Background())
	if err != nil {
		t.Fatal("explicit transaction did not open")
	}
	_, _, err = tx.QueryCypher(context.Background(), "RETURN 1", nil)
	if err == nil {
		t.Fatal("follow-up transport failure was not returned")
	}
	if strings.Contains(err.Error(), credential) {
		t.Fatal("remote follow-up error exposed the synthetic credential")
	}
	if !strings.Contains(err.Error(), observability.RedactedPlaceholder()) {
		t.Fatal("remote follow-up error did not retain the redaction marker")
	}
}

func TestCredentialedRemoteBoltClientErrorsAreRedacted(t *testing.T) {
	credential := strings.Join([]string{"synthetic", "bolt", "credential"}, "-")
	engine, err := NewRemoteEngine(RemoteEngineConfig{
		URI:      "bolt://127.0.0.1:1?api_key=" + credential,
		Database: "nornic",
	})
	if err != nil {
		if strings.Contains(err.Error(), credential) {
			t.Fatal("Bolt construction error exposed the synthetic credential")
		}
		return
	}
	t.Cleanup(func() { _ = engine.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err = engine.QueryCypher(ctx, "RETURN 1", nil)
	if err == nil {
		t.Fatal("Bolt transport failure was not returned")
	}
	if strings.Contains(err.Error(), credential) {
		t.Fatal("Bolt transport error exposed the synthetic credential")
	}
}
