package observability_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
)

func TestCredentialBootstrapEndpointDiagnostics(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	// Public initialization must be safe even before the configured redacting
	// logger exists. This path rejects plaintext before attempting any network IO.
	provider, err := observability.New(context.Background(), observability.ObservabilityConfig{
		Tracing: observability.TracingConfig{Enabled: true, Endpoint: "http://synthetic%2Fuser:synthetic-password-871429@example.invalid?api_key=synthetic-query-317"},
	}, observability.ServiceInfo{}, logger, &output)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	if output.Len() == 0 {
		t.Fatal("expected actual initialization diagnostic")
	}
	for _, secret := range []string{"synthetic%2Fuser", "synthetic-password-871429", "synthetic-query-317"} {
		if strings.Contains(output.String(), secret) {
			t.Error("bootstrap diagnostic contains configured URL credentials; capture withheld")
		}
	}
}
