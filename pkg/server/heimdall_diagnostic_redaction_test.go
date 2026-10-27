package server

import (
	"errors"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/observability"
)

func TestHeimdallDiagnosticRedactionCoversStartupAndPluginErrors(t *testing.T) {
	apiURL := "https://synthetic-user:synthetic-pass@example.invalid/v1"
	apiKey := "synthetic-api-key"
	cfg := heimdall.Config{APIURL: apiURL, APIKey: apiKey}

	raw := errors.New("initialization failed for " + apiURL + " with key " + apiKey)
	credentials := observability.ClientCredentials(cfg.APIURL, cfg.APIKey)
	safeErr := observability.RedactError(raw, credentials...)
	if safeErr == nil {
		t.Fatal("expected a sanitized error")
	}
	safeDir := observability.RedactText("plugins/"+apiURL, credentials...)
	for _, value := range []string{"synthetic-user", "synthetic-pass", apiKey} {
		if strings.Contains(safeErr.Error(), value) || strings.Contains(safeDir, value) {
			t.Fatal("Heimdall diagnostics exposed a credential")
		}
	}
}
