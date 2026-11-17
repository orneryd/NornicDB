package server

import (
	"log/slog"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testfixture"
	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestHeimdallStartupUsesResolvedProviderName(t *testing.T) {
	for _, name := range []string{"", " UberLLM "} {
		t.Run(name, func(t *testing.T) {
			var output synchronizedLogBuffer
			server, _ := setupTestServerWithConfig(t, func(cfg *Config) {
				cfg.Logger = slog.New(slog.NewJSONHandler(&output, nil))
				cfg.Features = &nornicConfig.FeatureFlagsConfig{
					HeimdallEnabled: true, HeimdallProvider: "openai", HeimdallProviderName: name,
					HeimdallAPIKey: "synthetic-name-api-key",
					HeimdallAPIURL: "http://127.0.0.1:1/proxy/v1?api_key=synthetic-name-query-key",
					HeimdallModel:  "vendor/model",
				}
			})
			require.Eventually(t, func() bool { return server.getHeimdallHandler() != nil }, 2*time.Second, time.Millisecond)
			captured := output.Bytes()
			record := findJSONLogRecord(t, captured, "server.heimdall.provider_resolved", "heimdall provider resolved")
			want, source := "openai", "NORNICDB_HEIMDALL_PROVIDER"
			if name != "" {
				want, source = "UberLLM", "NORNICDB_HEIMDALL_PROVIDER_NAME"
			}
			require.Equal(t, want, record["provider"])
			require.Equal(t, source, record["override_env"])
			testfixture.RequireMarkersAbsent(t, string(captured), "synthetic-name-api-key", "synthetic-name-query-key")
		})
	}
}
