package server

import (
	"bytes"
	"encoding/json"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"testing"
	"testing/fstest"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/graphql"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestHeadlessUILogPreservesEventIdentityAcrossLocales(t *testing.T) {
	tests := []struct {
		name    string
		tag     language.Tag
		message string
	}{
		{name: "English", tag: language.AmericanEnglish, message: "headless mode: UI disabled"},
		{name: "Spanish", tag: language.EuropeanSpanish, message: "modo sin interfaz: UI deshabilitada"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&output, nil)).With("component", "server")
			manager, err := localization.NewManager([]language.Tag{test.tag}, logger)
			require.NoError(t, err)
			server := &Server{
				config:    &Config{Headless: true},
				log:       logger,
				localizer: manager,
			}

			require.Nil(t, server.registerUIRoutes(http.NewServeMux()))

			var record map[string]any
			require.NoError(t, json.Unmarshal(output.Bytes(), &record))
			require.Equal(t, test.message, record["msg"])
			require.Equal(t, "server.ui.headless", record["event_id"])
			require.Equal(t, "server", record["component"])
		})
	}
}

func TestUIInitializationFailedLogPreservesFieldsAcrossLocales(t *testing.T) {
	withUIAssets(t, true, fstest.MapFS{})
	testRouterLogAcrossLocales(t,
		"server.ui.initialization_failed",
		"UI initialization failed",
		"error al inicializar la UI",
		func(server *Server) { require.Nil(t, server.registerUIRoutes(http.NewServeMux())) },
		func(t *testing.T, record map[string]any) { require.NotEmpty(t, record["error"]) },
	)
}

func TestUIEnabledLogPreservesFieldsAcrossLocales(t *testing.T) {
	withUIAssets(t, true, testUIAssetsFS())
	testRouterLogAcrossLocales(t,
		"server.ui.enabled",
		"UI browser enabled",
		"navegador de UI habilitado",
		func(server *Server) { require.NotNil(t, server.registerUIRoutes(http.NewServeMux())) },
		func(t *testing.T, record map[string]any) { require.Equal(t, "/", record["route"]) },
	)
}

func TestGraphQLEnabledLogPreservesFieldsAcrossLocales(t *testing.T) {
	testRouterLogAcrossLocales(t,
		"server.graphql.enabled",
		"graphql API enabled",
		"API GraphQL habilitada",
		func(server *Server) {
			server.graphqlHandler = graphql.NewHandler(nil, &multidb.DatabaseManager{})
			server.registerGraphQLRoutes(http.NewServeMux())
		},
		func(t *testing.T, record map[string]any) { require.Equal(t, "/graphql", record["route"]) },
	)
}

func testRouterLogAcrossLocales(t *testing.T, eventID, english, spanish string, invoke func(*Server), assertFields func(*testing.T, map[string]any)) {
	t.Helper()
	for _, test := range []struct {
		name    string
		tag     language.Tag
		message string
	}{
		{name: "English", tag: language.AmericanEnglish, message: english},
		{name: "Spanish", tag: language.EuropeanSpanish, message: spanish},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&output, nil)).With("component", "server")
			manager, err := localization.NewManager([]language.Tag{test.tag}, logger)
			require.NoError(t, err)
			server := &Server{config: &Config{}, log: logger, localizer: manager}

			invoke(server)

			record := findJSONLogRecord(t, output.Bytes(), eventID, english)
			require.Equal(t, test.message, record["msg"])
			require.Equal(t, eventID, record["event_id"])
			require.Equal(t, "server", record["component"])
			assertFields(t, record)
		})
	}
}

func withUIAssets(t *testing.T, enabled bool, assets fs.FS) {
	t.Helper()
	originalEnabled, originalAssets := UIEnabled, UIAssets
	UIEnabled, UIAssets = enabled, assets
	t.Cleanup(func() { UIEnabled, UIAssets = originalEnabled, originalAssets })
}

func TestMCPServer_DisabledLogPreservesEventFieldsAcrossLocales(t *testing.T) {
	testServerLogAcrossLocales(t, "server.mcp.disabled", "mcp server disabled via configuration", "servidor MCP deshabilitado mediante la configuración", nil, nil)
}

func TestRemoteCredentialFallbackLogPreservesFieldsAcrossLocales(t *testing.T) {
	testServerLogAcrossLocales(t, "server.remote_credentials.key_fallback", "remote credential encryption key fallback in use", "respaldo de clave de cifrado de credenciales remotas en uso", nil, func(t *testing.T, record map[string]any) {
		require.Equal(t, "database_encryption_password", record["fallback"])
		require.Equal(t, "set NORNICDB_REMOTE_CREDENTIALS_KEY for key separation", record["remediation"])
	})
}

func TestAuthenticationDisabledLogPreservesEventIdentityAcrossLocales(t *testing.T) {
	testServerLogAcrossLocales(t, "server.auth.disabled", "authentication disabled", "autenticación deshabilitada", nil, nil)
}

func TestRateLimitingEnabledLogPreservesFieldsAcrossLocales(t *testing.T) {
	testServerLogAcrossLocales(t, "server.rate_limit.enabled", "rate limiting enabled", "limitación de solicitudes habilitada", func(config *Config) {
		config.RateLimitEnabled = true
		config.RateLimitPerMinute = 120
		config.RateLimitPerHour = 2400
		config.RateLimitBurst = 30
	}, func(t *testing.T, record map[string]any) {
		require.Equal(t, float64(120), record["per_minute"])
		require.Equal(t, float64(2400), record["per_hour"])
		require.Equal(t, "per_ip", record["scope"])
	})
}

func TestHeimdallDisabledLogPreservesFieldsAcrossLocales(t *testing.T) {
	testServerLogAcrossLocales(t, "server.heimdall.disabled", "heimdall AI assistant disabled", "asistente de IA Heimdall deshabilitado", func(config *Config) {
		config.Features = &nornicConfig.FeatureFlagsConfig{HeimdallEnabled: false}
	}, func(t *testing.T, record map[string]any) {
		require.Equal(t, "heimdall", record["subsystem"])
		require.Equal(t, "NORNICDB_HEIMDALL_ENABLED", record["override_env"])
	})
}

func testServerLogAcrossLocales(t *testing.T, eventID, english, spanish string, configure func(*Config), assertFields func(*testing.T, map[string]any)) {
	t.Helper()
	for _, test := range []struct {
		name    string
		tag     language.Tag
		message string
	}{
		{name: "English", tag: language.AmericanEnglish, message: english},
		{name: "Spanish", tag: language.EuropeanSpanish, message: spanish},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, err := nornicdb.Open("", nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			t.Setenv("NORNICDB_REMOTE_CREDENTIALS_KEY", "test-remote-credential-key-32b!")
			if eventID == "server.remote_credentials.key_fallback" {
				t.Setenv("NORNICDB_REMOTE_CREDENTIALS_KEY", "")
				t.Setenv("NORNICDB_ENCRYPTION_PASSWORD", "test-database-encryption-password")
			}

			var output bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&output, nil))
			manager, err := localization.NewManager([]language.Tag{test.tag}, logger)
			require.NoError(t, err)
			config := DefaultConfig()
			config.MCPEnabled = false
			config.EmbeddingEnabled = false
			config.Logger = logger
			config.Localizer = manager
			if configure != nil {
				configure(config)
			}

			_, err = New(db, nil, config)
			require.NoError(t, err)

			record := findJSONLogRecord(t, output.Bytes(), eventID, english)
			require.Equal(t, test.message, record["msg"])
			require.Equal(t, eventID, record["event_id"])
			require.Equal(t, "server", record["component"])
			if assertFields != nil {
				assertFields(t, record)
			}
		})
	}
}

func findJSONLogRecord(t *testing.T, output []byte, eventID, englishMessage string) map[string]any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(output))
	for {
		var record map[string]any
		err := decoder.Decode(&record)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if record["event_id"] == eventID || record["msg"] == englishMessage {
			return record
		}
	}
	require.FailNow(t, "log event not found", "event_id=%s", eventID)
	return nil
}
