package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"

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

func TestSearchRerankDisabledLogPreservesFieldsAcrossLocales(t *testing.T) {
	testServerLogAcrossLocales(t, "server.search_rerank.disabled", "search rerank disabled", "reordenación de búsqueda deshabilitada", func(config *Config) {
		config.Features = &nornicConfig.FeatureFlagsConfig{SearchRerankEnabled: false}
	}, func(t *testing.T, record map[string]any) {
		require.Equal(t, "search_rerank", record["subsystem"])
		require.Equal(t, "NORNICDB_SEARCH_RERANK_ENABLED", record["override_env"])
	})
}

func TestSearchRerankMissingAPILogPreservesFieldsAcrossLocales(t *testing.T) {
	testServerLogAcrossLocales(t, "server.search_rerank.api_url_missing", "search rerank enabled but API URL not set; stage-2 reranking disabled", "reordenación de búsqueda habilitada pero sin URL de API; reordenación de segunda etapa deshabilitada", func(config *Config) {
		config.Features = &nornicConfig.FeatureFlagsConfig{SearchRerankEnabled: true, SearchRerankProvider: "http"}
	}, func(t *testing.T, record map[string]any) {
		require.Equal(t, "search_rerank", record["subsystem"])
		require.Equal(t, "http", record["provider"])
		require.Equal(t, "NORNICDB_SEARCH_RERANK_API_URL", record["required_env"])
	})
}

func TestSearchRerankerReadyLogPreservesFieldsAcrossLocales(t *testing.T) {
	const apiURL = "http://127.0.0.1:1/rerank"
	testServerLogAcrossLocales(t, "server.search_rerank.ready", "search reranker ready (stage-2 reranking enabled)", "reordenador de búsqueda listo (reordenación de segunda etapa habilitada)", func(config *Config) {
		config.Features = &nornicConfig.FeatureFlagsConfig{
			SearchRerankEnabled:  true,
			SearchRerankProvider: "http",
			SearchRerankAPIURL:   apiURL,
		}
	}, func(t *testing.T, record map[string]any) {
		require.Equal(t, "search_rerank", record["subsystem"])
		require.Equal(t, "http", record["provider"])
		require.Equal(t, apiURL, record["url"])
	})
}

func TestSearchRerankerLoadingLogPreservesFieldsAcrossLocales(t *testing.T) {
	testServerLogAcrossLocales(t, "server.search_rerank.loading", "loading search reranker model", "cargando modelo de reordenación de búsqueda", func(config *Config) {
		config.ModelsDir = "/test/models"
		config.Features = &nornicConfig.FeatureFlagsConfig{
			SearchRerankEnabled: true,
			SearchRerankModel:   "test-reranker.gguf",
		}
	}, func(t *testing.T, record map[string]any) {
		require.Equal(t, "search_rerank", record["subsystem"])
		require.Equal(t, "local", record["provider"])
		require.Equal(t, "/test/models/test-reranker.gguf", record["model_path"])
		require.Equal(t, "server starts immediately; reranking available after model loads", record["note"])
	})
}

func TestEmbeddingModelLoadingLogPreservesFieldsAcrossLocales(t *testing.T) {
	embedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"embedding":[0.1,0.2,0.3,0.4],"index":0}]}`))
	}))
	t.Cleanup(embedServer.Close)

	for _, test := range []struct {
		name    string
		tag     language.Tag
		message string
	}{
		{name: "English", tag: language.AmericanEnglish, message: "loading embedding model"},
		{name: "Spanish", tag: language.EuropeanSpanish, message: "cargando modelo de incrustaciones"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, err := nornicdb.Open("", nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })

			var output bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&output, nil))
			manager, err := localization.NewManager([]language.Tag{test.tag}, logger)
			require.NoError(t, err)
			config := DefaultConfig()
			config.MCPEnabled = false
			config.EmbeddingEnabled = true
			config.EmbeddingProvider = "openai"
			config.EmbeddingAPIURL = embedServer.URL
			config.EmbeddingAPIKey = "test-key"
			config.EmbeddingModel = "text-embedding-3-small"
			config.EmbeddingDimensions = 4
			config.Logger = logger
			config.Localizer = manager

			server, err := New(db, nil, config)
			require.NoError(t, err)
			require.NoError(t, server.Stop(context.Background()))

			record := findJSONLogRecord(t, output.Bytes(), "server.embedding.model_loading", "loading embedding model")
			require.Equal(t, test.message, record["msg"])
			require.Equal(t, "server.embedding.model_loading", record["event_id"])
			require.Equal(t, "embed_init", record["subsystem"])
			require.Equal(t, "text-embedding-3-small", record["model"])
			require.Equal(t, "openai", record["provider"])
			require.Equal(t, "server starts immediately; embeddings available after model loads", record["note"])
		})
	}
}

func TestSlowQueryLoggingEnabledLogPreservesFieldsAcrossLocales(t *testing.T) {
	testServerLogAcrossLocales(t, "server.slow_query.enabled", "slow query logging enabled", "registro de consultas lentas habilitado", func(config *Config) {
		config.SlowQueryEnabled = true
		config.Logging.SlowQueryThreshold = 250 * time.Millisecond
	}, func(t *testing.T, record map[string]any) {
		require.Equal(t, "slow_query", record["subsystem"])
		require.Equal(t, float64((250 * time.Millisecond).Nanoseconds()), record["threshold"])
	})
}

func TestSlowQueryLoggingConfiguredLogPreservesFieldsAcrossLocales(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "slow.log")
	testServerLogAcrossLocales(t, "server.slow_query.configured", "slow query logging configured", "registro de consultas lentas configurado", func(config *Config) {
		config.SlowQueryEnabled = true
		config.Logging.SlowQueryThreshold = 500 * time.Millisecond
		config.Logging.SlowQueryLogFile = logPath
	}, func(t *testing.T, record map[string]any) {
		require.Equal(t, "slow_query", record["subsystem"])
		require.Equal(t, logPath, record["file"])
		require.Equal(t, float64((500 * time.Millisecond).Nanoseconds()), record["threshold"])
	})
}

func TestSlowQueryLogOpenFailedLogPreservesFieldsAcrossLocales(t *testing.T) {
	logPath := t.TempDir()
	testServerLogAcrossLocales(t, "server.slow_query.open_failed", "failed to open slow query log file", "no se pudo abrir el archivo de registro de consultas lentas", func(config *Config) {
		config.SlowQueryEnabled = true
		config.Logging.SlowQueryLogFile = logPath
	}, func(t *testing.T, record map[string]any) {
		require.Equal(t, "slow_query", record["subsystem"])
		require.Equal(t, logPath, record["file"])
		require.NotEmpty(t, record["error"])
	})
}

func TestHTTP2EnabledLogPreservesFieldsAcrossLocales(t *testing.T) {
	for _, test := range []struct {
		name    string
		tag     language.Tag
		message string
	}{
		{name: "English", tag: language.AmericanEnglish, message: "HTTP/2 enabled"},
		{name: "Spanish", tag: language.EuropeanSpanish, message: "HTTP/2 habilitado"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, err := nornicdb.Open("", nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })

			var output bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&output, nil))
			manager, err := localization.NewManager([]language.Tag{test.tag}, logger)
			require.NoError(t, err)
			config := DefaultConfig()
			config.Port = 0
			config.MCPEnabled = false
			config.EmbeddingEnabled = false
			config.Logger = logger
			config.Localizer = manager

			server, err := New(db, nil, config)
			require.NoError(t, err)
			require.NoError(t, server.Start())
			t.Cleanup(func() { require.NoError(t, server.Stop(context.Background())) })

			record := findJSONLogRecord(t, output.Bytes(), "server.http2.enabled", "HTTP/2 enabled")
			require.Equal(t, test.message, record["msg"])
			require.Equal(t, "server.http2.enabled", record["event_id"])
			require.Equal(t, "server", record["component"])
			require.Equal(t, "h2c_cleartext", record["mode"])
			require.Equal(t, "http/1.1", record["compat"])
		})
	}
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
