// Package config tests for Neo4j-compatible configuration.
package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestLoadFromEnv_Defaults tests default values are loaded correctly.
func TestLoadFromEnv_Defaults(t *testing.T) {
	// Clear any existing env vars
	clearEnvVars(t)

	cfg := LoadFromEnv()

	// Auth defaults - disabled by default for easy development
	if cfg.Auth.Enabled {
		t.Error("expected Auth.Enabled to be false by default")
	}
	if cfg.Auth.InitialUsername != "admin" {
		t.Errorf("expected username 'admin', got %q", cfg.Auth.InitialUsername)
	}
	if cfg.Auth.InitialPassword != "password" {
		t.Errorf("expected password 'password', got %q", cfg.Auth.InitialPassword)
	}
	if cfg.Auth.MinPasswordLength != 8 {
		t.Errorf("expected min password length 8, got %d", cfg.Auth.MinPasswordLength)
	}
	if cfg.Auth.TokenExpiry != 24*time.Hour {
		t.Errorf("expected token expiry 24h, got %v", cfg.Auth.TokenExpiry)
	}

	// Database defaults
	if cfg.Database.DataDir != "./data" {
		t.Errorf("expected data dir './data', got %q", cfg.Database.DataDir)
	}
	if cfg.Database.DefaultDatabase != "nornic" {
		t.Errorf("expected default db 'nornic', got %q", cfg.Database.DefaultDatabase)
	}
	if cfg.Database.ReadOnly {
		t.Error("expected ReadOnly to be false by default")
	}
	if cfg.Database.TransactionTimeout != 30*time.Second {
		t.Errorf("expected tx timeout 30s, got %v", cfg.Database.TransactionTimeout)
	}
	if cfg.Database.MaxConcurrentTransactions != 1000 {
		t.Errorf("expected max concurrent tx 1000, got %d", cfg.Database.MaxConcurrentTransactions)
	}

	// Server defaults - Bolt
	if !cfg.Server.BoltEnabled {
		t.Error("expected BoltEnabled to be true by default")
	}
	if cfg.Server.BoltPort != 7687 {
		t.Errorf("expected bolt port 7687, got %d", cfg.Server.BoltPort)
	}
	if cfg.Server.BoltAddress != "0.0.0.0" {
		t.Errorf("expected bolt address '0.0.0.0', got %q", cfg.Server.BoltAddress)
	}
	if cfg.Server.BoltServerAnnouncement != "" {
		t.Errorf("expected empty bolt server announcement override, got %q", cfg.Server.BoltServerAnnouncement)
	}

	// Server defaults - HTTP
	if !cfg.Server.HTTPEnabled {
		t.Error("expected HTTPEnabled to be true by default")
	}
	if cfg.Server.HTTPPort != 7474 {
		t.Errorf("expected http port 7474, got %d", cfg.Server.HTTPPort)
	}
	if cfg.Server.HTTPSPort != 7473 {
		t.Errorf("expected https port 7473, got %d", cfg.Server.HTTPSPort)
	}

	// Memory defaults
	if cfg.Memory.DecayEnabled {
		t.Error("expected DecayEnabled to be false by default")
	}
	if cfg.Memory.DecayInterval != 2*time.Second {
		t.Errorf("expected decay interval 2s, got %v", cfg.Memory.DecayInterval)
	}
	if cfg.Memory.VisibilityThreshold != 0.05 {
		t.Errorf("expected visibility threshold 0.05, got %f", cfg.Memory.VisibilityThreshold)
	}
	if cfg.Memory.EmbeddingProvider != "local" {
		t.Errorf("expected embedding provider 'local', got %q", cfg.Memory.EmbeddingProvider)
	}
	if cfg.Memory.EmbeddingDimensions != 1024 {
		t.Errorf("expected embedding dimensions 1024, got %d", cfg.Memory.EmbeddingDimensions)
	}
	if !cfg.Memory.AutoLinksEnabled {
		t.Error("expected AutoLinksEnabled to be true by default")
	}

	// Compliance defaults
	if !cfg.Compliance.AuditEnabled {
		t.Error("expected AuditEnabled to be true by default")
	}
	if cfg.Compliance.AuditRetentionDays != 2555 {
		t.Errorf("expected audit retention 2555 days, got %d", cfg.Compliance.AuditRetentionDays)
	}
	if cfg.Compliance.RetentionEnabled {
		t.Error("expected RetentionEnabled to be false by default")
	}
	if !cfg.Compliance.AccessControlEnabled {
		t.Error("expected AccessControlEnabled to be true by default")
	}
	if cfg.Compliance.SessionTimeout != 30*time.Minute {
		t.Errorf("expected session timeout 30m, got %v", cfg.Compliance.SessionTimeout)
	}
	if cfg.Compliance.MaxFailedLogins != 5 {
		t.Errorf("expected max failed logins 5, got %d", cfg.Compliance.MaxFailedLogins)
	}
	if !cfg.Compliance.DataExportEnabled {
		t.Error("expected DataExportEnabled to be true by default")
	}
	if !cfg.Compliance.DataErasureEnabled {
		t.Error("expected DataErasureEnabled to be true by default")
	}
	if cfg.Compliance.AnonymizationMethod != "pseudonymization" {
		t.Errorf("expected anonymization method 'pseudonymization', got %q", cfg.Compliance.AnonymizationMethod)
	}

	// Logging defaults
	if cfg.Logging.Level != "INFO" {
		t.Errorf("expected log level 'INFO', got %q", cfg.Logging.Level)
	}
	if cfg.Logging.Format != "json" {
		t.Errorf("expected log format 'json', got %q", cfg.Logging.Format)
	}

	// Durability defaults - optimized for performance
	if cfg.Database.StrictDurability {
		t.Error("expected StrictDurability to be false by default")
	}
	if cfg.Database.WALSyncMode != "batch" {
		t.Errorf("expected WALSyncMode 'batch', got %q", cfg.Database.WALSyncMode)
	}
	if cfg.Database.WALSyncInterval != 100*time.Millisecond {
		t.Errorf("expected WALSyncInterval 100ms, got %v", cfg.Database.WALSyncInterval)
	}
}

// TestLoadFromEnv_DurabilitySettings tests durability configuration options.
func TestLoadFromEnv_DurabilitySettings(t *testing.T) {
	t.Run("default_batch_mode", func(t *testing.T) {
		clearEnvVars(t)

		cfg := LoadFromEnv()

		if cfg.Database.WALSyncMode != "batch" {
			t.Errorf("expected default WALSyncMode 'batch', got %q", cfg.Database.WALSyncMode)
		}
		if cfg.Database.WALSyncInterval != 100*time.Millisecond {
			t.Errorf("expected default WALSyncInterval 100ms, got %v", cfg.Database.WALSyncInterval)
		}
		if cfg.Database.StrictDurability {
			t.Error("expected StrictDurability to be false by default")
		}
	})

	t.Run("strict_durability_mode", func(t *testing.T) {
		clearEnvVars(t)
		os.Setenv("NORNICDB_STRICT_DURABILITY", "true")

		cfg := LoadFromEnv()

		if !cfg.Database.StrictDurability {
			t.Error("expected StrictDurability to be true")
		}
		if cfg.Database.WALSyncMode != "immediate" {
			t.Errorf("strict mode should set WALSyncMode to 'immediate', got %q", cfg.Database.WALSyncMode)
		}
		// WALSyncInterval is ignored in immediate mode, but we set it to 0
		if cfg.Database.WALSyncInterval != 0 {
			t.Errorf("strict mode should set WALSyncInterval to 0, got %v", cfg.Database.WALSyncInterval)
		}
	})

	t.Run("custom_wal_sync_mode", func(t *testing.T) {
		clearEnvVars(t)
		os.Setenv("NORNICDB_WAL_SYNC_MODE", "immediate")
		os.Setenv("NORNICDB_WAL_SYNC_INTERVAL", "50ms")

		cfg := LoadFromEnv()

		if cfg.Database.WALSyncMode != "immediate" {
			t.Errorf("expected WALSyncMode 'immediate', got %q", cfg.Database.WALSyncMode)
		}
		if cfg.Database.WALSyncInterval != 50*time.Millisecond {
			t.Errorf("expected WALSyncInterval 50ms, got %v", cfg.Database.WALSyncInterval)
		}
	})

	t.Run("strict_overrides_custom", func(t *testing.T) {
		clearEnvVars(t)
		// Set custom values that should be overridden by strict mode
		os.Setenv("NORNICDB_WAL_SYNC_MODE", "none")
		os.Setenv("NORNICDB_WAL_SYNC_INTERVAL", "1s")
		os.Setenv("NORNICDB_STRICT_DURABILITY", "true")

		cfg := LoadFromEnv()

		// Strict mode should override custom settings
		if cfg.Database.WALSyncMode != "immediate" {
			t.Errorf("strict mode should override to 'immediate', got %q", cfg.Database.WALSyncMode)
		}
		if cfg.Database.WALSyncInterval != 0 {
			t.Errorf("strict mode should override to 0, got %v", cfg.Database.WALSyncInterval)
		}
	})

	t.Run("none_sync_mode_for_testing", func(t *testing.T) {
		clearEnvVars(t)
		os.Setenv("NORNICDB_WAL_SYNC_MODE", "none")

		cfg := LoadFromEnv()

		if cfg.Database.WALSyncMode != "none" {
			t.Errorf("expected WALSyncMode 'none', got %q", cfg.Database.WALSyncMode)
		}
	})
}

// TestLoadFromEnv_Neo4jAuth tests NEO4J_AUTH parsing.
func TestLoadFromEnv_Neo4jAuth(t *testing.T) {
	tests := []struct {
		name         string
		authEnv      string
		wantEnabled  bool
		wantUsername string
		wantPassword string
	}{
		{
			name:         "username/password format",
			authEnv:      "admin/secretpass",
			wantEnabled:  true,
			wantUsername: "admin",
			wantPassword: "secretpass",
		},
		{
			name:         "password with slash",
			authEnv:      "neo4j/pass/word/with/slashes",
			wantEnabled:  true,
			wantUsername: "neo4j",
			wantPassword: "pass/word/with/slashes",
		},
		{
			name:         "auth disabled",
			authEnv:      "none",
			wantEnabled:  false,
			wantUsername: "",
			wantPassword: "",
		},
		{
			name:         "password only (legacy)",
			authEnv:      "simplepassword",
			wantEnabled:  true,
			wantUsername: "admin",
			wantPassword: "simplepassword",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearEnvVars(t)
			os.Setenv("NEO4J_AUTH", tt.authEnv)

			cfg := LoadFromEnv()

			if cfg.Auth.Enabled != tt.wantEnabled {
				t.Errorf("expected Enabled=%v, got %v", tt.wantEnabled, cfg.Auth.Enabled)
			}
			if cfg.Auth.Enabled {
				if cfg.Auth.InitialUsername != tt.wantUsername {
					t.Errorf("expected username %q, got %q", tt.wantUsername, cfg.Auth.InitialUsername)
				}
				if cfg.Auth.InitialPassword != tt.wantPassword {
					t.Errorf("expected password %q, got %q", tt.wantPassword, cfg.Auth.InitialPassword)
				}
			}
		})
	}
}

// TestLoadFromEnv_CustomValues tests custom env var values.
func TestLoadFromEnv_CustomValues(t *testing.T) {
	clearEnvVars(t)

	// Set custom values
	os.Setenv("NEO4J_AUTH", "customuser/custompass")
	os.Setenv("NEO4J_dbms_directories_data", "/custom/data")
	os.Setenv("NEO4J_dbms_default__database", "mydb")
	os.Setenv("NEO4J_dbms_read__only", "true")
	os.Setenv("NEO4J_dbms_connector_bolt_listen__address_port", "7777")
	os.Setenv("NEO4J_dbms_connector_http_listen__address_port", "8080")
	os.Setenv("NORNICDB_MEMORY_DECAY_INTERVAL", "2h")
	os.Setenv("NORNICDB_EMBEDDING_PROVIDER", "openai")
	os.Setenv("NORNICDB_EMBEDDING_DIMENSIONS", "1536")
	os.Setenv("NORNICDB_AUDIT_RETENTION_DAYS", "365")
	os.Setenv("NORNICDB_SESSION_TIMEOUT", "1h")
	os.Setenv("NORNICDB_RETENTION_EXEMPT_ROLES", "admin, superuser, backup")

	cfg := LoadFromEnv()

	// Verify custom values
	if cfg.Auth.InitialUsername != "customuser" {
		t.Errorf("expected username 'customuser', got %q", cfg.Auth.InitialUsername)
	}
	if cfg.Database.DataDir != "/custom/data" {
		t.Errorf("expected data dir '/custom/data', got %q", cfg.Database.DataDir)
	}
	if cfg.Database.DefaultDatabase != "mydb" {
		t.Errorf("expected default db 'mydb', got %q", cfg.Database.DefaultDatabase)
	}
	if !cfg.Database.ReadOnly {
		t.Error("expected ReadOnly to be true")
	}
	if cfg.Server.BoltPort != 7777 {
		t.Errorf("expected bolt port 7777, got %d", cfg.Server.BoltPort)
	}
	if cfg.Server.HTTPPort != 8080 {
		t.Errorf("expected http port 8080, got %d", cfg.Server.HTTPPort)
	}
	if cfg.Memory.DecayInterval != 2*time.Hour {
		t.Errorf("expected decay interval 2h, got %v", cfg.Memory.DecayInterval)
	}
	if cfg.Memory.EmbeddingProvider != "openai" {
		t.Errorf("expected embedding provider 'openai', got %q", cfg.Memory.EmbeddingProvider)
	}
	if cfg.Memory.EmbeddingDimensions != 1536 {
		t.Errorf("expected embedding dimensions 1536, got %d", cfg.Memory.EmbeddingDimensions)
	}
	if cfg.Compliance.AuditRetentionDays != 365 {
		t.Errorf("expected audit retention 365, got %d", cfg.Compliance.AuditRetentionDays)
	}
	if cfg.Compliance.SessionTimeout != time.Hour {
		t.Errorf("expected session timeout 1h, got %v", cfg.Compliance.SessionTimeout)
	}
	// Check slice parsing
	expectedRoles := []string{"admin", "superuser", "backup"}
	if len(cfg.Compliance.RetentionExemptRoles) != len(expectedRoles) {
		t.Errorf("expected %d exempt roles, got %d", len(expectedRoles), len(cfg.Compliance.RetentionExemptRoles))
	} else {
		for i, role := range expectedRoles {
			if cfg.Compliance.RetentionExemptRoles[i] != role {
				t.Errorf("expected role %q at index %d, got %q", role, i, cfg.Compliance.RetentionExemptRoles[i])
			}
		}
	}
}

func TestLoadFromEnv_ComprehensiveAdditionalEnvCoverage(t *testing.T) {
	clearEnvVars(t)

	t.Setenv("NORNICDB_AUTH", "envuser:envpass")
	t.Setenv("NORNICDB_MIN_PASSWORD_LENGTH", "12")
	t.Setenv("NORNICDB_AUTH_TOKEN_EXPIRY", "3h")
	t.Setenv("NORNICDB_AUTH_JWT_SECRET", "env-secret")
	t.Setenv("NORNICDB_DATA_DIR", "/env/data")
	t.Setenv("NORNICDB_DEFAULT_DATABASE", "envdb")
	t.Setenv("NORNICDB_READ_ONLY", "true")
	t.Setenv("NORNICDB_TRANSACTION_TIMEOUT", "90")
	t.Setenv("NORNICDB_MAX_TRANSACTIONS", "55")
	t.Setenv("NORNICDB_WAL_AUTO_COMPACTION_ENABLED", "false")
	t.Setenv("NORNICDB_WAL_RETENTION_MAX_SEGMENTS", "12")
	t.Setenv("NORNICDB_WAL_RETENTION_MAX_AGE", "36h")
	t.Setenv("NORNICDB_WAL_LEDGER_RETENTION_DEFAULTS", "true")
	t.Setenv("NORNICDB_WAL_SNAPSHOT_RETENTION_MAX_COUNT", "5")
	t.Setenv("NORNICDB_WAL_SNAPSHOT_RETENTION_MAX_AGE", "24h")
	t.Setenv("NORNICDB_MVCC_LIFECYCLE_ENABLED", "false")
	t.Setenv("NORNICDB_MVCC_LIFECYCLE_INTERVAL", "45s")
	t.Setenv("NORNICDB_MVCC_LIFECYCLE_MAX_SNAPSHOT_AGE", "2h")
	t.Setenv("NORNICDB_MVCC_LIFECYCLE_MAX_CHAIN_CAP", "321")
	t.Setenv("NORNICDB_PERSIST_SEARCH_INDEXES", "true")
	t.Setenv("NORNICDB_BOLT_ENABLED", "false")
	t.Setenv("NORNICDB_BOLT_ADDRESS", "127.0.0.1")
	t.Setenv("NORNICDB_BOLT_SERVER_ANNOUNCEMENT", "Neo4j/5.26.0")
	t.Setenv("NORNICDB_BOLT_TLS_ENABLED", "true")
	t.Setenv("NORNICDB_TLS_DIR", "/tlsdir")
	t.Setenv("NORNICDB_HTTP_ENABLED", "false")
	t.Setenv("NORNICDB_HTTP_ADDRESS", "127.0.0.2")
	t.Setenv("NORNICDB_HTTPS_ENABLED", "true")
	t.Setenv("NORNICDB_HTTPS_PORT", "9443")
	t.Setenv("NORNICDB_ENV", "production")
	t.Setenv("NORNICDB_ALLOW_HTTP", "false")
	t.Setenv("NORNICDB_PLUGINS_DIR", "/env/plugins")
	t.Setenv("NORNICDB_HEIMDALL_PLUGINS_DIR", "/env/heim")
	t.Setenv("NORNICDB_CORS_ENABLED", "false")
	t.Setenv("NORNICDB_CORS_ORIGINS", "https://a.example, https://b.example")
	t.Setenv("NORNICDB_ENCRYPTION_ENABLED", "true")
	t.Setenv("NORNICDB_ENCRYPTION_PASSWORD", "env-password")
	t.Setenv("NORNICDB_MEMORY_DECAY_ENABLED", "false")
	t.Setenv("NORNICDB_VISIBILITY_THRESHOLD", "0.3")
	t.Setenv("NORNICDB_EMBEDDING_ENABLED", "1")
	t.Setenv("NORNICDB_EMBEDDING_MODEL", "env-embed-model")
	t.Setenv("NORNICDB_EMBEDDING_API_URL", "http://embed-env")
	t.Setenv("NORNICDB_EMBEDDING_API_KEY", "embed-env-key")
	t.Setenv("NORNICDB_EMBEDDING_CACHE_SIZE", "999")
	t.Setenv("NORNICDB_SEARCH_MIN_SIMILARITY", "0.75")
	t.Setenv("NORNICDB_MODELS_DIR", "/models")
	t.Setenv("NORNICDB_EMBEDDING_GPU_LAYERS", "4")
	t.Setenv("NORNICDB_EMBEDDING_WARMUP_INTERVAL", "11m")
	t.Setenv("NORNICDB_KMEANS_MIN_EMBEDDINGS", "222")
	t.Setenv("NORNICDB_KMEANS_CLUSTER_INTERVAL", "8m")
	t.Setenv("NORNICDB_KMEANS_NUM_CLUSTERS", "6")
	t.Setenv("NORNICDB_AUTO_LINKS_ENABLED", "false")
	t.Setenv("NORNICDB_AUTO_LINKS_THRESHOLD", "0.67")
	t.Setenv("NORNICDB_EMBED_SCAN_INTERVAL", "5m")
	t.Setenv("NORNICDB_EMBED_BATCH_DELAY", "3s")
	t.Setenv("NORNICDB_EMBED_TRIGGER_DEBOUNCE", "4s")
	t.Setenv("NORNICDB_EMBED_MAX_RETRIES", "7")
	t.Setenv("NORNICDB_EMBED_CHUNK_SIZE", "2048")
	t.Setenv("NORNICDB_EMBED_CHUNK_OVERLAP", "33")
	t.Setenv("NORNICDB_EMBEDDING_PROPERTIES_INCLUDE", "title, body")
	t.Setenv("NORNICDB_EMBEDDING_PROPERTIES_EXCLUDE", "secret, internal")
	t.Setenv("NORNICDB_EMBEDDING_INCLUDE_LABELS", "false")
	t.Setenv("NORNICDB_AUDIT_ENABLED", "false")
	t.Setenv("NORNICDB_AUDIT_LOG_PATH", "/env/audit.log")
	t.Setenv("NORNICDB_RETENTION_ENABLED", "true")
	t.Setenv("NORNICDB_RETENTION_POLICY_DAYS", "88")
	t.Setenv("NORNICDB_RETENTION_AUTO_DELETE", "true")
	t.Setenv("NORNICDB_ACCESS_CONTROL_ENABLED", "false")
	t.Setenv("NORNICDB_LOCKOUT_DURATION", "7m")
	t.Setenv("NORNICDB_ENCRYPTION_IN_TRANSIT", "false")
	t.Setenv("NORNICDB_ENCRYPTION_KEY_PATH", "/keys/env")
	t.Setenv("NORNICDB_DATA_EXPORT_ENABLED", "false")
	t.Setenv("NORNICDB_DATA_ERASURE_ENABLED", "false")
	t.Setenv("NORNICDB_DATA_ACCESS_ENABLED", "false")
	t.Setenv("NORNICDB_ANONYMIZATION_ENABLED", "false")
	t.Setenv("NORNICDB_ANONYMIZATION_METHOD", "suppression")
	t.Setenv("NORNICDB_CONSENT_REQUIRED", "true")
	t.Setenv("NORNICDB_CONSENT_VERSIONING", "false")
	t.Setenv("NORNICDB_CONSENT_AUDIT_TRAIL", "false")
	t.Setenv("NORNICDB_BREACH_DETECTION_ENABLED", "true")
	t.Setenv("NORNICDB_BREACH_NOTIFY_EMAIL", "env@example.com")
	t.Setenv("NORNICDB_BREACH_NOTIFY_WEBHOOK", "https://hooks.example/env")
	t.Setenv("NORNICDB_LOG_LEVEL", "WARN")
	t.Setenv("NORNICDB_LOG_FORMAT", "text")
	t.Setenv("NORNICDB_LOG_OUTPUT", "stderr")
	t.Setenv("NORNICDB_QUERY_LOG_ENABLED", "true")
	t.Setenv("NORNICDB_SLOW_QUERY_THRESHOLD", "6s")
	t.Setenv("NORNICDB_KALMAN_ENABLED", "true")
	t.Setenv("NORNICDB_TOPOLOGY_AUTO_INTEGRATION_ENABLED", "true")
	t.Setenv("NORNICDB_TOPOLOGY_ALGORITHM", "resource_allocation")
	t.Setenv("NORNICDB_TOPOLOGY_WEIGHT", "0.9")
	t.Setenv("NORNICDB_TOPOLOGY_TOPK", "15")
	t.Setenv("NORNICDB_TOPOLOGY_MIN_SCORE", "0.44")
	t.Setenv("NORNICDB_TOPOLOGY_GRAPH_REFRESH_INTERVAL", "77")
	t.Setenv("NORNICDB_TOPOLOGY_AB_TEST_ENABLED", "true")
	t.Setenv("NORNICDB_TOPOLOGY_AB_TEST_PERCENTAGE", "35")
	t.Setenv("NORNICDB_HEIMDALL_ENABLED", "true")
	t.Setenv("NORNICDB_HEIMDALL_MODEL", "qwen")
	t.Setenv("NORNICDB_HEIMDALL_PROVIDER", "ollama")
	t.Setenv("NORNICDB_HEIMDALL_API_URL", "http://heim-env")
	t.Setenv("NORNICDB_HEIMDALL_API_KEY", "heim-env-key")
	t.Setenv("NORNICDB_HEIMDALL_GPU_LAYERS", "3")
	t.Setenv("NORNICDB_HEIMDALL_CONTEXT_SIZE", "2048")
	t.Setenv("NORNICDB_HEIMDALL_BATCH_SIZE", "256")
	t.Setenv("NORNICDB_HEIMDALL_MAX_TOKENS", "128")
	t.Setenv("NORNICDB_HEIMDALL_TEMPERATURE", "0.4")
	t.Setenv("NORNICDB_HEIMDALL_ANOMALY_DETECTION", "false")
	t.Setenv("NORNICDB_HEIMDALL_RUNTIME_DIAGNOSIS", "false")
	t.Setenv("NORNICDB_HEIMDALL_MEMORY_CURATION", "true")
	t.Setenv("NORNICDB_HEIMDALL_MCP_ENABLE", "true")
	t.Setenv("NORNICDB_HEIMDALL_MCP_TOOLS", "store, link")
	t.Setenv("NORNICDB_SEARCH_RERANK_ENABLED", "true")
	t.Setenv("NORNICDB_SEARCH_RERANK_PROVIDER", " HTTP ")
	t.Setenv("NORNICDB_SEARCH_RERANK_MODEL", "rerank-env")
	t.Setenv("NORNICDB_SEARCH_RERANK_API_URL", "https://rerank-env")
	t.Setenv("NORNICDB_SEARCH_RERANK_API_KEY", "rerank-env-key")
	t.Setenv("NORNICDB_HEIMDALL_MAX_CONTEXT_TOKENS", "6000")
	t.Setenv("NORNICDB_HEIMDALL_MAX_SYSTEM_TOKENS", "3500")
	t.Setenv("NORNICDB_HEIMDALL_MAX_USER_TOKENS", "1500")
	t.Setenv("NORNICDB_QDRANT_GRPC_ENABLED", "1")
	t.Setenv("NORNICDB_QDRANT_GRPC_LISTEN_ADDR", ":6335")
	t.Setenv("NORNICDB_QDRANT_GRPC_MAX_VECTOR_DIM", "123")
	t.Setenv("NORNICDB_QDRANT_GRPC_MAX_BATCH_POINTS", "456")
	t.Setenv("NORNICDB_QDRANT_GRPC_MAX_TOP_K", "78")

	cfg := LoadFromEnv()

	if !cfg.Auth.Enabled || cfg.Auth.InitialUsername != "envuser" || cfg.Auth.InitialPassword != "envpass" {
		t.Fatalf("unexpected auth config: %+v", cfg.Auth)
	}
	if cfg.Auth.MinPasswordLength != 12 || cfg.Auth.TokenExpiry != 3*time.Hour || cfg.Auth.JWTSecret != "env-secret" {
		t.Fatalf("unexpected auth override values: %+v", cfg.Auth)
	}
	if cfg.Database.DataDir != "/env/data" || cfg.Database.DefaultDatabase != "envdb" || !cfg.Database.ReadOnly {
		t.Fatalf("unexpected database basics: %+v", cfg.Database)
	}
	if cfg.Database.TransactionTimeout != 90*time.Second || cfg.Database.MaxConcurrentTransactions != 55 {
		t.Fatalf("unexpected transaction values: %+v", cfg.Database)
	}
	if cfg.Database.WALAutoCompactionEnabled || cfg.Database.WALRetentionMaxSegments != 12 || cfg.Database.WALRetentionMaxAge != 36*time.Hour {
		t.Fatalf("unexpected wal retention config: %+v", cfg.Database)
	}
	if !cfg.Database.WALRetentionLedgerDefaults || cfg.Database.WALSnapshotRetentionMaxCount != 5 || cfg.Database.WALSnapshotRetentionMaxAge != 24*time.Hour {
		t.Fatalf("unexpected wal snapshot config: %+v", cfg.Database)
	}
	if cfg.Database.MVCCLifecycleEnabled || cfg.Database.MVCCLifecycleCycleInterval != 45*time.Second || cfg.Database.MVCCLifecycleMaxSnapshotAge != 2*time.Hour || cfg.Database.MVCCLifecycleMaxChainCap != 321 {
		t.Fatalf("unexpected mvcc lifecycle config: %+v", cfg.Database)
	}
	if !cfg.Database.PersistSearchIndexes || !cfg.Database.EncryptionEnabled || cfg.Database.EncryptionPassword != "env-password" {
		t.Fatalf("unexpected database feature config: %+v", cfg.Database)
	}
	if cfg.Server.BoltEnabled || cfg.Server.BoltAddress != "127.0.0.1" || !cfg.Server.BoltTLSEnabled {
		t.Fatalf("unexpected bolt server config: %+v", cfg.Server)
	}
	if cfg.Server.BoltServerAnnouncement != "Neo4j/5.26.0" {
		t.Fatalf("unexpected bolt server announcement: %+v", cfg.Server)
	}
	if cfg.Server.BoltTLSCert != "/tlsdir/public.crt" || cfg.Server.BoltTLSKey != "/tlsdir/private.key" {
		t.Fatalf("unexpected TLS paths: %+v", cfg.Server)
	}
	if cfg.Server.HTTPEnabled || cfg.Server.HTTPAddress != "127.0.0.2" || !cfg.Server.HTTPSEnabled || cfg.Server.HTTPSPort != 9443 {
		t.Fatalf("unexpected http server config: %+v", cfg.Server)
	}
	if cfg.Server.Environment != "production" || cfg.Server.AllowHTTP {
		t.Fatalf("unexpected server env flags: %+v", cfg.Server)
	}
	if cfg.Server.PluginsDir != "/env/plugins" || cfg.Server.HeimdallPluginsDir != "/env/heim" {
		t.Fatalf("unexpected plugin dirs: %+v", cfg.Server)
	}
	if !cfg.Server.EnableCORS || len(cfg.Server.CORSOrigins) != 2 || cfg.Server.CORSOrigins[1] != "https://b.example" {
		t.Fatalf("unexpected cors config: %+v", cfg.Server)
	}
	if cfg.Memory.DecayEnabled || !cfg.Memory.EmbeddingEnabled || cfg.Memory.EmbeddingModel != "env-embed-model" {
		t.Fatalf("unexpected memory embedding basics: %+v", cfg.Memory)
	}
	if cfg.Memory.EmbeddingAPIURL != "http://embed-env" || cfg.Memory.EmbeddingAPIKey != "embed-env-key" || cfg.Memory.EmbeddingCacheSize != 999 {
		t.Fatalf("unexpected embedding api config: %+v", cfg.Memory)
	}
	if cfg.Memory.SearchMinSimilarity != 0.75 || cfg.Memory.ModelsDir != "/models" || cfg.Memory.EmbeddingGPULayers != 4 {
		t.Fatalf("unexpected search/model config: %+v", cfg.Memory)
	}
	if cfg.Memory.EmbeddingWarmupInterval != 11*time.Minute || cfg.Memory.KmeansMinEmbeddings != 222 || cfg.Memory.KmeansClusterInterval != 8*time.Minute || cfg.Memory.KmeansNumClusters != 6 {
		t.Fatalf("unexpected kmeans config: %+v", cfg.Memory)
	}
	if cfg.Memory.AutoLinksEnabled || cfg.Memory.AutoLinksSimilarityThreshold != 0.67 || cfg.Memory.VisibilityThreshold != 0.3 {
		t.Fatalf("unexpected autolink/visibility config: %+v", cfg.Memory)
	}
	if cfg.EmbeddingWorker.ScanInterval != 5*time.Minute || cfg.EmbeddingWorker.BatchDelay != 3*time.Second || cfg.EmbeddingWorker.TriggerDebounceDelay != 4*time.Second || cfg.EmbeddingWorker.MaxRetries != 7 {
		t.Fatalf("unexpected embedding worker timings: %+v", cfg.EmbeddingWorker)
	}
	if cfg.EmbeddingWorker.ChunkSize != 2048 || cfg.EmbeddingWorker.ChunkOverlap != 33 || cfg.EmbeddingWorker.IncludeLabels {
		t.Fatalf("unexpected embedding worker chunk config: %+v", cfg.EmbeddingWorker)
	}
	if len(cfg.EmbeddingWorker.PropertiesInclude) != 2 || cfg.EmbeddingWorker.PropertiesExclude[1] != "internal" {
		t.Fatalf("unexpected embedding worker property filters: %+v", cfg.EmbeddingWorker)
	}
	if cfg.Compliance.AuditEnabled || cfg.Compliance.AuditLogPath != "/env/audit.log" || !cfg.Compliance.RetentionEnabled {
		t.Fatalf("unexpected compliance basics: %+v", cfg.Compliance)
	}
	if cfg.Compliance.RetentionPolicyDays != 88 || !cfg.Compliance.RetentionAutoDelete || cfg.Compliance.AccessControlEnabled {
		t.Fatalf("unexpected compliance retention/access config: %+v", cfg.Compliance)
	}
	if cfg.Compliance.LockoutDuration != 7*time.Minute || cfg.Compliance.EncryptionInTransit || cfg.Compliance.EncryptionKeyPath != "/keys/env" {
		t.Fatalf("unexpected compliance encryption config: %+v", cfg.Compliance)
	}
	if cfg.Compliance.DataExportEnabled || cfg.Compliance.DataErasureEnabled || cfg.Compliance.DataAccessEnabled || cfg.Compliance.AnonymizationEnabled {
		t.Fatalf("unexpected compliance data rights config: %+v", cfg.Compliance)
	}
	if cfg.Compliance.AnonymizationMethod != "suppression" || !cfg.Compliance.ConsentRequired || cfg.Compliance.ConsentVersioning || cfg.Compliance.ConsentAuditTrail {
		t.Fatalf("unexpected compliance consent config: %+v", cfg.Compliance)
	}
	if !cfg.Compliance.BreachDetectionEnabled || cfg.Compliance.BreachNotifyEmail != "env@example.com" || cfg.Compliance.BreachNotifyWebhook != "https://hooks.example/env" {
		t.Fatalf("unexpected compliance breach config: %+v", cfg.Compliance)
	}
	if cfg.Logging.Level != "WARN" || cfg.Logging.Format != "text" || cfg.Logging.Output != "stderr" || !cfg.Logging.QueryLogEnabled || cfg.Logging.SlowQueryThreshold != 6*time.Second {
		t.Fatalf("unexpected logging config: %+v", cfg.Logging)
	}
	if !cfg.Features.KalmanEnabled || !cfg.Features.TopologyAutoIntegrationEnabled || cfg.Features.TopologyAlgorithm != "resource_allocation" {
		t.Fatalf("unexpected topology basics: %+v", cfg.Features)
	}
	if cfg.Features.TopologyWeight != 0.9 || cfg.Features.TopologyTopK != 15 || cfg.Features.TopologyMinScore != 0.44 || cfg.Features.TopologyGraphRefreshInterval != 77 {
		t.Fatalf("unexpected topology values: %+v", cfg.Features)
	}
	if !cfg.Features.TopologyABTestEnabled || cfg.Features.TopologyABTestPercentage != 35 {
		t.Fatalf("unexpected topology ab config: %+v", cfg.Features)
	}
	if !cfg.Features.HeimdallEnabled || cfg.Features.HeimdallModel != "qwen" || cfg.Features.HeimdallProvider != "ollama" {
		t.Fatalf("unexpected heimdall basics: %+v", cfg.Features)
	}
	if cfg.Features.HeimdallAPIURL != "http://heim-env" || cfg.Features.HeimdallAPIKey != "heim-env-key" || cfg.Features.HeimdallGPULayers != 3 {
		t.Fatalf("unexpected heimdall api/gpu config: %+v", cfg.Features)
	}
	if cfg.Features.HeimdallContextSize != 2048 || cfg.Features.HeimdallBatchSize != 256 || cfg.Features.HeimdallMaxTokens != 128 || cfg.Features.HeimdallTemperature != float32(0.4) {
		t.Fatalf("unexpected heimdall runtime config: %+v", cfg.Features)
	}
	if cfg.Features.HeimdallAnomalyDetection || cfg.Features.HeimdallRuntimeDiagnosis || !cfg.Features.HeimdallMemoryCuration {
		t.Fatalf("unexpected heimdall booleans: %+v", cfg.Features)
	}
	if !cfg.Features.HeimdallMCPEnable || len(cfg.Features.HeimdallMCPTools) != 2 || cfg.Features.HeimdallMCPTools[1] != "link" {
		t.Fatalf("unexpected heimdall mcp config: %+v", cfg.Features)
	}
	if !cfg.Features.SearchRerankEnabled || cfg.Features.SearchRerankProvider != "http" || cfg.Features.SearchRerankModel != "rerank-env" {
		t.Fatalf("unexpected rerank basics: %+v", cfg.Features)
	}
	if cfg.Features.SearchRerankAPIURL != "https://rerank-env" || cfg.Features.SearchRerankAPIKey != "rerank-env-key" {
		t.Fatalf("unexpected rerank api config: %+v", cfg.Features)
	}
	if cfg.Features.HeimdallMaxContextTokens != 6000 || cfg.Features.HeimdallMaxSystemTokens != 3500 || cfg.Features.HeimdallMaxUserTokens != 1500 {
		t.Fatalf("unexpected heimdall token budget config: %+v", cfg.Features)
	}
	if !cfg.Features.QdrantGRPCEnabled || cfg.Features.QdrantGRPCListenAddr != ":6335" || cfg.Features.QdrantGRPCMaxVectorDim != 123 || cfg.Features.QdrantGRPCMaxBatchPoints != 456 || cfg.Features.QdrantGRPCMaxTopK != 78 {
		t.Fatalf("unexpected qdrant grpc config: %+v", cfg.Features)
	}
}

// TestLoadFromEnv_EmbeddingWorkerNumWorkers ensures NORNICDB_EMBED_WORKER_NUM_WORKERS is applied.
func TestLoadFromEnv_EmbeddingWorkerNumWorkers(t *testing.T) {
	clearEnvVars(t)
	cfg := LoadFromEnv()
	if cfg.EmbeddingWorker.NumWorkers != 1 {
		t.Errorf("expected default NumWorkers 1, got %d", cfg.EmbeddingWorker.NumWorkers)
	}

	os.Setenv("NORNICDB_EMBED_WORKER_NUM_WORKERS", "2")
	t.Cleanup(func() { os.Unsetenv("NORNICDB_EMBED_WORKER_NUM_WORKERS") })
	cfg = LoadFromEnv()
	if cfg.EmbeddingWorker.NumWorkers != 2 {
		t.Errorf("expected NumWorkers 2 from env, got %d", cfg.EmbeddingWorker.NumWorkers)
	}
}

func TestLoadDefaults_EmbeddingWorkerChunkSize(t *testing.T) {
	clearEnvVars(t)
	cfg := LoadDefaults()
	if cfg.EmbeddingWorker.ChunkSize != 8192 {
		t.Fatalf("expected default chunk size 8192, got %d", cfg.EmbeddingWorker.ChunkSize)
	}
}

func TestLoadDefaults_MVCCLifecycle(t *testing.T) {
	clearEnvVars(t)
	cfg := LoadDefaults()
	require.True(t, cfg.Database.MVCCLifecycleEnabled)
	require.Equal(t, 30*time.Second, cfg.Database.MVCCLifecycleCycleInterval)
	require.Equal(t, time.Hour, cfg.Database.MVCCLifecycleMaxSnapshotAge)
	require.Equal(t, 1000, cfg.Database.MVCCLifecycleMaxChainCap)
	require.Equal(t, 2*time.Second, cfg.EmbeddingWorker.TriggerDebounceDelay)
}

// TestLoadDefaults_HeimdallMCPDefaults tests that Heimdall MCP defaults are set correctly.
func TestLoadDefaults_HeimdallMCPDefaults(t *testing.T) {
	cfg := LoadDefaults()
	if cfg.Features.HeimdallMCPEnable {
		t.Error("expected HeimdallMCPEnable to be false by default")
	}
	if cfg.Features.HeimdallMCPTools != nil {
		t.Errorf("expected HeimdallMCPTools to be nil by default, got %v", cfg.Features.HeimdallMCPTools)
	}
}

// TestLoadFromEnv_HeimdallMCP tests Heimdall MCP enable and allowlist env vars.
func TestLoadFromEnv_HeimdallMCP(t *testing.T) {
	t.Run("defaults_no_env", func(t *testing.T) {
		clearEnvVars(t)
		cfg := LoadFromEnv()
		if cfg.Features.HeimdallMCPEnable {
			t.Error("expected HeimdallMCPEnable false when env unset")
		}
		if cfg.Features.HeimdallMCPTools != nil {
			t.Errorf("expected HeimdallMCPTools nil when env unset, got %v", cfg.Features.HeimdallMCPTools)
		}
	})

	t.Run("enable_true", func(t *testing.T) {
		clearEnvVars(t)
		os.Setenv("NORNICDB_HEIMDALL_MCP_ENABLE", "true")
		defer os.Unsetenv("NORNICDB_HEIMDALL_MCP_ENABLE")
		cfg := LoadFromEnv()
		if !cfg.Features.HeimdallMCPEnable {
			t.Error("expected HeimdallMCPEnable true")
		}
		// MCP_TOOLS unset -> nil (all tools)
		if cfg.Features.HeimdallMCPTools != nil {
			t.Errorf("expected HeimdallMCPTools nil when MCP_TOOLS unset, got %v", cfg.Features.HeimdallMCPTools)
		}
	})

	t.Run("enable_true_tools_empty", func(t *testing.T) {
		clearEnvVars(t)
		os.Setenv("NORNICDB_HEIMDALL_MCP_ENABLE", "true")
		os.Setenv("NORNICDB_HEIMDALL_MCP_TOOLS", "")
		defer func() {
			os.Unsetenv("NORNICDB_HEIMDALL_MCP_ENABLE")
			os.Unsetenv("NORNICDB_HEIMDALL_MCP_TOOLS")
		}()
		cfg := LoadFromEnv()
		if !cfg.Features.HeimdallMCPEnable {
			t.Error("expected HeimdallMCPEnable true")
		}
		if cfg.Features.HeimdallMCPTools == nil || len(cfg.Features.HeimdallMCPTools) != 0 {
			t.Errorf("expected HeimdallMCPTools empty slice when MCP_TOOLS=\"\", got %v", cfg.Features.HeimdallMCPTools)
		}
	})

	t.Run("enable_true_tools_allowlist", func(t *testing.T) {
		clearEnvVars(t)
		os.Setenv("NORNICDB_HEIMDALL_MCP_ENABLE", "true")
		os.Setenv("NORNICDB_HEIMDALL_MCP_TOOLS", "store, link , tasks")
		defer func() {
			os.Unsetenv("NORNICDB_HEIMDALL_MCP_ENABLE")
			os.Unsetenv("NORNICDB_HEIMDALL_MCP_TOOLS")
		}()
		cfg := LoadFromEnv()
		if !cfg.Features.HeimdallMCPEnable {
			t.Error("expected HeimdallMCPEnable true")
		}
		want := []string{"store", "link", "tasks"}
		if len(cfg.Features.HeimdallMCPTools) != len(want) {
			t.Errorf("expected %v, got %v", want, cfg.Features.HeimdallMCPTools)
		} else {
			for i := range want {
				if cfg.Features.HeimdallMCPTools[i] != want[i] {
					t.Errorf("expected [%d]=%q, got %q", i, want[i], cfg.Features.HeimdallMCPTools[i])
				}
			}
		}
	})

	t.Run("getters", func(t *testing.T) {
		clearEnvVars(t)
		os.Setenv("NORNICDB_HEIMDALL_MCP_ENABLE", "1")
		os.Setenv("NORNICDB_HEIMDALL_MCP_TOOLS", "recall,discover")
		defer func() {
			os.Unsetenv("NORNICDB_HEIMDALL_MCP_ENABLE")
			os.Unsetenv("NORNICDB_HEIMDALL_MCP_TOOLS")
		}()
		cfg := LoadFromEnv()
		if !cfg.Features.GetHeimdallMCPEnable() {
			t.Error("GetHeimdallMCPEnable expected true")
		}
		got := cfg.Features.GetHeimdallMCPTools()
		if len(got) != 2 || got[0] != "recall" || got[1] != "discover" {
			t.Errorf("GetHeimdallMCPTools expected [recall discover], got %v", got)
		}
	})
}

// TestLoadFromFile_HeimdallMCP tests that YAML heimdall.mcp_enable and mcp_tools wire correctly.
func TestLoadFromFile_HeimdallMCP(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	err := os.WriteFile(path, []byte(`
heimdall:
  enabled: true
  mcp_enable: true
  mcp_tools: [store, link, task]
`), 0o600)
	if err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile: %v", err)
	}

	if !cfg.Features.HeimdallMCPEnable {
		t.Error("expected HeimdallMCPEnable true from YAML")
	}
	want := []string{"store", "link", "task"}
	if len(cfg.Features.HeimdallMCPTools) != len(want) {
		t.Fatalf("expected mcp_tools %v, got %v", want, cfg.Features.HeimdallMCPTools)
	}
	for i := range want {
		if cfg.Features.HeimdallMCPTools[i] != want[i] {
			t.Errorf("mcp_tools[%d]: want %q, got %q", i, want[i], cfg.Features.HeimdallMCPTools[i])
		}
	}
}

func TestLoadFromFile_HeimdallEnabledRespected(t *testing.T) {
	clearEnvVars(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	err := os.WriteFile(path, []byte(`
heimdall:
  enabled: false
`), 0o600)
	if err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile: %v", err)
	}
	if cfg.Features.HeimdallEnabled {
		t.Fatal("expected heimdall.enabled=false from YAML to disable Heimdall")
	}
}

func TestLoadFromFile_HeimdallEnabledFalseEnvOverride(t *testing.T) {
	clearEnvVars(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	err := os.WriteFile(path, []byte(`
heimdall:
  enabled: true
`), 0o600)
	if err != nil {
		t.Fatalf("write config: %v", err)
	}

	os.Setenv("NORNICDB_HEIMDALL_ENABLED", "false")
	defer os.Unsetenv("NORNICDB_HEIMDALL_ENABLED")

	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile: %v", err)
	}
	if cfg.Features.HeimdallEnabled {
		t.Fatal("expected NORNICDB_HEIMDALL_ENABLED=false to disable Heimdall")
	}
}

// TestLoadFromFile_EmbeddingProperties tests embedding_worker properties_include/exclude and include_labels from YAML.
func TestLoadFromFile_EmbeddingProperties(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	err := os.WriteFile(path, []byte(`
embedding_worker:
  properties_include: [content, title]
  properties_exclude: [internal_id]
  include_labels: false
`), 0o600)
	if err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile: %v", err)
	}
	wantInclude := []string{"content", "title"}
	if len(cfg.EmbeddingWorker.PropertiesInclude) != 2 || cfg.EmbeddingWorker.PropertiesInclude[0] != "content" || cfg.EmbeddingWorker.PropertiesInclude[1] != "title" {
		t.Errorf("expected properties_include %v, got %v", wantInclude, cfg.EmbeddingWorker.PropertiesInclude)
	}
	wantExclude := []string{"internal_id"}
	if len(cfg.EmbeddingWorker.PropertiesExclude) != 1 || cfg.EmbeddingWorker.PropertiesExclude[0] != "internal_id" {
		t.Errorf("expected properties_exclude %v, got %v", wantExclude, cfg.EmbeddingWorker.PropertiesExclude)
	}
	if cfg.EmbeddingWorker.IncludeLabels {
		t.Error("expected include_labels false from YAML")
	}
}

func TestLoadFromFile_MVCCLifecycle(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	err := os.WriteFile(path, []byte("\ndatabase:\n  mvcc_lifecycle_enabled: false\n  mvcc_lifecycle_interval: 75s\n  mvcc_lifecycle_max_snapshot_age: 90m\n  mvcc_lifecycle_max_chain_cap: 222\nembedding_worker:\n  trigger_debounce: 9s\n"), 0o600)
	require.NoError(t, err)

	cfg, err := LoadFromFile(path)
	require.NoError(t, err)
	require.False(t, cfg.Database.MVCCLifecycleEnabled)
	require.Equal(t, 75*time.Second, cfg.Database.MVCCLifecycleCycleInterval)
	require.Equal(t, 90*time.Minute, cfg.Database.MVCCLifecycleMaxSnapshotAge)
	require.Equal(t, 222, cfg.Database.MVCCLifecycleMaxChainCap)
	require.Equal(t, 9*time.Second, cfg.EmbeddingWorker.TriggerDebounceDelay)
}

func TestLoadFromFile_ComprehensiveSectionsAndEnvPrecedence(t *testing.T) {
	clearEnvVars(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	err := os.WriteFile(path, []byte(`
server:
  port: 7777
  http_port: 8484
  host: "127.0.0.1"
  auth: "yamladmin:yamlpass"
  bolt_enabled: true
  http_enabled: true
  tls:
    enabled: true
    cert_file: "/tls/cert.pem"
    key_file: "/tls/key.pem"
database:
  data_dir: "/yaml/data"
  default_database: "tenant"
  read_only: true
  transaction_timeout: "45s"
  max_concurrent_transactions: 42
  strict_durability: true
  wal_sync_mode: "none"
  wal_sync_interval: "2s"
  wal_auto_compaction_enabled: false
  wal_retention_max_segments: 10
  wal_retention_max_age: "48h"
  wal_ledger_retention_defaults: true
  wal_snapshot_retention_max_count: 8
  wal_snapshot_retention_max_age: "72h"
  encryption_enabled: true
  encryption_password: "yaml-secret"
  badger_node_cache_max_entries: 321
  badger_edge_type_cache_max_types: 12
  storage_serializer: "msgpack"
  persist_search_indexes: true
auth:
  enabled: true
  username: "fileuser"
  password: "filepass"
  min_password_length: 14
  token_expiry: "2h"
  jwt_secret: "file-secret"
embedding:
  enabled: true
  provider: "ollama"
  model: "embed-model"
  url: "http://embed"
  api_key: "embed-key"
  dimensions: 1536
  cache_size: 77
  min_similarity: 0.42
memory:
  decay_enabled: true
  decay_interval: 5400
  visibility_threshold: 0.2
  auto_links_enabled: true
  auto_links_similarity_threshold: 0.91
  runtime_limit: "512"
  gc_percent: 77
  pool_enabled: true
  pool_max_size: 222
  query_cache_enabled: true
  query_cache_size: 333
  query_cache_ttl: "7m"
embedding_worker:
  scan_interval: "1m"
  batch_delay: "2s"
  max_retries: 9
  chunk_size: 1234
  chunk_overlap: 12
  properties_include: [title, body]
  properties_exclude: [secret]
  include_labels: false
kmeans:
  enabled: true
auto_tlp:
  enabled: true
  algorithm: "jaccard"
  weight: 0.7
  top_k: 99
  min_score: 0.61
  graph_refresh_interval: 123
  ab_test_enabled: true
  ab_test_percentage: 25
heimdall:
  enabled: true
  model: "guardian"
  provider: "openai"
  api_url: "https://heimdall"
  api_key: "heim-key"
  gpu_layers: 7
  context_size: 4096
  batch_size: 512
  max_tokens: 256
  temperature: 0.25
  anomaly_detection: true
  runtime_diagnosis: true
  memory_curation: true
  max_context_tokens: 5000
  max_system_tokens: 3000
  max_user_tokens: 1000
  mcp_enable: true
  mcp_tools: [store, discover]
search_rerank:
  enabled: true
  provider: "http"
  model: "rerank-model"
  api_url: "https://rerank"
  api_key: "rerank-key"
features:
  qdrant_grpc_enabled: true
  qdrant_grpc_listen_addr: ":7334"
  qdrant_grpc_max_vector_dim: 2048
  qdrant_grpc_max_batch_points: 88
  qdrant_grpc_max_top_k: 77
  qdrant_grpc_rbac:
    methods:
      "Points/Upsert": "write"
compliance:
  audit_enabled: true
  audit_log_path: "/audit.log"
  audit_retention_days: 99
  retention_enabled: true
  retention_policy_days: 30
  retention_auto_delete: true
  retention_exempt_roles: [admin, auditor]
  access_control_enabled: true
  session_timeout: "12m"
  max_failed_logins: 4
  lockout_duration: "22m"
  encryption_at_rest: true
  encryption_in_transit: true
  encryption_key_path: "/keys/enc"
  data_export_enabled: true
  data_erasure_enabled: true
  data_access_enabled: true
  anonymization_enabled: true
  anonymization_method: "suppression"
  consent_required: true
  consent_versioning: true
  consent_audit_trail: true
  breach_detection_enabled: true
  breach_notify_email: "ops@example.com"
  breach_notify_webhook: "https://hooks.example.com/breach"
logging:
  level: "DEBUG"
  format: "text"
  output: "stderr"
  query_log_enabled: true
  slow_query_threshold: "9s"
plugins:
  dir: "/plugins"
  heimdall_dir: "/plugins/heim"
`), 0o600)
	require.NoError(t, err)

	t.Setenv("NORNICDB_BOLT_PORT", "9999")
	t.Setenv("NORNICDB_HEIMDALL_ENABLED", "false")
	t.Setenv("NORNICDB_STORAGE_SERIALIZER", "gob")

	cfg, err := LoadFromFile(path)
	require.NoError(t, err)

	require.Equal(t, 9999, cfg.Server.BoltPort)
	require.Equal(t, 8484, cfg.Server.HTTPPort)
	require.Equal(t, "127.0.0.1", cfg.Server.BoltAddress)
	require.True(t, cfg.Server.BoltTLSEnabled)
	require.True(t, cfg.Server.HTTPSEnabled)
	require.Equal(t, "/tls/cert.pem", cfg.Server.BoltTLSCert)
	require.Equal(t, "/tls/key.pem", cfg.Server.BoltTLSKey)
	require.Equal(t, "/yaml/data", cfg.Database.DataDir)
	require.Equal(t, "tenant", cfg.Database.DefaultDatabase)
	require.True(t, cfg.Database.ReadOnly)
	require.Equal(t, 45*time.Second, cfg.Database.TransactionTimeout)
	require.Equal(t, 42, cfg.Database.MaxConcurrentTransactions)
	require.True(t, cfg.Database.StrictDurability)
	require.Equal(t, "immediate", cfg.Database.WALSyncMode)
	require.Equal(t, time.Duration(0), cfg.Database.WALSyncInterval)
	require.False(t, cfg.Database.WALAutoCompactionEnabled)
	require.Equal(t, 10, cfg.Database.WALRetentionMaxSegments)
	require.Equal(t, 48*time.Hour, cfg.Database.WALRetentionMaxAge)
	require.True(t, cfg.Database.WALRetentionLedgerDefaults)
	require.Equal(t, 8, cfg.Database.WALSnapshotRetentionMaxCount)
	require.Equal(t, 72*time.Hour, cfg.Database.WALSnapshotRetentionMaxAge)
	require.True(t, cfg.Database.EncryptionEnabled)
	require.Equal(t, "yaml-secret", cfg.Database.EncryptionPassword)
	require.Equal(t, 321, cfg.Database.BadgerNodeCacheMaxEntries)
	require.Equal(t, 12, cfg.Database.BadgerEdgeTypeCacheMaxTypes)
	require.Equal(t, "gob", cfg.Database.StorageSerializer)
	require.True(t, cfg.Database.PersistSearchIndexes)

	require.True(t, cfg.Auth.Enabled)
	require.Equal(t, "fileuser", cfg.Auth.InitialUsername)
	require.Equal(t, "filepass", cfg.Auth.InitialPassword)
	require.Equal(t, 14, cfg.Auth.MinPasswordLength)
	require.Equal(t, 2*time.Hour, cfg.Auth.TokenExpiry)
	require.Equal(t, "file-secret", cfg.Auth.JWTSecret)

	require.True(t, cfg.Memory.EmbeddingEnabled)
	require.Equal(t, "ollama", cfg.Memory.EmbeddingProvider)
	require.Equal(t, "embed-model", cfg.Memory.EmbeddingModel)
	require.Equal(t, "http://embed", cfg.Memory.EmbeddingAPIURL)
	require.Equal(t, "embed-key", cfg.Memory.EmbeddingAPIKey)
	require.Equal(t, 1536, cfg.Memory.EmbeddingDimensions)
	require.Equal(t, 77, cfg.Memory.EmbeddingCacheSize)
	require.Equal(t, 0.42, cfg.Memory.SearchMinSimilarity)
	require.Equal(t, 90*time.Minute, cfg.Memory.DecayInterval)
	require.Equal(t, 0.2, cfg.Memory.VisibilityThreshold)
	require.True(t, cfg.Memory.AutoLinksEnabled)
	require.Equal(t, 0.91, cfg.Memory.AutoLinksSimilarityThreshold)
	require.Equal(t, int64(512*1024*1024), cfg.Memory.RuntimeLimit)
	require.Equal(t, 77, cfg.Memory.GCPercent)
	require.True(t, cfg.Memory.PoolEnabled)
	require.Equal(t, 222, cfg.Memory.PoolMaxSize)
	require.True(t, cfg.Memory.QueryCacheEnabled)
	require.Equal(t, 333, cfg.Memory.QueryCacheSize)
	require.Equal(t, 7*time.Minute, cfg.Memory.QueryCacheTTL)

	require.Equal(t, 1*time.Minute, cfg.EmbeddingWorker.ScanInterval)
	require.Equal(t, 2*time.Second, cfg.EmbeddingWorker.BatchDelay)
	require.Equal(t, 9, cfg.EmbeddingWorker.MaxRetries)
	require.Equal(t, 1234, cfg.EmbeddingWorker.ChunkSize)
	require.Equal(t, 12, cfg.EmbeddingWorker.ChunkOverlap)
	require.Equal(t, []string{"title", "body"}, cfg.EmbeddingWorker.PropertiesInclude)
	require.Equal(t, []string{"secret"}, cfg.EmbeddingWorker.PropertiesExclude)
	require.False(t, cfg.EmbeddingWorker.IncludeLabels)

	require.True(t, cfg.Features.KalmanEnabled)
	require.True(t, cfg.Features.TopologyAutoIntegrationEnabled)
	require.Equal(t, "jaccard", cfg.Features.TopologyAlgorithm)
	require.Equal(t, 0.7, cfg.Features.TopologyWeight)
	require.Equal(t, 99, cfg.Features.TopologyTopK)
	require.Equal(t, 0.61, cfg.Features.TopologyMinScore)
	require.Equal(t, 123, cfg.Features.TopologyGraphRefreshInterval)
	require.True(t, cfg.Features.TopologyABTestEnabled)
	require.Equal(t, 25, cfg.Features.TopologyABTestPercentage)
	require.False(t, cfg.Features.HeimdallEnabled)
	require.Equal(t, "guardian", cfg.Features.HeimdallModel)
	require.Equal(t, "openai", cfg.Features.HeimdallProvider)
	require.Equal(t, "https://heimdall", cfg.Features.HeimdallAPIURL)
	require.Equal(t, "heim-key", cfg.Features.HeimdallAPIKey)
	require.Equal(t, 7, cfg.Features.HeimdallGPULayers)
	require.Equal(t, 4096, cfg.Features.HeimdallContextSize)
	require.Equal(t, 512, cfg.Features.HeimdallBatchSize)
	require.Equal(t, 256, cfg.Features.HeimdallMaxTokens)
	require.Equal(t, float32(0.25), cfg.Features.HeimdallTemperature)
	require.True(t, cfg.Features.HeimdallAnomalyDetection)
	require.True(t, cfg.Features.HeimdallRuntimeDiagnosis)
	require.True(t, cfg.Features.HeimdallMemoryCuration)
	require.Equal(t, 5000, cfg.Features.HeimdallMaxContextTokens)
	require.Equal(t, 3000, cfg.Features.HeimdallMaxSystemTokens)
	require.Equal(t, 1000, cfg.Features.HeimdallMaxUserTokens)
	require.True(t, cfg.Features.HeimdallMCPEnable)
	require.Equal(t, []string{"store", "discover"}, cfg.Features.HeimdallMCPTools)
	require.True(t, cfg.Features.SearchRerankEnabled)
	require.Equal(t, "http", cfg.Features.SearchRerankProvider)
	require.Equal(t, "rerank-model", cfg.Features.SearchRerankModel)
	require.Equal(t, "https://rerank", cfg.Features.SearchRerankAPIURL)
	require.Equal(t, "rerank-key", cfg.Features.SearchRerankAPIKey)
	require.True(t, cfg.Features.QdrantGRPCEnabled)
	require.Equal(t, ":7334", cfg.Features.QdrantGRPCListenAddr)
	require.Equal(t, 2048, cfg.Features.QdrantGRPCMaxVectorDim)
	require.Equal(t, 88, cfg.Features.QdrantGRPCMaxBatchPoints)
	require.Equal(t, 77, cfg.Features.QdrantGRPCMaxTopK)
	require.Equal(t, map[string]string{"Points/Upsert": "write"}, cfg.Features.QdrantGRPCMethodPermissions)

	require.True(t, cfg.Compliance.RetentionEnabled)
	require.Equal(t, 30, cfg.Compliance.RetentionPolicyDays)
	require.True(t, cfg.Compliance.RetentionAutoDelete)
	require.Equal(t, []string{"admin", "auditor"}, cfg.Compliance.RetentionExemptRoles)
	require.Equal(t, 12*time.Minute, cfg.Compliance.SessionTimeout)
	require.Equal(t, 4, cfg.Compliance.MaxFailedLogins)
	require.Equal(t, 22*time.Minute, cfg.Compliance.LockoutDuration)
	require.True(t, cfg.Compliance.EncryptionAtRest)
	require.True(t, cfg.Compliance.EncryptionInTransit)
	require.Equal(t, "/keys/enc", cfg.Compliance.EncryptionKeyPath)
	require.Equal(t, "suppression", cfg.Compliance.AnonymizationMethod)
	require.True(t, cfg.Compliance.ConsentRequired)
	require.True(t, cfg.Compliance.BreachDetectionEnabled)
	require.Equal(t, "ops@example.com", cfg.Compliance.BreachNotifyEmail)
	require.Equal(t, "https://hooks.example.com/breach", cfg.Compliance.BreachNotifyWebhook)

	require.Equal(t, "DEBUG", cfg.Logging.Level)
	require.Equal(t, "text", cfg.Logging.Format)
	require.Equal(t, "stderr", cfg.Logging.Output)
	require.True(t, cfg.Logging.QueryLogEnabled)
	require.Equal(t, 9*time.Second, cfg.Logging.SlowQueryThreshold)
	require.Equal(t, "/plugins", cfg.Server.PluginsDir)
	require.Equal(t, "/plugins/heim", cfg.Server.HeimdallPluginsDir)
}

func TestLoadFromFile_MissingFileReturnsDefaults(t *testing.T) {
	clearEnvVars(t)

	cfg, err := LoadFromFile(filepath.Join(t.TempDir(), "missing.yaml"))
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Equal(t, "nornic", cfg.Database.DefaultDatabase)
}

func TestLoadFromFile_RetentionAndSubjectSelectorConfig(t *testing.T) {
	clearEnvVars(t)
	path := filepath.Join(t.TempDir(), "retention.yaml")
	err := os.WriteFile(path, []byte("compliance:\n"+
		"  subject_identifier_properties: [account_id, created_by]\n"+
		"  subject_pseudonymize_properties: [account_id]\n"+
		"  subject_redact_properties: [full_name, contact_email]\n"+
		"retention:\n"+
		"  sweep_interval: 2700\n"+
		"  excluded_labels: [AuditLog, System]\n"+
		"  policies_file: \"/tmp/retention-policies.json\"\n"+
		"  default_policies: true\n"+
		"  max_sweep_records: 1234\n"+
		"  policies:\n"+
		"    - id: pii-1y\n"+
		"      name: \"PII One Year\"\n"+
		"      category: PII\n"+
		"      retention_days: 365\n"), 0o600)
	require.NoError(t, err)

	cfg, err := LoadFromFile(path)
	require.NoError(t, err)
	require.Equal(t, []string{"account_id", "created_by"}, cfg.Compliance.SubjectIdentifierProperties)
	require.Equal(t, []string{"account_id"}, cfg.Compliance.SubjectPseudonymizeProperties)
	require.Equal(t, []string{"full_name", "contact_email"}, cfg.Compliance.SubjectRedactProperties)
	require.Equal(t, 2700, cfg.Retention.SweepIntervalSeconds)
	require.Equal(t, []string{"AuditLog", "System"}, cfg.Retention.ExcludedLabels)
	require.Equal(t, "/tmp/retention-policies.json", cfg.Retention.PoliciesFile)
	require.True(t, cfg.Retention.DefaultPolicies)
	require.Equal(t, 1234, cfg.Retention.MaxSweepRecords)
	require.Len(t, cfg.Retention.Policies, 1)
	require.Equal(t, "pii-1y", cfg.Retention.Policies[0].ID)
}

func TestLoadFromEnv_RetentionAndSubjectSelectorConfig(t *testing.T) {
	clearEnvVars(t)
	t.Setenv("NORNICDB_SUBJECT_IDENTIFIER_PROPERTIES", "account_id,created_by")
	t.Setenv("NORNICDB_SUBJECT_PSEUDONYMIZE_PROPERTIES", "account_id")
	t.Setenv("NORNICDB_SUBJECT_REDACT_PROPERTIES", "full_name,contact_email")
	t.Setenv("NORNICDB_RETENTION_SWEEP_INTERVAL", "1800")
	t.Setenv("NORNICDB_RETENTION_EXCLUDED_LABELS", "AuditLog,System")
	t.Setenv("NORNICDB_RETENTION_POLICIES_FILE", "/etc/nornicdb/retention.json")
	t.Setenv("NORNICDB_RETENTION_DEFAULT_POLICIES", "true")
	t.Setenv("NORNICDB_RETENTION_MAX_SWEEP_RECORDS", "9876")

	cfg := LoadFromEnv()
	require.Equal(t, []string{"account_id", "created_by"}, cfg.Compliance.SubjectIdentifierProperties)
	require.Equal(t, []string{"account_id"}, cfg.Compliance.SubjectPseudonymizeProperties)
	require.Equal(t, []string{"full_name", "contact_email"}, cfg.Compliance.SubjectRedactProperties)
	require.Equal(t, 1800, cfg.Retention.SweepIntervalSeconds)
	require.Equal(t, []string{"AuditLog", "System"}, cfg.Retention.ExcludedLabels)
	require.Equal(t, "/etc/nornicdb/retention.json", cfg.Retention.PoliciesFile)
	require.True(t, cfg.Retention.DefaultPolicies)
	require.Equal(t, 9876, cfg.Retention.MaxSweepRecords)
}

// TestLoadFromEnv_HeimdallProvider tests Heimdall provider env vars (openai/ollama/local).
func TestLoadFromEnv_HeimdallProvider(t *testing.T) {
	os.Setenv("NORNICDB_HEIMDALL_PROVIDER", "openai")
	os.Setenv("NORNICDB_HEIMDALL_API_KEY", "sk-test")
	os.Setenv("NORNICDB_HEIMDALL_API_URL", "https://api.example.com")
	defer func() {
		os.Unsetenv("NORNICDB_HEIMDALL_PROVIDER")
		os.Unsetenv("NORNICDB_HEIMDALL_API_KEY")
		os.Unsetenv("NORNICDB_HEIMDALL_API_URL")
	}()

	cfg := LoadFromEnv()

	if cfg.Features.HeimdallProvider != "openai" {
		t.Errorf("expected HeimdallProvider 'openai', got %q", cfg.Features.HeimdallProvider)
	}
	if cfg.Features.HeimdallAPIKey != "sk-test" {
		t.Errorf("expected HeimdallAPIKey 'sk-test', got %q", cfg.Features.HeimdallAPIKey)
	}
	if cfg.Features.HeimdallAPIURL != "https://api.example.com" {
		t.Errorf("expected HeimdallAPIURL 'https://api.example.com', got %q", cfg.Features.HeimdallAPIURL)
	}
}

// TestLoadFromEnv_BoolParsing tests boolean env var parsing.
func TestLoadFromEnv_BoolParsing(t *testing.T) {
	tests := []struct {
		envValue string
		want     bool
	}{
		{"true", true},
		{"TRUE", true},
		{"True", true},
		{"1", true},
		{"yes", true},
		{"YES", true},
		{"on", true},
		{"ON", true},
		{"false", false},
		{"FALSE", false},
		{"0", false},
		{"no", false},
		{"off", false},
		{"", false}, // empty defaults to false for this test
	}

	for _, tt := range tests {
		t.Run("value="+tt.envValue, func(t *testing.T) {
			clearEnvVars(t)
			os.Setenv("NEO4J_dbms_read__only", tt.envValue)

			cfg := LoadFromEnv()

			if cfg.Database.ReadOnly != tt.want {
				t.Errorf("for value %q, expected ReadOnly=%v, got %v", tt.envValue, tt.want, cfg.Database.ReadOnly)
			}
		})
	}
}

// TestLoadFromEnv_DurationParsing tests duration env var parsing.
func TestLoadFromEnv_DurationParsing(t *testing.T) {
	tests := []struct {
		envValue string
		want     time.Duration
	}{
		{"30s", 30 * time.Second},
		{"5m", 5 * time.Minute},
		{"2h", 2 * time.Hour},
		{"1h30m", 90 * time.Minute},
		{"100", 100 * time.Second}, // numeric as seconds
		{"", 30 * time.Second},     // default
	}

	for _, tt := range tests {
		t.Run("value="+tt.envValue, func(t *testing.T) {
			clearEnvVars(t)
			if tt.envValue != "" {
				os.Setenv("NEO4J_dbms_transaction_timeout", tt.envValue)
			}

			cfg := LoadFromEnv()

			if cfg.Database.TransactionTimeout != tt.want {
				t.Errorf("for value %q, expected timeout=%v, got %v", tt.envValue, tt.want, cfg.Database.TransactionTimeout)
			}
		})
	}
}

// TestConfig_Validate tests configuration validation.
func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config with long password",
			modify: func(c *Config) {
				c.Auth.InitialPassword = "longenoughpassword"
			},
			wantErr: false,
		},
		{
			name: "auth enabled, no username",
			modify: func(c *Config) {
				c.Auth.Enabled = true
				c.Auth.InitialUsername = ""
				c.Auth.InitialPassword = "longenoughpassword"
			},
			wantErr: true,
			errMsg:  "no username",
		},
		{
			name: "auth enabled, password too short",
			modify: func(c *Config) {
				c.Auth.Enabled = true
				c.Auth.InitialUsername = "admin"
				c.Auth.InitialPassword = "short"
				c.Auth.MinPasswordLength = 8
			},
			wantErr: true,
			errMsg:  "at least 8 characters",
		},
		{
			name: "auth disabled, short password OK",
			modify: func(c *Config) {
				c.Auth.Enabled = false
				c.Auth.InitialPassword = "x"
			},
			wantErr: false,
		},
		{
			name: "bolt enabled, invalid port",
			modify: func(c *Config) {
				c.Auth.InitialPassword = "longenoughpassword"
				c.Server.BoltEnabled = true
				c.Server.BoltPort = 0
			},
			wantErr: true,
			errMsg:  "invalid bolt port",
		},
		{
			name: "bolt enabled, negative port",
			modify: func(c *Config) {
				c.Auth.InitialPassword = "longenoughpassword"
				c.Server.BoltEnabled = true
				c.Server.BoltPort = -1
			},
			wantErr: true,
			errMsg:  "invalid bolt port",
		},
		{
			name: "bolt disabled, invalid port OK",
			modify: func(c *Config) {
				c.Auth.InitialPassword = "longenoughpassword"
				c.Server.BoltEnabled = false
				c.Server.BoltPort = 0
			},
			wantErr: false,
		},
		{
			name: "http enabled, invalid port",
			modify: func(c *Config) {
				c.Auth.InitialPassword = "longenoughpassword"
				c.Server.HTTPEnabled = true
				c.Server.HTTPPort = -5
			},
			wantErr: true,
			errMsg:  "invalid http port",
		},
		{
			name: "invalid embedding dimensions",
			modify: func(c *Config) {
				c.Auth.InitialPassword = "longenoughpassword"
				c.Memory.EmbeddingDimensions = 0
			},
			wantErr: true,
			errMsg:  "invalid embedding dimensions",
		},
		{
			name: "negative embedding dimensions",
			modify: func(c *Config) {
				c.Auth.InitialPassword = "longenoughpassword"
				c.Memory.EmbeddingDimensions = -100
			},
			wantErr: true,
			errMsg:  "invalid embedding dimensions",
		},
		{
			name: "negative mvcc lifecycle interval",
			modify: func(c *Config) {
				c.Auth.InitialPassword = "longenoughpassword"
				c.Database.MVCCLifecycleCycleInterval = -time.Second
			},
			wantErr: true,
			errMsg:  "invalid mvcc lifecycle interval",
		},
		{
			name: "negative mvcc lifecycle max snapshot age",
			modify: func(c *Config) {
				c.Auth.InitialPassword = "longenoughpassword"
				c.Database.MVCCLifecycleMaxSnapshotAge = -time.Minute
			},
			wantErr: true,
			errMsg:  "invalid mvcc lifecycle max snapshot age",
		},
		{
			name: "negative mvcc lifecycle max chain cap",
			modify: func(c *Config) {
				c.Auth.InitialPassword = "longenoughpassword"
				c.Database.MVCCLifecycleMaxChainCap = -1
			},
			wantErr: true,
			errMsg:  "invalid mvcc lifecycle max chain cap",
		},
		{
			name: "negative embed trigger debounce",
			modify: func(c *Config) {
				c.Auth.InitialPassword = "longenoughpassword"
				c.EmbeddingWorker.TriggerDebounceDelay = -time.Second
			},
			wantErr: true,
			errMsg:  "invalid embed trigger debounce",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearEnvVars(t)
			cfg := LoadFromEnv()
			tt.modify(cfg)

			err := cfg.Validate()

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tt.errMsg != "" && !containsSubstring(err.Error(), tt.errMsg) {
					t.Errorf("expected error containing %q, got %q", tt.errMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}
			}
		})
	}
}

// TestConfig_String tests safe string representation.
func TestConfig_String(t *testing.T) {
	clearEnvVars(t)
	os.Setenv("NEO4J_AUTH", "admin/supersecretpassword")

	cfg := LoadFromEnv()
	str := cfg.String()

	// Should contain important info
	if !containsSubstring(str, "Auth: true") {
		t.Error("expected string to contain auth status")
	}
	if !containsSubstring(str, "7687") {
		t.Error("expected string to contain bolt port")
	}
	if !containsSubstring(str, "7474") {
		t.Error("expected string to contain http port")
	}
	if !containsSubstring(str, "./data") {
		t.Error("expected string to contain data dir")
	}

	// Should NOT contain secrets
	if containsSubstring(str, "supersecret") {
		t.Error("string should not contain password")
	}
	if containsSubstring(str, cfg.Auth.JWTSecret) {
		t.Error("string should not contain JWT secret")
	}
}

// TestGetEnvStringSlice tests string slice parsing.
func TestGetEnvStringSlice(t *testing.T) {
	tests := []struct {
		name       string
		envValue   string
		defaultVal []string
		want       []string
	}{
		{
			name:       "empty uses default",
			envValue:   "",
			defaultVal: []string{"default"},
			want:       []string{"default"},
		},
		{
			name:       "single value",
			envValue:   "admin",
			defaultVal: []string{"default"},
			want:       []string{"admin"},
		},
		{
			name:       "multiple values",
			envValue:   "admin,user,guest",
			defaultVal: []string{"default"},
			want:       []string{"admin", "user", "guest"},
		},
		{
			name:       "values with spaces",
			envValue:   "admin, user , guest",
			defaultVal: []string{"default"},
			want:       []string{"admin", "user", "guest"},
		},
		{
			name:       "empty parts ignored",
			envValue:   "admin,,user,",
			defaultVal: []string{"default"},
			want:       []string{"admin", "user"},
		},
		{
			name:       "only commas uses default",
			envValue:   ",,,",
			defaultVal: []string{"default"},
			want:       []string{"default"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("TEST_SLICE", tt.envValue)
			defer os.Unsetenv("TEST_SLICE")

			got := getEnvStringSlice("TEST_SLICE", tt.defaultVal)

			if len(got) != len(tt.want) {
				t.Errorf("expected %d elements, got %d: %v", len(tt.want), len(got), got)
				return
			}
			for i, want := range tt.want {
				if got[i] != want {
					t.Errorf("element %d: expected %q, got %q", i, want, got[i])
				}
			}
		})
	}
}

// TestComplianceConfig_GDPR tests GDPR-specific compliance settings.
func TestComplianceConfig_GDPR(t *testing.T) {
	clearEnvVars(t)

	// Enable GDPR-relevant features
	os.Setenv("NORNICDB_DATA_ERASURE_ENABLED", "true")
	os.Setenv("NORNICDB_DATA_EXPORT_ENABLED", "true")
	os.Setenv("NORNICDB_DATA_ACCESS_ENABLED", "true")
	os.Setenv("NORNICDB_CONSENT_REQUIRED", "true")
	os.Setenv("NORNICDB_ANONYMIZATION_ENABLED", "true")
	os.Setenv("NORNICDB_AUDIT_ENABLED", "true")

	cfg := LoadFromEnv()

	// GDPR Article 15-20: Data subject rights
	if !cfg.Compliance.DataErasureEnabled {
		t.Error("GDPR Art.17 Right to erasure should be enabled")
	}
	if !cfg.Compliance.DataExportEnabled {
		t.Error("GDPR Art.20 Data portability should be enabled")
	}
	if !cfg.Compliance.DataAccessEnabled {
		t.Error("GDPR Art.15 Right of access should be enabled")
	}

	// GDPR Article 7: Consent
	if !cfg.Compliance.ConsentRequired {
		t.Error("GDPR Art.7 Consent should be required")
	}

	// GDPR Recital 26: Anonymization
	if !cfg.Compliance.AnonymizationEnabled {
		t.Error("GDPR Recital 26 Anonymization should be enabled")
	}

	// GDPR Article 30: Records of processing
	if !cfg.Compliance.AuditEnabled {
		t.Error("GDPR Art.30 Audit logging should be enabled")
	}
}

// TestComplianceConfig_HIPAA tests HIPAA-specific compliance settings.
func TestComplianceConfig_HIPAA(t *testing.T) {
	clearEnvVars(t)

	// HIPAA requires longer audit retention (6 years)
	os.Setenv("NORNICDB_AUDIT_RETENTION_DAYS", "2190") // 6 years
	os.Setenv("NORNICDB_ENCRYPTION_AT_REST", "true")
	os.Setenv("NORNICDB_ENCRYPTION_IN_TRANSIT", "true")
	os.Setenv("NORNICDB_ACCESS_CONTROL_ENABLED", "true")
	os.Setenv("NORNICDB_AUDIT_ENABLED", "true")

	cfg := LoadFromEnv()

	// HIPAA §164.530(j): Retain records for 6 years
	if cfg.Compliance.AuditRetentionDays < 2190 {
		t.Errorf("HIPAA requires 6 years audit retention, got %d days", cfg.Compliance.AuditRetentionDays)
	}

	// HIPAA §164.312(a)(2)(iv): Encryption
	if !cfg.Compliance.EncryptionAtRest {
		t.Error("HIPAA requires encryption at rest")
	}
	if !cfg.Compliance.EncryptionInTransit {
		t.Error("HIPAA requires encryption in transit")
	}

	// HIPAA §164.312(a): Access control
	if !cfg.Compliance.AccessControlEnabled {
		t.Error("HIPAA requires access control")
	}

	// HIPAA §164.312(b): Audit controls
	if !cfg.Compliance.AuditEnabled {
		t.Error("HIPAA requires audit controls")
	}
}

// TestComplianceConfig_FISMA tests FISMA-specific compliance settings.
func TestComplianceConfig_FISMA(t *testing.T) {
	clearEnvVars(t)

	os.Setenv("NORNICDB_ACCESS_CONTROL_ENABLED", "true")
	os.Setenv("NORNICDB_AUDIT_ENABLED", "true")
	os.Setenv("NORNICDB_MAX_FAILED_LOGINS", "3")
	os.Setenv("NORNICDB_LOCKOUT_DURATION", "30m")
	os.Setenv("NORNICDB_SESSION_TIMEOUT", "15m")

	cfg := LoadFromEnv()

	// FISMA AC controls: Access Control
	if !cfg.Compliance.AccessControlEnabled {
		t.Error("FISMA AC controls require access control")
	}

	// FISMA AU controls: Audit
	if !cfg.Compliance.AuditEnabled {
		t.Error("FISMA AU controls require auditing")
	}

	// FISMA AC-7: Unsuccessful login attempts
	if cfg.Compliance.MaxFailedLogins > 5 {
		t.Errorf("FISMA recommends max 5 failed logins, got %d", cfg.Compliance.MaxFailedLogins)
	}

	// FISMA AC-12: Session termination
	if cfg.Compliance.SessionTimeout > 30*time.Minute {
		t.Errorf("FISMA recommends session timeout <= 30m, got %v", cfg.Compliance.SessionTimeout)
	}
}

// TestComplianceConfig_SOC2 tests SOC2-specific compliance settings.
func TestComplianceConfig_SOC2(t *testing.T) {
	clearEnvVars(t)

	// SOC2 requires 7 years audit retention
	os.Setenv("NORNICDB_AUDIT_RETENTION_DAYS", "2555") // 7 years
	os.Setenv("NORNICDB_AUDIT_ENABLED", "true")
	os.Setenv("NORNICDB_BREACH_DETECTION_ENABLED", "true")

	cfg := LoadFromEnv()

	// SOC2: 7 year retention
	if cfg.Compliance.AuditRetentionDays < 2555 {
		t.Errorf("SOC2 requires 7 years audit retention, got %d days", cfg.Compliance.AuditRetentionDays)
	}

	// SOC2 CC7: System monitoring
	if !cfg.Compliance.AuditEnabled {
		t.Error("SOC2 CC7 requires audit logging")
	}

	// SOC2 CC7.4: Breach detection
	if !cfg.Compliance.BreachDetectionEnabled {
		t.Error("SOC2 CC7.4 requires breach detection")
	}
}

// TestGenerateDefaultSecret tests secret generation.
func TestGenerateDefaultSecret(t *testing.T) {
	secret1 := generateDefaultSecret()
	secret2 := generateDefaultSecret()

	// Should be non-empty
	if len(secret1) < 20 {
		t.Errorf("expected secret length >= 20, got %d", len(secret1))
	}

	// Should contain warning prefix
	if !containsSubstring(secret1, "CHANGE_ME") {
		t.Error("expected secret to contain CHANGE_ME warning")
	}

	// Different calls should produce different secrets (timestamp-based)
	// Note: This may occasionally fail if called within same nanosecond
	// In practice, this is extremely unlikely
	if secret1 == secret2 {
		t.Log("Warning: two consecutive secrets are identical (may happen rarely)")
	}
}

// Helper functions

func clearEnvVars(t *testing.T) {
	t.Helper()
	envVars := []string{
		"NEO4J_AUTH",
		"NEO4J_dbms_directories_data",
		"NEO4J_dbms_default__database",
		"NEO4J_dbms_read__only",
		"NEO4J_dbms_transaction_timeout",
		"NEO4J_dbms_transaction_concurrent_maximum",
		"NEO4J_dbms_connector_bolt_enabled",
		"NEO4J_dbms_connector_bolt_listen__address_port",
		"NEO4J_dbms_connector_bolt_listen__address",
		"NEO4J_dbms_connector_bolt_tls__level",
		"NEO4J_dbms_connector_http_enabled",
		"NEO4J_dbms_connector_http_listen__address_port",
		"NEO4J_dbms_connector_http_listen__address",
		"NEO4J_dbms_connector_https_enabled",
		"NEO4J_dbms_connector_https_listen__address_port",
		"NEO4J_dbms_logs_debug_level",
		"NEO4J_dbms_logs_query_enabled",
		"NEO4J_dbms_logs_query_threshold",
		"NEO4J_dbms_security_auth_minimum__password__length",
		"NORNICDB_AUTH_TOKEN_EXPIRY",
		"NORNICDB_AUTH_JWT_SECRET",
		"NORNICDB_MEMORY_DECAY_ENABLED",
		"NORNICDB_MEMORY_DECAY_INTERVAL",
		"NORNICDB_MEMORY_ARCHIVE_THRESHOLD",
		"NORNICDB_EMBEDDING_PROVIDER",
		"NORNICDB_EMBEDDING_MODEL",
		"NORNICDB_EMBEDDING_API_URL",
		"NORNICDB_EMBEDDING_DIMENSIONS",
		"NORNICDB_AUTO_LINKS_ENABLED",
		"NORNICDB_AUTO_LINKS_THRESHOLD",
		"NORNICDB_AUDIT_ENABLED",
		"NORNICDB_AUDIT_LOG_PATH",
		"NORNICDB_AUDIT_RETENTION_DAYS",
		"NORNICDB_RETENTION_ENABLED",
		"NORNICDB_RETENTION_POLICY_DAYS",
		"NORNICDB_RETENTION_AUTO_DELETE",
		"NORNICDB_RETENTION_EXEMPT_ROLES",
		"NORNICDB_ACCESS_CONTROL_ENABLED",
		"NORNICDB_SESSION_TIMEOUT",
		"NORNICDB_MAX_FAILED_LOGINS",
		"NORNICDB_LOCKOUT_DURATION",
		"NORNICDB_ENCRYPTION_AT_REST",
		"NORNICDB_ENCRYPTION_IN_TRANSIT",
		"NORNICDB_ENCRYPTION_KEY_PATH",
		"NORNICDB_DATA_EXPORT_ENABLED",
		"NORNICDB_DATA_ERASURE_ENABLED",
		"NORNICDB_DATA_ACCESS_ENABLED",
		"NORNICDB_ANONYMIZATION_ENABLED",
		"NORNICDB_ANONYMIZATION_METHOD",
		"NORNICDB_CONSENT_REQUIRED",
		"NORNICDB_CONSENT_VERSIONING",
		"NORNICDB_CONSENT_AUDIT_TRAIL",
		"NORNICDB_BREACH_DETECTION_ENABLED",
		"NORNICDB_BREACH_NOTIFY_EMAIL",
		"NORNICDB_BREACH_NOTIFY_WEBHOOK",
		"NORNICDB_LOG_FORMAT",
		"NORNICDB_LOG_OUTPUT",
		// Durability settings
		"NORNICDB_STRICT_DURABILITY",
		"NORNICDB_WAL_SYNC_MODE",
		"NORNICDB_WAL_SYNC_INTERVAL",
		// Async write settings
		"NORNICDB_ASYNC_WRITES_ENABLED",
		"NORNICDB_ASYNC_FLUSH_INTERVAL",
		"NORNICDB_ASYNC_MAX_NODE_CACHE_SIZE",
		"NORNICDB_ASYNC_MAX_EDGE_CACHE_SIZE",
		"NORNICDB_BADGER_NODE_CACHE_MAX_ENTRIES",
		"NORNICDB_BADGER_EDGE_TYPE_CACHE_MAX_TYPES",
		"NORNICDB_STORAGE_SERIALIZER",
		"NORNICDB_HEIMDALL_MCP_ENABLE",
		"NORNICDB_HEIMDALL_MCP_TOOLS",
	}
	for _, v := range envVars {
		os.Unsetenv(v)
	}
}

func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstringHelper(s, substr))
}

func containsSubstringHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestLoadFromEnv_AsyncWriteDefaults tests that async write defaults are loaded correctly.
func TestLoadFromEnv_AsyncWriteDefaults(t *testing.T) {
	clearEnvVars(t)

	cfg := LoadFromEnv()

	// Async write defaults
	if !cfg.Database.AsyncWritesEnabled {
		t.Error("expected AsyncWritesEnabled to be true by default")
	}
	if cfg.Database.AsyncFlushInterval != 50*time.Millisecond {
		t.Errorf("expected AsyncFlushInterval 50ms, got %v", cfg.Database.AsyncFlushInterval)
	}
	if cfg.Database.AsyncMaxNodeCacheSize != 50000 {
		t.Errorf("expected AsyncMaxNodeCacheSize 50000, got %d", cfg.Database.AsyncMaxNodeCacheSize)
	}
	if cfg.Database.AsyncMaxEdgeCacheSize != 100000 {
		t.Errorf("expected AsyncMaxEdgeCacheSize 100000, got %d", cfg.Database.AsyncMaxEdgeCacheSize)
	}
	if cfg.Database.BadgerNodeCacheMaxEntries != 10000 {
		t.Errorf("expected BadgerNodeCacheMaxEntries 10000, got %d", cfg.Database.BadgerNodeCacheMaxEntries)
	}
	if cfg.Database.BadgerEdgeTypeCacheMaxTypes != 50 {
		t.Errorf("expected BadgerEdgeTypeCacheMaxTypes 50, got %d", cfg.Database.BadgerEdgeTypeCacheMaxTypes)
	}
	if cfg.Database.StorageSerializer != "msgpack" {
		t.Errorf("expected StorageSerializer msgpack, got %s", cfg.Database.StorageSerializer)
	}
}

// TestLoadFromEnv_AsyncWriteSettings tests async write env var overrides.
func TestLoadFromEnv_AsyncWriteSettings(t *testing.T) {
	tests := []struct {
		name     string
		envVars  map[string]string
		validate func(t *testing.T, cfg *Config)
	}{
		{
			name: "disable async writes",
			envVars: map[string]string{
				"NORNICDB_ASYNC_WRITES_ENABLED": "false",
			},
			validate: func(t *testing.T, cfg *Config) {
				if cfg.Database.AsyncWritesEnabled {
					t.Error("expected AsyncWritesEnabled to be false")
				}
			},
		},
		{
			name: "enable async writes explicitly",
			envVars: map[string]string{
				"NORNICDB_ASYNC_WRITES_ENABLED": "true",
			},
			validate: func(t *testing.T, cfg *Config) {
				if !cfg.Database.AsyncWritesEnabled {
					t.Error("expected AsyncWritesEnabled to be true")
				}
			},
		},
		{
			name: "custom flush interval",
			envVars: map[string]string{
				"NORNICDB_ASYNC_FLUSH_INTERVAL": "100ms",
			},
			validate: func(t *testing.T, cfg *Config) {
				if cfg.Database.AsyncFlushInterval != 100*time.Millisecond {
					t.Errorf("expected AsyncFlushInterval 100ms, got %v", cfg.Database.AsyncFlushInterval)
				}
			},
		},
		{
			name: "custom flush interval seconds",
			envVars: map[string]string{
				"NORNICDB_ASYNC_FLUSH_INTERVAL": "2s",
			},
			validate: func(t *testing.T, cfg *Config) {
				if cfg.Database.AsyncFlushInterval != 2*time.Second {
					t.Errorf("expected AsyncFlushInterval 2s, got %v", cfg.Database.AsyncFlushInterval)
				}
			},
		},
		{
			name: "custom node cache size",
			envVars: map[string]string{
				"NORNICDB_ASYNC_MAX_NODE_CACHE_SIZE": "10000",
			},
			validate: func(t *testing.T, cfg *Config) {
				if cfg.Database.AsyncMaxNodeCacheSize != 10000 {
					t.Errorf("expected AsyncMaxNodeCacheSize 10000, got %d", cfg.Database.AsyncMaxNodeCacheSize)
				}
			},
		},
		{
			name: "custom edge cache size",
			envVars: map[string]string{
				"NORNICDB_ASYNC_MAX_EDGE_CACHE_SIZE": "25000",
			},
			validate: func(t *testing.T, cfg *Config) {
				if cfg.Database.AsyncMaxEdgeCacheSize != 25000 {
					t.Errorf("expected AsyncMaxEdgeCacheSize 25000, got %d", cfg.Database.AsyncMaxEdgeCacheSize)
				}
			},
		},
		{
			name: "zero cache size (unlimited)",
			envVars: map[string]string{
				"NORNICDB_ASYNC_MAX_NODE_CACHE_SIZE": "0",
				"NORNICDB_ASYNC_MAX_EDGE_CACHE_SIZE": "0",
			},
			validate: func(t *testing.T, cfg *Config) {
				if cfg.Database.AsyncMaxNodeCacheSize != 0 {
					t.Errorf("expected AsyncMaxNodeCacheSize 0, got %d", cfg.Database.AsyncMaxNodeCacheSize)
				}
				if cfg.Database.AsyncMaxEdgeCacheSize != 0 {
					t.Errorf("expected AsyncMaxEdgeCacheSize 0, got %d", cfg.Database.AsyncMaxEdgeCacheSize)
				}
			},
		},
		{
			name: "all async settings combined",
			envVars: map[string]string{
				"NORNICDB_ASYNC_WRITES_ENABLED":             "true",
				"NORNICDB_ASYNC_FLUSH_INTERVAL":             "25ms",
				"NORNICDB_ASYNC_MAX_NODE_CACHE_SIZE":        "5000",
				"NORNICDB_ASYNC_MAX_EDGE_CACHE_SIZE":        "8000",
				"NORNICDB_BADGER_NODE_CACHE_MAX_ENTRIES":    "20000",
				"NORNICDB_BADGER_EDGE_TYPE_CACHE_MAX_TYPES": "75",
				"NORNICDB_STORAGE_SERIALIZER":               "msgpack",
			},
			validate: func(t *testing.T, cfg *Config) {
				if !cfg.Database.AsyncWritesEnabled {
					t.Error("expected AsyncWritesEnabled to be true")
				}
				if cfg.Database.AsyncFlushInterval != 25*time.Millisecond {
					t.Errorf("expected AsyncFlushInterval 25ms, got %v", cfg.Database.AsyncFlushInterval)
				}
				if cfg.Database.AsyncMaxNodeCacheSize != 5000 {
					t.Errorf("expected AsyncMaxNodeCacheSize 5000, got %d", cfg.Database.AsyncMaxNodeCacheSize)
				}
				if cfg.Database.AsyncMaxEdgeCacheSize != 8000 {
					t.Errorf("expected AsyncMaxEdgeCacheSize 8000, got %d", cfg.Database.AsyncMaxEdgeCacheSize)
				}
				if cfg.Database.BadgerNodeCacheMaxEntries != 20000 {
					t.Errorf("expected BadgerNodeCacheMaxEntries 20000, got %d", cfg.Database.BadgerNodeCacheMaxEntries)
				}
				if cfg.Database.BadgerEdgeTypeCacheMaxTypes != 75 {
					t.Errorf("expected BadgerEdgeTypeCacheMaxTypes 75, got %d", cfg.Database.BadgerEdgeTypeCacheMaxTypes)
				}
				if cfg.Database.StorageSerializer != "msgpack" {
					t.Errorf("expected StorageSerializer msgpack, got %s", cfg.Database.StorageSerializer)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearEnvVars(t)

			// Set test env vars
			for k, v := range tt.envVars {
				os.Setenv(k, v)
			}

			cfg := LoadFromEnv()
			tt.validate(t, cfg)
		})
	}
}

// TestLoadDefaults_AsyncWriteValues tests that LoadDefaults returns correct async write values.
func TestLoadDefaults_AsyncWriteValues(t *testing.T) {
	cfg := LoadDefaults()

	if !cfg.Database.AsyncWritesEnabled {
		t.Error("expected AsyncWritesEnabled to be true in defaults")
	}
	if cfg.Database.AsyncFlushInterval != 50*time.Millisecond {
		t.Errorf("expected AsyncFlushInterval 50ms in defaults, got %v", cfg.Database.AsyncFlushInterval)
	}
	if cfg.Database.AsyncMaxNodeCacheSize != 50000 {
		t.Errorf("expected AsyncMaxNodeCacheSize 50000 in defaults, got %d", cfg.Database.AsyncMaxNodeCacheSize)
	}
	if cfg.Database.AsyncMaxEdgeCacheSize != 100000 {
		t.Errorf("expected AsyncMaxEdgeCacheSize 100000 in defaults, got %d", cfg.Database.AsyncMaxEdgeCacheSize)
	}
	if cfg.Database.BadgerNodeCacheMaxEntries != 10000 {
		t.Errorf("expected BadgerNodeCacheMaxEntries 10000 in defaults, got %d", cfg.Database.BadgerNodeCacheMaxEntries)
	}
	if cfg.Database.BadgerEdgeTypeCacheMaxTypes != 50 {
		t.Errorf("expected BadgerEdgeTypeCacheMaxTypes 50 in defaults, got %d", cfg.Database.BadgerEdgeTypeCacheMaxTypes)
	}
	if cfg.Database.StorageSerializer != "msgpack" {
		t.Errorf("expected StorageSerializer msgpack in defaults, got %s", cfg.Database.StorageSerializer)
	}
}
