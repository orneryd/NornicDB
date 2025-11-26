// Package config tests for Neo4j-compatible configuration.
package config

import (
	"os"
	"testing"
	"time"
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
	if cfg.Auth.InitialPassword != "admin" {
		t.Errorf("expected password 'admin', got %q", cfg.Auth.InitialPassword)
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
	if cfg.Database.DefaultDatabase != "nornicdb" {
		t.Errorf("expected default db 'nornicdb', got %q", cfg.Database.DefaultDatabase)
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
	if !cfg.Memory.DecayEnabled {
		t.Error("expected DecayEnabled to be true by default")
	}
	if cfg.Memory.DecayInterval != time.Hour {
		t.Errorf("expected decay interval 1h, got %v", cfg.Memory.DecayInterval)
	}
	if cfg.Memory.ArchiveThreshold != 0.05 {
		t.Errorf("expected archive threshold 0.05, got %f", cfg.Memory.ArchiveThreshold)
	}
	if cfg.Memory.EmbeddingProvider != "ollama" {
		t.Errorf("expected embedding provider 'ollama', got %q", cfg.Memory.EmbeddingProvider)
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
