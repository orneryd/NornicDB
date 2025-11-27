// Package config handles Neo4j-compatible configuration via environment variables.
//
// NornicDB uses environment variables for configuration to maintain compatibility with
// Neo4j tooling and deployment workflows. All Neo4j environment variables are supported,
// plus NornicDB-specific extensions prefixed with NORNICDB_.
//
// Configuration is loaded from environment variables using LoadFromEnv() and can be
// validated with Validate() before use.
//
// Example Usage:
//
//	config := config.LoadFromEnv()
//	if err := config.Validate(); err != nil {
//		log.Fatalf("Invalid config: %v", err)
//	}
//
//	fmt.Printf("Bolt server: %s:%d\n",
//		config.Server.BoltAddress, config.Server.BoltPort)
//
// Environment Variables:
//
// Neo4j-Compatible:
//   - NEO4J_AUTH="username/password" or "none"
//   - NEO4J_dbms_connector_bolt_listen__address_port=7687
//   - NEO4J_dbms_connector_http_listen__address_port=7474
//   - NEO4J_dbms_directories_data="./data"
//
// NornicDB-Specific:
//   - NORNICDB_MEMORY_DECAY_ENABLED=true
//   - NORNICDB_MEMORY_DECAY_INTERVAL=1h
//   - NORNICDB_EMBEDDING_PROVIDER="ollama" or "openai"
//   - NORNICDB_EMBEDDING_MODEL="mxbai-embed-large"
//   - NORNICDB_AUDIT_ENABLED=true
//
// For a complete list, see the Config struct field documentation.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all NornicDB configuration loaded from environment variables.
//
// Configuration is organized into logical sections:
//   - Auth: Authentication and authorization
//   - Database: Storage and transaction settings
//   - Server: Bolt and HTTP server settings
//   - Memory: NornicDB-specific memory decay and embeddings
//   - Compliance: GDPR/HIPAA/FISMA/SOC2 compliance controls
//   - Logging: Logging configuration
//   - Features: Experimental and optional features (feature flags)
//
// Use LoadFromEnv() to create a Config from environment variables.
//
// Example:
//
//	config := config.LoadFromEnv()
//	if err := config.Validate(); err != nil {
//		log.Fatal(err)
//	}
//
//	fmt.Printf("Config: %s\n", config)
type Config struct {
	// Authentication (NEO4J_AUTH format: "username/password" or "none")
	Auth AuthConfig

	// Database settings
	Database DatabaseConfig

	// Server settings
	Server ServerConfig

	// Memory/Decay settings (NornicDB-specific)
	Memory MemoryConfig

	// Compliance settings for GDPR/HIPAA/FISMA/SOC2 (NornicDB-specific)
	Compliance ComplianceConfig

	// Logging
	Logging LoggingConfig

	// Feature flags for experimental/optional features
	Features FeatureFlagsConfig
}

// AuthConfig holds authentication settings.
type AuthConfig struct {
	// Enabled controls whether authentication is required
	Enabled bool
	// InitialUsername is the default admin username
	InitialUsername string
	// InitialPassword is the default admin password
	InitialPassword string
	// MinPasswordLength for password policy
	MinPasswordLength int
	// TokenExpiry for JWT tokens
	TokenExpiry time.Duration
	// JWTSecret for signing tokens
	JWTSecret string
}

// DatabaseConfig holds database settings.
type DatabaseConfig struct {
	// DataDir is the directory for data storage
	DataDir string
	// DefaultDatabase name
	DefaultDatabase string
	// ReadOnly mode
	ReadOnly bool
	// TransactionTimeout for long-running queries
	TransactionTimeout time.Duration
	// MaxConcurrentTransactions limit
	MaxConcurrentTransactions int
}

// ServerConfig holds server settings.
type ServerConfig struct {
	// BoltEnabled controls Bolt protocol server
	BoltEnabled bool
	// BoltPort for Bolt connections (default 7687)
	BoltPort int
	// BoltAddress to bind to
	BoltAddress string
	// BoltTLSEnabled for encrypted connections
	BoltTLSEnabled bool
	// BoltTLSCert path to certificate
	BoltTLSCert string
	// BoltTLSKey path to private key
	BoltTLSKey string

	// HTTPEnabled controls HTTP API server
	HTTPEnabled bool
	// HTTPPort for HTTP connections (default 7474)
	HTTPPort int
	// HTTPAddress to bind to
	HTTPAddress string
	// HTTPSEnabled for encrypted connections
	HTTPSEnabled bool
	// HTTPSPort for HTTPS connections (default 7473)
	HTTPSPort int
	// HTTPTLSCert path to certificate
	HTTPTLSCert string
	// HTTPTLSKey path to private key
	HTTPTLSKey string
}

// MemoryConfig holds NornicDB memory decay settings.
type MemoryConfig struct {
	// DecayEnabled controls memory decay
	DecayEnabled bool
	// DecayInterval for recalculation
	DecayInterval time.Duration
	// ArchiveThreshold below which memories are archived
	ArchiveThreshold float64
	// EmbeddingProvider (ollama, openai)
	EmbeddingProvider string
	// EmbeddingModel name
	EmbeddingModel string
	// EmbeddingAPIURL endpoint
	EmbeddingAPIURL string
	// EmbeddingDimensions size
	EmbeddingDimensions int
	// AutoLinksEnabled for automatic relationship detection
	AutoLinksEnabled bool
	// AutoLinksSimilarityThreshold for similarity-based links
	AutoLinksSimilarityThreshold float64
}

// ComplianceConfig holds settings for GDPR/HIPAA/FISMA/SOC2 compliance.
// These are framework-agnostic controls that satisfy multiple regulations.
type ComplianceConfig struct {
	// AuditLogging - Required by: GDPR Art.30, HIPAA §164.312(b), FISMA, SOC2
	AuditEnabled       bool
	AuditLogPath       string
	AuditRetentionDays int // How long to keep audit logs (HIPAA: 6 years, SOC2: 7 years)

	// Data Retention - Required by: GDPR Art.5(1)(e), HIPAA §164.530(j)
	RetentionEnabled     bool
	RetentionPolicyDays  int  // Default retention period (0 = indefinite)
	RetentionAutoDelete  bool // Auto-delete vs archive after retention
	RetentionExemptRoles []string // Roles exempt from retention (e.g., "admin")

	// Access Control - Required by: GDPR Art.32, HIPAA §164.312(a), FISMA
	AccessControlEnabled bool
	SessionTimeout       time.Duration
	MaxFailedLogins      int
	LockoutDuration      time.Duration

	// Encryption - Required by: GDPR Art.32, HIPAA §164.312(a)(2)(iv)
	EncryptionAtRest    bool
	EncryptionInTransit bool
	EncryptionKeyPath   string

	// Data Subject Rights - Required by: GDPR Art.15-20
	DataExportEnabled  bool // Right to data portability
	DataErasureEnabled bool // Right to erasure/be forgotten
	DataAccessEnabled  bool // Right of access

	// Anonymization - Required by: GDPR Recital 26
	AnonymizationEnabled bool
	AnonymizationMethod  string // "pseudonymization", "generalization", "suppression"

	// Consent - Required by: GDPR Art.7
	ConsentRequired      bool
	ConsentVersioning    bool
	ConsentAuditTrail    bool

	// Breach Notification - Required by: GDPR Art.33-34, HIPAA §164.408
	BreachDetectionEnabled bool
	BreachNotifyEmail      string
	BreachNotifyWebhook    string
}

// LoggingConfig holds logging settings.
type LoggingConfig struct {
	// Level (DEBUG, INFO, WARN, ERROR)
	Level string
	// Format (json, text)
	Format string
	// Output path (stdout, stderr, or file path)
	Output string
	// QueryLogEnabled for query logging
	QueryLogEnabled bool
	// SlowQueryThreshold for logging slow queries
	SlowQueryThreshold time.Duration
}

// FeatureFlagsConfig holds all feature flags for experimental/optional features.
// Centralized location for all feature toggles in NornicDB.
type FeatureFlagsConfig struct {
	// Kalman filtering for predictive smoothing
	KalmanEnabled bool
	
	// Topological link prediction AUTOMATIC integration
	// NOTE: Neo4j GDS procedures (CALL gds.linkPrediction.*) are ALWAYS available
	// This flag only controls automatic integration with inference.Engine.OnStore()
	TopologyAutoIntegrationEnabled bool    // Enable automatic topology in OnStore()
	TopologyAlgorithm              string  // adamic_adar, jaccard, etc.
	TopologyWeight                 float64 // 0.0-1.0, weight vs semantic
	TopologyTopK                   int
	TopologyMinScore               float64
	TopologyGraphRefreshInterval   int
	
	// A/B testing for automatic topology integration
	TopologyABTestEnabled    bool
	TopologyABTestPercentage int // 0-100
}

// LoadFromEnv loads configuration from environment variables.
//
// This function reads all configuration from the environment, using Neo4j-compatible
// variable names where applicable (e.g., NEO4J_AUTH, NEO4J_dbms_*) and NornicDB-specific
// variables prefixed with NORNICDB_.
//
// All values have sensible defaults, so LoadFromEnv() can be called without any
// environment variables set.
//
// Example:
//
//	// Minimal setup - uses all defaults
//	config := config.LoadFromEnv()
//
//	// With custom environment
//	os.Setenv("NEO4J_AUTH", "myuser/mypass")
//	os.Setenv("NEO4J_dbms_connector_bolt_listen__address_port", "7688")
//	os.Setenv("NORNICDB_EMBEDDING_PROVIDER", "openai")
//	os.Setenv("NORNICDB_EMBEDDING_API_KEY", "sk-...")
//	config = config.LoadFromEnv()
//
//	if err := config.Validate(); err != nil {
//		log.Fatal(err)
//	}
//
// Returns a fully populated Config with defaults applied where environment
// variables are not set.
func LoadFromEnv() *Config {
	config := &Config{}

	// Authentication - NEO4J_AUTH format: "username/password" or "none"
	// Default: disabled for easy development
	authStr := getEnv("NEO4J_AUTH", "none")
	if authStr == "none" {
		config.Auth.Enabled = false
		config.Auth.InitialUsername = "admin"
		config.Auth.InitialPassword = "admin"
	} else {
		config.Auth.Enabled = true
		parts := strings.SplitN(authStr, "/", 2)
		if len(parts) == 2 {
			config.Auth.InitialUsername = parts[0]
			config.Auth.InitialPassword = parts[1]
		} else {
			config.Auth.InitialUsername = "admin"
			config.Auth.InitialPassword = authStr
		}
	}
	config.Auth.MinPasswordLength = getEnvInt("NEO4J_dbms_security_auth_minimum__password__length", 8)
	config.Auth.TokenExpiry = getEnvDuration("NORNICDB_AUTH_TOKEN_EXPIRY", 24*time.Hour)
	config.Auth.JWTSecret = getEnv("NORNICDB_AUTH_JWT_SECRET", generateDefaultSecret())

	// Database settings
	config.Database.DataDir = getEnv("NEO4J_dbms_directories_data", "./data")
	config.Database.DefaultDatabase = getEnv("NEO4J_dbms_default__database", "nornicdb")
	config.Database.ReadOnly = getEnvBool("NEO4J_dbms_read__only", false)
	config.Database.TransactionTimeout = getEnvDuration("NEO4J_dbms_transaction_timeout", 30*time.Second)
	config.Database.MaxConcurrentTransactions = getEnvInt("NEO4J_dbms_transaction_concurrent_maximum", 1000)

	// Server settings - Bolt
	config.Server.BoltEnabled = getEnvBool("NEO4J_dbms_connector_bolt_enabled", true)
	config.Server.BoltPort = getEnvInt("NEO4J_dbms_connector_bolt_listen__address_port", 7687)
	config.Server.BoltAddress = getEnv("NEO4J_dbms_connector_bolt_listen__address", "0.0.0.0")
	config.Server.BoltTLSEnabled = getEnvBool("NEO4J_dbms_connector_bolt_tls__level", false)
	config.Server.BoltTLSCert = getEnv("NEO4J_dbms_ssl_policy_bolt_base__directory", "") + "/public.crt"
	config.Server.BoltTLSKey = getEnv("NEO4J_dbms_ssl_policy_bolt_base__directory", "") + "/private.key"

	// Server settings - HTTP
	config.Server.HTTPEnabled = getEnvBool("NEO4J_dbms_connector_http_enabled", true)
	config.Server.HTTPPort = getEnvInt("NEO4J_dbms_connector_http_listen__address_port", 7474)
	config.Server.HTTPAddress = getEnv("NEO4J_dbms_connector_http_listen__address", "0.0.0.0")
	config.Server.HTTPSEnabled = getEnvBool("NEO4J_dbms_connector_https_enabled", false)
	config.Server.HTTPSPort = getEnvInt("NEO4J_dbms_connector_https_listen__address_port", 7473)
	config.Server.HTTPTLSCert = getEnv("NEO4J_dbms_ssl_policy_https_base__directory", "") + "/public.crt"
	config.Server.HTTPTLSKey = getEnv("NEO4J_dbms_ssl_policy_https_base__directory", "") + "/private.key"

	// Memory settings (NornicDB-specific, prefixed with NORNICDB_)
	config.Memory.DecayEnabled = getEnvBool("NORNICDB_MEMORY_DECAY_ENABLED", true)
	config.Memory.DecayInterval = getEnvDuration("NORNICDB_MEMORY_DECAY_INTERVAL", time.Hour)
	config.Memory.ArchiveThreshold = getEnvFloat("NORNICDB_MEMORY_ARCHIVE_THRESHOLD", 0.05)
	config.Memory.EmbeddingProvider = getEnv("NORNICDB_EMBEDDING_PROVIDER", "ollama")
	config.Memory.EmbeddingModel = getEnv("NORNICDB_EMBEDDING_MODEL", "mxbai-embed-large")
	config.Memory.EmbeddingAPIURL = getEnv("NORNICDB_EMBEDDING_API_URL", "http://localhost:11434")
	config.Memory.EmbeddingDimensions = getEnvInt("NORNICDB_EMBEDDING_DIMENSIONS", 1024)
	config.Memory.AutoLinksEnabled = getEnvBool("NORNICDB_AUTO_LINKS_ENABLED", true)
	config.Memory.AutoLinksSimilarityThreshold = getEnvFloat("NORNICDB_AUTO_LINKS_THRESHOLD", 0.82)

	// Compliance settings (NornicDB-specific, framework-agnostic)
	// Audit Logging
	config.Compliance.AuditEnabled = getEnvBool("NORNICDB_AUDIT_ENABLED", true)
	config.Compliance.AuditLogPath = getEnv("NORNICDB_AUDIT_LOG_PATH", "./logs/audit.log")
	config.Compliance.AuditRetentionDays = getEnvInt("NORNICDB_AUDIT_RETENTION_DAYS", 2555) // ~7 years for SOC2

	// Data Retention
	config.Compliance.RetentionEnabled = getEnvBool("NORNICDB_RETENTION_ENABLED", false)
	config.Compliance.RetentionPolicyDays = getEnvInt("NORNICDB_RETENTION_POLICY_DAYS", 0)
	config.Compliance.RetentionAutoDelete = getEnvBool("NORNICDB_RETENTION_AUTO_DELETE", false)
	config.Compliance.RetentionExemptRoles = getEnvStringSlice("NORNICDB_RETENTION_EXEMPT_ROLES", []string{"admin"})

	// Access Control
	config.Compliance.AccessControlEnabled = getEnvBool("NORNICDB_ACCESS_CONTROL_ENABLED", true)
	config.Compliance.SessionTimeout = getEnvDuration("NORNICDB_SESSION_TIMEOUT", 30*time.Minute)
	config.Compliance.MaxFailedLogins = getEnvInt("NORNICDB_MAX_FAILED_LOGINS", 5)
	config.Compliance.LockoutDuration = getEnvDuration("NORNICDB_LOCKOUT_DURATION", 15*time.Minute)

	// Encryption
	config.Compliance.EncryptionAtRest = getEnvBool("NORNICDB_ENCRYPTION_AT_REST", false)
	config.Compliance.EncryptionInTransit = getEnvBool("NORNICDB_ENCRYPTION_IN_TRANSIT", true)
	config.Compliance.EncryptionKeyPath = getEnv("NORNICDB_ENCRYPTION_KEY_PATH", "")

	// Data Subject Rights
	config.Compliance.DataExportEnabled = getEnvBool("NORNICDB_DATA_EXPORT_ENABLED", true)
	config.Compliance.DataErasureEnabled = getEnvBool("NORNICDB_DATA_ERASURE_ENABLED", true)
	config.Compliance.DataAccessEnabled = getEnvBool("NORNICDB_DATA_ACCESS_ENABLED", true)

	// Anonymization
	config.Compliance.AnonymizationEnabled = getEnvBool("NORNICDB_ANONYMIZATION_ENABLED", true)
	config.Compliance.AnonymizationMethod = getEnv("NORNICDB_ANONYMIZATION_METHOD", "pseudonymization")

	// Consent
	config.Compliance.ConsentRequired = getEnvBool("NORNICDB_CONSENT_REQUIRED", false)
	config.Compliance.ConsentVersioning = getEnvBool("NORNICDB_CONSENT_VERSIONING", true)
	config.Compliance.ConsentAuditTrail = getEnvBool("NORNICDB_CONSENT_AUDIT_TRAIL", true)

	// Breach Notification
	config.Compliance.BreachDetectionEnabled = getEnvBool("NORNICDB_BREACH_DETECTION_ENABLED", false)
	config.Compliance.BreachNotifyEmail = getEnv("NORNICDB_BREACH_NOTIFY_EMAIL", "")
	config.Compliance.BreachNotifyWebhook = getEnv("NORNICDB_BREACH_NOTIFY_WEBHOOK", "")

	// Logging settings
	config.Logging.Level = getEnv("NEO4J_dbms_logs_debug_level", "INFO")
	config.Logging.Format = getEnv("NORNICDB_LOG_FORMAT", "json")
	config.Logging.Output = getEnv("NORNICDB_LOG_OUTPUT", "stdout")
	config.Logging.QueryLogEnabled = getEnvBool("NEO4J_dbms_logs_query_enabled", false)
	config.Logging.SlowQueryThreshold = getEnvDuration("NEO4J_dbms_logs_query_threshold", 5*time.Second)

	// Feature flags
	config.Features.KalmanEnabled = getEnvBool("NORNICDB_KALMAN_ENABLED", false)
	// Topology procedures are always available; this controls automatic integration only
	config.Features.TopologyAutoIntegrationEnabled = getEnvBool("NORNICDB_TOPOLOGY_AUTO_INTEGRATION_ENABLED", false)
	config.Features.TopologyAlgorithm = getEnv("NORNICDB_TOPOLOGY_ALGORITHM", "adamic_adar")
	config.Features.TopologyWeight = getEnvFloat("NORNICDB_TOPOLOGY_WEIGHT", 0.4)
	config.Features.TopologyTopK = getEnvInt("NORNICDB_TOPOLOGY_TOPK", 10)
	config.Features.TopologyMinScore = getEnvFloat("NORNICDB_TOPOLOGY_MIN_SCORE", 0.3)
	config.Features.TopologyGraphRefreshInterval = getEnvInt("NORNICDB_TOPOLOGY_GRAPH_REFRESH_INTERVAL", 100)
	config.Features.TopologyABTestEnabled = getEnvBool("NORNICDB_TOPOLOGY_AB_TEST_ENABLED", false)
	config.Features.TopologyABTestPercentage = getEnvInt("NORNICDB_TOPOLOGY_AB_TEST_PERCENTAGE", 50)

	return config
}

// Validate checks the configuration for logical errors and invalid values.
//
// This method checks:
//   - Authentication is properly configured if enabled
//   - Password meets minimum length requirements
//   - Port numbers are valid (> 0)
//   - Embedding dimensions are positive
//
// Call Validate() after LoadFromEnv() and before using the Config.
//
// Example:
//
//	config := config.LoadFromEnv()
//	if err := config.Validate(); err != nil {
//		log.Fatalf("Configuration error: %v", err)
//	}
//	// Config is valid, proceed with startup
//
// Returns nil if configuration is valid, or an error describing the problem.
func (c *Config) Validate() error {
	if c.Auth.Enabled {
		if c.Auth.InitialUsername == "" {
			return fmt.Errorf("authentication enabled but no username provided")
		}
		if len(c.Auth.InitialPassword) < c.Auth.MinPasswordLength {
			return fmt.Errorf("password must be at least %d characters", c.Auth.MinPasswordLength)
		}
	}

	if c.Server.BoltEnabled && c.Server.BoltPort <= 0 {
		return fmt.Errorf("invalid bolt port: %d", c.Server.BoltPort)
	}

	if c.Server.HTTPEnabled && c.Server.HTTPPort <= 0 {
		return fmt.Errorf("invalid http port: %d", c.Server.HTTPPort)
	}

	if c.Memory.EmbeddingDimensions <= 0 {
		return fmt.Errorf("invalid embedding dimensions: %d", c.Memory.EmbeddingDimensions)
	}

	return nil
}

// String returns a safe string representation of the Config.
//
// Sensitive values like passwords and API keys are NOT included in the output,
// making this safe for logging.
//
// Example:
//
//	config := config.LoadFromEnv()
//	log.Printf("Starting with config: %s", config)
//	// Output: Config{Auth: true, Bolt: 0.0.0.0:7687, HTTP: 0.0.0.0:7474, DataDir: ./data}
//
// Returns a string suitable for logging and debugging.
func (c *Config) String() string {
	return fmt.Sprintf(
		"Config{Auth: %v, Bolt: %s:%d, HTTP: %s:%d, DataDir: %s}",
		c.Auth.Enabled,
		c.Server.BoltAddress, c.Server.BoltPort,
		c.Server.HTTPAddress, c.Server.HTTPPort,
		c.Database.DataDir,
	)
}

// Helper functions for environment variable parsing

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvFloat(key string, defaultVal float64) float64 {
	if val := os.Getenv(key); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return defaultVal
}

func getEnvBool(key string, defaultVal bool) bool {
	if val := os.Getenv(key); val != "" {
		val = strings.ToLower(val)
		return val == "true" || val == "1" || val == "yes" || val == "on"
	}
	return defaultVal
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
		// Try parsing as seconds
		if secs, err := strconv.Atoi(val); err == nil {
			return time.Duration(secs) * time.Second
		}
	}
	return defaultVal
}

func getEnvStringSlice(key string, defaultVal []string) []string {
	if val := os.Getenv(key); val != "" {
		// Split by comma, trim whitespace
		parts := strings.Split(val, ",")
		result := make([]string, 0, len(parts))
		for _, p := range parts {
			if trimmed := strings.TrimSpace(p); trimmed != "" {
				result = append(result, trimmed)
			}
		}
		if len(result) > 0 {
			return result
		}
	}
	return defaultVal
}

func generateDefaultSecret() string {
	// In production, this should be explicitly set
	return "CHANGE_ME_IN_PRODUCTION_" + strconv.FormatInt(time.Now().UnixNano(), 36)
}
