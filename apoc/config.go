// Package apoc configuration provides APOC function configuration for NornicDB.
//
// Configuration can be loaded from:
//   - Environment variables (recommended for Docker/K8s)
//   - YAML configuration file
//   - Programmatic defaults
//
// Environment Variables:
//
//	NORNICDB_APOC_PLUGINS_DIR       - Directory to auto-load .so plugins from
//	NORNICDB_APOC_COLL_ENABLED      - Enable collection functions (default: true)
//	NORNICDB_APOC_TEXT_ENABLED      - Enable text functions (default: true)
//	NORNICDB_APOC_MATH_ENABLED      - Enable math functions (default: true)
//	NORNICDB_APOC_ALGO_ENABLED      - Enable graph algorithms (default: true)
//	NORNICDB_APOC_CREATE_ENABLED    - Enable dynamic creation (default: true)
//	NORNICDB_APOC_SECURITY_ALLOW_FILE_ACCESS - Allow file operations (default: false)
//	NORNICDB_APOC_SECURITY_MAX_COLLECTION_SIZE - Max collection size (default: 100000)
//
// Example Docker Usage:
//
//	docker run -e NORNICDB_APOC_PLUGINS_DIR=/plugins \
//	           -e NORNICDB_APOC_ALGO_ENABLED=false \
//	           -v ./my-plugins:/plugins \
//	           nornicdb/nornicdb
package apoc

import (
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config controls which APOC functions are enabled.
//
// Example:
//
//	// Load from environment (Docker/K8s friendly)
//	cfg := apoc.LoadFromEnv()
//
//	// Or load from YAML file
//	cfg, err := apoc.LoadConfig("./apoc.yaml")
//
//	// Or use defaults
//	cfg := apoc.DefaultConfig()
type Config struct {
	// Categories controls entire function categories
	// Keys: coll, text, math, convert, map, date, json, util, agg, node, path, algo, create
	Categories map[string]bool `yaml:"categories"`

	// Functions controls specific functions (overrides category settings)
	// Supports wildcards: "apoc.export.*": false
	Functions map[string]bool `yaml:"functions"`

	// Security settings
	Security SecurityConfig `yaml:"security"`

	// PluginsDir is the directory to auto-load .so plugins from (optional)
	// If set, all .so files in this directory are loaded on Initialize()
	// Can be set via NORNICDB_APOC_PLUGINS_DIR environment variable
	PluginsDir string `yaml:"plugins_dir"`
}

// SecurityConfig contains security-related settings.
type SecurityConfig struct {
	// AllowDynamicCreation permits apoc.create.* functions
	AllowDynamicCreation bool `yaml:"allow_dynamic_creation"`
	// AllowFileAccess permits file system operations
	AllowFileAccess bool `yaml:"allow_file_access"`
	// MaxCollectionSize limits collection function input sizes
	MaxCollectionSize int `yaml:"max_collection_size"`
}

// DefaultConfig returns a configuration with all functions enabled.
func DefaultConfig() *Config {
	return &Config{
		Categories: map[string]bool{
			"coll":    true,
			"text":    true,
			"math":    true,
			"convert": true,
			"map":     true,
			"date":    true,
			"json":    true,
			"util":    true,
			"agg":     true,
			"node":    true,
			"path":    true,
			"algo":    true,
			"create":  true,
		},
		Functions: make(map[string]bool),
		Security: SecurityConfig{
			AllowDynamicCreation: true,
			AllowFileAccess:      false,
			MaxCollectionSize:    100000,
		},
	}
}

// LoadFromEnv loads APOC configuration from environment variables.
//
// This is the recommended approach for Docker/Kubernetes deployments.
//
// Environment Variables:
//
//	NORNICDB_APOC_PLUGINS_DIR           - Plugin directory path
//	NORNICDB_APOC_COLL_ENABLED          - Enable apoc.coll.* (default: true)
//	NORNICDB_APOC_TEXT_ENABLED          - Enable apoc.text.* (default: true)
//	NORNICDB_APOC_MATH_ENABLED          - Enable apoc.math.* (default: true)
//	NORNICDB_APOC_CONVERT_ENABLED       - Enable apoc.convert.* (default: true)
//	NORNICDB_APOC_MAP_ENABLED           - Enable apoc.map.* (default: true)
//	NORNICDB_APOC_DATE_ENABLED          - Enable apoc.date.* (default: true)
//	NORNICDB_APOC_JSON_ENABLED          - Enable apoc.json.* (default: true)
//	NORNICDB_APOC_UTIL_ENABLED          - Enable apoc.util.* (default: true)
//	NORNICDB_APOC_AGG_ENABLED           - Enable apoc.agg.* (default: true)
//	NORNICDB_APOC_NODE_ENABLED          - Enable apoc.node.* (default: true)
//	NORNICDB_APOC_PATH_ENABLED          - Enable apoc.path.* (default: true)
//	NORNICDB_APOC_ALGO_ENABLED          - Enable apoc.algo.* (default: true)
//	NORNICDB_APOC_CREATE_ENABLED        - Enable apoc.create.* (default: true)
//	NORNICDB_APOC_SECURITY_ALLOW_DYNAMIC_CREATION - Allow dynamic creation (default: true)
//	NORNICDB_APOC_SECURITY_ALLOW_FILE_ACCESS      - Allow file access (default: false)
//	NORNICDB_APOC_SECURITY_MAX_COLLECTION_SIZE    - Max collection size (default: 100000)
//
// Example:
//
//	os.Setenv("NORNICDB_APOC_PLUGINS_DIR", "/opt/nornicdb/plugins")
//	os.Setenv("NORNICDB_APOC_ALGO_ENABLED", "false")
//	cfg := apoc.LoadFromEnv()
func LoadFromEnv() *Config {
	cfg := DefaultConfig()

	// Plugin directory
	if dir := os.Getenv("NORNICDB_APOC_PLUGINS_DIR"); dir != "" {
		cfg.PluginsDir = dir
	}

	// Category enables/disables
	categories := []string{"coll", "text", "math", "convert", "map", "date", "json", "util", "agg", "node", "path", "algo", "create"}
	for _, cat := range categories {
		envKey := "NORNICDB_APOC_" + strings.ToUpper(cat) + "_ENABLED"
		if val := os.Getenv(envKey); val != "" {
			cfg.Categories[cat] = parseBool(val, true)
		}
	}

	// Security settings
	if val := os.Getenv("NORNICDB_APOC_SECURITY_ALLOW_DYNAMIC_CREATION"); val != "" {
		cfg.Security.AllowDynamicCreation = parseBool(val, true)
	}
	if val := os.Getenv("NORNICDB_APOC_SECURITY_ALLOW_FILE_ACCESS"); val != "" {
		cfg.Security.AllowFileAccess = parseBool(val, false)
	}
	if val := os.Getenv("NORNICDB_APOC_SECURITY_MAX_COLLECTION_SIZE"); val != "" {
		if size, err := strconv.Atoi(val); err == nil {
			cfg.Security.MaxCollectionSize = size
		}
	}

	return cfg
}

// parseBool parses a boolean from string with a default value.
func parseBool(s string, defaultVal bool) bool {
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "true", "1", "yes", "on":
		return true
	case "false", "0", "no", "off":
		return false
	default:
		return defaultVal
	}
}

// LoadConfig loads configuration from a YAML file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	// Set defaults for unspecified categories
	if cfg.Categories == nil {
		cfg.Categories = make(map[string]bool)
	}
	if cfg.Functions == nil {
		cfg.Functions = make(map[string]bool)
	}

	return &cfg, nil
}

// LoadConfigOrDefault loads config from file, or returns default if file doesn't exist.
func LoadConfigOrDefault(path string) *Config {
	cfg, err := LoadConfig(path)
	if err != nil {
		return DefaultConfig()
	}
	return cfg
}

// LoadFromEnvOrFile loads config from environment variables first, then falls back to file.
// Environment variables take precedence over file settings.
func LoadFromEnvOrFile(filePath string) *Config {
	// Start with file config or defaults
	cfg := LoadConfigOrDefault(filePath)

	// Override with environment variables
	if dir := os.Getenv("NORNICDB_APOC_PLUGINS_DIR"); dir != "" {
		cfg.PluginsDir = dir
	}

	categories := []string{"coll", "text", "math", "convert", "map", "date", "json", "util", "agg", "node", "path", "algo", "create"}
	for _, cat := range categories {
		envKey := "NORNICDB_APOC_" + strings.ToUpper(cat) + "_ENABLED"
		if val := os.Getenv(envKey); val != "" {
			cfg.Categories[cat] = parseBool(val, cfg.Categories[cat])
		}
	}

	if val := os.Getenv("NORNICDB_APOC_SECURITY_ALLOW_DYNAMIC_CREATION"); val != "" {
		cfg.Security.AllowDynamicCreation = parseBool(val, cfg.Security.AllowDynamicCreation)
	}
	if val := os.Getenv("NORNICDB_APOC_SECURITY_ALLOW_FILE_ACCESS"); val != "" {
		cfg.Security.AllowFileAccess = parseBool(val, cfg.Security.AllowFileAccess)
	}
	if val := os.Getenv("NORNICDB_APOC_SECURITY_MAX_COLLECTION_SIZE"); val != "" {
		if size, err := strconv.Atoi(val); err == nil {
			cfg.Security.MaxCollectionSize = size
		}
	}

	return cfg
}

// IsEnabled checks if a specific function is enabled.
func (c *Config) IsEnabled(functionName string) bool {
	// Check specific function override first
	if enabled, ok := c.Functions[functionName]; ok {
		return enabled
	}

	// Check wildcard patterns (e.g., "apoc.export.*")
	for pattern, enabled := range c.Functions {
		if matchesPattern(functionName, pattern) {
			return enabled
		}
	}

	// Check category
	category := extractCategory(functionName)
	if enabled, ok := c.Categories[category]; ok {
		return enabled
	}

	// Default: enabled
	return true
}

// IsCategoryEnabled checks if a category is enabled.
func (c *Config) IsCategoryEnabled(category string) bool {
	if enabled, ok := c.Categories[category]; ok {
		return enabled
	}
	return true // Default: enabled
}

// extractCategory extracts the category from a function name.
// Example: "apoc.coll.sum" -> "coll"
func extractCategory(functionName string) string {
	parts := strings.Split(functionName, ".")
	if len(parts) >= 2 {
		return parts[1]
	}
	return ""
}

// matchesPattern checks if a function name matches a wildcard pattern.
// Example: "apoc.coll.sum" matches "apoc.coll.*"
func matchesPattern(functionName, pattern string) bool {
	if !strings.Contains(pattern, "*") {
		return functionName == pattern
	}

	// Simple wildcard matching
	prefix := strings.TrimSuffix(pattern, "*")
	return strings.HasPrefix(functionName, prefix)
}

// Example configuration file content
const ExampleConfigYAML = `# APOC Configuration for NornicDB
# Controls which APOC functions are available

# Plugin directory for custom .so plugins (optional)
plugins_dir: /opt/nornicdb/plugins

# Enable/disable entire categories
categories:
  coll: true        # Collection functions
  text: true        # Text processing
  math: true        # Mathematical operations
  convert: true     # Type conversions
  map: true         # Map operations
  date: true        # Date/time functions
  json: true        # JSON operations
  util: true        # Utility functions
  agg: true         # Aggregation functions
  node: true        # Node operations
  path: true        # Path finding
  algo: false       # Graph algorithms (expensive, disabled by default)
  create: false     # Dynamic creation (write operations, disabled by default)

# Enable/disable specific functions (overrides category settings)
functions:
  # Disable all export functions
  "apoc.export.*": false
  
  # Disable all import functions
  "apoc.import.*": false
  
  # Enable specific algorithm
  "apoc.algo.pageRank": true

# Security settings
security:
  allow_dynamic_creation: false
  allow_file_access: false
  max_collection_size: 10000
`
