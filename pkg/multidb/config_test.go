package multidb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewConfigFromDefaultDatabase(t *testing.T) {
	tests := []struct {
		name           string
		defaultDB      string
		expectedDB     string
		expectedSystem string
	}{
		{
			name:           "custom database name",
			defaultDB:      "customdb",
			expectedDB:     "customdb",
			expectedSystem: "system",
		},
		{
			name:           "empty string falls back to nornic",
			defaultDB:      "",
			expectedDB:     "nornic",
			expectedSystem: "system",
		},
		{
			name:           "nornic database",
			defaultDB:      "nornic",
			expectedDB:     "nornic",
			expectedSystem: "system",
		},
		{
			name:           "legacy nornicdb name",
			defaultDB:      "nornicdb",
			expectedDB:     "nornicdb",
			expectedSystem: "system",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := NewConfigFromDefaultDatabase(tt.defaultDB)
			assert.Equal(t, tt.expectedDB, config.DefaultDatabase)
			assert.Equal(t, tt.expectedSystem, config.SystemDatabase)
			assert.Equal(t, 0, config.MaxDatabases) // Unlimited
			assert.False(t, config.AllowDropDefault)
		})
	}
}

func TestConfig_ConfigurationPrecedence(t *testing.T) {
	// This test verifies that configuration precedence works correctly
	// when DatabaseManager is initialized from main config.
	// The actual precedence is handled in pkg/config, but we verify
	// that NewConfigFromDefaultDatabase respects the value passed to it.

	// Test that the value passed is used exactly
	config := NewConfigFromDefaultDatabase("from-config")
	assert.Equal(t, "from-config", config.DefaultDatabase)

	// Test that empty string is handled
	config = NewConfigFromDefaultDatabase("")
	assert.Equal(t, "nornic", config.DefaultDatabase)
}

func TestConfig_EnvironmentVariableSupport(t *testing.T) {
	// Note: Actual env var parsing is in pkg/config
	// This test verifies that if a value comes from env var,
	// it's properly passed through to DatabaseManager

	// Save original env
	originalEnv := os.Getenv("NORNICDB_DEFAULT_DATABASE")
	defer func() {
		if originalEnv != "" {
			os.Setenv("NORNICDB_DEFAULT_DATABASE", originalEnv)
		} else {
			os.Unsetenv("NORNICDB_DEFAULT_DATABASE")
		}
	}()

	// Set env var
	os.Setenv("NORNICDB_DEFAULT_DATABASE", "env-database")

	// Simulate what would happen: config.Load() would read env var
	// and pass it to NewConfigFromDefaultDatabase
	// For this test, we just verify the function works with the value
	config := NewConfigFromDefaultDatabase("env-database")
	assert.Equal(t, "env-database", config.DefaultDatabase)
}
