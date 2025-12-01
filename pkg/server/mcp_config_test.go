package server

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

func TestMCPEnabled_Default(t *testing.T) {
	config := DefaultConfig()
	if !config.MCPEnabled {
		t.Error("MCPEnabled should be true by default")
	}
}

func TestMCPServer_EnabledByDefault(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Default config should have MCP enabled
	config := DefaultConfig()
	config.EmbeddingEnabled = false // Disable embeddings to skip health check

	server, err := New(db, nil, config)
	if err != nil {
		t.Fatalf("Server creation failed: %v", err)
	}

	if server == nil {
		t.Fatal("Server should not be nil")
	}

	if server.mcpServer == nil {
		t.Fatal("MCP server should be created when MCPEnabled is true (default)")
	}

	t.Log("✓ MCP server enabled by default")
}

func TestMCPServer_DisabledByConfig(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Explicitly disable MCP
	config := DefaultConfig()
	config.MCPEnabled = false
	config.EmbeddingEnabled = false

	server, err := New(db, nil, config)
	if err != nil {
		t.Fatalf("Server creation failed: %v", err)
	}

	if server == nil {
		t.Fatal("Server should not be nil")
	}

	// MCP server should be nil when disabled
	if server.mcpServer != nil {
		t.Fatal("MCP server should be nil when MCPEnabled is false")
	}

	t.Log("✓ MCP server correctly disabled by config")
}

func TestMCPServer_DisabledStillAllowsHTTP(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Disable MCP but keep server functional
	config := DefaultConfig()
	config.MCPEnabled = false
	config.EmbeddingEnabled = false

	server, err := New(db, nil, config)
	if err != nil {
		t.Fatalf("Server creation failed: %v", err)
	}

	// Server should still be functional for HTTP API
	if server.db == nil {
		t.Fatal("Database reference should be set")
	}

	if server.config == nil {
		t.Fatal("Config should be set")
	}

	t.Log("✓ HTTP server still functional with MCP disabled")
}
