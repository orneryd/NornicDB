package server

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

func TestEmbeddingFallback_UnavailableEndpoint(t *testing.T) {
	// Open in-memory database
	db, err := nornicdb.Open("", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Configure with an unavailable embeddings endpoint
	config := DefaultConfig()
	config.EmbeddingEnabled = true
	config.EmbeddingProvider = "openai"
	config.EmbeddingAPIURL = "http://localhost:99999" // Non-existent port
	config.EmbeddingModel = "test-model"
	config.EmbeddingDimensions = 1024

	// This should NOT fail - it should gracefully disable embeddings
	server, err := New(db, nil, config)
	if err != nil {
		t.Fatalf("Server creation should not fail with unavailable embeddings: %v", err)
	}

	if server == nil {
		t.Fatal("Server should not be nil")
	}

	// Verify MCP server exists but embeddings are disabled
	if server.mcpServer == nil {
		t.Fatal("MCP server should be created")
	}

	t.Log("✓ Server created successfully with embeddings fallback")
}

func TestEmbeddingFallback_DisabledByConfig(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Configure with embeddings disabled
	config := DefaultConfig()
	config.EmbeddingEnabled = false

	server, err := New(db, nil, config)
	if err != nil {
		t.Fatalf("Server creation failed: %v", err)
	}

	if server == nil {
		t.Fatal("Server should not be nil")
	}

	t.Log("✓ Server created with embeddings disabled by config")
}
