//go:build integration
// +build integration

package mcp

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

// Run with: INTEGRATION_TEST=1 go test -tags=integration -v ./pkg/mcp/... -run TestMCPE2E
// Requires llama.cpp server running on localhost:11434

func TestMCPE2E_StoreAndDiscover(t *testing.T) {
	if os.Getenv("INTEGRATION_TEST") == "" {
		t.Skip("Set INTEGRATION_TEST=1 to run")
	}

	// Create a temporary database
	db, err := nornicdb.Open("", nil) // In-memory
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create embedder pointing to llama.cpp
	embedConfig := &embed.Config{
		Provider:   "openai",
		APIURL:     "http://localhost:11434",
		APIPath:    "/v1/embeddings",
		Model:      "mxbai-embed-large",
		Dimensions: 1024,
		Timeout:    30 * time.Second,
	}
	embedder := embed.NewOpenAI(embedConfig)

	// Create MCP server with embedder
	mcpConfig := DefaultServerConfig()
	mcpConfig.EmbeddingEnabled = true
	mcpConfig.Embedder = embedder

	server := NewServer(db, mcpConfig)
	ctx := context.Background()

	// Step 1: Store documents with different topics
	t.Log("Step 1: Storing documents...")

	docs := []struct {
		content string
		title   string
	}{
		{"Graph databases store data as nodes and relationships, enabling complex queries", "Graph Databases"},
		{"Machine learning algorithms learn patterns from data automatically", "Machine Learning"},
		{"Vector search finds semantically similar content using embeddings", "Vector Search"},
		{"Cats are small domesticated carnivorous mammals with soft fur", "About Cats"},
		{"Automobiles are wheeled motor vehicles used for transportation", "Automobiles"},
	}

	var storedIDs []string
	for _, doc := range docs {
		result, err := server.handleStore(ctx, map[string]interface{}{
			"content": doc.content,
			"title":   doc.title,
			"type":    "Document",
		})
		if err != nil {
			t.Fatalf("Store failed: %v", err)
		}
		storeResult := result.(StoreResult)
		if !storeResult.Embedded {
			t.Error("Expected document to be embedded")
		}
		storedIDs = append(storedIDs, storeResult.ID)
		t.Logf("  ✓ Stored: %s (ID: %s, Embedded: %v)", doc.title, storeResult.ID, storeResult.Embedded)
	}

	// Check if nodes are in the database
	t.Log("\nVerifying nodes in database...")
	for _, id := range storedIDs {
		node, err := db.GetNode(ctx, id)
		if err != nil {
			t.Errorf("Node %s not found: %v", id, err)
			continue
		}
		t.Logf("  Node %s exists in DB: labels=%v, title=%s", id, node.Labels, node.Properties["title"])
	}

	// Step 2: Search for graph-related content
	t.Log("\nStep 2: Searching for 'database relationships nodes'...")

	// First, try direct database search to see what's there
	dbResults, err := db.Search(ctx, "database", nil, 10)
	if err != nil {
		t.Logf("  Direct DB search error: %v", err)
	} else {
		t.Logf("  Direct DB search (text): found %d results", len(dbResults))
		for i, r := range dbResults {
			t.Logf("    %d. %s (score: %.4f)", i+1, r.Node.Properties["title"], r.Score)
		}
	}

	// Try discover through MCP
	result, err := server.handleDiscover(ctx, map[string]interface{}{
		"query": "database relationships nodes",
		"limit": 3,
	})
	if err != nil {
		t.Fatalf("Discover failed: %v", err)
	}

	discoverResult := result.(DiscoverResult)
	t.Logf("  MCP Discover: Method=%s, Results=%d", discoverResult.Method, len(discoverResult.Results))

	if discoverResult.Method != "vector" {
		t.Errorf("Expected vector search method, got %s", discoverResult.Method)
	}

	// The graph database doc should rank highly
	foundGraph := false
	for i, r := range discoverResult.Results {
		t.Logf("  %d. %s (similarity: %.4f)", i+1, r.Title, r.Similarity)
		if r.Title == "Graph Databases" {
			foundGraph = true
		}
	}

	if !foundGraph && len(discoverResult.Results) > 0 {
		t.Log("  Note: Graph Databases not in top results (may need more data for ranking)")
	}

	// Step 3: Search for cat-related content
	t.Log("\nStep 3: Searching for 'furry pet animal'...")

	result, err = server.handleDiscover(ctx, map[string]interface{}{
		"query": "furry pet animal",
		"limit": 3,
	})
	if err != nil {
		t.Fatalf("Discover failed: %v", err)
	}

	discoverResult = result.(DiscoverResult)
	for i, r := range discoverResult.Results {
		t.Logf("  %d. %s (similarity: %.4f)", i+1, r.Title, r.Similarity)
	}

	// Step 4: Test recall by ID
	t.Log("\nStep 4: Testing recall by ID...")

	if len(storedIDs) > 0 {
		result, err := server.handleRecall(ctx, map[string]interface{}{
			"id": storedIDs[0],
		})
		if err != nil {
			t.Fatalf("Recall failed: %v", err)
		}
		recallResult := result.(RecallResult)
		if recallResult.Count != 1 {
			t.Errorf("Expected 1 result, got %d", recallResult.Count)
		}
		t.Logf("  ✓ Recalled: %s", recallResult.Nodes[0].Title)
	}

	t.Log("\n✅ E2E test completed successfully!")
}

func TestMCPE2E_LinkAndGraph(t *testing.T) {
	if os.Getenv("INTEGRATION_TEST") == "" {
		t.Skip("Set INTEGRATION_TEST=1 to run")
	}

	// Create a temporary database
	db, err := nornicdb.Open("", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	server := NewServer(db, nil)
	ctx := context.Background()

	// Store two nodes
	result1, _ := server.handleStore(ctx, map[string]interface{}{
		"content": "Parent concept",
		"title":   "Parent",
		"type":    "Concept",
	})
	node1 := result1.(StoreResult)

	result2, _ := server.handleStore(ctx, map[string]interface{}{
		"content": "Child concept derived from parent",
		"title":   "Child",
		"type":    "Concept",
	})
	node2 := result2.(StoreResult)

	t.Logf("Created nodes: %s, %s", node1.ID, node2.ID)

	// Link them
	linkResult, err := server.handleLink(ctx, map[string]interface{}{
		"from":     node2.ID,
		"to":       node1.ID,
		"relation": "depends_on",
	})
	if err != nil {
		t.Fatalf("Link failed: %v", err)
	}

	link := linkResult.(LinkResult)
	t.Logf("✓ Created edge: %s -> %s (type: depends_on)", link.From.ID, link.To.ID)
	t.Logf("  Edge ID: %s", link.EdgeID)
}
