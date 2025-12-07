package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEmbedThenFindNext tests that after embedding a node, the next call to
// FindNodeNeedingEmbedding finds a DIFFERENT node, not the same one.
// This is the suspected production bug - n1 keeps getting found repeatedly.
func TestEmbedThenFindNext(t *testing.T) {
	// Use in-memory BadgerDB to avoid disk I/O and Windows OOM issues
	badger, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer badger.Close()

	// Create AsyncEngine
	config := DefaultAsyncEngineConfig()
	config.FlushInterval = 1 * time.Hour // No auto-flush
	async := NewAsyncEngine(badger, config)
	defer async.Close()

	// Create 5 nodes
	for i := 0; i < 5; i++ {
		node := &Node{
			ID:     NodeID("node" + string(rune('a'+i))),
			Labels: []string{"File", "Node"},
			Properties: map[string]any{
				"path":    "/test/file" + string(rune('a'+i)) + ".md",
				"content": "Test content " + string(rune('a'+i)),
			},
		}
		err := async.CreateNode(node)
		require.NoError(t, err)
	}

	// Find first node
	first := async.FindNodeNeedingEmbedding()
	require.NotNil(t, first, "Should find first node")
	t.Logf("First node found: %s", first.ID)

	// Simulate embedding it - set embedding directly
	first.Embedding = []float32{0.1, 0.2, 0.3}
	first.Properties["has_embedding"] = true
	first.Properties["embedding"] = true
	err = async.UpdateNode(first)
	require.NoError(t, err)

	// Find next node - should be DIFFERENT from first
	second := async.FindNodeNeedingEmbedding()
	require.NotNil(t, second, "Should find second node")
	t.Logf("Second node found: %s", second.ID)

	assert.NotEqual(t, first.ID, second.ID, "Second node should be different from first!")

	// Keep going - embed second, find third
	second.Embedding = []float32{0.1, 0.2, 0.3}
	second.Properties["has_embedding"] = true
	err = async.UpdateNode(second)
	require.NoError(t, err)

	third := async.FindNodeNeedingEmbedding()
	require.NotNil(t, third, "Should find third node")
	t.Logf("Third node found: %s", third.ID)

	assert.NotEqual(t, first.ID, third.ID, "Third should not be first")
	assert.NotEqual(t, second.ID, third.ID, "Third should not be second")
}

// TestEmbedThenFindNextWithFlush tests the same scenario but WITH flush between ops
// This tests if the bug is related to flush timing
func TestEmbedThenFindNextWithFlush(t *testing.T) {
	// Use in-memory BadgerDB to avoid disk I/O and Windows OOM issues
	badger, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer badger.Close()

	// Create AsyncEngine
	config := DefaultAsyncEngineConfig()
	config.FlushInterval = 1 * time.Hour // No auto-flush
	async := NewAsyncEngine(badger, config)
	defer async.Close()

	// Create 5 nodes
	for i := 0; i < 5; i++ {
		node := &Node{
			ID:     NodeID("node" + string(rune('a'+i))),
			Labels: []string{"File", "Node"},
			Properties: map[string]any{
				"path":    "/test/file" + string(rune('a'+i)) + ".md",
				"content": "Test content " + string(rune('a'+i)),
			},
		}
		err := async.CreateNode(node)
		require.NoError(t, err)
	}

	// Flush nodes to BadgerDB
	err = async.Flush()
	require.NoError(t, err)

	// Now cache is empty, all nodes in BadgerDB
	async.mu.RLock()
	cacheSize := len(async.nodeCache)
	async.mu.RUnlock()
	t.Logf("Cache size after flush: %d", cacheSize)

	// Find first node (from BadgerDB)
	first := async.FindNodeNeedingEmbedding()
	require.NotNil(t, first, "Should find first node")
	t.Logf("First node found: %s", first.ID)

	// Simulate embedding it
	first.Embedding = []float32{0.1, 0.2, 0.3}
	first.Properties["has_embedding"] = true
	first.Properties["embedding"] = true
	err = async.UpdateNode(first)
	require.NoError(t, err)

	// Flush the embedding update to BadgerDB
	err = async.Flush()
	require.NoError(t, err)

	// Find next node - should be DIFFERENT
	second := async.FindNodeNeedingEmbedding()
	require.NotNil(t, second, "Should find second node")
	t.Logf("Second node found: %s", second.ID)

	// THIS IS THE KEY ASSERTION - in production this fails
	assert.NotEqual(t, first.ID, second.ID, "Second node should be different from first after flush!")
}

// TestProductionScenario replicates the exact production issue
func TestProductionScenario(t *testing.T) {
	// Use in-memory BadgerDB to avoid disk I/O and Windows OOM issues
	badger, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer badger.Close()

	// Create AsyncEngine like production
	config := DefaultAsyncEngineConfig()
	config.FlushInterval = 50 * time.Millisecond
	async := NewAsyncEngine(badger, config)
	defer async.Close()

	// Simulate Mimir indexing 10 files
	for i := 0; i < 10; i++ {
		node := &Node{
			ID:     NodeID("n" + string(rune('0'+i))),
			Labels: []string{"File", "Node"},
			Properties: map[string]any{
				"path":      "/app/docs/file" + string(rune('0'+i)) + ".md",
				"extension": ".md",
				"content":   "# Test Document\n\nThis is test content.",
			},
		}
		err := async.CreateNode(node)
		require.NoError(t, err)
	}

	// Wait for auto-flush
	time.Sleep(200 * time.Millisecond)
	err = async.Flush()
	require.NoError(t, err)

	// Count nodes in BadgerDB
	var badgerCount int
	badger.IterateNodes(func(n *Node) bool {
		badgerCount++
		return true
	})
	t.Logf("BadgerDB node count: %d", badgerCount)
	assert.Equal(t, 10, badgerCount, "BadgerDB should have all 10 nodes")

	// Now simulate what embed worker does
	embedded := make(map[NodeID]bool)
	for i := 0; i < 10; i++ {
		node := async.FindNodeNeedingEmbedding()
		if node == nil {
			t.Logf("No more nodes to embed after %d", i)
			break
		}

		if embedded[node.ID] {
			t.Fatalf("BUG: Node %s was returned TWICE! Already embedded.", node.ID)
		}

		t.Logf("Embedding node %d: %s", i, node.ID)
		embedded[node.ID] = true

		// Embed it
		node.Embedding = []float32{0.1, 0.2, 0.3}
		node.Properties["has_embedding"] = true
		node.Properties["embedding"] = true
		err = async.UpdateNode(node)
		require.NoError(t, err)

		// Flush after each embed (like production does periodically)
		err = async.Flush()
		require.NoError(t, err)
	}

	assert.Equal(t, 10, len(embedded), "Should have embedded all 10 nodes")
}
