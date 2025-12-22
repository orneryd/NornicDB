package cypher

import (
	"context"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNodeCreatedCallbackOnCreate verifies the callback is invoked when nodes are created via CREATE
func TestNodeCreatedCallbackOnCreate(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	var mu sync.Mutex
	createdNodeIDs := []string{}

	// Set up callback to track created nodes
	exec.SetNodeCreatedCallback(func(nodeID string) {
		mu.Lock()
		defer mu.Unlock()
		createdNodeIDs = append(createdNodeIDs, nodeID)
	})

	// Create a single node
	_, err := exec.Execute(ctx, `CREATE (n:Person {name: 'Alice'})`, nil)
	require.NoError(t, err)

	mu.Lock()
	assert.Len(t, createdNodeIDs, 1, "Expected 1 callback for single CREATE")
	mu.Unlock()
}

// TestNodeCreatedCallbackOnCreateMultiple verifies callback is invoked for each node in multi-node CREATE
func TestNodeCreatedCallbackOnCreateMultiple(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	var mu sync.Mutex
	createdNodeIDs := []string{}

	exec.SetNodeCreatedCallback(func(nodeID string) {
		mu.Lock()
		defer mu.Unlock()
		createdNodeIDs = append(createdNodeIDs, nodeID)
	})

	// Create multiple nodes in one statement
	_, err := exec.Execute(ctx, `CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})`, nil)
	require.NoError(t, err)

	mu.Lock()
	assert.Len(t, createdNodeIDs, 2, "Expected 2 callbacks for two-node CREATE")
	mu.Unlock()
}

// TestNodeCreatedCallbackOnCreateWithRelationship verifies callback for nodes created with relationships
func TestNodeCreatedCallbackOnCreateWithRelationship(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	var mu sync.Mutex
	createdNodeIDs := []string{}

	exec.SetNodeCreatedCallback(func(nodeID string) {
		mu.Lock()
		defer mu.Unlock()
		createdNodeIDs = append(createdNodeIDs, nodeID)
	})

	// Create nodes and relationship in one statement
	_, err := exec.Execute(ctx, `CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})`, nil)
	require.NoError(t, err)

	mu.Lock()
	assert.Len(t, createdNodeIDs, 2, "Expected 2 callbacks for CREATE with relationship")
	mu.Unlock()
}

// TestNodeCreatedCallbackOnMergeCreate verifies callback is invoked when MERGE creates a new node
func TestNodeCreatedCallbackOnMergeCreate(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	var mu sync.Mutex
	createdNodeIDs := []string{}

	exec.SetNodeCreatedCallback(func(nodeID string) {
		mu.Lock()
		defer mu.Unlock()
		createdNodeIDs = append(createdNodeIDs, nodeID)
	})

	// MERGE on non-existent node should create it
	_, err := exec.Execute(ctx, `MERGE (n:Person {name: 'Alice'})`, nil)
	require.NoError(t, err)

	mu.Lock()
	assert.Len(t, createdNodeIDs, 1, "Expected 1 callback for MERGE creating new node")
	mu.Unlock()
}

// TestNodeCreatedCallbackOnMergeMatch verifies callback is NOT invoked when MERGE matches existing node
func TestNodeCreatedCallbackOnMergeMatch(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	var mu sync.Mutex
	createdNodeIDs := []string{}

	exec.SetNodeCreatedCallback(func(nodeID string) {
		mu.Lock()
		defer mu.Unlock()
		createdNodeIDs = append(createdNodeIDs, nodeID)
	})

	// First MERGE creates the node
	_, err := exec.Execute(ctx, `MERGE (n:Person {name: 'Alice'})`, nil)
	require.NoError(t, err)

	mu.Lock()
	initialCount := len(createdNodeIDs)
	mu.Unlock()

	// Second MERGE should match existing - no new callback
	_, err = exec.Execute(ctx, `MERGE (n:Person {name: 'Alice'})`, nil)
	require.NoError(t, err)

	mu.Lock()
	assert.Equal(t, initialCount, len(createdNodeIDs), "MERGE matching existing node should not trigger callback")
	mu.Unlock()
}

// TestNodeCreatedCallbackNotSet verifies no panic when callback is nil
func TestNodeCreatedCallbackNotSet(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Don't set callback - should not panic
	_, err := exec.Execute(ctx, `CREATE (n:Person {name: 'Alice'})`, nil)
	require.NoError(t, err, "CREATE should succeed even without callback set")

	_, err = exec.Execute(ctx, `MERGE (m:Person {name: 'Bob'})`, nil)
	require.NoError(t, err, "MERGE should succeed even without callback set")
}

// TestNodeCreatedCallbackNodeIDsAreValid verifies the callback receives valid node IDs
func TestNodeCreatedCallbackNodeIDsAreValid(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	var mu sync.Mutex
	createdNodeIDs := []string{}

	exec.SetNodeCreatedCallback(func(nodeID string) {
		mu.Lock()
		defer mu.Unlock()
		createdNodeIDs = append(createdNodeIDs, nodeID)
	})

	// Create nodes
	_, err := exec.Execute(ctx, `CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})`, nil)
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()

	// Verify each ID corresponds to a real node
	for _, nodeID := range createdNodeIDs {
		node, err := store.GetNode(storage.NodeID(nodeID))
		require.NoError(t, err, "Node ID from callback should exist in storage")
		require.NotNil(t, node, "Node should not be nil")
	}
}

// TestNodeCreatedCallbackConcurrentCreates verifies callback is thread-safe
func TestNodeCreatedCallbackConcurrentCreates(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	var mu sync.Mutex
	callbackCount := 0

	exec.SetNodeCreatedCallback(func(nodeID string) {
		mu.Lock()
		defer mu.Unlock()
		callbackCount++
	})

	// Run concurrent CREATE operations
	var wg sync.WaitGroup
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, _ = exec.Execute(ctx, `CREATE (n:Test {idx: $idx})`, map[string]interface{}{"idx": idx})
		}(i)
	}

	wg.Wait()

	mu.Lock()
	assert.Equal(t, numGoroutines, callbackCount, "Should have received callback for each concurrent CREATE")
	mu.Unlock()
}

// TestNodeCreatedCallbackOnMatchCreate verifies callback for MATCH...CREATE pattern
func TestNodeCreatedCallbackOnMatchCreate(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create an initial node
	_, err := exec.Execute(ctx, `CREATE (p:Person {name: 'Alice'})`, nil)
	require.NoError(t, err)

	var mu sync.Mutex
	createdNodeIDs := []string{}

	// Set callback after creating initial node
	exec.SetNodeCreatedCallback(func(nodeID string) {
		mu.Lock()
		defer mu.Unlock()
		createdNodeIDs = append(createdNodeIDs, nodeID)
	})

	// MATCH existing node and CREATE new inline node with relationship to it
	// This tests the inline node definition in CREATE relationship pattern
	_, err = exec.Execute(ctx, `
		MATCH (p:Person {name: 'Alice'})
		CREATE (c:Company {name: 'Acme'})-[:EMPLOYS]->(p)
	`, nil)
	require.NoError(t, err)

	mu.Lock()
	assert.Equal(t, 1, len(createdNodeIDs), "Should have callback for new inline node in MATCH...CREATE")
	mu.Unlock()
}

// TestSetNodeCreatedCallbackReplacesExisting verifies callback can be replaced
func TestSetNodeCreatedCallbackReplacesExisting(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	callback1Count := 0
	callback2Count := 0

	// Set first callback
	exec.SetNodeCreatedCallback(func(nodeID string) {
		callback1Count++
	})

	_, _ = exec.Execute(ctx, `CREATE (n:Test1)`, nil)
	assert.Equal(t, 1, callback1Count)
	assert.Equal(t, 0, callback2Count)

	// Replace with second callback
	exec.SetNodeCreatedCallback(func(nodeID string) {
		callback2Count++
	})

	_, _ = exec.Execute(ctx, `CREATE (n:Test2)`, nil)
	assert.Equal(t, 1, callback1Count, "Old callback should not be called")
	assert.Equal(t, 1, callback2Count, "New callback should be called")
}
