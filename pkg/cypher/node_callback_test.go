package cypher

import (
	"context"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNodeMutatedCallbackOnCreate verifies the callback is invoked when nodes are created via CREATE
func TestNodeMutatedCallbackOnCreate(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	var mu sync.Mutex
	createdNodeIDs := []string{}

	// Set up callback to track created nodes
	exec.SetNodeMutatedCallback(func(nodeID string) {
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

// TestNodeMutatedCallbackOnCreateMultiple verifies callback is invoked for each node in multi-node CREATE
func TestNodeMutatedCallbackOnCreateMultiple(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	var mu sync.Mutex
	createdNodeIDs := []string{}

	exec.SetNodeMutatedCallback(func(nodeID string) {
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

// TestNodeMutatedCallbackOnCreateWithRelationship verifies callback for nodes created with relationships
func TestNodeMutatedCallbackOnCreateWithRelationship(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	var mu sync.Mutex
	createdNodeIDs := []string{}

	exec.SetNodeMutatedCallback(func(nodeID string) {
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

// TestNodeMutatedCallbackOnMergeCreate verifies callback is invoked when MERGE creates a new node
func TestNodeMutatedCallbackOnMergeCreate(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	var mu sync.Mutex
	createdNodeIDs := []string{}

	exec.SetNodeMutatedCallback(func(nodeID string) {
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

// TestNodeMutatedCallbackOnMergeMatch distinguishes no-op matches from actual mutations.
func TestNodeMutatedCallbackOnMergeMatch(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	var mu sync.Mutex
	createdNodeIDs := []string{}

	exec.SetNodeMutatedCallback(func(nodeID string) {
		mu.Lock()
		defer mu.Unlock()
		createdNodeIDs = append(createdNodeIDs, nodeID)
	})

	snapshot := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), createdNodeIDs...)
	}

	_, err := exec.Execute(ctx, `MERGE (n:Person {name: 'Alice'})`, nil)
	require.NoError(t, err)
	initial := snapshot()
	require.Len(t, initial, 1)

	for _, query := range []string{
		`MERGE (n:Person {name: 'Alice'})`,
		`MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.unexpected = true`,
		`MATCH (n:Person {name: 'Alice'}) WITH n.name AS name MERGE (m:Person {name:name})`,
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Zero(t, result.Stats.NodesCreated)
		require.Zero(t, result.Stats.PropertiesSet)
		require.Equal(t, initial, snapshot(), "no-op MERGE must not notify: %s", query)
	}

	for index, query := range []string{
		`MERGE (n:Person {name: 'Alice'}) ON MATCH SET n.seen = true`,
		`MERGE (n:Person {name: 'Alice'}) SET n.age = 42`,
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Zero(t, result.Stats.NodesCreated)
		require.EqualValues(t, 1, result.Stats.PropertiesSet)
		actual := snapshot()
		require.Len(t, actual, index+2)
		require.Equal(t, initial[0], actual[len(actual)-1])
	}
	stored, err := store.GetNode(storage.NodeID(initial[0]))
	require.NoError(t, err)
	require.Equal(t, true, stored.Properties["seen"])
	require.EqualValues(t, 42, stored.Properties["age"])
	require.NotContains(t, stored.Properties, "unexpected")
}

// TestNodeMutatedCallbackNotSet verifies no panic when callback is nil
func TestNodeMutatedCallbackNotSet(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Don't set callback - should not panic
	_, err := exec.Execute(ctx, `CREATE (n:Person {name: 'Alice'})`, nil)
	require.NoError(t, err, "CREATE should succeed even without callback set")

	_, err = exec.Execute(ctx, `MERGE (m:Person {name: 'Bob'})`, nil)
	require.NoError(t, err, "MERGE should succeed even without callback set")
}

// TestNodeMutatedCallbackNodeIDsAreValid verifies the callback receives valid node IDs
func TestNodeMutatedCallbackNodeIDsAreValid(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	var mu sync.Mutex
	createdNodeIDs := []string{}

	exec.SetNodeMutatedCallback(func(nodeID string) {
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

// TestNodeMutatedCallbackConcurrentCreates verifies callback is thread-safe
func TestNodeMutatedCallbackConcurrentCreates(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	var mu sync.Mutex
	callbackCount := 0

	exec.SetNodeMutatedCallback(func(nodeID string) {
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

// TestNodeMutatedCallbackOnMatchCreate verifies callback for MATCH...CREATE pattern
func TestNodeMutatedCallbackOnMatchCreate(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create an initial node
	_, err := exec.Execute(ctx, `CREATE (p:Person {name: 'Alice'})`, nil)
	require.NoError(t, err)

	var mu sync.Mutex
	createdNodeIDs := []string{}

	// Set callback after creating initial node
	exec.SetNodeMutatedCallback(func(nodeID string) {
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

// TestSetNodeMutatedCallbackReplacesExisting verifies callback can be replaced
func TestSetNodeMutatedCallbackReplacesExisting(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	callback1Count := 0
	callback2Count := 0

	// Set first callback
	exec.SetNodeMutatedCallback(func(nodeID string) {
		callback1Count++
	})

	_, _ = exec.Execute(ctx, `CREATE (n:Test1)`, nil)
	assert.Equal(t, 1, callback1Count)
	assert.Equal(t, 0, callback2Count)

	// Replace with second callback
	exec.SetNodeMutatedCallback(func(nodeID string) {
		callback2Count++
	})

	_, _ = exec.Execute(ctx, `CREATE (n:Test2)`, nil)
	assert.Equal(t, 1, callback1Count, "Old callback should not be called")
	assert.Equal(t, 1, callback2Count, "New callback should be called")
}
