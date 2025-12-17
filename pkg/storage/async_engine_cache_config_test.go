package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAsyncEngineConfig_Defaults tests that DefaultAsyncEngineConfig returns expected values.
func TestAsyncEngineConfig_Defaults(t *testing.T) {
	config := DefaultAsyncEngineConfig()

	assert.Equal(t, 50*time.Millisecond, config.FlushInterval, "default FlushInterval should be 50ms")
	assert.Equal(t, 50000, config.MaxNodeCacheSize, "default MaxNodeCacheSize should be 50000")
	assert.Equal(t, 100000, config.MaxEdgeCacheSize, "default MaxEdgeCacheSize should be 100000")
}

// TestAsyncEngineConfig_PassedToEngine tests that config values are correctly passed to the engine.
func TestAsyncEngineConfig_PassedToEngine(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	config := &AsyncEngineConfig{
		FlushInterval:    123 * time.Millisecond,
		MaxNodeCacheSize: 5000,
		MaxEdgeCacheSize: 10000,
	}

	async := NewAsyncEngine(engine, config)
	defer async.Close()

	// Verify the values are stored correctly
	assert.Equal(t, 123*time.Millisecond, async.flushInterval, "flushInterval should match config")
	assert.Equal(t, 5000, async.maxNodeCacheSize, "maxNodeCacheSize should match config")
	assert.Equal(t, 10000, async.maxEdgeCacheSize, "maxEdgeCacheSize should match config")
}

// TestAsyncEngineConfig_NilUsesDefaults tests that nil config uses defaults.
func TestAsyncEngineConfig_NilUsesDefaults(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	async := NewAsyncEngine(engine, nil)
	defer async.Close()

	defaults := DefaultAsyncEngineConfig()
	assert.Equal(t, defaults.FlushInterval, async.flushInterval, "flushInterval should use default")
	assert.Equal(t, defaults.MaxNodeCacheSize, async.maxNodeCacheSize, "maxNodeCacheSize should use default")
	assert.Equal(t, defaults.MaxEdgeCacheSize, async.maxEdgeCacheSize, "maxEdgeCacheSize should use default")
}

// TestAsyncEngine_NodeCacheLimit_TriggersFlush tests that exceeding MaxNodeCacheSize triggers a flush.
func TestAsyncEngine_NodeCacheLimit_TriggersFlush(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	// Small cache size to trigger flush quickly
	config := &AsyncEngineConfig{
		FlushInterval:    1 * time.Hour, // Don't auto-flush
		MaxNodeCacheSize: 10,            // Flush after 10 nodes
		MaxEdgeCacheSize: 100,
	}

	async := NewAsyncEngine(engine, config)
	defer async.Close()

	// Create 15 nodes - should trigger at least one flush
	for i := 0; i < 15; i++ {
		err := async.CreateNode(&Node{
			ID:     NodeID(fmt.Sprintf("node-%d", i)),
			Labels: []string{"Test"},
		})
		require.NoError(t, err)
	}

	// Give a small time for any concurrent operations
	time.Sleep(10 * time.Millisecond)

	// The underlying engine should have received some nodes from the flush
	underlyingCount, err := engine.NodeCount()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, underlyingCount, int64(10), "underlying engine should have at least 10 nodes from flush")

	// Total count should still be 15
	totalCount, err := async.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(15), totalCount, "total count should be 15")
}

// TestAsyncEngine_EdgeCacheLimit_TriggersFlush tests that exceeding MaxEdgeCacheSize triggers a flush.
func TestAsyncEngine_EdgeCacheLimit_TriggersFlush(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	// Create source and target nodes first
	for i := 0; i < 20; i++ {
		err := engine.CreateNode(&Node{
			ID:     NodeID(fmt.Sprintf("node-%d", i)),
			Labels: []string{"Test"},
		})
		require.NoError(t, err)
	}

	// Small cache size to trigger flush quickly
	config := &AsyncEngineConfig{
		FlushInterval:    1 * time.Hour, // Don't auto-flush
		MaxNodeCacheSize: 100,
		MaxEdgeCacheSize: 10, // Flush after 10 edges
	}

	async := NewAsyncEngine(engine, config)
	defer async.Close()

	// Create 15 edges - should trigger at least one flush
	for i := 0; i < 15; i++ {
		err := async.CreateEdge(&Edge{
			ID:        EdgeID(fmt.Sprintf("edge-%d", i)),
			StartNode: NodeID(fmt.Sprintf("node-%d", i)),
			EndNode:   NodeID(fmt.Sprintf("node-%d", i+1)),
			Type:      "KNOWS",
		})
		require.NoError(t, err)
	}

	// Give a small time for any concurrent operations
	time.Sleep(10 * time.Millisecond)

	// The underlying engine should have received some edges from the flush
	underlyingCount, err := engine.EdgeCount()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, underlyingCount, int64(10), "underlying engine should have at least 10 edges from flush")

	// Total count should still be 15
	totalCount, err := async.EdgeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(15), totalCount, "total count should be 15")
}

// TestAsyncEngine_ZeroCacheSize_NoAutoFlush tests that MaxNodeCacheSize=0 disables auto-flush on cache size.
func TestAsyncEngine_ZeroCacheSize_NoAutoFlush(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	// Zero cache size = unlimited
	config := &AsyncEngineConfig{
		FlushInterval:    1 * time.Hour, // Don't auto-flush
		MaxNodeCacheSize: 0,             // Unlimited
		MaxEdgeCacheSize: 0,             // Unlimited
	}

	async := NewAsyncEngine(engine, config)
	defer async.Close()

	// Create 100 nodes - should NOT trigger any flush
	for i := 0; i < 100; i++ {
		err := async.CreateNode(&Node{
			ID:     NodeID(fmt.Sprintf("node-%d", i)),
			Labels: []string{"Test"},
		})
		require.NoError(t, err)
	}

	// Underlying engine should have 0 nodes (no flush triggered)
	underlyingCount, err := engine.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(0), underlyingCount, "underlying engine should have 0 nodes when cache limit disabled")

	// But total count via async should be 100
	totalCount, err := async.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(100), totalCount, "async count should be 100")
}

// TestAsyncEngine_CacheLimit_ConcurrentWrites tests that cache limit works correctly under concurrent writes.
func TestAsyncEngine_CacheLimit_ConcurrentWrites(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	config := &AsyncEngineConfig{
		FlushInterval:    1 * time.Hour, // Don't auto-flush
		MaxNodeCacheSize: 50,            // Small limit
		MaxEdgeCacheSize: 100,
	}

	async := NewAsyncEngine(engine, config)
	defer async.Close()

	// Concurrent writes from multiple goroutines
	var wg sync.WaitGroup
	numGoroutines := 10
	nodesPerGoroutine := 20
	totalNodes := numGoroutines * nodesPerGoroutine

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < nodesPerGoroutine; i++ {
				nodeID := fmt.Sprintf("node-%d-%d", goroutineID, i)
				err := async.CreateNode(&Node{
					ID:     NodeID(nodeID),
					Labels: []string{"Test"},
				})
				if err != nil {
					t.Errorf("CreateNode failed: %v", err)
				}
			}
		}(g)
	}

	wg.Wait()

	// Final flush to ensure all nodes are persisted
	err := async.Flush()
	require.NoError(t, err)

	// Total count should match expected
	totalCount, err := async.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(totalNodes), totalCount, "total count should match expected")

	// Underlying engine should have all nodes after flush
	underlyingCount, err := engine.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(totalNodes), underlyingCount, "underlying engine should have all nodes")
}

// TestAsyncEngine_FlushInterval_Configurable tests that FlushInterval is correctly used.
func TestAsyncEngine_FlushInterval_Configurable(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	// Very short flush interval
	config := &AsyncEngineConfig{
		FlushInterval:    20 * time.Millisecond,
		MaxNodeCacheSize: 1000000, // Large - won't trigger size-based flush
		MaxEdgeCacheSize: 1000000,
	}

	async := NewAsyncEngine(engine, config)
	defer async.Close()

	// Create a node
	err := async.CreateNode(&Node{
		ID:     NodeID("test-node"),
		Labels: []string{"Test"},
	})
	require.NoError(t, err)

	// Underlying engine should have 0 initially
	underlyingCount, err := engine.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(0), underlyingCount, "underlying should be 0 before flush interval")

	// Wait for auto-flush (20ms + buffer)
	time.Sleep(50 * time.Millisecond)

	// Now underlying engine should have the node
	underlyingCount, err = engine.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(1), underlyingCount, "underlying should have 1 after flush interval")
}

// TestAsyncEngine_CacheLimit_ExactBoundary tests behavior at exact cache limit boundary.
func TestAsyncEngine_CacheLimit_ExactBoundary(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	cacheLimit := 10
	config := &AsyncEngineConfig{
		FlushInterval:    1 * time.Hour, // Don't auto-flush
		MaxNodeCacheSize: cacheLimit,
		MaxEdgeCacheSize: 100,
	}

	async := NewAsyncEngine(engine, config)
	defer async.Close()

	// Create exactly cacheLimit nodes - should NOT trigger flush yet
	for i := 0; i < cacheLimit; i++ {
		err := async.CreateNode(&Node{
			ID:     NodeID(fmt.Sprintf("node-%d", i)),
			Labels: []string{"Test"},
		})
		require.NoError(t, err)
	}

	// Check cache size is at limit
	async.mu.RLock()
	cacheSize := len(async.nodeCache)
	async.mu.RUnlock()
	assert.Equal(t, cacheLimit, cacheSize, "cache should be at exact limit")

	// One more node should trigger flush
	err := async.CreateNode(&Node{
		ID:     NodeID("node-trigger"),
		Labels: []string{"Test"},
	})
	require.NoError(t, err)

	// After flush, underlying should have at least cacheLimit nodes
	time.Sleep(10 * time.Millisecond) // Small buffer for flush
	underlyingCount, err := engine.NodeCount()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, underlyingCount, int64(cacheLimit), "underlying should have nodes after flush triggered")
}

// TestAsyncEngine_MultipleFlushes_CacheLimit tests multiple flush cycles triggered by cache limit.
func TestAsyncEngine_MultipleFlushes_CacheLimit(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	cacheLimit := 5
	config := &AsyncEngineConfig{
		FlushInterval:    1 * time.Hour, // Don't auto-flush
		MaxNodeCacheSize: cacheLimit,
		MaxEdgeCacheSize: 100,
	}

	async := NewAsyncEngine(engine, config)
	defer async.Close()

	// Track flush count
	var flushCount atomic.Int64

	// Create 25 nodes - should trigger ~5 flushes
	totalNodes := 25
	for i := 0; i < totalNodes; i++ {
		// Check if we need to measure flush
		async.mu.RLock()
		beforeSize := len(async.nodeCache)
		async.mu.RUnlock()

		err := async.CreateNode(&Node{
			ID:     NodeID(fmt.Sprintf("node-%d", i)),
			Labels: []string{"Test"},
		})
		require.NoError(t, err)

		// If cache was at limit before create, a flush was triggered
		if beforeSize >= cacheLimit {
			flushCount.Add(1)
		}
	}

	// Final flush
	err := async.Flush()
	require.NoError(t, err)

	// All nodes should be in underlying engine
	underlyingCount, err := engine.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(totalNodes), underlyingCount, "all nodes should be persisted")

	// We should have triggered multiple flushes
	assert.GreaterOrEqual(t, flushCount.Load(), int64(4), "should have triggered multiple cache-limit flushes")
}
