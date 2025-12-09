package storage

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewAsyncEngine(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	async := NewAsyncEngine(engine, nil)
	require.NotNil(t, async)
	defer async.Close()

	count, err := async.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestAsyncEngine_CreateNode(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	async := NewAsyncEngine(engine, &AsyncEngineConfig{
		FlushInterval: 100 * time.Millisecond,
	})
	defer async.Close()

	// Create a node
	node := &Node{
		ID:         "node-1",
		Labels:     []string{"Person"},
		Properties: map[string]any{"name": "Alice"},
	}
	err := async.CreateNode(node)
	require.NoError(t, err)

	// Should be readable immediately from cache
	stored, err := async.GetNode("node-1")
	require.NoError(t, err)
	assert.Equal(t, "Alice", stored.Properties["name"])

	// Count should reflect pending create
	count, err := async.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func TestAsyncEngine_Flush(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	async := NewAsyncEngine(engine, &AsyncEngineConfig{
		FlushInterval: 1 * time.Hour, // Don't auto-flush
	})
	defer async.Close()

	// Create nodes
	for i := 0; i < 10; i++ {
		err := async.CreateNode(&Node{
			ID:     NodeID(fmt.Sprintf("node-%d", i)),
			Labels: []string{"Test"},
		})
		require.NoError(t, err)
	}

	// Count before flush
	count, err := async.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(10), count)

	// Manually flush
	err = async.Flush()
	require.NoError(t, err)

	// Count after flush - should still be 10
	count, err = async.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(10), count)

	// Underlying engine should have nodes now
	underlyingCount, err := engine.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(10), underlyingCount)
}

// TestAsyncEngine_NodeCount_RaceCondition is a regression test for a bug where
// NodeCount() could return inconsistent values due to a race condition with Flush().
//
// The bug: NodeCount() would read the cache size and underlying engine count
// without holding a lock, allowing Flush() to clear the cache and write to
// the engine between the two reads, causing nodes to be "missed" in the count.
//
// BUG: Node count fluctuated between 2000 and 2001 even with no adds/deletes
// FIX: Hold RLock during entire NodeCount operation to prevent race with Flush
func TestAsyncEngine_NodeCount_RaceCondition(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	async := NewAsyncEngine(engine, &AsyncEngineConfig{
		FlushInterval: 1 * time.Millisecond, // Very fast flushes to trigger race
	})
	defer async.Close()

	const numNodes = 100

	// Create nodes
	for i := 0; i < numNodes; i++ {
		err := async.CreateNode(&Node{
			ID:     NodeID(fmt.Sprintf("node-%d", i)),
			Labels: []string{"Test"},
		})
		require.NoError(t, err)
	}

	// Wait for initial flush
	time.Sleep(10 * time.Millisecond)

	var (
		wg            sync.WaitGroup
		inconsistency atomic.Int32
		minSeen       atomic.Int64
		maxSeen       atomic.Int64
	)

	minSeen.Store(numNodes)
	maxSeen.Store(numNodes)

	// Hammer NodeCount() from multiple goroutines while flushes occur
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				count, err := async.NodeCount()
				if err != nil {
					continue
				}
				if count != numNodes {
					inconsistency.Add(1)
				}
				// Track min/max seen
				for {
					old := minSeen.Load()
					if count >= old || minSeen.CompareAndSwap(old, count) {
						break
					}
				}
				for {
					old := maxSeen.Load()
					if count <= old || maxSeen.CompareAndSwap(old, count) {
						break
					}
				}
				// Small sleep to allow flushes
				time.Sleep(100 * time.Microsecond)
			}
		}()
	}

	// Also trigger manual flushes concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			async.Flush()
			time.Sleep(200 * time.Microsecond)
		}
	}()

	wg.Wait()

	// Verify no inconsistencies detected
	assert.Equal(t, int32(0), inconsistency.Load(),
		"NodeCount() returned inconsistent values during concurrent flush. Min=%d, Max=%d (expected %d)",
		minSeen.Load(), maxSeen.Load(), numNodes)

	// Final verification
	finalCount, err := async.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(numNodes), finalCount,
		"Final node count should be exactly %d", numNodes)
}

// TestAsyncEngine_EdgeCount_RaceCondition tests the same race condition for edges.
func TestAsyncEngine_EdgeCount_RaceCondition(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	async := NewAsyncEngine(engine, &AsyncEngineConfig{
		FlushInterval: 1 * time.Millisecond,
	})
	defer async.Close()

	const numNodes = 20
	const numEdges = 50

	// Create nodes first
	for i := 0; i < numNodes; i++ {
		err := async.CreateNode(&Node{
			ID:     NodeID(fmt.Sprintf("node-%d", i)),
			Labels: []string{"Test"},
		})
		require.NoError(t, err)
	}

	// Create edges
	for i := 0; i < numEdges; i++ {
		err := async.CreateEdge(&Edge{
			ID:        EdgeID(fmt.Sprintf("edge-%d", i)),
			Type:      "CONNECTS",
			StartNode: NodeID(fmt.Sprintf("node-%d", i%numNodes)),
			EndNode:   NodeID(fmt.Sprintf("node-%d", (i+1)%numNodes)),
		})
		require.NoError(t, err)
	}

	// Wait for initial flush
	time.Sleep(10 * time.Millisecond)

	var (
		wg            sync.WaitGroup
		inconsistency atomic.Int32
	)

	// Hammer EdgeCount() from multiple goroutines
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				count, err := async.EdgeCount()
				if err != nil {
					continue
				}
				if count != numEdges {
					inconsistency.Add(1)
				}
				time.Sleep(100 * time.Microsecond)
			}
		}()
	}

	// Trigger manual flushes
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			async.Flush()
			time.Sleep(200 * time.Microsecond)
		}
	}()

	wg.Wait()

	assert.Equal(t, int32(0), inconsistency.Load(),
		"EdgeCount() returned inconsistent values during concurrent flush")

	finalCount, err := async.EdgeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(numEdges), finalCount)
}

// TestAsyncEngine_DeleteNode tests that deletes are properly counted.
func TestAsyncEngine_DeleteNode(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	async := NewAsyncEngine(engine, &AsyncEngineConfig{
		FlushInterval: 1 * time.Hour, // Don't auto-flush
	})
	defer async.Close()

	// Create and flush nodes
	for i := 0; i < 10; i++ {
		err := async.CreateNode(&Node{
			ID:     NodeID(fmt.Sprintf("node-%d", i)),
			Labels: []string{"Test"},
		})
		require.NoError(t, err)
	}
	require.NoError(t, async.Flush())

	// Delete some nodes
	for i := 0; i < 3; i++ {
		err := async.DeleteNode(NodeID(fmt.Sprintf("node-%d", i)))
		require.NoError(t, err)
	}

	// Count should reflect pending deletes
	count, err := async.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(7), count, "Should have 10 - 3 = 7 nodes")

	// Flush and verify
	require.NoError(t, async.Flush())
	count, err = async.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(7), count)
}

// ============================================================================
// StreamingEngine Tests
// ============================================================================

func TestAsyncEngine_StreamNodes(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	async := NewAsyncEngine(engine, &AsyncEngineConfig{
		FlushInterval: 1 * time.Hour, // Don't auto-flush
	})
	defer async.Close()

	ctx := context.Background()

	// Create 100 nodes
	for i := 0; i < 100; i++ {
		err := async.CreateNode(&Node{
			ID:     NodeID(fmt.Sprintf("node-%d", i)),
			Labels: []string{"Test"},
		})
		require.NoError(t, err)
	}

	t.Run("StreamAllNodes", func(t *testing.T) {
		var count int
		err := async.StreamNodes(ctx, func(node *Node) error {
			count++
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 100, count, "Should stream all 100 nodes")
	})

	t.Run("StreamWithEarlyTermination", func(t *testing.T) {
		var count int
		err := async.StreamNodes(ctx, func(node *Node) error {
			count++
			if count >= 10 {
				return ErrIterationStopped
			}
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 10, count, "Should stop after 10 nodes")
	})

	t.Run("StreamAfterFlush", func(t *testing.T) {
		// Flush to underlying engine
		require.NoError(t, async.Flush())

		var count int
		err := async.StreamNodes(ctx, func(node *Node) error {
			count++
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 100, count, "Should stream all 100 nodes after flush")
	})

	t.Run("StreamWithCacheAndEngine", func(t *testing.T) {
		// Add more nodes to cache (not flushed yet)
		for i := 100; i < 150; i++ {
			err := async.CreateNode(&Node{
				ID:     NodeID(fmt.Sprintf("node-%d", i)),
				Labels: []string{"Test"},
			})
			require.NoError(t, err)
		}

		var count int
		err := async.StreamNodes(ctx, func(node *Node) error {
			count++
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 150, count, "Should stream 100 flushed + 50 cached = 150 nodes")
	})

	t.Run("StreamExcludesDeletedNodes", func(t *testing.T) {
		// Delete some nodes
		for i := 0; i < 10; i++ {
			err := async.DeleteNode(NodeID(fmt.Sprintf("node-%d", i)))
			require.NoError(t, err)
		}

		var count int
		err := async.StreamNodes(ctx, func(node *Node) error {
			count++
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 140, count, "Should stream 150 - 10 deleted = 140 nodes")
	})
}

func TestAsyncEngine_StreamEdges(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	async := NewAsyncEngine(engine, &AsyncEngineConfig{
		FlushInterval: 1 * time.Hour,
	})
	defer async.Close()

	ctx := context.Background()

	// Create nodes first
	for i := 0; i < 10; i++ {
		err := async.CreateNode(&Node{
			ID:     NodeID(fmt.Sprintf("node-%d", i)),
			Labels: []string{"Test"},
		})
		require.NoError(t, err)
	}

	// Flush nodes
	require.NoError(t, async.Flush())

	// Create edges
	for i := 0; i < 50; i++ {
		err := async.CreateEdge(&Edge{
			ID:        EdgeID(fmt.Sprintf("edge-%d", i)),
			Type:      "CONNECTS",
			StartNode: NodeID(fmt.Sprintf("node-%d", i%10)),
			EndNode:   NodeID(fmt.Sprintf("node-%d", (i+1)%10)),
		})
		require.NoError(t, err)
	}

	t.Run("StreamAllEdges", func(t *testing.T) {
		var count int
		err := async.StreamEdges(ctx, func(edge *Edge) error {
			count++
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 50, count, "Should stream all 50 edges")
	})

	t.Run("StreamWithEarlyTermination", func(t *testing.T) {
		var count int
		err := async.StreamEdges(ctx, func(edge *Edge) error {
			count++
			if count >= 5 {
				return ErrIterationStopped
			}
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 5, count, "Should stop after 5 edges")
	})
}

func TestAsyncEngine_StreamNodeChunks(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	async := NewAsyncEngine(engine, &AsyncEngineConfig{
		FlushInterval: 1 * time.Hour,
	})
	defer async.Close()

	ctx := context.Background()

	// Create 100 nodes
	for i := 0; i < 100; i++ {
		err := async.CreateNode(&Node{
			ID:     NodeID(fmt.Sprintf("node-%d", i)),
			Labels: []string{"Test"},
		})
		require.NoError(t, err)
	}

	t.Run("StreamInChunks", func(t *testing.T) {
		var totalNodes int
		var chunkCount int
		err := async.StreamNodeChunks(ctx, 25, func(nodes []*Node) error {
			chunkCount++
			totalNodes += len(nodes)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 100, totalNodes, "Should stream all 100 nodes")
		assert.Equal(t, 4, chunkCount, "Should have 4 chunks of 25")
	})
}

// TestAsyncEngine_ImplementsStreamingEngine verifies the interface is implemented
func TestAsyncEngine_ImplementsStreamingEngine(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	async := NewAsyncEngine(engine, nil)
	defer async.Close()

	// This should compile - AsyncEngine implements StreamingEngine
	var _ StreamingEngine = async
	t.Log("AsyncEngine implements StreamingEngine interface")
}
