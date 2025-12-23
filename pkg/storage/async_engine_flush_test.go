// Package storage - Unit tests for AsyncEngine flush error handling.
//
// These tests verify critical fixes:
// 1. Failed items stay in cache for retry (no silent data loss)
// 2. FlushResult provides accurate statistics
// 3. Partial flush success is handled correctly
package storage

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// FLUSH RESULT TESTS
// =============================================================================

// TestFlushResultHasErrors verifies error detection in FlushResult.
func TestFlushResultHasErrors(t *testing.T) {
	tests := []struct {
		name     string
		result   FlushResult
		expected bool
	}{
		{
			name:     "no errors",
			result:   FlushResult{NodesWritten: 10, EdgesWritten: 5},
			expected: false,
		},
		{
			name:     "node failures",
			result:   FlushResult{NodesWritten: 8, NodesFailed: 2},
			expected: true,
		},
		{
			name:     "edge failures",
			result:   FlushResult{EdgesWritten: 3, EdgesFailed: 1},
			expected: true,
		},
		{
			name:     "delete failures",
			result:   FlushResult{NodesDeleted: 5, DeletesFailed: 1},
			expected: true,
		},
		{
			name:     "mixed success and failure",
			result:   FlushResult{NodesWritten: 10, NodesFailed: 2, EdgesWritten: 5, EdgesFailed: 1},
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.result.HasErrors(); got != tc.expected {
				t.Errorf("HasErrors() = %v, want %v", got, tc.expected)
			}
		})
	}
}

// =============================================================================
// MOCK ENGINE FOR ERROR SIMULATION
// =============================================================================

// errorEngine wraps an engine and simulates failures for specific operations.
type errorEngine struct {
	Engine
	baseEngine Engine
	mu              sync.Mutex
	failNodeIDs     map[NodeID]bool
	failEdgeIDs     map[EdgeID]bool
	failBulkNodes   bool
	failBulkEdges   bool
	failBulkDeletes bool
	updateCalls     int
	createCalls     int
	deleteCalls     int
}

func newErrorEngine() *errorEngine {
	base := NewMemoryEngine()
	namespaced := NewNamespacedEngine(base, "test")
	return &errorEngine{
		Engine:      namespaced,
		baseEngine:  base,
		failNodeIDs: make(map[NodeID]bool),
		failEdgeIDs: make(map[EdgeID]bool),
	}
}

func (e *errorEngine) Close() error {
	// Close the underlying engine (NamespacedEngine.Close() is a no-op).
	if e.baseEngine != nil {
		return e.baseEngine.Close()
	}
	return nil
}

func (e *errorEngine) UpdateNode(node *Node) error {
	e.mu.Lock()
	e.updateCalls++
	fail := e.failNodeIDs[node.ID]
	e.mu.Unlock()

	if fail {
		return errors.New("simulated node update failure")
	}
	return e.Engine.UpdateNode(node)
}

func (e *errorEngine) CreateEdge(edge *Edge) error {
	e.mu.Lock()
	e.createCalls++
	fail := e.failEdgeIDs[edge.ID]
	e.mu.Unlock()

	if fail {
		return errors.New("simulated edge create failure")
	}
	return e.Engine.CreateEdge(edge)
}

func (e *errorEngine) UpdateEdge(edge *Edge) error {
	e.mu.Lock()
	fail := e.failEdgeIDs[edge.ID]
	e.mu.Unlock()

	if fail {
		return errors.New("simulated edge update failure")
	}
	return e.Engine.UpdateEdge(edge)
}

func (e *errorEngine) BulkCreateNodes(nodes []*Node) error {
	e.mu.Lock()
	fail := e.failBulkNodes
	e.mu.Unlock()

	if fail {
		return errors.New("simulated bulk node create failure")
	}
	return e.Engine.BulkCreateNodes(nodes)
}

func (e *errorEngine) BulkCreateEdges(edges []*Edge) error {
	e.mu.Lock()
	fail := e.failBulkEdges
	e.mu.Unlock()

	if fail {
		return errors.New("simulated bulk edge create failure")
	}
	return e.Engine.BulkCreateEdges(edges)
}

func (e *errorEngine) BulkDeleteNodes(ids []NodeID) error {
	e.mu.Lock()
	fail := e.failBulkDeletes
	e.mu.Unlock()

	if fail {
		return errors.New("simulated bulk node delete failure")
	}
	return e.Engine.BulkDeleteNodes(ids)
}

func (e *errorEngine) BulkDeleteEdges(ids []EdgeID) error {
	e.mu.Lock()
	fail := e.failBulkDeletes
	e.mu.Unlock()

	if fail {
		return errors.New("simulated bulk edge delete failure")
	}
	return e.Engine.BulkDeleteEdges(ids)
}

func (e *errorEngine) DeleteNode(id NodeID) error {
	e.mu.Lock()
	e.deleteCalls++
	e.mu.Unlock()
	return e.Engine.DeleteNode(id)
}

func (e *errorEngine) DeleteEdge(id EdgeID) error {
	e.mu.Lock()
	e.deleteCalls++
	e.mu.Unlock()
	return e.Engine.DeleteEdge(id)
}

// =============================================================================
// CRITICAL FIX: FAILED ITEMS STAY IN CACHE
// =============================================================================

// TestFlushFailedNodesStayInCache verifies failed nodes remain in cache for retry.
func TestFlushFailedNodesStayInCache(t *testing.T) {
	errEngine := newErrorEngine()

	// Make node "fail-me" fail on update
	errEngine.failNodeIDs["fail-me"] = true

	cfg := &AsyncEngineConfig{FlushInterval: 1000000} // Don't auto-flush
	ae := NewAsyncEngine(errEngine, cfg)
	defer ae.Close()

	// Create two nodes - one will succeed, one will fail
	successNode := &Node{ID: "success", Labels: []string{"Test"}}
	failNode := &Node{ID: "fail-me", Labels: []string{"Test"}}

	ae.CreateNode(successNode)
	ae.CreateNode(failNode)

	// Verify both are in cache before flush
	ae.mu.RLock()
	initialCacheSize := len(ae.nodeCache)
	ae.mu.RUnlock()
	if initialCacheSize != 2 {
		t.Fatalf("Expected 2 nodes in cache, got %d", initialCacheSize)
	}

	// Flush - one should succeed, one should fail
	result := ae.FlushWithResult()

	if result.NodesWritten != 1 {
		t.Errorf("Expected 1 node written, got %d", result.NodesWritten)
	}
	if result.NodesFailed != 1 {
		t.Errorf("Expected 1 node failed, got %d", result.NodesFailed)
	}

	// CRITICAL: Failed node should still be in cache
	ae.mu.RLock()
	_, failedInCache := ae.nodeCache["fail-me"]
	_, successInCache := ae.nodeCache["success"]
	ae.mu.RUnlock()

	if !failedInCache {
		t.Error("CRITICAL: Failed node was removed from cache - DATA LOSS!")
	}
	if successInCache {
		t.Error("Successful node should have been removed from cache")
	}

	// Verify failed node ID is tracked
	if len(result.FailedNodeIDs) != 1 || result.FailedNodeIDs[0] != "fail-me" {
		t.Errorf("Expected failed node ID 'fail-me', got %v", result.FailedNodeIDs)
	}
}

// TestFlushFailedEdgesStayInCache verifies failed edges remain in cache for retry.
func TestFlushFailedEdgesStayInCache(t *testing.T) {
	errEngine := newErrorEngine()

	// First create nodes for the edges to reference
	errEngine.CreateNode(&Node{ID: "n1", Labels: []string{"Test"}})
	errEngine.CreateNode(&Node{ID: "n2", Labels: []string{"Test"}})

	// Make edge "fail-edge" fail on create
	errEngine.failEdgeIDs["fail-edge"] = true
	errEngine.failBulkEdges = true // Force individual creates

	cfg := &AsyncEngineConfig{FlushInterval: 1000000} // Don't auto-flush
	ae := NewAsyncEngine(errEngine, cfg)
	defer ae.Close()

	// Create two edges - one will succeed, one will fail
	successEdge := &Edge{ID: "success-edge", StartNode: "n1", EndNode: "n2", Type: "KNOWS"}
	failEdge := &Edge{ID: "fail-edge", StartNode: "n1", EndNode: "n2", Type: "KNOWS"}

	ae.CreateEdge(successEdge)
	ae.CreateEdge(failEdge)

	// Flush
	result := ae.FlushWithResult()

	if result.EdgesWritten != 1 {
		t.Errorf("Expected 1 edge written, got %d", result.EdgesWritten)
	}
	if result.EdgesFailed != 1 {
		t.Errorf("Expected 1 edge failed, got %d", result.EdgesFailed)
	}

	// CRITICAL: Failed edge should still be in cache
	ae.mu.RLock()
	_, failedInCache := ae.edgeCache["fail-edge"]
	_, successInCache := ae.edgeCache["success-edge"]
	ae.mu.RUnlock()

	if !failedInCache {
		t.Error("CRITICAL: Failed edge was removed from cache - DATA LOSS!")
	}
	if successInCache {
		t.Error("Successful edge should have been removed from cache")
	}
}

// TestFlushReturnsErrorOnFailures verifies Flush() returns error when there are failures.
func TestFlushReturnsErrorOnFailures(t *testing.T) {
	errEngine := newErrorEngine()
	errEngine.failNodeIDs["fail"] = true

	cfg := &AsyncEngineConfig{FlushInterval: 1000000}
	ae := NewAsyncEngine(errEngine, cfg)
	defer ae.Close()

	ae.CreateNode(&Node{ID: "fail", Labels: []string{"Test"}})

	err := ae.Flush()
	if err == nil {
		t.Error("Flush() should return error when nodes fail to flush")
	}
}

// TestFlushSucceedsWithNoErrors verifies Flush() returns nil on success.
func TestFlushSucceedsWithNoErrors(t *testing.T) {
	errEngine := newErrorEngine()

	cfg := &AsyncEngineConfig{FlushInterval: 1000000}
	ae := NewAsyncEngine(errEngine, cfg)
	defer ae.Close()

	ae.CreateNode(&Node{ID: "ok", Labels: []string{"Test"}})

	err := ae.Flush()
	if err != nil {
		t.Errorf("Flush() should succeed, got error: %v", err)
	}
}

// =============================================================================
// RETRY BEHAVIOR TESTS
// =============================================================================

// TestFailedNodesRetryOnNextFlush verifies failed items are retried.
func TestFailedNodesRetryOnNextFlush(t *testing.T) {
	errEngine := newErrorEngine()

	// First flush will fail
	errEngine.failNodeIDs["retry-me"] = true

	cfg := &AsyncEngineConfig{FlushInterval: 1000000}
	ae := NewAsyncEngine(errEngine, cfg)
	defer ae.Close()

	ae.CreateNode(&Node{ID: "retry-me", Labels: []string{"Test"}})

	// First flush - should fail
	result1 := ae.FlushWithResult()
	if result1.NodesFailed != 1 {
		t.Errorf("First flush: expected 1 failure, got %d", result1.NodesFailed)
	}

	// Node should still be in cache
	ae.mu.RLock()
	_, inCache := ae.nodeCache["retry-me"]
	ae.mu.RUnlock()
	if !inCache {
		t.Fatal("Node should be in cache after failed flush")
	}

	// Fix the error condition
	errEngine.mu.Lock()
	errEngine.failNodeIDs["retry-me"] = false
	errEngine.mu.Unlock()

	// Second flush - should succeed now
	result2 := ae.FlushWithResult()
	if result2.NodesWritten != 1 {
		t.Errorf("Second flush: expected 1 written, got %d", result2.NodesWritten)
	}
	if result2.NodesFailed != 0 {
		t.Errorf("Second flush: expected 0 failures, got %d", result2.NodesFailed)
	}

	// Node should be removed from cache now
	ae.mu.RLock()
	_, stillInCache := ae.nodeCache["retry-me"]
	ae.mu.RUnlock()
	if stillInCache {
		t.Error("Node should have been removed from cache after successful retry")
	}

	// Verify data is in underlying engine
	node, err := errEngine.GetNode("retry-me")
	if err != nil || node == nil {
		t.Error("Node should be in underlying engine after successful retry")
	}
}

// =============================================================================
// PARTIAL FLUSH TESTS
// =============================================================================

// TestPartialFlushClearsOnlySuccessfulItems verifies partial success handling.
func TestPartialFlushClearsOnlySuccessfulItems(t *testing.T) {
	errEngine := newErrorEngine()

	// Fail some nodes but not others
	errEngine.failNodeIDs["fail1"] = true
	errEngine.failNodeIDs["fail2"] = true

	cfg := &AsyncEngineConfig{FlushInterval: 1000000}
	ae := NewAsyncEngine(errEngine, cfg)
	defer ae.Close()

	// Create 5 nodes - 3 will succeed, 2 will fail
	for i := 0; i < 3; i++ {
		ae.CreateNode(&Node{ID: NodeID("ok" + string(rune('0'+i))), Labels: []string{"Test"}})
	}
	ae.CreateNode(&Node{ID: "fail1", Labels: []string{"Test"}})
	ae.CreateNode(&Node{ID: "fail2", Labels: []string{"Test"}})

	result := ae.FlushWithResult()

	if result.NodesWritten != 3 {
		t.Errorf("Expected 3 written, got %d", result.NodesWritten)
	}
	if result.NodesFailed != 2 {
		t.Errorf("Expected 2 failed, got %d", result.NodesFailed)
	}

	// Verify only failed nodes remain in cache
	ae.mu.RLock()
	cacheSize := len(ae.nodeCache)
	ae.mu.RUnlock()

	if cacheSize != 2 {
		t.Errorf("Expected 2 nodes in cache (failed ones), got %d", cacheSize)
	}
}

// =============================================================================
// BULK DELETE FALLBACK TESTS
// =============================================================================

// TestBulkDeleteFallbackToIndividual verifies fallback when bulk fails.
func TestBulkDeleteFallbackToIndividual(t *testing.T) {
	errEngine := newErrorEngine()

	// Create nodes first
	for i := 0; i < 3; i++ {
		errEngine.CreateNode(&Node{ID: NodeID("delete" + string(rune('0'+i))), Labels: []string{"Test"}})
	}

	// Make bulk deletes fail - should fall back to individual
	errEngine.failBulkDeletes = true

	cfg := &AsyncEngineConfig{FlushInterval: 1000000}
	ae := NewAsyncEngine(errEngine, cfg)
	defer ae.Close()

	// Delete nodes
	ae.DeleteNode("delete0")
	ae.DeleteNode("delete1")
	ae.DeleteNode("delete2")

	result := ae.FlushWithResult()

	// Should have fallen back to individual deletes
	errEngine.mu.Lock()
	deleteCalls := errEngine.deleteCalls
	errEngine.mu.Unlock()

	if deleteCalls == 0 {
		t.Error("Expected individual delete calls when bulk fails")
	}

	if result.NodesDeleted != 3 {
		t.Errorf("Expected 3 nodes deleted, got %d", result.NodesDeleted)
	}
}

// =============================================================================
// CONCURRENT ACCESS TESTS
// =============================================================================

// TestFlushConcurrentWriteSafety verifies concurrent writes during flush are safe.
func TestFlushConcurrentWriteSafety(t *testing.T) {
	base := NewMemoryEngine()
	defer base.Close()
	engine := NewNamespacedEngine(base, "test")
	cfg := &AsyncEngineConfig{FlushInterval: 1000000}
	ae := NewAsyncEngine(engine, cfg)
	defer ae.Close()

	// Write initial nodes
	for i := 0; i < 100; i++ {
		ae.CreateNode(&Node{ID: NodeID(fmt.Sprintf("node-%d", i)), Labels: []string{"Test"}})
	}

	// Start flush in background
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ae.Flush()
	}()

	// Concurrent writes during flush
	for i := 100; i < 200; i++ {
		ae.CreateNode(&Node{ID: NodeID(fmt.Sprintf("node-%d", i)), Labels: []string{"Test"}})
	}

	wg.Wait()

	// Second flush for remaining nodes
	ae.Flush()

	// Verify no data loss
	count, _ := base.NodeCount()
	if count < 100 {
		t.Errorf("Expected at least 100 nodes, got %d", count)
	}
}

// TestEmptyFlushReturnsEmptyResult verifies empty flush behavior.
func TestEmptyFlushReturnsEmptyResult(t *testing.T) {
	base := NewMemoryEngine()
	defer base.Close()
	engine := NewNamespacedEngine(base, "test")
	cfg := &AsyncEngineConfig{FlushInterval: 1000000}
	ae := NewAsyncEngine(engine, cfg)
	defer ae.Close()

	result := ae.FlushWithResult()

	if result.HasErrors() {
		t.Error("Empty flush should not have errors")
	}
	if result.NodesWritten != 0 || result.EdgesWritten != 0 {
		t.Error("Empty flush should have no writes")
	}
}

// =============================================================================
// CLOSE ERROR HANDLING TESTS
// =============================================================================

// TestCloseSucceedsWithNoData verifies clean close when no data.
func TestCloseSucceedsWithNoData(t *testing.T) {
	base := NewMemoryEngine()
	defer base.Close()
	engine := NewNamespacedEngine(base, "test")
	cfg := &AsyncEngineConfig{FlushInterval: 1000000}
	ae := NewAsyncEngine(engine, cfg)

	err := ae.Close()
	if err != nil {
		t.Errorf("Close with no data should succeed, got: %v", err)
	}
}

// TestCloseSucceedsAfterSuccessfulFlush verifies clean close after flush.
func TestCloseSucceedsAfterSuccessfulFlush(t *testing.T) {
	base := NewMemoryEngine()
	defer base.Close()
	engine := NewNamespacedEngine(base, "test")
	cfg := &AsyncEngineConfig{FlushInterval: 1000000}
	ae := NewAsyncEngine(engine, cfg)

	// Add some data
	ae.CreateNode(&Node{ID: "n1", Labels: []string{"Test"}})
	ae.CreateNode(&Node{ID: "n2", Labels: []string{"Test"}})

	// Verify data is accessible before close
	n, err := ae.GetNode("n1")
	if err != nil || n == nil {
		t.Fatal("Data should be accessible before close")
	}

	// Manually flush to verify data goes to underlying engine
	flushResult := ae.FlushWithResult()
	if flushResult.NodesWritten != 2 {
		t.Errorf("Should have written 2 nodes, got %d", flushResult.NodesWritten)
	}

	// Verify data in underlying engine BEFORE close
	n, err = base.GetNode(NodeID(prefixTestID("n1")))
	if err != nil || n == nil {
		t.Error("Data should be in underlying engine after flush")
	}

	// Now close - should succeed
	err = ae.Close()
	if err != nil {
		t.Errorf("Close should succeed after successful flush, got: %v", err)
	}
}

// TestCloseReportsFlushErrors verifies close reports flush failures.
func TestCloseReportsFlushErrors(t *testing.T) {
	errEngine := newErrorEngine()
	errEngine.failNodeIDs["fail-on-close"] = true

	cfg := &AsyncEngineConfig{FlushInterval: 1000000}
	ae := NewAsyncEngine(errEngine, cfg)

	// Add a node that will fail to flush
	ae.CreateNode(&Node{ID: "fail-on-close", Labels: []string{"Test"}})

	err := ae.Close()
	if err == nil {
		t.Error("Close should return error when flush fails")
	}

	// Error message should mention the failure
	if err != nil && !containsStr(err.Error(), "flush errors") {
		t.Errorf("Error should mention flush errors, got: %v", err)
	}
}

// TestCloseReportsUnflushedData verifies close reports unflushed data.
func TestCloseReportsUnflushedData(t *testing.T) {
	errEngine := newErrorEngine()
	errEngine.failNodeIDs["stuck"] = true

	cfg := &AsyncEngineConfig{FlushInterval: 1000000}
	ae := NewAsyncEngine(errEngine, cfg)

	// Add a node that will fail to flush
	ae.CreateNode(&Node{ID: "stuck", Labels: []string{"Test"}})

	err := ae.Close()
	if err == nil {
		t.Error("Close should return error when data remains unflushed")
	}

	// Error should mention potential data loss
	if err != nil && !containsStr(err.Error(), "POTENTIAL DATA LOSS") {
		t.Errorf("Error should warn about data loss, got: %v", err)
	}
}

// TestCloseCleansUpGoroutines verifies close stops background goroutines.
func TestCloseCleansUpGoroutines(t *testing.T) {
	base := NewMemoryEngine()
	defer base.Close()
	engine := NewNamespacedEngine(base, "test")
	cfg := &AsyncEngineConfig{FlushInterval: 10} // Fast interval
	ae := NewAsyncEngine(engine, cfg)

	// Let it run for a bit
	ae.CreateNode(&Node{ID: "n1", Labels: []string{"Test"}})

	// Close should complete without hanging
	done := make(chan error)
	go func() {
		done <- ae.Close()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Close returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("Close timed out - goroutine cleanup may be stuck")
	}
}

// containsStr is a simple string contains helper for tests.
func containsStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
