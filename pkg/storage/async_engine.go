// Package storage - AsyncEngine provides write-behind caching for eventual consistency.
//
// AsyncEngine wraps a storage engine and provides:
//   - Immediate writes to in-memory cache (fast)
//   - Background writes to underlying engine (async)
//   - Reads check cache first, then engine (eventual consistency)
//
// Trade-offs:
//   - Much faster writes (returns immediately)
//   - Reads may see stale data briefly (eventual consistency)
//   - Data loss risk if crash before flush (use with WAL for durability)
package storage

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

// AsyncEngine wraps a storage engine with write-behind caching.
// Writes return immediately after updating the cache, and are
// flushed to the underlying engine asynchronously.
type AsyncEngine struct {
	engine Engine

	// In-memory cache for pending writes
	nodeCache   map[NodeID]*Node
	edgeCache   map[EdgeID]*Edge
	deleteNodes map[NodeID]bool
	deleteEdges map[EdgeID]bool
	mu          sync.RWMutex

	// In-flight tracking: nodes/edges being written but not yet cleared from cache
	// This prevents double-counting in NodeCount/EdgeCount during flush
	inFlightNodes map[NodeID]bool
	inFlightEdges map[EdgeID]bool

	// Updates tracking: nodes/edges in cache that are UPDATES (not creates)
	// These exist in underlying engine and shouldn't be counted as pending creates
	updateNodes map[NodeID]bool
	updateEdges map[EdgeID]bool

	// Label index for fast lookups - maps normalized label to node IDs
	labelIndex map[string]map[NodeID]bool

	// Background flush
	flushInterval    time.Duration
	flushTicker      *time.Ticker
	stopChan         chan struct{}
	wg               sync.WaitGroup
	maxNodeCacheSize int
	maxEdgeCacheSize int

	// Stats
	pendingWrites int64
	totalFlushes  int64

	// Flush mutex prevents concurrent flushes (race condition fix)
	flushMu sync.Mutex
}

// AsyncEngineConfig configures the async engine behavior.
type AsyncEngineConfig struct {
	// FlushInterval controls how often pending writes are flushed.
	// Smaller = more consistent, larger = better throughput.
	// Default: 50ms
	FlushInterval time.Duration

	// MaxNodeCacheSize is the maximum number of nodes to buffer before forcing a flush.
	// When this limit is reached, CreateNode will block and flush synchronously.
	// This prevents unbounded memory growth during bulk inserts.
	// Set to 0 for unlimited (not recommended for bulk operations).
	// Default: 50000 (50K nodes, ~35MB assuming 700 bytes/node)
	MaxNodeCacheSize int

	// MaxEdgeCacheSize is the maximum number of edges to buffer before forcing a flush.
	// When this limit is reached, CreateEdge will block and flush synchronously.
	// This prevents unbounded memory growth during bulk inserts.
	// Set to 0 for unlimited (not recommended for bulk operations).
	// Default: 100000 (100K edges, ~50MB assuming 500 bytes/edge)
	MaxEdgeCacheSize int
}

// DefaultAsyncEngineConfig returns sensible defaults.
func DefaultAsyncEngineConfig() *AsyncEngineConfig {
	return &AsyncEngineConfig{
		FlushInterval:    50 * time.Millisecond,
		MaxNodeCacheSize: 50000,  // 50K nodes (~35MB)
		MaxEdgeCacheSize: 100000, // 100K edges (~50MB)
	}
}

// GetUnderlying returns the underlying storage engine.
// This is used for transaction support when the underlying engine
// supports ACID transactions (e.g., BadgerEngine).
func (ae *AsyncEngine) GetUnderlying() Engine {
	return ae.engine
}

// NewAsyncEngine wraps an engine with write-behind caching.
func NewAsyncEngine(engine Engine, config *AsyncEngineConfig) *AsyncEngine {
	if config == nil {
		config = DefaultAsyncEngineConfig()
	}

	ae := &AsyncEngine{
		engine:           engine,
		nodeCache:        make(map[NodeID]*Node),
		edgeCache:        make(map[EdgeID]*Edge),
		deleteNodes:      make(map[NodeID]bool),
		deleteEdges:      make(map[EdgeID]bool),
		inFlightNodes:    make(map[NodeID]bool),
		inFlightEdges:    make(map[EdgeID]bool),
		updateNodes:      make(map[NodeID]bool),
		updateEdges:      make(map[EdgeID]bool),
		labelIndex:       make(map[string]map[NodeID]bool),
		flushInterval:    config.FlushInterval,
		maxNodeCacheSize: config.MaxNodeCacheSize,
		maxEdgeCacheSize: config.MaxEdgeCacheSize,
		stopChan:         make(chan struct{}),
	}

	// Start background flush goroutine
	ae.flushTicker = time.NewTicker(config.FlushInterval)
	ae.wg.Add(1)
	go ae.flushLoop()

	return ae
}

// flushLoop periodically flushes pending writes to the underlying engine.
func (ae *AsyncEngine) flushLoop() {
	defer ae.wg.Done()

	for {
		select {
		case <-ae.flushTicker.C:
			ae.Flush()
		case <-ae.stopChan:
			// Final flush on shutdown
			ae.Flush()
			return
		}
	}
}

// FlushResult tracks the outcome of a flush operation for observability.
type FlushResult struct {
	NodesWritten  int
	NodesFailed   int
	EdgesWritten  int
	EdgesFailed   int
	NodesDeleted  int
	EdgesDeleted  int
	DeletesFailed int
	FailedNodeIDs []NodeID // IDs that failed - still in cache for retry
	FailedEdgeIDs []EdgeID // IDs that failed - still in cache for retry
}

// HasErrors returns true if any flush operations failed.
func (r FlushResult) HasErrors() bool {
	return r.NodesFailed > 0 || r.EdgesFailed > 0 || r.DeletesFailed > 0
}

// Flush writes all pending changes to the underlying engine.
// Uses batched operations for better performance - all deletes in one transaction.
//
// CRITICAL FIX: Failed items are NOT removed from cache - they will be retried
// on the next flush. This prevents silent data loss.
//
// Design: Snapshot caches, clear them, UNLOCK, then write to engine.
// Reads during write see engine data (consistent since cache is empty).
// This avoids blocking reads during I/O which kills Mac M-series performance.
//
// Thread-safe: Uses flushMu to prevent concurrent flushes which can cause
// race conditions when cache limit is reached during concurrent writes.
func (ae *AsyncEngine) Flush() error {
	ae.flushMu.Lock()
	defer ae.flushMu.Unlock()

	result := ae.FlushWithResult()
	if result.HasErrors() {
		return fmt.Errorf("flush incomplete: %d nodes failed, %d edges failed, %d deletes failed",
			result.NodesFailed, result.EdgesFailed, result.DeletesFailed)
	}
	return nil
}

// FlushWithResult writes pending changes and returns detailed results.
// Use this for programmatic access to flush statistics.
func (ae *AsyncEngine) FlushWithResult() FlushResult {
	result := FlushResult{
		FailedNodeIDs: make([]NodeID, 0),
		FailedEdgeIDs: make([]EdgeID, 0),
	}

	ae.mu.Lock()

	// Nothing to flush
	if len(ae.nodeCache) == 0 && len(ae.edgeCache) == 0 && len(ae.deleteNodes) == 0 && len(ae.deleteEdges) == 0 {
		ae.mu.Unlock()
		return result
	}

	ae.totalFlushes++

	// Snapshot pending changes
	nodesToWrite := make(map[NodeID]*Node, len(ae.nodeCache))
	for k, v := range ae.nodeCache {
		nodesToWrite[k] = v
	}
	edgesToWrite := make(map[EdgeID]*Edge, len(ae.edgeCache))
	for k, v := range ae.edgeCache {
		edgesToWrite[k] = v
	}
	nodesToDelete := make(map[NodeID]bool, len(ae.deleteNodes))
	for k, v := range ae.deleteNodes {
		nodesToDelete[k] = v
	}
	edgesToDelete := make(map[EdgeID]bool, len(ae.deleteEdges))
	for k, v := range ae.deleteEdges {
		edgesToDelete[k] = v
	}

	// RACE FIX: Mark nodes/edges as in-flight BEFORE releasing lock
	// This prevents NodeCount/EdgeCount from double-counting during the window
	// where items are written to underlying engine but not yet cleared from cache
	for k := range nodesToWrite {
		ae.inFlightNodes[k] = true
	}
	for k := range edgesToWrite {
		ae.inFlightEdges[k] = true
	}

	ae.mu.Unlock()

	// Track successful operations
	successfulNodeWrites := make(map[NodeID]bool)
	successfulEdgeWrites := make(map[EdgeID]bool)
	successfulNodeDeletes := make(map[NodeID]bool)
	successfulEdgeDeletes := make(map[EdgeID]bool)

	// Apply bulk deletes first
	if len(nodesToDelete) > 0 {
		nodeIDs := make([]NodeID, 0, len(nodesToDelete))
		for id := range nodesToDelete {
			nodeIDs = append(nodeIDs, id)
		}
		if err := ae.engine.BulkDeleteNodes(nodeIDs); err != nil {
			// Bulk failed - try individual deletes
			for _, id := range nodeIDs {
				if err := ae.engine.DeleteNode(id); err != nil {
					result.DeletesFailed++
				} else {
					successfulNodeDeletes[id] = true
					result.NodesDeleted++
				}
			}
		} else {
			for _, id := range nodeIDs {
				successfulNodeDeletes[id] = true
			}
			result.NodesDeleted = len(nodeIDs)
		}
	}

	if len(edgesToDelete) > 0 {
		edgeIDs := make([]EdgeID, 0, len(edgesToDelete))
		for id := range edgesToDelete {
			edgeIDs = append(edgeIDs, id)
		}
		if err := ae.engine.BulkDeleteEdges(edgeIDs); err != nil {
			// Bulk failed - try individual deletes
			for _, id := range edgeIDs {
				if err := ae.engine.DeleteEdge(id); err != nil {
					result.DeletesFailed++
				} else {
					successfulEdgeDeletes[id] = true
					result.EdgesDeleted++
				}
			}
		} else {
			for _, id := range edgeIDs {
				successfulEdgeDeletes[id] = true
			}
			result.EdgesDeleted = len(edgeIDs)
		}
	}

	// Apply creates/updates using UpdateNode (upsert) for each node
	// This handles both new nodes and updates to existing nodes
	if len(nodesToWrite) > 0 {
		for _, node := range nodesToWrite {
			if !nodesToDelete[node.ID] {
				// UpdateNode now has upsert behavior - creates if not exists, updates if exists
				if err := ae.engine.UpdateNode(node); err != nil {
					// CRITICAL FIX: Track failed node - DON'T remove from cache
					result.NodesFailed++
					result.FailedNodeIDs = append(result.FailedNodeIDs, node.ID)
				} else {
					successfulNodeWrites[node.ID] = true
					result.NodesWritten++
				}
			}
		}
	}

	if len(edgesToWrite) > 0 {
		edges := make([]*Edge, 0, len(edgesToWrite))
		for _, edge := range edgesToWrite {
			if !edgesToDelete[edge.ID] {
				edges = append(edges, edge)
			}
		}
		if len(edges) > 0 {
			if err := ae.engine.BulkCreateEdges(edges); err != nil {
				// Bulk failed - try individual creates
				for _, edge := range edges {
					if err := ae.engine.CreateEdge(edge); err != nil {
						// Try update if create fails (might already exist)
						if err := ae.engine.UpdateEdge(edge); err != nil {
							result.EdgesFailed++
							result.FailedEdgeIDs = append(result.FailedEdgeIDs, edge.ID)
						} else {
							successfulEdgeWrites[edge.ID] = true
							result.EdgesWritten++
						}
					} else {
						successfulEdgeWrites[edge.ID] = true
						result.EdgesWritten++
					}
				}
			} else {
				for _, edge := range edges {
					successfulEdgeWrites[edge.ID] = true
				}
				result.EdgesWritten = len(edges)
			}
		}
	}

	// CRITICAL FIX: Only clear SUCCESSFULLY flushed items
	ae.mu.Lock()
	for id := range nodesToWrite {
		// Only clear if successfully written AND still the same object in cache
		if successfulNodeWrites[id] && ae.nodeCache[id] == nodesToWrite[id] {
			delete(ae.nodeCache, id)
			delete(ae.updateNodes, id) // Clear update flag
		}
		// Always clear in-flight marker for this batch (success or fail)
		delete(ae.inFlightNodes, id)
	}
	for id := range edgesToWrite {
		if successfulEdgeWrites[id] && ae.edgeCache[id] == edgesToWrite[id] {
			delete(ae.edgeCache, id)
			delete(ae.updateEdges, id) // Clear update flag
		}
		// Always clear in-flight marker for this batch (success or fail)
		delete(ae.inFlightEdges, id)
	}
	for id := range nodesToDelete {
		if successfulNodeDeletes[id] && ae.deleteNodes[id] {
			delete(ae.deleteNodes, id)
		}
	}
	for id := range edgesToDelete {
		if successfulEdgeDeletes[id] && ae.deleteEdges[id] {
			delete(ae.deleteEdges, id)
		}
	}
	ae.mu.Unlock()

	return result
}

// GetEngine returns the underlying storage engine.
// Used for transaction support which needs direct access.
func (ae *AsyncEngine) GetEngine() Engine {
	return ae.engine
}

// CreateNode adds to cache and returns immediately.
func (ae *AsyncEngine) CreateNode(node *Node) (NodeID, error) {
	// Check cache size limit BEFORE acquiring lock to avoid deadlock
	// If cache is full, flush synchronously to make room
	if ae.maxNodeCacheSize > 0 {
		ae.mu.RLock()
		cacheSize := len(ae.nodeCache)
		ae.mu.RUnlock()
		if cacheSize >= ae.maxNodeCacheSize {
			if err := ae.Flush(); err != nil {
				return "", err
			}
		}
	}

	ae.mu.Lock()
	defer ae.mu.Unlock()

	// Remove from delete set if present (recreating a deleted node)
	wasDeleted := ae.deleteNodes[node.ID]
	delete(ae.deleteNodes, node.ID)

	// Check if this node already exists in cache (being updated, not created)
	_, existsInCache := ae.nodeCache[node.ID]

	// Mark as update only if it was pending delete OR already in cache
	// DO NOT check underlying engine - that causes race conditions and is slow
	// New nodes from CREATE always have fresh UUIDs that won't exist anywhere
	isUpdate := wasDeleted || existsInCache
	if isUpdate {
		ae.updateNodes[node.ID] = true
	} else {
		delete(ae.updateNodes, node.ID)
	}

	ae.nodeCache[node.ID] = node

	// Update label index
	for _, label := range node.Labels {
		normalLabel := strings.ToLower(label)
		if ae.labelIndex[normalLabel] == nil {
			ae.labelIndex[normalLabel] = make(map[NodeID]bool)
		}
		ae.labelIndex[normalLabel][node.ID] = true
	}

	ae.pendingWrites++
	return node.ID, nil
}

// UpdateNode adds to cache and returns immediately.
func (ae *AsyncEngine) UpdateNode(node *Node) error {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	ae.nodeCache[node.ID] = node
	ae.pendingWrites++
	return nil
}

// UpdateNodeEmbedding updates an existing node with its embedding.
// Unlike UpdateNode, this MUST NOT create a new node; it returns ErrNotFound
// if the node does not exist (in cache, in-flight, or in the underlying engine).
func (ae *AsyncEngine) UpdateNodeEmbedding(node *Node) error {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	if ae.deleteNodes[node.ID] {
		return ErrNotFound
	}

	// Exists in cache (including nodes created/updated but not yet flushed).
	if _, ok := ae.nodeCache[node.ID]; ok {
		// Important: do NOT mark this as an update here. If the node is a pending create
		// (not yet flushed), it must still count as a create for NodeCount/EdgeCount.
		ae.nodeCache[node.ID] = node
		ae.pendingWrites++
		return nil
	}

	// In-flight nodes will exist in the underlying engine after flush; allow update.
	if ae.inFlightNodes[node.ID] {
		// This is an update to an existing node (at minimum, it will exist after the in-flight write).
		// Mark as update so NodeCount doesn't temporarily treat it as a pending create.
		ae.updateNodes[node.ID] = true
		ae.nodeCache[node.ID] = node
		ae.pendingWrites++
		return nil
	}

	// Verify existence in underlying engine before accepting the update.
	if _, err := ae.engine.GetNode(node.ID); err != nil {
		return ErrNotFound
	}

	// Node exists in the underlying engine, so this is an update.
	// Mark as update so NodeCount doesn't temporarily treat it as a pending create.
	ae.updateNodes[node.ID] = true
	ae.nodeCache[node.ID] = node
	ae.pendingWrites++
	return nil
}

// DeleteNode marks for deletion and returns immediately.
// Optimized: if node was created in this transaction (still in cache),
// just remove it from cache - no need to delete from underlying engine.
// CRITICAL: If node is in-flight (being flushed), we must also mark for deletion
// because the flush will write it to the underlying engine.
func (ae *AsyncEngine) DeleteNode(id NodeID) error {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	// Check if already marked for deletion (idempotent)
	if ae.deleteNodes[id] {
		return nil
	}

	// Check if node is being flushed right now (in-flight)
	isInFlight := ae.inFlightNodes[id]

	// Check if node was created in this transaction (still in cache)
	if node, existsInCache := ae.nodeCache[id]; existsInCache {
		// Remove from label index
		for _, label := range node.Labels {
			normalLabel := strings.ToLower(label)
			if ae.labelIndex[normalLabel] != nil {
				delete(ae.labelIndex[normalLabel], id)
			}
		}
		// Remove from cache
		delete(ae.nodeCache, id)

		// CRITICAL FIX: If node is in-flight, the flush will still write it
		// to the underlying engine, so we must also mark it for deletion
		if isInFlight {
			ae.deleteNodes[id] = true
			ae.pendingWrites++
		}
		return nil
	}

	// If in-flight, it will exist in underlying engine after flush - mark for deletion
	if isInFlight {
		ae.deleteNodes[id] = true
		ae.pendingWrites++
		return nil
	}

	// Check if node actually exists in underlying engine before marking for deletion
	// This prevents count going negative for non-existent nodes
	if _, err := ae.engine.GetNode(id); err != nil {
		// Node doesn't exist anywhere - nothing to delete
		return ErrNotFound
	}

	// Node exists in underlying engine - mark for deletion
	ae.deleteNodes[id] = true
	ae.pendingWrites++
	return nil
}

// CreateEdge adds to cache and returns immediately.
func (ae *AsyncEngine) CreateEdge(edge *Edge) error {
	// Check cache size limit BEFORE acquiring lock to avoid deadlock
	// If cache is full, flush synchronously to make room
	if ae.maxEdgeCacheSize > 0 {
		ae.mu.RLock()
		cacheSize := len(ae.edgeCache)
		ae.mu.RUnlock()
		if cacheSize >= ae.maxEdgeCacheSize {
			ae.Flush() // Synchronous flush - blocks until complete
		}
	}

	ae.mu.Lock()
	defer ae.mu.Unlock()

	// Remove from delete set if present (recreating a deleted edge)
	wasDeleted := ae.deleteEdges[edge.ID]
	delete(ae.deleteEdges, edge.ID)

	// Check if this edge already exists in cache (being updated, not created)
	_, existsInCache := ae.edgeCache[edge.ID]

	// Mark as update only if it was pending delete OR already in cache
	// DO NOT check underlying engine - that causes race conditions and is slow
	if wasDeleted || existsInCache {
		ae.updateEdges[edge.ID] = true
	} else {
		delete(ae.updateEdges, edge.ID)
	}

	ae.edgeCache[edge.ID] = edge
	ae.pendingWrites++
	return nil
}

// UpdateEdge adds to cache and returns immediately.
func (ae *AsyncEngine) UpdateEdge(edge *Edge) error {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	ae.edgeCache[edge.ID] = edge
	ae.pendingWrites++
	return nil
}

// DeleteEdge marks for deletion and returns immediately.
// Optimized: if edge was created in this transaction (still in cache),
// just remove it from cache - no need to delete from underlying engine.
// CRITICAL: If edge is in-flight (being flushed), we must also mark for deletion
// because the flush will write it to the underlying engine.
func (ae *AsyncEngine) DeleteEdge(id EdgeID) error {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	// Check if already marked for deletion (idempotent)
	if ae.deleteEdges[id] {
		return nil
	}

	// Check if edge is being flushed right now (in-flight)
	isInFlight := ae.inFlightEdges[id]

	// Check if edge was created in this transaction (still in cache)
	if _, existsInCache := ae.edgeCache[id]; existsInCache {
		// Edge was created but not flushed - just remove from cache
		delete(ae.edgeCache, id)

		// CRITICAL FIX: If edge is in-flight, the flush will still write it
		// to the underlying engine, so we must also mark it for deletion
		if isInFlight {
			ae.deleteEdges[id] = true
			ae.pendingWrites++
		}
		return nil
	}

	// If in-flight, it will exist in underlying engine after flush - mark for deletion
	if isInFlight {
		ae.deleteEdges[id] = true
		ae.pendingWrites++
		return nil
	}

	// Check if edge actually exists in underlying engine before marking for deletion
	// This prevents count going negative for non-existent edges
	if _, err := ae.engine.GetEdge(id); err != nil {
		// Edge doesn't exist anywhere - nothing to delete
		return ErrNotFound
	}

	// Edge exists in underlying engine - mark for deletion
	ae.deleteEdges[id] = true
	ae.pendingWrites++
	return nil
}

// GetNode checks cache first, then underlying engine.
func (ae *AsyncEngine) GetNode(id NodeID) (*Node, error) {
	ae.mu.RLock()
	// Check if deleted
	if ae.deleteNodes[id] {
		ae.mu.RUnlock()
		return nil, ErrNotFound
	}
	// Check cache
	if node, ok := ae.nodeCache[id]; ok {
		ae.mu.RUnlock()
		return node, nil
	}
	ae.mu.RUnlock()

	// Fall through to engine
	return ae.engine.GetNode(id)
}

// GetEdge checks cache first, then underlying engine.
func (ae *AsyncEngine) GetEdge(id EdgeID) (*Edge, error) {
	ae.mu.RLock()
	if ae.deleteEdges[id] {
		ae.mu.RUnlock()
		return nil, ErrNotFound
	}
	if edge, ok := ae.edgeCache[id]; ok {
		ae.mu.RUnlock()
		return edge, nil
	}
	ae.mu.RUnlock()

	return ae.engine.GetEdge(id)
}

// GetNodesByLabel checks cache and merges with engine results.
// Uses case-insensitive label matching for Neo4j compatibility.
// Snapshots cache state quickly, then releases lock before engine I/O.
// GetFirstNodeByLabel returns the first node with the specified label.
// Optimized for MATCH...LIMIT 1 patterns - uses label index for O(1) lookup.
func (ae *AsyncEngine) GetFirstNodeByLabel(label string) (*Node, error) {
	ae.mu.RLock()
	normalLabel := strings.ToLower(label)

	// Use label index for O(1) lookup instead of scanning entire cache
	if nodeIDs := ae.labelIndex[normalLabel]; len(nodeIDs) > 0 {
		for id := range nodeIDs {
			if !ae.deleteNodes[id] {
				if node := ae.nodeCache[id]; node != nil {
					ae.mu.RUnlock()
					return node, nil
				}
			}
		}
	}
	ae.mu.RUnlock()

	// Check underlying engine
	if getter, ok := ae.engine.(interface{ GetFirstNodeByLabel(string) (*Node, error) }); ok {
		return getter.GetFirstNodeByLabel(label)
	}

	// Fallback: get all and return first
	nodes, err := ae.engine.GetNodesByLabel(label)
	if err != nil || len(nodes) == 0 {
		return nil, err
	}
	return nodes[0], nil
}

func (ae *AsyncEngine) GetNodesByLabel(label string) ([]*Node, error) {
	ae.mu.RLock()
	cachedNodes := make([]*Node, 0)
	deletedIDs := make(map[NodeID]bool)

	// Normalize label for case-insensitive matching (Neo4j compatible)
	normalLabel := strings.ToLower(label)

	for id := range ae.deleteNodes {
		deletedIDs[id] = true
	}
	for _, node := range ae.nodeCache {
		for _, l := range node.Labels {
			if strings.ToLower(l) == normalLabel { // Case-insensitive comparison
				cachedNodes = append(cachedNodes, node)
				break
			}
		}
	}
	ae.mu.RUnlock()

	// Get from engine WITHOUT lock (I/O can be slow)
	engineNodes, err := ae.engine.GetNodesByLabel(label)
	if err != nil {
		return cachedNodes, nil // Return cache-only on error
	}

	// Merge: cache overrides engine
	result := make([]*Node, 0, len(cachedNodes)+len(engineNodes))

	// Add cached nodes first
	seenIDs := make(map[NodeID]bool)
	for _, node := range cachedNodes {
		result = append(result, node)
		seenIDs[node.ID] = true
	}

	// Add engine nodes not in cache or deleted
	for _, node := range engineNodes {
		if !seenIDs[node.ID] && !deletedIDs[node.ID] {
			result = append(result, node)
		}
	}

	return result, nil
}

// BatchGetNodes fetches multiple nodes, checking cache first then engine.
// Returns a map for O(1) lookup. Missing nodes are not included.
func (ae *AsyncEngine) BatchGetNodes(ids []NodeID) (map[NodeID]*Node, error) {
	if len(ids) == 0 {
		return make(map[NodeID]*Node), nil
	}

	ae.mu.RLock()
	defer ae.mu.RUnlock()

	result := make(map[NodeID]*Node, len(ids))
	var missingIDs []NodeID

	// Check cache and deleted set first
	for _, id := range ids {
		if id == "" {
			continue
		}

		// Skip if marked for deletion
		if ae.deleteNodes[id] {
			continue
		}

		// Check cache first
		if node, exists := ae.nodeCache[id]; exists {
			result[id] = node
			continue
		}

		// Need to fetch from engine
		missingIDs = append(missingIDs, id)
	}

	// Batch fetch missing from engine
	if len(missingIDs) > 0 {
		engineNodes, err := ae.engine.BatchGetNodes(missingIDs)
		if err != nil {
			return result, nil // Return what we have from cache
		}

		// Add engine nodes not marked for deletion
		for id, node := range engineNodes {
			if !ae.deleteNodes[id] {
				result[id] = node
			}
		}
	}

	return result, nil
}

// AllNodes returns merged view of cache, in-flight nodes, and engine.
// NOTE: We hold the read lock for the ENTIRE operation to prevent race conditions
// with Flush() which clears the cache after writing to the engine.
// CRITICAL: We must include in-flight nodes - these are being written to the engine
// but haven't been cleared from tracking yet. Without this, nodes can become
// "invisible" during the flush window, causing DETACH DELETE to miss them.
func (ae *AsyncEngine) AllNodes() ([]*Node, error) {
	ae.mu.RLock()
	defer ae.mu.RUnlock()

	// Build set of deleted IDs (these should NOT appear in results)
	deletedIDs := make(map[NodeID]bool)
	for id := range ae.deleteNodes {
		deletedIDs[id] = true
	}

	// Collect nodes from cache (pending writes not yet flushed)
	cachedNodes := make(map[NodeID]*Node, len(ae.nodeCache))
	for id, node := range ae.nodeCache {
		cachedNodes[id] = node
	}

	// Get nodes from underlying engine
	engineNodes, err := ae.engine.AllNodes()
	if err != nil {
		// If engine fails, return what we have in cache
		result := make([]*Node, 0, len(cachedNodes))
		for _, node := range cachedNodes {
			if !deletedIDs[node.ID] {
				result = append(result, node)
			}
		}
		return result, nil
	}

	// Merge: cache takes precedence, then engine
	// Track what we've seen to avoid duplicates
	seenIDs := make(map[NodeID]bool)
	result := make([]*Node, 0, len(cachedNodes)+len(engineNodes))

	// Add cached nodes first (they're the "freshest" view)
	for id, node := range cachedNodes {
		if !deletedIDs[id] {
			result = append(result, node)
			seenIDs[id] = true
		}
	}

	// Add engine nodes that aren't in cache and aren't deleted
	for _, node := range engineNodes {
		if !seenIDs[node.ID] && !deletedIDs[node.ID] {
			result = append(result, node)
			seenIDs[node.ID] = true
		}
	}

	return result, nil
}

// AllEdges returns merged view of cache and engine.
// NOTE: We hold the read lock for the ENTIRE operation to prevent race conditions
// with Flush() which clears the cache after writing to the engine.
func (ae *AsyncEngine) AllEdges() ([]*Edge, error) {
	ae.mu.RLock()
	defer ae.mu.RUnlock()

	cachedEdges := make([]*Edge, 0, len(ae.edgeCache))
	deletedIDs := make(map[EdgeID]bool)

	for id := range ae.deleteEdges {
		deletedIDs[id] = true
	}
	for _, edge := range ae.edgeCache {
		cachedEdges = append(cachedEdges, edge)
	}

	engineEdges, err := ae.engine.AllEdges()
	if err != nil {
		return cachedEdges, nil
	}

	result := make([]*Edge, 0, len(cachedEdges)+len(engineEdges))
	seenIDs := make(map[EdgeID]bool)

	for _, edge := range cachedEdges {
		result = append(result, edge)
		seenIDs[edge.ID] = true
	}
	for _, edge := range engineEdges {
		if !seenIDs[edge.ID] && !deletedIDs[edge.ID] {
			result = append(result, edge)
		}
	}

	return result, nil
}

// GetEdgesByType returns all edges of a specific type, merging cache and engine.
func (ae *AsyncEngine) GetEdgesByType(edgeType string) ([]*Edge, error) {
	if edgeType == "" {
		return ae.AllEdges()
	}

	ae.mu.RLock()
	normalizedType := strings.ToLower(edgeType)
	cachedEdges := make([]*Edge, 0)
	deletedIDs := make(map[EdgeID]bool)

	for id := range ae.deleteEdges {
		deletedIDs[id] = true
	}
	for _, edge := range ae.edgeCache {
		if strings.ToLower(edge.Type) == normalizedType {
			cachedEdges = append(cachedEdges, edge)
		}
	}
	ae.mu.RUnlock()

	engineEdges, err := ae.engine.GetEdgesByType(edgeType)
	if err != nil {
		return cachedEdges, nil
	}

	result := make([]*Edge, 0, len(cachedEdges)+len(engineEdges))
	seenIDs := make(map[EdgeID]bool)

	for _, edge := range cachedEdges {
		result = append(result, edge)
		seenIDs[edge.ID] = true
	}
	for _, edge := range engineEdges {
		if !seenIDs[edge.ID] && !deletedIDs[edge.ID] {
			result = append(result, edge)
		}
	}

	return result, nil
}

// Delegate read-only methods to engine

func (ae *AsyncEngine) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	// Check cache first for this node's outgoing edges
	ae.mu.RLock()
	var cached []*Edge
	for _, edge := range ae.edgeCache {
		if edge.StartNode == nodeID && !ae.deleteEdges[edge.ID] {
			cached = append(cached, edge)
		}
	}
	deletedIDs := make(map[EdgeID]bool)
	for id := range ae.deleteEdges {
		deletedIDs[id] = true
	}
	ae.mu.RUnlock()

	engineEdges, err := ae.engine.GetOutgoingEdges(nodeID)
	if err != nil {
		return cached, nil
	}

	// Merge
	seenIDs := make(map[EdgeID]bool)
	result := make([]*Edge, 0, len(cached)+len(engineEdges))
	for _, e := range cached {
		result = append(result, e)
		seenIDs[e.ID] = true
	}
	for _, e := range engineEdges {
		if !seenIDs[e.ID] && !deletedIDs[e.ID] {
			result = append(result, e)
		}
	}
	return result, nil
}

func (ae *AsyncEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	ae.mu.RLock()
	var cached []*Edge
	for _, edge := range ae.edgeCache {
		if edge.EndNode == nodeID && !ae.deleteEdges[edge.ID] {
			cached = append(cached, edge)
		}
	}
	deletedIDs := make(map[EdgeID]bool)
	for id := range ae.deleteEdges {
		deletedIDs[id] = true
	}
	ae.mu.RUnlock()

	engineEdges, err := ae.engine.GetIncomingEdges(nodeID)
	if err != nil {
		return cached, nil
	}

	seenIDs := make(map[EdgeID]bool)
	result := make([]*Edge, 0, len(cached)+len(engineEdges))
	for _, e := range cached {
		result = append(result, e)
		seenIDs[e.ID] = true
	}
	for _, e := range engineEdges {
		if !seenIDs[e.ID] && !deletedIDs[e.ID] {
			result = append(result, e)
		}
	}
	return result, nil
}

func (ae *AsyncEngine) GetEdgesBetween(startID, endID NodeID) ([]*Edge, error) {
	return ae.engine.GetEdgesBetween(startID, endID)
}

func (ae *AsyncEngine) GetEdgeBetween(startID, endID NodeID, edgeType string) *Edge {
	return ae.engine.GetEdgeBetween(startID, endID, edgeType)
}

func (ae *AsyncEngine) GetAllNodes() []*Node {
	nodes, _ := ae.AllNodes()
	return nodes
}

func (ae *AsyncEngine) GetInDegree(nodeID NodeID) int {
	return ae.engine.GetInDegree(nodeID)
}

func (ae *AsyncEngine) GetOutDegree(nodeID NodeID) int {
	return ae.engine.GetOutDegree(nodeID)
}

func (ae *AsyncEngine) GetSchema() *SchemaManager {
	return ae.engine.GetSchema()
}

func (ae *AsyncEngine) NodeCount() (int64, error) {
	// Hold lock during entire operation to get consistent count
	// This prevents race with flush which clears cache before writing to engine
	ae.mu.RLock()

	// Count pending creates, excluding:
	// - update nodes (exist in engine, just being modified)
	// NOTE: We DO count in-flight nodes because they are being written to engine
	// but engine.NodeCount() won't include them until the write commits.
	// During flush, nodes transition: cache -> inFlight -> engine
	// If we skip inFlight nodes AND engine hasn't committed, count = 0 (BUG!)
	pendingCreates := int64(0)
	pendingUpdates := int64(0)
	inFlightCreates := int64(0)
	for id := range ae.nodeCache {
		if ae.updateNodes[id] {
			pendingUpdates++ // Exists in engine, just updating
			continue
		}
		if ae.inFlightNodes[id] {
			inFlightCreates++ // Being written to engine right now
			continue
		}
		pendingCreates++
	}
	// Also count nodes that are in-flight but NOT updates (they're being created)
	pendingDeletes := int64(len(ae.deleteNodes))

	engineCount, err := ae.engine.NodeCount()
	ae.mu.RUnlock()

	if err != nil {
		return 0, err
	}

	// Adjust for pending creates and deletes
	// Note: pendingUpdates don't change count (already counted in engineCount)
	// Include inFlightCreates because they're being written but not yet in engineCount
	count := engineCount + pendingCreates + inFlightCreates - pendingDeletes

	// Clamp to zero if negative (should never happen, log for debugging)
	if count < 0 {
		log.Printf("⚠️ [COUNT BUG] NodeCount went negative: engineCount=%d pendingCreates=%d pendingDeletes=%d result=%d (clamping to 0)",
			engineCount, pendingCreates, pendingDeletes, count)
		return 0, nil
	}
	return count, nil
}

func (ae *AsyncEngine) EdgeCount() (int64, error) {
	// Hold lock during entire operation to get consistent count
	// This prevents race with flush which clears cache before writing to engine
	ae.mu.RLock()

	// Count pending creates, excluding:
	// - update edges (exist in engine, just being modified)
	// NOTE: We DO count in-flight edges because they are being written to engine
	// but engine.EdgeCount() won't include them until the write commits.
	pendingCreates := int64(0)
	inFlightCreates := int64(0)
	for id := range ae.edgeCache {
		if ae.updateEdges[id] {
			continue // Exists in engine, just updating
		}
		if ae.inFlightEdges[id] {
			inFlightCreates++ // Being written to engine right now
			continue
		}
		pendingCreates++
	}
	pendingDeletes := int64(len(ae.deleteEdges))

	engineCount, err := ae.engine.EdgeCount()
	ae.mu.RUnlock()

	if err != nil {
		return 0, err
	}

	// Adjust for pending creates and deletes
	// Note: updates don't change count (already counted in engineCount)
	// Include inFlightCreates because they're being written but not yet in engineCount
	count := engineCount + pendingCreates + inFlightCreates - pendingDeletes

	// Clamp to zero if negative (should never happen, log for debugging)
	if count < 0 {
		log.Printf("⚠️ [COUNT BUG] EdgeCount went negative: engineCount=%d pendingCreates=%d pendingDeletes=%d result=%d (clamping to 0)",
			engineCount, pendingCreates, pendingDeletes, count)
		return 0, nil
	}
	return count, nil
}

func (ae *AsyncEngine) NodeCountByPrefix(prefix string) (int64, error) {
	// Keep same consistency semantics as NodeCount(): hold lock across engine read.
	ae.mu.RLock()

	pendingCreates := int64(0)
	pendingUpdates := int64(0)
	inFlightCreates := int64(0)
	for id := range ae.nodeCache {
		if !strings.HasPrefix(string(id), prefix) {
			continue
		}
		if ae.updateNodes[id] {
			pendingUpdates++
			continue
		}
		if ae.inFlightNodes[id] {
			inFlightCreates++
			continue
		}
		pendingCreates++
	}

	pendingDeletes := int64(0)
	for id := range ae.deleteNodes {
		if strings.HasPrefix(string(id), prefix) {
			pendingDeletes++
		}
	}

	var engineCount int64
	var err error
	if stats, ok := ae.engine.(PrefixStatsEngine); ok {
		engineCount, err = stats.NodeCountByPrefix(prefix)
	} else {
		// Correctness fallback for uncommon engines (slower).
		engineCount, err = countNodesInEngineByPrefix(ae.engine, prefix)
	}

	ae.mu.RUnlock()
	if err != nil {
		return 0, err
	}

	count := engineCount + pendingCreates + inFlightCreates - pendingDeletes
	if count < 0 {
		log.Printf("⚠️ [COUNT BUG] NodeCountByPrefix went negative: prefix=%q engineCount=%d pendingCreates=%d pendingDeletes=%d result=%d (clamping to 0)",
			prefix, engineCount, pendingCreates, pendingDeletes, count)
		return 0, nil
	}
	_ = pendingUpdates // no-op (kept for symmetry with NodeCount)
	return count, nil
}

func (ae *AsyncEngine) EdgeCountByPrefix(prefix string) (int64, error) {
	ae.mu.RLock()

	pendingCreates := int64(0)
	inFlightCreates := int64(0)
	for id := range ae.edgeCache {
		if !strings.HasPrefix(string(id), prefix) {
			continue
		}
		if ae.updateEdges[id] {
			continue
		}
		if ae.inFlightEdges[id] {
			inFlightCreates++
			continue
		}
		pendingCreates++
	}

	pendingDeletes := int64(0)
	for id := range ae.deleteEdges {
		if strings.HasPrefix(string(id), prefix) {
			pendingDeletes++
		}
	}

	var engineCount int64
	var err error
	if stats, ok := ae.engine.(PrefixStatsEngine); ok {
		engineCount, err = stats.EdgeCountByPrefix(prefix)
	} else {
		engineCount, err = countEdgesInEngineByPrefix(ae.engine, prefix)
	}

	ae.mu.RUnlock()
	if err != nil {
		return 0, err
	}

	count := engineCount + pendingCreates + inFlightCreates - pendingDeletes
	if count < 0 {
		log.Printf("⚠️ [COUNT BUG] EdgeCountByPrefix went negative: prefix=%q engineCount=%d pendingCreates=%d pendingDeletes=%d result=%d (clamping to 0)",
			prefix, engineCount, pendingCreates, pendingDeletes, count)
		return 0, nil
	}
	return count, nil
}

func countNodesInEngineByPrefix(engine Engine, prefix string) (int64, error) {
	if streamer, ok := engine.(StreamingEngine); ok {
		var count int64
		err := streamer.StreamNodes(context.Background(), func(node *Node) error {
			if strings.HasPrefix(string(node.ID), prefix) {
				count++
			}
			return nil
		})
		return count, err
	}

	nodes, err := engine.AllNodes()
	if err != nil {
		return 0, err
	}
	var count int64
	for _, node := range nodes {
		if strings.HasPrefix(string(node.ID), prefix) {
			count++
		}
	}
	return count, nil
}

func countEdgesInEngineByPrefix(engine Engine, prefix string) (int64, error) {
	if streamer, ok := engine.(StreamingEngine); ok {
		var count int64
		err := streamer.StreamEdges(context.Background(), func(edge *Edge) error {
			if strings.HasPrefix(string(edge.ID), prefix) {
				count++
			}
			return nil
		})
		return count, err
	}

	edges, err := engine.AllEdges()
	if err != nil {
		return 0, err
	}
	var count int64
	for _, edge := range edges {
		if strings.HasPrefix(string(edge.ID), prefix) {
			count++
		}
	}
	return count, nil
}

// Close stops the background flush goroutine and flushes all pending data.
// Returns an error if the final flush fails or if data remains unflushed.
func (ae *AsyncEngine) Close() error {
	// Stop flush loop
	close(ae.stopChan)
	ae.flushTicker.Stop()
	ae.wg.Wait()

	// Final flush with error tracking
	result := ae.FlushWithResult()

	// Check for unflushed data after final flush attempt
	ae.mu.RLock()
	pendingNodes := len(ae.nodeCache)
	pendingEdges := len(ae.edgeCache)
	pendingNodeDeletes := len(ae.deleteNodes)
	pendingEdgeDeletes := len(ae.deleteEdges)
	ae.mu.RUnlock()

	// Close underlying engine
	engineErr := ae.engine.Close()

	// Build error message if there are issues
	if result.HasErrors() || pendingNodes > 0 || pendingEdges > 0 {
		var errMsg string
		if result.HasErrors() {
			errMsg = fmt.Sprintf("flush errors: %d nodes failed, %d edges failed, %d deletes failed",
				result.NodesFailed, result.EdgesFailed, result.DeletesFailed)
		}
		if pendingNodes > 0 || pendingEdges > 0 || pendingNodeDeletes > 0 || pendingEdgeDeletes > 0 {
			if errMsg != "" {
				errMsg += "; "
			}
			errMsg += fmt.Sprintf("unflushed: %d nodes, %d edges, %d node deletes, %d edge deletes (POTENTIAL DATA LOSS)",
				pendingNodes, pendingEdges, pendingNodeDeletes, pendingEdgeDeletes)
		}
		if engineErr != nil {
			return fmt.Errorf("%s; engine close: %w", errMsg, engineErr)
		}
		return fmt.Errorf("async engine close: %s", errMsg)
	}

	return engineErr
}

// BulkCreateNodes creates nodes in batch (async).
func (ae *AsyncEngine) BulkCreateNodes(nodes []*Node) error {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	for _, node := range nodes {
		delete(ae.deleteNodes, node.ID)
		ae.nodeCache[node.ID] = node
	}
	ae.pendingWrites += int64(len(nodes))
	return nil
}

// BulkCreateEdges creates edges in batch (async).
func (ae *AsyncEngine) BulkCreateEdges(edges []*Edge) error {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	for _, edge := range edges {
		delete(ae.deleteEdges, edge.ID)
		ae.edgeCache[edge.ID] = edge
	}
	ae.pendingWrites += int64(len(edges))
	return nil
}

// BulkDeleteNodes marks multiple nodes for deletion (async).
func (ae *AsyncEngine) BulkDeleteNodes(ids []NodeID) error {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	for _, id := range ids {
		delete(ae.nodeCache, id)
		ae.deleteNodes[id] = true
	}
	ae.pendingWrites += int64(len(ids))
	return nil
}

// BulkDeleteEdges marks multiple edges for deletion (async).
func (ae *AsyncEngine) BulkDeleteEdges(ids []EdgeID) error {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	for _, id := range ids {
		delete(ae.edgeCache, id)
		ae.deleteEdges[id] = true
	}
	ae.pendingWrites += int64(len(ids))
	return nil
}

// Stats returns async engine statistics.
func (ae *AsyncEngine) Stats() (pendingWrites, totalFlushes int64) {
	ae.mu.RLock()
	defer ae.mu.RUnlock()
	return ae.pendingWrites, ae.totalFlushes
}

// HasPendingWrites returns true if there are unflushed writes.
// This is a cheap check that can be used to avoid unnecessary flush calls.
func (ae *AsyncEngine) HasPendingWrites() bool {
	ae.mu.RLock()
	defer ae.mu.RUnlock()
	return len(ae.nodeCache) > 0 || len(ae.edgeCache) > 0 ||
		len(ae.deleteNodes) > 0 || len(ae.deleteEdges) > 0
}

// FindNodeNeedingEmbedding returns a node that needs embedding.
// IMPORTANT: This checks the in-memory cache first to ensure we don't re-process
// nodes that have embeddings pending flush to the underlying engine.
//
// The algorithm:
// 1. Build set of node IDs that have embeddings in cache (pending flush)
// 2. First check nodes in our cache that need embedding
// 3. Then check underlying engine, skipping nodes we have in cache with embeddings
func (ae *AsyncEngine) FindNodeNeedingEmbedding() *Node {
	ae.mu.RLock()

	// Build set of node IDs in cache that already have embeddings
	cachedWithEmbedding := make(map[NodeID]bool)
	for id, node := range ae.nodeCache {
		if len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0 {
			cachedWithEmbedding[id] = true
		}
	}

	// First check nodes in our own cache that might need embedding
	for _, node := range ae.nodeCache {
		if ae.deleteNodes[node.ID] {
			continue
		}
		if !cachedWithEmbedding[node.ID] && NodeNeedsEmbedding(node) {
			ae.mu.RUnlock()
			return node
		}
	}
	ae.mu.RUnlock()

	// Try dedicated finder on underlying engine
	if finder, ok := ae.engine.(interface{ FindNodeNeedingEmbedding() *Node }); ok {
		node := finder.FindNodeNeedingEmbedding()
		if node == nil {
			return nil
		}

		// Check if this node has an embedding in our cache
		if cachedWithEmbedding[node.ID] {
			// This node has embedding pending flush - no work to do
			return nil
		}

		return node
	}

	// Fallback: use AllNodes from ExportableEngine
	if exportable, ok := ae.engine.(ExportableEngine); ok {
		nodes, err := exportable.AllNodes()
		if err != nil {
			return nil
		}
		for _, node := range nodes {
			// Skip if in cache with embedding
			if cachedWithEmbedding[node.ID] {
				continue
			}
			if NodeNeedsEmbedding(node) {
				return node
			}
		}
	}

	return nil
}

// RefreshPendingEmbeddingsIndex delegates to the underlying engine, if supported.
// This keeps the pending-embeddings secondary index consistent even when AsyncEngine
// is the outer-most storage layer.
func (ae *AsyncEngine) RefreshPendingEmbeddingsIndex() int {
	if mgr, ok := ae.engine.(interface{ RefreshPendingEmbeddingsIndex() int }); ok {
		return mgr.RefreshPendingEmbeddingsIndex()
	}
	return 0
}

// MarkNodeEmbedded delegates to the underlying engine, if supported.
// This removes a node from the pending-embeddings secondary index once embedded.
func (ae *AsyncEngine) MarkNodeEmbedded(nodeID NodeID) {
	if mgr, ok := ae.engine.(interface{ MarkNodeEmbedded(NodeID) }); ok {
		mgr.MarkNodeEmbedded(nodeID)
	}
}

// IterateNodes iterates through all nodes, checking cache first.
func (ae *AsyncEngine) IterateNodes(fn func(*Node) bool) error {
	// First iterate cache
	// We need to make copies since the callback may be called without locks held
	// and the node could be modified by other goroutines
	ae.mu.RLock()
	cachedIDs := make(map[NodeID]bool)
	cachedCopies := make([]*Node, 0, len(ae.nodeCache))
	for id, node := range ae.nodeCache {
		if ae.deleteNodes[id] {
			continue
		}
		cachedIDs[id] = true
		// Make a deep copy of the node to avoid concurrent access issues
		nodeCopy := &Node{
			ID:           node.ID,
			Labels:       append([]string(nil), node.Labels...),
			Properties:   make(map[string]any, len(node.Properties)),
			CreatedAt:    node.CreatedAt,
			UpdatedAt:    node.UpdatedAt,
			DecayScore:   node.DecayScore,
			LastAccessed: node.LastAccessed,
			AccessCount:  node.AccessCount,
			ChunkEmbeddings: func() [][]float32 {
				chunks := make([][]float32, len(node.ChunkEmbeddings))
				for i, emb := range node.ChunkEmbeddings {
					chunks[i] = append([]float32(nil), emb...)
				}
				return chunks
			}(),
		}
		for k, v := range node.Properties {
			nodeCopy.Properties[k] = v
		}
		cachedCopies = append(cachedCopies, nodeCopy)
	}
	ae.mu.RUnlock()

	// Call callback with copies (safe to do without lock)
	for _, nodeCopy := range cachedCopies {
		if !fn(nodeCopy) {
			return nil
		}
	}

	// Then iterate underlying engine, skipping cached nodes
	if iterator, ok := ae.engine.(interface{ IterateNodes(func(*Node) bool) error }); ok {
		return iterator.IterateNodes(func(node *Node) bool {
			if cachedIDs[node.ID] {
				return true // Skip, already visited from cache
			}
			ae.mu.RLock()
			deleted := ae.deleteNodes[node.ID]
			ae.mu.RUnlock()

			if deleted {
				return true // Skip deleted
			}

			return fn(node)
		})
	}

	return nil
}

// ============================================================================
// StreamingEngine Implementation
// ============================================================================

// StreamNodes implements StreamingEngine.StreamNodes by delegating to the underlying engine.
// It merges cached nodes with the underlying stream for consistency.
func (ae *AsyncEngine) StreamNodes(ctx context.Context, fn func(node *Node) error) error {
	ae.mu.RLock()

	// First, stream cached nodes (not yet flushed)
	for id, node := range ae.nodeCache {
		if ae.deleteNodes[id] {
			continue // Skip if marked for deletion
		}
		ae.mu.RUnlock()
		if err := fn(node); err != nil {
			if err == ErrIterationStopped {
				return nil // Normal early termination
			}
			return err
		}
		ae.mu.RLock()
	}

	// Build set of cached node IDs to skip in underlying stream
	cachedIDs := make(map[NodeID]bool, len(ae.nodeCache))
	for id := range ae.nodeCache {
		cachedIDs[id] = true
	}
	deletedIDs := make(map[NodeID]bool, len(ae.deleteNodes))
	for id := range ae.deleteNodes {
		deletedIDs[id] = true
	}
	ae.mu.RUnlock()

	// Then stream from underlying engine, skipping cached/deleted nodes
	if streamer, ok := ae.engine.(StreamingEngine); ok {
		return streamer.StreamNodes(ctx, func(node *Node) error {
			// Skip if we already returned this from cache or it's deleted
			if cachedIDs[node.ID] || deletedIDs[node.ID] {
				return nil
			}
			return fn(node)
		})
	}

	// Fallback: load all from underlying engine
	nodes, err := ae.engine.AllNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if cachedIDs[node.ID] || deletedIDs[node.ID] {
			continue
		}
		if err := fn(node); err != nil {
			if err == ErrIterationStopped {
				return nil
			}
			return err
		}
	}
	return nil
}

// StreamEdges implements StreamingEngine.StreamEdges by delegating to the underlying engine.
func (ae *AsyncEngine) StreamEdges(ctx context.Context, fn func(edge *Edge) error) error {
	ae.mu.RLock()

	// First, stream cached edges
	for id, edge := range ae.edgeCache {
		if ae.deleteEdges[id] {
			continue
		}
		ae.mu.RUnlock()
		if err := fn(edge); err != nil {
			if err == ErrIterationStopped {
				return nil
			}
			return err
		}
		ae.mu.RLock()
	}

	// Build set of cached edge IDs
	cachedIDs := make(map[EdgeID]bool, len(ae.edgeCache))
	for id := range ae.edgeCache {
		cachedIDs[id] = true
	}
	deletedIDs := make(map[EdgeID]bool, len(ae.deleteEdges))
	for id := range ae.deleteEdges {
		deletedIDs[id] = true
	}
	ae.mu.RUnlock()

	// Stream from underlying engine
	if streamer, ok := ae.engine.(StreamingEngine); ok {
		return streamer.StreamEdges(ctx, func(edge *Edge) error {
			if cachedIDs[edge.ID] || deletedIDs[edge.ID] {
				return nil
			}
			return fn(edge)
		})
	}

	// Fallback
	edges, err := ae.engine.AllEdges()
	if err != nil {
		return err
	}
	for _, edge := range edges {
		if cachedIDs[edge.ID] || deletedIDs[edge.ID] {
			continue
		}
		if err := fn(edge); err != nil {
			if err == ErrIterationStopped {
				return nil
			}
			return err
		}
	}
	return nil
}

// StreamNodeChunks implements StreamingEngine.StreamNodeChunks by using StreamNodes.
// We always use StreamNodes (not delegate) to properly merge cache + underlying engine.
func (ae *AsyncEngine) StreamNodeChunks(ctx context.Context, chunkSize int, fn func(nodes []*Node) error) error {
	// Always use our StreamNodes to properly handle cache + engine merging
	chunk := make([]*Node, 0, chunkSize)
	err := ae.StreamNodes(ctx, func(node *Node) error {
		chunk = append(chunk, node)
		if len(chunk) >= chunkSize {
			if err := fn(chunk); err != nil {
				return err
			}
			chunk = make([]*Node, 0, chunkSize)
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Final partial chunk
	if len(chunk) > 0 {
		return fn(chunk)
	}
	return nil
}

// DeleteByPrefix delegates to the underlying engine.
func (ae *AsyncEngine) DeleteByPrefix(prefix string) (nodesDeleted int64, edgesDeleted int64, err error) {
	// Flush any pending writes first
	ae.Flush()
	return ae.engine.DeleteByPrefix(prefix)
}

// Verify AsyncEngine implements Engine interface
var _ Engine = (*AsyncEngine)(nil)

// Verify AsyncEngine implements StreamingEngine interface
var _ StreamingEngine = (*AsyncEngine)(nil)
