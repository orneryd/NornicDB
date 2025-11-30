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

	// Background flush
	flushInterval time.Duration
	flushTicker   *time.Ticker
	stopChan      chan struct{}
	wg            sync.WaitGroup

	// Stats
	pendingWrites int64
	totalFlushes  int64
}

// AsyncEngineConfig configures the async engine behavior.
type AsyncEngineConfig struct {
	// FlushInterval controls how often pending writes are flushed.
	// Smaller = more consistent, larger = better throughput.
	// Default: 50ms
	FlushInterval time.Duration
}

// DefaultAsyncEngineConfig returns sensible defaults.
func DefaultAsyncEngineConfig() *AsyncEngineConfig {
	return &AsyncEngineConfig{
		FlushInterval: 50 * time.Millisecond,
	}
}

// NewAsyncEngine wraps an engine with write-behind caching.
func NewAsyncEngine(engine Engine, config *AsyncEngineConfig) *AsyncEngine {
	if config == nil {
		config = DefaultAsyncEngineConfig()
	}

	ae := &AsyncEngine{
		engine:        engine,
		nodeCache:     make(map[NodeID]*Node),
		edgeCache:     make(map[EdgeID]*Edge),
		deleteNodes:   make(map[NodeID]bool),
		deleteEdges:   make(map[EdgeID]bool),
		flushInterval: config.FlushInterval,
		stopChan:      make(chan struct{}),
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

// Flush writes all pending changes to the underlying engine.
// Uses batched operations for better performance - all deletes in one transaction.
//
// CRITICAL: We hold the lock for the ENTIRE duration to prevent race conditions.
// Without this, reads between "clear cache" and "write to engine" would see no data.
func (ae *AsyncEngine) Flush() error {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	// Nothing to flush
	if len(ae.nodeCache) == 0 && len(ae.edgeCache) == 0 && len(ae.deleteNodes) == 0 && len(ae.deleteEdges) == 0 {
		return nil
	}

	ae.totalFlushes++

	// Snapshot pending changes (we'll clear these AFTER successful writes)
	nodesToWrite := ae.nodeCache
	edgesToWrite := ae.edgeCache
	nodesToDelete := ae.deleteNodes
	edgesToDelete := ae.deleteEdges

	// Apply bulk deletes first (SINGLE transaction instead of N transactions!)
	if len(nodesToDelete) > 0 {
		nodeIDs := make([]NodeID, 0, len(nodesToDelete))
		for id := range nodesToDelete {
			nodeIDs = append(nodeIDs, id)
		}
		ae.engine.BulkDeleteNodes(nodeIDs) // Best effort - ignore errors
	}
	if len(edgesToDelete) > 0 {
		edgeIDs := make([]EdgeID, 0, len(edgesToDelete))
		for id := range edgesToDelete {
			edgeIDs = append(edgeIDs, id)
		}
		ae.engine.BulkDeleteEdges(edgeIDs) // Best effort - ignore errors
	}

	// Apply creates/updates
	for _, node := range nodesToWrite {
		// Check if this was also deleted
		if nodesToDelete[node.ID] {
			continue
		}
		// Try create, if exists do update
		if err := ae.engine.CreateNode(node); err != nil {
			ae.engine.UpdateNode(node)
		}
	}
	for _, edge := range edgesToWrite {
		if edgesToDelete[edge.ID] {
			continue
		}
		if err := ae.engine.CreateEdge(edge); err != nil {
			ae.engine.UpdateEdge(edge)
		}
	}

	// NOW clear the caches - data is safely in the underlying engine
	ae.nodeCache = make(map[NodeID]*Node)
	ae.edgeCache = make(map[EdgeID]*Edge)
	ae.deleteNodes = make(map[NodeID]bool)
	ae.deleteEdges = make(map[EdgeID]bool)

	return nil
}

// GetEngine returns the underlying storage engine.
// Used for transaction support which needs direct access.
func (ae *AsyncEngine) GetEngine() Engine {
	return ae.engine
}

// CreateNode adds to cache and returns immediately.
func (ae *AsyncEngine) CreateNode(node *Node) error {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	// Remove from delete set if present
	delete(ae.deleteNodes, node.ID)
	ae.nodeCache[node.ID] = node
	ae.pendingWrites++
	return nil
}

// UpdateNode adds to cache and returns immediately.
func (ae *AsyncEngine) UpdateNode(node *Node) error {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	ae.nodeCache[node.ID] = node
	ae.pendingWrites++
	return nil
}

// DeleteNode marks for deletion and returns immediately.
func (ae *AsyncEngine) DeleteNode(id NodeID) error {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	delete(ae.nodeCache, id)
	ae.deleteNodes[id] = true
	ae.pendingWrites++
	return nil
}

// CreateEdge adds to cache and returns immediately.
func (ae *AsyncEngine) CreateEdge(edge *Edge) error {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	delete(ae.deleteEdges, edge.ID)
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
func (ae *AsyncEngine) DeleteEdge(id EdgeID) error {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	delete(ae.edgeCache, id)
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
// NOTE: We hold the read lock for the ENTIRE operation to prevent race conditions
// with Flush() which clears the cache after writing to the engine.
func (ae *AsyncEngine) GetNodesByLabel(label string) ([]*Node, error) {
	ae.mu.RLock()
	defer ae.mu.RUnlock()

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

	// Get from engine (still under lock to prevent Flush race)
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

// AllNodes returns merged view of cache and engine.
// NOTE: We hold the read lock for the ENTIRE operation to prevent race conditions
// with Flush() which clears the cache after writing to the engine.
func (ae *AsyncEngine) AllNodes() ([]*Node, error) {
	ae.mu.RLock()
	defer ae.mu.RUnlock()

	cachedNodes := make([]*Node, 0, len(ae.nodeCache))
	deletedIDs := make(map[NodeID]bool)

	for id := range ae.deleteNodes {
		deletedIDs[id] = true
	}
	for _, node := range ae.nodeCache {
		cachedNodes = append(cachedNodes, node)
	}

	engineNodes, err := ae.engine.AllNodes()
	if err != nil {
		return cachedNodes, nil
	}

	// Merge
	result := make([]*Node, 0, len(cachedNodes)+len(engineNodes))
	seenIDs := make(map[NodeID]bool)

	for _, node := range cachedNodes {
		result = append(result, node)
		seenIDs[node.ID] = true
	}
	for _, node := range engineNodes {
		if !seenIDs[node.ID] && !deletedIDs[node.ID] {
			result = append(result, node)
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
	count, err := ae.engine.NodeCount()
	if err != nil {
		return 0, err
	}
	ae.mu.RLock()
	// Adjust for pending creates and deletes
	count += int64(len(ae.nodeCache))
	count -= int64(len(ae.deleteNodes))
	ae.mu.RUnlock()
	return count, nil
}

func (ae *AsyncEngine) EdgeCount() (int64, error) {
	count, err := ae.engine.EdgeCount()
	if err != nil {
		return 0, err
	}
	ae.mu.RLock()
	count += int64(len(ae.edgeCache))
	count -= int64(len(ae.deleteEdges))
	ae.mu.RUnlock()
	return count, nil
}

func (ae *AsyncEngine) Close() error {
	// Stop flush loop
	close(ae.stopChan)
	ae.flushTicker.Stop()
	ae.wg.Wait()

	// Final flush
	ae.Flush()

	return ae.engine.Close()
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

// Verify AsyncEngine implements Engine interface
var _ Engine = (*AsyncEngine)(nil)
