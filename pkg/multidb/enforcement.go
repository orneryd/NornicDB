// Package multidb provides limit enforcement for multi-database support.
//
// This package implements resource limit checking and enforcement for NornicDB's
// multi-database feature. Limits are enforced at runtime to prevent any single
// database from consuming excessive resources.
//
// Key Features:
//   - Storage limits: MaxNodes, MaxEdges, MaxBytes (enforced with exact size calculation)
//   - Query limits: MaxQueryTime, MaxResults, MaxConcurrentQueries
//   - Connection limits: MaxConnections
//   - Rate limits: MaxQueriesPerSecond, MaxWritesPerSecond
//
// Example - Checking limits before creating a node:
//
//	checker, err := manager.GetLimitChecker("tenant_a")
//	if err != nil {
//		return err
//	}
//
//	node := &storage.Node{
//		ID:     storage.NodeID("user-123"),
//		Labels: []string{"User"},
//		Properties: map[string]any{"name": "Alice"},
//	}
//
//	// Check if creating this node would exceed limits
//	if err := checker.CheckStorageLimits("create_node", node, nil); err != nil {
//		return fmt.Errorf("cannot create node: %w", err)
//	}
//
//	// Safe to create - limits checked
//	storage.CreateNode(node)
//
// MaxBytes Enforcement:
//
// MaxBytes enforcement uses exact size calculation, not estimation. When checking
// limits for a create operation, the actual serialized size of the node/edge is
// calculated using gob encoding (matching the storage format). The current total
// storage size is tracked incrementally in DatabaseInfo, and the check verifies
// that currentSize + newEntitySize <= MaxBytes.
//
// Storage size is tracked incrementally:
//   - Initialized lazily on first access (calculated from all existing nodes/edges)
//   - Updated incrementally after successful creates/deletes
//   - No recalculation needed - O(1) limit checks
package multidb

import (
	"bytes"
	"context"
	"encoding/gob"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// LimitChecker provides an interface for checking resource limits.
// This allows storage engines to check limits without depending on DatabaseManager.
// It implements both storage.LimitChecker and storage.QueryLimitChecker.
type LimitChecker interface {
	// CheckStorageLimits checks if storage operations are within limits.
	// Returns error if limit would be exceeded.
	// For create operations, pass the node/edge being created to calculate exact size.
	CheckStorageLimits(operation string, node *storage.Node, edge *storage.Edge) error

	// CheckQueryLimits checks if query execution is allowed.
	// Returns error if limit would be exceeded.
	CheckQueryLimits(ctx context.Context) (context.Context, context.CancelFunc, error)

	// GetQueryLimits returns the query limits for this database.
	GetQueryLimits() interface{}

	// GetRateLimits returns the rate limits for this database.
	GetRateLimits() *RateLimits

	// CheckQueryRate checks if query rate limit is allowed.
	CheckQueryRate() error

	// CheckWriteRate checks if write rate limit is allowed.
	CheckWriteRate() error
}

// databaseLimitChecker implements LimitChecker for a specific database.
type databaseLimitChecker struct {
	manager      *DatabaseManager
	databaseName string
	limitsMu     sync.RWMutex
	limits       *Limits

	// Query tracking
	activeQueries   int
	activeQueriesMu sync.Mutex

	// Rate limiting
	queryRateLimiter *rateLimiter
	writeRateLimiter *rateLimiter

	// Cached size info (from DatabaseInfo)
	nodeSize int64
	edgeSize int64
}

// newDatabaseLimitChecker creates a limit checker for a specific database.
func newDatabaseLimitChecker(manager *DatabaseManager, databaseName string) (*databaseLimitChecker, error) {
	manager.mu.RLock()
	info, exists := manager.databases[databaseName]
	manager.mu.RUnlock()

	if !exists {
		return nil, ErrDatabaseNotFound
	}

	return newDatabaseLimitCheckerWithLimits(manager, databaseName, info.Limits), nil
}

func newDatabaseLimitCheckerWithLimits(manager *DatabaseManager, databaseName string, limits *Limits) *databaseLimitChecker {
	checker := &databaseLimitChecker{manager: manager, databaseName: databaseName}
	checker.updateLimits(limits)
	return checker
}

func (c *databaseLimitChecker) updateLimits(limits *Limits) {
	limits = cloneLimits(limits)
	c.limitsMu.Lock()
	defer c.limitsMu.Unlock()
	c.limits = limits
	c.queryRateLimiter = nil
	c.writeRateLimiter = nil
	if limits != nil {
		if limits.Rate.MaxQueriesPerSecond > 0 {
			c.queryRateLimiter = newRateLimiter(limits.Rate.MaxQueriesPerSecond)
		}
		if limits.Rate.MaxWritesPerSecond > 0 {
			c.writeRateLimiter = newRateLimiter(limits.Rate.MaxWritesPerSecond)
		}
	}
}

func (c *databaseLimitChecker) limitsSnapshot() *Limits {
	c.limitsMu.RLock()
	defer c.limitsMu.RUnlock()
	return cloneLimits(c.limits)
}

// CheckStorageLimits checks if storage operations are within limits.
//
// Parameters:
//   - operation: The operation type ("create_node" or "create_edge")
//   - node: The node being created (required for "create_node" operations when MaxBytes is set)
//   - edge: The edge being created (required for "create_edge" operations when MaxBytes is set)
//
// Returns an error if the operation would exceed any configured limits.
//
// For MaxBytes enforcement, the exact serialized size of the node/edge is calculated
// using gob encoding (matching the actual storage format). This ensures accurate
// limit checking without estimation.
//
// Example:
//
//	// Check before creating a node
//	node := &storage.Node{
//		ID:     storage.NodeID("user-123"),
//		Labels: []string{"User"},
//		Properties: map[string]any{"name": "Alice", "email": "alice@example.com"},
//	}
//	if err := checker.CheckStorageLimits("create_node", node, nil); err != nil {
//		// Error message: "storage limit exceeded: database 'tenant_a' would exceed
//		// max_bytes limit (current: 500 bytes, limit: 1024 bytes, new entity: 600 bytes)"
//		return err
//	}
//
//	// Safe to create
//	storage.CreateNode(node)
//
// Error Messages:
//
// All limit errors are clear and actionable:
//   - MaxNodes: "has reached max_nodes limit (1000/1000)"
//   - MaxEdges: "has reached max_edges limit (5000/5000)"
//   - MaxBytes: "would exceed max_bytes limit (current: 500 bytes, limit: 1024 bytes, new entity: 600 bytes)"
func (c *databaseLimitChecker) CheckStorageLimits(operation string, node *storage.Node, edge *storage.Edge) error {
	limits := c.limitsSnapshot()
	if limits == nil || limits.IsUnlimited() {
		return nil // No limits
	}

	// Get current usage
	storage, err := c.manager.GetStorage(c.databaseName)
	if err != nil {
		return err
	}

	nodeCount, err := storage.NodeCount()
	if err != nil {
		return localizedError(localization.MultidbStorageGetNodeCountFailed(err), err)
	}

	edgeCount, err := storage.EdgeCount()
	if err != nil {
		return localizedError(localization.MultidbStorageGetEdgeCountFailed(err), err)
	}

	// Check limits
	storageLimits := limits.Storage

	if operation == "create_node" {
		if storageLimits.MaxNodes > 0 && nodeCount >= storageLimits.MaxNodes {
			return localizedError(localization.MultidbMaxNodesReached(c.databaseName, nodeCount, storageLimits.MaxNodes), ErrStorageLimitExceeded)
		}
	}

	if operation == "create_edge" {
		if storageLimits.MaxEdges > 0 && edgeCount >= storageLimits.MaxEdges {
			return localizedError(localization.MultidbMaxEdgesReached(c.databaseName, edgeCount, storageLimits.MaxEdges), ErrStorageLimitExceeded)
		}
	}

	// Check MaxBytes limit if set
	if storageLimits.MaxBytes > 0 {
		currentSize, err := c.getCurrentStorageSize(storage)
		if err != nil {
			return localizedError(localization.MultidbStorageGetSizeFailed(err), err)
		}

		// Calculate EXACT size of new entity being created (serialized size)
		var newEntitySize int64
		if operation == "create_node" && node != nil {
			newEntitySize, err = calculateNodeSize(node)
			if err != nil {
				return localizedError(localization.MultidbStorageCalculateNodeSizeFailed(err), err)
			}
		} else if operation == "update_node" && node != nil {
			existing, getErr := storage.GetNode(node.ID)
			if getErr != nil {
				return nil
			}
			oldSize, sizeErr := calculateNodeSize(existing)
			if sizeErr != nil {
				return localizedError(localization.MultidbStorageCalculateNodeSizeFailed(sizeErr), sizeErr)
			}
			newSize, sizeErr := calculateNodeSize(node)
			if sizeErr != nil {
				return localizedError(localization.MultidbStorageCalculateNodeSizeFailed(sizeErr), sizeErr)
			}
			newEntitySize = newSize - oldSize
		} else if operation == "create_edge" && edge != nil {
			newEntitySize, err = calculateEdgeSize(edge)
			if err != nil {
				return localizedError(localization.MultidbStorageCalculateEdgeSizeFailed(err), err)
			}
		} else if operation == "update_edge" && edge != nil {
			existing, getErr := storage.GetEdge(edge.ID)
			if getErr != nil {
				return nil
			}
			oldSize, sizeErr := calculateEdgeSize(existing)
			if sizeErr != nil {
				return localizedError(localization.MultidbStorageCalculateEdgeSizeFailed(sizeErr), sizeErr)
			}
			newSize, sizeErr := calculateEdgeSize(edge)
			if sizeErr != nil {
				return localizedError(localization.MultidbStorageCalculateEdgeSizeFailed(sizeErr), sizeErr)
			}
			newEntitySize = newSize - oldSize
		} else {
			// Fallback: if node/edge not provided, we can't check MaxBytes
			// This shouldn't happen in practice, but handle gracefully
			return nil
		}

		// Check if adding the new entity would exceed the limit
		if currentSize+newEntitySize > storageLimits.MaxBytes {
			return localizedError(localization.MultidbMaxBytesExceeded(c.databaseName, currentSize, storageLimits.MaxBytes, newEntitySize), ErrStorageLimitExceeded)
		}
	}

	return nil
}

// getCurrentStorageSize gets the current storage size, initializing it if needed.
// Uses tracked size from DatabaseInfo for efficiency.
func (c *databaseLimitChecker) getCurrentStorageSize(engine storage.Engine) (int64, error) {
	// Get database info to access tracked size
	c.manager.mu.RLock()
	info, exists := c.manager.databases[c.databaseName]
	c.manager.mu.RUnlock()

	if !exists {
		return 0, ErrDatabaseNotFound
	}

	// Check if size has been initialized
	info.sizeMu.RLock()
	initialized := info.sizeInitialized
	currentSize := info.totalSize
	nodeSize := info.nodeSize
	edgeSize := info.edgeSize
	info.sizeMu.RUnlock()

	if !initialized {
		// Calculate size on first access (lazy initialization)
		calcNodeSize, calcEdgeSize, err := c.calculateCurrentStorageSize(engine)
		if err != nil {
			return 0, err
		}
		totalSize := calcNodeSize + calcEdgeSize

		// Update tracked size
		info.sizeMu.Lock()
		info.totalSize = totalSize
		info.nodeSize = calcNodeSize
		info.edgeSize = calcEdgeSize
		info.sizeInitialized = true
		info.sizeMu.Unlock()

		return totalSize, nil
	}

	// Store node/edge sizes for use in estimation
	c.nodeSize = nodeSize
	c.edgeSize = edgeSize

	return currentSize, nil
}

// calculateCurrentStorageSize calculates the total size of all nodes and edges in storage.
// Returns nodeSize, edgeSize, and error.
// This is only called once for lazy initialization, then size is tracked incrementally.
func (c *databaseLimitChecker) calculateCurrentStorageSize(engine storage.Engine) (int64, int64, error) {
	var nodeSize int64
	var edgeSize int64

	// Calculate size of all nodes
	nodes, err := engine.AllNodes()
	if err != nil {
		return 0, 0, localizedError(localization.MultidbStorageGetAllNodesFailed(err), err)
	}
	for _, node := range nodes {
		size, err := calculateNodeSize(node)
		if err != nil {
			// If calculation fails, use a conservative default
			size = 1024 // 1KB default
		}
		nodeSize += size
	}

	// Calculate size of all edges
	edges, err := engine.AllEdges()
	if err != nil {
		return 0, 0, localizedError(localization.MultidbStorageGetAllEdgesFailed(err), err)
	}
	for _, edge := range edges {
		size, err := calculateEdgeSize(edge)
		if err != nil {
			// If calculation fails, use a conservative default
			size = 512 // 512 bytes default
		}
		edgeSize += size
	}

	return nodeSize, edgeSize, nil
}

// calculateNodeSize calculates the ACTUAL serialized size of a node using gob encoding.
//
// This matches the exact storage format used by BadgerEngine - no estimation, this is the real size.
// The size returned is the exact number of bytes that will be stored on disk.
//
// Example:
//
//	node := &storage.Node{
//		ID:     storage.NodeID("user-123"),
//		Labels: []string{"User", "Person"},
//		Properties: map[string]any{
//			"name":  "Alice",
//			"email": "alice@example.com",
//			"age":   30,
//		},
//	}
//	size, err := calculateNodeSize(node)
//	// size is the exact byte count that will be stored
func calculateNodeSize(node *storage.Node) (int64, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(node); err != nil {
		return 0, localizedError(localization.MultidbStorageEncodeNodeFailed(err), err)
	}
	return int64(buf.Len()), nil
}

// calculateEdgeSize calculates the ACTUAL serialized size of an edge using gob encoding.
//
// This matches the exact storage format used by BadgerEngine - no estimation, this is the real size.
// The size returned is the exact number of bytes that will be stored on disk.
//
// Example:
//
//	edge := &storage.Edge{
//		ID:        storage.EdgeID("follows-1"),
//		StartNode: storage.NodeID("user-123"),
//		EndNode:   storage.NodeID("user-456"),
//		Type:      "FOLLOWS",
//		Properties: map[string]any{
//			"since": "2024-01-01",
//			"strength": "strong",
//		},
//	}
//	size, err := calculateEdgeSize(edge)
//	// size is the exact byte count that will be stored
func calculateEdgeSize(edge *storage.Edge) (int64, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(edge); err != nil {
		return 0, localizedError(localization.MultidbStorageEncodeEdgeFailed(err), err)
	}
	return int64(buf.Len()), nil
}

// CheckQueryLimits checks if query execution is allowed and returns a context with timeout.
func (c *databaseLimitChecker) CheckQueryLimits(ctx context.Context) (context.Context, context.CancelFunc, error) {
	limits := c.limitsSnapshot()
	if limits == nil || limits.IsUnlimited() {
		return ctx, func() {}, nil // No limits
	}

	queryLimits := limits.Query

	// Check concurrent query limit
	if queryLimits.MaxConcurrentQueries > 0 {
		c.activeQueriesMu.Lock()
		if c.activeQueries >= queryLimits.MaxConcurrentQueries {
			c.activeQueriesMu.Unlock()
			return nil, nil, localizedError(localization.MultidbMaxConcurrentQueriesReached(c.databaseName, c.activeQueries, queryLimits.MaxConcurrentQueries), ErrQueryLimitExceeded)
		}
		c.activeQueries++
		c.activeQueriesMu.Unlock()
	}

	// Create context with timeout if limit is set
	var cancel context.CancelFunc
	if queryLimits.MaxQueryTime > 0 {
		ctx, cancel = context.WithTimeout(ctx, queryLimits.MaxQueryTime)
	} else {
		cancel = func() {}
	}

	return ctx, func() {
		cancel()
		// Decrement active queries when done
		if queryLimits.MaxConcurrentQueries > 0 {
			c.activeQueriesMu.Lock()
			c.activeQueries--
			c.activeQueriesMu.Unlock()
		}
	}, nil
}

// GetQueryLimits returns the query limits for this database.
// Implements storage.QueryLimitChecker interface.
func (c *databaseLimitChecker) GetQueryLimits() interface{} {
	limits := c.limitsSnapshot()
	if limits == nil {
		return nil
	}
	return &limits.Query
}

// GetRateLimits returns the rate limits for this database.
func (c *databaseLimitChecker) GetRateLimits() *RateLimits {
	limits := c.limitsSnapshot()
	if limits == nil {
		return nil
	}
	return &limits.Rate
}

// CheckQueryRate checks if query rate limit is allowed.
func (c *databaseLimitChecker) CheckQueryRate() error {
	limits := c.limitsSnapshot()
	if c.queryRateLimiter == nil {
		return nil // No limit
	}
	if !c.queryRateLimiter.Allow() {
		return localizedError(localization.MultidbQueryRateExceeded(c.databaseName, limits.Rate.MaxQueriesPerSecond), ErrRateLimitExceeded)
	}
	return nil
}

// CheckWriteRate checks if write rate limit is allowed.
func (c *databaseLimitChecker) CheckWriteRate() error {
	limits := c.limitsSnapshot()
	if c.writeRateLimiter == nil {
		return nil // No limit
	}
	if !c.writeRateLimiter.Allow() {
		return localizedError(localization.MultidbWriteRateExceeded(c.databaseName, limits.Rate.MaxWritesPerSecond), ErrRateLimitExceeded)
	}
	return nil
}

// rateLimiter implements a simple token bucket rate limiter.
type rateLimiter struct {
	rate      int // Requests per second
	lastCheck time.Time
	tokens    int
	now       func() time.Time
	mu        sync.Mutex
}

// newRateLimiter creates a new rate limiter.
func newRateLimiter(rate int) *rateLimiter {
	return newRateLimiterWithClock(rate, time.Now)
}

func newRateLimiterWithClock(rate int, now func() time.Time) *rateLimiter {
	if now == nil {
		now = time.Now
	}
	return &rateLimiter{
		rate:      rate,
		lastCheck: now(),
		tokens:    rate, // Start with full bucket
		now:       now,
	}
}

// Allow checks if a request is allowed under the rate limit.
func (r *rateLimiter) Allow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := r.now()
	elapsed := now.Sub(r.lastCheck)

	// Add tokens based on elapsed time (1 token per second)
	if elapsed > 0 {
		tokensToAdd := int(elapsed.Seconds() * float64(r.rate))
		if tokensToAdd > 0 {
			r.tokens = min(r.tokens+tokensToAdd, r.rate) // Cap at rate
			r.lastCheck = now
		}
	}

	// Check if we have tokens
	if r.tokens > 0 {
		r.tokens--
		return true
	}

	return false
}

// min returns the minimum of two integers.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ConnectionTracker tracks active connections per database.
type ConnectionTracker struct {
	connections map[string]int // database -> connection count
	mu          sync.RWMutex
}

// NewConnectionTracker creates a new connection tracker.
func NewConnectionTracker() *ConnectionTracker {
	return &ConnectionTracker{
		connections: make(map[string]int),
	}
}

// CheckConnectionLimit checks if a new connection is allowed.
func (t *ConnectionTracker) CheckConnectionLimit(manager *DatabaseManager, databaseName string) error {
	manager.mu.RLock()
	info, exists := manager.databases[databaseName]
	manager.mu.RUnlock()

	if !exists {
		return ErrDatabaseNotFound
	}

	if info.Limits == nil || info.Limits.Connection.MaxConnections == 0 {
		return nil // No limit
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	current := t.connections[databaseName]
	if current >= info.Limits.Connection.MaxConnections {
		return localizedError(localization.MultidbMaxConnectionsReached(databaseName, current, info.Limits.Connection.MaxConnections), ErrConnectionLimitExceeded)
	}

	return nil
}

// TryIncrementConnection checks the connection limit and increments atomically.
//
// This avoids the race window of a "check-then-increment" pattern under
// concurrent connection attempts.
func (t *ConnectionTracker) TryIncrementConnection(manager *DatabaseManager, databaseName string) error {
	manager.mu.RLock()
	info, exists := manager.databases[databaseName]
	manager.mu.RUnlock()

	if !exists {
		return ErrDatabaseNotFound
	}

	limit := 0
	if info.Limits != nil {
		limit = info.Limits.Connection.MaxConnections
	}
	if limit == 0 {
		t.mu.Lock()
		t.connections[databaseName]++
		t.mu.Unlock()
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	current := t.connections[databaseName]
	if current >= limit {
		return localizedError(localization.MultidbMaxConnectionsReached(databaseName, current, limit), ErrConnectionLimitExceeded)
	}

	t.connections[databaseName]++
	return nil
}

// IncrementConnection increments the connection count for a database.
func (t *ConnectionTracker) IncrementConnection(databaseName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.connections[databaseName]++
}

// DecrementConnection decrements the connection count for a database.
func (t *ConnectionTracker) DecrementConnection(databaseName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.connections[databaseName] > 0 {
		t.connections[databaseName]--
	}
}

// GetConnectionCount returns the current connection count for a database.
func (t *ConnectionTracker) GetConnectionCount(databaseName string) int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.connections[databaseName]
}
