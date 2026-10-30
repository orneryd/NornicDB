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
// MaxBytes enforcement charges the MessagePack size of a canonical logical
// entity. This is a consistent quota measure, not the physical Badger size.
// The current total is tracked incrementally in DatabaseInfo, and the check
// verifies that currentSize + newEntitySize <= MaxBytes.
//
// Storage size is tracked incrementally:
//   - Initialized lazily on first access (calculated from all existing nodes/edges)
//   - Updated incrementally after successful creates/deletes
//   - No recalculation needed - O(1) limit checks
package multidb

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/vmihailenco/msgpack/v5"
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
	info         atomic.Pointer[DatabaseInfo]
	localPolicy  *databaseLimitPolicy

	// Query tracking
	activeQueries   int
	activeQueriesMu sync.Mutex
}

func (c *databaseLimitChecker) policy() *databaseLimitPolicy {
	if info := c.info.Load(); info != nil {
		if runtime := info.managed.Load(); runtime != nil {
			return runtime.policy
		}
	}
	return c.localPolicy
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
// MaxBytes charges the MessagePack size of the canonical logical node or
// relationship, consistently with the incremental usage counter.
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
	limits := c.policy().snapshot()
	if limits == nil || limits.IsUnlimited() {
		return nil // No limits
	}

	// Get current usage
	engine, err := c.manager.GetStorage(c.databaseName)
	if err != nil {
		return err
	}

	nodeCount, err := engine.NodeCount()
	if err != nil {
		return localizedError(localization.MultidbStorageGetNodeCountFailed(err), err)
	}

	edgeCount, err := engine.EdgeCount()
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
		currentSize, _, _, err := c.manager.readStorageSize(c.databaseName, engine, sizeForAdmission)
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
			existing, getErr := storage.GetStoredNode(engine, node.ID)
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
			existing, getErr := storage.GetStoredEdge(engine, edge.ID)
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

// calculateNodeSize measures the MessagePack logical byte quota. Persistent
// bodies have their own encoding, so this is not the physical Badger/WAL footprint.
// Empty and nil properties represent the same propertyless entity across
// point/stream readers and staged writes, so they must have one quota size.
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
//	// size is the logical byte quota charged for the entity
func calculateNodeSize(node *storage.Node) (int64, error) {
	if node != nil {
		canonical := *node
		{
			metadata := canonical.RuntimeMetadata()
			metadata.Database = "" // Query provenance does not consume stored-byte quota.
			canonical.SetRuntimeMetadata(metadata)
		}
		if len(canonical.Properties) == 0 {
			canonical.Properties = nil
		}
		node = &canonical
	}
	encoded, err := msgpack.Marshal(node)
	if err != nil {
		return 0, localizedError(localization.MultidbStorageEncodeNodeFailed(err), err)
	}
	return int64(len(encoded)), nil
}

// calculateEdgeSize uses the same canonical logical quota model as nodes.
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
//	// size is the logical byte quota charged for the entity
func calculateEdgeSize(edge *storage.Edge) (int64, error) {
	if edge != nil {
		canonical := *edge
		canonical.Database = ""
		if len(canonical.Properties) == 0 {
			canonical.Properties = nil
		}
		edge = &canonical
	}
	encoded, err := msgpack.Marshal(edge)
	if err != nil {
		return 0, localizedError(localization.MultidbStorageEncodeEdgeFailed(err), err)
	}
	return int64(len(encoded)), nil
}

// CheckQueryLimits checks if query execution is allowed and returns a context with timeout.
func (c *databaseLimitChecker) CheckQueryLimits(ctx context.Context) (context.Context, context.CancelFunc, error) {
	limits := c.policy().snapshot()
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

	// Create context with timeout if limit is set. Its cause names the
	// database's limit as the deadline that ended the statement.
	var cancel context.CancelFunc
	if queryLimits.MaxQueryTime > 0 {
		ctx, cancel = context.WithTimeoutCause(ctx, queryLimits.MaxQueryTime, nornicerrors.ErrDatabaseQueryTimeLimit)
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
	limits := c.policy().snapshot()
	if limits == nil {
		return nil
	}
	return &limits.Query
}

// GetRateLimits returns the rate limits for this database.
func (c *databaseLimitChecker) GetRateLimits() *RateLimits {
	limits := c.policy().snapshot()
	if limits == nil {
		return nil
	}
	return &limits.Rate
}

// CheckQueryRate checks if query rate limit is allowed.
func (c *databaseLimitChecker) CheckQueryRate() error { return c.checkRate(queryRate) }

// CheckWriteRate checks if write rate limit is allowed.
func (c *databaseLimitChecker) CheckWriteRate() error { return c.checkRate(writeRate) }

// checkRate uses one atomic policy snapshot and the namespace's shared bucket.
func (c *databaseLimitChecker) checkRate(kind rateKind) error {
	limits, limiter := c.policy().rateSnapshot(kind)
	if limiter == nil {
		return nil
	}
	if limiter.Allow() {
		return nil
	}
	switch kind {
	case queryRate:
		return localizedError(localization.MultidbQueryRateExceeded(c.databaseName, limits.Rate.MaxQueriesPerSecond), ErrRateLimitExceeded)
	default:
		return localizedError(localization.MultidbWriteRateExceeded(c.databaseName, limits.Rate.MaxWritesPerSecond), ErrRateLimitExceeded)
	}
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

	limits := info.limitsSnapshot()
	if limits == nil || limits.Connection.MaxConnections == 0 {
		return nil // No limit
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	current := t.connections[databaseName]
	if current >= limits.Connection.MaxConnections {
		return localizedError(localization.MultidbMaxConnectionsReached(databaseName, current, limits.Connection.MaxConnections), ErrConnectionLimitExceeded)
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
	if limits := info.limitsSnapshot(); limits != nil {
		limit = limits.Connection.MaxConnections
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
