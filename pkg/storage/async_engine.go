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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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
	// Per-node inverted indexes over edgeCache so GetOutgoingEdges /
	// GetIncomingEdges run in O(degree) instead of O(len(edgeCache)).
	// Maintained alongside every edgeCache mutation (Create/Update/Delete/
	// Bulk*/flush eviction). Without these, BFS-style traversals over the
	// async cache scaled with total cached edges per frontier expansion,
	// producing a roughly constant per-traversal floor regardless of true
	// path length.
	cacheEdgesByStart map[NodeID]map[EdgeID]struct{}
	cacheEdgesByEnd   map[NodeID]map[EdgeID]struct{}
	mu                sync.RWMutex

	// Event callbacks (optional): used to keep external services in sync when
	// operations are satisfied purely from the async cache (i.e., no inner engine
	// callback will fire because the data never hit persistent storage).
	onNodeCreated NodeEventCallback
	onNodeUpdated NodeEventCallback
	onNodeDeleted NodeDeleteCallback
	onEdgeCreated EdgeEventCallback
	onEdgeUpdated EdgeEventCallback
	onEdgeDeleted EdgeDeleteCallback
	callbackMu    sync.RWMutex

	// In-flight tracking: nodes/edges being written but not yet cleared from cache
	// This prevents double-counting in NodeCount/EdgeCount during flush
	inFlightNodes map[NodeID]bool
	inFlightEdges map[EdgeID]bool

	// Updates tracking: nodes/edges in cache that are UPDATES (not creates)
	// These exist in underlying engine and shouldn't be counted as pending creates
	updateNodes map[NodeID]bool
	updateEdges map[EdgeID]bool
	// Baseline snapshots captured when a node update is first queued.
	// Used to detect stale async updates and rebase onto newer committed state.
	nodeUpdateBaseline map[NodeID]*Node

	// Label index for fast lookups - maps normalized label to node IDs
	labelIndex map[string]map[NodeID]bool

	// Background flush
	flushInterval    time.Duration
	minFlushInterval time.Duration
	maxFlushInterval time.Duration
	targetFlushSize  int
	adaptiveFlush    bool
	flushTicker      *time.Ticker
	stopChan         chan struct{}
	wg               sync.WaitGroup
	maxNodeCacheSize int
	maxEdgeCacheSize int

	lastFlush time.Time

	// Stats
	pendingWrites int64
	totalFlushes  int64

	// Flush mutex prevents concurrent flushes (race condition fix)
	flushMu sync.RWMutex

	// log is the structured *slog.Logger for AsyncEngine emissions, tagged
	// with component=storage, engine=async at construction. D-01 logger DI;
	// D-06 single-allocation flushLog derives from this in flushLoop. Discard
	// fallback installed in NewAsyncEngine when config.Logger == nil.
	log *slog.Logger

	// spanLinks collects span contexts from request-scoped writes so the
	// flush span (TRC-23) can link back to the originating requests.
	spanLinks   []trace.Link
	spanLinksMu sync.Mutex
}

// HoldFlush acquires a shared flush guard and returns a release function.
//
// While held, background or manual Flush calls block on flushMu.Lock(), which is
// useful when a higher-level transaction needs a stable committed view for its
// full lifetime. Regular async writes still queue into memory and will flush once
// the returned release function is called.
func (ae *AsyncEngine) HoldFlush() func() {
	if ae == nil {
		return func() {}
	}
	ae.flushMu.RLock()
	return func() {
		ae.flushMu.RUnlock()
	}
}

// AsyncEngineConfig configures the async engine behavior.
type AsyncEngineConfig struct {
	// FlushInterval controls how often pending writes are flushed.
	// Smaller = more consistent, larger = better throughput.
	// Default: 50ms
	FlushInterval time.Duration

	// AdaptiveFlush enables volume-based flush timing.
	// When enabled, the flush loop ticks at MinFlushInterval and only
	// flushes when the adaptive interval has elapsed.
	AdaptiveFlush bool

	// MinFlushInterval is the shortest interval between flushes when adaptive flush is enabled.
	// Default: 10ms
	MinFlushInterval time.Duration

	// MaxFlushInterval is the longest interval between flushes when adaptive flush is enabled.
	// Default: 200ms
	MaxFlushInterval time.Duration

	// TargetFlushSize is the pending write count at which we reach MaxFlushInterval.
	// Smaller batches flush more frequently; larger batches flush less frequently.
	// Default: 1000 pending writes
	TargetFlushSize int

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

	// Logger is the structured *slog.Logger threaded into the AsyncEngine.
	// D-01 logger DI: optional; nil falls back to a discard handler at ctor
	// entry per D-01a so existing callers (tests, scripts) compile unchanged.
	// D-06: the flush goroutine derives a single-allocation child logger from
	// this field at goroutine start.
	Logger *slog.Logger
}

// NewAsyncEngine wraps an engine with write-behind caching.
func NewAsyncEngine(engine Engine, config *AsyncEngineConfig) *AsyncEngine {
	if config == nil {
		config = DefaultAsyncEngineConfig()
	}
	if config.MinFlushInterval <= 0 {
		config.MinFlushInterval = 10 * time.Millisecond
	}
	if config.MaxFlushInterval <= 0 {
		config.MaxFlushInterval = 200 * time.Millisecond
	}
	if config.TargetFlushSize <= 0 {
		config.TargetFlushSize = 1000
	}
	if config.MinFlushInterval > config.MaxFlushInterval {
		config.MinFlushInterval = config.MaxFlushInterval
	}
	// D-01a discard fallback: callers that do not supply a structured logger
	// get a discard handler so LOG-01 holds even on uninstrumented paths
	// (tests, scripts/perf_direct, embedded usage).
	if config.Logger == nil {
		config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	ae := &AsyncEngine{
		engine:             engine,
		nodeCache:          make(map[NodeID]*Node),
		edgeCache:          make(map[EdgeID]*Edge),
		cacheEdgesByStart:  make(map[NodeID]map[EdgeID]struct{}),
		cacheEdgesByEnd:    make(map[NodeID]map[EdgeID]struct{}),
		deleteNodes:        make(map[NodeID]bool),
		deleteEdges:        make(map[EdgeID]bool),
		inFlightNodes:      make(map[NodeID]bool),
		inFlightEdges:      make(map[EdgeID]bool),
		updateNodes:        make(map[NodeID]bool),
		updateEdges:        make(map[EdgeID]bool),
		nodeUpdateBaseline: make(map[NodeID]*Node),
		labelIndex:         make(map[string]map[NodeID]bool),
		flushInterval:      config.FlushInterval,
		minFlushInterval:   config.MinFlushInterval,
		maxFlushInterval:   config.MaxFlushInterval,
		targetFlushSize:    config.TargetFlushSize,
		adaptiveFlush:      config.AdaptiveFlush,
		maxNodeCacheSize:   config.MaxNodeCacheSize,
		maxEdgeCacheSize:   config.MaxEdgeCacheSize,
		stopChan:           make(chan struct{}),
		lastFlush:          time.Now(),
		log:                config.Logger.With("component", "storage", "engine", "async"),
	}

	// Start background flush goroutine
	if ae.adaptiveFlush {
		ae.flushTicker = time.NewTicker(config.MinFlushInterval)
	} else {
		ae.flushTicker = time.NewTicker(config.FlushInterval)
	}
	ae.wg.Add(1)
	go ae.flushLoop()

	return ae
}

// GetInnerEngine returns the wrapped storage engine.
func (e *AsyncEngine) GetInnerEngine() Engine {
	if e == nil {
		return nil
	}
	return e.engine
}

// indexCacheEdgeLocked records edge in the per-node inverted indexes.
// Caller must hold ae.mu.Lock(). Idempotent.
func (ae *AsyncEngine) indexCacheEdgeLocked(edge *Edge) {
	if edge == nil {
		return
	}
	startSet, ok := ae.cacheEdgesByStart[edge.StartNode]
	if !ok {
		startSet = make(map[EdgeID]struct{})
		ae.cacheEdgesByStart[edge.StartNode] = startSet
	}
	startSet[edge.ID] = struct{}{}
	endSet, ok := ae.cacheEdgesByEnd[edge.EndNode]
	if !ok {
		endSet = make(map[EdgeID]struct{})
		ae.cacheEdgesByEnd[edge.EndNode] = endSet
	}
	endSet[edge.ID] = struct{}{}
}

// unindexCacheEdgeLocked removes id from the per-node inverted indexes
// for the endpoints of prev. Caller must hold ae.mu.Lock(). Safe if prev is nil.
func (ae *AsyncEngine) unindexCacheEdgeLocked(prev *Edge) {
	if prev == nil {
		return
	}
	if startSet, ok := ae.cacheEdgesByStart[prev.StartNode]; ok {
		delete(startSet, prev.ID)
		if len(startSet) == 0 {
			delete(ae.cacheEdgesByStart, prev.StartNode)
		}
	}
	if endSet, ok := ae.cacheEdgesByEnd[prev.EndNode]; ok {
		delete(endSet, prev.ID)
		if len(endSet) == 0 {
			delete(ae.cacheEdgesByEnd, prev.EndNode)
		}
	}
}

// putCacheEdgeLocked inserts or replaces edge in edgeCache, keeping the
// per-node inverted indexes in sync. Caller must hold ae.mu.Lock().
func (ae *AsyncEngine) putCacheEdgeLocked(edge *Edge) {
	if edge == nil {
		return
	}
	if prev, ok := ae.edgeCache[edge.ID]; ok && prev != nil {
		// Endpoints can change on update; rebuild only if they shifted.
		if prev.StartNode != edge.StartNode || prev.EndNode != edge.EndNode {
			ae.unindexCacheEdgeLocked(prev)
			ae.indexCacheEdgeLocked(edge)
		}
	} else {
		ae.indexCacheEdgeLocked(edge)
	}
	ae.edgeCache[edge.ID] = edge
}

// deleteCacheEdgeLocked removes id from edgeCache and the inverted indexes.
// Caller must hold ae.mu.Lock().
func (ae *AsyncEngine) deleteCacheEdgeLocked(id EdgeID) {
	if prev, ok := ae.edgeCache[id]; ok {
		ae.unindexCacheEdgeLocked(prev)
		delete(ae.edgeCache, id)
	}
}

// flushLoop periodically flushes pending writes to the underlying engine.
//
// D-06 single-allocation rule: flushLog is derived ONCE at goroutine start
// from ae.log via .With("subsystem","async_flush","operation","flush"). Per-
// flush emissions reuse flushLog with no further .With(...) allocations on
// the hot path. This pre-bind pattern matches Phase 1's pre-bound metric-
// counter rule for hot paths in pkg/observability.
func (ae *AsyncEngine) flushLoop() {
	defer ae.wg.Done()
	flushLog := ae.log.With("subsystem", "async_flush", "operation", "flush")

	for {
		select {
		case <-ae.flushTicker.C:
			if ae.adaptiveFlush {
				pending := ae.pendingWriteCount()
				if pending == 0 {
					continue
				}
				interval := ae.adaptiveFlushInterval(pending)
				ae.flushMu.RLock()
				lastFlush := ae.lastFlush
				ae.flushMu.RUnlock()
				if time.Since(lastFlush) < interval {
					continue
				}
			}
			if err := ae.Flush(); err != nil {
				// Don't log storage closed during shutdown (engine closed by teardown order or race with stopChan).
				if !errors.Is(err, ErrStorageClosed) && !strings.Contains(err.Error(), ErrStorageClosed.Error()) {
					flushLog.Warn("async flush failed", slog.Any("error", err))
				}
			}
		case <-ae.stopChan:
			// Final flush on shutdown
			ae.Flush()
			return
		}
	}
}

// FlushResult tracks the outcome of a flush operation for observability.
type FlushResult struct {
	NodesWritten     int
	NodesFailed      int
	EdgesWritten     int
	EdgesFailed      int
	NodesDeleted     int
	EdgesDeleted     int
	DeletesFailed    int
	FailedNodeIDs    []NodeID // IDs that failed - still in cache for retry
	FailedEdgeIDs    []EdgeID // IDs that failed - still in cache for retry
	FirstNodeError   string
	FirstEdgeError   string
	FirstDeleteError string
}

// HasErrors returns true if any flush operations failed.
func (r FlushResult) HasErrors() bool {
	return r.NodesFailed > 0 || r.EdgesFailed > 0 || r.DeletesFailed > 0
}

// isStorageClosedOnly returns true if the flush failed only due to storage closed (expected during shutdown).
func (r FlushResult) isStorageClosedOnly() bool {
	if !r.HasErrors() {
		return false
	}
	closed := ErrStorageClosed.Error()
	first := r.FirstNodeError
	if first == "" {
		first = r.FirstEdgeError
	}
	if first == "" {
		first = r.FirstDeleteError
	}
	return first == closed || strings.Contains(first, closed)
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
//
// Fast path: if there's nothing pending, avoid taking the exclusive
// flushMu.Lock() entirely. In seed-heavy workloads the implicit-txn
// path flushes the cache inline before starting each transaction, so by
// the time the background ticker fires the cache is almost always
// empty. Skipping the lock here keeps transaction-path HoldFlush
// RLockers from queueing behind a no-op write lock acquisition.
func (ae *AsyncEngine) Flush() error {
	if ae.pendingWriteCount() == 0 {
		return nil
	}
	ae.flushMu.Lock()
	defer ae.flushMu.Unlock()
	// Re-check inside the lock: another goroutine may have flushed
	// between our initial check and taking the write lock.
	if ae.pendingWriteCount() == 0 {
		return nil
	}

	result := ae.FlushWithResult()
	if result.HasErrors() {
		details := ""
		if result.FirstNodeError != "" {
			details = result.FirstNodeError
		} else if result.FirstEdgeError != "" {
			details = result.FirstEdgeError
		} else if result.FirstDeleteError != "" {
			details = result.FirstDeleteError
		}
		if details != "" {
			return fmt.Errorf("flush incomplete: %d nodes failed, %d edges failed, %d deletes failed (%s)",
				result.NodesFailed, result.EdgesFailed, result.DeletesFailed, details)
		}
		return fmt.Errorf("flush incomplete: %d nodes failed, %d edges failed, %d deletes failed",
			result.NodesFailed, result.EdgesFailed, result.DeletesFailed)
	}
	if result.NodesWritten+result.EdgesWritten+result.NodesDeleted+result.EdgesDeleted > 0 {
		ae.lastFlush = time.Now()
	}
	return nil
}

// AddSpanLink records a request span context so the next flush span can link
// back to it (TRC-23). Called by TracedEngine on each write operation.
func (ae *AsyncEngine) AddSpanLink(sc trace.SpanContext) {
	if !sc.IsValid() {
		return
	}
	ae.spanLinksMu.Lock()
	if len(ae.spanLinks) < 32 {
		ae.spanLinks = append(ae.spanLinks, trace.Link{SpanContext: sc})
	}
	ae.spanLinksMu.Unlock()
}

// drainSpanLinks returns and clears accumulated span links for the flush span.
func (ae *AsyncEngine) drainSpanLinks() []trace.Link {
	ae.spanLinksMu.Lock()
	links := ae.spanLinks
	ae.spanLinks = nil
	ae.spanLinksMu.Unlock()
	return links
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

	// TRC-23: start a flush span linked to the originating request spans.
	// The `kind` attribute ("batch") satisfies the TRC-17 universal contract
	// that every nornicdb.storage.* span carries a `kind` attr — otherwise a
	// flush span leaking into a concurrent TracerProvider (e.g. a later test
	// that swaps the global provider) would break downstream assertions.
	links := ae.drainSpanLinks()
	_, flushSpan := otel.Tracer("nornicdb/storage").Start(
		context.Background(), "nornicdb.storage.flush",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithLinks(links...),
		trace.WithAttributes(attribute.String("kind", "batch")),
	)
	defer func() {
		flushSpan.SetAttributes(
			attribute.Int("nodes_written", result.NodesWritten),
			attribute.Int("edges_written", result.EdgesWritten),
			attribute.Int("nodes_deleted", result.NodesDeleted),
			attribute.Int("edges_deleted", result.EdgesDeleted),
		)
		flushSpan.End()
	}()

	ae.totalFlushes++

	// Snapshot pending changes
	nodesToWrite := make(map[NodeID]*Node, len(ae.nodeCache))
	for k, v := range ae.nodeCache {
		nodesToWrite[k] = v
	}
	nodeBaselines := make(map[NodeID]*Node, len(nodesToWrite))
	for id := range nodesToWrite {
		if baseline, ok := ae.nodeUpdateBaseline[id]; ok {
			nodeBaselines[id] = CopyNode(baseline)
		}
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
			if result.FirstDeleteError == "" {
				result.FirstDeleteError = err.Error()
			}
			// Bulk failed - try individual deletes
			for _, id := range nodeIDs {
				if err := ae.engine.DeleteNode(id); err != nil {
					result.DeletesFailed++
					if result.FirstDeleteError == "" {
						result.FirstDeleteError = err.Error()
					}
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
			if result.FirstDeleteError == "" {
				result.FirstDeleteError = err.Error()
			}
			// Bulk failed - try individual deletes
			for _, id := range edgeIDs {
				if err := ae.engine.DeleteEdge(id); err != nil {
					result.DeletesFailed++
					if result.FirstDeleteError == "" {
						result.FirstDeleteError = err.Error()
					}
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

	// Apply creates/updates. Path selection:
	//
	//   - Fresh creates (no rebase baseline) go through BulkCreateNodes in a
	//     single Badger transaction. This is the bulk-seed hot path — N
	//     per-node UpdateNode calls would each spawn their own Badger
	//     transaction + WAL sync, which is the slow O(N × fsync) behaviour
	//     that made 16 000-row seeds take >20s.
	//   - Queued updates (baseline != nil) go through flushNodeWithRebase
	//     so the rebase-on-concurrent-change machinery still runs per node.
	//     That path is correctness-critical for the update-while-write race;
	//     we cannot batch it without recomputing rebase per group.
	// Apply creates/updates using UpdateNode (upsert) for each node
	// This handles both new nodes and updates to existing nodes
	if len(nodesToWrite) > 0 {
		for _, node := range nodesToWrite {
			if !nodesToDelete[node.ID] {
				// UpdateNode now has upsert behavior - creates if not exists, updates if exists.
				// If this node was queued as an update, detect staleness and rebase before write.
				if err := ae.flushNodeWithRebase(node, nodeBaselines[node.ID]); err != nil {
					// CRITICAL FIX: Track failed node - DON'T remove from cache
					result.NodesFailed++
					result.FailedNodeIDs = append(result.FailedNodeIDs, node.ID)
					if result.FirstNodeError == "" {
						result.FirstNodeError = err.Error()
					}
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
				if result.FirstEdgeError == "" {
					result.FirstEdgeError = err.Error()
				}
				// Bulk failed - try individual creates
				for _, edge := range edges {
					if err := ae.engine.CreateEdge(edge); err != nil {
						// Try update if create fails (might already exist)
						if err := ae.engine.UpdateEdge(edge); err != nil {
							result.EdgesFailed++
							result.FailedEdgeIDs = append(result.FailedEdgeIDs, edge.ID)
							if result.FirstEdgeError == "" {
								result.FirstEdgeError = err.Error()
							}
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
			delete(ae.nodeUpdateBaseline, id)
			// Drop labelIndex entries for this node — without this, the
			// per-label sets grow unbounded across flushes, and entries
			// reference nodes that are no longer in nodeCache. That makes
			// every label-scoped read pay an O(flushed_history) cost
			// dereferencing stale IDs back through nodeCache to find nils.
			ae.removeNodeIDFromLabelIndexLocked(id)
		}
		// Always clear in-flight marker for this batch (success or fail)
		delete(ae.inFlightNodes, id)
	}
	for id := range edgesToWrite {
		if successfulEdgeWrites[id] && ae.edgeCache[id] == edgesToWrite[id] {
			ae.deleteCacheEdgeLocked(id)
			delete(ae.updateEdges, id) // Clear update flag
		}
		// Always clear in-flight marker for this batch (success or fail)
		delete(ae.inFlightEdges, id)
	}
	for id := range nodesToDelete {
		if successfulNodeDeletes[id] && ae.deleteNodes[id] {
			delete(ae.deleteNodes, id)
			delete(ae.nodeUpdateBaseline, id)
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

const asyncNodeRebaseMaxAttempts = 3

func (ae *AsyncEngine) flushNodeWithRebase(pending, baseline *Node) error {
	if pending == nil {
		return ErrInvalidData
	}
	if baseline == nil {
		return ae.engine.UpdateNode(pending)
	}

	candidate := CopyNode(pending)
	baselineSnapshot := CopyNode(baseline)

	for attempt := 0; attempt < asyncNodeRebaseMaxAttempts; attempt++ {
		latest, err := ae.engine.GetNode(candidate.ID)
		if err == nil && !nodesEquivalentForAsyncRebase(latest, baselineSnapshot) {
			// Underlying row changed since async queue accepted this update.
			// Rebase async intent onto latest committed state.
			candidate = rebaseNodeUpdate(baselineSnapshot, pending, latest)
			baselineSnapshot = CopyNode(latest)
		}

		if err := ae.engine.UpdateNode(candidate); err != nil {
			if attempt == asyncNodeRebaseMaxAttempts-1 {
				return err
			}
			continue
		}
		return nil
	}

	return fmt.Errorf("failed to apply async node update for %s after %d attempts", pending.ID, asyncNodeRebaseMaxAttempts)
}

func nodesEquivalentForAsyncRebase(a, b *Node) bool {
	if a == nil || b == nil {
		return a == b
	}
	return reflect.DeepEqual(a.Labels, b.Labels) &&
		reflect.DeepEqual(a.Properties, b.Properties) &&
		reflect.DeepEqual(a.NamedEmbeddings, b.NamedEmbeddings) &&
		reflect.DeepEqual(a.ChunkEmbeddings, b.ChunkEmbeddings) &&
		reflect.DeepEqual(a.EmbedMeta, b.EmbedMeta)
}

func rebaseNodeUpdate(base, pending, latest *Node) *Node {
	if pending == nil {
		return CopyNode(latest)
	}
	if latest == nil || base == nil {
		return CopyNode(pending)
	}

	rebased := CopyNode(latest)
	if rebased.Properties == nil {
		rebased.Properties = make(map[string]any)
	}

	if !reflect.DeepEqual(base.Labels, pending.Labels) {
		rebased.Labels = append([]string(nil), pending.Labels...)
	}

	baseProps := base.Properties
	if baseProps == nil {
		baseProps = map[string]any{}
	}
	pendingProps := pending.Properties
	if pendingProps == nil {
		pendingProps = map[string]any{}
	}

	for key, pendingValue := range pendingProps {
		baseValue, existedInBase := baseProps[key]
		if !existedInBase || !reflect.DeepEqual(baseValue, pendingValue) {
			rebased.Properties[key] = pendingValue
		}
	}
	for key := range baseProps {
		if _, existsInPending := pendingProps[key]; !existsInPending {
			delete(rebased.Properties, key)
		}
	}

	if !reflect.DeepEqual(base.NamedEmbeddings, pending.NamedEmbeddings) {
		rebased.NamedEmbeddings = CopyNode(pending).NamedEmbeddings
	}
	if !reflect.DeepEqual(base.ChunkEmbeddings, pending.ChunkEmbeddings) {
		rebased.ChunkEmbeddings = CopyNode(pending).ChunkEmbeddings
	}
	if !reflect.DeepEqual(base.EmbedMeta, pending.EmbedMeta) {
		rebased.EmbedMeta = CopyNode(pending).EmbedMeta
	}

	return rebased
}

// GetEngine returns the underlying storage engine.
// Used for transaction support which needs direct access.
func (ae *AsyncEngine) GetEngine() Engine {
	return ae.engine
}

func (ae *AsyncEngine) addNodeToLabelIndexLocked(node *Node) {
	if node == nil {
		return
	}
	for _, label := range node.Labels {
		normalLabel := strings.ToLower(label)
		if ae.labelIndex[normalLabel] == nil {
			ae.labelIndex[normalLabel] = make(map[NodeID]bool)
		}
		ae.labelIndex[normalLabel][node.ID] = true
	}
}

func (ae *AsyncEngine) removeNodeIDFromLabelIndexLocked(id NodeID) {
	for label, ids := range ae.labelIndex {
		delete(ids, id)
		if len(ids) == 0 {
			delete(ae.labelIndex, label)
		}
	}
}

func (ae *AsyncEngine) syncNodeLabelIndexLocked(node *Node) {
	if node == nil {
		return
	}
	ae.removeNodeIDFromLabelIndexLocked(node.ID)
	ae.addNodeToLabelIndexLocked(node)
}

// CreateNode adds to cache and returns immediately.
func (ae *AsyncEngine) CreateNode(node *Node) (NodeID, error) {
	if node == nil {
		return "", ErrInvalidData
	}
	if err := validatePropertiesForStorage(node.Properties); err != nil {
		return "", err
	}
	if err := ae.validateNodeConstraints(node); err != nil {
		return "", err
	}

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
	delete(ae.nodeUpdateBaseline, node.ID)
	ae.syncNodeLabelIndexLocked(node)

	ae.pendingWrites++
	return node.ID, nil
}

// UpdateNode adds to cache and returns immediately.
func (ae *AsyncEngine) UpdateNode(node *Node) error {
	if node == nil {
		return ErrInvalidData
	}
	if err := validatePropertiesForStorage(node.Properties); err != nil {
		return err
	}
	if err := ae.validateNodeConstraints(node); err != nil {
		return err
	}

	var baseline *Node
	ae.mu.RLock()
	_, alreadyQueued := ae.nodeCache[node.ID]
	_, hasBaseline := ae.nodeUpdateBaseline[node.ID]
	ae.mu.RUnlock()
	if !alreadyQueued && !hasBaseline {
		if existing, err := ae.engine.GetNode(node.ID); err == nil {
			baseline = CopyNode(existing)
		}
	}

	ae.mu.Lock()
	defer ae.mu.Unlock()

	if _, exists := ae.nodeUpdateBaseline[node.ID]; !exists {
		if _, existsInCache := ae.nodeCache[node.ID]; !existsInCache {
			ae.nodeUpdateBaseline[node.ID] = baseline
		}
	}

	ae.nodeCache[node.ID] = node
	ae.syncNodeLabelIndexLocked(node)
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
	for {
		ae.mu.Lock()

		// Check if already marked for deletion (idempotent)
		if ae.deleteNodes[id] {
			ae.mu.Unlock()
			return nil
		}

		// Check if node is being flushed right now (in-flight)
		isInFlight := ae.inFlightNodes[id]

		// Check if node was created/updated in this transaction (still in cache)
		if _, existsInCache := ae.nodeCache[id]; existsInCache {
			// If this is a pending CREATE (not an update of an existing node), deleting it
			// will never hit the inner engine, so no inner OnNodeDeleted callback will fire.
			// Emit a best-effort delete notification so external services (search indexes,
			// embedding counts) can drop any speculative state for this node.
			shouldNotify := !ae.updateNodes[id]

			ae.removeNodeIDFromLabelIndexLocked(id)
			// Remove from cache
			delete(ae.nodeCache, id)
			delete(ae.nodeUpdateBaseline, id)

			// CRITICAL FIX: If node is in-flight, the flush will still write it
			// to the underlying engine, so we must also mark it for deletion
			if isInFlight {
				ae.deleteNodes[id] = true
				ae.pendingWrites++
			}

			ae.mu.Unlock()
			ae.MarkNodeEmbedded(id)
			if shouldNotify {
				ae.notifyNodeDeleted(id)
			}
			return nil
		}

		// If in-flight, it will exist in underlying engine after flush - mark for deletion
		if isInFlight {
			ae.deleteNodes[id] = true
			ae.pendingWrites++
			ae.mu.Unlock()
			ae.MarkNodeEmbedded(id)
			return nil
		}

		ae.mu.Unlock()

		// Check if node actually exists in underlying engine before marking for deletion.
		// This prevents count going negative for non-existent nodes.
		if _, err := ae.engine.GetNode(id); err != nil {
			// Node doesn't exist anywhere - nothing to delete
			return ErrNotFound
		}

		// Re-check state under lock in case the node was recreated/updated concurrently.
		ae.mu.Lock()
		if ae.deleteNodes[id] {
			ae.mu.Unlock()
			return nil
		}
		if _, existsInCache := ae.nodeCache[id]; existsInCache {
			ae.mu.Unlock()
			continue
		}
		if ae.inFlightNodes[id] {
			ae.deleteNodes[id] = true
			ae.pendingWrites++
			ae.mu.Unlock()
			ae.MarkNodeEmbedded(id)
			return nil
		}

		ae.deleteNodes[id] = true
		ae.pendingWrites++
		ae.mu.Unlock()
		ae.MarkNodeEmbedded(id)
		return nil
	}
}

// CreateEdge adds to cache and returns immediately.
func (ae *AsyncEngine) CreateEdge(edge *Edge) error {
	if edge == nil {
		return ErrInvalidData
	}
	if err := validatePropertiesForStorage(edge.Properties); err != nil {
		return err
	}
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

	ae.putCacheEdgeLocked(edge)
	ae.pendingWrites++
	return nil
}

// UpdateEdge adds to cache and returns immediately.
func (ae *AsyncEngine) UpdateEdge(edge *Edge) error {
	if edge == nil {
		return ErrInvalidData
	}
	if err := validatePropertiesForStorage(edge.Properties); err != nil {
		return err
	}
	ae.mu.Lock()
	defer ae.mu.Unlock()

	ae.putCacheEdgeLocked(edge)
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
		ae.deleteCacheEdgeLocked(id)

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

// ForEachNodeIDByLabel streams node IDs for a label, combining cache + engine.
// Stops early when visit returns false.
func (ae *AsyncEngine) ForEachNodeIDByLabel(label string, visit func(NodeID) bool) error {
	if visit == nil {
		return nil
	}

	normalLabel := strings.ToLower(label)

	ae.mu.RLock()
	deletedIDs := make(map[NodeID]bool, len(ae.deleteNodes))
	for id := range ae.deleteNodes {
		deletedIDs[id] = true
	}
	cachedIDs := make([]NodeID, 0, len(ae.labelIndex[normalLabel]))
	for id := range ae.labelIndex[normalLabel] {
		if !deletedIDs[id] {
			cachedIDs = append(cachedIDs, id)
		}
	}
	for _, node := range ae.nodeCache {
		if node == nil || deletedIDs[node.ID] {
			continue
		}
		matched := false
		for _, l := range node.Labels {
			if strings.EqualFold(l, label) {
				matched = true
				break
			}
		}
		if matched {
			cachedIDs = append(cachedIDs, node.ID)
		}
	}
	ae.mu.RUnlock()

	seen := make(map[NodeID]struct{}, len(cachedIDs))
	for _, id := range cachedIDs {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		if !visit(id) {
			return nil
		}
	}

	if lookup, ok := ae.engine.(LabelNodeIDLookupEngine); ok {
		return lookup.ForEachNodeIDByLabel(label, func(id NodeID) bool {
			if deletedIDs[id] {
				return true
			}
			if _, ok := seen[id]; ok {
				return true
			}
			seen[id] = struct{}{}
			return visit(id)
		})
	}

	nodes, err := ae.engine.GetNodesByLabel(label)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if node == nil {
			continue
		}
		if deletedIDs[node.ID] {
			continue
		}
		if _, ok := seen[node.ID]; ok {
			continue
		}
		seen[node.ID] = struct{}{}
		if !visit(node.ID) {
			return nil
		}
	}
	return nil
}

// GetNodesByLabel checks cache and merges with engine results.
// Uses case-insensitive label matching for Neo4j compatibility.
// Snapshots cache state quickly, then releases lock before engine I/O.
// GetFirstNodeByLabel returns the first node with the specified label.
// Optimized for MATCH...LIMIT 1 patterns - uses label index for O(1) lookup.
func (ae *AsyncEngine) GetFirstNodeByLabel(label string) (*Node, error) {
	ae.mu.RLock()
	normalLabel := strings.ToLower(label)
	deletedIDs := make(map[NodeID]bool, len(ae.deleteNodes))
	overriddenIDs := make(map[NodeID]bool, len(ae.nodeCache))
	for id := range ae.deleteNodes {
		deletedIDs[id] = true
	}
	var scannedMatch *Node
	for id, node := range ae.nodeCache {
		overriddenIDs[id] = true
		if node == nil || deletedIDs[id] {
			continue
		}
		for _, l := range node.Labels {
			if strings.EqualFold(l, label) {
				scannedMatch = node
				break
			}
		}
		if scannedMatch != nil {
			break
		}
	}

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

	if scannedMatch != nil {
		return scannedMatch, nil
	}

	first, err := ae.engine.GetFirstNodeByLabel(label)
	if err != nil {
		return nil, err
	}
	if first != nil && !deletedIDs[first.ID] && !overriddenIDs[first.ID] {
		return first, nil
	}

	nodes, err := ae.GetNodesByLabel(label)
	if err != nil || len(nodes) == 0 {
		return nil, err
	}
	return nodes[0], nil
}

func (ae *AsyncEngine) GetNodesByLabel(label string) ([]*Node, error) {
	// Use labelIndex (per-label inverted index over nodeCache) instead of
	// scanning every cached node. Before this, every label-scoped read paid
	// O(len(nodeCache)) — and prior to the FlushWithResult cleanup fix the
	// labelIndex itself accumulated stale IDs across flushes, so even O(1)
	// readers walked through old entries.
	ae.mu.RLock()
	normalLabel := strings.ToLower(label)
	var cachedNodes []*Node
	if ids := ae.labelIndex[normalLabel]; len(ids) > 0 {
		cachedNodes = make([]*Node, 0, len(ids))
		for id := range ids {
			if ae.deleteNodes[id] {
				continue
			}
			node := ae.nodeCache[id]
			if node == nil {
				continue
			}
			cachedNodes = append(cachedNodes, node)
		}
	}
	ae.mu.RUnlock()

	// Get from engine WITHOUT lock (I/O can be slow)
	engineNodes, err := ae.engine.GetNodesByLabel(label)
	if err != nil {
		return cachedNodes, nil // Return cache-only on error
	}

	if len(engineNodes) == 0 {
		return cachedNodes, nil
	}

	result := make([]*Node, 0, len(cachedNodes)+len(engineNodes))
	seenIDs := make(map[NodeID]bool, len(cachedNodes))
	for _, node := range cachedNodes {
		result = append(result, node)
		seenIDs[node.ID] = true
	}

	// Filter engine results by the (small) live cache state by ID — keeps
	// per-engine-edge work O(1) without materializing two N-sized sets.
	ae.mu.RLock()
	for _, node := range engineNodes {
		if seenIDs[node.ID] || ae.deleteNodes[node.ID] {
			continue
		}
		if _, overridden := ae.nodeCache[node.ID]; overridden {
			continue
		}
		result = append(result, node)
	}
	ae.mu.RUnlock()
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
// It snapshots async cache state quickly under lock, then releases the lock
// before engine I/O to avoid holding ae.mu across potentially slow scans.
func (ae *AsyncEngine) AllNodes() ([]*Node, error) {
	ae.mu.RLock()

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
	ae.mu.RUnlock()

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
// It snapshots async cache state quickly under lock, then releases the lock
// before engine I/O to avoid holding ae.mu across potentially slow scans.
func (ae *AsyncEngine) AllEdges() ([]*Edge, error) {
	ae.mu.RLock()

	cachedEdges := make([]*Edge, 0, len(ae.edgeCache))
	deletedIDs := make(map[EdgeID]bool)

	for id := range ae.deleteEdges {
		deletedIDs[id] = true
	}
	for _, edge := range ae.edgeCache {
		cachedEdges = append(cachedEdges, edge)
	}
	ae.mu.RUnlock()

	engineEdges, err := ae.engine.AllEdges()
	if err != nil {
		result := make([]*Edge, 0, len(cachedEdges))
		for _, edge := range cachedEdges {
			if !deletedIDs[edge.ID] {
				result = append(result, edge)
			}
		}
		return result, nil
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
	overriddenIDs := make(map[EdgeID]bool, len(ae.edgeCache))

	for id := range ae.deleteEdges {
		deletedIDs[id] = true
	}
	for id, edge := range ae.edgeCache {
		overriddenIDs[id] = true
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
		if !seenIDs[edge.ID] && !deletedIDs[edge.ID] && !overriddenIDs[edge.ID] {
			result = append(result, edge)
		}
	}

	return result, nil
}

// Delegate read-only methods to engine

func (ae *AsyncEngine) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	// Pull cached outgoing edges through the per-node inverted index so
	// per-call work is O(degree), not O(len(edgeCache)). Prior to the
	// index this loop scanned every cached edge per BFS frontier
	// expansion, producing a roughly constant per-traversal floor on any
	// graph that fit in the async cache.
	ae.mu.RLock()
	var cached []*Edge
	if startSet, ok := ae.cacheEdgesByStart[nodeID]; ok && len(startSet) > 0 {
		cached = make([]*Edge, 0, len(startSet))
		for id := range startSet {
			if ae.deleteEdges[id] {
				continue
			}
			edge, ok := ae.edgeCache[id]
			if !ok || edge == nil || edge.StartNode != nodeID {
				continue
			}
			cached = append(cached, edge)
		}
	}
	ae.mu.RUnlock()

	engineEdges, err := ae.engine.GetOutgoingEdges(nodeID)
	if err != nil {
		return cached, nil
	}
	if len(engineEdges) == 0 {
		return cached, nil
	}

	result := make([]*Edge, 0, len(cached)+len(engineEdges))
	seenIDs := make(map[EdgeID]bool, len(cached))
	for _, e := range cached {
		result = append(result, e)
		seenIDs[e.ID] = true
	}

	// Filter engine results by the (small) live cache state. Re-acquiring
	// the RLock briefly is cheap; checking each engine edge against the
	// cache by ID stays O(1) per edge, so total cost is O(engineEdges).
	ae.mu.RLock()
	for _, e := range engineEdges {
		if seenIDs[e.ID] || ae.deleteEdges[e.ID] {
			continue
		}
		if _, overridden := ae.edgeCache[e.ID]; overridden {
			continue
		}
		result = append(result, e)
	}
	ae.mu.RUnlock()
	return result, nil
}

func (ae *AsyncEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	ae.mu.RLock()
	var cached []*Edge
	if endSet, ok := ae.cacheEdgesByEnd[nodeID]; ok && len(endSet) > 0 {
		cached = make([]*Edge, 0, len(endSet))
		for id := range endSet {
			if ae.deleteEdges[id] {
				continue
			}
			edge, ok := ae.edgeCache[id]
			if !ok || edge == nil || edge.EndNode != nodeID {
				continue
			}
			cached = append(cached, edge)
		}
	}
	ae.mu.RUnlock()

	engineEdges, err := ae.engine.GetIncomingEdges(nodeID)
	if err != nil {
		return cached, nil
	}
	if len(engineEdges) == 0 {
		return cached, nil
	}

	result := make([]*Edge, 0, len(cached)+len(engineEdges))
	seenIDs := make(map[EdgeID]bool, len(cached))
	for _, e := range cached {
		result = append(result, e)
		seenIDs[e.ID] = true
	}

	ae.mu.RLock()
	for _, e := range engineEdges {
		if seenIDs[e.ID] || ae.deleteEdges[e.ID] {
			continue
		}
		if _, overridden := ae.edgeCache[e.ID]; overridden {
			continue
		}
		result = append(result, e)
	}
	ae.mu.RUnlock()
	return result, nil
}

// GetAdjacentEdges fetches outgoing+incoming edges for nodeID, folding the
// async cache with a single inner-engine call. Mirrors the merge logic of
// the per-direction methods; the win is one transaction at the inner engine
// instead of two per BFS frontier expansion.
func (ae *AsyncEngine) GetAdjacentEdges(nodeID NodeID) ([]*Edge, []*Edge, error) {
	// Cached side first (lock-only work, no I/O).
	ae.mu.RLock()
	var cachedOut, cachedIn []*Edge
	if startSet, ok := ae.cacheEdgesByStart[nodeID]; ok && len(startSet) > 0 {
		cachedOut = make([]*Edge, 0, len(startSet))
		for id := range startSet {
			if ae.deleteEdges[id] {
				continue
			}
			edge := ae.edgeCache[id]
			if edge == nil || edge.StartNode != nodeID {
				continue
			}
			cachedOut = append(cachedOut, edge)
		}
	}
	if endSet, ok := ae.cacheEdgesByEnd[nodeID]; ok && len(endSet) > 0 {
		cachedIn = make([]*Edge, 0, len(endSet))
		for id := range endSet {
			if ae.deleteEdges[id] {
				continue
			}
			edge := ae.edgeCache[id]
			if edge == nil || edge.EndNode != nodeID {
				continue
			}
			cachedIn = append(cachedIn, edge)
		}
	}
	ae.mu.RUnlock()

	// Engine side: one call when supported, two otherwise.
	var engineOut, engineIn []*Edge
	if inner, ok := ae.engine.(AdjacentEdgesEngine); ok {
		out, in, err := inner.GetAdjacentEdges(nodeID)
		if err != nil {
			return cachedOut, cachedIn, nil //nolint:nilerr // engine errors fall back to cache, matching per-direction methods
		}
		engineOut, engineIn = out, in
	} else {
		var err error
		engineOut, err = ae.engine.GetOutgoingEdges(nodeID)
		if err != nil {
			return cachedOut, cachedIn, nil //nolint:nilerr
		}
		engineIn, err = ae.engine.GetIncomingEdges(nodeID)
		if err != nil {
			// Cache + outgoing-only fallback.
			merged := mergeAsyncEdges(ae, cachedOut, engineOut)
			return merged, cachedIn, nil
		}
	}

	outgoing := mergeAsyncEdges(ae, cachedOut, engineOut)
	incoming := mergeAsyncEdges(ae, cachedIn, engineIn)
	return outgoing, incoming, nil
}

// mergeAsyncEdges merges async-cache edges with engine-returned edges,
// honoring the cache's override and delete sets. Caller-supplied cached
// already excludes pending deletes for that direction.
func mergeAsyncEdges(ae *AsyncEngine, cached, engineEdges []*Edge) []*Edge {
	if len(engineEdges) == 0 {
		return cached
	}
	result := make([]*Edge, 0, len(cached)+len(engineEdges))
	seen := make(map[EdgeID]bool, len(cached))
	for _, e := range cached {
		result = append(result, e)
		seen[e.ID] = true
	}
	ae.mu.RLock()
	for _, e := range engineEdges {
		if e == nil {
			continue
		}
		if seen[e.ID] || ae.deleteEdges[e.ID] {
			continue
		}
		if _, overridden := ae.edgeCache[e.ID]; overridden {
			continue
		}
		result = append(result, e)
	}
	ae.mu.RUnlock()
	return result
}

func (ae *AsyncEngine) GetEdgesBetween(startID, endID NodeID) ([]*Edge, error) {
	ae.mu.RLock()
	deletedIDs := make(map[EdgeID]bool, len(ae.deleteEdges))
	overriddenIDs := make(map[EdgeID]bool, len(ae.edgeCache))
	cachedEdges := make([]*Edge, 0)
	for id := range ae.deleteEdges {
		deletedIDs[id] = true
	}
	for id, edge := range ae.edgeCache {
		overriddenIDs[id] = true
		if edge != nil && edge.StartNode == startID && edge.EndNode == endID && !deletedIDs[id] {
			cachedEdges = append(cachedEdges, edge)
		}
	}
	ae.mu.RUnlock()

	engineEdges, err := ae.engine.GetEdgesBetween(startID, endID)
	if err != nil {
		return cachedEdges, nil
	}

	result := make([]*Edge, 0, len(cachedEdges)+len(engineEdges))
	seenIDs := make(map[EdgeID]bool, len(cachedEdges))
	for _, edge := range cachedEdges {
		result = append(result, edge)
		seenIDs[edge.ID] = true
	}
	for _, edge := range engineEdges {
		if edge == nil {
			continue
		}
		if !seenIDs[edge.ID] && !deletedIDs[edge.ID] && !overriddenIDs[edge.ID] {
			result = append(result, edge)
		}
	}
	return result, nil
}

func (ae *AsyncEngine) GetEdgeBetween(startID, endID NodeID, edgeType string) *Edge {
	edges, err := ae.GetEdgesBetween(startID, endID)
	if err != nil {
		return nil
	}
	for _, edge := range edges {
		if edge == nil {
			continue
		}
		if edgeType == "" || strings.EqualFold(edge.Type, edgeType) {
			return edge
		}
	}
	return nil
}

func (ae *AsyncEngine) GetAllNodes() []*Node {
	nodes, _ := ae.AllNodes()
	return nodes
}

func (ae *AsyncEngine) GetInDegree(nodeID NodeID) int {
	edges, err := ae.GetIncomingEdges(nodeID)
	if err != nil {
		return 0
	}
	return len(edges)
}

func (ae *AsyncEngine) GetOutDegree(nodeID NodeID) int {
	edges, err := ae.GetOutgoingEdges(nodeID)
	if err != nil {
		return 0
	}
	return len(edges)
}

func (ae *AsyncEngine) GetSchema() *SchemaManager {
	return ae.engine.GetSchema()
}

// GetSchemaForNamespace implements NamespaceSchemaProvider when the underlying engine supports it.
func (ae *AsyncEngine) GetSchemaForNamespace(namespace string) *SchemaManager {
	if p, ok := ae.engine.(NamespaceSchemaProvider); ok {
		return p.GetSchemaForNamespace(namespace)
	}
	return ae.engine.GetSchema()
}

func (ae *AsyncEngine) NodeCount() (int64, error) {
	// Prevent double-counting during flush I/O:
	// Flush holds flushMu.Lock() across the entire flush (including engine writes).
	// By taking a read lock, NodeCount sees either:
	//   - pre-flush state (cache populated, engine not yet updated), or
	//   - post-flush state (cache cleared, engine updated),
	// but never the mixed mid-flush state where engine reflects writes while cache
	// still contains in-flight items.
	ae.flushMu.RLock()
	defer ae.flushMu.RUnlock()

	// Snapshot cache state under lock, then release before engine I/O to avoid
	// blocking writers/readers on potentially slow storage calls.
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
	ae.mu.RUnlock()

	engineCount, err := ae.engine.NodeCount()

	if err != nil {
		return 0, err
	}

	// Adjust for pending creates and deletes
	// Note: pendingUpdates don't change count (already counted in engineCount)
	// Include inFlightCreates because they're being written but not yet in engineCount
	count := engineCount + pendingCreates + inFlightCreates - pendingDeletes

	// Clamp to zero if negative (should never happen, log for debugging).
	// D-06 structured form: emit operator-actionable WARN with stable
	// attributes so dashboards can alert on (op=NodeCount AND result < 0)
	// aggregations. The original bracketed marker is replaced by the op
	// attribute combined with action=clamp_to_zero.
	if count < 0 {
		ae.log.Warn("count went negative",
			"op", "NodeCount",
			"engine_count", engineCount,
			"pending_creates", pendingCreates,
			"in_flight_creates", inFlightCreates,
			"pending_deletes", pendingDeletes,
			"result", count,
			"action", "clamp_to_zero",
		)
		return 0, nil
	}
	return count, nil
}

func (ae *AsyncEngine) EdgeCount() (int64, error) {
	ae.flushMu.RLock()
	defer ae.flushMu.RUnlock()

	// Snapshot cache state under lock, then release before engine I/O to avoid
	// blocking writers/readers on potentially slow storage calls.
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
	ae.mu.RUnlock()

	engineCount, err := ae.engine.EdgeCount()

	if err != nil {
		return 0, err
	}

	// Adjust for pending creates and deletes
	// Note: updates don't change count (already counted in engineCount)
	// Include inFlightCreates because they're being written but not yet in engineCount
	count := engineCount + pendingCreates + inFlightCreates - pendingDeletes

	// Clamp to zero if negative (should never happen, log for debugging).
	// D-06 structured form (see NodeCount above for rationale).
	if count < 0 {
		ae.log.Warn("count went negative",
			"op", "EdgeCount",
			"engine_count", engineCount,
			"pending_creates", pendingCreates,
			"in_flight_creates", inFlightCreates,
			"pending_deletes", pendingDeletes,
			"result", count,
			"action", "clamp_to_zero",
		)
		return 0, nil
	}
	return count, nil
}

func (ae *AsyncEngine) NodeCountByPrefix(prefix string) (int64, error) {
	locked := ae.flushMu.TryRLock()
	if locked {
		defer ae.flushMu.RUnlock()
	}

	// Snapshot cache state under lock, then release before engine I/O.
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
	ae.mu.RUnlock()

	var engineCount int64
	var err error
	if stats, ok := ae.engine.(PrefixStatsEngine); ok {
		engineCount, err = stats.NodeCountByPrefix(prefix)
	} else {
		// Correctness fallback for uncommon engines (slower).
		engineCount, err = countNodesInEngineByPrefix(ae.engine, prefix)
	}

	if err != nil {
		return 0, err
	}

	count := engineCount + pendingCreates + inFlightCreates - pendingDeletes
	if count < 0 {
		ae.log.Warn("count went negative",
			"op", "NodeCountByPrefix",
			"prefix", prefix,
			"engine_count", engineCount,
			"pending_creates", pendingCreates,
			"in_flight_creates", inFlightCreates,
			"pending_deletes", pendingDeletes,
			"result", count,
			"action", "clamp_to_zero",
		)
		return 0, nil
	}
	_ = pendingUpdates // no-op (kept for symmetry with NodeCount)
	return count, nil
}

func (ae *AsyncEngine) EdgeCountByPrefix(prefix string) (int64, error) {
	locked := ae.flushMu.TryRLock()
	if locked {
		defer ae.flushMu.RUnlock()
	}

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
	ae.mu.RUnlock()

	var engineCount int64
	var err error
	if stats, ok := ae.engine.(PrefixStatsEngine); ok {
		engineCount, err = stats.EdgeCountByPrefix(prefix)
	} else {
		engineCount, err = countEdgesInEngineByPrefix(ae.engine, prefix)
	}

	if err != nil {
		return 0, err
	}

	count := engineCount + pendingCreates + inFlightCreates - pendingDeletes
	if count < 0 {
		ae.log.Warn("count went negative",
			"op", "EdgeCountByPrefix",
			"prefix", prefix,
			"engine_count", engineCount,
			"pending_creates", pendingCreates,
			"in_flight_creates", inFlightCreates,
			"pending_deletes", pendingDeletes,
			"result", count,
			"action", "clamp_to_zero",
		)
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

	// Treat "storage closed" as expected during shutdown (teardown closed engine before async, or ticker/stopChan race).
	if result.HasErrors() && result.isStorageClosedOnly() {
		result = FlushResult{}
	}

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
	if err := ae.validateBulkNodeConstraints(nodes); err != nil {
		return err
	}
	for _, node := range nodes {
		if node == nil {
			return ErrInvalidData
		}
		if err := validatePropertiesForStorage(node.Properties); err != nil {
			return err
		}
	}

	ae.mu.Lock()
	defer ae.mu.Unlock()

	for _, node := range nodes {
		delete(ae.deleteNodes, node.ID)
		delete(ae.updateNodes, node.ID)
		delete(ae.nodeUpdateBaseline, node.ID)
		ae.nodeCache[node.ID] = node
		ae.syncNodeLabelIndexLocked(node)
	}
	ae.pendingWrites += int64(len(nodes))
	return nil
}

func (ae *AsyncEngine) validateBulkNodeConstraints(nodes []*Node) error {
	seen := make(map[string]struct{})

	for _, node := range nodes {
		if node == nil {
			return ErrInvalidData
		}
		namespace, prefixRequired, err := ae.resolveNamespace(node.ID)
		if err != nil {
			return err
		}
		if err := ae.validateNodeConstraintsWithNamespace(node, namespace, prefixRequired); err != nil {
			return err
		}

		schema := ae.GetSchemaForNamespace(namespace)
		if schema == nil {
			continue
		}

		constraints := schema.GetConstraintsForLabels(node.Labels)
		for _, c := range constraints {
			switch c.Type {
			case ConstraintUnique:
				if len(c.Properties) != 1 {
					continue
				}
				prop := c.Properties[0]
				value := node.Properties[prop]
				if value == nil {
					continue
				}
				key := fmt.Sprintf("%s:%s:%s", namespace, c.Name, constraintValueKey(value))
				if _, exists := seen[key]; exists {
					return &ConstraintViolationError{
						Type:       ConstraintUnique,
						Label:      c.Label,
						Properties: []string{prop},
						Message:    fmt.Sprintf("Node with %s=%v already exists in batch", prop, value),
					}
				}
				seen[key] = struct{}{}
			case ConstraintNodeKey:
				values := make([]interface{}, len(c.Properties))
				for i, prop := range c.Properties {
					values[i] = node.Properties[prop]
					if values[i] == nil {
						return &ConstraintViolationError{
							Type:       ConstraintNodeKey,
							Label:      c.Label,
							Properties: c.Properties,
							Message:    fmt.Sprintf("NODE KEY property %s cannot be null", prop),
						}
					}
				}
				key := fmt.Sprintf("%s:%s:%s", namespace, c.Name, constraintCompositeKey(values))
				if _, exists := seen[key]; exists {
					return &ConstraintViolationError{
						Type:       ConstraintNodeKey,
						Label:      c.Label,
						Properties: c.Properties,
						Message:    fmt.Sprintf("Node with key %v=%v already exists in batch", c.Properties, values),
					}
				}
				seen[key] = struct{}{}
			}
		}
	}

	return nil
}

func (ae *AsyncEngine) validateNodeConstraints(node *Node) error {
	if node == nil {
		return ErrInvalidData
	}
	namespace, prefixRequired, err := ae.resolveNamespace(node.ID)
	if err != nil {
		return err
	}
	return ae.validateNodeConstraintsWithNamespace(node, namespace, prefixRequired)
}

func (ae *AsyncEngine) validateNodeConstraintsWithNamespace(node *Node, namespace string, prefixRequired bool) error {
	if node == nil {
		return ErrInvalidData
	}

	schema := ae.GetSchemaForNamespace(namespace)
	if schema == nil {
		return nil
	}

	constraints := schema.GetConstraintsForLabels(node.Labels)
	for _, constraint := range constraints {
		switch constraint.Type {
		case ConstraintUnique:
			if err := ae.checkUniqueConstraint(node, constraint, namespace, prefixRequired); err != nil {
				return err
			}
		case ConstraintNodeKey:
			if err := ae.checkNodeKeyConstraint(node, constraint, namespace, prefixRequired); err != nil {
				return err
			}
		case ConstraintExists:
			if err := ae.checkExistenceConstraint(node, constraint); err != nil {
				return err
			}
		}
	}

	typeConstraints := schema.GetPropertyTypeConstraintsForLabels(node.Labels)
	for _, constraint := range typeConstraints {
		value := node.Properties[constraint.Property]
		if err := ValidatePropertyType(value, constraint.ExpectedType); err != nil {
			return &ConstraintViolationError{
				Type:       ConstraintPropertyType,
				Label:      constraint.Label,
				Properties: []string{constraint.Property},
				Message:    fmt.Sprintf("Property %s must be %s (%v)", constraint.Property, constraint.ExpectedType, err),
			}
		}
	}

	return nil
}

func (ae *AsyncEngine) checkUniqueConstraint(node *Node, c Constraint, namespace string, prefixRequired bool) error {
	if len(c.Properties) != 1 {
		return nil
	}
	prop := c.Properties[0]
	if node.Properties == nil {
		return nil
	}
	value := node.Properties[prop]
	if value == nil {
		return nil
	}

	nsPrefix := namespace + ":"
	ae.mu.RLock()
	for id, n := range ae.nodeCache {
		if id == node.ID || ae.deleteNodes[id] {
			continue
		}
		if prefixRequired && !strings.HasPrefix(string(id), nsPrefix) {
			continue
		}
		if hasLabel(n.Labels, c.Label) && compareValues(n.Properties[prop], value) {
			ae.mu.RUnlock()
			return &ConstraintViolationError{
				Type:       ConstraintUnique,
				Label:      c.Label,
				Properties: []string{prop},
				Message:    fmt.Sprintf("Node with %s=%v already exists in async cache", prop, value),
			}
		}
	}
	ae.mu.RUnlock()

	nodes, err := ae.engine.GetNodesByLabel(c.Label)
	if err != nil {
		return nil
	}
	for _, existing := range nodes {
		if existing.ID == node.ID {
			continue
		}
		if prefixRequired && !strings.HasPrefix(string(existing.ID), nsPrefix) {
			continue
		}
		if compareValues(existing.Properties[prop], value) {
			return &ConstraintViolationError{
				Type:       ConstraintUnique,
				Label:      c.Label,
				Properties: []string{prop},
				Message:    fmt.Sprintf("Node with %s=%v already exists (nodeID: %s)", prop, value, existing.ID),
			}
		}
	}

	return nil
}

func (ae *AsyncEngine) checkNodeKeyConstraint(node *Node, c Constraint, namespace string, prefixRequired bool) error {
	if len(c.Properties) < 1 {
		return nil
	}
	if node.Properties == nil {
		return &ConstraintViolationError{
			Type:       ConstraintNodeKey,
			Label:      c.Label,
			Properties: c.Properties,
			Message:    "NODE KEY properties cannot be null",
		}
	}

	values := make([]interface{}, len(c.Properties))
	for i, prop := range c.Properties {
		value := node.Properties[prop]
		if value == nil {
			return &ConstraintViolationError{
				Type:       ConstraintNodeKey,
				Label:      c.Label,
				Properties: c.Properties,
				Message:    fmt.Sprintf("NODE KEY property %s cannot be null", prop),
			}
		}
		values[i] = value
	}

	nsPrefix := namespace + ":"
	ae.mu.RLock()
	for id, n := range ae.nodeCache {
		if id == node.ID || ae.deleteNodes[id] {
			continue
		}
		if prefixRequired && !strings.HasPrefix(string(id), nsPrefix) {
			continue
		}
		if !hasLabel(n.Labels, c.Label) {
			continue
		}
		match := true
		for i, prop := range c.Properties {
			if !compareValues(n.Properties[prop], values[i]) {
				match = false
				break
			}
		}
		if match {
			ae.mu.RUnlock()
			return &ConstraintViolationError{
				Type:       ConstraintNodeKey,
				Label:      c.Label,
				Properties: c.Properties,
				Message:    fmt.Sprintf("Node with key %v=%v already exists in async cache", c.Properties, values),
			}
		}
	}
	ae.mu.RUnlock()

	nodes, err := ae.engine.GetNodesByLabel(c.Label)
	if err != nil {
		return nil
	}
	for _, existing := range nodes {
		if existing.ID == node.ID {
			continue
		}
		if prefixRequired && !strings.HasPrefix(string(existing.ID), nsPrefix) {
			continue
		}
		match := true
		for i, prop := range c.Properties {
			if !compareValues(existing.Properties[prop], values[i]) {
				match = false
				break
			}
		}
		if match {
			return &ConstraintViolationError{
				Type:       ConstraintNodeKey,
				Label:      c.Label,
				Properties: c.Properties,
				Message:    fmt.Sprintf("Node with key %v=%v already exists (nodeID: %s)", c.Properties, values, existing.ID),
			}
		}
	}

	return nil
}

func (ae *AsyncEngine) resolveNamespace(nodeID NodeID) (string, bool, error) {
	if namespace, _, ok := ParseDatabasePrefix(string(nodeID)); ok {
		return namespace, true, nil
	}
	if provider, ok := ae.engine.(interface{ Namespace() string }); ok {
		ns := provider.Namespace()
		if ns != "" {
			return ns, false, nil
		}
	}
	return "", false, fmt.Errorf("node ID must be prefixed with namespace (e.g., 'nornic:node-123'), got: %s", nodeID)
}

func (ae *AsyncEngine) checkExistenceConstraint(node *Node, c Constraint) error {
	if len(c.Properties) != 1 {
		return nil
	}
	prop := c.Properties[0]
	if node.Properties == nil {
		return &ConstraintViolationError{
			Type:       ConstraintExists,
			Label:      c.Label,
			Properties: []string{prop},
			Message:    fmt.Sprintf("Required property %s is missing", prop),
		}
	}
	if val, ok := node.Properties[prop]; !ok || val == nil {
		return &ConstraintViolationError{
			Type:       ConstraintExists,
			Label:      c.Label,
			Properties: []string{prop},
			Message:    fmt.Sprintf("Required property %s is missing", prop),
		}
	}
	return nil
}

// BulkCreateEdges creates edges in batch (async).
func (ae *AsyncEngine) BulkCreateEdges(edges []*Edge) error {
	for _, edge := range edges {
		if edge == nil {
			return ErrInvalidData
		}
		if err := validatePropertiesForStorage(edge.Properties); err != nil {
			return err
		}
	}

	ae.mu.Lock()
	defer ae.mu.Unlock()

	for _, edge := range edges {
		delete(ae.deleteEdges, edge.ID)
		delete(ae.updateEdges, edge.ID)
		ae.putCacheEdgeLocked(edge)
	}
	ae.pendingWrites += int64(len(edges))
	return nil
}

// BulkDeleteNodes marks multiple nodes for deletion (async).
func (ae *AsyncEngine) BulkDeleteNodes(ids []NodeID) error {
	ae.mu.Lock()
	notifyDeletes := make([]NodeID, 0)

	for _, id := range ids {
		// If this is a pending create in cache, deleting it won’t hit the inner engine.
		// Notify best-effort so external services can remove any speculative indexes.
		if _, ok := ae.nodeCache[id]; ok && !ae.updateNodes[id] {
			notifyDeletes = append(notifyDeletes, id)
		}
		ae.removeNodeIDFromLabelIndexLocked(id)
		delete(ae.nodeCache, id)
		delete(ae.updateNodes, id)
		delete(ae.nodeUpdateBaseline, id)
		ae.deleteNodes[id] = true
	}
	ae.pendingWrites += int64(len(ids))
	ae.mu.Unlock()

	for _, id := range notifyDeletes {
		ae.notifyNodeDeleted(id)
	}
	return nil
}

// BulkDeleteEdges marks multiple edges for deletion (async).
func (ae *AsyncEngine) BulkDeleteEdges(ids []EdgeID) error {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	for _, id := range ids {
		ae.deleteCacheEdgeLocked(id)
		ae.deleteEdges[id] = true
	}
	ae.pendingWrites += int64(len(ids))
	return nil
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
		if ae.isNodeMarkedDeleted(node.ID) {
			// If delete is pending in AsyncEngine, proactively remove this stale queue
			// entry from the underlying pending-embeddings index.
			ae.MarkNodeEmbedded(node.ID)
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
			if ae.isNodeMarkedDeleted(node.ID) {
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

// AddToPendingEmbeddings delegates to the underlying engine, if supported.
// Call this to re-queue a node for embedding after a failed attempt (e.g. so another worker can retry).
func (ae *AsyncEngine) AddToPendingEmbeddings(nodeID NodeID) {
	// Don't allow re-queue while delete is pending in AsyncEngine.
	if ae.isNodeMarkedDeleted(nodeID) {
		return
	}
	if mgr, ok := ae.engine.(interface{ AddToPendingEmbeddings(NodeID) }); ok {
		mgr.AddToPendingEmbeddings(nodeID)
	}
}

// RecordMaterializedAccess delegates result-materialization access recording to
// the underlying engine, if supported.
func (ae *AsyncEngine) RecordMaterializedAccess(entityID string) {
	if recorder, ok := ae.engine.(interface{ RecordMaterializedAccess(string) }); ok {
		recorder.RecordMaterializedAccess(entityID)
	}
}

// PendingEmbeddingsCount delegates to the underlying engine, if supported.
func (ae *AsyncEngine) PendingEmbeddingsCount() int {
	if mgr, ok := ae.engine.(interface{ PendingEmbeddingsCount() int }); ok {
		return mgr.PendingEmbeddingsCount()
	}
	return 0
}

func (ae *AsyncEngine) isNodeMarkedDeleted(nodeID NodeID) bool {
	ae.mu.RLock()
	deleted := ae.deleteNodes[nodeID]
	ae.mu.RUnlock()
	return deleted
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
			ID:         node.ID,
			Labels:     append([]string(nil), node.Labels...),
			Properties: make(map[string]any, len(node.Properties)),
			CreatedAt:  node.CreatedAt,
			UpdatedAt:  node.UpdatedAt,
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
		select {
		case <-ctx.Done():
			ae.mu.RUnlock()
			return ctx.Err()
		default:
		}
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
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
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

// StreamNodesByPrefix implements PrefixStreamingEngine by merging pending cache
// entries with prefix-scoped streaming from the underlying engine.
func (ae *AsyncEngine) StreamNodesByPrefix(ctx context.Context, prefix string, fn func(node *Node) error) error {
	ae.mu.RLock()
	cachedIDs := make(map[NodeID]bool, len(ae.nodeCache))
	deletedIDs := make(map[NodeID]bool, len(ae.deleteNodes))
	cachedCopies := make([]*Node, 0, len(ae.nodeCache))

	for id, node := range ae.nodeCache {
		cachedIDs[id] = true
		if ae.deleteNodes[id] {
			continue
		}
		if strings.HasPrefix(string(id), prefix) {
			cachedCopies = append(cachedCopies, CopyNode(node))
		}
	}
	for id := range ae.deleteNodes {
		deletedIDs[id] = true
	}
	ae.mu.RUnlock()

	for _, node := range cachedCopies {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := fn(node); err != nil {
			if err == ErrIterationStopped {
				return nil
			}
			return err
		}
	}

	if prefixStreamer, ok := ae.engine.(PrefixStreamingEngine); ok {
		return prefixStreamer.StreamNodesByPrefix(ctx, prefix, func(node *Node) error {
			if cachedIDs[node.ID] || deletedIDs[node.ID] {
				return nil
			}
			return fn(node)
		})
	}

	// Fallback to full stream if underlying engine does not support prefix stream.
	if streamer, ok := ae.engine.(StreamingEngine); ok {
		return streamer.StreamNodes(ctx, func(node *Node) error {
			if cachedIDs[node.ID] || deletedIDs[node.ID] {
				return nil
			}
			if !strings.HasPrefix(string(node.ID), prefix) {
				return nil
			}
			return fn(node)
		})
	}

	nodes, err := ae.engine.AllNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if cachedIDs[node.ID] || deletedIDs[node.ID] {
			continue
		}
		if !strings.HasPrefix(string(node.ID), prefix) {
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
		select {
		case <-ctx.Done():
			ae.mu.RUnlock()
			return ctx.Err()
		default:
		}
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
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
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

// LastWriteTime returns the last known write time from the underlying engine, if available.
func (ae *AsyncEngine) LastWriteTime() time.Time {
	if ae == nil {
		return time.Time{}
	}
	if p, ok := ae.engine.(interface{ LastWriteTime() time.Time }); ok {
		return p.LastWriteTime()
	}
	return time.Time{}
}

// Verify AsyncEngine implements Engine interface
var _ Engine = (*AsyncEngine)(nil)

// Verify AsyncEngine implements StreamingEngine interface
var _ StreamingEngine = (*AsyncEngine)(nil)
