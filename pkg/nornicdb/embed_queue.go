// Package nornicdb provides async embedding worker for background embedding generation.
package nornicdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/embeddingsource"
	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	defaultEmbedChunkSize    = 8192
	defaultEmbedChunkOverlap = 50
)

type deterministicTextChunker interface {
	ChunkText(text string, maxTokens, overlap int) ([]string, error)
}

// EmbedWorker manages async embedding generation using a pull-based model.
// Enqueue wakes sleeping workers; a long recovery scan finds missed writes.
type EmbedWorker struct {
	embedder             embed.Embedder
	embedderResolver     func(storage.NodeID) (embed.Embedder, error)
	failureErrorRedactor func(storage.NodeID, error) error
	storage              storage.Engine
	config               *EmbedWorkerConfig

	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	lifecycleMu sync.Mutex

	// Trigger channel to wake up worker immediately
	trigger chan struct{}
	// readiness is a broadcast generation for model and pause-state changes.
	// Each worker captures it before checking readiness, so a transition cannot
	// be lost between that check and entering the idle wait.
	readiness chan struct{}
	// Debounced external-trigger state (write-lull signaling).
	triggerMu            sync.Mutex
	triggerDebounceTimer *time.Timer
	triggerDebounceSeq   atomic.Uint64

	// Callback after embedding a node (for search index update)
	onEmbedded func(node *storage.Node)

	// Callback when queue becomes empty (for triggering k-means clustering)
	onQueueEmpty func(processedCount int)

	// Stats
	mu sync.Mutex
	// Atomic stats fields so /embed/stats never waits on worker map locks.
	processed atomic.Int64
	failed    atomic.Int64
	parked    atomic.Int64
	inFlight  atomic.Int64
	closed    atomic.Bool // Set to true when Close() is called
	// restoreHolds counts the restores holding the worker (holdForRestore);
	// while one does, it claims no nodes and doesn't scan the store.
	restoreHolds atomic.Int32

	// Recently processed node IDs to prevent re-processing before DB commit is visible
	// This prevents the same node being processed multiple times in quick succession
	recentlyProcessed  map[string]recentNodeEmbedding
	nodeUpdates        map[string]struct{} // mu; mutation notifications consumed by retry owner
	nodeUpdatesPending atomic.Bool

	// Track nodes we've already logged as skipped (to avoid log spam)
	loggedSkip map[string]bool

	// Debounce state for k-means clustering trigger
	clusterDebounceTimer   *time.Timer
	clusterDebounceMu      sync.Mutex
	pendingClusterCount    int  // Accumulated count for debounced callback
	clusterDebounceRunning bool // Whether a debounce timer is active

	// claimMu serializes find+claim. claimedNodes remains authoritative while a
	// provider request is in flight because storage-side pending-index removals
	// are not guaranteed to become visible immediately.
	claimMu       sync.Mutex
	claimedNodes  map[string]*nodeEmbeddingAttempt
	claimSequence uint64
	// Provider setters linearize against sidecar commit and publication.
	providerWriteMu sync.RWMutex

	nodeRetries   map[storage.NodeID]*nodeEmbeddingRetry // claimMu: retry ownership until an accepted outcome
	retrySequence uint64                                 // claimMu: FIFO failure order, independent of deadline

	embeddingEpoch atomic.Uint64

	// workersStarted is true once StartWorkers() has been called (used when DeferWorkerStart is true).
	workersStarted bool

	// initialScanDone ensures startup refresh/index scan runs once for the whole worker pool,
	// not once per worker goroutine.
	initialScanDone bool
	// No worker may claim during the startup refresh: it could otherwise
	// re-add an in-flight node's marker and hide later candidates behind it.
	initialScanMu sync.Mutex

	// refreshMu/lastRefreshAt throttle full pending-index refresh scans, which are
	// potentially expensive on large datasets and can contend with write traffic.
	refreshMu     sync.Mutex
	lastRefreshAt time.Time

	// shouldYield reports whether foreground request pressure is high enough that
	// background embedding should pause. This keeps tx/commit hot path prioritized.
	shouldYield func() bool
}

const minRefreshOnEmptyInterval = 30 * time.Second

// EmbedWorkerConfig holds configuration for the embedding worker.
type EmbedWorkerConfig struct {
	// Worker settings
	NumWorkers   int           // Number of concurrent workers (default: 1, 0 disables background workers)
	ScanInterval time.Duration // Recovery scan interval (default: 15m, minimum: 30s); Enqueue wakes immediately.
	BatchDelay   time.Duration // Minimum delay after each provider request (default: 500ms)
	MaxRetries   int           // DLQ retries after the initial attempt; then terminal (default: 3)
	// ProviderRetryBackoff and ProviderRetryBackoffMax control per-node DLQ
	// backoff for provider and persistence failures. MAIN is never held back.
	ProviderRetryBackoff    time.Duration // default: 2s
	ProviderRetryBackoffMax time.Duration // default: 1m
	// TriggerDebounceDelay delays enqueue-triggered scans until writes lull.
	// Each new Enqueue resets the timer. Set 0 for immediate trigger behavior.
	TriggerDebounceDelay time.Duration // default: 2s

	// Text chunking settings.
	ChunkSize    int // Max tokens per chunk (default: 8192)
	ChunkOverlap int // Tokens to overlap between chunks (default: 50)
	// EmbedBatchSize caps chunks per EmbedBatch call to avoid oversized requests.
	EmbedBatchSize int // Max chunks per batch request (default: 32)

	// Debounce settings for k-means clustering trigger
	ClusterDebounceDelay time.Duration // How long to wait after last embedding before triggering k-means (default: 30s)
	ClusterMinBatchSize  int           // Minimum embeddings processed before triggering k-means (default: 10)

	// Property include/exclude for embedding text (optional)
	// PropertiesInclude: if non-empty, only these property keys are used when building embedding text.
	PropertiesInclude []string
	// PropertiesExclude: these property keys are never used (in addition to built-in metadata skips).
	PropertiesExclude []string
	// RelationshipPropertiesInclude / RelationshipPropertiesExclude are the
	// lists for relationship search's embedding text; unset, the node lists
	// apply (relationshipEmbedProperties).
	RelationshipPropertiesInclude []string
	RelationshipPropertiesExclude []string
	// IncludeLabels: if true (default), node labels are prepended to the embedding text.
	IncludeLabels bool

	// DeferWorkerStart, when true, creates the queue but does not start worker goroutines.
	// Call StartWorkers() after the database has warmed up (e.g. after search index build).
	DeferWorkerStart bool
}

// DefaultEmbedWorkerConfig returns sensible defaults.
func DefaultEmbedWorkerConfig() *EmbedWorkerConfig {
	return &EmbedWorkerConfig{
		NumWorkers:              1,                      // Single worker by default
		ScanInterval:            15 * time.Minute,       // Scan for missed nodes every 15 minutes
		BatchDelay:              500 * time.Millisecond, // Delay between processing nodes
		MaxRetries:              3,
		ProviderRetryBackoff:    2 * time.Second,
		ProviderRetryBackoffMax: time.Minute,
		TriggerDebounceDelay:    2 * time.Second,
		ChunkSize:               defaultEmbedChunkSize,
		ChunkOverlap:            defaultEmbedChunkOverlap,
		EmbedBatchSize:          32,
		ClusterDebounceDelay:    30 * time.Second, // Wait 30s after last embedding before k-means
		ClusterMinBatchSize:     10,               // Need at least 10 embeddings to trigger k-means
		PropertiesInclude:       nil,
		PropertiesExclude:       nil,
		IncludeLabels:           true,
	}
}

// NewEmbedWorker creates a new async embedding worker pool.
// If embedder is nil, the worker will wait for SetEmbedder() to be called.
// NumWorkers controls how many concurrent workers process embeddings in parallel; zero disables them.
// Use more workers for network-based embedders (OpenAI, etc.) or when you have
// multiple GPUs/CPUs available for local embedding generation.
func NewEmbedWorker(embedder embed.Embedder, storage storage.Engine, config *EmbedWorkerConfig) *EmbedWorker {
	if config == nil {
		config = DefaultEmbedWorkerConfig()
	}

	if config.NumWorkers < 0 {
		config.NumWorkers = 1
	}
	if config.EmbedBatchSize < 1 {
		config.EmbedBatchSize = 32
	}

	ctx, cancel := context.WithCancel(context.Background())

	ew := &EmbedWorker{
		embedder:          embedder,
		storage:           storage,
		config:            config,
		ctx:               ctx,
		cancel:            cancel,
		trigger:           make(chan struct{}, 1),
		recentlyProcessed: make(map[string]recentNodeEmbedding),
		loggedSkip:        make(map[string]bool),
		claimedNodes:      make(map[string]*nodeEmbeddingAttempt),
	}

	// Start N workers unless deferred until after DB warmup
	if !config.DeferWorkerStart {
		numWorkers := config.NumWorkers
		for i := 0; i < numWorkers; i++ {
			ew.wg.Add(1)
			go ew.worker()
		}
		if numWorkers > 1 {
			fmt.Printf("🧠 Started %d embedding workers for parallel processing\n", numWorkers)
		}
	}

	return ew
}

// StartWorkers starts the embedding worker goroutines. It is used when the queue was
// created with DeferWorkerStart=true (e.g. to avoid competing with DB warmup). Idempotent.
func (ew *EmbedWorker) StartWorkers() {
	ew.lifecycleMu.Lock()
	defer ew.lifecycleMu.Unlock()
	ew.mu.Lock()
	defer ew.mu.Unlock()
	if ew.closed.Load() || ew.workersStarted {
		return
	}
	ew.workersStarted = true
	numWorkers := ew.config.NumWorkers
	for i := 0; i < numWorkers; i++ {
		ew.wg.Add(1)
		go ew.worker()
	}
	if numWorkers == 0 {
		fmt.Println("🧠 Embed queue workers disabled (configured workers=0)")
	} else if numWorkers > 1 {
		fmt.Printf("🧠 Started %d embedding workers for parallel processing\n", numWorkers)
	} else {
		fmt.Println("🧠 Embed queue workers started (after DB warmup)")
	}
}

// SetEmbedder sets or updates the embedder (for async initialization).
// This allows the worker to start before the model is loaded.
func (ew *EmbedWorker) SetEmbedder(embedder embed.Embedder) {
	ew.providerWriteMu.Lock()
	defer ew.providerWriteMu.Unlock()
	ew.setEmbedderUnderWriteBarrier(embedder)
}

// setEmbedderUnderWriteBarrier is also used by DB.SetEmbedder, which acquires
// the provider barrier before db.mu so publication can enter DB locks.
func (ew *EmbedWorker) setEmbedderUnderWriteBarrier(embedder embed.Embedder) {
	ew.mu.Lock()
	ew.embedder = embedder
	ew.embeddingEpoch.Add(1)
	ew.mu.Unlock()
	ew.invalidateNodeEmbeddingRetries()
	// Trigger immediate processing now that embedder is available
	ew.signalAllWorkers()
}

// currentEmbedder returns the embedder under ew.mu. SetEmbedder can replace
// it at any time (async model loading starts the workers before the model
// is set), so every read goes through here instead of the bare field.
func (ew *EmbedWorker) currentEmbedder() embed.Embedder {
	ew.mu.Lock()
	defer ew.mu.Unlock()
	return ew.embedder
}

// SetEmbedderResolver selects a provider for a fully-qualified node ID. This
// supports per-database model spaces without coupling the worker to a provider.
func (ew *EmbedWorker) SetEmbedderResolver(resolver func(storage.NodeID) (embed.Embedder, error)) {
	ew.providerWriteMu.Lock()
	defer ew.providerWriteMu.Unlock()
	ew.mu.Lock()
	ew.embedderResolver = resolver
	ew.embeddingEpoch.Add(1)
	ew.mu.Unlock()
	ew.invalidateNodeEmbeddingRetries()
	ew.signalTrigger()
}

func (ew *EmbedWorker) resolveEmbedder(nodeID storage.NodeID) (embed.Embedder, error) {
	return ew.selectEmbeddingProvider().resolve(nodeID)
}

// SetOnEmbedded sets a callback to be called after a node is embedded.
// Use this to update search indexes.
func (ew *EmbedWorker) SetOnEmbedded(fn func(node *storage.Node)) {
	ew.onEmbedded = fn
}

// SetOnQueueEmpty sets a callback to be called when the queue becomes empty.
// Use this to trigger k-means clustering after batch embedding completes.
// The callback receives the total number of embeddings processed in this batch.
func (ew *EmbedWorker) SetOnQueueEmpty(fn func(processedCount int)) {
	ew.onQueueEmpty = fn
}

// SetShouldYield configures an optional pressure probe used to pause background
// embedding while foreground request traffic is active.
func (ew *EmbedWorker) SetShouldYield(fn func() bool) {
	ew.mu.Lock()
	ew.shouldYield = fn
	ew.mu.Unlock()
	ew.signalAllWorkers()
}

// Trigger wakes up the worker to check for nodes without embeddings.
// Call this after creating a new node.
func (ew *EmbedWorker) Trigger() {
	if ew.closed.Load() {
		return
	}
	delay := time.Duration(0)
	if ew.config != nil {
		delay = ew.config.TriggerDebounceDelay
	}
	if delay <= 0 {
		ew.signalTrigger()
		return
	}

	seq := ew.triggerDebounceSeq.Add(1)
	ew.triggerMu.Lock()
	if ew.triggerDebounceTimer != nil {
		ew.triggerDebounceTimer.Stop()
	}
	ew.triggerDebounceTimer = time.AfterFunc(delay, func() {
		if ew.closed.Load() {
			return
		}
		// Debounce reset behavior: only latest schedule fires.
		if ew.triggerDebounceSeq.Load() != seq {
			return
		}
		ew.signalTrigger()
	})
	ew.triggerMu.Unlock()
}

// TriggerImmediate bypasses debounce and signals worker immediately.
// Use for explicit/manual triggers, not high-frequency write-path enqueue.
func (ew *EmbedWorker) TriggerImmediate() {
	ew.signalTrigger()
}

func (ew *EmbedWorker) signalTrigger() {
	if ew.closed.Load() {
		return
	}
	select {
	case ew.trigger <- struct{}{}:
	default:
		// Already triggered
	}
}

func (ew *EmbedWorker) readinessNotification() <-chan struct{} {
	ew.triggerMu.Lock()
	defer ew.triggerMu.Unlock()
	if ew.readiness == nil {
		ew.readiness = make(chan struct{})
	}
	return ew.readiness
}

// signalAllWorkers broadcasts readiness changes; ordinary enqueue notifications
// remain coalesced because their recipient drains the durable queue.
func (ew *EmbedWorker) signalAllWorkers() {
	if ew.closed.Load() {
		return
	}
	ew.triggerMu.Lock()
	if ew.readiness != nil {
		close(ew.readiness)
	}
	ew.readiness = make(chan struct{})
	ew.triggerMu.Unlock()
	ew.signalTrigger()
}

// WorkerStats returns current worker statistics.
type WorkerStats struct {
	Running   bool `json:"running"`
	InFlight  int  `json:"in_flight"`
	Pending   int  `json:"pending"`
	Processed int  `json:"processed"`
	Failed    int  `json:"failed"`
	Parked    int  `json:"parked"`
}

// EmbeddingFailure describes a node parked after a terminal provider error.
// Failure metadata is stored with the node so it survives process restarts.
type EmbeddingFailure struct {
	NodeID   storage.NodeID `json:"node_id"`
	Error    string         `json:"error"`
	FailedAt string         `json:"failed_at,omitempty"`
}

// QueueLen returns the canonical outstanding work count, including backoff DLQ.
func (ew *EmbedWorker) QueueLen() int { return ew.PendingEmbeddingsCount() }

func (ew *EmbedWorker) Stats() WorkerStats {
	inFlight := int(ew.inFlight.Load())
	return WorkerStats{
		Running:   inFlight > 0,
		InFlight:  inFlight,
		Pending:   ew.PendingEmbeddingsCount(),
		Processed: int(ew.processed.Load()),
		Failed:    int(ew.failed.Load()),
		Parked:    int(ew.parked.Load()),
	}
}

// ParkedEmbeddingFailures lists terminal embedding failures without loading
// vector payloads. A non-positive limit returns all failures.
func (ew *EmbedWorker) ParkedEmbeddingFailures(ctx context.Context, limit int) ([]EmbeddingFailure, error) {
	failures := make([]EmbeddingFailure, 0)
	count, err := ew.scanParkedEmbeddingFailures(ctx, func(failure EmbeddingFailure) error {
		failure.Error = ew.redactFailureText(failure.NodeID, failure.Error)
		if limit <= 0 || len(failures) < limit {
			failures = append(failures, failure)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	ew.parked.Store(int64(count))
	return failures, nil
}

// RetryParkedEmbeddingFailures clears terminal failure metadata and restores
// the selected nodes to the durable pending queue. An empty ID list retries
// every parked node.
func (ew *EmbedWorker) RetryParkedEmbeddingFailures(ctx context.Context, ids []storage.NodeID) (int, error) {
	ctx, origin := storage.AdmitVectorWork(ctx)
	if ew == nil || ew.storage == nil {
		return 0, errors.New("embedding worker storage is unavailable")
	}
	selected := make(map[storage.NodeID]struct{}, len(ids))
	for _, id := range ids {
		selected[id] = struct{}{}
	}
	failures, err := ew.ParkedEmbeddingFailures(ctx, 0)
	if err != nil {
		return 0, err
	}
	retried := 0
	for _, failure := range failures {
		if len(selected) > 0 {
			if _, ok := selected[failure.NodeID]; !ok {
				continue
			}
		}
		var node *storage.Node
		if reader, ok := ew.storage.(storage.NodeWithoutEmbeddingsReader); ok {
			node, err = reader.GetNodeWithoutEmbeddings(failure.NodeID)
		} else {
			node, err = ew.storage.GetNode(failure.NodeID)
		}
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return retried, err
		}
		node, err = storage.SnapshotNodeForEmbedding(node)
		if err != nil {
			return retried, err
		}
		embeddingutil.InvalidateManagedEmbeddings(node)
		// Clearing the embedding state deletes the sidecar records (meta
		// and chunk keys) so the node can be re-embedded cleanly.
		if err := ew.commitNodeReset(node, origin); err != nil {
			return retried, err
		}
		ew.addNodeToPendingEmbeddings(node.ID, origin)
		retried++
	}
	remaining := len(failures) - retried
	if remaining < 0 {
		remaining = 0
	}
	ew.parked.Store(int64(remaining))
	if retried > 0 {
		ew.signalTrigger()
	}
	return retried, nil
}

func (ew *EmbedWorker) scanParkedEmbeddingFailures(ctx context.Context, visit func(EmbeddingFailure) error) (int, error) {
	// Prefer the sidecar key-space scan: failure markers live in the
	// embedding metadata record, not the node body, so no node body is
	// decoded.
	if streamer, ok := ew.storage.(storage.EmbeddingFailureStreamer); ok {
		count := 0
		_, err := streamer.StreamParkedEmbeddingFailures(ctx, func(nodeID storage.NodeID, meta map[string]any) error {
			count++
			failure := EmbeddingFailure{NodeID: nodeID}
			failure.Error, _ = meta["embedding_error"].(string)
			failure.FailedAt, _ = meta["embedding_failed_at"].(string)
			return visit(failure)
		})
		return count, err
	}

	// Legacy body-based scan for storage engines without the sidecar space.
	count := 0
	inspect := func(node *storage.Node) error {
		if node == nil {
			return nil
		}
		failed, _ := node.EmbedMeta["embedding_failed"].(bool)
		if !failed {
			return nil
		}
		count++
		failure := EmbeddingFailure{NodeID: node.ID}
		failure.Error, _ = node.EmbedMeta["embedding_error"].(string)
		failure.FailedAt, _ = node.EmbedMeta["embedding_failed_at"].(string)
		return visit(failure)
	}
	// StreamNodesWithOptions is part of the storage.Engine contract, so the
	// lightweight scan is always available on the worker's engine.
	err := ew.storage.StreamNodesWithOptions(ctx, storage.StreamNodesOptions{Prefix: "", Projection: []string{}, StripEmbeddings: true}, inspect)
	return count, err
}

// Reset stops the current worker and restarts it fresh.
// This clears processed counts and the recently-processed cache,
// which is necessary when regenerating all embeddings.
func (ew *EmbedWorker) Reset() {
	ew.lifecycleMu.Lock()
	defer ew.lifecycleMu.Unlock()

	ew.mu.Lock()
	if ew.closed.Load() {
		ew.mu.Unlock()
		return
	}
	// Mark as resetting to prevent Trigger() from sending during reset
	wasRunning := ew.inFlight.Load() > 0
	ew.mu.Unlock()

	fmt.Println("🔄 Resetting embed worker for regeneration...")

	// Cancel context to stop current processing
	ew.cancel()

	// Stop pending debounced trigger timer.
	ew.triggerMu.Lock()
	if ew.triggerDebounceTimer != nil {
		ew.triggerDebounceTimer.Stop()
		ew.triggerDebounceTimer = nil
	}
	ew.triggerMu.Unlock()

	// Wait synchronously for previous workers to exit before reusing the WaitGroup.
	// This avoids "WaitGroup is reused before previous Wait has returned" panics
	// when Reset and Close overlap under load.
	ew.wg.Wait()
	if ew.closed.Load() {
		return
	}

	// Reset state under lock
	ew.mu.Lock()
	ew.initialScanDone = false
	ew.recentlyProcessed = make(map[string]recentNodeEmbedding)
	ew.loggedSkip = make(map[string]bool)
	ew.mu.Unlock()
	ew.claimMu.Lock()
	ew.claimedNodes = make(map[string]*nodeEmbeddingAttempt)
	ew.clearNodeEmbeddingRetriesLocked()
	ew.claimMu.Unlock()
	ew.processed.Store(0)
	ew.failed.Store(0)
	ew.parked.Store(0)
	ew.inFlight.Store(0)

	// Create new context (don't recreate trigger channel - just drain it)
	ew.ctx, ew.cancel = context.WithCancel(context.Background())

	// Drain any pending triggers
	select {
	case <-ew.trigger:
	default:
	}

	// Restart worker
	ew.wg.Add(1)
	go ew.worker()

	_ = wasRunning // suppress unused warning
	fmt.Println("✅ Embed worker reset complete, starting fresh scan")
}

// Close gracefully shuts down the worker.
func (ew *EmbedWorker) Close() {
	ew.lifecycleMu.Lock()
	defer ew.lifecycleMu.Unlock()

	ew.closed.Store(true)

	// Stop pending debounced trigger timer.
	ew.triggerMu.Lock()
	if ew.triggerDebounceTimer != nil {
		ew.triggerDebounceTimer.Stop()
		ew.triggerDebounceTimer = nil
	}
	ew.triggerMu.Unlock()

	// Stop any pending debounce timer
	ew.clusterDebounceMu.Lock()
	if ew.clusterDebounceTimer != nil {
		ew.clusterDebounceTimer.Stop()
		ew.clusterDebounceTimer = nil
	}
	ew.clusterDebounceMu.Unlock()

	ew.cancel()
	// Do NOT close trigger channel: Trigger() can still race and send, which would panic.
	// Context cancellation is enough to stop workers.
	// Wait synchronously for worker shutdown to complete.
	ew.wg.Wait()
	ew.claimMu.Lock()
	ew.clearNodeEmbeddingRetriesLocked()
	ew.claimMu.Unlock()
}

// scheduleClusteringDebounced accumulates embedding counts and debounces the k-means trigger.
// This prevents constant re-clustering when embeddings trickle in one at a time.
// The callback will fire after ClusterDebounceDelay of inactivity, if MinBatchSize is met.
func (ew *EmbedWorker) scheduleClusteringDebounced(processedCount int) {
	ew.clusterDebounceMu.Lock()
	defer ew.clusterDebounceMu.Unlock()

	// Accumulate the count
	ew.pendingClusterCount += processedCount

	// Cancel existing timer if any
	if ew.clusterDebounceTimer != nil {
		ew.clusterDebounceTimer.Stop()
	}

	// Get debounce delay from config (default 30s)
	delay := ew.config.ClusterDebounceDelay
	if delay == 0 {
		delay = 30 * time.Second
	}

	// Get minimum batch size from config (default 10)
	minBatch := ew.config.ClusterMinBatchSize
	if minBatch == 0 {
		minBatch = 10
	}

	// Schedule new timer
	ew.clusterDebounceRunning = true
	ew.clusterDebounceTimer = time.AfterFunc(delay, func() {
		ew.clusterDebounceMu.Lock()
		count := ew.pendingClusterCount
		ew.pendingClusterCount = 0
		ew.clusterDebounceRunning = false
		ew.clusterDebounceTimer = nil
		ew.clusterDebounceMu.Unlock()

		// Only trigger if we have enough embeddings
		if count >= minBatch && ew.onQueueEmpty != nil {
			fmt.Printf("🔬 Debounced k-means trigger: %d embeddings processed (waited %.0fs for more)\n", count, delay.Seconds())
			ew.onQueueEmpty(count)
		} else if count > 0 && count < minBatch {
			fmt.Printf("⏸️  Skipping k-means: only %d embeddings (min batch: %d)\n", count, minBatch)
		}
	})

	fmt.Printf("⏳ K-means debounce: %d pending embeddings, will trigger in %.0fs if no more arrive\n",
		ew.pendingClusterCount, delay.Seconds())
}

// worker runs the embedding loop.
func (ew *EmbedWorker) worker() {
	defer ew.wg.Done()

	fmt.Println("🧠 Embed worker started")

	// Short initial delay to let server start
	if !ew.waitForWorkDelay(500 * time.Millisecond) {
		return
	}

	recoveryInterval := ew.config.ScanInterval
	if recoveryInterval < minRefreshOnEmptyInterval {
		recoveryInterval = minRefreshOnEmptyInterval
	}
	for {
		readiness := ew.readinessNotification()
		if ew.closed.Load() || ew.ctx.Err() != nil {
			return
		}
		paused := ew.embeddingPaused()
		if ew.currentEmbedder() != nil && !paused {
			// Refresh the pending embeddings index on startup to catch any nodes
			// that need embedding (e.g., after restart, bulk import, or cleared embeddings).
			// Run this once for the whole worker pool to avoid duplicate startup scans/logs.
			ew.initialScanMu.Lock()
			ew.mu.Lock()
			doInitialScan := !ew.initialScanDone
			if doInitialScan {
				ew.initialScanDone = true
			}
			ew.mu.Unlock()
			if doInitialScan {
				fmt.Println("🔍 Initial scan for nodes needing embeddings...")
				// Refresh index to clean up stale entries from deleted nodes
				ew.refreshEmbeddingIndexIfDue(true)
				if _, err := ew.ParkedEmbeddingFailures(ew.ctx, 1); err != nil {
					fmt.Printf("⚠️  Failed to restore parked embedding count: %s\n", compactWorkerError(err, 300))
				}
			}
			ew.initialScanMu.Unlock()

			paused = ew.processUntilEmpty() == embedBatchPaused
		}

		// Only the foreground-pressure callback needs a readiness recheck: its
		// owner can change the answer without sending a notification. This does
		// not read storage. Model loading, restore release and writes signal us.
		// A pass that observed pressure must retain its bounded readiness retry
		// even if the pressure clears before this final sample.
		paused = ew.embeddingPaused() || paused
		delay := recoveryInterval
		if paused && ew.restoreHolds.Load() == 0 {
			delay = time.Second
		}
		if !paused {
			delay = ew.nodeEmbeddingRetryDelay(delay)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ew.ctx.Done():
			timer.Stop()
			fmt.Println("🧠 Embed worker stopped")
			return

		case <-ew.trigger:
			// A buffered signal also covers writes racing the final empty claim.
		case <-readiness:
			// Every worker rechecks model/pause readiness after a broadcast.

		case <-timer.C:
			if delay == recoveryInterval && ew.currentEmbedder() != nil && !ew.embeddingPaused() {
				// Catch writes/imports that bypass Enqueue, including missing
				// pending-index entries. Never scan on a short empty-queue tick.
				ew.refreshEmbeddingIndexIfDue(false)
			}
		}
		timer.Stop()
	}
}

func (ew *EmbedWorker) waitForWorkDelay(delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ew.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// embeddingPaused is shared by every provider/batching path, so none can
// claim nodes while a restore or foreground-pressure pause is active.
func (ew *EmbedWorker) embeddingPaused() bool {
	if ew.restoreHolds.Load() > 0 {
		return true
	}
	ew.mu.Lock()
	shouldYield := ew.shouldYield
	ew.mu.Unlock()
	return shouldYield != nil && shouldYield()
}

// processUntilEmpty keeps processing nodes until no more need embeddings.
// When the queue becomes empty, it schedules a debounced k-means clustering trigger.
func (ew *EmbedWorker) processUntilEmpty() embedBatchResult {
	batchProcessed := 0
	refreshed := false

	for {
		if ew.restoreHolds.Load() > 0 {
			return embedBatchPaused // the release triggers a new pass
		}
		if ew.currentEmbedder() == nil {
			return embedBatchIdle
		}
		select {
		case <-ew.ctx.Done():
			return embedBatchIdle
		default:
			// Preserve a pause separately from an empty lookup or a skipped item.
			result := ew.processNextBatchResult()
			if result != embedBatchWorked {
				if result != embedBatchPaused && !refreshed {
					if ew.embeddingPaused() {
						result = embedBatchPaused
					} else {
						refreshed = true
						if ew.refreshEmbeddingIndexIfDue(false) > 0 {
							continue
						}
					}
				}
				if batchProcessed > 0 && ew.onQueueEmpty != nil && ew.PendingEmbeddingsCount() == 0 {
					ew.scheduleClusteringDebounced(batchProcessed)
				}
				return result
			}
			batchProcessed++
			if !ew.waitForWorkDelay(50 * time.Millisecond) {
				return embedBatchIdle
			}
		}
	}
}

// nodeGone reports whether a pending node's read found it deleted: not
// found, or no node without an error. A failed read (for example while a
// restore replaces the store) isn't, and the node stays pending.
func nodeGone(node *storage.Node, err error) bool {
	return errors.Is(err, storage.ErrNotFound) || err == nil && node == nil
}

// holdForRestore keeps the worker from claiming nodes or scanning the store
// while a restore replaces it, and waits up to wait for the nodes it is
// embedding to finish, so none is read from a store being replaced (#1020).
// The returned release lets it work again and starts a scan of the restored
// store.
func (ew *EmbedWorker) holdForRestore(wait time.Duration) (release func()) {
	ew.restoreHolds.Add(1)
	for deadline := time.Now().Add(wait); ew.inFlight.Load() > 0 && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
	}
	return func() {
		ew.restoreHolds.Add(-1)
		ew.refreshMu.Lock()
		ew.lastRefreshAt = time.Time{}
		ew.refreshMu.Unlock()
		ew.signalAllWorkers()
	}
}

// processNextBatch finds and processes nodes without embeddings.
// Returns true if it did useful work (processed or permanently skipped a node).
// Returns false if there was nothing to process, if a node was temporarily
// skipped, or while a restore holds the worker.
func (ew *EmbedWorker) processNextBatch() bool {
	return ew.processNextBatchResult() == embedBatchWorked
}

type embedBatchResult uint8

const (
	embedBatchIdle embedBatchResult = iota
	embedBatchWorked
	embedBatchPaused
)

// processNextBatchResult preserves why a pass stopped for the wait owner.
func (ew *EmbedWorker) processNextBatchResult() embedBatchResult {
	if ew.ctx.Err() != nil {
		return embedBatchIdle
	}
	if ew.embeddingPaused() {
		return embedBatchPaused
	}
	if ew.processNextReadyBatch() {
		return embedBatchWorked
	}
	return embedBatchIdle
}

func (ew *EmbedWorker) processNextReadyBatch() bool {
	selection := ew.selectEmbeddingProvider()
	hasResolver, embedder := selection.resolver != nil, selection.provider
	if hasResolver {
		return ew.processNextResolvedBatch(selection)
	}
	if provider, ok := embedder.(embed.DocumentPropertyChunkEmbedder); ok && provider.UsesDocumentProperties() {
		return ew.processNextNode(selection)
	}
	if batcher, ok := embedder.(embed.DocumentBatchChunkEmbedder); ok && ew.config.EmbedBatchSize > 1 {
		if _, indexed := ew.storage.(EmbeddingIndexManager); indexed {
			return ew.processNextDocumentBatch(embedder, batcher, selection)
		}
	}
	return ew.processNextNode(selection)
}

func (ew *EmbedWorker) processNextResolvedBatch(selections ...embeddingSelection) bool {
	limit := ew.config.EmbedBatchSize
	if limit < 1 {
		limit = 1
	}
	nodes, skipped := ew.claimNodeBatch(limit, ew.embeddingSelection(selections))
	if len(nodes) == 0 {
		return skipped
	}

	progressed := skipped
	type resolvedGroup struct {
		provider        embed.Embedder
		batcher         embed.DocumentBatchChunkEmbedder
		propertyBatcher embed.DocumentPropertyBatchChunkEmbedder
		nodes           []*storage.Node
	}
	groups := make(map[string]*resolvedGroup)
	for _, node := range nodes {
		provider, err := ew.resolveAttemptEmbedder(node)
		if err != nil || provider == nil {
			if err == nil {
				err = errors.New("embedding provider is not configured")
			}
			progressed = ew.failNodeEmbedding(node, err) || progressed
			ew.releaseNodeClaim(node.ID)
			continue
		}
		if structured, ok := provider.(embed.DocumentPropertyChunkEmbedder); ok && structured.UsesDocumentProperties() {
			propertyBatcher, batchOK := provider.(embed.DocumentPropertyBatchChunkEmbedder)
			if !batchOK || limit == 1 {
				progressed = ew.processClaimedNode(node, provider) || progressed
				continue
			}
			key := fmt.Sprintf("%T:%p", provider, provider)
			group := groups[key]
			if group == nil {
				group = &resolvedGroup{provider: provider, propertyBatcher: propertyBatcher}
				groups[key] = group
			}
			group.nodes = append(group.nodes, node)
			continue
		}
		batcher, ok := provider.(embed.DocumentBatchChunkEmbedder)
		if !ok || limit == 1 {
			progressed = ew.processClaimedNode(node, provider) || progressed
			continue
		}
		key := fmt.Sprintf("%T:%p", provider, provider)
		group := groups[key]
		if group == nil {
			group = &resolvedGroup{provider: provider, batcher: batcher}
			groups[key] = group
		}
		group.nodes = append(group.nodes, node)
	}

	opts := embeddingutil.EmbedTextOptionsFromFields(ew.config.PropertiesInclude, ew.config.PropertiesExclude, ew.config.IncludeLabels)
	for _, group := range groups {

		sources := make([]embeddingutil.EmbedText, len(group.nodes))
		texts := make([]string, len(group.nodes))
		properties := make([]map[string]any, len(group.nodes))
		for index, node := range group.nodes {
			sources[index] = embeddingutil.BuildEmbedText(node.Properties, node.Labels, opts)
			texts[index] = sources[index].Text
			properties[index] = node.Properties
		}
		var results []*embed.DocumentChunkResult
		var resultErrors []error
		if group.propertyBatcher != nil {
			results, resultErrors = ew.embedDocumentPropertyBatchIsolated(group.propertyBatcher, texts, properties)
		} else {
			results, resultErrors = ew.embedDocumentBatchIsolated(group.batcher, texts)
		}
		for _, resultErr := range resultErrors {
			if resultErr != nil {
				diagnosticErr := resultErr
				for _, node := range group.nodes {
					diagnosticErr = ew.redactFailure(node.ID, diagnosticErr)
				}
				fmt.Printf("⚠️  Failed to embed %d-node batch: %s\n", len(group.nodes), compactWorkerError(diagnosticErr, 300))
				break
			}
		}
		for index, node := range group.nodes {
			result := documentNodeEmbeddingResult(results[index], resultErrors[index])
			progressed = ew.persistEmbeddedNode(node, sources[index], result, group.provider) || progressed
			ew.releaseNodeClaim(node.ID)
		}
	}
	if progressed {
		ew.signalTrigger()
	}
	return progressed
}

func (ew *EmbedWorker) processNextNode(selections ...embeddingSelection) bool {
	// Check for cancellation at the start
	select {
	case <-ew.ctx.Done():
		return false
	default:
	}

	if ew.embeddingPaused() {
		return false
	}

	node, progressed := ew.claimNextNodeResult(ew.embeddingSelection(selections))
	if node == nil {
		return progressed
	}

	// Check for cancellation before processing
	select {
	case <-ew.ctx.Done():
		ew.releaseNodeClaim(node.ID)
		return false
	default:
	}

	fmt.Printf("🔄 Processing node %s for embedding...\n", node.ID)

	// IMPORTANT: Deep copy properties to avoid race conditions
	// The node from storage may be accessed by other goroutines (e.g., HTTP handlers)
	// Modifying the Properties map directly causes "concurrent map iteration and map write"
	node = copyNodeForEmbedding(node)
	provider, resolveErr := ew.resolveAttemptEmbedder(node)
	if resolveErr != nil || provider == nil {
		if resolveErr == nil {
			resolveErr = errors.New("embedding provider is not configured")
		}
		progressed := ew.failNodeEmbedding(node, resolveErr)
		ew.releaseNodeClaim(node.ID)
		return progressed
	}
	return ew.processClaimedNode(node, provider)
}

func (ew *EmbedWorker) processClaimedNode(node *storage.Node, provider embed.Embedder) bool {
	defer ew.releaseNodeClaim(node.ID)

	// Build text for embedding (labels and properties per config include/exclude)
	opts := embeddingutil.EmbedTextOptionsFromFields(ew.config.PropertiesInclude, ew.config.PropertiesExclude, ew.config.IncludeLabels)
	source := embeddingutil.BuildEmbedText(node.Properties, node.Labels, opts)
	text := source.Text

	// Embed documents through the provider-managed document path when available.
	// Voyage contextualized mode uses this to return provider-generated chunks;
	// other providers fall back to deterministic local chunking + micro-batches.
	propertyEmbedder, usesProperties := provider.(embed.DocumentPropertyChunkEmbedder)
	var result nodeEmbeddingResult
	if usesProperties && propertyEmbedder.UsesDocumentProperties() {
		document, err := propertyEmbedder.EmbedDocumentPropertyChunks(ew.ctx, text, node.Properties, ew.config.ChunkSize, ew.config.ChunkOverlap)
		ew.waitAfterProviderRequest()
		result = documentNodeEmbeddingResult(document, err)
	} else {
		result.chunks, result.vectors, result.metadata, result.err = ew.embedDocumentWith(provider, text, node.ID)
	}

	return ew.persistEmbeddedNode(node, source, result, provider)
}

// processNextDocumentBatch embeds a batch of nodes with batcher, the batch
// interface of provider. The whole batch is attributed to that one provider
// (retry gate, outcome, persisted metadata), even if SetEmbedder replaces the
// worker's embedder meanwhile.
func (ew *EmbedWorker) processNextDocumentBatch(provider embed.Embedder, batcher embed.DocumentBatchChunkEmbedder, selections ...embeddingSelection) bool {
	select {
	case <-ew.ctx.Done():
		return false
	default:
	}

	limit := ew.config.EmbedBatchSize
	nodes, skipped := ew.claimNodeBatch(limit, ew.embeddingSelection(selections))
	if len(nodes) == 0 {
		return skipped
	}

	progressed := skipped
	opts := embeddingutil.EmbedTextOptionsFromFields(ew.config.PropertiesInclude, ew.config.PropertiesExclude, ew.config.IncludeLabels)
	sources := make([]embeddingutil.EmbedText, len(nodes))
	texts := make([]string, len(nodes))
	for i, node := range nodes {
		sources[i] = embeddingutil.BuildEmbedText(node.Properties, node.Labels, opts)
		texts[i] = sources[i].Text
	}
	results, resultErrors := ew.embedDocumentBatchIsolated(batcher, texts)
	for i, node := range nodes {
		result := documentNodeEmbeddingResult(results[i], resultErrors[i])
		progressed = ew.persistEmbeddedNode(node, sources[i], result, provider) || progressed
		ew.releaseNodeClaim(node.ID)
	}
	if progressed {
		ew.signalTrigger()
	}
	return progressed
}

func (ew *EmbedWorker) embedDocumentBatchIsolated(batcher embed.DocumentBatchChunkEmbedder, texts []string) ([]*embed.DocumentChunkResult, []error) {
	return ew.embedDocumentResultsIsolated(len(texts), func(start, end int) ([]*embed.DocumentChunkResult, error) {
		return batcher.EmbedDocumentBatchChunks(ew.ctx, texts[start:end], ew.config.ChunkSize, ew.config.ChunkOverlap)
	})
}

func (ew *EmbedWorker) embedDocumentPropertyBatchIsolated(batcher embed.DocumentPropertyBatchChunkEmbedder, texts []string, properties []map[string]any) ([]*embed.DocumentChunkResult, []error) {
	return ew.embedDocumentResultsIsolated(len(texts), func(start, end int) ([]*embed.DocumentChunkResult, error) {
		return batcher.EmbedDocumentPropertyBatchChunks(
			ew.ctx,
			texts[start:end],
			properties[start:end],
			ew.config.ChunkSize,
			ew.config.ChunkOverlap,
		)
	})
}

func (ew *EmbedWorker) embedDocumentResultsIsolated(count int, request func(start, end int) ([]*embed.DocumentChunkResult, error)) ([]*embed.DocumentChunkResult, []error) {
	results := make([]*embed.DocumentChunkResult, count)
	errs := make([]error, count)
	if count == 0 {
		return results, errs
	}
	var run func(start, end int)
	run = func(start, end int) {
		batch, err := request(start, end)
		ew.waitAfterProviderRequest()
		if err == nil && len(batch) == end-start {
			copy(results[start:end], batch)
			return
		}
		if retryable, classified := embeddingErrorRetryability(err); err != nil && classified && retryable {
			for i := start; i < end; i++ {
				errs[i] = err
			}
			return
		}
		if end-start > 1 {
			mid := start + (end-start)/2
			run(start, mid)
			run(mid, end)
			return
		}
		if err == nil {
			err = fmt.Errorf("embedding document count mismatch: got %d, expected 1", len(batch))
		}
		errs[start] = err
	}
	run(0, count)
	return results, errs
}

func (ew *EmbedWorker) waitAfterProviderRequest() {
	if ew.config.BatchDelay <= 0 {
		return
	}
	select {
	case <-ew.ctx.Done():
	case <-time.After(ew.config.BatchDelay):
	}
}

func documentResultMeta(result *embed.DocumentChunkResult) map[string]any {
	meta := make(map[string]any)
	if result == nil {
		return meta
	}
	if result.Source != nil {
		meta[embeddingsource.MetaKey] = result.Source.Metadata()
	}
	if result.ChunkerVersion != "" {
		meta["chunker_version"] = result.ChunkerVersion
	}
	if result.TotalTokens > 0 {
		meta["embedding_total_tokens"] = result.TotalTokens
	}
	if len(result.Chunks) > 0 && (result.ChunkerVersion != "" || result.TotalTokens > 0) {
		meta["chunk_texts"] = append([]string(nil), result.Chunks...)
	}
	return meta
}

// writeEmbeddedNode commits a newly requested provider result. Failed saves
// discard that result and record only the per-node DLQ state.
func (ew *EmbedWorker) writeEmbeddedNode(node *storage.Node) bool {
	updateErr := ew.commitNodeOutcome(node)
	if errors.Is(updateErr, storage.ErrEmbeddingSourceChanged) {
		ew.rejectNodeOutcome(node.ID)
		fmt.Printf("🔁 Node %s changed while it was being embedded; re-embedding its new content\n", node.ID)
		return true // releaseNodeClaim alone restores MAIN, exactly once.
	}
	if errors.Is(updateErr, storage.ErrNotFound) {
		// Node was deleted - remove from pending index and skip.
		fmt.Printf("⚠️  Node %s was deleted before embedding could be saved - skipping\n", node.ID)
		ew.markNodeEmbedded(node.ID)
		ew.dropNodeEmbeddingRetry(node.ID)
		return true // Removing a deleted candidate advances the drain.
	}
	if updateErr != nil {
		fmt.Printf("⚠️  Failed to update node embedding state: %s; re-queuing for retry\n", compactWorkerError(updateErr, 300))
		return ew.failNodeEmbedding(node, updateErr)
	}

	// Log success with appropriate message
	if len(node.ChunkEmbeddings) > 0 {
		dims := 0
		if len(node.ChunkEmbeddings[0]) > 0 {
			dims = len(node.ChunkEmbeddings[0])
		}
		if len(node.ChunkEmbeddings) > 1 {
			fmt.Printf("✅ Embedded %s (%d dims, %d chunks)\n", node.ID, dims, len(node.ChunkEmbeddings))
		} else {
			fmt.Printf("✅ Embedded %s (%d dims)\n", node.ID, dims)
		}
	}

	// Trigger another check immediately if there might be more.
	// Internal chaining should not be debounced.
	ew.signalTrigger()

	return true // Successfully processed
}

func compactWorkerError(err error, maxLen int) string {
	if err == nil {
		return ""
	}
	text := strings.TrimSpace(err.Error())
	if maxLen <= 0 || len(text) <= maxLen {
		return text
	}
	return text[:maxLen] + "...(truncated)"
}

// retryableEmbeddingError is implemented by providers that can distinguish a
// permanent request error (such as an HTTP 4xx) from a transient failure.
type retryableEmbeddingError interface {
	Retryable() bool
}

func isRetryableEmbeddingError(err error) bool {
	retryable, classified := embeddingErrorRetryability(err)
	if classified {
		return retryable
	}
	// Existing providers do not all expose error classification. Preserve their
	// retry behavior until they can state that an error is permanent.
	return true
}

func embeddingErrorRetryability(err error) (retryable, classified bool) {
	var classification retryableEmbeddingError
	if errors.As(err, &classification) {
		return classification.Retryable(), true
	}
	return false, false
}

func retryDelay(err error) (time.Duration, bool) {
	type delayedRetry interface{ RetryDelay() time.Duration }
	var delayed delayedRetry
	if errors.As(err, &delayed) {
		return delayed.RetryDelay(), true
	}
	return 0, false
}

// markRecentlyProcessed records the accepted body revision. The time bounds
// retention of that record; a different revision never waits for its expiry.
func (ew *EmbedWorker) markRecentlyProcessed(node *storage.Node) {
	ew.mu.Lock()
	defer ew.mu.Unlock()
	if ew.recentlyProcessed == nil {
		ew.recentlyProcessed = make(map[string]recentNodeEmbedding)
	}
	ew.recentlyProcessed[string(node.ID)] = recentNodeEmbedding{at: time.Now(), revision: node.EmbeddingRevision()}
}

// markNodeEmbeddingFailed records a permanent provider failure outside user
// properties, removes it from the pending index, and lets later content edits
// retry it by clearing managed embedding metadata during invalidation.
func (ew *EmbedWorker) markNodeEmbeddingFailed(nodeID storage.NodeID, embedErr error, sources ...*storage.Node) {
	embedErr = ew.redactFailure(nodeID, embedErr)
	var node *storage.Node
	if len(sources) != 0 {
		node = sources[0]
	} else {
		var err error
		node, err = ew.storage.GetNode(nodeID)
		if err == nil {
			node, err = storage.SnapshotNodeForEmbedding(node)
		}
		if err != nil || node == nil {
			if nodeGone(node, err) {
				ew.markNodeEmbedded(nodeID)
			}
			return
		}
		// Explicit failure administration also gets a fresh owned attempt.
		selection := ew.selectEmbeddingProvider()
		ew.claimMu.Lock()
		if ew.claimedNodes == nil {
			ew.claimedNodes = make(map[string]*nodeEmbeddingAttempt)
		}
		ew.observeNodeRevisionLocked(node)
		ew.bindNodeAttemptLocked(node, selection)
		ew.claimMu.Unlock()
		defer ew.releaseNodeClaim(nodeID)
	}
	node = copyNodeForEmbedding(node)
	if node.EmbedMeta == nil {
		node.EmbedMeta = make(map[string]any)
	}
	node.EmbedMeta["has_embedding"] = false
	node.EmbedMeta["embedding_failed"] = true
	node.EmbedMeta["embedding_error"] = compactWorkerError(embedErr, 300)
	node.EmbedMeta["embedding_failed_at"] = time.Now().UTC().Format(time.RFC3339)

	if updateErr := ew.commitNodeOutcome(node); updateErr != nil {
		if errors.Is(updateErr, storage.ErrEmbeddingSourceChanged) {
			ew.rejectNodeOutcome(nodeID)
			return
		}
		fmt.Printf("⚠️  Failed to persist permanent embedding failure for node %s: %s; re-queuing\n", nodeID, compactWorkerError(updateErr, 300))
		// The attempt owner keeps this DLQ entry retryable until the marker
		// commits. Re-added MAIN markers cannot hide it from draining.
		ew.addNodeToPendingEmbeddings(nodeID, node.EmbeddingAttempt().VectorWorkOrigin())
		return
	}

	// The writeback removed the pending marker in its own commit (#889).
	// The accepted outcome callback moved ownership to the durable terminal marker.
	fmt.Printf("⛔ Permanent embedding failure for node %s: %s\n", nodeID, compactWorkerError(embedErr, 300))
}

// EmbeddingFinder interface for efficient node lookup
type EmbeddingFinder interface {
	FindNodeNeedingEmbedding() *storage.Node
}

// EmbeddingIndexManager is an optional interface for storage engines
// that support efficient pending embeddings tracking via Badger secondary index.
type EmbeddingIndexManager interface {
	RefreshPendingEmbeddingsIndex() int
	MarkNodeEmbedded(nodeID storage.NodeID)
}

// claimNextNode reserves one pending node for this worker pool before the
// provider call begins. The in-memory reservation closes the visibility gap
// between removing a storage-side pending marker and persisting embeddings.
func (ew *EmbedWorker) claimNextNode() *storage.Node {
	node, _ := ew.claimNextNodeResult()
	return node
}

// claimNextNodeResultFrom reserves one fresh authoritative body and provider selection.
func (ew *EmbedWorker) claimNextNodeResultFrom(mainOnly bool, selections ...embeddingSelection) (*storage.Node, bool) {
	selection := ew.embeddingSelection(selections)
	ew.claimMu.Lock()
	defer ew.claimMu.Unlock()
	ew.applyNodeUpdatesLocked()
	if selection.generation != ew.embeddingEpoch.Load() {
		return nil, false
	}

	pendingID := ew.nextEmbeddingNodeID(time.Now(), mainOnly)
	if pendingID == "" {
		return nil, false
	}
	if ew.claimedNodes == nil {
		ew.claimedNodes = make(map[string]*nodeEmbeddingAttempt)
	}
	id := string(pendingID)
	if _, claimed := ew.claimedNodes[id]; claimed {
		return nil, false
	}

	node, err := ew.storage.GetNode(pendingID)
	if err == nil {
		node, err = storage.SnapshotNodeForEmbedding(node)
	}
	if err != nil || node == nil {
		// A deleted node needs no embedding; one that couldn't be read (for
		// example while a restore replaces the store) stays pending, as in
		// markNodeEmbeddingFailed.
		if nodeGone(node, err) {
			ew.dropNodeEmbeddingRetryLocked(pendingID)
			ew.markNodeEmbedded(pendingID)
			return nil, true
		}
		return nil, false
	}
	if ew.wasRecentlyProcessed(node.ID) {
		return nil, false
	}

	ew.applyNodeUpdatesLocked() // Include notifications received while finding/reading the fresh body.
	ew.observeNodeRevisionLocked(node)
	ew.bindNodeAttemptLocked(node, selection)
	ew.markNodeEmbedded(node.ID)
	return copyNodeForEmbedding(node), false
}

func (ew *EmbedWorker) releaseNodeClaim(nodeID storage.NodeID) {
	ew.claimMu.Lock()
	id := string(nodeID)
	attempt, claimed := ew.claimedNodes[id]
	if claimed {
		delete(ew.claimedNodes, id)
		ew.inFlight.Add(-1)
	}
	if claimed && (attempt.invalidated || attempt.token.ProviderGeneration != ew.embeddingEpoch.Load() || ew.ctx.Err() != nil) {
		// An update needs another attempt; a deletion must not recreate its
		// pending marker when the invalidated claim finishes.
		if node, err := ew.embeddingSnapshot(nodeID); !nodeGone(node, err) {
			ew.addNodeToPendingEmbeddings(nodeID, attempt.selection.vectorWorkOrigin)
		}
	}
	ew.claimMu.Unlock()
}

func (ew *EmbedWorker) wasRecentlyProcessed(nodeID storage.NodeID) bool {
	ew.mu.Lock()
	defer ew.mu.Unlock()
	if ew.recentlyProcessed == nil {
		ew.recentlyProcessed = make(map[string]recentNodeEmbedding)
	}
	if ew.loggedSkip == nil {
		ew.loggedSkip = make(map[string]bool)
	}
	id := string(nodeID)
	if previous, ok := ew.recentlyProcessed[id]; ok && time.Since(previous.at) < 30*time.Second {
		same := true
		if ew.storage != nil {
			current, err := ew.embeddingSnapshot(nodeID)
			if err != nil {
				same = false
			} else {
				same, err = previous.revision.MatchesNode(current)
				same = same && err == nil
			}
		}
		if same {
			if !ew.loggedSkip[id] {
				ew.loggedSkip[id] = true
				fmt.Printf("Skipping node %s: same processed body revision\n", nodeID)
			}
			return true
		}
		delete(ew.recentlyProcessed, id)
	}
	delete(ew.loggedSkip, id)
	for candidate, previous := range ew.recentlyProcessed {
		if time.Since(previous.at) > time.Minute {
			delete(ew.recentlyProcessed, candidate)
			delete(ew.loggedSkip, candidate)
		}
	}
	return false
}

// findNodeWithoutEmbedding finds a single node that needs embedding.
// Uses efficient streaming iteration if available, falls back to AllNodes.
func (ew *EmbedWorker) findNodeWithoutEmbedding() *storage.Node {
	// Try efficient streaming method first (BadgerEngine, WALEngine)
	if finder, ok := ew.storage.(EmbeddingFinder); ok {
		return finder.FindNodeNeedingEmbedding()
	}

	// Fallback: use storage helper
	return storage.FindNodeNeedingEmbedding(ew.storage)
}

// refreshEmbeddingIndex refreshes the pending embeddings index
// to catch any nodes that were added during processing.
// Returns the number of stale entries removed.
func (ew *EmbedWorker) refreshEmbeddingIndex() int {
	if mgr, ok := ew.storage.(EmbeddingIndexManager); ok {
		return mgr.RefreshPendingEmbeddingsIndex()
	}
	return 0
}

// refreshEmbeddingIndexIfDue runs a full pending-index refresh at most once per
// minRefreshOnEmptyInterval unless forced. This avoids repeated full scans when
// the worker is repeatedly triggered by bursty writes.
func (ew *EmbedWorker) refreshEmbeddingIndexIfDue(force bool) int {
	ew.refreshMu.Lock()
	if !force && !ew.lastRefreshAt.IsZero() && time.Since(ew.lastRefreshAt) < minRefreshOnEmptyInterval {
		ew.refreshMu.Unlock()
		return 0
	}
	ew.lastRefreshAt = time.Now()
	ew.refreshMu.Unlock()
	return ew.refreshEmbeddingIndex()
}

// markNodeEmbedded removes a node from the pending embeddings index.
func (ew *EmbedWorker) markNodeEmbedded(nodeID storage.NodeID) {
	if mgr, ok := ew.storage.(EmbeddingIndexManager); ok {
		mgr.MarkNodeEmbedded(nodeID)
	}
}

// addNodeToPendingEmbeddings re-queues admitted work for embedding (e.g. after
// a failed attempt so another worker can retry). The origin is required.
func (ew *EmbedWorker) addNodeToPendingEmbeddings(nodeID storage.NodeID, origin uint64) {
	if adder, ok := ew.storage.(interface{ AddToPendingEmbeddingsWithOrigin(storage.NodeID, uint64) }); ok {
		adder.AddToPendingEmbeddingsWithOrigin(nodeID, origin)
	}
}

// embedChunksInBatches embeds chunks using bounded request sizes.
// This avoids sending massive single EmbedBatch requests for large files.
func (ew *EmbedWorker) embedChunksInBatches(chunks []string, nodeID storage.NodeID) ([][]float32, error) {
	return ew.embedChunksInBatchesWith(ew.currentEmbedder(), chunks, nodeID)
}

func (ew *EmbedWorker) embedChunksInBatchesWith(provider embed.Embedder, chunks []string, nodeID storage.NodeID) ([][]float32, error) {
	if len(chunks) == 0 {
		return nil, nil
	}
	batchSize := ew.config.EmbedBatchSize
	if batchSize < 1 {
		batchSize = 32
	}
	allEmbeddings := make([][]float32, 0, len(chunks))
	for start := 0; start < len(chunks); start += batchSize {
		end := start + batchSize
		if end > len(chunks) {
			end = len(chunks)
		}
		batch := chunks[start:end]
		batchEmbeddings, err := ew.embedBatchWithRetryFor(provider, batch)
		if err != nil {
			return nil, localizedError(localization.NornicDBCoreEmbedBatchFailed(start+1, end, len(chunks), string(nodeID), err), err)
		}
		// Preserve response/input association before flattening. A later
		// underfull response cannot cancel an earlier overfull one. This
		// transport error is carried to the common result-acceptance owner;
		// it does not certify vector usability or publish an outcome.
		if len(batchEmbeddings) != len(batch) {
			return nil, localizedError(localization.NornicDBCoreEmbeddingCountMismatch(string(nodeID), len(batchEmbeddings), len(batch)), nil)
		}
		allEmbeddings = append(allEmbeddings, batchEmbeddings...)
	}
	return allEmbeddings, nil
}

func (ew *EmbedWorker) embedDocumentWith(provider embed.Embedder, text string, nodeID storage.NodeID) ([]string, [][]float32, map[string]any, error) {
	if documentEmbedder, ok := provider.(embed.DocumentChunkEmbedder); ok {
		result, err := documentEmbedder.EmbedDocumentChunks(ew.ctx, text, ew.config.ChunkSize, ew.config.ChunkOverlap)
		ew.waitAfterProviderRequest()
		if err != nil {
			return nil, nil, nil, err
		}
		meta := documentResultMeta(result)
		if result != nil {
			return result.Chunks, result.Embeddings, meta, nil
		}
		return nil, nil, meta, nil
	}

	chunker, ok := provider.(deterministicTextChunker)
	if !ok {
		return nil, nil, nil, fmt.Errorf("embedder %T does not support deterministic token chunking", provider)
	}
	chunks, err := chunker.ChunkText(text, ew.config.ChunkSize, ew.config.ChunkOverlap)
	if err != nil {
		return nil, nil, nil, err
	}
	embeddings, err := ew.embedChunksInBatchesWith(provider, chunks, nodeID)
	if err != nil {
		return nil, nil, nil, err
	}
	return chunks, embeddings, nil, nil
}

// embedBatchWithRetry retries a single micro-batch with backoff.
func (ew *EmbedWorker) embedBatchWithRetry(chunks []string) ([][]float32, error) {
	return ew.embedBatchWithRetryFor(ew.currentEmbedder(), chunks)
}

func (ew *EmbedWorker) embedBatchWithRetryFor(provider embed.Embedder, chunks []string) ([][]float32, error) {
	if err := ew.ctx.Err(); err != nil {
		return nil, err
	}
	// One request per queue attempt. Keep the call in the joined worker:
	// DB.Close must not release its provider while this request still runs.
	var vectors [][]float32
	var err error
	if typed, ok := provider.(embed.TypedEmbedder); ok {
		vectors, err = typed.EmbedBatchWithInputType(ew.ctx, chunks, embed.InputTypeDocument)
	} else {
		vectors, err = provider.EmbedBatch(ew.ctx, chunks)
	}
	if canceled := ew.ctx.Err(); canceled != nil {
		return nil, canceled
	}
	ew.waitAfterProviderRequest()
	return vectors, err
}

// averageEmbeddings computes the element-wise average of multiple embeddings.
func averageEmbeddings(embeddings [][]float32) []float32 {
	if len(embeddings) == 0 {
		return nil
	}
	if len(embeddings) == 1 {
		return embeddings[0]
	}

	dims := len(embeddings[0])
	avg := make([]float32, dims)

	for _, emb := range embeddings {
		for i, v := range emb {
			if i < dims {
				avg[i] += v
			}
		}
	}

	n := float32(len(embeddings))
	for i := range avg {
		avg[i] /= n
	}

	return avg
}

// MarshalJSON for worker stats.
func (s WorkerStats) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"running":   s.Running,
		"in_flight": s.InFlight,
		"pending":   s.Pending,
		"processed": s.Processed,
		"failed":    s.Failed,
		"parked":    s.Parked,
	})
}

// copyNodeForEmbedding creates a deep copy of a node to avoid race conditions.
// The original node from storage may be accessed by other goroutines (HTTP handlers,
// search service, etc.) Modifying the Properties map directly while another goroutine
// iterates over it causes "concurrent map iteration and map write" panic.
//
// This function copies:
//   - All scalar fields (ID, Labels, Embedding, etc.)
//   - Deep copy of Properties map
func copyNodeForEmbedding(src *storage.Node) *storage.Node {
	if src == nil {
		return nil
	}

	// Create a new node with copied scalar fields
	dst := &storage.Node{
		ID:        src.ID,
		Labels:    make([]string, len(src.Labels)),
		CreatedAt: src.CreatedAt,
		UpdatedAt: src.UpdatedAt,
	}

	dst.CopyEmbeddingIdentityFrom(src)

	// Copy labels
	copy(dst.Labels, src.Labels)

	// Copy chunk embeddings if present (always stored in ChunkEmbeddings, even single chunk = array of 1)
	if len(src.ChunkEmbeddings) > 0 {
		dst.ChunkEmbeddings = make([][]float32, len(src.ChunkEmbeddings))
		for i, emb := range src.ChunkEmbeddings {
			dst.ChunkEmbeddings[i] = make([]float32, len(emb))
			copy(dst.ChunkEmbeddings[i], emb)
		}
	}

	// Deep copy Properties map - this is the critical part to avoid race condition
	if src.Properties != nil {
		dst.Properties = make(map[string]any, len(src.Properties))
		for k, v := range src.Properties {
			dst.Properties[k] = v // Shallow copy of values is OK for our use case
		}
	}

	return dst
}

// Legacy aliases for compatibility with existing code
type EmbedQueue = EmbedWorker
type EmbedQueueConfig = EmbedWorkerConfig
type QueueStats = WorkerStats

func DefaultEmbedQueueConfig() *EmbedQueueConfig {
	return DefaultEmbedWorkerConfig()
}

func NewEmbedQueue(embedder embed.Embedder, storage storage.Engine, config *EmbedQueueConfig) *EmbedQueue {
	return NewEmbedWorker(embedder, storage, config)
}

// Enqueue is a wakeup/read hint. Only an observed body-revision change resets
// attempts or bypasses the processed-body wait, even when notifications repeat.
func (ew *EmbedWorker) Enqueue(nodeID string) {
	if ew.closed.Load() {
		return
	}
	// Do not wait on claimMu/storage: updates also wake an idle empty claim.
	ew.mu.Lock()
	if ew.nodeUpdates == nil {
		ew.nodeUpdates = make(map[string]struct{})
	}
	ew.nodeUpdates[nodeID] = struct{}{}
	ew.nodeUpdatesPending.Store(true)
	ew.mu.Unlock()
	ew.Trigger()
}
