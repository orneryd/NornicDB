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

type providerRetryState struct {
	failures int
	retryAt  time.Time
}

// EmbedWorker manages async embedding generation using a pull-based model.
// On each cycle, it scans for nodes without embeddings and processes them.
type EmbedWorker struct {
	embedder         embed.Embedder
	embedderResolver func(storage.NodeID) (embed.Embedder, error)
	storage          storage.Engine
	config           *EmbedWorkerConfig

	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	lifecycleMu sync.Mutex

	// Trigger channel to wake up worker immediately
	trigger chan struct{}
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

	// Recently processed node IDs to prevent re-processing before DB commit is visible
	// This prevents the same node being processed multiple times in quick succession
	recentlyProcessed map[string]time.Time

	// Track nodes we've already logged as skipped (to avoid log spam)
	loggedSkip map[string]bool
	// providerRetries coordinates a provider-wide cooldown after transient
	// failures so retained queue work cannot hot-loop against an outage.
	providerRetries map[string]providerRetryState

	// Debounce state for k-means clustering trigger
	clusterDebounceTimer   *time.Timer
	clusterDebounceMu      sync.Mutex
	pendingClusterCount    int  // Accumulated count for debounced callback
	clusterDebounceRunning bool // Whether a debounce timer is active

	// claimMu serializes find+claim. claimedNodes remains authoritative while a
	// provider request is in flight because storage-side pending-index removals
	// are not guaranteed to become visible immediately.
	claimMu      sync.Mutex
	claimedNodes map[string]struct{}

	// workersStarted is true once StartWorkers() has been called (used when DeferWorkerStart is true).
	workersStarted bool

	// initialScanDone ensures startup refresh/index scan runs once for the whole worker pool,
	// not once per worker goroutine.
	initialScanDone bool

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
	ScanInterval time.Duration // How often to scan for nodes without embeddings (default: 5s)
	BatchDelay   time.Duration // Minimum delay after each provider request (default: 500ms)
	MaxRetries   int           // Max attempts within one provider request cycle (default: 3)
	// ProviderRetryBackoff and ProviderRetryBackoffMax control the cooldown
	// between queue-level retries after transient provider failures. MaxRetries
	// still bounds attempts within one provider call; it never parks a node.
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
		recentlyProcessed: make(map[string]time.Time),
		loggedSkip:        make(map[string]bool),
		providerRetries:   make(map[string]providerRetryState),
		claimedNodes:      make(map[string]struct{}),
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
	ew.mu.Lock()
	ew.embedder = embedder
	ew.mu.Unlock()
	// Trigger immediate processing now that embedder is available
	ew.TriggerImmediate()
}

// SetEmbedderResolver selects a provider for a fully-qualified node ID. This
// supports per-database model spaces without coupling the worker to a provider.
func (ew *EmbedWorker) SetEmbedderResolver(resolver func(storage.NodeID) (embed.Embedder, error)) {
	ew.mu.Lock()
	ew.embedderResolver = resolver
	ew.mu.Unlock()
}

func (ew *EmbedWorker) resolveEmbedder(nodeID storage.NodeID) (embed.Embedder, error) {
	ew.mu.Lock()
	resolver := ew.embedderResolver
	fallback := ew.embedder
	ew.mu.Unlock()
	if resolver == nil {
		return fallback, nil
	}
	provider, err := resolver(nodeID)
	if err != nil {
		return nil, err
	}
	if provider == nil {
		return fallback, nil
	}
	return provider, nil
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

// WorkerStats returns current worker statistics.
type WorkerStats struct {
	Running   bool `json:"running"`
	InFlight  int  `json:"in_flight"`
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

// Stats returns current worker statistics.
// QueueLen returns the current pending-embedding queue depth for the
// observability nornicdb_embed_queue_depth GaugeFunc (Plan 04-05 D-15b).
//
// EmbedWorker is pull-based: there is no in-memory queue of work items
// waiting to be processed. Instead, the worker periodically scans
// storage for nodes lacking embeddings (the storage-side
// "pending-embedding index" is the durable queue). For the M1 scrape
// surface we report the trigger channel buffer depth (a coarse upper
// bound on outstanding wake-up signals); deeper visibility into the
// storage-side pending count is deferred to a future plan that wires
// the AddToPendingEmbeddings counter through here.
//
// The metric value is therefore a lower-bound on actual outstanding
// work — when alerts trigger SREs should also check the storage-side
// pending-embeddings index. Documented in CONTEXT D-15b.
func (ew *EmbedWorker) QueueLen() int {
	if ew == nil {
		return 0
	}
	return len(ew.trigger)
}

func (ew *EmbedWorker) Stats() WorkerStats {
	inFlight := int(ew.inFlight.Load())
	return WorkerStats{
		Running:   inFlight > 0,
		InFlight:  inFlight,
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
		embeddingutil.InvalidateManagedEmbeddings(node)
		if updater, ok := ew.storage.(interface{ UpdateNodeEmbedding(*storage.Node) error }); ok {
			err = updater.UpdateNodeEmbedding(node)
		} else {
			err = ew.storage.UpdateNode(node)
		}
		if err != nil {
			return retried, err
		}
		ew.addNodeToPendingEmbeddings(node.ID)
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
	if reader, ok := ew.storage.(storage.PrefixNodeWithoutEmbeddingsReader); ok {
		err := reader.StreamNodesByPrefixWithoutEmbeddings(ctx, "", inspect)
		return count, err
	}
	err := storage.StreamNodesWithFallback(ctx, ew.storage, 256, inspect)
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
	ew.recentlyProcessed = make(map[string]time.Time)
	ew.loggedSkip = make(map[string]bool)
	ew.mu.Unlock()
	ew.claimMu.Lock()
	ew.claimedNodes = make(map[string]struct{})
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

	// Wait for embedder to be set (async model loading)
	if ew.embedder == nil {
		fmt.Println("⏳ Waiting for embedding model to load...")
		for {
			ew.mu.Lock()
			hasEmbedder := ew.embedder != nil
			ew.mu.Unlock()

			if hasEmbedder {
				fmt.Println("✅ Embedding model loaded, worker active")
				break
			}
			if ew.closed.Load() {
				return
			}

			select {
			case <-ew.ctx.Done():
				return
			case <-time.After(1 * time.Second):
				// Check again
			}
		}
	}

	// Short initial delay to let server start
	time.Sleep(500 * time.Millisecond)

	// Refresh the pending embeddings index on startup to catch any nodes
	// that need embedding (e.g., after restart, bulk import, or cleared embeddings).
	// Run this once for the whole worker pool to avoid duplicate startup scans/logs.
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

	ew.processUntilEmpty()

	ticker := time.NewTicker(ew.config.ScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ew.ctx.Done():
			fmt.Println("🧠 Embed worker stopped")
			return

		case <-ew.trigger:
			// Immediate trigger - process until queue is empty
			ew.processUntilEmpty()

		case <-ticker.C:
			// Regular interval scan
			ew.processNextBatch()
		}
	}
}

// processUntilEmpty keeps processing nodes until no more need embeddings.
// When the queue becomes empty, it schedules a debounced k-means clustering trigger.
func (ew *EmbedWorker) processUntilEmpty() {
	batchProcessed := 0
	consecutiveEmptyCount := 0
	maxConsecutiveEmpty := 3 // Stop after 3 consecutive empty checks

	for {
		select {
		case <-ew.ctx.Done():
			return
		default:
			// processNextBatch returns true if it actually processed or skipped a node
			// It returns false if there was nothing to process
			didWork := ew.processNextBatch()
			if !didWork {
				consecutiveEmptyCount++
				if consecutiveEmptyCount == 1 {
					// First empty check - refresh index to catch any new nodes and clean up stale entries
					removed := ew.refreshEmbeddingIndexIfDue(false)
					if removed > 0 {
						fmt.Printf("🧹 Cleaned up %d stale entries from pending embeddings index\n", removed)
						// Reset counter since we found and cleaned stale entries - try again
						consecutiveEmptyCount = 0
						continue
					}
				} else if consecutiveEmptyCount >= maxConsecutiveEmpty {
					// Multiple empty checks - we're done
					// Queue is empty - schedule debounced k-means callback if we processed anything
					if batchProcessed > 0 && ew.onQueueEmpty != nil {
						ew.scheduleClusteringDebounced(batchProcessed)
					}
					return // No more nodes to process
				}
				// Small delay before next check
				time.Sleep(100 * time.Millisecond)
			} else {
				// Successfully processed - reset counter
				consecutiveEmptyCount = 0
				batchProcessed++
				// Small delay between batches to avoid CPU spin
				time.Sleep(50 * time.Millisecond)
			}
		}
	}
}

// processNextBatch finds and processes nodes without embeddings.
// Returns true if it did useful work (processed or permanently skipped a node).
// Returns false if there was nothing to process or if a node was temporarily skipped.
func (ew *EmbedWorker) processNextBatch() bool {
	ew.mu.Lock()
	hasResolver := ew.embedderResolver != nil
	ew.mu.Unlock()
	if hasResolver {
		return ew.processNextResolvedBatch()
	}
	if provider, ok := ew.embedder.(embed.DocumentPropertyChunkEmbedder); ok && provider.UsesDocumentProperties() {
		return ew.processNextNode()
	}
	if batcher, ok := ew.embedder.(embed.DocumentBatchChunkEmbedder); ok && ew.config.EmbedBatchSize > 1 {
		if _, indexed := ew.storage.(EmbeddingIndexManager); indexed {
			return ew.processNextDocumentBatch(batcher)
		}
	}
	return ew.processNextNode()
}

func (ew *EmbedWorker) processNextResolvedBatch() bool {
	limit := ew.config.EmbedBatchSize
	if limit < 1 {
		limit = 1
	}
	nodes := make([]*storage.Node, 0, limit)
	for len(nodes) < limit {
		node := ew.claimNextNode()
		if node == nil {
			break
		}
		nodes = append(nodes, node)
	}
	if len(nodes) == 0 {
		return false
	}

	type resolvedGroup struct {
		provider        embed.Embedder
		batcher         embed.DocumentBatchChunkEmbedder
		propertyBatcher embed.DocumentPropertyBatchChunkEmbedder
		nodes           []*storage.Node
	}
	groups := make(map[string]*resolvedGroup)
	for _, node := range nodes {
		provider, err := ew.resolveEmbedder(node.ID)
		if err != nil || provider == nil {
			if err == nil {
				err = errors.New("embedding provider is not configured")
			}
			ew.failed.Add(1)
			ew.markNodeEmbeddingFailed(node.ID, err)
			ew.releaseNodeClaim(node.ID)
			continue
		}
		if structured, ok := provider.(embed.DocumentPropertyChunkEmbedder); ok && structured.UsesDocumentProperties() {
			propertyBatcher, batchOK := provider.(embed.DocumentPropertyBatchChunkEmbedder)
			if !batchOK || limit == 1 {
				ew.processClaimedNode(node, provider)
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
			ew.processClaimedNode(node, provider)
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
		if !ew.waitForProviderRetry(group.provider) {
			for _, node := range group.nodes {
				ew.addNodeToPendingEmbeddings(node.ID)
				ew.releaseNodeClaim(node.ID)
			}
			continue
		}
		texts := make([]string, len(group.nodes))
		properties := make([]map[string]any, len(group.nodes))
		for index, node := range group.nodes {
			texts[index] = embeddingutil.BuildText(node.Properties, node.Labels, opts)
			properties[index] = node.Properties
		}
		var results []*embed.DocumentChunkResult
		var resultErrors []error
		if group.propertyBatcher != nil {
			results, resultErrors = ew.embedDocumentPropertyBatchIsolated(group.propertyBatcher, texts, properties)
		} else {
			results, resultErrors = ew.embedDocumentBatchIsolated(group.batcher, texts)
		}
		ew.recordProviderBatchOutcome(group.provider, resultErrors)
		for _, resultErr := range resultErrors {
			if resultErr != nil {
				fmt.Printf("⚠️  Failed to embed %d-node batch: %s\n", len(group.nodes), compactWorkerError(resultErr, 300))
				break
			}
		}
		for index, node := range group.nodes {
			if resultErrors[index] != nil {
				ew.failed.Add(1)
				if !isRetryableEmbeddingError(resultErrors[index]) {
					ew.markNodeEmbeddingFailed(node.ID, resultErrors[index])
				} else {
					ew.addNodeToPendingEmbeddings(node.ID)
				}
				ew.releaseNodeClaim(node.ID)
				continue
			}
			if results[index] == nil {
				ew.failed.Add(1)
				ew.markNodeEmbeddingFailed(node.ID, errors.New("embedding provider returned no document result"))
				ew.releaseNodeClaim(node.ID)
				continue
			}
			ew.persistEmbeddedNode(node, results[index].Embeddings, documentResultMeta(results[index]), group.provider)
			ew.releaseNodeClaim(node.ID)
		}
	}
	ew.signalTrigger()
	return true
}

func (ew *EmbedWorker) processNextNode() bool {
	// Check for cancellation at the start
	select {
	case <-ew.ctx.Done():
		return false
	default:
	}

	// Foreground-first policy: if tx load is active, pause background embedding.
	ew.mu.Lock()
	shouldYield := ew.shouldYield
	ew.mu.Unlock()
	if shouldYield != nil && shouldYield() {
		time.Sleep(25 * time.Millisecond)
		ew.signalTrigger()
		return false
	}

	node := ew.claimNextNode()
	if node == nil {
		return false // Nothing to process
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
	provider, resolveErr := ew.resolveEmbedder(node.ID)
	if resolveErr != nil || provider == nil {
		if resolveErr == nil {
			resolveErr = errors.New("embedding provider is not configured")
		}
		ew.failed.Add(1)
		ew.markNodeEmbeddingFailed(node.ID, resolveErr)
		ew.releaseNodeClaim(node.ID)
		return true
	}
	return ew.processClaimedNode(node, provider)
}

func (ew *EmbedWorker) processClaimedNode(node *storage.Node, provider embed.Embedder) bool {
	defer ew.releaseNodeClaim(node.ID)
	if !ew.waitForProviderRetry(provider) {
		ew.addNodeToPendingEmbeddings(node.ID)
		return false
	}
	// Build text for embedding (labels and properties per config include/exclude)
	opts := embeddingutil.EmbedTextOptionsFromFields(ew.config.PropertiesInclude, ew.config.PropertiesExclude, ew.config.IncludeLabels)
	text := embeddingutil.BuildText(node.Properties, node.Labels, opts)

	// Embed documents through the provider-managed document path when available.
	// Voyage contextualized mode uses this to return provider-generated chunks;
	// other providers fall back to deterministic local chunking + micro-batches.
	propertyEmbedder, usesProperties := provider.(embed.DocumentPropertyChunkEmbedder)
	var embeddings [][]float32
	var providerMeta map[string]any
	var err error
	if usesProperties && propertyEmbedder.UsesDocumentProperties() {
		var result *embed.DocumentChunkResult
		result, err = propertyEmbedder.EmbedDocumentPropertyChunks(ew.ctx, text, node.Properties, ew.config.ChunkSize, ew.config.ChunkOverlap)
		ew.waitAfterProviderRequest()
		if result != nil {
			embeddings = result.Embeddings
			providerMeta = documentResultMeta(result)
		}
	} else {
		_, embeddings, providerMeta, err = ew.embedDocumentWith(provider, text, node.ID)
	}
	if err != nil {
		ew.failed.Add(1)
		ew.recordProviderFailure(provider, err)
		if !isRetryableEmbeddingError(err) {
			ew.markNodeEmbeddingFailed(node.ID, err)
			return true
		}
		fmt.Printf("⚠️  Failed to embed node %s: %v\n", node.ID, err)
		ew.addNodeToPendingEmbeddings(node.ID) // Re-queue so another worker can retry
		return true
	}
	ew.clearProviderFailure(provider)

	return ew.persistEmbeddedNode(node, embeddings, providerMeta, provider)
}

func (ew *EmbedWorker) processNextDocumentBatch(batcher embed.DocumentBatchChunkEmbedder) bool {
	select {
	case <-ew.ctx.Done():
		return false
	default:
	}

	limit := ew.config.EmbedBatchSize
	nodes := make([]*storage.Node, 0, limit)
	for len(nodes) < limit {
		node := ew.claimNextNode()
		if node == nil {
			break
		}
		nodes = append(nodes, node)
	}
	if len(nodes) == 0 {
		return false
	}
	if !ew.waitForProviderRetry(ew.embedder) {
		for _, node := range nodes {
			ew.addNodeToPendingEmbeddings(node.ID)
			ew.releaseNodeClaim(node.ID)
		}
		return false
	}

	opts := embeddingutil.EmbedTextOptionsFromFields(ew.config.PropertiesInclude, ew.config.PropertiesExclude, ew.config.IncludeLabels)
	texts := make([]string, len(nodes))
	for i, node := range nodes {
		texts[i] = embeddingutil.BuildText(node.Properties, node.Labels, opts)
	}
	results, resultErrors := ew.embedDocumentBatchIsolated(batcher, texts)
	ew.recordProviderBatchOutcome(ew.embedder, resultErrors)
	for i, node := range nodes {
		if resultErrors[i] != nil {
			ew.failed.Add(1)
			err := resultErrors[i]
			if !isRetryableEmbeddingError(err) {
				ew.markNodeEmbeddingFailed(node.ID, err)
			} else {
				ew.addNodeToPendingEmbeddings(node.ID)
			}
			ew.releaseNodeClaim(node.ID)
			continue
		}
		result := results[i]
		if result == nil {
			ew.markNodeEmbeddingFailed(node.ID, errors.New("embedding provider returned no document result"))
			ew.failed.Add(1)
			ew.releaseNodeClaim(node.ID)
			continue
		}
		ew.persistEmbeddedNode(node, result.Embeddings, documentResultMeta(result), ew.embedder)
		ew.releaseNodeClaim(node.ID)
	}
	ew.signalTrigger()
	return true
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

func (ew *EmbedWorker) persistEmbeddedNode(node *storage.Node, embeddings [][]float32, providerMeta map[string]any, provider embed.Embedder) bool {
	if len(embeddings) == 0 || embeddings[0] == nil || len(embeddings[0]) == 0 {
		fmt.Printf("⚠️  Failed to generate embedding for node %s: empty embedding\n", node.ID)
		ew.failed.Add(1)
		ew.markNodeEmbeddingFailed(node.ID, errors.New("embedding provider returned an empty embedding"))
		return true
	}

	embeddingutil.ApplyManagedEmbedding(node, embeddings, provider.Model(), provider.Dimensions(), time.Now())
	if spaceProvider, ok := provider.(embed.EmbeddingSpaceProvider); ok {
		if space := spaceProvider.EmbeddingSpace(); space != "" {
			node.EmbedMeta["embedding_space"] = space
		}
	}
	for key, value := range providerMeta {
		node.EmbedMeta[key] = value
	}
	chunkEmbeddingsToSave := node.ChunkEmbeddings
	embedMetaToSave := make(map[string]any, len(node.EmbedMeta))
	for key, value := range node.EmbedMeta {
		embedMetaToSave[key] = value
	}

	existingNode, err := ew.storage.GetNode(node.ID)
	if err != nil {
		// Node was deleted - remove from pending index and skip
		fmt.Printf("⚠️  Node %s was deleted before embedding could be saved - skipping\n", node.ID)
		ew.markNodeEmbedded(node.ID)
		return false // Skip this node, try next one
	}

	// CRITICAL: Preserve the embeddings we just generated!
	// Don't overwrite node with existingNode - that would lose the embeddings
	// Instead, update the existing node's embedding field while preserving other fields
	node = existingNode                          // Get latest data from storage
	node.ChunkEmbeddings = chunkEmbeddingsToSave // Restore chunk embeddings (struct field, opaque to users)
	node.UpdatedAt = time.Now()                  // Update timestamp

	// Restore embedding metadata (in EmbedMeta, not Properties)
	node.EmbedMeta = embedMetaToSave

	// Save the parent node (either with embedding for single chunk, or metadata for chunked files)
	// CRITICAL: Use UpdateNodeEmbedding if available (only updates existing nodes, doesn't create)
	// This prevents creating orphaned nodes when the pending index has stale entries
	var updateErr error
	if embedUpdater, ok := ew.storage.(interface{ UpdateNodeEmbedding(*storage.Node) error }); ok {
		// UpdateNodeEmbedding only updates existing nodes - returns ErrNotFound if node doesn't exist
		updateErr = embedUpdater.UpdateNodeEmbedding(node)
		if updateErr == storage.ErrNotFound {
			// Node was deleted - remove from pending index and skip
			fmt.Printf("⚠️  Node %s was deleted - skipping update to prevent orphaned node\n", node.ID)
			ew.markNodeEmbedded(node.ID)
			return false
		}
	} else {
		// Fallback: UpdateNode has upsert behavior which can create orphaned nodes
		// This should only happen if the storage engine doesn't support UpdateNodeEmbedding
		// For safety, we've already verified the node exists above
		updateErr = ew.storage.UpdateNode(node)
	}
	if updateErr != nil {
		// If update failed because node doesn't exist, skip it (already claimed, don't re-queue)
		if updateErr == storage.ErrNotFound {
			fmt.Printf("⚠️  Node no longer exists - skipping update to prevent orphaned node\n")
			return false
		}
		fmt.Printf("⚠️  Failed to update node embedding state: %s; re-queuing for retry\n", compactWorkerError(updateErr, 300))
		ew.addNodeToPendingEmbeddings(node.ID) // Re-queue so another worker can retry
		ew.failed.Add(1)
		return true // Failed but we tried - continue to next node
	}

	// Call callback to update search index
	if ew.onEmbedded != nil {
		ew.onEmbedded(node)
	}

	// Remove from pending embeddings index (O(1) operation)
	ew.markNodeEmbedded(node.ID)
	ew.processed.Add(1)
	// Track this node as recently processed to prevent re-processing before DB commit is visible
	ew.mu.Lock()
	if ew.recentlyProcessed == nil {
		ew.recentlyProcessed = make(map[string]time.Time)
	}
	ew.recentlyProcessed[string(node.ID)] = time.Now()
	ew.mu.Unlock()

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

func providerRetryKey(provider embed.Embedder) string {
	return fmt.Sprintf("%T:%p", provider, provider)
}

func (ew *EmbedWorker) waitForProviderRetry(provider embed.Embedder) bool {
	if provider == nil || ew.config == nil || ew.config.ProviderRetryBackoff <= 0 {
		return true
	}
	key := providerRetryKey(provider)
	for {
		ew.mu.Lock()
		state := ew.providerRetries[key]
		ew.mu.Unlock()
		delay := time.Until(state.retryAt)
		if delay <= 0 {
			return true
		}
		timer := time.NewTimer(delay)
		select {
		case <-ew.ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
	}
}

func (ew *EmbedWorker) recordProviderBatchOutcome(provider embed.Embedder, errs []error) {
	for _, err := range errs {
		if err != nil && isRetryableEmbeddingError(err) {
			ew.recordProviderFailure(provider, err)
			return
		}
	}
	// A terminal input error is not a provider outage. Successful neighboring
	// documents likewise prove the provider is available.
	ew.clearProviderFailure(provider)
}

func (ew *EmbedWorker) recordProviderFailure(provider embed.Embedder, err error) {
	if provider == nil || !isRetryableEmbeddingError(err) || ew.config == nil || ew.config.ProviderRetryBackoff <= 0 {
		return
	}
	backoffMax := ew.config.ProviderRetryBackoffMax
	if backoffMax < ew.config.ProviderRetryBackoff {
		backoffMax = ew.config.ProviderRetryBackoff
	}
	key := providerRetryKey(provider)
	ew.mu.Lock()
	if ew.providerRetries == nil {
		ew.providerRetries = make(map[string]providerRetryState)
	}
	state := ew.providerRetries[key]
	state.failures++
	delay := ew.config.ProviderRetryBackoff
	for attempt := 1; attempt < state.failures && delay < backoffMax; attempt++ {
		if delay > backoffMax/2 {
			delay = backoffMax
			break
		}
		delay *= 2
	}
	if retryAfter, ok := retryDelay(err); ok && retryAfter > delay {
		delay = retryAfter
	}
	state.retryAt = time.Now().Add(delay)
	ew.providerRetries[key] = state
	ew.mu.Unlock()
}

func (ew *EmbedWorker) clearProviderFailure(provider embed.Embedder) {
	if provider == nil {
		return
	}
	ew.mu.Lock()
	delete(ew.providerRetries, providerRetryKey(provider))
	ew.mu.Unlock()
}

func retryDelay(err error) (time.Duration, bool) {
	type delayedRetry interface{ RetryDelay() time.Duration }
	var delayed delayedRetry
	if errors.As(err, &delayed) {
		return delayed.RetryDelay(), true
	}
	return 0, false
}

// markNodeEmbeddingFailed records a permanent provider failure outside user
// properties, removes it from the pending index, and lets later content edits
// retry it by clearing managed embedding metadata during invalidation.
func (ew *EmbedWorker) markNodeEmbeddingFailed(nodeID storage.NodeID, embedErr error) {
	node, err := ew.storage.GetNode(nodeID)
	if err != nil || node == nil {
		// A deleted node does not need retrying; an unexpected storage read error
		// does, so leave it pending in that case.
		if errors.Is(err, storage.ErrNotFound) || node == nil {
			ew.markNodeEmbedded(nodeID)
		}
		return
	}
	node = copyNodeForEmbedding(node)
	if node.EmbedMeta == nil {
		node.EmbedMeta = make(map[string]any)
	}
	node.EmbedMeta["has_embedding"] = false
	node.EmbedMeta["embedding_failed"] = true
	node.EmbedMeta["embedding_error"] = compactWorkerError(embedErr, 300)
	node.EmbedMeta["embedding_failed_at"] = time.Now().UTC().Format(time.RFC3339)

	var updateErr error
	if updater, ok := ew.storage.(interface{ UpdateNodeEmbedding(*storage.Node) error }); ok {
		updateErr = updater.UpdateNodeEmbedding(node)
	} else {
		updateErr = ew.storage.UpdateNode(node)
	}
	if updateErr != nil {
		fmt.Printf("⚠️  Failed to persist permanent embedding failure for node %s: %s; re-queuing\n", nodeID, compactWorkerError(updateErr, 300))
		ew.addNodeToPendingEmbeddings(nodeID)
		return
	}

	ew.markNodeEmbedded(nodeID)
	ew.parked.Add(1)
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
	ew.claimMu.Lock()
	defer ew.claimMu.Unlock()

	pending := ew.findNodeWithoutEmbedding()
	if pending == nil {
		return nil
	}
	if ew.claimedNodes == nil {
		ew.claimedNodes = make(map[string]struct{})
	}
	id := string(pending.ID)
	if _, claimed := ew.claimedNodes[id]; claimed {
		return nil
	}

	node, err := ew.storage.GetNode(pending.ID)
	if err != nil || node == nil {
		ew.markNodeEmbedded(pending.ID)
		return nil
	}
	if ew.wasRecentlyProcessed(node.ID) {
		return nil
	}

	ew.claimedNodes[id] = struct{}{}
	ew.inFlight.Add(1)
	ew.markNodeEmbedded(node.ID)
	return copyNodeForEmbedding(node)
}

func (ew *EmbedWorker) releaseNodeClaim(nodeID storage.NodeID) {
	ew.claimMu.Lock()
	id := string(nodeID)
	if _, claimed := ew.claimedNodes[id]; claimed {
		delete(ew.claimedNodes, id)
		ew.inFlight.Add(-1)
	}
	ew.claimMu.Unlock()
}

func (ew *EmbedWorker) wasRecentlyProcessed(nodeID storage.NodeID) bool {
	ew.mu.Lock()
	defer ew.mu.Unlock()
	if ew.recentlyProcessed == nil {
		ew.recentlyProcessed = make(map[string]time.Time)
	}
	if ew.loggedSkip == nil {
		ew.loggedSkip = make(map[string]bool)
	}
	id := string(nodeID)
	if lastProcessed, ok := ew.recentlyProcessed[id]; ok && time.Since(lastProcessed) < 30*time.Second {
		if !ew.loggedSkip[id] {
			ew.loggedSkip[id] = true
			fmt.Printf("⏭️  Skipping node %s: recently processed (waiting for DB sync)\n", nodeID)
		}
		return true
	}
	delete(ew.loggedSkip, id)
	for candidate, processedAt := range ew.recentlyProcessed {
		if time.Since(processedAt) > time.Minute {
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

// addNodeToPendingEmbeddings re-queues a node for embedding (e.g. after a failed attempt so another worker can retry).
func (ew *EmbedWorker) addNodeToPendingEmbeddings(nodeID storage.NodeID) {
	if adder, ok := ew.storage.(interface{ AddToPendingEmbeddings(storage.NodeID) }); ok {
		adder.AddToPendingEmbeddings(nodeID)
	}
}

// embedChunksInBatches embeds chunks using bounded request sizes.
// This avoids sending massive single EmbedBatch requests for large files.
func (ew *EmbedWorker) embedChunksInBatches(chunks []string, nodeID storage.NodeID) ([][]float32, error) {
	return ew.embedChunksInBatchesWith(ew.embedder, chunks, nodeID)
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
		meta := make(map[string]any)
		if result != nil {
			if result.ChunkerVersion != "" {
				meta["chunker_version"] = result.ChunkerVersion
			}
			if result.TotalTokens > 0 {
				meta["embedding_total_tokens"] = result.TotalTokens
			}
			if len(result.Chunks) > 0 && (result.ChunkerVersion != "" || result.TotalTokens > 0) {
				meta["chunk_texts"] = append([]string(nil), result.Chunks...)
			}
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
	return ew.embedBatchWithRetryFor(ew.embedder, chunks)
}

func (ew *EmbedWorker) embedBatchWithRetryFor(provider embed.Embedder, chunks []string) ([][]float32, error) {
	var embeddings [][]float32
	var err error
	for attempt := 1; attempt <= ew.config.MaxRetries; attempt++ {
		type embedResult struct {
			embeddings [][]float32
			err        error
		}
		resultCh := make(chan embedResult, 1)
		go func() {
			var embs [][]float32
			var embedErr error
			if typed, ok := provider.(embed.TypedEmbedder); ok {
				embs, embedErr = typed.EmbedBatchWithInputType(ew.ctx, chunks, embed.InputTypeDocument)
			} else {
				embs, embedErr = provider.EmbedBatch(ew.ctx, chunks)
			}
			resultCh <- embedResult{embeddings: embs, err: embedErr}
		}()
		select {
		case <-ew.ctx.Done():
			return nil, ew.ctx.Err()
		case result := <-resultCh:
			embeddings, err = result.embeddings, result.err
		}
		ew.waitAfterProviderRequest()
		if err == nil {
			return embeddings, nil
		}
		if !isRetryableEmbeddingError(err) {
			break
		}
		if attempt < ew.config.MaxRetries {
			backoff := time.Duration(attempt) * 2 * time.Second
			fmt.Printf("   ⚠️  Embed batch attempt %d failed (batch_size=%d), retrying in %v\n", attempt, len(chunks), backoff)
			select {
			case <-ew.ctx.Done():
				return nil, ew.ctx.Err()
			case <-time.After(backoff):
			}
			continue
		}
	}
	return nil, err
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

// Enqueue is now just a trigger - tells worker to check for work.
func (ew *EmbedWorker) Enqueue(nodeID string) {
	ew.Trigger()
}
