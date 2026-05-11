// Package nornicdb provides async embedding worker for background embedding generation.
package nornicdb

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type deterministicTextChunker interface {
	ChunkText(text string, maxTokens, overlap int) ([]string, error)
}

// EmbedWorker manages async embedding generation using a pull-based model.
// On each cycle, it scans for nodes without embeddings and processes them.
type EmbedWorker struct {
	embedder embed.Embedder
	storage  storage.Engine
	config   *EmbedWorkerConfig

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
	running   atomic.Bool
	closed    atomic.Bool // Set to true when Close() is called

	// Recently processed node IDs to prevent re-processing before DB commit is visible
	// This prevents the same node being processed multiple times in quick succession
	recentlyProcessed map[string]time.Time

	// Track nodes we've already logged as skipped (to avoid log spam)
	loggedSkip map[string]bool

	// Debounce state for k-means clustering trigger
	clusterDebounceTimer   *time.Timer
	clusterDebounceMu      sync.Mutex
	pendingClusterCount    int  // Accumulated count for debounced callback
	clusterDebounceRunning bool // Whether a debounce timer is active

	// claimMu serializes find+claim so only one worker can take a node at a time (prevents double-processing).
	claimMu sync.Mutex

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
	NumWorkers   int           // Number of concurrent workers (default: 1, use more for network/parallel processing)
	ScanInterval time.Duration // How often to scan for nodes without embeddings (default: 5s)
	BatchDelay   time.Duration // Delay between processing nodes (default: 500ms)
	MaxRetries   int           // Max retry attempts per node (default: 3)
	// TriggerDebounceDelay delays enqueue-triggered scans until writes lull.
	// Each new Enqueue resets the timer. Set 0 for immediate trigger behavior.
	TriggerDebounceDelay time.Duration // default: 2s

	// Text chunking settings.
	ChunkSize    int // Max tokens per chunk (default: 512)
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
		NumWorkers:           1,                      // Single worker by default
		ScanInterval:         15 * time.Minute,       // Scan for missed nodes every 15 minutes
		BatchDelay:           500 * time.Millisecond, // Delay between processing nodes
		MaxRetries:           3,
		TriggerDebounceDelay: 2 * time.Second,
		ChunkSize:            512,
		ChunkOverlap:         50,
		EmbedBatchSize:       32,
		ClusterDebounceDelay: 30 * time.Second, // Wait 30s after last embedding before k-means
		ClusterMinBatchSize:  10,               // Need at least 10 embeddings to trigger k-means
		PropertiesInclude:    nil,
		PropertiesExclude:    nil,
		IncludeLabels:        true,
	}
}

// NewEmbedWorker creates a new async embedding worker pool.
// If embedder is nil, the worker will wait for SetEmbedder() to be called.
// NumWorkers controls how many concurrent workers process embeddings in parallel.
// Use more workers for network-based embedders (OpenAI, etc.) or when you have
// multiple GPUs/CPUs available for local embedding generation.
func NewEmbedWorker(embedder embed.Embedder, storage storage.Engine, config *EmbedWorkerConfig) *EmbedWorker {
	if config == nil {
		config = DefaultEmbedWorkerConfig()
	}

	// Ensure at least 1 worker
	if config.NumWorkers < 1 {
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
	ew.lifecycleMu.Lock()
	defer ew.lifecycleMu.Unlock()
	if ew.closed.Load() {
		return
	}
	ew.workersStarted = true
	numWorkers := ew.config.NumWorkers
	if numWorkers < 1 {
		numWorkers = 1
	}
	for i := 0; i < numWorkers; i++ {
		ew.wg.Add(1)
		go ew.worker()
	}
	if numWorkers > 1 {
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
	Processed int  `json:"processed"`
	Failed    int  `json:"failed"`
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
	return WorkerStats{
		Running:   ew.running.Load(),
		Processed: int(ew.processed.Load()),
		Failed:    int(ew.failed.Load()),
	}
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
	wasRunning := ew.running.Load()
	ew.mu.Unlock()

	fmt.Println("🔄 Resetting embed worker for regeneration...")
	ew.lifecycleMu.Lock()
	defer ew.lifecycleMu.Unlock()

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
	ew.processed.Store(0)
	ew.failed.Store(0)
	ew.running.Store(false)

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
	ew.lifecycleMu.Lock()
	defer ew.lifecycleMu.Unlock()

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

	ew.running.Store(true)

	defer func() {
		ew.running.Store(false)
	}()

	// Serialize find+claim so only one worker can take a node at a time (prevents double-processing).
	ew.claimMu.Lock()
	node := ew.findNodeWithoutEmbedding()
	if node == nil {
		ew.claimMu.Unlock()
		return false // Nothing to process
	}

	// Check for cancellation before processing
	select {
	case <-ew.ctx.Done():
		ew.claimMu.Unlock()
		return false
	default:
	}

	// CRITICAL: Verify node still exists before processing
	// Node might have been deleted between index lookup and now
	// This prevents trying to embed deleted nodes
	existingNode, err := ew.storage.GetNode(node.ID)
	if err != nil {
		// Node was deleted - remove from pending index and skip
		fmt.Printf("⚠️  Node %s from pending index doesn't exist - removing stale entry\n", node.ID)
		ew.markNodeEmbedded(node.ID)
		ew.claimMu.Unlock()
		return false // Skip this node, try next one
	}

	// DEBUG: Verify the node we found matches what's in storage
	if existingNode == nil {
		fmt.Printf("⚠️  Node %s from pending index is nil - removing stale entry\n", node.ID)
		ew.markNodeEmbedded(node.ID)
		ew.claimMu.Unlock()
		return false
	}

	// Update node with latest data from storage
	node = existingNode

	// Check if this node was recently processed (prevents re-processing before DB commit is visible)
	ew.mu.Lock()
	if ew.recentlyProcessed == nil {
		ew.recentlyProcessed = make(map[string]time.Time)
	}
	if ew.loggedSkip == nil {
		ew.loggedSkip = make(map[string]bool)
	}
	if lastProcessed, ok := ew.recentlyProcessed[string(node.ID)]; ok {
		if time.Since(lastProcessed) < 30*time.Second {
			if !ew.loggedSkip[string(node.ID)] {
				ew.loggedSkip[string(node.ID)] = true
				fmt.Printf("⏭️  Skipping node %s: recently processed (waiting for DB sync)\n", node.ID)
			}
			ew.mu.Unlock()
			ew.claimMu.Unlock()
			return false // Temporary skip - don't continue looping
		}
		delete(ew.loggedSkip, string(node.ID))
	}
	// Clean up old entries (older than 1 minute)
	for id, t := range ew.recentlyProcessed {
		if time.Since(t) > time.Minute {
			delete(ew.recentlyProcessed, id)
			delete(ew.loggedSkip, id)
		}
	}
	ew.mu.Unlock()

	// Claim the node so no other worker can pick it (remove from pending index now; re-queue on failure).
	ew.markNodeEmbedded(node.ID)
	ew.claimMu.Unlock()

	fmt.Printf("🔄 Processing node %s for embedding...\n", node.ID)

	// IMPORTANT: Deep copy properties to avoid race conditions
	// The node from storage may be accessed by other goroutines (e.g., HTTP handlers)
	// Modifying the Properties map directly causes "concurrent map iteration and map write"
	node = copyNodeForEmbedding(node)

	// Build text for embedding (labels and properties per config include/exclude)
	opts := embeddingutil.EmbedTextOptionsFromFields(ew.config.PropertiesInclude, ew.config.PropertiesExclude, ew.config.IncludeLabels)
	text := embeddingutil.BuildText(node.Properties, node.Labels, opts)

	chunker, ok := ew.embedder.(deterministicTextChunker)
	if !ok {
		fmt.Printf("⚠️  Failed to chunk node %s: embedder %T does not support deterministic token chunking\n", node.ID, ew.embedder)
		ew.addNodeToPendingEmbeddings(node.ID)
		ew.failed.Add(1)
		return true
	}

	// Chunk text using the embedder's tokenizer so every chunk respects the true token cap.
	chunks, err := chunker.ChunkText(text, ew.config.ChunkSize, ew.config.ChunkOverlap)
	if err != nil {
		fmt.Printf("⚠️  Failed to chunk node %s: %v\n", node.ID, err)
		ew.addNodeToPendingEmbeddings(node.ID)
		ew.failed.Add(1)
		return true
	}

	// Embed chunks in micro-batches to avoid oversized single requests for large files.
	embeddings, err := ew.embedChunksInBatches(chunks, node.ID)
	if err != nil {
		fmt.Printf("⚠️  Failed to embed node %s: %v\n", node.ID, err)
		ew.addNodeToPendingEmbeddings(node.ID) // Re-queue so another worker can retry
		ew.failed.Add(1)
		return true
	}

	// Validate embeddings were generated
	if len(embeddings) == 0 || embeddings[0] == nil || len(embeddings[0]) == 0 {
		fmt.Printf("⚠️  Failed to generate embedding for node %s: empty embedding\n", node.ID)
		ew.addNodeToPendingEmbeddings(node.ID) // Re-queue so another worker can retry
		ew.failed.Add(1)
		return true // Failed but we tried - continue to next node
	}

	// Persist worker-managed embedding fields in a shared canonical shape.
	embeddingutil.ApplyManagedEmbedding(node, embeddings, ew.embedder.Model(), ew.embedder.Dimensions(), time.Now())

	// CRITICAL: Double-check node still exists before updating
	// This prevents creating orphaned nodes if the node was deleted between
	// the initial check and now. Reload from storage to get latest version.
	// BUT: Preserve the embeddings we just generated!
	chunkEmbeddingsToSave := node.ChunkEmbeddings // Save the chunk embeddings we just generated
	embedMetaToSave := make(map[string]any)
	if node.EmbedMeta != nil {
		// Save embedding metadata
		for k, v := range node.EmbedMeta {
			embedMetaToSave[k] = v
		}
	}

	existingNode, err = ew.storage.GetNode(node.ID)
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
			fmt.Printf("⚠️  Node %s doesn't exist - skipping update to prevent orphaned node\n", node.ID)
			return false
		}
		fmt.Printf("⚠️  Failed to update node %s: %v\n", node.ID, updateErr)
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

	// Small delay before next
	time.Sleep(ew.config.BatchDelay)

	// Trigger another check immediately if there might be more.
	// Internal chaining should not be debounced.
	ew.signalTrigger()

	return true // Successfully processed
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
		batchEmbeddings, err := ew.embedBatchWithRetry(batch)
		if err != nil {
			return nil, fmt.Errorf("batch %d-%d/%d failed for %s: %w", start+1, end, len(chunks), nodeID, err)
		}
		if len(batchEmbeddings) != len(batch) {
			return nil, fmt.Errorf("embedding count mismatch for %s: got %d, expected %d", nodeID, len(batchEmbeddings), len(batch))
		}
		allEmbeddings = append(allEmbeddings, batchEmbeddings...)
	}
	return allEmbeddings, nil
}

// embedBatchWithRetry retries a single micro-batch with backoff.
func (ew *EmbedWorker) embedBatchWithRetry(chunks []string) ([][]float32, error) {
	var embeddings [][]float32
	var err error
	for attempt := 1; attempt <= ew.config.MaxRetries; attempt++ {
		type embedResult struct {
			embeddings [][]float32
			err        error
		}
		resultCh := make(chan embedResult, 1)
		go func() {
			embs, embedErr := ew.embedder.EmbedBatch(ew.ctx, chunks)
			resultCh <- embedResult{embeddings: embs, err: embedErr}
		}()
		select {
		case <-ew.ctx.Done():
			return nil, ew.ctx.Err()
		case result := <-resultCh:
			embeddings, err = result.embeddings, result.err
		}
		if err == nil {
			return embeddings, nil
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
		"processed": s.Processed,
		"failed":    s.Failed,
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
