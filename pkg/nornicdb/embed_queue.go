// Package nornicdb provides async embedding worker for background embedding generation.
package nornicdb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// EmbedWorker manages async embedding generation using a pull-based model.
// On each cycle, it scans for nodes without embeddings and processes them.
type EmbedWorker struct {
	embedder embed.Embedder
	storage  storage.Engine
	config   *EmbedWorkerConfig

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Trigger channel to wake up worker immediately
	trigger chan struct{}

	// Callback after embedding a node (for search index update)
	onEmbedded func(node *storage.Node)

	// Callback when queue becomes empty (for triggering k-means clustering)
	onQueueEmpty func(processedCount int)

	// Stats
	mu        sync.Mutex
	processed int
	failed    int
	running   bool
	closed    bool // Set to true when Close() is called

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
}

// EmbedWorkerConfig holds configuration for the embedding worker.
type EmbedWorkerConfig struct {
	// Worker settings
	NumWorkers   int           // Number of concurrent workers (default: 1, use more for network/parallel processing)
	ScanInterval time.Duration // How often to scan for nodes without embeddings (default: 5s)
	BatchDelay   time.Duration // Delay between processing nodes (default: 500ms)
	MaxRetries   int           // Max retry attempts per node (default: 3)

	// Text chunking settings (matches Mimir: MIMIR_EMBEDDINGS_CHUNK_SIZE, MIMIR_EMBEDDINGS_CHUNK_OVERLAP)
	ChunkSize    int // Max characters per chunk (default: 512)
	ChunkOverlap int // Characters to overlap between chunks (default: 50)

	// Debounce settings for k-means clustering trigger
	ClusterDebounceDelay time.Duration // How long to wait after last embedding before triggering k-means (default: 30s)
	ClusterMinBatchSize  int           // Minimum embeddings processed before triggering k-means (default: 10)
}

// DefaultEmbedWorkerConfig returns sensible defaults.
func DefaultEmbedWorkerConfig() *EmbedWorkerConfig {
	return &EmbedWorkerConfig{
		NumWorkers:           1,                      // Single worker by default
		ScanInterval:         15 * time.Minute,       // Scan for missed nodes every 15 minutes
		BatchDelay:           500 * time.Millisecond, // Delay between processing nodes
		MaxRetries:           3,
		ChunkSize:            512,
		ChunkOverlap:         50,
		ClusterDebounceDelay: 30 * time.Second, // Wait 30s after last embedding before k-means
		ClusterMinBatchSize:  10,               // Need at least 10 embeddings to trigger k-means
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

	// Start N workers for parallel processing
	numWorkers := config.NumWorkers
	for i := 0; i < numWorkers; i++ {
		ew.wg.Add(1)
		go ew.worker()
	}

	if numWorkers > 1 {
		fmt.Printf("🧠 Started %d embedding workers for parallel processing\n", numWorkers)
	}

	return ew
}

// SetEmbedder sets or updates the embedder (for async initialization).
// This allows the worker to start before the model is loaded.
func (ew *EmbedWorker) SetEmbedder(embedder embed.Embedder) {
	ew.mu.Lock()
	ew.embedder = embedder
	ew.mu.Unlock()
	// Trigger immediate processing now that embedder is available
	ew.Trigger()
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

// Trigger wakes up the worker to check for nodes without embeddings.
// Call this after creating a new node.
func (ew *EmbedWorker) Trigger() {
	ew.mu.Lock()
	if ew.closed {
		ew.mu.Unlock()
		return
	}
	ew.mu.Unlock()

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
func (ew *EmbedWorker) Stats() WorkerStats {
	ew.mu.Lock()
	defer ew.mu.Unlock()
	return WorkerStats{
		Running:   ew.running,
		Processed: ew.processed,
		Failed:    ew.failed,
	}
}

// Reset stops the current worker and restarts it fresh.
// This clears processed counts and the recently-processed cache,
// which is necessary when regenerating all embeddings.
func (ew *EmbedWorker) Reset() {
	ew.mu.Lock()
	if ew.closed {
		ew.mu.Unlock()
		return
	}
	// Mark as resetting to prevent Trigger() from sending during reset
	wasRunning := ew.running
	ew.mu.Unlock()

	fmt.Println("🔄 Resetting embed worker for regeneration...")

	// Cancel context to stop current processing
	ew.cancel()

	// Wait for worker to exit (it will exit when context is cancelled)
	ew.wg.Wait()

	// Reset state under lock
	ew.mu.Lock()
	ew.processed = 0
	ew.failed = 0
	ew.running = false
	ew.recentlyProcessed = make(map[string]time.Time)
	ew.loggedSkip = make(map[string]bool)
	ew.mu.Unlock()

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
	ew.mu.Lock()
	ew.closed = true
	ew.mu.Unlock()

	// Stop any pending debounce timer
	ew.clusterDebounceMu.Lock()
	if ew.clusterDebounceTimer != nil {
		ew.clusterDebounceTimer.Stop()
		ew.clusterDebounceTimer = nil
	}
	ew.clusterDebounceMu.Unlock()

	ew.cancel()
	close(ew.trigger)
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
			closed := ew.closed
			ew.mu.Unlock()

			if hasEmbedder {
				fmt.Println("✅ Embedding model loaded, worker active")
				break
			}
			if closed {
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
	// that need embedding (e.g., after restart, bulk import, or cleared embeddings)
	fmt.Println("🔍 Initial scan for nodes needing embeddings...")
	// Refresh index to clean up stale entries from deleted nodes
	ew.refreshEmbeddingIndex()

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
					removed := ew.refreshEmbeddingIndex()
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

	ew.mu.Lock()
	ew.running = true
	ew.mu.Unlock()

	defer func() {
		ew.mu.Lock()
		ew.running = false
		ew.mu.Unlock()
	}()

	// Find one node without embedding
	node := ew.findNodeWithoutEmbedding()
	if node == nil {
		return false // Nothing to process
	}

	// DEBUG: Log where this node came from
	fmt.Printf("🔍 Found node %s from pending embeddings index\n", node.ID)

	// Check for cancellation before processing
	select {
	case <-ew.ctx.Done():
		return false
	default:
	}

	// CRITICAL: Verify node still exists before processing
	// Node might have been deleted between index lookup and now
	// This prevents trying to embed deleted nodes
	existingNode, err := ew.storage.GetNode(node.ID)
	if err != nil {
		// Node was deleted - remove from pending index and skip
		// This is a stale entry - remove it immediately to prevent re-processing
		fmt.Printf("⚠️  Node %s from pending index doesn't exist - removing stale entry\n", node.ID)
		ew.markNodeEmbedded(node.ID)
		return false // Skip this node, try next one
	}

	// DEBUG: Verify the node we found matches what's in storage
	if existingNode == nil {
		fmt.Printf("⚠️  Node %s from pending index is nil - removing stale entry\n", node.ID)
		ew.markNodeEmbedded(node.ID)
		return false
	}

	// Update node with latest data from storage
	node = existingNode

	// Check if this node was recently processed (prevents re-processing before DB commit is visible)
	ew.mu.Lock()
	if lastProcessed, ok := ew.recentlyProcessed[string(node.ID)]; ok {
		if time.Since(lastProcessed) < 30*time.Second {
			// Only log skip message once per node to avoid spam
			if !ew.loggedSkip[string(node.ID)] {
				ew.loggedSkip[string(node.ID)] = true
				fmt.Printf("⏭️  Skipping node %s: recently processed (waiting for DB sync)\n", node.ID)
			}
			ew.mu.Unlock()
			return false // Temporary skip - don't continue looping
		}
		// Time expired, clear the logged flag so we log again if needed
		delete(ew.loggedSkip, string(node.ID))
	}
	// Clean up old entries (older than 1 minute)
	for id, t := range ew.recentlyProcessed {
		if time.Since(t) > time.Minute {
			delete(ew.recentlyProcessed, id)
			delete(ew.loggedSkip, id) // Also clean up logged skip flag
		}
	}
	ew.mu.Unlock()

	fmt.Printf("🔄 Processing node %s for embedding...\n", node.ID)

	// IMPORTANT: Deep copy properties to avoid race conditions
	// The node from storage may be accessed by other goroutines (e.g., HTTP handlers)
	// Modifying the Properties map directly causes "concurrent map iteration and map write"
	node = copyNodeForEmbedding(node)

	// Build text for embedding - always includes labels and all properties
	// Every node gets an embedding, even if it only has labels
	text := buildEmbeddingText(node.Properties, node.Labels)

	// Chunk text if needed
	chunks := chunkText(text, ew.config.ChunkSize, ew.config.ChunkOverlap)

	// Embed all chunks (all nodes are handled the same way - no special File handling)
	embeddings, err := ew.embedder.EmbedBatch(ew.ctx, chunks)
	if err != nil {
		fmt.Printf("⚠️  Failed to embed node %s: %v\n", node.ID, err)
		ew.mu.Lock()
		ew.failed++
		ew.mu.Unlock()
		return true
	}

	// Validate embeddings were generated
	if len(embeddings) == 0 || embeddings[0] == nil || len(embeddings[0]) == 0 {
		fmt.Printf("⚠️  Failed to generate embedding for node %s: empty embedding\n", node.ID)
		ew.mu.Lock()
		ew.failed++
		ew.mu.Unlock()
		return true // Failed but we tried - continue to next node
	}

	// Always store ALL embeddings in ChunkEmbeddings (even single chunk = array of 1)
	// This simplifies logic - always iterate over ChunkEmbeddings, always count the same way
	// Store as struct field, not in properties (so it's filtered out from user queries - opaque)
	node.ChunkEmbeddings = embeddings
	if len(embeddings) > 1 {
		node.Properties["chunk_count"] = len(embeddings)
	}

	// Update node with embedding metadata
	node.Properties["embedding_model"] = ew.embedder.Model()
	node.Properties["embedding_dimensions"] = ew.embedder.Dimensions()
	node.Properties["has_embedding"] = true
	node.Properties["embedded_at"] = time.Now().Format(time.RFC3339)
	node.Properties["embedding"] = true // Marker for IS NOT NULL check

	// CRITICAL: Double-check node still exists before updating
	// This prevents creating orphaned nodes if the node was deleted between
	// the initial check and now. Reload from storage to get latest version.
	// BUT: Preserve the embeddings we just generated!
	chunkEmbeddingsToSave := node.ChunkEmbeddings // Save the chunk embeddings we just generated
	embeddingProps := make(map[string]interface{})
	if node.Properties != nil {
		// Save embedding-related properties
		if val, ok := node.Properties["embedding_model"]; ok {
			embeddingProps["embedding_model"] = val
		}
		if val, ok := node.Properties["embedding_dimensions"]; ok {
			embeddingProps["embedding_dimensions"] = val
		}
		if val, ok := node.Properties["has_embedding"]; ok {
			embeddingProps["has_embedding"] = val
		}
		if val, ok := node.Properties["embedded_at"]; ok {
			embeddingProps["embedded_at"] = val
		}
		if val, ok := node.Properties["embedding"]; ok {
			embeddingProps["embedding"] = val
		}
		if val, ok := node.Properties["has_chunks"]; ok {
			embeddingProps["has_chunks"] = val
		}
		if val, ok := node.Properties["chunk_count"]; ok {
			embeddingProps["chunk_count"] = val
		}
	}

	// Preserve chunk embeddings from struct field (not properties - opaque to users)
	// (chunkEmbeddingsToSave was already declared above)
	chunkEmbeddingsToSave = node.ChunkEmbeddings

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

	// Restore embedding-related properties
	if node.Properties == nil {
		node.Properties = make(map[string]interface{})
	}
	for k, v := range embeddingProps {
		node.Properties[k] = v
	}

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
		// If update failed because node doesn't exist, skip it
		if updateErr == storage.ErrNotFound {
			fmt.Printf("⚠️  Node %s doesn't exist - skipping update to prevent orphaned node\n", node.ID)
			ew.markNodeEmbedded(node.ID)
			return false
		}
		fmt.Printf("⚠️  Failed to update node %s: %v\n", node.ID, updateErr)
		ew.mu.Lock()
		ew.failed++
		ew.mu.Unlock()
		return true // Failed but we tried - continue to next node
	}

	// Call callback to update search index
	if ew.onEmbedded != nil {
		ew.onEmbedded(node)
	}

	// Remove from pending embeddings index (O(1) operation)
	ew.markNodeEmbedded(node.ID)

	ew.mu.Lock()
	ew.processed++
	// Track this node as recently processed to prevent re-processing before DB commit is visible
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

	// Trigger another check immediately if there might be more
	ew.Trigger()

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

// markNodeEmbedded removes a node from the pending embeddings index.
func (ew *EmbedWorker) markNodeEmbedded(nodeID storage.NodeID) {
	if mgr, ok := ew.storage.(EmbeddingIndexManager); ok {
		mgr.MarkNodeEmbedded(nodeID)
	}
}

// embedWithRetry embeds chunks with retry logic and averages if multiple chunks.
func (ew *EmbedWorker) embedWithRetry(chunks []string) ([]float32, error) {
	var allEmbeddings [][]float32
	var err error

	for attempt := 1; attempt <= ew.config.MaxRetries; attempt++ {
		allEmbeddings, err = ew.embedder.EmbedBatch(ew.ctx, chunks)
		if err == nil {
			break
		}

		if attempt < ew.config.MaxRetries {
			backoff := time.Duration(attempt) * 2 * time.Second
			fmt.Printf("   ⚠️  Embed attempt %d failed, retrying in %v\n", attempt, backoff)
			time.Sleep(backoff)
		} else {
			return nil, err
		}
	}

	// If single chunk, return directly
	if len(allEmbeddings) == 1 {
		return allEmbeddings[0], nil
	}

	// Average multiple chunk embeddings
	return averageEmbeddings(allEmbeddings), nil
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

// buildEmbeddingText creates text for embedding from node properties and labels.
// Always returns a non-empty string - at minimum includes labels.
// This ensures every node gets an embedding regardless of property names or types.
//
// The function:
//   - Always includes labels (even if node has no properties)
//   - Includes all non-metadata properties
//   - Converts all property values to string representation
//   - Handles arrays, booleans, numbers, and complex types (via JSON)
func buildEmbeddingText(properties map[string]interface{}, labels []string) string {
	var parts []string

	// Always include labels first - this ensures we always have embeddable content
	if len(labels) > 0 {
		parts = append(parts, fmt.Sprintf("labels: %s", strings.Join(labels, ", ")))
	}

	// Skip these metadata/internal fields
	skipFields := map[string]bool{
		"embedding":            true,
		"has_embedding":        true,
		"embedding_skipped":    true,
		"embedding_model":      true,
		"embedding_dimensions": true,
		"embedded_at":          true,
		"createdAt":            true,
		"updatedAt":            true,
		"id":                   true,
	}

	// Include all properties (even empty strings - they're part of the node representation)
	for key, val := range properties {
		if skipFields[key] {
			continue
		}

		// Convert value to string representation
		var strVal string
		switch v := val.(type) {
		case string:
			// Include even empty strings - they're part of the node
			strVal = v
		case []interface{}:
			// Handle arrays (like tags, lists, etc.)
			strs := make([]string, 0, len(v))
			for _, item := range v {
				if s, ok := item.(string); ok {
					strs = append(strs, s)
				} else {
					// Convert non-string items to string
					strs = append(strs, fmt.Sprintf("%v", item))
				}
			}
			strVal = strings.Join(strs, ", ")
		case bool:
			strVal = fmt.Sprintf("%v", v)
		case int, int64, float64:
			strVal = fmt.Sprintf("%v", v)
		case nil:
			// Include nil values as "null" - they're part of the node representation
			strVal = "null"
		default:
			// For complex types (maps, nested objects), use JSON
			if b, err := json.Marshal(v); err == nil {
				strVal = string(b)
			} else {
				// Fallback to string representation
				strVal = fmt.Sprintf("%v", v)
			}
		}

		parts = append(parts, fmt.Sprintf("%s: %s", key, strVal))
	}

	// Always return at least labels, even if no properties
	result := strings.Join(parts, "\n")
	if result == "" {
		// Fallback: if somehow we have no labels and no properties, use node ID
		// This should never happen, but ensures we always return something
		return "node"
	}
	return result
}

// chunkText splits text into chunks with overlap, trying to break at natural boundaries.
// Returns the original text as single chunk if it fits within chunkSize.
func chunkText(text string, chunkSize, overlap int) []string {
	if len(text) <= chunkSize {
		return []string{text}
	}

	var chunks []string
	start := 0

	for start < len(text) {
		end := start + chunkSize
		if end > len(text) {
			end = len(text)
		}

		// If not the last chunk, try to break at natural boundary
		if end < len(text) {
			chunk := text[start:end]

			// Try paragraph break
			if idx := strings.LastIndex(chunk, "\n\n"); idx > chunkSize/2 {
				end = start + idx
			} else if idx := strings.LastIndex(chunk, ". "); idx > chunkSize/2 {
				// Try sentence break
				end = start + idx + 1
			} else if idx := strings.LastIndex(chunk, " "); idx > chunkSize/2 {
				// Try word break
				end = start + idx
			}
		}

		chunks = append(chunks, text[start:end])

		// Move start forward, accounting for overlap
		nextStart := end - overlap
		if nextStart <= start {
			nextStart = end // Prevent infinite loop
		}
		start = nextStart
	}

	return chunks
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
		ID:           src.ID,
		Labels:       make([]string, len(src.Labels)),
		CreatedAt:    src.CreatedAt,
		UpdatedAt:    src.UpdatedAt,
		LastAccessed: src.LastAccessed,
		AccessCount:  src.AccessCount,
		DecayScore:   src.DecayScore,
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
