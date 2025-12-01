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
}

// EmbedWorkerConfig holds configuration for the embedding worker.
type EmbedWorkerConfig struct {
	// Worker settings
	ScanInterval time.Duration // How often to scan for nodes without embeddings (default: 5s)
	BatchDelay   time.Duration // Delay between processing nodes (default: 500ms)
	MaxRetries   int           // Max retry attempts per node (default: 3)

	// Text chunking settings (matches Mimir: MIMIR_EMBEDDINGS_CHUNK_SIZE, MIMIR_EMBEDDINGS_CHUNK_OVERLAP)
	ChunkSize    int // Max characters per chunk (default: 512)
	ChunkOverlap int // Characters to overlap between chunks (default: 50)
}

// DefaultEmbedWorkerConfig returns sensible defaults.
func DefaultEmbedWorkerConfig() *EmbedWorkerConfig {
	return &EmbedWorkerConfig{
		ScanInterval: 15 * time.Minute,       // Scan for missed nodes every 15 minutes
		BatchDelay:   500 * time.Millisecond, // Delay between processing nodes
		MaxRetries:   3,
		ChunkSize:    512,
		ChunkOverlap: 50,
	}
}

// NewEmbedWorker creates a new async embedding worker.
func NewEmbedWorker(embedder embed.Embedder, storage storage.Engine, config *EmbedWorkerConfig) *EmbedWorker {
	if config == nil {
		config = DefaultEmbedWorkerConfig()
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

	// Start worker
	ew.wg.Add(1)
	go ew.worker()

	return ew
}

// SetOnEmbedded sets a callback to be called after a node is embedded.
// Use this to update search indexes.
func (ew *EmbedWorker) SetOnEmbedded(fn func(node *storage.Node)) {
	ew.onEmbedded = fn
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

// Close gracefully shuts down the worker.
func (ew *EmbedWorker) Close() {
	ew.mu.Lock()
	ew.closed = true
	ew.mu.Unlock()

	ew.cancel()
	close(ew.trigger)
	ew.wg.Wait()
}

// worker runs the embedding loop.
func (ew *EmbedWorker) worker() {
	defer ew.wg.Done()

	fmt.Println("🧠 Embed worker started")

	// Short initial delay to let server start
	time.Sleep(500 * time.Millisecond)

	// Immediate scan on startup for any existing nodes needing embedding
	fmt.Println("🔍 Initial scan for nodes needing embeddings...")
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
func (ew *EmbedWorker) processUntilEmpty() {
	for {
		select {
		case <-ew.ctx.Done():
			return
		default:
			// processNextBatch returns true if it actually processed or skipped a node
			// It returns false if there was nothing to process
			didWork := ew.processNextBatch()
			if !didWork {
				return // No more nodes to process
			}
			// Small delay between batches to avoid CPU spin
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// processNextBatch finds and processes nodes without embeddings.
// Returns true if it did useful work (processed or permanently skipped a node).
// Returns false if there was nothing to process or if a node was temporarily skipped.
func (ew *EmbedWorker) processNextBatch() bool {
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

	// Build text for embedding
	text := buildEmbeddingText(node.Properties)
	if text == "" {
		// No content - mark as processed but skip
		fmt.Printf("⏭️  Skipping node %s: no embeddable content\n", node.ID)
		node.Properties["has_embedding"] = false
		node.Properties["embedding_skipped"] = "no content"
		_ = ew.storage.UpdateNode(node)

		// Track as recently processed
		ew.mu.Lock()
		ew.recentlyProcessed[string(node.ID)] = time.Now()
		ew.mu.Unlock()

		// Immediately try next node (don't wait for next trigger)
		ew.wg.Add(1)
		go func(ctx context.Context) {
			defer ew.wg.Done()
			select {
			case <-time.After(100 * time.Millisecond):
				ew.Trigger()
			case <-ctx.Done():
				// Worker is shutting down, abort
				return
			}
		}(ew.ctx)
		return true // Permanently skipped node (no content) - continue to next
	}

	// Chunk text if needed
	chunks := chunkText(text, ew.config.ChunkSize, ew.config.ChunkOverlap)

	// Embed with retry
	embedding, err := ew.embedWithRetry(chunks)
	if err != nil {
		fmt.Printf("⚠️  Failed to embed node %s: %v\n", node.ID, err)
		ew.mu.Lock()
		ew.failed++
		ew.mu.Unlock()
		return true // Failed but we tried - continue to next node
	}

	// Update node with embedding
	node.Embedding = embedding
	node.Properties["embedding_model"] = ew.embedder.Model()
	node.Properties["embedding_dimensions"] = ew.embedder.Dimensions()
	node.Properties["has_embedding"] = true
	node.Properties["embedded_at"] = time.Now().Format(time.RFC3339)
	if len(chunks) > 1 {
		node.Properties["embedding_chunks"] = len(chunks)
	}

	fmt.Printf("📝 Saving embedding for node %s: %d dims, embedding length=%d\n",
		node.ID, len(embedding), len(node.Embedding))

	// Use embedding-specific update if available (uses OpUpdateEmbedding in WAL)
	// This is safe to skip during recovery since embeddings can be regenerated
	var updateErr error
	if embedUpdater, ok := ew.storage.(interface{ UpdateNodeEmbedding(*storage.Node) error }); ok {
		fmt.Printf("   Using UpdateNodeEmbedding for node %s\n", node.ID)
		updateErr = embedUpdater.UpdateNodeEmbedding(node)
	} else {
		fmt.Printf("   Using UpdateNode for node %s\n", node.ID)
		updateErr = ew.storage.UpdateNode(node)
	}
	if updateErr != nil {
		fmt.Printf("⚠️  Failed to update node %s: %v\n", node.ID, updateErr)
		ew.mu.Lock()
		ew.failed++
		ew.mu.Unlock()
		return true // Failed but we tried - continue to next node
	}

	// Verify the update was persisted
	if getter, ok := ew.storage.(interface {
		GetNode(storage.NodeID) (*storage.Node, error)
	}); ok {
		verified, verifyErr := getter.GetNode(node.ID)
		if verifyErr != nil {
			fmt.Printf("⚠️  Failed to verify update for %s: %v\n", node.ID, verifyErr)
		} else if len(verified.Embedding) == 0 {
			fmt.Printf("🔴 VERIFY FAILED: Node %s has NO embedding after update!\n", node.ID)
		} else {
			fmt.Printf("✓ Verified: Node %s has %d-dim embedding stored\n", node.ID, len(verified.Embedding))
		}
	}

	// Call callback to update search index
	if ew.onEmbedded != nil {
		ew.onEmbedded(node)
	}

	ew.mu.Lock()
	ew.processed++
	// Track this node as recently processed to prevent re-processing before DB commit is visible
	ew.recentlyProcessed[string(node.ID)] = time.Now()
	ew.mu.Unlock()

	fmt.Printf("✅ Embedded node %s (%d dims, %d chunks)\n", node.ID, len(embedding), len(chunks))

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

// buildEmbeddingText creates text for embedding from node properties.
// The text will be chunked by the caller if it exceeds ChunkSize.
func buildEmbeddingText(properties map[string]interface{}) string {
	var parts []string

	// Priority fields for embedding
	priorityFields := []string{"title", "content", "description", "name", "text", "body", "summary"}

	for _, field := range priorityFields {
		if val, ok := properties[field]; ok {
			if str, ok := val.(string); ok && str != "" {
				parts = append(parts, str)
			}
		}
	}

	// Add type if present
	if nodeType, ok := properties["type"].(string); ok && nodeType != "" {
		parts = append(parts, "Type: "+nodeType)
	}

	// Add tags if present
	if tags, ok := properties["tags"].([]interface{}); ok && len(tags) > 0 {
		tagStrs := make([]string, 0, len(tags))
		for _, t := range tags {
			if s, ok := t.(string); ok {
				tagStrs = append(tagStrs, s)
			}
		}
		if len(tagStrs) > 0 {
			parts = append(parts, "Tags: "+strings.Join(tagStrs, ", "))
		}
	}

	// Add reasoning/rationale if present (important for memories)
	if reasoning, ok := properties["reasoning"].(string); ok && reasoning != "" {
		parts = append(parts, reasoning)
	}

	return strings.Join(parts, "\n\n")
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

	// Copy embedding if present
	if len(src.Embedding) > 0 {
		dst.Embedding = make([]float32, len(src.Embedding))
		copy(dst.Embedding, src.Embedding)
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
