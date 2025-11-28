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

	// Stats
	mu        sync.Mutex
	processed int
	failed    int
	running   bool
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
		ScanInterval: 5 * time.Second,
		BatchDelay:   500 * time.Millisecond,
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
		embedder: embedder,
		storage:  storage,
		config:   config,
		ctx:      ctx,
		cancel:   cancel,
		trigger:  make(chan struct{}, 1),
	}

	// Start worker
	ew.wg.Add(1)
	go ew.worker()

	return ew
}

// Trigger wakes up the worker to check for nodes without embeddings.
// Call this after creating a new node.
func (ew *EmbedWorker) Trigger() {
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
	ew.cancel()
	close(ew.trigger)
	ew.wg.Wait()
}

// worker runs the embedding loop.
func (ew *EmbedWorker) worker() {
	defer ew.wg.Done()

	fmt.Println("🧠 Embed worker started")

	// Initial delay to let server start
	time.Sleep(2 * time.Second)

	ticker := time.NewTicker(ew.config.ScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ew.ctx.Done():
			fmt.Println("🧠 Embed worker stopped")
			return

		case <-ew.trigger:
			// Immediate trigger - process now
			ew.processNextBatch()

		case <-ticker.C:
			// Regular interval scan
			ew.processNextBatch()
		}
	}
}

// processNextBatch finds and processes nodes without embeddings.
func (ew *EmbedWorker) processNextBatch() {
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
		return // Nothing to process
	}

	fmt.Printf("🔄 Processing node %s for embedding...\n", node.ID)

	// Build text for embedding
	text := buildEmbeddingText(node.Properties)
	if text == "" {
		// No content - mark as processed but skip
		node.Properties["has_embedding"] = false
		node.Properties["embedding_skipped"] = "no content"
		_ = ew.storage.UpdateNode(node)
		return
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
		return
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

	if err := ew.storage.UpdateNode(node); err != nil {
		fmt.Printf("⚠️  Failed to update node %s: %v\n", node.ID, err)
		ew.mu.Lock()
		ew.failed++
		ew.mu.Unlock()
		return
	}

	ew.mu.Lock()
	ew.processed++
	ew.mu.Unlock()

	fmt.Printf("✅ Embedded node %s (%d dims, %d chunks)\n", node.ID, len(embedding), len(chunks))

	// Small delay before next
	time.Sleep(ew.config.BatchDelay)

	// Trigger another check immediately if there might be more
	ew.Trigger()
}

// findNodeWithoutEmbedding scans storage for a single node that needs embedding.
// Uses streaming/iteration to avoid loading all nodes at once.
func (ew *EmbedWorker) findNodeWithoutEmbedding() *storage.Node {
	// Get all nodes but iterate - AllNodes returns a slice unfortunately
	// For now, just get first match. Could optimize with a query later.
	nodes, err := ew.storage.AllNodes()
	if err != nil {
		return nil
	}

	for _, node := range nodes {
		// Skip internal nodes
		for _, label := range node.Labels {
			if strings.HasPrefix(label, "_") {
				continue
			}
		}

		// Skip if already has embedding
		if len(node.Embedding) > 0 {
			continue
		}

		// Skip if already checked and has no content
		if _, ok := node.Properties["embedding_skipped"]; ok {
			continue
		}

		// Skip if marked as having embedding (even if empty - might be queued)
		if hasEmbed, ok := node.Properties["has_embedding"].(bool); ok && hasEmbed {
			continue
		}

		// Found one that needs processing
		return node
	}

	return nil
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
