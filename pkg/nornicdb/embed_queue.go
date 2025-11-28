// Package nornicdb provides async embedding queue for background embedding generation.
package nornicdb

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// EmbedQueue manages async embedding generation with durable persistence.
// Nodes are queued for embedding and processed in the background without blocking.
type EmbedQueue struct {
	embedder embed.Embedder
	storage  storage.Engine
	
	mu       sync.Mutex
	queue    chan string        // In-memory channel for fast queuing
	pending  map[string]bool    // Track what's in queue to avoid duplicates
	
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	
	batchSize     int
	batchInterval time.Duration
	workers       int
}

// EmbedQueueConfig holds configuration for the embedding queue.
type EmbedQueueConfig struct {
	BatchSize     int           // How many to process at once (default: 10)
	BatchInterval time.Duration // Max time to wait before processing partial batch (default: 5s)
	Workers       int           // Number of worker goroutines (default: 2)
	QueueSize     int           // In-memory queue buffer size (default: 10000)
}

// DefaultEmbedQueueConfig returns sensible defaults.
func DefaultEmbedQueueConfig() *EmbedQueueConfig {
	return &EmbedQueueConfig{
		BatchSize:     10,
		BatchInterval: 5 * time.Second,
		Workers:       2,
		QueueSize:     10000,
	}
}

// NewEmbedQueue creates a new async embedding queue.
func NewEmbedQueue(embedder embed.Embedder, storage storage.Engine, config *EmbedQueueConfig) *EmbedQueue {
	if config == nil {
		config = DefaultEmbedQueueConfig()
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	eq := &EmbedQueue{
		embedder:      embedder,
		storage:       storage,
		queue:         make(chan string, config.QueueSize),
		pending:       make(map[string]bool),
		ctx:           ctx,
		cancel:        cancel,
		batchSize:     config.BatchSize,
		batchInterval: config.BatchInterval,
		workers:       config.Workers,
	}
	
	// Start workers
	for i := 0; i < config.Workers; i++ {
		eq.wg.Add(1)
		go eq.worker(i)
	}
	
	// Recover any pending items from storage on startup
	go eq.recoverPending()
	
	return eq
}

// Enqueue adds a node ID to the embedding queue (non-blocking).
func (eq *EmbedQueue) Enqueue(nodeID string) {
	eq.mu.Lock()
	if eq.pending[nodeID] {
		eq.mu.Unlock()
		return // Already queued
	}
	eq.pending[nodeID] = true
	eq.mu.Unlock()
	
	// Mark as pending in storage for durability
	eq.markPending(nodeID)
	
	// Non-blocking send to channel
	select {
	case eq.queue <- nodeID:
	default:
		// Queue full - will be recovered from storage later
	}
}

// QueueStats returns current queue statistics.
type QueueStats struct {
	Pending    int `json:"pending"`
	Processing int `json:"processing"`
	Completed  int `json:"completed"`
	Failed     int `json:"failed"`
}

// Stats returns current queue statistics.
func (eq *EmbedQueue) Stats() QueueStats {
	eq.mu.Lock()
	defer eq.mu.Unlock()
	return QueueStats{
		Pending: len(eq.pending),
	}
}

// Close gracefully shuts down the queue.
func (eq *EmbedQueue) Close() {
	eq.cancel()
	close(eq.queue)
	eq.wg.Wait()
}

// worker processes embedding requests in batches.
func (eq *EmbedQueue) worker(id int) {
	defer eq.wg.Done()
	
	batch := make([]string, 0, eq.batchSize)
	timer := time.NewTimer(eq.batchInterval)
	
	for {
		select {
		case <-eq.ctx.Done():
			// Process remaining batch before exit
			if len(batch) > 0 {
				eq.processBatch(batch)
			}
			return
			
		case nodeID, ok := <-eq.queue:
			if !ok {
				// Channel closed
				if len(batch) > 0 {
					eq.processBatch(batch)
				}
				return
			}
			
			batch = append(batch, nodeID)
			
			if len(batch) >= eq.batchSize {
				eq.processBatch(batch)
				batch = make([]string, 0, eq.batchSize)
				timer.Reset(eq.batchInterval)
			}
			
		case <-timer.C:
			// Process partial batch on timeout
			if len(batch) > 0 {
				eq.processBatch(batch)
				batch = make([]string, 0, eq.batchSize)
			}
			timer.Reset(eq.batchInterval)
		}
	}
}

// processBatch generates embeddings for a batch of nodes.
func (eq *EmbedQueue) processBatch(nodeIDs []string) {
	if len(nodeIDs) == 0 {
		return
	}
	
	// Collect texts for batch embedding
	texts := make([]string, 0, len(nodeIDs))
	validIDs := make([]string, 0, len(nodeIDs))
	nodes := make([]*storage.Node, 0, len(nodeIDs))
	
	for _, id := range nodeIDs {
		node, err := eq.storage.GetNode(storage.NodeID(id))
		if err != nil {
			eq.clearPending(id)
			continue
		}
		
		// Skip if already has embedding
		if len(node.Embedding) > 0 {
			eq.clearPending(id)
			continue
		}
		
		text := buildEmbeddingText(node.Properties)
		if text == "" {
			eq.clearPending(id)
			continue
		}
		
		texts = append(texts, text)
		validIDs = append(validIDs, id)
		nodes = append(nodes, node)
	}
	
	if len(texts) == 0 {
		return
	}
	
	// Batch embed
	embeddings, err := eq.embedder.EmbedBatch(eq.ctx, texts)
	if err != nil {
		fmt.Printf("⚠️  Embed batch failed: %v\n", err)
		return
	}
	
	// Update nodes with embeddings
	for i, emb := range embeddings {
		if i >= len(nodes) {
			break
		}
		
		node := nodes[i]
		node.Embedding = emb
		node.Properties["embedding_model"] = eq.embedder.Model()
		node.Properties["embedding_dimensions"] = eq.embedder.Dimensions()
		node.Properties["has_embedding"] = true
		node.Properties["embedded_at"] = time.Now().Format(time.RFC3339)
		
		if err := eq.storage.UpdateNode(node); err != nil {
			fmt.Printf("⚠️  Failed to update node %s with embedding: %v\n", validIDs[i], err)
		}
		
		eq.clearPending(validIDs[i])
	}
	
	fmt.Printf("🧠 Embedded %d nodes\n", len(embeddings))
}

// markPending persists node ID to durable queue.
func (eq *EmbedQueue) markPending(nodeID string) {
	// Store as a special node type for durability
	pendingNode := &storage.Node{
		ID:     storage.NodeID("_embed_pending_" + nodeID),
		Labels: []string{"_EmbedPending"},
		Properties: map[string]interface{}{
			"target_node_id": nodeID,
			"queued_at":      time.Now().Format(time.RFC3339),
		},
	}
	_ = eq.storage.CreateNode(pendingNode) // Best effort
}

// clearPending removes node from durable queue.
func (eq *EmbedQueue) clearPending(nodeID string) {
	eq.mu.Lock()
	delete(eq.pending, nodeID)
	eq.mu.Unlock()
	
	// Remove from durable storage
	_ = eq.storage.DeleteNode(storage.NodeID("_embed_pending_" + nodeID))
}

// recoverPending loads unprocessed items from storage on startup.
func (eq *EmbedQueue) recoverPending() {
	// Find all pending embed nodes
	nodes, err := eq.storage.AllNodes()
	if err != nil {
		return
	}
	
	recovered := 0
	for _, node := range nodes {
		for _, label := range node.Labels {
			if label == "_EmbedPending" {
				if targetID, ok := node.Properties["target_node_id"].(string); ok {
					eq.mu.Lock()
					if !eq.pending[targetID] {
						eq.pending[targetID] = true
						eq.mu.Unlock()
						select {
						case eq.queue <- targetID:
							recovered++
						default:
						}
					} else {
						eq.mu.Unlock()
					}
				}
				break
			}
		}
	}
	
	if recovered > 0 {
		fmt.Printf("🔄 Recovered %d pending embeddings from queue\n", recovered)
	}
}

// buildEmbeddingText creates text for embedding from node properties.
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
			parts = append(parts, "Tags: "+joinStrings(tagStrs, ", "))
		}
	}
	
	// Add reasoning/rationale if present (important for memories)
	if reasoning, ok := properties["reasoning"].(string); ok && reasoning != "" {
		parts = append(parts, reasoning)
	}
	
	return joinStrings(parts, "\n\n")
}

// joinStrings joins strings with separator.
func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += sep + strs[i]
	}
	return result
}

// EnqueueExisting queues all existing nodes that don't have embeddings.
func (eq *EmbedQueue) EnqueueExisting(ctx context.Context) (int, error) {
	nodes, err := eq.storage.AllNodes()
	if err != nil {
		return 0, err
	}
	
	count := 0
	for _, node := range nodes {
		// Skip internal nodes
		isInternal := false
		for _, label := range node.Labels {
			if label == "_EmbedPending" {
				isInternal = true
				break
			}
		}
		if isInternal {
			continue
		}
		
		// Skip if already has embedding
		if len(node.Embedding) > 0 {
			continue
		}
		
		// Skip if no content to embed
		text := buildEmbeddingText(node.Properties)
		if text == "" {
			continue
		}
		
		eq.Enqueue(string(node.ID))
		count++
	}
	
	return count, nil
}

// MarshalJSON for queue stats.
func (s QueueStats) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]int{
		"pending":    s.Pending,
		"processing": s.Processing,
		"completed":  s.Completed,
		"failed":     s.Failed,
	})
}
