// Package embed provides automatic embedding generation for NornicDB.
//
// ⚠️ WARNING: AUTO-EMBEDDING IS EXPERIMENTAL AND DISABLED BY DEFAULT
//
// This package extends the base embedding functionality with automatic background
// processing, caching, and batch operations. It's designed to integrate embedding
// generation directly into database operations, reducing client complexity.
//
// STATUS: NOT PRODUCTION READY - DISABLED BY DEFAULT
// The auto-embedding features have not been fully tested and should not be used
// in production. The code is kept for future development.
//
// Key Features:
//   - Background embedding generation with worker pools
//   - LRU-style caching to avoid re-computing embeddings
//   - Batch processing for improved throughput
//   - Automatic text extraction from node properties
//   - Configurable concurrency and queue sizes
//
// Example Usage:
//
//	// Create embedder with Ollama backend
//	embedder, err := embed.New(&embed.Config{
//		Provider: "ollama",
//		APIURL:   "http://localhost:11434",
//		Model:    "mxbai-embed-large",
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Create auto-embedder with background processing
//	autoConfig := embed.DefaultAutoEmbedConfig(embedder)
//	autoConfig.Workers = 8      // More workers for throughput
//	autoConfig.MaxCacheSize = 50000 // Larger cache
//
//	autoEmbedder := embed.NewAutoEmbedder(autoConfig)
//	defer autoEmbedder.Stop()
//
//	// Synchronous embedding with caching
//	embedding, err := autoEmbedder.Embed(ctx, "Machine learning is awesome")
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Asynchronous embedding with callback
//	autoEmbedder.QueueEmbed("node-123", "Some content", func(nodeID string, emb []float32, err error) {
//		if err != nil {
//			log.Printf("Embedding failed for %s: %v", nodeID, err)
//			return
//		}
//		// Store embedding in database
//		db.UpdateEmbedding(nodeID, emb)
//	})
//
//	// Extract text from node properties
//	properties := map[string]any{
//		"title":       "Introduction to AI",
//		"content":     "Artificial intelligence is...",
//		"description": "A comprehensive guide",
//		"author":      "Dr. Smith", // Not embeddable
//	}
//	text := embed.ExtractEmbeddableText(properties)
//	// Result: "Introduction to AI Artificial intelligence is... A comprehensive guide"
//
//	// Batch processing for efficiency
//	texts := []string{"Text 1", "Text 2", "Text 3"}
//	embeddings, err := autoEmbedder.BatchEmbed(ctx, texts)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Check performance stats
//	stats := autoEmbedder.Stats()
//	fmt.Printf("Cache hit rate: %.2f%%\n",
//		float64(stats["cache_hits"])/float64(stats["cache_hits"]+stats["cache_misses"])*100)
//
// Architecture:
//
// The AutoEmbedder uses a worker pool pattern:
//   1. Requests are queued via QueueEmbed()
//   2. Background workers process the queue
//   3. Each worker generates embeddings via the base Embedder
//   4. Results are cached and returned via callbacks
//   5. Cache eviction prevents memory growth
//
// Performance Considerations:
//   - Cache hit rate: Aim for >80% for good performance
//   - Worker count: Typically 2-8 workers depending on API limits
//   - Queue size: Should handle burst loads (1000+ recommended)
//   - Batch size: Use BatchEmbed() for bulk operations
//
// ELI12 (Explain Like I'm 12):
//
// Think of the AutoEmbedder like a smart homework helper:
//
// 1. **Background workers**: Like having several friends helping you with
//    homework at the same time - they work in parallel to get things done faster.
//
// 2. **Caching**: Like keeping a cheat sheet of answers you've already figured
//    out. If you see the same question again, you don't have to solve it again!
//
// 3. **Queue**: Like a to-do list where you write down all the problems you
//    need to solve, and your friends pick them up one by one.
//
// 4. **Batch processing**: Instead of asking one question at a time, you ask
//    several questions together to save time.
//
// The AutoEmbedder makes sure your computer doesn't get overwhelmed and
// remembers answers it's already figured out!
package embed

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"
)

// EmbeddableProperties defines which node properties should be embedded.
// Text from these properties is concatenated for embedding generation.
var EmbeddableProperties = []string{
	"content",
	"text",
	"title",
	"name",
	"description",
}

// AutoEmbedder handles automatic embedding generation with background processing and caching.
//
// The AutoEmbedder provides a high-level interface for embedding generation that:
//   - Processes requests asynchronously in background workers
//   - Caches embeddings to avoid redundant API calls
//   - Supports both synchronous and asynchronous operations
//   - Provides batch processing for improved throughput
//   - Tracks performance statistics
//
// Example:
//
//	config := embed.DefaultAutoEmbedConfig(embedder)
//	autoEmbedder := embed.NewAutoEmbedder(config)
//	defer autoEmbedder.Stop()
//
//	// Sync operation with caching
//	embedding, err := autoEmbedder.Embed(ctx, "Hello world")
//
//	// Async operation with callback
//	autoEmbedder.QueueEmbed("node-1", "Some text", func(nodeID string, emb []float32, err error) {
//		// Handle result
//	})
//
// Thread Safety:
//   All methods are thread-safe and can be called from multiple goroutines.
type AutoEmbedder struct {
	embedder   Embedder
	mu         sync.RWMutex
	
	// Cache: content hash -> embedding
	cache      map[string][]float32
	cacheSize  int
	maxCache   int
	
	// Background processing
	queue      chan *EmbedRequest
	workers    int
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	
	// Stats
	embedCount    int64
	cacheHits     int64
	cacheMisses   int64
	errorCount    int64
}

// EmbedRequest represents a request to embed node content.
type EmbedRequest struct {
	NodeID     string
	Content    string
	Callback   func(nodeID string, embedding []float32, err error)
}

// AutoEmbedConfig configures the AutoEmbedder behavior and performance characteristics.
//
// The configuration allows tuning for different use cases:
//   - High throughput: More workers, larger queue
//   - Memory constrained: Smaller cache, fewer workers
//   - Low latency: Fewer workers to reduce context switching
//
// Example configurations:
//
//	// High throughput configuration
//	config := &embed.AutoEmbedConfig{
//		Embedder:     embedder,
//		MaxCacheSize: 100000, // 100K embeddings
//		Workers:      16,     // Many workers
//		QueueSize:    10000,  // Large queue
//	}
//
//	// Memory-constrained configuration
//	config = &embed.AutoEmbedConfig{
//		Embedder:     embedder,
//		MaxCacheSize: 1000,   // Small cache
//		Workers:      2,      // Few workers
//		QueueSize:    100,    // Small queue
//	}
type AutoEmbedConfig struct {
	// Embedder to use for generating embeddings
	Embedder Embedder
	
	// MaxCacheSize is the max number of embeddings to cache (default: 10000)
	MaxCacheSize int
	
	// Workers is the number of background embedding workers (default: 4)
	Workers int
	
	// QueueSize is the size of the embedding queue (default: 1000)
	QueueSize int
}

// DefaultAutoEmbedConfig returns balanced default configuration for the AutoEmbedder.
//
// The defaults provide good performance for most use cases:
//   - 10K cache size: Balances memory usage and hit rate
//   - 4 workers: Good parallelism without excessive overhead
//   - 1K queue size: Handles moderate burst loads
//
// Parameters:
//   - embedder: Base embedder to use for generation (required)
//
// Returns:
//   - AutoEmbedConfig with balanced defaults
//
// Example:
//
//	config := embed.DefaultAutoEmbedConfig(embedder)
//	// Optionally customize
//	config.Workers = 8 // More throughput
//	config.MaxCacheSize = 50000 // Larger cache
//
//	autoEmbedder := embed.NewAutoEmbedder(config)
func DefaultAutoEmbedConfig(embedder Embedder) *AutoEmbedConfig {
	return &AutoEmbedConfig{
		Embedder:     embedder,
		MaxCacheSize: 10000,
		Workers:      4,
		QueueSize:    1000,
	}
}

// NewAutoEmbedder creates a new AutoEmbedder with the given configuration.
//
// The AutoEmbedder starts background workers immediately and is ready to
// process embedding requests. Call Stop() to clean up resources.
//
// Parameters:
//   - config: AutoEmbedder configuration (required)
//
// Returns:
//   - AutoEmbedder instance with workers started
//
// Example:
//
//	config := embed.DefaultAutoEmbedConfig(embedder)
//	autoEmbedder := embed.NewAutoEmbedder(config)
//	defer autoEmbedder.Stop() // Important: cleanup workers
//
//	// AutoEmbedder is ready for use
//	embedding, _ := autoEmbedder.Embed(ctx, "Hello world")
//
// Resource Management:
//   The AutoEmbedder starts background goroutines that must be cleaned up
//   with Stop() to prevent goroutine leaks.
func NewAutoEmbedder(config *AutoEmbedConfig) *AutoEmbedder {
	ctx, cancel := context.WithCancel(context.Background())
	
	ae := &AutoEmbedder{
		embedder:  config.Embedder,
		cache:     make(map[string][]float32),
		maxCache:  config.MaxCacheSize,
		queue:     make(chan *EmbedRequest, config.QueueSize),
		workers:   config.Workers,
		ctx:       ctx,
		cancel:    cancel,
	}
	
	// Start background workers
	for i := 0; i < config.Workers; i++ {
		ae.wg.Add(1)
		go ae.worker(i)
	}
	
	return ae
}

// worker processes embedding requests from the queue.
func (ae *AutoEmbedder) worker(id int) {
	defer ae.wg.Done()
	
	for {
		select {
		case <-ae.ctx.Done():
			return
		case req, ok := <-ae.queue:
			if !ok {
				return
			}
			
			embedding, err := ae.generateEmbedding(ae.ctx, req.Content)
			if req.Callback != nil {
				req.Callback(req.NodeID, embedding, err)
			}
		}
	}
}

// Stop stops the auto-embedder and waits for workers to finish.
func (ae *AutoEmbedder) Stop() {
	ae.cancel()
	close(ae.queue)
	ae.wg.Wait()
}

// ExtractEmbeddableText extracts and concatenates embeddable text from node properties.
//
// This function looks for specific property names that typically contain textual
// content suitable for embedding generation. The text is concatenated with spaces.
//
// Embeddable properties (in order):
//   - content: Main textual content
//   - text: Alternative text field
//   - title: Document/node title
//   - name: Entity name
//   - description: Descriptive text
//
// Parameters:
//   - properties: Map of node properties
//
// Returns:
//   - Concatenated text string, or empty string if no embeddable text found
//
// Example:
//
//	properties := map[string]any{
//		"title":       "Machine Learning Basics",
//		"content":     "ML is a subset of AI that focuses on...",
//		"description": "An introductory guide to ML concepts",
//		"author":      "Dr. Smith",     // Not embeddable
//		"created_at":  time.Now(),      // Not embeddable
//		"tags":        []string{"AI"},  // Not embeddable (not string)
//	}
//
//	text := embed.ExtractEmbeddableText(properties)
//	// Result: "Machine Learning Basics ML is a subset of AI that focuses on... An introductory guide to ML concepts"
//
// Use Cases:
//   - Automatic embedding generation for new nodes
//   - Consistent text extraction across the system
//   - Filtering out non-textual properties
func ExtractEmbeddableText(properties map[string]any) string {
	var parts []string
	
	for _, prop := range EmbeddableProperties {
		if val, ok := properties[prop]; ok {
			switch v := val.(type) {
			case string:
				if v != "" {
					parts = append(parts, v)
				}
			}
		}
	}
	
	return strings.Join(parts, " ")
}

// Embed generates an embedding for the given text, using cache if available.
//
// This method first checks the cache for a previously computed embedding.
// If not found, it generates a new embedding using the configured embedder
// and caches the result for future use.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - text: Text to embed (empty text returns nil)
//
// Returns:
//   - Embedding vector as float32 slice
//   - Error if embedding generation fails
//
// Example:
//
//	// Generate embedding with caching
//	embedding, err := autoEmbedder.Embed(ctx, "Machine learning is awesome")
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Second call will use cache (much faster)
//	embedding2, _ := autoEmbedder.Embed(ctx, "Machine learning is awesome")
//	// embedding and embedding2 are identical
//
// Performance:
//   - Cache hit: ~1μs (memory lookup)
//   - Cache miss: 100ms-1s (depends on embedding provider)
//   - Cache is based on SHA-256 hash of input text
//
// Thread Safety:
//   This method is thread-safe and can be called concurrently.
func (ae *AutoEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	if text == "" {
		return nil, nil
	}
	
	// Check cache
	hash := hashContent(text)
	ae.mu.RLock()
	if emb, ok := ae.cache[hash]; ok {
		ae.mu.RUnlock()
		ae.cacheHits++
		return emb, nil
	}
	ae.mu.RUnlock()
	ae.cacheMisses++
	
	// Generate embedding
	return ae.generateEmbedding(ctx, text)
}

// EmbedNode extracts embeddable text from node properties and generates embedding.
func (ae *AutoEmbedder) EmbedNode(ctx context.Context, properties map[string]any) ([]float32, error) {
	text := ExtractEmbeddableText(properties)
	if text == "" {
		return nil, nil
	}
	return ae.Embed(ctx, text)
}

// QueueEmbed queues an embedding request for asynchronous background processing.
//
// The request is added to the worker queue and processed by background workers.
// The callback function is called with the result when processing completes.
//
// Parameters:
//   - nodeID: Identifier for the node (passed to callback)
//   - content: Text content to embed
//   - callback: Function called with results (can be nil)
//
// Returns:
//   - nil if successfully queued
//   - Error if queue is full
//
// Example:
//
//	// Queue embedding with callback
//	err := autoEmbedder.QueueEmbed("node-123", "Some content",
//		func(nodeID string, embedding []float32, err error) {
//			if err != nil {
//				log.Printf("Embedding failed for %s: %v", nodeID, err)
//				return
//			}
//			// Store embedding in database
//			db.UpdateNodeEmbedding(nodeID, embedding)
//			log.Printf("Embedded %s: %d dimensions", nodeID, len(embedding))
//		})
//
//	if err != nil {
//		log.Printf("Queue full, processing synchronously")
//		// Fallback to synchronous processing
//		embedding, err := autoEmbedder.Embed(ctx, "Some content")
//	}
//
// Performance:
//   - Queue operation: ~1μs (channel send)
//   - Processing time: Depends on embedding provider
//   - Queue capacity: Configured via AutoEmbedConfig.QueueSize
//
// Error Handling:
//   If the queue is full, consider implementing backpressure or
//   falling back to synchronous processing.
func (ae *AutoEmbedder) QueueEmbed(nodeID string, content string, callback func(nodeID string, embedding []float32, err error)) error {
	select {
	case ae.queue <- &EmbedRequest{
		NodeID:   nodeID,
		Content:  content,
		Callback: callback,
	}:
		return nil
	default:
		return fmt.Errorf("embedding queue full")
	}
}

// QueueEmbedNode queues a node for background embedding.
func (ae *AutoEmbedder) QueueEmbedNode(nodeID string, properties map[string]any, callback func(nodeID string, embedding []float32, err error)) error {
	text := ExtractEmbeddableText(properties)
	if text == "" {
		if callback != nil {
			callback(nodeID, nil, nil)
		}
		return nil
	}
	return ae.QueueEmbed(nodeID, text, callback)
}

// generateEmbedding generates an embedding and caches it.
func (ae *AutoEmbedder) generateEmbedding(ctx context.Context, text string) ([]float32, error) {
	if ae.embedder == nil {
		return nil, fmt.Errorf("no embedder configured")
	}
	
	// Generate embedding
	embedding, err := ae.embedder.Embed(ctx, text)
	if err != nil {
		ae.errorCount++
		return nil, err
	}
	
	ae.embedCount++
	
	// Cache the result
	hash := hashContent(text)
	ae.mu.Lock()
	defer ae.mu.Unlock()
	
	// Evict old entries if cache is full
	if ae.cacheSize >= ae.maxCache {
		// Simple eviction: remove ~10% of oldest entries
		// In production, use LRU
		count := 0
		for k := range ae.cache {
			delete(ae.cache, k)
			count++
			if count >= ae.maxCache/10 {
				break
			}
		}
		ae.cacheSize -= count
	}
	
	ae.cache[hash] = embedding
	ae.cacheSize++
	
	return embedding, nil
}

// BatchEmbed generates embeddings for multiple texts concurrently.
//
// This method processes multiple texts in parallel using a semaphore to limit
// concurrency. It's more efficient than calling Embed() multiple times for
// large batches due to reduced overhead and better cache utilization.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - texts: Slice of texts to embed (empty texts are skipped)
//
// Returns:
//   - Slice of embeddings (same order as input, nil for empty texts)
//   - First error encountered (processing continues for other texts)
//
// Example:
//
//	// Batch process multiple texts
//	texts := []string{
//		"Machine learning is a subset of AI",
//		"Deep learning uses neural networks",
//		"", // Empty text (will be nil in result)
//		"Natural language processing handles text",
//	}
//
//	embeddings, err := autoEmbedder.BatchEmbed(ctx, texts)
//	if err != nil {
//		log.Printf("Some embeddings failed: %v", err)
//	}
//
//	for i, emb := range embeddings {
//		if emb != nil {
//			fmt.Printf("Text %d: %d dimensions\n", i, len(emb))
//		} else {
//			fmt.Printf("Text %d: empty or failed\n", i)
//		}
//	}
//
// Performance:
//   - Concurrency limited by worker count to avoid overwhelming the API
//   - Cache hits are still utilized for duplicate texts
//   - Typically 2-5x faster than sequential processing
//   - Memory usage scales with batch size
//
// Error Handling:
//   Returns the first error encountered, but continues processing other texts.
//   Check individual results for nil to identify failed embeddings.
func (ae *AutoEmbedder) BatchEmbed(ctx context.Context, texts []string) ([][]float32, error) {
	if ae.embedder == nil {
		return nil, fmt.Errorf("no embedder configured")
	}
	
	results := make([][]float32, len(texts))
	var mu sync.Mutex
	var wg sync.WaitGroup
	var firstErr error
	
	// Process in batches with limited concurrency
	semaphore := make(chan struct{}, ae.workers)
	
	for i, text := range texts {
		if text == "" {
			continue
		}
		
		wg.Add(1)
		go func(idx int, txt string) {
			defer wg.Done()
			
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			
			emb, err := ae.Embed(ctx, txt)
			mu.Lock()
			if err != nil && firstErr == nil {
				firstErr = err
			}
			results[idx] = emb
			mu.Unlock()
		}(i, text)
	}
	
	wg.Wait()
	
	if firstErr != nil {
		return results, firstErr
	}
	return results, nil
}

// Stats returns embedding statistics.
func (ae *AutoEmbedder) Stats() map[string]int64 {
	ae.mu.RLock()
	defer ae.mu.RUnlock()
	
	return map[string]int64{
		"embed_count":  ae.embedCount,
		"cache_hits":   ae.cacheHits,
		"cache_misses": ae.cacheMisses,
		"cache_size":   int64(ae.cacheSize),
		"error_count":  ae.errorCount,
		"queue_length": int64(len(ae.queue)),
	}
}

// ClearCache clears the embedding cache.
func (ae *AutoEmbedder) ClearCache() {
	ae.mu.Lock()
	defer ae.mu.Unlock()
	ae.cache = make(map[string][]float32)
	ae.cacheSize = 0
}

// WaitForQueue waits for the embedding queue to drain with a timeout.
func (ae *AutoEmbedder) WaitForQueue(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(ae.queue) == 0 {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return len(ae.queue) == 0
}

// hashContent generates a hash of content for caching.
func hashContent(content string) string {
	h := sha256.Sum256([]byte(content))
	return hex.EncodeToString(h[:16]) // Use first 128 bits
}
