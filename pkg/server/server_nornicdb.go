package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sort"

	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// =============================================================================
// NornicDB-Specific Handlers (Memory OS for LLMs)
// =============================================================================

// Search Handlers
// =============================================================================

// handleDecay returns memory decay information (NornicDB-specific)
func (s *Server) handleDecay(w http.ResponseWriter, r *http.Request) {
	info := s.db.GetDecayInfo()

	response := map[string]interface{}{
		"enabled":          info.Enabled,
		"archiveThreshold": info.ArchiveThreshold,
		"interval":         info.RecalcInterval.String(),
		"weights": map[string]interface{}{
			"recency":    info.RecencyWeight,
			"frequency":  info.FrequencyWeight,
			"importance": info.ImportanceWeight,
		},
	}
	s.writeJSON(w, http.StatusOK, response)
}

// handleEmbedTrigger triggers the embedding worker to process nodes without embeddings.
// Query params:
//   - regenerate=true: Clear all existing embeddings first, then regenerate (async)
func (s *Server) handleEmbedTrigger(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeNeo4jError(w, http.StatusMethodNotAllowed, "Neo.ClientError.Request.Invalid", "POST required")
		return
	}

	stats := s.db.EmbedQueueStats()
	if stats == nil {
		s.writeNeo4jError(w, http.StatusServiceUnavailable, "Neo.DatabaseError.General.UnknownError", "Auto-embed not enabled")
		return
	}

	// Check if regenerate=true to clear existing embeddings first
	regenerate := r.URL.Query().Get("regenerate") == "true"

	if regenerate {
		// Return 202 Accepted immediately - clearing happens in background
		response := map[string]interface{}{
			"accepted":   true,
			"regenerate": true,
			"message":    "Regeneration started - clearing embeddings and regenerating in background. Check /nornicdb/embed/stats for progress.",
		}
		s.writeJSON(w, http.StatusAccepted, response)

		// Start background clearing and regeneration
		go func() {
			log.Printf("[EMBED] Starting background regeneration - stopping worker and clearing embeddings...")

			// First, reset the embed worker to stop any in-progress work and clear its state
			if err := s.db.ResetEmbedWorker(); err != nil {
				log.Printf("[EMBED] ⚠️ Failed to reset embed worker: %v", err)
			}

			// Now clear all embeddings
			cleared, err := s.db.ClearAllEmbeddings()
			if err != nil {
				log.Printf("[EMBED] ❌ Failed to clear embeddings: %v", err)
				return
			}
			log.Printf("[EMBED] ✅ Cleared %d embeddings - triggering regeneration", cleared)

			// Trigger embedding worker to regenerate (worker was already restarted by Reset)
			ctx := context.Background()
			if _, err := s.db.EmbedExisting(ctx); err != nil {
				log.Printf("[EMBED] ❌ Failed to trigger embedding worker: %v", err)
				return
			}
			log.Printf("[EMBED] 🚀 Embedding worker triggered for regeneration")
		}()
		return
	}

	// Non-regenerate case: just trigger the worker (fast, synchronous is fine)
	wasRunning := stats.Running

	// Trigger (safe to call even if already running - just wakes up worker)
	_, err := s.db.EmbedExisting(r.Context())
	if err != nil {
		s.writeNeo4jError(w, http.StatusInternalServerError, "Neo.DatabaseError.General.UnknownError", err.Error())
		return
	}

	// Get updated stats
	stats = s.db.EmbedQueueStats()

	var message string
	if wasRunning {
		message = "Embedding worker already running - will continue processing"
	} else {
		message = "Embedding worker triggered - processing nodes in background"
	}

	response := map[string]interface{}{
		"triggered":      true,
		"regenerate":     false,
		"already_active": wasRunning,
		"message":        message,
		"stats":          stats,
	}
	s.writeJSON(w, http.StatusOK, response)
}

// handleEmbedStats returns embedding worker statistics.
func (s *Server) handleEmbedStats(w http.ResponseWriter, r *http.Request) {
	stats := s.db.EmbedQueueStats()
	totalEmbeddings := s.db.EmbeddingCount()
	vectorIndexDims := s.db.VectorIndexDimensions()

	if stats == nil {
		response := map[string]interface{}{
			"enabled":                 false,
			"message":                 "Auto-embed not enabled",
			"total_embeddings":        totalEmbeddings,
			"configured_model":        s.config.EmbeddingModel,
			"configured_dimensions":   s.config.EmbeddingDimensions,
			"configured_provider":     s.config.EmbeddingProvider,
			"vector_index_dimensions": vectorIndexDims,
		}
		s.writeJSON(w, http.StatusOK, response)
		return
	}
	response := map[string]interface{}{
		"enabled":                 true,
		"stats":                   stats,
		"total_embeddings":        totalEmbeddings,
		"configured_model":        s.config.EmbeddingModel,
		"configured_dimensions":   s.config.EmbeddingDimensions,
		"configured_provider":     s.config.EmbeddingProvider,
		"vector_index_dimensions": vectorIndexDims,
	}
	s.writeJSON(w, http.StatusOK, response)
}

// handleEmbedClear clears all embeddings from nodes (admin only).
// This allows regeneration with a new model or fixing corrupted embeddings.
func (s *Server) handleEmbedClear(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		s.writeNeo4jError(w, http.StatusMethodNotAllowed, "Neo.ClientError.Request.Invalid", "POST or DELETE required")
		return
	}

	cleared, err := s.db.ClearAllEmbeddings()
	if err != nil {
		s.writeNeo4jError(w, http.StatusInternalServerError, "Neo.DatabaseError.General.UnknownError", err.Error())
		return
	}

	response := map[string]interface{}{
		"success": true,
		"cleared": cleared,
		"message": fmt.Sprintf("Cleared embeddings from %d nodes - use /nornicdb/embed/trigger to regenerate", cleared),
	}
	s.writeJSON(w, http.StatusOK, response)
}

// handleSearchRebuild rebuilds search indexes from all nodes in the specified database.
func (s *Server) handleSearchRebuild(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeNeo4jError(w, http.StatusMethodNotAllowed, "Neo.ClientError.Request.Invalid", "POST required")
		return
	}

	var req struct {
		Database string `json:"database,omitempty"` // Optional: defaults to default database
	}

	if err := s.readJSON(r, &req); err != nil {
		// If no JSON body, use default database
		req.Database = ""
	}

	// Get database name (default to default database if not specified)
	dbName := req.Database
	if dbName == "" {
		dbName = s.dbManager.DefaultDatabaseName()
	}

	// Get namespaced storage for the specified database
	storageEngine, err := s.dbManager.GetStorage(dbName)
	if err != nil {
		s.writeError(w, http.StatusNotFound, fmt.Sprintf("Database '%s' not found", dbName), ErrNotFound)
		return
	}

	// Invalidate cache and rebuild from scratch
	s.db.ResetSearchService(dbName)

	searchSvc, err := s.db.GetOrCreateSearchService(dbName, storageEngine)
	if err != nil {
		s.writeNeo4jError(w, http.StatusInternalServerError, "Neo.DatabaseError.General.UnknownError", err.Error())
		return
	}

	if err := searchSvc.BuildIndexes(r.Context()); err != nil {
		s.writeNeo4jError(w, http.StatusInternalServerError, "Neo.DatabaseError.General.UnknownError", err.Error())
		return
	}

	response := map[string]interface{}{
		"success":  true,
		"database": dbName,
		"message":  fmt.Sprintf("Search indexes rebuilt for database '%s'", dbName),
	}
	s.writeJSON(w, http.StatusOK, response)
}

// getOrCreateSearchService returns a cached search service for the database,
// creating and caching it if it doesn't exist. Search services are namespace-aware
// because they're built from NamespacedEngine which automatically filters nodes.
func (s *Server) getOrCreateSearchService(dbName string, storageEngine storage.Engine) (*search.Service, error) {
	return s.db.GetOrCreateSearchService(dbName, storageEngine)
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "POST required", ErrMethodNotAllowed)
		return
	}

	var req struct {
		Database string   `json:"database,omitempty"` // Optional: defaults to default database
		Query    string   `json:"query"`
		Labels   []string `json:"labels,omitempty"`
		Limit    int      `json:"limit,omitempty"`
	}

	if err := s.readJSON(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body", ErrBadRequest)
		return
	}

	if req.Limit <= 0 {
		req.Limit = 10
	}

	// Get database name (default to default database if not specified)
	dbName := req.Database
	if dbName == "" {
		dbName = s.dbManager.DefaultDatabaseName()
	}

	// Get namespaced storage for the specified database
	storageEngine, err := s.dbManager.GetStorage(dbName)
	if err != nil {
		s.writeError(w, http.StatusNotFound, fmt.Sprintf("Database '%s' not found", dbName), ErrNotFound)
		return
	}

	// Try to generate embedding for hybrid search
	queryEmbedding, embedErr := s.db.EmbedQuery(r.Context(), req.Query)
	if embedErr != nil {
		log.Printf("⚠️ Query embedding failed: %v", embedErr)
	}

	// Get or create search service for this database
	// Search services are cached per database since indexes are namespace-aware
	// (the storage engine already filters to the correct namespace)
	ctx := r.Context()
	searchSvc, err := s.getOrCreateSearchService(dbName, storageEngine)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error(), ErrInternalError)
		return
	}

	// Build indexes if needed (only on first use or after rebuild)
	if searchSvc.EmbeddingCount() == 0 {
		// Indexes are empty, build them from existing nodes
		if err := searchSvc.BuildIndexes(ctx); err != nil {
			log.Printf("⚠️ Failed to build search indexes: %v", err)
			// Continue anyway - search will work with whatever is indexed
		}
	}

	opts := search.DefaultSearchOptions()
	opts.Limit = req.Limit
	if len(req.Labels) > 0 {
		opts.Types = req.Labels
	}

	var searchResponse *search.SearchResponse
	if queryEmbedding != nil {
		// Use hybrid search (vector + text)
		searchResponse, err = searchSvc.Search(ctx, req.Query, queryEmbedding, opts)
	} else {
		// Fall back to text-only search
		searchResponse, err = searchSvc.Search(ctx, req.Query, nil, opts)
	}

	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error(), ErrInternalError)
		return
	}

	// Convert search results to our format
	// SearchResult.NodeID is already unprefixed (NamespacedEngine.GetNode() unprefixes automatically)
	results := make([]*nornicdb.SearchResult, len(searchResponse.Results))
	for i, r := range searchResponse.Results {
		results[i] = &nornicdb.SearchResult{
			Node: &nornicdb.Node{
				ID:         string(r.NodeID),
				Labels:     r.Labels,
				Properties: r.Properties,
			},
			Score:      r.Score,
			RRFScore:   r.RRFScore,
			VectorRank: r.VectorRank,
			BM25Rank:   r.BM25Rank,
		}
	}

	s.writeJSON(w, http.StatusOK, results)
}

func (s *Server) handleSimilar(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "POST required", ErrMethodNotAllowed)
		return
	}

	var req struct {
		Database string `json:"database,omitempty"` // Optional: defaults to default database
		NodeID   string `json:"node_id"`
		Limit    int    `json:"limit,omitempty"`
	}

	if err := s.readJSON(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body", ErrBadRequest)
		return
	}

	if req.Limit <= 0 {
		req.Limit = 10
	}

	// Get database name (default to default database if not specified)
	dbName := req.Database
	if dbName == "" {
		dbName = s.dbManager.DefaultDatabaseName()
	}

	// Get namespaced storage for the specified database
	storageEngine, err := s.dbManager.GetStorage(dbName)
	if err != nil {
		s.writeError(w, http.StatusNotFound, fmt.Sprintf("Database '%s' not found", dbName), ErrNotFound)
		return
	}

	// Get the target node from namespaced storage
	targetNode, err := storageEngine.GetNode(storage.NodeID(req.NodeID))
	if err != nil {
		s.writeError(w, http.StatusNotFound, fmt.Sprintf("Node '%s' not found", req.NodeID), ErrNotFound)
		return
	}

	if len(targetNode.ChunkEmbeddings) == 0 || len(targetNode.ChunkEmbeddings[0]) == 0 {
		s.writeError(w, http.StatusBadRequest, "Node has no embedding", ErrBadRequest)
		return
	}

	// Find similar nodes using vector similarity search
	type scored struct {
		node  *storage.Node
		score float64
	}
	var results []scored

	ctx := r.Context()
	err = storage.StreamNodesWithFallback(ctx, storageEngine, 1000, func(n *storage.Node) error {
		// Skip self and nodes without embeddings
		if string(n.ID) == req.NodeID || len(n.ChunkEmbeddings) == 0 || len(n.ChunkEmbeddings[0]) == 0 {
			return nil
		}

		// Use first chunk embedding for similarity (always stored in ChunkEmbeddings, even single chunk = array of 1)
		var targetEmb, nEmb []float32
		if len(targetNode.ChunkEmbeddings) > 0 && len(targetNode.ChunkEmbeddings[0]) > 0 {
			targetEmb = targetNode.ChunkEmbeddings[0]
		}
		if len(n.ChunkEmbeddings) > 0 && len(n.ChunkEmbeddings[0]) > 0 {
			nEmb = n.ChunkEmbeddings[0]
		}
		sim := vector.CosineSimilarity(targetEmb, nEmb)

		// Maintain top-k results
		if len(results) < req.Limit {
			results = append(results, scored{node: n, score: sim})
			if len(results) == req.Limit {
				sort.Slice(results, func(i, j int) bool {
					return results[i].score > results[j].score
				})
			}
		} else if sim > results[req.Limit-1].score {
			results[req.Limit-1] = scored{node: n, score: sim}
			sort.Slice(results, func(i, j int) bool {
				return results[i].score > results[j].score
			})
		}
		return nil
	})

	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error(), ErrInternalError)
		return
	}

	// Final sort
	sort.Slice(results, func(i, j int) bool {
		return results[i].score > results[j].score
	})

	// Convert to response format (node IDs are already unprefixed from NamespacedEngine)
	searchResults := make([]*nornicdb.SearchResult, len(results))
	for i, r := range results {
		searchResults[i] = &nornicdb.SearchResult{
			Node: &nornicdb.Node{
				ID:         string(r.node.ID),
				Labels:     r.node.Labels,
				Properties: r.node.Properties,
				CreatedAt:  r.node.CreatedAt,
			},
			Score: r.score,
		}
	}

	s.writeJSON(w, http.StatusOK, searchResults)
}
