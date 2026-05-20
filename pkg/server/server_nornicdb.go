package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

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
		"enabled":             info.Enabled,
		"visibilityThreshold": info.VisibilityThreshold,
		"flushInterval":       info.FlushInterval.String(),
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

		// Start background clearing and regeneration. Derive a child logger
		// once at goroutine entry so every record carries subsystem=embed
		// (Phase 2 D-10a bracket-prefix → component-attribute rewrite).
		embedLog := s.log.With("subsystem", "embed")
		go func() {
			defer func() {
				if rec := recover(); rec != nil {
					// Background regeneration can race with DB shutdown in tests/teardown.
					// Never crash the process for this async maintenance path.
					embedLog.Warn("regeneration aborted during shutdown", "panic", rec)
				}
			}()

			embedLog.Info("starting background regeneration: stopping worker and clearing embeddings")

			// First, reset the embed worker to stop any in-progress work and clear its state
			if err := s.db.ResetEmbedWorker(); err != nil {
				embedLog.Warn("failed to reset embed worker", "error", err)
			}

			// Now clear all embeddings
			cleared, err := s.db.ClearAllEmbeddings()
			if err != nil {
				if errors.Is(err, nornicdb.ErrClosed) || strings.Contains(strings.ToLower(err.Error()), "closed") {
					embedLog.Info("regeneration skipped: database is closing")
					return
				}
				embedLog.Error("failed to clear embeddings", "error", err)
				return
			}
			embedLog.Info("cleared embeddings; triggering regeneration", "cleared", cleared)

			// Trigger embedding worker to regenerate (worker was already restarted by Reset)
			ctx := context.Background()
			if _, err := s.db.EmbedExisting(ctx); err != nil {
				embedLog.Error("failed to trigger embedding worker", "error", err)
				return
			}
			embedLog.Info("embedding worker triggered for regeneration")
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
	// Keep contended stats reads bounded. total_embeddings uses authoritative
	// aggregate count to avoid false zeros.
	readIntWithTimeout := func(timeout time.Duration, fallback int, fn func() int) int {
		done := make(chan int, 1)
		go func() {
			done <- fn()
		}()
		select {
		case v := <-done:
			return v
		case <-time.After(timeout):
			return fallback
		}
	}

	// Use authoritative aggregate count so we never report a false zero.
	// This may be a bit slower than cached-only reads, but correctness is required.
	totalEmbeddings := s.db.EmbeddingCount()
	vectorIndexDims := readIntWithTimeout(100*time.Millisecond, s.config.EmbeddingDimensions, func() int {
		return s.db.VectorIndexDimensionsCached()
	})
	pendingEmbeddings := readIntWithTimeout(100*time.Millisecond, -1, func() int {
		return s.db.PendingEmbeddingsCount()
	})

	if stats == nil {
		response := map[string]interface{}{
			"enabled":                 false,
			"message":                 "Auto-embed not enabled",
			"total_embeddings":        totalEmbeddings,
			"pending_nodes":           pendingEmbeddings,
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
		"pending_nodes":           pendingEmbeddings,
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

	// Per-database RBAC: deny if principal may not access this database (Neo4j-aligned).
	claims := getClaims(r)
	if !s.getDatabaseAccessMode(claims).CanAccessDatabase(dbName) {
		s.writeNeo4jError(w, http.StatusForbidden, "Neo.ClientError.Security.Forbidden",
			fmt.Sprintf("Access to database '%s' is not allowed.", dbName))
		return
	}
	if s.dbManager.IsCompositeDatabase(dbName) {
		s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.Statement.NotSupported",
			fmt.Sprintf("Search rebuild on composite database '%s' is not supported; target a constituent database explicitly.", dbName))
		return
	}
	// Rebuild is a write to the database; require ResolvedAccess.Write for this DB.
	if !s.getResolvedAccess(claims, dbName).Write {
		s.writeNeo4jError(w, http.StatusForbidden, "Neo.ClientError.Security.Forbidden",
			fmt.Sprintf("Write on database '%s' is not allowed.", dbName))
		return
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
	reqStart := time.Now()
	searchDiagEnabled := false
	if raw := os.Getenv("NORNICDB_SEARCH_DIAG_TIMINGS"); raw != "" {
		if b, err := strconv.ParseBool(raw); err == nil {
			searchDiagEnabled = b
		}
	}
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "POST required", ErrMethodNotAllowed)
		return
	}

	var req struct {
		Database string              `json:"database,omitempty"` // Optional: defaults to default database
		Query    string              `json:"query"`
		Labels   []string            `json:"labels,omitempty"`
		Limit    int                 `json:"limit,omitempty"`
		Filters  map[string][]string `json:"filters,omitempty"`
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
	s.log.Info("search request", "subsystem", "search", "db", dbName, "query", req.Query)
	if dbName == "translations" {
		searchDiagEnabled = true
	}

	var (
		serviceLookupDur   time.Duration
		embedTotalDur      time.Duration
		searchExecDur      time.Duration
		chunkLoopDur       time.Duration
		embedCalls         int
		embedSuccessCalls  int
		searchCalls        int
		fallbackBM25Calls  int
		vectorChunkQueries int
	)

	// Per-database RBAC: deny if principal may not access this database (Neo4j-aligned).
	if !s.getDatabaseAccessMode(getClaims(r)).CanAccessDatabase(dbName) {
		s.writeNeo4jError(w, http.StatusForbidden, "Neo.ClientError.Security.Forbidden",
			fmt.Sprintf("Access to database '%s' is not allowed.", dbName))
		return
	}
	if s.dbManager.IsCompositeDatabase(dbName) {
		s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.Statement.NotSupported",
			fmt.Sprintf("Search on composite database '%s' is not supported; target a constituent database explicitly.", dbName))
		return
	}

	// Get namespaced storage for the specified database
	storageEngine, err := s.dbManager.GetStorage(dbName)
	if err != nil {
		s.log.Warn("search: storage lookup failed", "subsystem", "search", "db", dbName, "error", err)
		s.writeError(w, http.StatusNotFound, fmt.Sprintf("Database '%s' not found", dbName), ErrNotFound)
		return
	}

	// Per-DB master switches: only the "both off" case short-circuits the
	// handler — that's a configuration result, not a transient state.
	// Lazy-warming readers fall through to Service.Search which calls
	// EnsureWarm() and blocks until the build completes; that path is
	// shared by every search entry point (Bolt, GraphQL, gRPC, Cypher
	// procedures), not just HTTP.
	searchStatus := s.db.GetDatabaseSearchStatus(dbName)
	if !searchStatus.BM25Enabled && !searchStatus.VectorEnabled {
		s.writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"error":          "search is disabled for this database",
			"database":       dbName,
			"bm25_enabled":   false,
			"vector_enabled": false,
			"retryable":      false,
			"http_code":      http.StatusServiceUnavailable,
			"request_status": "search_disabled_for_database",
		})
		return
	}
	// "Still building" 503 — fires when an EAGER build is mid-flight at
	// the moment of the request. Lazy databases skip this branch because
	// LazyTriggerNeeded=true; the handler then falls through to
	// GetOrCreateSearchService + EnsureWarm which synchronously triggers
	// the build and blocks until ready. That preserves correctness for
	// the first lazy request: by the time we reach EmbeddingCount() and
	// the embedding decision, the in-memory ANN substrate is populated.
	if !searchStatus.Ready && !searchStatus.LazyTriggerNeeded {
		s.writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"error":          search.ErrSearchIndexBuilding.Error(),
			"database":       dbName,
			"bm25_enabled":   searchStatus.BM25Enabled,
			"vector_enabled": searchStatus.VectorEnabled,
			"search_status":  searchStatus,
			"retryable":      true,
			"http_code":      http.StatusServiceUnavailable,
			"request_status": "search_not_ready",
		})
		return
	}

	// Search service should already be initialized at startup once status is ready.
	ctx := r.Context()
	serviceLookupStart := time.Now()
	searchSvc, err := s.db.GetOrCreateSearchService(dbName, storageEngine)
	serviceLookupDur = time.Since(serviceLookupStart)
	if err != nil {
		s.writeError(w, http.StatusServiceUnavailable, "search service unavailable", ErrInternalError)
		return
	}
	// Lazy-warm: synchronously trigger and wait for the build BEFORE any
	// handler-side decisions that depend on search state (EmbeddingCount,
	// RerankerAvailable, ChunkQueryForDB, etc). Without this, a lazy DB's
	// first request would observe EmbeddingCount=0, skip query embedding,
	// and return BM25-only results — even though vector search is enabled
	// and embeddings exist. EnsureWarm is a fast no-op for already-warm
	// services, so this is free for the steady-state hot path.
	if err := searchSvc.EnsureWarm(ctx); err != nil {
		s.writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"error":          "search index warming did not complete: " + err.Error(),
			"database":       dbName,
			"bm25_enabled":   searchStatus.BM25Enabled,
			"vector_enabled": searchStatus.VectorEnabled,
			"retryable":      true,
			"http_code":      http.StatusServiceUnavailable,
			"request_status": "search_index_warming_failed",
		})
		return
	}

	// If embeddings are available, chunk long queries by length so vector search
	// remains usable for paragraph-sized inputs (no relying on tokenization errors).
	//
	// For short queries (1 chunk), we preserve the legacy behavior and return the
	// search service response directly.
	const (
		queryChunkSize    = 512
		queryChunkOverlap = 50
		maxQueryChunks    = 32
		outerRRFK         = 60
		embedTimeout      = 8 * time.Second
	)

	queryChunks, err := s.db.ChunkQueryForDB(ctx, dbName, req.Query)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "failed to chunk query", ErrBadRequest)
		return
	}
	if len(queryChunks) > maxQueryChunks {
		queryChunks = queryChunks[:maxQueryChunks]
	}

	var searchResponse *search.SearchResponse
	// Helper to build search options consistently.
	buildOpts := func(q string, limit int) *search.SearchOptions {
		opts := search.GetAdaptiveRRFConfig(q)
		opts.Limit = limit
		if len(req.Labels) > 0 {
			opts.Types = req.Labels
		}
		if len(req.Filters) > 0 {
			opts.Filters = req.Filters
		}
		opts.RerankEnabled = searchSvc.RerankerAvailable(ctx)
		return opts
	}
	// Bound embedding latency even if the embedder ignores context cancellation.
	embedQuery := func(parent context.Context, q string) ([]float32, error) {
		embedCalls++
		embedStart := time.Now()
		emb, embedErr := runEmbedWithTimeout(parent, embedTimeout, func(embedCtx context.Context) ([]float32, error) {
			return s.db.EmbedQueryForDB(embedCtx, dbName, q)
		})
		embedTotalDur += time.Since(embedStart)
		if embedErr == nil && len(emb) > 0 {
			embedSuccessCalls++
		}
		return emb, embedErr
	}
	vectorSearchUsable := searchSvc.EmbeddingCount() > 0

	if !vectorSearchUsable {
		// No indexed vectors for this database: skip query embedding entirely and
		// run fulltext directly to avoid paying embedding latency with guaranteed fallback.
		searchCalls++
		searchStart := time.Now()
		searchResponse, err = searchSvc.Search(ctx, req.Query, nil, buildOpts(req.Query, req.Limit))
		searchExecDur += time.Since(searchStart)
	} else if len(queryChunks) <= 1 {
		// Fast path: short query (single chunk). Try hybrid; fall back to BM25.
		// Use per-DB query embedding so vector dims match the index for this database.
		emb, embedErr := embedQuery(ctx, req.Query)
		if embedErr != nil {
			if errors.Is(embedErr, nornicdb.ErrQueryEmbeddingDimensionMismatch) {
				s.writeError(w, http.StatusBadRequest, embedErr.Error(), ErrBadRequest)
				return
			}
			s.log.Warn("query embedding failed", "subsystem", "search", "error", embedErr)
		}
		if len(emb) > 0 {
			searchCalls++
			searchStart := time.Now()
			searchResponse, err = searchSvc.Search(ctx, req.Query, emb, buildOpts(req.Query, req.Limit))
			searchExecDur += time.Since(searchStart)
		} else {
			searchCalls++
			searchStart := time.Now()
			searchResponse, err = searchSvc.Search(ctx, req.Query, nil, buildOpts(req.Query, req.Limit))
			searchExecDur += time.Since(searchStart)
		}
	} else {
		// Multi-chunk: embed/search each chunk, then fuse results across chunks using RRF.
		chunkLoopStart := time.Now()
		perChunkLimit := req.Limit
		if perChunkLimit < 10 {
			perChunkLimit = 10
		}
		if perChunkLimit < req.Limit*3 {
			perChunkLimit = req.Limit * 3
		}
		if perChunkLimit > 100 {
			perChunkLimit = 100
		}

		type fused struct {
			node       *nornicdb.Node
			scoreRRF   float64
			vectorRank int
			bm25Rank   int
		}
		fusedByID := make(map[string]*fused)

		var usedVectorChunks int
		for _, chunkQuery := range queryChunks {
			emb, embedErr := embedQuery(ctx, chunkQuery)
			if embedErr != nil {
				if errors.Is(embedErr, nornicdb.ErrQueryEmbeddingDimensionMismatch) {
					s.writeError(w, http.StatusBadRequest, embedErr.Error(), ErrBadRequest)
					return
				}
				s.log.Warn("query embedding failed (chunked)", "subsystem", "search", "error", embedErr)
				continue
			}
			if len(emb) == 0 {
				continue
			}
			usedVectorChunks++
			vectorChunkQueries++

			searchCalls++
			searchStart := time.Now()
			resp, searchErr := searchSvc.Search(ctx, chunkQuery, emb, buildOpts(chunkQuery, perChunkLimit))
			searchExecDur += time.Since(searchStart)
			if searchErr != nil {
				if errors.Is(searchErr, search.ErrSearchIndexBuilding) {
					err = searchErr
					break
				}
				continue
			}
			if resp == nil {
				continue
			}

			for rank := range resp.Results {
				r := resp.Results[rank]
				id := string(r.NodeID) // already unprefixed by namespaced storage/search layer
				f := fusedByID[id]
				if f == nil {
					f = &fused{
						node: &nornicdb.Node{
							ID:         id,
							Labels:     r.Labels,
							Properties: r.Properties,
						},
						scoreRRF:   0,
						vectorRank: r.VectorRank,
						bm25Rank:   r.BM25Rank,
					}
					fusedByID[id] = f
				}

				// Outer RRF: 1/(k + rank), rank is 1-based.
				f.scoreRRF += 1.0 / (outerRRFK + float64(rank+1))
			}
		}

		if err == nil {
			if usedVectorChunks == 0 || len(fusedByID) == 0 {
				// Embeddings not available (or all failed): fall back to BM25.
				fallbackBM25Calls++
				searchCalls++
				searchStart := time.Now()
				searchResponse, err = searchSvc.Search(ctx, req.Query, nil, buildOpts(req.Query, req.Limit))
				searchExecDur += time.Since(searchStart)
			} else {
				// Materialize fused response.
				fusedList := make([]*fused, 0, len(fusedByID))
				for _, f := range fusedByID {
					fusedList = append(fusedList, f)
				}
				sort.Slice(fusedList, func(i, j int) bool {
					return fusedList[i].scoreRRF > fusedList[j].scoreRRF
				})
				if len(fusedList) > req.Limit {
					fusedList = fusedList[:req.Limit]
				}

				// Build a SearchResponse-like structure to reuse existing conversion code.
				searchResponse = &search.SearchResponse{
					SearchMethod:      "chunked_rrf_hybrid",
					FallbackTriggered: false,
					Results:           make([]search.SearchResult, 0, len(fusedList)),
				}
				for _, f := range fusedList {
					// Preserve vector_rank and bm25_rank from the first chunk where the node appeared.
					searchResponse.Results = append(searchResponse.Results, search.SearchResult{
						ID:         f.node.ID,
						NodeID:     storage.NodeID(f.node.ID),
						Labels:     f.node.Labels,
						Properties: f.node.Properties,
						Score:      f.scoreRRF,
						RRFScore:   f.scoreRRF,
						VectorRank: f.vectorRank,
						BM25Rank:   f.bm25Rank,
					})
				}
			}
		}
		chunkLoopDur = time.Since(chunkLoopStart)
	}

	if err != nil {
		if searchDiagEnabled {
			s.log.Info("search timing",
				"subsystem", "search",
				"status", "error",
				"db", dbName,
				"total", time.Since(reqStart),
				"svc_lookup", serviceLookupDur,
				"embed_total", embedTotalDur,
				"embed_calls", embedCalls,
				"embed_ok", embedSuccessCalls,
				"search_total", searchExecDur,
				"search_calls", searchCalls,
				"chunks", len(queryChunks),
				"vector_chunks", vectorChunkQueries,
				"chunk_loop", chunkLoopDur,
				"fallback_bm25", fallbackBM25Calls,
				"error", err,
			)
		}
		if errors.Is(err, search.ErrSearchIndexBuilding) {
			s.writeError(w, http.StatusServiceUnavailable, err.Error(), ErrServiceUnavailable)
			return
		}
		s.writeError(w, http.StatusInternalServerError, err.Error(), ErrInternalError)
		return
	}

	// Canonical mapping keeps DB and server adapters consistent.
	results := nornicdb.MapSearchResponse(searchResponse)
	if searchDiagEnabled {
		s.log.Info("search timing",
			"subsystem", "search",
			"status", "ok",
			"db", dbName,
			"total", time.Since(reqStart),
			"svc_lookup", serviceLookupDur,
			"embed_total", embedTotalDur,
			"embed_calls", embedCalls,
			"embed_ok", embedSuccessCalls,
			"search_total", searchExecDur,
			"search_calls", searchCalls,
			"chunks", len(queryChunks),
			"vector_chunks", vectorChunkQueries,
			"chunk_loop", chunkLoopDur,
			"fallback_bm25", fallbackBM25Calls,
			"search_method", searchResponse.SearchMethod,
			"fallback", searchResponse.FallbackTriggered,
			"results", len(searchResponse.Results),
		)
	}

	s.writeJSON(w, http.StatusOK, results)
}

func runEmbedWithTimeout(parent context.Context, timeout time.Duration, fn func(context.Context) ([]float32, error)) ([]float32, error) {
	if parent == nil {
		parent = context.Background()
	}
	embedCtx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	emb, err := fn(embedCtx)
	if errors.Is(err, context.DeadlineExceeded) || embedCtx.Err() == context.DeadlineExceeded {
		return nil, context.DeadlineExceeded
	}
	return emb, err
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

	// Per-database RBAC: deny if principal may not access this database (Neo4j-aligned).
	if !s.getDatabaseAccessMode(getClaims(r)).CanAccessDatabase(dbName) {
		s.writeNeo4jError(w, http.StatusForbidden, "Neo.ClientError.Security.Forbidden",
			fmt.Sprintf("Access to database '%s' is not allowed.", dbName))
		return
	}
	if s.dbManager.IsCompositeDatabase(dbName) {
		s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.Statement.NotSupported",
			fmt.Sprintf("Vector similarity on composite database '%s' is not supported; target a constituent database explicitly.", dbName))
		return
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
