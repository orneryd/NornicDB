package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/resultstream"
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
		s.writeNeo4jPostRequired(w, r, "Neo.ClientError.Request.Invalid")
		return
	}

	stats := s.db.EmbedQueueStats()
	if stats == nil {
		s.writeNeo4jAutoEmbedNotEnabled(w, r)
		return
	}

	// Check if regenerate=true to clear existing embeddings first
	regenerate := r.URL.Query().Get("regenerate") == "true"

	if regenerate {
		// Return 202 Accepted immediately - clearing happens in background
		response := map[string]interface{}{
			"accepted":   true,
			"regenerate": true,
			"message":    s.localizedText(w, r, localization.EmbeddingRegenerationStarted()),
		}
		s.writeJSON(w, http.StatusAccepted, response)

		// Start background clearing and regeneration.
		go func() {
			defer func() {
				if rec := recover(); rec != nil {
					// Background regeneration can race with DB shutdown in tests/teardown.
					// Never crash the process for this async maintenance path.
					s.logEvent(context.Background(), slog.LevelWarn, localization.ServerEmbedRegenerationAbortedEvent(rec))
				}
			}()

			s.logEvent(context.Background(), slog.LevelInfo, localization.ServerEmbedRegenerationStartingEvent())

			// First, reset the embed worker to stop any in-progress work and clear its state
			if err := s.db.ResetEmbedWorker(); err != nil {
				s.logEvent(context.Background(), slog.LevelWarn, localization.ServerEmbedWorkerResetFailedEvent(err))
			}

			// Now clear all embeddings
			cleared, err := s.db.ClearAllEmbeddings()
			if err != nil {
				if errors.Is(err, nornicdb.ErrClosed) || strings.Contains(strings.ToLower(err.Error()), "closed") {
					s.logEvent(context.Background(), slog.LevelInfo, localization.ServerEmbedRegenerationSkippedDBClosingEvent())
					return
				}
				s.logEvent(context.Background(), slog.LevelError, localization.ServerEmbedClearFailedEvent(err))
				return
			}
			s.logEvent(context.Background(), slog.LevelInfo, localization.ServerEmbedClearedEvent(cleared))

			// Trigger embedding worker to regenerate (worker was already restarted by Reset)
			ctx := context.Background()
			if _, err := s.db.EmbedExisting(ctx); err != nil {
				s.logEvent(ctx, slog.LevelError, localization.ServerEmbedWorkerTriggerFailedEvent(err))
				return
			}
			s.logEvent(ctx, slog.LevelInfo, localization.ServerEmbedWorkerTriggeredEvent())
		}()
		return
	}

	// Non-regenerate case: just trigger the worker (fast, synchronous is fine)
	wasRunning := stats.Running

	// Trigger (safe to call even if already running - just wakes up worker)
	_, err := s.db.EmbedExisting(r.Context())
	if err != nil {
		s.writeBoundaryNeo4jError(w, r, http.StatusInternalServerError, "Neo.DatabaseError.General.UnknownError", err)
		return
	}

	// Get updated stats
	stats = s.db.EmbedQueueStats()

	var message localization.Message
	if wasRunning {
		message = localization.EmbeddingWorkerAlreadyRunning()
	} else {
		message = localization.EmbeddingWorkerTriggered()
	}

	response := map[string]interface{}{
		"triggered":      true,
		"regenerate":     false,
		"already_active": wasRunning,
		"message":        s.localizedText(w, r, message),
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
		s.writeNeo4jPostOrDeleteRequired(w, r)
		return
	}

	cleared, err := s.db.ClearAllEmbeddings()
	if err != nil {
		s.writeBoundaryNeo4jError(w, r, http.StatusInternalServerError, "Neo.DatabaseError.General.UnknownError", err)
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
		s.writeNeo4jPostRequired(w, r, "Neo.ClientError.Request.Invalid")
		return
	}

	var req struct {
		Database string `json:"database,omitempty"` // Optional: defaults to default database
	}

	if err := s.readJSON(r, &req); err != nil {
		// If no JSON body, use default database
		req.Database = ""
	}

	dbName := req.Database
	if dbName == "" {
		dbName = s.dbManager.DefaultDatabaseName()
	}

	// Per-database RBAC: deny if principal may not access this database (Neo4j-aligned).
	claims := getClaims(r)
	if !s.getDatabaseAccessMode(claims).CanAccessDatabase(dbName) {
		s.writeNeo4jDatabaseAccessDenied(w, r, dbName)
		return
	}
	if s.dbManager.IsCompositeDatabase(dbName) {
		s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.Statement.NotSupported",
			fmt.Sprintf("Search rebuild on composite database '%s' is not supported; target a constituent database explicitly.", dbName))
		return
	}
	// Rebuild is a write to the database; require ResolvedAccess.Write for this DB.
	if !s.getResolvedAccess(claims, dbName).Write {
		s.writeNeo4jDatabaseWriteDenied(w, r, dbName)
		return
	}

	// Get namespaced storage for the specified database
	storageEngine, err := s.dbManager.GetStorage(dbName)
	if err != nil {
		s.writeDatabaseNotFound(w, r, http.StatusNotFound, ErrNotFound, dbName)
		return
	}

	// Invalidate cache and rebuild from scratch
	s.db.ResetSearchService(dbName)

	searchSvc, err := s.db.GetOrCreateSearchService(dbName, storageEngine)
	if err != nil {
		s.writeBoundaryNeo4jError(w, r, http.StatusInternalServerError, "Neo.DatabaseError.General.UnknownError", err)
		return
	}

	if err := searchSvc.BuildIndexes(r.Context()); err != nil {
		s.writeBoundaryNeo4jError(w, r, http.StatusInternalServerError, "Neo.DatabaseError.General.UnknownError", err)
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
		s.writePostRequired(w, r)
		return
	}

	var req struct {
		Database    string              `json:"database,omitempty"` // Optional: defaults to default database
		Query       string              `json:"query"`
		Labels      []string            `json:"labels,omitempty"`
		Limit       int                 `json:"limit,omitempty"`
		Filters     map[string][]string `json:"filters,omitempty"`
		QID         string              `json:"qid,omitempty"`
		N           int                 `json:"n,omitempty"`
		Discard     bool                `json:"discard,omitempty"`
		MaxResults  int                 `json:"max_results,omitempty"`
		Mode        string              `json:"mode,omitempty"`
		GroupBy     string              `json:"group_by,omitempty"`
		RankedLimit int                 `json:"ranked_limit,omitempty"`
	}

	if err := s.readJSON(r, &req); err != nil {
		s.writeInvalidRequestBody(w, r)
		return
	}

	if req.Limit <= 0 {
		req.Limit = 10
	}
	if req.QID != "" && req.N == 0 {
		req.N = 50
	}

	owner := transactionOwnerKey(r, getClaims(r))
	// A qid selects its signed database binding. An explicit database is only
	// accepted when it matches that binding.
	dbName := req.Database
	if req.QID != "" {
		var err error
		dbName, err = s.db.ResolveSearchContinuationDatabase(owner, req.QID, dbName)
		if err != nil {
			s.writeSearchContinuationError(w, r, err)
			return
		}
	} else if dbName == "" {
		dbName = s.dbManager.DefaultDatabaseName()
	}
	s.logEvent(r.Context(), slog.LevelInfo, localization.ServerSearchRequestEvent(dbName, req.Query))
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
		s.writeNeo4jDatabaseAccessDenied(w, r, dbName)
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
		s.logEvent(r.Context(), slog.LevelWarn, localization.ServerSearchStorageLookupFailedEvent(dbName, err))
		s.writeDatabaseNotFound(w, r, http.StatusNotFound, ErrNotFound, dbName)
		return
	}

	// Per-DB master switches: only the "both off" case short-circuits the
	// handler — that's a configuration result, not a transient state.
	// Lazy-warming readers fall through to Service.Search which calls
	// EnsureWarm() and blocks until the build completes; that path is
	// shared by every search entry point (Bolt, GraphQL, gRPC, Cypher
	// procedures), not just HTTP.
	mode := search.SearchContinuationMode(req.Mode)
	idStart := req.QID == "" && mode == search.SearchContinuationID
	continuationResume := req.QID != ""
	searchStatus := s.db.GetDatabaseSearchStatus(dbName)
	if !continuationResume && !idStart && !searchStatus.BM25Enabled && !searchStatus.VectorEnabled {
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
	if !continuationResume && !idStart && !searchStatus.Ready && !searchStatus.LazyTriggerNeeded {
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
		s.writeSearchServiceUnavailable(w, r)
		return
	}
	// Lazy-warm: synchronously trigger and wait for the build BEFORE any
	// handler-side decisions that depend on search state (EmbeddingCount,
	// RerankerAvailable, ChunkQueryForDB, etc). Without this, a lazy DB's
	// first request would observe EmbeddingCount=0, skip query embedding,
	// and return BM25-only results — even though vector search is enabled
	// and embeddings exist. EnsureWarm is a fast no-op for already-warm
	// services, so this is free for the steady-state hot path.
	if !continuationResume && !idStart {
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
	}

	continuationRequested := req.N != 0 || req.QID != "" || req.Discard || req.Mode != "" || req.GroupBy != "" || req.RankedLimit != 0
	continuationRequest := search.SearchContinuationRequest{
		Owner:       owner,
		Database:    dbName,
		QID:         req.QID,
		N:           req.N,
		Discard:     req.Discard,
		MaxResults:  req.MaxResults,
		Mode:        mode,
		GroupBy:     req.GroupBy,
		RankedLimit: req.RankedLimit,
	}
	if req.QID != "" {
		page, continuationErr := searchSvc.SearchTextContinuation(ctx, "", nil, continuationRequest, nil, nil, nil, search.ChunkedSearchErrorPolicy{})
		if continuationErr != nil {
			s.writeSearchContinuationError(w, r, continuationErr)
			return
		}
		s.writeSearchContinuationPage(w, page)
		return
	}

	const embedTimeout = 8 * time.Second

	queryChunks := []string(nil)
	if !idStart {
		queryChunks, err = s.db.ChunkQueryForDB(ctx, dbName, req.Query)
		if err != nil {
			s.writeQueryChunkingFailed(w, r)
			return
		}
	}

	opts := search.GetAdaptiveRRFConfig(req.Query)
	opts.Limit = req.Limit
	if len(req.Labels) > 0 {
		opts.Types = req.Labels
	}
	if len(req.Filters) > 0 {
		opts.Filters = req.Filters
	}
	opts.RerankEnabled = searchSvc.RerankerAvailable(ctx)

	var embedQuery search.EmbedQueryFunc
	if searchSvc.EmbeddingCount() > 0 {
		embedQuery = func(parent context.Context, query string) ([]float32, error) {
			embedCalls++
			embedStart := time.Now()
			embedding, embedErr := runEmbedWithTimeout(parent, embedTimeout, func(embedCtx context.Context) ([]float32, error) {
				return s.db.EmbedQueryForDB(embedCtx, dbName, query)
			})
			embedTotalDur += time.Since(embedStart)
			if embedErr == nil && len(embedding) > 0 {
				embedSuccessCalls++
			}
			return embedding, embedErr
		}
	}

	searchQuery := func(searchCtx context.Context, query string, embedding []float32, searchOpts *search.SearchOptions) (*search.SearchResponse, error) {
		searchCalls++
		if len(embedding) > 0 {
			vectorChunkQueries++
		} else {
			fallbackBM25Calls++
		}
		searchStart := time.Now()
		response, searchErr := searchSvc.Search(searchCtx, query, embedding, searchOpts)
		searchExecDur += time.Since(searchStart)
		return response, searchErr
	}

	chunkLoopStart := time.Now()
	errorPolicy := search.ChunkedSearchErrorPolicy{
		Transport: "http",
		FatalEmbeddingError: func(err error) bool {
			return errors.Is(err, nornicdb.ErrQueryEmbeddingDimensionMismatch)
		},
		FatalSearchError: func(err error) bool {
			return errors.Is(err, search.ErrSearchIndexBuilding)
		},
	}
	var searchResponse *search.SearchResponse
	var continuationPage *search.SearchContinuationPage
	if continuationRequested {
		continuationPage, err = searchSvc.SearchTextContinuation(
			ctx,
			req.Query,
			opts,
			continuationRequest,
			func(context.Context, string) ([]string, error) { return queryChunks, nil },
			embedQuery,
			searchQuery,
			errorPolicy,
		)
		if continuationPage != nil {
			searchResponse = continuationPage.SearchResponse()
		}
	} else {
		searchResponse, err = search.SearchTextChunksWithErrorPolicy(
			ctx,
			req.Query,
			opts,
			func(context.Context, string) ([]string, error) { return queryChunks, nil },
			embedQuery,
			searchQuery,
			errorPolicy,
		)
	}
	chunkLoopDur = time.Since(chunkLoopStart)
	if errors.Is(err, nornicdb.ErrQueryEmbeddingDimensionMismatch) {
		s.writeBoundaryError(w, r, http.StatusBadRequest, err, ErrBadRequest)
		return
	}

	if err != nil {
		if searchDiagEnabled {
			s.logEvent(ctx, slog.LevelInfo, localization.ServerSearchTimingEvent(localization.ServerSearchTimingFields{
				Status:        "error",
				Database:      dbName,
				Total:         time.Since(reqStart),
				ServiceLookup: serviceLookupDur,
				EmbedTotal:    embedTotalDur,
				EmbedCalls:    embedCalls,
				EmbedOK:       embedSuccessCalls,
				SearchTotal:   searchExecDur,
				SearchCalls:   searchCalls,
				Chunks:        len(queryChunks),
				VectorChunks:  vectorChunkQueries,
				ChunkLoop:     chunkLoopDur,
				FallbackBM25:  fallbackBM25Calls,
				Error:         err,
			}))
		}
		if errors.Is(err, search.ErrSearchIndexBuilding) {
			s.writeBoundaryError(w, r, http.StatusServiceUnavailable, err, ErrServiceUnavailable)
			return
		}
		if continuationRequested {
			s.writeSearchContinuationError(w, r, err)
			return
		}
		s.writeBoundaryError(w, r, http.StatusInternalServerError, err, ErrInternalError)
		return
	}

	// Canonical mapping keeps DB and server adapters consistent.
	results := nornicdb.MapSearchResponse(searchResponse)
	if continuationPage != nil {
		setSearchFallbackReasonHeader(w, continuationPage.FallbackReason)
		s.writeJSON(w, http.StatusOK, map[string]any{
			"results":               results,
			"qid":                   continuationPage.QID,
			"has_more":              continuationPage.HasMore,
			"position":              continuationPage.Position,
			"returned":              continuationPage.Returned,
			"discovered":            continuationPage.Discovered,
			"total":                 continuationPage.Total,
			"expires_at":            continuationPage.ExpiresAt,
			"search_method":         continuationPage.SearchMethod,
			"fallback_triggered":    continuationPage.FallbackTriggered,
			"fallback_reason":       continuationPage.FallbackReason,
			"mode":                  continuationPage.Mode,
			"ranked_count":          continuationPage.RankedCount,
			"eligible_count":        continuationPage.EligibleCount,
			"ranked_pool_exhausted": continuationPage.RankedPoolExhausted,
			"collection_exhausted":  continuationPage.CollectionExhausted,
			"completion":            continuationPage.Completion,
		})
		return
	}
	setSearchFallbackReasonHeader(w, searchResponse.FallbackReason)
	if searchDiagEnabled {
		s.logEvent(ctx, slog.LevelInfo, localization.ServerSearchTimingEvent(localization.ServerSearchTimingFields{
			Status:        "ok",
			Database:      dbName,
			Total:         time.Since(reqStart),
			ServiceLookup: serviceLookupDur,
			EmbedTotal:    embedTotalDur,
			EmbedCalls:    embedCalls,
			EmbedOK:       embedSuccessCalls,
			SearchTotal:   searchExecDur,
			SearchCalls:   searchCalls,
			Chunks:        len(queryChunks),
			VectorChunks:  vectorChunkQueries,
			ChunkLoop:     chunkLoopDur,
			FallbackBM25:  fallbackBM25Calls,
			SearchMethod:  searchResponse.SearchMethod,
			Fallback:      searchResponse.FallbackTriggered,
			Results:       len(searchResponse.Results),
		}))
	}

	s.writeJSON(w, http.StatusOK, results)
}

func (s *Server) writeSearchContinuationError(w http.ResponseWriter, _ *http.Request, err error) {
	status := http.StatusBadRequest
	retryable := false
	requestStatus := "continuation_invalid"
	switch {
	case errors.Is(err, resultstream.ErrDisabled):
		status = http.StatusConflict
		requestStatus = "continuation_disabled"
	case errors.Is(err, resultstream.ErrCapacity), errors.Is(err, resultstream.ErrClosed):
		status = http.StatusServiceUnavailable
		retryable = true
		requestStatus = "continuation_saturated"
		w.Header().Set("Retry-After", "1")
	case errors.Is(err, resultstream.ErrExpiredQID), errors.Is(err, resultstream.ErrGoneQID), errors.Is(err, resultstream.ErrInvalidated):
		status = http.StatusGone
		requestStatus = "continuation_gone"
	}
	s.writeJSON(w, status, map[string]any{
		"error": err.Error(), "retryable": retryable,
		"http_code": status, "request_status": requestStatus,
	})
}

func (s *Server) writeSearchContinuationPage(w http.ResponseWriter, page *search.SearchContinuationPage) {
	response := page.SearchResponse()
	setSearchFallbackReasonHeader(w, page.FallbackReason)
	if provider, ok := any(response).(interface{ ResponseHeaders() map[string]string }); ok {
		for key, value := range provider.ResponseHeaders() {
			w.Header().Set(key, value)
		}
	}
	body := map[string]any{
		"results":               nornicdb.MapSearchResponse(page.SearchResponse()),
		"qid":                   page.QID,
		"has_more":              page.HasMore,
		"position":              page.Position,
		"returned":              page.Returned,
		"discovered":            page.Discovered,
		"total":                 page.Total,
		"expires_at":            page.ExpiresAt,
		"released":              page.Released,
		"search_method":         page.SearchMethod,
		"fallback_triggered":    page.FallbackTriggered,
		"fallback_reason":       page.FallbackReason,
		"mode":                  page.Mode,
		"ranked_count":          page.RankedCount,
		"eligible_count":        page.EligibleCount,
		"ranked_pool_exhausted": page.RankedPoolExhausted,
		"collection_exhausted":  page.CollectionExhausted,
		"completion":            page.Completion,
	}
	if provider, ok := any(response).(interface{ ResultMetadata() map[string]any }); ok {
		for key, value := range provider.ResultMetadata() {
			body[key] = value
		}
	}
	s.writeJSON(w, http.StatusOK, body)
}

func setSearchFallbackReasonHeader(w http.ResponseWriter, reason search.SearchFallbackReason) {
	if reason != search.SearchFallbackNone {
		w.Header().Set("X-NornicDB-Search-Fallback-Reason", string(reason))
	}
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
		s.writePostRequired(w, r)
		return
	}

	var req struct {
		Database string `json:"database,omitempty"` // Optional: defaults to default database
		NodeID   string `json:"node_id"`
		Limit    int    `json:"limit,omitempty"`
	}

	if err := s.readJSON(r, &req); err != nil {
		s.writeInvalidRequestBody(w, r)
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
		s.writeNeo4jDatabaseAccessDenied(w, r, dbName)
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
		s.writeDatabaseNotFound(w, r, http.StatusNotFound, ErrNotFound, dbName)
		return
	}

	// Get the target node from namespaced storage
	targetNode, err := storageEngine.GetNode(storage.NodeID(req.NodeID))
	if err != nil {
		s.writeLocalizedError(w, r, http.StatusNotFound, localization.SearchNodeNotFound(req.NodeID), ErrNotFound)
		return
	}

	if len(targetNode.ChunkEmbeddings) == 0 || len(targetNode.ChunkEmbeddings[0]) == 0 {
		s.writeNodeHasNoEmbedding(w, r)
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
		s.writeBoundaryError(w, r, http.StatusInternalServerError, err, ErrInternalError)
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
