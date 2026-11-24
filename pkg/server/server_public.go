package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/orneryd/nornicdb/internal/httpresponse"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// =============================================================================
// Discovery & Health Handlers
// =============================================================================

func (s *Server) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		s.writeNeo4jNotFound(w, r, "Neo.ClientError.Request.Invalid")
		return
	}

	// Pick the host the way the caller addressed us so browser-based
	// clients construct cookie-aware bolt:// / bolt+s:// URLs that match
	// the page origin (and produce ws:// / wss:// upgrades on the wire).
	// Cookies are scoped by exact hostname per RFC 6265 (localhost and
	// 127.0.0.1 are distinct), so emitting bolt://localhost when the
	// page is on 127.0.0.1 (or vice versa) silently breaks the
	// implicit-bearer flow. Fall back to the configured Address only when
	// the request didn't include a Host header (rare).
	host := hostnameFromRequest(r)
	if host == "" {
		host = s.config.Address
	}
	if host == "0.0.0.0" || host == "::" {
		host = "localhost"
	}

	// Neo4j-compatible discovery response - minimal info to reduce reconnaissance surface.
	// Feature details moved to authenticated /status endpoint.
	//
	// All bolt URLs share s.config.BoltPort so non-default ports
	// (Docker, multi-instance dev) are reflected accurately. Browser
	// clients use bolt:// (or bolt+s:// over HTTPS) — neo4j-driver's
	// browser bundle picks the WebSocket transport internally; the
	// driver itself rejects ws:// / wss:// schemes via its allowlist.
	response := map[string]interface{}{
		"transaction":   s.getBaseURL(r) + "/db/{databaseName}/tx",
		"neo4j_version": "5.0.0",
		"neo4j_edition": "community",
		"bolt_enabled":  s.config.BoltEnabled,
	}
	if s.config.BoltEnabled {
		boltPort := s.config.BoltPort
		if boltPort == 0 {
			boltPort = 7687
		}
		response["bolt_direct"] = fmt.Sprintf("bolt://%s:%d", host, boltPort)
		response["bolt_routing"] = fmt.Sprintf("neo4j://%s:%d", host, boltPort)
	}

	// Add default database name for UI compatibility (NornicDB extension)
	// This allows clients to know which database to use by default
	if s.dbManager != nil {
		response["default_database"] = s.dbManager.DefaultDatabaseName()
	}

	httpresponse.WritePropertyJSON(w, http.StatusOK, response)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	// Minimal health response - no operational details to reduce reconnaissance surface
	// Detailed status available at authenticated /status endpoint
	response := map[string]interface{}{
		"status": "healthy",
	}
	httpresponse.WritePropertyJSON(w, http.StatusOK, response)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	stats := s.Stats()

	totalNodeCount, totalEdgeCount, databaseCount, countErr := s.readableDatabaseCounts(r, false, s.databaseReadPredicate(getClaims(r)))
	if r.Context().Err() != nil {
		return
	}
	if countErr != nil {
		// The admitted population could not be read, so the reported totals
		// cannot be distinguished from real zeros.
		s.writeInternalServerError(w, r)
		return
	}

	// Build embedding info
	embedInfo := map[string]interface{}{
		"enabled": false,
	}
	if s.canReadAllDatabases(r) {
		if embedStats := s.readEmbeddingMonitoringStats(r.Context()); embedStats != nil {
			status := "idle"
			if embedStats.Running {
				status = "processing"
			}
			embedInfo = map[string]interface{}{
				"enabled":   true,
				"status":    status,
				"processed": embedStats.Processed,
				"pending":   embedStats.Pending,
				"failed":    embedStats.Failed,
			}
		}
	}

	searchReadyDatabases := 0
	searchBuildingDatabases := 0
	if s.dbManager != nil {
		for _, info := range s.dbManager.ListDatabases() {
			if info.Name == "" || info.Name == "system" {
				continue
			}
			if _, readable := s.databaseReadAccess(getClaims(r), info.Name); !readable {
				continue
			}
			st := s.db.GetDatabaseSearchStatus(info.Name)
			if st.Ready {
				searchReadyDatabases++
			}
			if st.Building {
				searchBuildingDatabases++
			}
		}
	}
	startupPhase := "ready"
	if searchBuildingDatabases > 0 {
		startupPhase = "search_warming"
	}

	response := map[string]interface{}{
		"status": "running",
		"startup": map[string]interface{}{
			"phase":                     startupPhase,
			"search_ready_databases":    searchReadyDatabases,
			"search_building_databases": searchBuildingDatabases,
		},
		"server": map[string]interface{}{
			"uptime_seconds": stats.Uptime.Seconds(),
			"requests":       stats.RequestCount,
			"errors":         stats.ErrorCount,
			"active":         stats.ActiveRequests,
			"version":        stats.Version,
			"commit":         stats.Commit,
			"build_time":     stats.BuildTime,
		},
		"database": map[string]interface{}{
			"nodes":     totalNodeCount, // Sum of all user databases (excluding system)
			"edges":     totalEdgeCount, // Sum of all user databases (excluding system)
			"databases": databaseCount,  // Number of databases
		},
		"embeddings": embedInfo,
	}

	httpresponse.WritePropertyJSON(w, http.StatusOK, response)
}

// handleMetrics serves the authenticated application metrics catalog. Existing
// compatibility names remain alongside the canonical registry families. Admin
// entitlement grants the server-wide view; other readers receive admitted
// database-labelled samples and the existing scoped compatibility reductions.
// The dedicated telemetry listener is unchanged and remains process-wide.
// Existing content-type and compatibility headers are preserved.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	registry := s.obsRegistryForHandler()
	var scope *observability.LegacyReadScope
	administrator := !s.isRBACEnforced()
	if registry != nil && s.dbManager != nil {
		claims := getClaims(r)
		administrator = administrator || (claims != nil && hasPermission(s, claims.Roles, auth.PermAdmin))
		canRead := s.databaseReadPredicate(claims)
		nodes, edges, databases, countErr := s.readableDatabaseCounts(r, true, canRead)
		if r.Context().Err() != nil {
			return
		}
		if countErr != nil {
			// A scrape cannot tell a failed count from an empty database, so
			// the catalogue is not produced.
			s.writeInternalServerError(w, r)
			return
		}
		anyReadable, allReadable := auth.DatabasePopulationAccess(s.dbManager.ListDatabaseNames(), canRead)
		if !anyReadable && databases == 0 {
			s.writeNeo4jDatabaseAccessDenied(w, r, "")
			return
		}
		scope = &observability.LegacyReadScope{CanReadDatabase: canRead, AllDatabasesReadable: allReadable, Nodes: nodes, Edges: edges}
	}
	w.Header().Set("Cache-Control", "no-store")
	if administrator {
		w.Header().Set("X-NornicDB-Metrics-Scope", "server")
	} else {
		w.Header().Set("X-NornicDB-Metrics-Scope", "database")
	}
	w.Header().Add("Vary", "Authorization")
	w.Header().Add("Vary", "Cookie")
	body, err := observability.RenderMetrics(registry, time.Now(), scope, administrator)
	if err != nil {
		http.Error(w, "Unable to collect metrics", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", observability.LegacyContentType)
	w.Header().Set("Deprecation", observability.LegacyDeprecation)
	w.Header().Set("Sunset", observability.LegacySunset)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// obsRegistryForHandler is a small RLock-protected accessor for the
// obsRegistry field (Phase 5 / Plan 05-04). Used by handleMetrics so the
// per-call read of the post-construction-injected registry is race-clean
// under -race -count=10.
//
// Nil-safe: returns nil when SetObsRegistry has not been called.
// observability.RenderLegacy tolerates a nil registry by emitting empty
// body bytes (Plan 05-02 contract).
func (s *Server) obsRegistryForHandler() *prometheus.Registry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.obsRegistry
}

// readableDatabaseCounts is the admitted aggregate population shared by
// /status and legacy /metrics. Physical database engines own namespace counts;
// their NodeCount/EdgeCount routes already use the context-aware prefix counters.
// Status excludes system; metrics includes it only when the caller can read it.
// A failed read returns no partial population.
func (s *Server) readableDatabaseCounts(r *http.Request, includeSystem bool, canRead func(string) bool) (totalNodeCount, totalEdgeCount int64, databaseCount int, countErr error) {
	if s.dbManager == nil {
		return
	}
	for _, dbInfo := range s.dbManager.ListDatabases() {
		if err := r.Context().Err(); err != nil {
			return 0, 0, 0, err
		}
		dbName := dbInfo.Name
		if (!includeSystem && dbName == "system") || !canRead(dbName) || dbInfo.Type == "composite" {
			continue
		}
		engine, err := s.dbManager.GetStorage(dbName)
		if err != nil {
			continue
		}
		nodes, edges, err := readDatabaseCounts(r.Context(), engine)
		if err != nil {
			return 0, 0, 0, err
		}
		databaseCount++
		totalNodeCount += nodes
		totalEdgeCount += edges
	}
	return
}

// readDatabaseCounts returns a complete pair or the original read error.
func readDatabaseCounts(ctx context.Context, engine storage.Engine) (int64, int64, error) {
	nodes, err := engine.NodeCount(ctx)
	if err != nil {
		return 0, 0, err
	}
	edges, err := engine.EdgeCount(ctx)
	if err != nil {
		return 0, 0, err
	}
	return nodes, edges, nil
}
