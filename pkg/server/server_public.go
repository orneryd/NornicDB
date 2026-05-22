package server

import (
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/orneryd/nornicdb/pkg/observability"
)

// =============================================================================
// Discovery & Health Handlers
// =============================================================================

func (s *Server) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		s.writeNeo4jError(w, http.StatusNotFound, "Neo.ClientError.Request.Invalid", "not found")
		return
	}

	// Pick the host the way the caller addressed us so browser-based
	// clients construct cookie-aware bolt:// / ws:// URLs that match
	// the page origin. Cookies are scoped by exact hostname per RFC 6265
	// (localhost and 127.0.0.1 are distinct), so emitting bolt://localhost
	// when the page is on 127.0.0.1 (or vice versa) silently breaks the
	// implicit-bearer flow. Fall back to the configured Address only when
	// the request didn't include a Host header (rare).
	host := hostnameFromRequest(r)
	if host == "" {
		host = s.config.Address
	}
	if host == "0.0.0.0" || host == "::" {
		host = "localhost"
	}

	// Neo4j-compatible discovery response - minimal info to reduce reconnaissance surface
	// Feature details moved to authenticated /status endpoint
	//
	// ws_direct / wss_direct are NornicDB extensions: browser-based
	// clients use these to construct neo4j-driver Bolt-over-WS sessions.
	// All four bolt URLs share s.config.BoltPort so non-default ports
	// (Docker, multi-instance dev) are reflected accurately.
	boltPort := s.config.BoltPort
	if boltPort == 0 {
		boltPort = 7687
	}
	response := map[string]interface{}{
		"bolt_direct":   fmt.Sprintf("bolt://%s:%d", host, boltPort),
		"bolt_routing":  fmt.Sprintf("neo4j://%s:%d", host, boltPort),
		"ws_direct":     fmt.Sprintf("ws://%s:%d", host, boltPort),
		"wss_direct":    fmt.Sprintf("wss://%s:%d", host, boltPort),
		"transaction":   fmt.Sprintf("http://%s:%d/db/{databaseName}/tx", host, s.config.Port),
		"neo4j_version": "5.0.0",
		"neo4j_edition": "community",
	}

	// Add default database name for UI compatibility (NornicDB extension)
	// This allows clients to know which database to use by default
	if s.dbManager != nil {
		response["default_database"] = s.dbManager.DefaultDatabaseName()
	}

	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	// Minimal health response - no operational details to reduce reconnaissance surface
	// Detailed status available at authenticated /status endpoint
	response := map[string]interface{}{
		"status": "healthy",
	}
	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	stats := s.Stats()

	// Calculate stats across all user databases (excluding system).
	// Primary source: storage-maintained cached per-namespace counters
	// (NodeCountByPrefix/EdgeCountByPrefix), which are incrementally updated.
	// Fallback: DatabaseManager metadata (may be stale).
	var totalNodeCount, totalEdgeCount int64
	databaseCount := 0

	usedCachedPrefixCounts := false
	if base := s.db.GetBaseStorageForManager(); base != nil {
		if lister, ok := base.(interface{ ListNamespaces() []string }); ok {
			if statsEngine, ok := base.(interface {
				NodeCountByPrefix(prefix string) (int64, error)
				EdgeCountByPrefix(prefix string) (int64, error)
			}); ok {
				for _, ns := range lister.ListNamespaces() {
					select {
					case <-r.Context().Done():
						return
					default:
					}
					if ns == "" || ns == "system" {
						continue
					}
					databaseCount++
					prefix := ns + ":"
					if n, err := statsEngine.NodeCountByPrefix(prefix); err == nil {
						totalNodeCount += n
						usedCachedPrefixCounts = true
					}
					if e, err := statsEngine.EdgeCountByPrefix(prefix); err == nil {
						totalEdgeCount += e
						usedCachedPrefixCounts = true
					}
				}
			}
		}
	}

	// Fallback when cached prefix counters are not available.
	if !usedCachedPrefixCounts && s.dbManager != nil {
		databases := s.dbManager.ListDatabases()
		for _, dbInfo := range databases {
			select {
			case <-r.Context().Done():
				return
			default:
			}
			dbName := dbInfo.Name
			if dbName == "system" {
				continue
			}
			databaseCount++
			totalNodeCount += dbInfo.NodeCount
		}
	}

	// Build embedding info
	embedInfo := map[string]interface{}{
		"enabled": false,
	}
	if embedStats := s.db.EmbedQueueStats(); embedStats != nil {
		status := "idle"
		if embedStats.Running {
			status = "processing"
		}
		embedInfo = map[string]interface{}{
			"enabled":   true,
			"status":    status,
			"processed": embedStats.Processed,
			"failed":    embedStats.Failed,
		}
	}

	searchReadyDatabases := 0
	searchBuildingDatabases := 0
	if s.dbManager != nil {
		for _, info := range s.dbManager.ListDatabases() {
			if info.Name == "" || info.Name == "system" {
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

	s.writeJSON(w, http.StatusOK, response)
}

// handleMetrics serves the legacy :7474/metrics endpoint by translating
// the unified pkg/observability *prometheus.Registry through
// observability.RenderLegacy (Phase 5 / Plan 05-04 D-01e). The body
// stream is identical (byte-for-byte, locked by
// pkg/observability/legacy_snapshot.golden) to the pre-Phase-5 hand-built
// emit in this file — but every line now flows from the unified Phase 4
// registry, eliminating the second source of truth (ROADMAP SC #1).
//
// Three locked headers per MET-19/MET-20:
//   - Content-Type: text/plain; version=0.0.4; charset=utf-8 (Prometheus exposition v0.0.4)
//   - Deprecation: true                                       (RFC 8594 — surface is sunset-tagged)
//   - Sunset:      Fri, 31 Dec 2027 23:59:59 GMT              (RFC 8594 — public deprecation cutoff)
//
// The auth gate at server_router.go:117 (s.withAuth(s.handleMetrics, auth.PermRead))
// is intentionally PRESERVED VERBATIM per CONTEXT D-04 — relaxing auth on
// the deprecated surface would itself require a separate deprecation cycle.
//
// Nil-safe: if SetObsRegistry was never called (test fixtures, pre-Phase-5
// callers), s.obsRegistryForHandler returns nil and observability.RenderLegacy
// emits empty bytes — handler still sets the three headers and returns 200.
//
// Example Prometheus config (legacy scrape — operators should migrate to
// the new :9090 telemetry listener before 31 Dec 2027):
//
//	scrape_configs:
//	  - job_name: 'nornicdb-legacy'
//	    static_configs:
//	      - targets: ['localhost:7474']
//	    metrics_path: '/metrics'
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	body := observability.RenderLegacy(s.obsRegistryForHandler(), time.Now())
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
