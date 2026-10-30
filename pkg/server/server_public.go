package server

import (
	"fmt"
	"github.com/orneryd/nornicdb/internal/httpresponse"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
	"github.com/orneryd/nornicdb/pkg/observability"
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

	httpresponse.WriteJSON(w, http.StatusOK, response)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	// Minimal health response - no operational details to reduce reconnaissance surface
	// Detailed status available at authenticated /status endpoint
	response := map[string]interface{}{
		"status": "healthy",
	}
	httpresponse.WriteJSON(w, http.StatusOK, response)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	stats := s.Stats()

	totalNodeCount, totalEdgeCount, databaseCount := s.readableDatabaseCounts(r, false, s.databaseReadPredicate(getClaims(r)))
	if bulk.ObserveContext(r.Context()) != nil {
		return
	}

	// Build embedding info
	embedInfo := map[string]interface{}{
		"enabled": false,
	}
	if embedStats := s.db.EmbedQueueStats(); embedStats != nil && s.canReadAllDatabases(r) {
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

	httpresponse.WriteJSON(w, http.StatusOK, response)
}

// handleMetrics serves the legacy :7474/metrics endpoint by translating
// the unified pkg/observability *prometheus.Registry through
// observability.RenderLegacy (Phase 5 / Plan 05-04 D-01e). The body
// schema is locked by pkg/observability/legacy_snapshot.golden. With auth
// enabled, storage counts and database-labelled samples use the caller's
// databaseReadAccess decision. Unpartitioned database-derived counters require
// access to their complete population. Auth-off retains the process-wide view.
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
	registry := s.obsRegistryForHandler()
	var body []byte
	if registry == nil || !s.isRBACEnforced() {
		body = observability.RenderLegacy(registry, time.Now())
	} else {
		canRead := s.databaseReadPredicate(getClaims(r))
		nodes, edges, databases := s.readableDatabaseCounts(r, true, canRead)
		if bulk.ObserveContext(r.Context()) != nil {
			return
		}
		anyReadable, allReadable := auth.DatabasePopulationAccess(s.dbManager.ListDatabaseNames(), canRead)
		anyReadable = anyReadable || databases > 0
		if !anyReadable {
			s.writeNeo4jDatabaseAccessDenied(w, r, "")
			return
		}
		body = observability.RenderLegacyForRead(registry, time.Now(), observability.LegacyReadScope{
			CanReadDatabase:      canRead,
			AllDatabasesReadable: allReadable,
			Nodes:                nodes, Edges: edges,
		})
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
// /status and legacy /metrics. Each namespace passes databaseReadAccess before
// reading cached prefix counts; aliases/composites cannot double-count storage.
// Status excludes system; metrics includes it only when the caller can read it.
func (s *Server) readableDatabaseCounts(r *http.Request, includeSystem bool, canRead func(string) bool) (totalNodeCount, totalEdgeCount int64, databaseCount int) {
	// Calculate stats across the caller's readable physical databases.
	// Primary source: storage-maintained cached per-namespace counters
	// (NodeCountByPrefix/EdgeCountByPrefix), which are incrementally updated.
	// Fallback: physical database engines when prefix counts are unavailable.

	usedCachedPrefixCounts := false
	if base := s.db.GetBaseStorageForManager(); base != nil {
		if lister, ok := base.(interface{ ListNamespaces() []string }); ok {
			if statsEngine, ok := base.(interface {
				NodeCountByPrefix(prefix string) (int64, error)
				EdgeCountByPrefix(prefix string) (int64, error)
			}); ok {
				for _, ns := range lister.ListNamespaces() {
					select {
					case <-bulk.Done(r.Context()):
						return
					default:
					}
					if ns == "" || (!includeSystem && ns == "system") {
						continue
					}
					if !canRead(ns) {
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
			case <-bulk.Done(r.Context()):
				return
			default:
			}
			dbName := dbInfo.Name
			if !includeSystem && dbName == "system" {
				continue
			}
			if !canRead(dbName) {
				continue
			}
			if dbInfo.Type == "composite" {
				continue
			}
			engine, err := s.dbManager.GetStorage(dbName)
			if err != nil {
				continue
			}
			databaseCount++
			if nodes, err := engine.NodeCount(); err == nil {
				totalNodeCount += nodes
			}
			if edges, err := engine.EdgeCount(); err == nil {
				totalEdgeCount += edges
			}
		}
	}

	return
}
