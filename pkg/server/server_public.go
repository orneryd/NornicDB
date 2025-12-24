package server

import (
	"fmt"
	"net/http"
	"strings"
)

// =============================================================================
// Discovery & Health Handlers
// =============================================================================

func (s *Server) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		s.writeNeo4jError(w, http.StatusNotFound, "Neo.ClientError.Request.Invalid", "not found")
		return
	}

	// Neo4j-compatible discovery response (exact format)
	host := s.config.Address
	if host == "0.0.0.0" {
		host = "localhost"
	}

	// Neo4j-compatible discovery response - minimal info to reduce reconnaissance surface
	// Feature details moved to authenticated /status endpoint
	response := map[string]interface{}{
		"bolt_direct":   fmt.Sprintf("bolt://%s:7687", host),
		"bolt_routing":  fmt.Sprintf("neo4j://%s:7687", host),
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

	// Calculate stats across all user databases (excluding system)
	// This matches the logic in handleAdminStats for consistency
	var totalNodeCount, totalEdgeCount int64
	databaseCount := 0
	if s.dbManager != nil {
		databases := s.dbManager.ListDatabases()
		databaseCount = len(databases)

		for _, dbInfo := range databases {
			dbName := dbInfo.Name
			// Skip system database from totals (it contains metadata)
			if dbName == "system" {
				continue
			}

			storage, err := s.dbManager.GetStorage(dbName)
			if err != nil {
				continue
			}

			nodeCount, _ := storage.NodeCount()
			edgeCount, _ := storage.EdgeCount()

			totalNodeCount += nodeCount
			totalEdgeCount += edgeCount
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

	response := map[string]interface{}{
		"status": "running",
		"server": map[string]interface{}{
			"uptime_seconds": stats.Uptime.Seconds(),
			"requests":       stats.RequestCount,
			"errors":         stats.ErrorCount,
			"active":         stats.ActiveRequests,
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

// handleMetrics returns Prometheus-compatible metrics.
// This endpoint can be scraped by Prometheus at /metrics.
//
// Metrics exported:
//   - nornicdb_uptime_seconds: Server uptime in seconds
//   - nornicdb_requests_total: Total HTTP requests
//   - nornicdb_errors_total: Total request errors
//   - nornicdb_active_requests: Currently active requests
//   - nornicdb_nodes_total: Total nodes in database
//   - nornicdb_edges_total: Total edges in database
//   - nornicdb_embeddings_processed: Embeddings processed
//   - nornicdb_embeddings_failed: Embedding failures
//   - nornicdb_embedding_worker_running: Whether embed worker is active (0/1)
//
// Example Prometheus config:
//
//	scrape_configs:
//	  - job_name: 'nornicdb'
//	    static_configs:
//	      - targets: ['localhost:7474']
//	    metrics_path: '/metrics'
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	stats := s.Stats()
	dbStats := s.db.Stats()

	// Build Prometheus format output
	var sb strings.Builder

	// Server metrics
	sb.WriteString("# HELP nornicdb_uptime_seconds Server uptime in seconds\n")
	sb.WriteString("# TYPE nornicdb_uptime_seconds gauge\n")
	fmt.Fprintf(&sb, "nornicdb_uptime_seconds %.2f\n", stats.Uptime.Seconds())

	sb.WriteString("# HELP nornicdb_requests_total Total HTTP requests\n")
	sb.WriteString("# TYPE nornicdb_requests_total counter\n")
	fmt.Fprintf(&sb, "nornicdb_requests_total %d\n", stats.RequestCount)

	sb.WriteString("# HELP nornicdb_errors_total Total request errors\n")
	sb.WriteString("# TYPE nornicdb_errors_total counter\n")
	fmt.Fprintf(&sb, "nornicdb_errors_total %d\n", stats.ErrorCount)

	sb.WriteString("# HELP nornicdb_active_requests Currently active requests\n")
	sb.WriteString("# TYPE nornicdb_active_requests gauge\n")
	fmt.Fprintf(&sb, "nornicdb_active_requests %d\n", stats.ActiveRequests)

	// Database metrics
	sb.WriteString("# HELP nornicdb_nodes_total Total nodes in database\n")
	sb.WriteString("# TYPE nornicdb_nodes_total gauge\n")
	fmt.Fprintf(&sb, "nornicdb_nodes_total %d\n", dbStats.NodeCount)

	sb.WriteString("# HELP nornicdb_edges_total Total edges in database\n")
	sb.WriteString("# TYPE nornicdb_edges_total gauge\n")
	fmt.Fprintf(&sb, "nornicdb_edges_total %d\n", dbStats.EdgeCount)

	// Embedding metrics
	if embedStats := s.db.EmbedQueueStats(); embedStats != nil {
		sb.WriteString("# HELP nornicdb_embeddings_processed Total embeddings processed\n")
		sb.WriteString("# TYPE nornicdb_embeddings_processed counter\n")
		fmt.Fprintf(&sb, "nornicdb_embeddings_processed %d\n", embedStats.Processed)

		sb.WriteString("# HELP nornicdb_embeddings_failed Total embedding failures\n")
		sb.WriteString("# TYPE nornicdb_embeddings_failed counter\n")
		fmt.Fprintf(&sb, "nornicdb_embeddings_failed %d\n", embedStats.Failed)

		sb.WriteString("# HELP nornicdb_embedding_worker_running Whether embed worker is active\n")
		sb.WriteString("# TYPE nornicdb_embedding_worker_running gauge\n")
		running := 0
		if embedStats.Running {
			running = 1
		}
		fmt.Fprintf(&sb, "nornicdb_embedding_worker_running %d\n", running)
	}

	// Slow query metrics
	sb.WriteString("# HELP nornicdb_slow_queries_total Total slow queries logged\n")
	sb.WriteString("# TYPE nornicdb_slow_queries_total counter\n")
	fmt.Fprintf(&sb, "nornicdb_slow_queries_total %d\n", s.slowQueryCount.Load())

	sb.WriteString("# HELP nornicdb_slow_query_threshold_ms Slow query threshold in milliseconds\n")
	sb.WriteString("# TYPE nornicdb_slow_query_threshold_ms gauge\n")
	fmt.Fprintf(&sb, "nornicdb_slow_query_threshold_ms %d\n", s.config.SlowQueryThreshold.Milliseconds())

	// Info metric with version
	sb.WriteString("# HELP nornicdb_info Database information\n")
	sb.WriteString("# TYPE nornicdb_info gauge\n")
	sb.WriteString("nornicdb_info{version=\"1.0.0\",backend=\"badger\"} 1\n")

	// Set content type for Prometheus
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(sb.String()))
}
