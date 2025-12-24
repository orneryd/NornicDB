package server

import (
	"net/http"
	"runtime"
)

// =============================================================================
// Admin Handlers
// =============================================================================

func (s *Server) handleAdminStats(w http.ResponseWriter, r *http.Request) {
	serverStats := s.Stats()

	// Calculate stats across all user databases (excluding system)
	// This provides accurate counts in a multi-database setup
	var totalNodeCount, totalEdgeCount int64
	databases := s.dbManager.ListDatabases()
	perDatabaseStats := make(map[string]map[string]int64)

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

		perDatabaseStats[dbName] = map[string]int64{
			"node_count": nodeCount,
			"edge_count": edgeCount,
		}
	}

	dbStats := map[string]interface{}{
		"node_count":   totalNodeCount,
		"edge_count":   totalEdgeCount,
		"databases":    len(databases),
		"per_database": perDatabaseStats,
	}

	response := map[string]interface{}{
		"server":   serverStats,
		"database": dbStats,
		"memory": map[string]interface{}{
			"alloc_mb":   getMemoryUsageMB(),
			"goroutines": runtime.NumGoroutine(),
		},
	}

	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleAdminConfig(w http.ResponseWriter, r *http.Request) {
	// Return safe config (no secrets)
	config := map[string]interface{}{
		"address":      s.config.Address,
		"port":         s.config.Port,
		"cors_enabled": s.config.EnableCORS,
		"compression":  s.config.EnableCompression,
		"tls_enabled":  s.config.TLSCertFile != "",
	}

	s.writeJSON(w, http.StatusOK, config)
}

func (s *Server) handleBackup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "POST required", ErrMethodNotAllowed)
		return
	}

	var req struct {
		Path string `json:"path"`
	}

	if err := s.readJSON(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body", ErrBadRequest)
		return
	}

	if err := s.db.Backup(r.Context(), req.Path); err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error(), ErrInternalError)
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{
		"status": "backup complete",
		"path":   req.Path,
	})
}
