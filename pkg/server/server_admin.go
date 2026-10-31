package server

import (
	"errors"
	"github.com/orneryd/nornicdb/internal/httpresponse"
	"net/http"
	"runtime"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
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
		if _, readable := s.databaseReadAccess(getClaims(r), dbName); !readable {
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
		"databases":    len(perDatabaseStats),
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

	httpresponse.WritePropertyJSON(w, http.StatusOK, response)
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

	httpresponse.WritePropertyJSON(w, http.StatusOK, config)
}

type backupOperation uint8

const (
	backupCreate backupOperation = iota
	backupRestore
)

// handleBackupOperation owns the common authorization, request and error
// contract. The selected operation retains its existing storage side effects.
func (s *Server) handleBackupOperation(w http.ResponseWriter, r *http.Request, operation backupOperation) {
	if !s.authorizeAllDatabaseOperation(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		s.writePostRequired(w, r)
		return
	}
	var req struct {
		Path string `json:"path"`
	}
	if err := s.readJSON(r, &req); err != nil {
		s.writeInvalidRequestBody(w, r)
		return
	}
	if strings.TrimSpace(req.Path) == "" {
		s.writeBoundaryError(w, r, http.StatusBadRequest, errors.New("backup path is required"), ErrBadRequest)
		return
	}
	var operationErr error
	var status string
	switch operation {
	case backupCreate:
		operationErr = s.db.Backup(r.Context(), req.Path)
		if operationErr == nil {
			status = s.localizedText(w, r, localization.BackupComplete())
		}
	case backupRestore:
		operationErr = s.db.Restore(r.Context(), req.Path)
		status = "restore complete"
	default:
		operationErr = errors.New("invalid backup operation")
	}
	if operationErr != nil {
		s.writeBoundaryError(w, r, http.StatusInternalServerError, operationErr, ErrInternalError)
		return
	}
	httpresponse.WritePropertyJSON(w, http.StatusOK, map[string]string{"status": status, "path": req.Path})
}
