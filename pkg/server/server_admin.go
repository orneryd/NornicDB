package server

import (
	"errors"
	"net/http"
	"runtime"
	"strings"

	"github.com/orneryd/nornicdb/internal/httpresponse"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
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
		if request := auth.FineGrainedRequestFromContext(r.Context()); request != nil {
			if !request.CanAccessDatabase(dbName) || !request.AllowsDatabase("database.stats.read", dbName, "") {
				continue
			}
		} else if _, readable := s.databaseReadAccess(getClaims(r), dbName); !readable {
			continue
		}

		storage, err := s.dbManager.GetStorage(dbName)
		if err != nil {
			continue
		}

		// A failed count read must not be published as a zero: an operator
		// cannot tell "this database is empty" from "the count could not be
		// read". The statistics answer fails instead, and the generic
		// internal error keeps the storage cause out of the response.
		nodeCount, edgeCount, err := readDatabaseCounts(r.Context(), storage)
		if err != nil {
			s.writeInternalServerError(w, r)
			return
		}

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
	if auth.FineGrainedRequestFromContext(r.Context()) == nil && !s.authorizeAllDatabaseOperation(w, r) {
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
	var status string
	denied := false
	ctx := r.Context()
	operationErr := s.dbManager.WithBackupCatalog(ctx, func(catalog multidb.BackupCatalog) error {
		if auth.FineGrainedRequestFromContext(ctx) == nil && !s.authorizeAllDatabaseOperation(w, r) {
			denied = true
			return nil
		}
		switch operation {
		case backupCreate:
			status = s.localizedText(w, r, localization.BackupComplete())
			return catalog.Write(ctx, req.Path, func() error { return s.db.Backup(ctx, req.Path) })
		case backupRestore:
			if catalog.HasDedicatedStores() {
				return storage.ErrOfflineDatabaseRestore
			}
			status = "restore complete"
			return s.db.Restore(ctx, req.Path)
		default:
			return errors.New("invalid backup operation")
		}
	})
	if denied {
		return
	}
	if operationErr != nil {
		if errors.Is(operationErr, storage.ErrOfflineDatabaseRestore) {
			s.writeBoundaryError(w, r, http.StatusConflict, operationErr, storage.ErrOfflineDatabaseRestore)
			return
		}
		s.writeBoundaryError(w, r, http.StatusInternalServerError, operationErr, ErrInternalError)
		return
	}
	httpresponse.WritePropertyJSON(w, http.StatusOK, map[string]string{"status": status, "path": req.Path})
}
