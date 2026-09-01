// Package server: per-database config override API (admin only).

package server

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/config/dbconfig"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// GET /admin/databases/config/keys
func (s *Server) handleDbConfigKeys(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeNeo4jMethodNotAllowed(w, r, "Neo.ClientError.General.BadRequest")
		return
	}
	keys := dbconfig.AllowedKeys()
	s.writeJSON(w, http.StatusOK, keys)
}

// handleDbConfigPrefix handles GET/PUT /admin/databases/{dbName}/config.
// Route is registered as /admin/databases/ so we receive e.g. /admin/databases/nornic/config.
func (s *Server) handleDbConfigPrefix(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/admin/databases/")
	if path == "" || path == "config/keys" {
		// config/keys is handled by handleDbConfigKeys
		s.writeNeo4jNotFound(w, r, "Neo.ClientError.General.BadRequest")
		return
	}
	parts := strings.SplitN(path, "/", 2)
	dbName := parts[0]
	if dbName == "system" {
		s.writeLocalizedNeo4jError(w, r, http.StatusBadRequest, "Neo.ClientError.General.BadRequest", localization.SystemDatabaseOverridesUnsupported())
		return
	}
	if len(parts) != 2 {
		s.writeNeo4jNotFound(w, r, "Neo.ClientError.General.BadRequest")
		return
	}
	switch {
	case parts[1] == "config":
		if s.dbConfigStore == nil {
			s.writeLocalizedNeo4jError(w, r, http.StatusServiceUnavailable, "Neo.ClientError.General.Unavailable", localization.DatabaseConfigUnavailable())
			return
		}
		switch r.Method {
		case http.MethodGet:
			s.handleGetDbConfig(w, r, dbName)
		case http.MethodPut:
			s.handlePutDbConfig(w, r, dbName)
		default:
			s.writeNeo4jMethodNotAllowed(w, r, "Neo.ClientError.General.BadRequest")
		}
	case parts[1] == "mvcc" || strings.HasPrefix(parts[1], "mvcc/"):
		s.handleDbLifecyclePrefix(w, r, dbName, parts[1])
	default:
		s.writeNeo4jNotFound(w, r, "Neo.ClientError.General.BadRequest")
	}
}

func (s *Server) handleDbLifecyclePrefix(w http.ResponseWriter, r *http.Request, dbName string, suffix string) {
	if s.dbManager == nil {
		s.writeLocalizedNeo4jError(w, r, http.StatusServiceUnavailable, "Neo.ClientError.General.Unavailable", localization.DatabaseManagerUnavailable())
		return
	}
	if s.dbManager.IsCompositeDatabase(dbName) {
		s.writeLocalizedNeo4jError(w, r, http.StatusBadRequest, "Neo.ClientError.Statement.NotSupported", localization.MVCCCompositeUnsupported())
		return
	}
	storageEngine, err := s.dbManager.GetStorage(dbName)
	if err != nil {
		s.writeNeo4jError(w, http.StatusNotFound, "Neo.ClientError.Database.DatabaseNotFound", err.Error())
		return
	}
	lce, ok := storageEngine.(storage.MVCCLifecycleEngine)
	if !ok {
		s.writeJSON(w, http.StatusOK, map[string]interface{}{"enabled": false, "database": dbName})
		return
	}
	switch suffix {
	case "mvcc", "mvcc/status":
		if r.Method != http.MethodGet {
			s.writeNeo4jGetRequired(w, r, "Neo.ClientError.General.BadRequest")
			return
		}
		status := lce.LifecycleStatus()
		status["database"] = dbName
		s.writeJSON(w, http.StatusOK, status)
	case "mvcc/prune":
		if r.Method != http.MethodPost {
			s.writeNeo4jPostRequired(w, r, "Neo.ClientError.General.BadRequest")
			return
		}
		if err := lce.TriggerPruneNow(r.Context()); err != nil {
			s.writeNeo4jError(w, http.StatusInternalServerError, "Neo.ClientError.General.UnknownError", err.Error())
			return
		}
		s.writeJSON(w, http.StatusOK, map[string]string{"status": "prune triggered", "database": dbName})
	case "mvcc/pause":
		if r.Method != http.MethodPost {
			s.writeNeo4jPostRequired(w, r, "Neo.ClientError.General.BadRequest")
			return
		}
		lce.PauseLifecycle()
		s.writeJSON(w, http.StatusOK, map[string]string{"status": "lifecycle paused", "database": dbName})
	case "mvcc/resume":
		if r.Method != http.MethodPost {
			s.writeNeo4jPostRequired(w, r, "Neo.ClientError.General.BadRequest")
			return
		}
		lce.ResumeLifecycle()
		s.writeJSON(w, http.StatusOK, map[string]string{"status": "lifecycle resumed", "database": dbName})
	case "mvcc/schedule":
		if r.Method != http.MethodPost {
			s.writeNeo4jPostRequired(w, r, "Neo.ClientError.General.BadRequest")
			return
		}
		scheduler, ok := storageEngine.(storage.MVCCLifecycleScheduleEngine)
		if !ok {
			s.writeLocalizedNeo4jError(w, r, http.StatusBadRequest, "Neo.ClientError.Statement.NotSupported", localization.MVCCScheduleUnsupported())
			return
		}
		var body struct {
			Interval string `json:"interval"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			s.writeNeo4jInvalidJSONBody(w, r)
			return
		}
		interval, err := time.ParseDuration(strings.TrimSpace(body.Interval))
		if err != nil {
			s.writeLocalizedNeo4jError(w, r, http.StatusBadRequest, "Neo.ClientError.General.BadRequest", localization.InvalidInterval())
			return
		}
		if err := scheduler.SetLifecycleSchedule(interval); err != nil {
			s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.General.BadRequest", err.Error())
			return
		}
		status := lce.LifecycleStatus()
		status["database"] = dbName
		s.writeJSON(w, http.StatusOK, status)
	case "mvcc/debt":
		if r.Method != http.MethodGet {
			s.writeNeo4jGetRequired(w, r, "Neo.ClientError.General.BadRequest")
			return
		}
		provider, ok := storageEngine.(storage.MVCCLifecycleDebtEngine)
		if !ok {
			s.writeLocalizedNeo4jError(w, r, http.StatusBadRequest, "Neo.ClientError.Statement.NotSupported", localization.MVCCDebtUnsupported())
			return
		}
		limit := 10
		const maxDebtKeyLimit = 100
		if rawLimit := strings.TrimSpace(r.URL.Query().Get("limit")); rawLimit != "" {
			parsed, err := strconv.Atoi(rawLimit)
			if err != nil || parsed < 0 {
				s.writeLocalizedNeo4jError(w, r, http.StatusBadRequest, "Neo.ClientError.General.BadRequest", localization.InvalidLimit())
				return
			}
			limit = parsed
		}
		if limit > maxDebtKeyLimit {
			limit = maxDebtKeyLimit
		}
		keys := provider.TopLifecycleDebtKeys(limit)
		if keys == nil {
			keys = []storage.MVCCLifecycleDebtKey{}
		}
		s.writeJSON(w, http.StatusOK, map[string]interface{}{
			"database": dbName,
			"limit":    limit,
			"keys":     keys,
		})
	default:
		s.writeNeo4jNotFound(w, r, "Neo.ClientError.General.BadRequest")
	}
}

func (s *Server) handleGetDbConfig(w http.ResponseWriter, r *http.Request, dbName string) {
	overrides := s.dbConfigStore.GetOverrides(dbName)
	if overrides == nil {
		overrides = make(map[string]string)
	}
	global := nornicConfig.LoadFromEnv()
	resolved := dbconfig.Resolve(global, overrides)
	effective := make(map[string]string)
	if resolved != nil && resolved.Effective != nil {
		effective = resolved.Effective
	}
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"overrides": overrides,
		"effective": effective,
	})
}

func (s *Server) handlePutDbConfig(w http.ResponseWriter, r *http.Request, dbName string) {
	var body struct {
		Overrides map[string]string `json:"overrides"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		s.writeNeo4jInvalidJSONBody(w, r)
		return
	}
	if body.Overrides == nil {
		body.Overrides = make(map[string]string)
	}
	for key, value := range body.Overrides {
		if !dbconfig.IsAllowedKey(key) {
			s.writeLocalizedNeo4jError(w, r, http.StatusBadRequest, "Neo.ClientError.General.BadRequest", localization.DisallowedOrUnknownConfigKey(key))
			return
		}
		if ok, allowed := dbconfig.IsValidEnumValue(key, value); !ok {
			s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.General.BadRequest",
				"invalid value for "+key+": got "+value+" (allowed: "+allowed+")")
			return
		}
	}
	if err := s.dbConfigStore.SetOverrides(r.Context(), dbName, body.Overrides); err != nil {
		s.writeNeo4jError(w, http.StatusInternalServerError, "Neo.ClientError.General.UnknownError", err.Error())
		return
	}
	// Reload so in-memory cache is current
	if err := s.dbConfigStore.Load(r.Context()); err != nil {
		s.log.Warn("failed to reload db config store after PUT", "error", err)
	}
	rebuildTriggered := false
	// Per-db overrides must apply via fresh search service initialization,
	// not runtime in-place strategy transitions.
	if !s.dbManager.IsCompositeDatabase(dbName) {
		s.db.ResetSearchService(dbName)
		if storageEngine, err := s.dbManager.GetStorage(dbName); err != nil {
			s.log.Warn("failed to resolve storage for db config rebuild", "db", dbName, "error", err)
		} else if _, err := s.db.EnsureSearchIndexesBuildStarted(dbName, storageEngine); err != nil {
			s.log.Warn("failed to start search service rebuild after db config update", "db", dbName, "error", err)
		} else {
			rebuildTriggered = true
		}
	}
	overrides := s.dbConfigStore.GetOverrides(dbName)
	if overrides == nil {
		overrides = make(map[string]string)
	}
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"overrides":        overrides,
		"rebuildTriggered": rebuildTriggered,
	})
}
