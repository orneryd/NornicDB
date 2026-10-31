// Package server: per-database config override API (admin only).

package server

import (
	"encoding/json"
	"github.com/orneryd/nornicdb/internal/httpresponse"
	"github.com/orneryd/nornicdb/pkg/auth"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/config/dbconfig"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/search/stemmer"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// handleDbConfigPrefix handles the keys endpoint and per-database config routes.
func (s *Server) handleDbConfigPrefix(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/admin/databases/")
	if path == "config/keys" {
		if r.Method != http.MethodGet {
			s.writeNeo4jMethodNotAllowed(w, r, "Neo.ClientError.General.BadRequest")
			return
		}
		httpresponse.WriteJSON(w, http.StatusOK, dbconfig.AllowedKeys())
		return
	}
	if path == "" {
		s.writeNeo4jNotFound(w, r, "Neo.ClientError.General.BadRequest")
		return
	}
	parts := strings.SplitN(path, "/", 2)
	dbName := parts[0]
	permissions := []auth.Permission{auth.PermRead, auth.PermAdmin}
	if r.Method != http.MethodGet {
		permissions = append(permissions, auth.PermWrite)
	}
	if !s.authorizeDatabaseOperation(w, r, dbName, permissions...) {
		return
	}
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
		case http.MethodGet, http.MethodPut:
			s.handleDbConfig(w, r, dbName)
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
		s.writeBoundaryNeo4jError(w, r, http.StatusNotFound, "Neo.ClientError.Database.DatabaseNotFound", err)
		return
	}
	lce, ok := storageEngine.(storage.MVCCLifecycleEngine)
	if !ok {
		httpresponse.WriteJSON(w, http.StatusOK, map[string]interface{}{"enabled": false, "database": dbName})
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
		httpresponse.WriteJSON(w, http.StatusOK, status)
	case "mvcc/prune":
		if r.Method != http.MethodPost {
			s.writeNeo4jPostRequired(w, r, "Neo.ClientError.General.BadRequest")
			return
		}
		if err := lce.TriggerPruneNow(r.Context()); err != nil {
			s.writeBoundaryNeo4jError(w, r, http.StatusInternalServerError, "Neo.ClientError.General.UnknownError", err)
			return
		}
		httpresponse.WriteJSON(w, http.StatusOK, map[string]string{"status": s.localizedText(w, r, localization.MVCCPruneTriggered()), "database": dbName})
	case "mvcc/pause":
		if r.Method != http.MethodPost {
			s.writeNeo4jPostRequired(w, r, "Neo.ClientError.General.BadRequest")
			return
		}
		lce.PauseLifecycle()
		httpresponse.WriteJSON(w, http.StatusOK, map[string]string{"status": s.localizedText(w, r, localization.MVCCLifecyclePaused()), "database": dbName})
	case "mvcc/resume":
		if r.Method != http.MethodPost {
			s.writeNeo4jPostRequired(w, r, "Neo.ClientError.General.BadRequest")
			return
		}
		lce.ResumeLifecycle()
		httpresponse.WriteJSON(w, http.StatusOK, map[string]string{"status": s.localizedText(w, r, localization.MVCCLifecycleResumed()), "database": dbName})
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
			s.writeBoundaryNeo4jError(w, r, http.StatusBadRequest, "Neo.ClientError.General.BadRequest", err)
			return
		}
		status := lce.LifecycleStatus()
		status["database"] = dbName
		httpresponse.WriteJSON(w, http.StatusOK, status)
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
		httpresponse.WriteJSON(w, http.StatusOK, map[string]interface{}{
			"database": dbName,
			"limit":    limit,
			"keys":     keys,
		})
	default:
		s.writeNeo4jNotFound(w, r, "Neo.ClientError.General.BadRequest")
	}
}

func (s *Server) handleDbConfig(w http.ResponseWriter, r *http.Request, dbName string) {
	if r.Method == http.MethodGet {
		overrides := s.dbConfigStore.GetOverrides(dbName)
		if overrides == nil {
			overrides = make(map[string]string)
		}
		resolved := dbconfig.Resolve(s.processConfig, overrides)
		effective, pendingRestart := s.databaseConfigRuntimeState(dbName, resolved)
		writeDatabaseConfigResponse(w, overrides, effective, pendingRestart, nil)
		return
	}
	if r.Method != http.MethodPut {
		s.writeNeo4jMethodNotAllowed(w, r, "Neo.ClientError.General.BadRequest")
		return
	}
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
	body.Overrides = dbconfig.CanonicalizeOverrides(body.Overrides)
	previousOverrides := s.dbConfigStore.GetOverrides(dbName)
	previousResolved := dbconfig.Resolve(s.processConfig, previousOverrides)
	hasSearchRebuildChange := false
	hasSearchCacheChange := false
	hasTransactionMemoryChange := false
	for key, value := range body.Overrides {
		if !dbconfig.IsAllowedKey(key) {
			s.writeLocalizedNeo4jError(w, r, http.StatusBadRequest, "Neo.ClientError.General.BadRequest", localization.DisallowedOrUnknownConfigKey(key))
			return
		}
		normalized, err := dbconfig.NormalizeSettingValue(key, value)
		if err != nil {
			s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.General.BadRequest", observability.RedactText(err.Error()))
			return
		}
		body.Overrides[key] = normalized
	}
	nextResolved := dbconfig.Resolve(s.processConfig, body.Overrides)
	selectedStemmer := stemmer.NormalizeSelection(nextResolved.Effective["db.nornic.search.bm25.stemmer"])
	if nextResolved.BM25Enabled && selectedStemmer != stemmer.NoneID {
		if _, ok := stemmer.Lookup(selectedStemmer); !ok {
			s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.General.BadRequest", observability.RedactText("configured BM25 stemmer \""+selectedStemmer+"\" is not registered"))
			return
		}
	}
	changedDefinitions := make([]dbconfig.SettingDefinition, 0)
	for _, definition := range dbconfig.Settings() {
		if previousResolved.Effective[definition.Name] == nextResolved.Effective[definition.Name] {
			continue
		}
		changedDefinitions = append(changedDefinitions, definition)
		switch definition.HotReload {
		case dbconfig.HotReloadSearchCache:
			hasSearchCacheChange = true
		case dbconfig.HotReloadSearchRebuild:
			hasSearchRebuildChange = true
		case dbconfig.HotReloadTransactionMemory:
			hasTransactionMemoryChange = true
		}
	}
	if err := s.dbConfigStore.SetOverrides(r.Context(), dbName, body.Overrides); err != nil {
		s.writeBoundaryNeo4jError(w, r, http.StatusInternalServerError, "Neo.ClientError.General.UnknownError", err)
		return
	}
	// Reload so in-memory cache is current
	if err := s.dbConfigStore.Load(r.Context()); err != nil {
		s.logEvent(r.Context(), slog.LevelWarn, localization.ServerDBConfigReloadFailedEvent(err))
	}
	rebuildTriggered := false
	// Per-db overrides must apply via fresh search service initialization,
	// not runtime in-place strategy transitions.
	if hasTransactionMemoryChange {
		s.applyTransactionMemoryLimits(dbName, nextResolved)
	}
	if hasSearchCacheChange {
		s.db.SetSearchResultCachePolicy(dbName, nextResolved.SearchResultCacheMaxEntries, nextResolved.SearchResultCacheTTL)
	}
	if hasSearchRebuildChange && !s.dbManager.IsCompositeDatabase(dbName) {
		s.db.ResetSearchService(dbName)
		if storageEngine, err := s.dbManager.GetStorage(dbName); err != nil {
			s.logEvent(r.Context(), slog.LevelWarn, localization.ServerDBConfigRebuildStorageResolveFailedEvent(dbName, err))
		} else if _, err := s.db.EnsureSearchIndexes(s.shutdown.work.Context(), dbName, storageEngine, nornicdb.SearchIndexBuildStart); err != nil {
			s.logEvent(r.Context(), slog.LevelWarn, localization.ServerDBConfigRebuildStartFailedEvent(dbName, err))
		} else {
			rebuildTriggered = true
		}
	}
	overrides := s.dbConfigStore.GetOverrides(dbName)
	if overrides == nil {
		overrides = make(map[string]string)
	}
	effective, pendingRestart := s.updateDatabaseConfigRuntimeState(dbName, previousResolved, nextResolved, changedDefinitions)
	writeDatabaseConfigResponse(w, overrides, effective, pendingRestart, &rebuildTriggered)
}

// Both operations return the same redacted configuration; a PUT also reports
// whether its change started a rebuild.
func writeDatabaseConfigResponse(w http.ResponseWriter, overrides, effective map[string]string, pendingRestart bool, rebuildTriggered *bool) {
	redacted := redactDatabaseSettings(overrides)
	response := map[string]interface{}{
		"overrides": redacted, "configured": redacted,
		"effective": redactDatabaseSettings(effective), "pendingRestart": pendingRestart,
	}
	if rebuildTriggered != nil {
		response["rebuildTriggered"] = *rebuildTriggered
	}
	httpresponse.WriteJSON(w, http.StatusOK, response)
}

func cloneStringMap(values map[string]string) map[string]string {
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func parseEmbeddingLabelList(value string) []string {
	var labels []string
	for _, part := range strings.Split(value, ",") {
		if label := strings.TrimSpace(part); label != "" {
			labels = append(labels, label)
		}
	}
	return labels
}

func redactDatabaseSettings(values map[string]string) map[string]string {
	redacted := cloneStringMap(values)
	for key := range redacted {
		definition, ok := dbconfig.LookupSetting(key)
		if ok && definition.Redacted {
			redacted[key] = observability.RedactedPlaceholder()
		} else {
			redacted[key] = observability.DisplayValue(key, redacted[key])
		}
	}
	return redacted
}

// databaseSettingsResolver is SHOW SETTINGS' view of dbName: its stored
// overrides and the values active now.
func (s *Server) databaseSettingsResolver(dbName string) cypher.SettingsResolver {
	return func() cypher.SettingsSnapshot {
		overrides := s.dbConfigStore.GetOverrides(dbName)
		current := dbconfig.Resolve(s.processConfig, overrides)
		active, _ := s.databaseConfigRuntimeState(dbName, current)
		return cypher.SettingsSnapshot{Configured: overrides, Active: active}
	}
}

func (s *Server) databaseConfigRuntimeState(dbName string, resolved *dbconfig.ResolvedDbConfig) (map[string]string, bool) {
	s.dbConfigStateMu.Lock()
	defer s.dbConfigStateMu.Unlock()
	active, exists := s.dbConfigActive[dbName]
	if !exists {
		active = cloneStringMap(resolved.Effective)
		s.dbConfigActive[dbName] = active
	}
	return cloneStringMap(active), len(s.dbConfigPending[dbName]) > 0
}

func (s *Server) updateDatabaseConfigRuntimeState(dbName string, previous, next *dbconfig.ResolvedDbConfig, changed []dbconfig.SettingDefinition) (map[string]string, bool) {
	s.dbConfigStateMu.Lock()
	defer s.dbConfigStateMu.Unlock()
	active, exists := s.dbConfigActive[dbName]
	if !exists {
		active = cloneStringMap(previous.Effective)
		s.dbConfigActive[dbName] = active
	}
	pending := s.dbConfigPending[dbName]
	if pending == nil {
		pending = make(map[string]struct{})
		s.dbConfigPending[dbName] = pending
	}
	for _, definition := range changed {
		if definition.RestartLevel == dbconfig.RestartNone {
			active[definition.Name] = next.Effective[definition.Name]
			delete(pending, definition.Name)
			continue
		}
		if active[definition.Name] == next.Effective[definition.Name] {
			delete(pending, definition.Name)
		} else {
			pending[definition.Name] = struct{}{}
		}
	}
	if len(pending) == 0 {
		delete(s.dbConfigPending, dbName)
	}
	return cloneStringMap(active), len(pending) > 0
}

// applyTransactionMemoryLimits applies a database's resolved transaction
// memory limits (db.memory.transaction.max, db.memory.transaction.total.max)
// to the statements that start from now on (#1343).
func (s *Server) applyTransactionMemoryLimits(dbName string, resolved *dbconfig.ResolvedDbConfig) {
	cypher.SetDatabaseTransactionMemoryLimits(dbName, cypher.TransactionMemoryLimits{
		TransactionMax:   resolved.TransactionMemoryMaxBytes,
		DatabaseTotalMax: resolved.TransactionMemoryTotalMaxBytes,
	})
}
