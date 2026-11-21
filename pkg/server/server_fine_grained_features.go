package server

import (
	"net/http"
	"strings"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/config/dbconfig"
)

func (s *Server) authorizeFineGrainedDatabaseOperation(w http.ResponseWriter, r *http.Request, database, action string, legacy ...auth.Permission) bool {
	request := auth.FineGrainedRequestFromContext(r.Context())
	if request == nil {
		if database == "" {
			return s.authorizeAllDatabaseOperation(w, r, legacy...)
		}
		return s.authorizeDatabaseOperation(w, r, database, legacy...)
	}
	if database == "" {
		if request.Access.AllowsAllDatabases(action) {
			return true
		}
	} else if request.CanAccessDatabase(database) && request.AllowsDatabase(action, database, "") {
		return true
	}
	s.writeNeo4jInsufficientPermissions(w, r)
	return false
}

func fineGrainedDatabaseSettingAction(definition dbconfig.SettingDefinition) string {
	switch definition.Name {
	case "db.nornic.embedding.provider", "db.nornic.embedding.api.url", "db.nornic.embedding.api.key":
		return "embedding.provider.update"
	case "db.nornic.embedding.model", "db.nornic.embedding.dimensions":
		return "embedding.model.update"
	}
	if strings.HasPrefix(definition.Name, "db.nornic.embedding.") {
		return "embedding.worker.settings.update"
	}
	return "database.config.update"
}

func fineGrainedDatabaseConfigRead(request *auth.FineGrainedRequest, database, key string) bool {
	if strings.HasPrefix(key, "db.nornic.embedding.") {
		return request.AllowsDatabase("embedding.config.read", database, "")
	}
	return request.AllowsDatabase("database.config.read", database, "")
}

func filterFineGrainedDatabaseConfig(request *auth.FineGrainedRequest, database string, settings map[string]string) map[string]string {
	filtered := make(map[string]string, len(settings))
	for key, value := range settings {
		if fineGrainedDatabaseConfigRead(request, database, key) {
			filtered[key] = value
		}
	}
	return filtered
}
