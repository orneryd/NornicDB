package server

import (
	"errors"
	"net/http"

	"github.com/orneryd/nornicdb/pkg/cypher"
)

type httpOperation struct {
	class        cypher.PrivilegeClass
	allDatabases bool
}

// Direct operations have no Cypher statement to classify. Their registration
// declares the actual effect; policy is still owned by cypher.AdmitOperation.
// Waking the embedding worker, retrying its failures and rebuilding a search
// index keep the write privilege their routes have always required; clearing
// or regenerating embeddings stays admin-only in its handler.
var httpOperationClasses = map[string]httpOperation{
	"decay.info":     {cypher.PrivilegeRead, false},
	"embed.trigger":  {cypher.PrivilegeRead | cypher.PrivilegeWrite, true},
	"embed.stats":    {cypher.PrivilegeRead, true},
	"embed.failures": {cypher.PrivilegeRead, true},
	"embed.retry":    {cypher.PrivilegeRead | cypher.PrivilegeWrite, true},
	"embed.target":   {cypher.PrivilegeWrite, false},
	"search.rebuild": {cypher.PrivilegeRead | cypher.PrivilegeWrite, false},
	"gdpr.export":    {cypher.PrivilegeRead, false},
	"gdpr.delete":    {cypher.PrivilegeRead | cypher.PrivilegeWrite, false},
	"status":         {cypher.PrivilegeRead, true},
}

func (s *Server) admitHTTPOperation(w http.ResponseWriter, r *http.Request, operation, database string) bool {
	contract := httpOperationClasses[operation] // a missing contract is invalid
	var manager cypher.DatabaseManagerInterface
	if s.dbManager != nil {
		manager = &databaseManagerAdapter{manager: s.dbManager, db: s.db, server: s}
	}
	if database == "" {
		if s.dbManager != nil {
			database = s.dbManager.DefaultDatabaseName()
		}
		if scoped, ok := s.db.GetStorage().(interface{ Namespace() string }); ok {
			database = scoped.Namespace()
		}
	}
	databases := []string{database}
	if contract.allDatabases {
		// Global worker/status operations expose every physical user namespace.
		// Read metadata only while finding their complete affected population.
		databases = cypher.PhysicalUserDatabases(manager, s.db.GetBaseStorageForManager(), database)
	}
	access := s.principalAccess(getClaims(r))
	for _, target := range databases {
		if err := cypher.AdmitOperation(r.Context(), access.Allows, target, contract.class); err != nil {
			// An unnamed global operation does not reveal which inaccessible
			// namespace exists. Explicit selections retain their own name.
			label := target
			if contract.allDatabases {
				label = "requested database"
			}
			var missing *cypher.PermissionDeniedError
			if errors.As(err, &missing) && missing.Permission == "write" {
				s.writeNeo4jDatabaseWriteDenied(w, r, label)
			} else {
				s.writeNeo4jDatabaseAccessDenied(w, r, label)
			}
			return false
		}
	}
	return true
}
