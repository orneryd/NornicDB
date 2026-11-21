package server

import (
	"net/http"

	"github.com/orneryd/nornicdb/internal/httpresponse"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func (s *Server) handleFineGrainedEffective(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeNeo4jGetRequired(w, r, "Neo.ClientError.Request.Invalid")
		return
	}
	if getClaims(r) == nil {
		s.writeNeo4jNoAuthenticationProvided(w, r)
		return
	}
	response := struct {
		Enforced       bool                          `json:"enforced"`
		Rules          []auth.FineGrainedEntitlement `json:"rules"`
		HomeDatabaseID string                        `json:"homeDatabaseId,omitempty"`
	}{}
	if principal := auth.FineGrainedRequestFromContext(r.Context()); principal != nil {
		response.Enforced, response.Rules, response.HomeDatabaseID = true, principal.Access.Rules(), principal.HomeDatabaseID
	}
	httpresponse.WritePropertyJSON(w, http.StatusOK, response)
}

func (s *Server) handleFineGrainedEntity(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeNeo4jGetRequired(w, r, "Neo.ClientError.Request.Invalid")
		return
	}
	if getClaims(r) == nil {
		s.writeNeo4jNoAuthenticationProvided(w, r)
		return
	}
	kind, selection, localID, valid := storage.ParseElementID(r.URL.Query().Get("element_id"))
	if !valid || (kind != "4" && kind != "5") {
		s.writeNeo4jRequestFieldRequired(w, r, "Neo.ClientError.Request.InvalidFormat", "element_id")
		return
	}
	principal := auth.FineGrainedRequestFromContext(r.Context())
	var allowed func(string) bool
	if principal != nil {
		allowed = principal.CanAccessDatabase
	}
	database, remote, selectable := cypher.GraphAccessTarget(s.dbManager, selection, allowed)
	if !selectable || remote || s.dbManager.IsCompositeDatabase(database) || database == "system" {
		s.writeNeo4jInsufficientPermissions(w, r)
		return
	}
	engine, err := s.dbManager.GetStorage(database)
	if err != nil {
		s.writeNeo4jNotFound(w, r, "Neo.ClientError.Database.DatabaseNotFound")
		return
	}
	var entityID string
	if kind == "4" {
		if node, err := engine.GetNode(r.Context(), storage.NodeID(localID)); err == nil {
			entityID = auth.FineGrainedEntityID(node.Identity().LocalID, node.CreatedAt)
		}
	} else if edge, err := engine.GetEdge(r.Context(), storage.EdgeID(localID)); err == nil {
		entityID = auth.FineGrainedEntityID(edge.Identity().LocalID, edge.CreatedAt)
	}
	if entityID == "" {
		s.writeNeo4jNotFound(w, r, "Neo.ClientError.Statement.EntityNotFound")
		return
	}
	databaseID, _ := s.dbManager.DatabaseIdentity(database)
	httpresponse.WritePropertyJSON(w, http.StatusOK, map[string]string{"databaseId": databaseID, "entityId": entityID})
}
