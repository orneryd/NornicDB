package server

import (
	"errors"
	"io"
	"net/http"
	"slices"

	"github.com/orneryd/nornicdb/internal/httpresponse"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/mcp"
)

type fineGrainedCatalogResponse struct {
	Catalog  auth.FineGrainedCatalog  `json:"catalog"`
	Revision string                   `json:"revision"`
	Actions  []auth.FineGrainedAction `json:"actions"`
	Enforced bool                     `json:"enforced"`
	Roles    []string                 `json:"roles"`
}

// handleFineGrainedCatalog edits or reads the active entitlement catalog.
func (s *Server) handleFineGrainedCatalog(w http.ResponseWriter, r *http.Request) {
	claims := getClaims(r)
	if claims == nil {
		s.writeNeo4jNoAuthenticationProvided(w, r)
		return
	}
	runtime := s.fineGrainedRuntime
	if runtime == nil {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		catalog, revision, err := runtime.ReadCatalog(r.Context())
		if err != nil {
			s.writeInternalServerError(w, r)
			return
		}
		httpresponse.WritePropertyJSON(w, http.StatusOK, fineGrainedCatalogResponse{
			Catalog: catalog, Revision: revision, Actions: runtime.Actions(), Enforced: true, Roles: runtime.RoleNames(),
		})
	case http.MethodPut:
		var body struct {
			Catalog  *auth.FineGrainedCatalog `json:"catalog"`
			Revision *string                  `json:"revision"`
		}
		decoder := convert.CypherInputJSON.NewDecoder(http.MaxBytesReader(w, r.Body, s.config.MaxRequestSize))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&body); err != nil || body.Catalog == nil || body.Revision == nil ||
			decoder.Decode(new(any)) != io.EOF {
			s.writeNeo4jInvalidRequestBody(w, r, "Neo.ClientError.Request.InvalidFormat")
			return
		}

		current, currentRevision, err := runtime.ReadCatalog(r.Context())
		if err == nil && *body.Revision != currentRevision {
			err = auth.ErrFineGrainedCatalogChanged
		}
		if err == nil {
			body.Catalog.Privileges = current.Privileges
			err = runtime.ReplaceCatalog(r.Context(), *body.Revision, *body.Catalog)
		}
		switch {
		case err == nil:
			s.logAudit(r, claims.Sub, "fgac_catalog_replace", true, "fine-grained entitlement catalog replaced")
			w.WriteHeader(http.StatusNoContent)
		case errors.Is(err, auth.ErrFineGrainedCatalogChanged):
			s.writeBoundaryNeo4jError(w, r, http.StatusConflict, "Neo.TransientError.Transaction.Conflict", err)
		case errors.Is(err, auth.ErrInvalidFineGrainedPolicy), errors.Is(err, auth.ErrRoleNotFound):
			s.writeBoundaryNeo4jError(w, r, http.StatusBadRequest, "Neo.ClientError.Request.InvalidFormat", err)
		default:
			s.writeInternalServerError(w, r)
		}
	default:
		s.writeGetOrPutRequired(w, r)
	}
}

func fineGrainedCatalogActions() []auth.FineGrainedAction {
	actions := auth.FineGrainedCoreActions()
	for _, id := range []string{
		"embedding.status.read", "embedding.failures.list", "embedding.failures.retry",
		"embedding.trigger", "embedding.regenerate", "embedding.clear",
		"database.mvcc.status", "database.mvcc.debt", "database.mvcc.prune",
		"database.mvcc.pause", "database.mvcc.resume", "database.mvcc.schedule",
		"retention.policy.list", "retention.policy.read", "retention.policy.create", "retention.policy.update", "retention.policy.delete",
		"retention.defaults.update",
		"retention.hold.list", "retention.hold.create", "retention.hold.delete",
		"retention.erasure.list", "retention.erasure.create", "retention.erasure.process",
		"retention.sweep", "retention.status",
		"embedding.config.read", "database.config.read", "database.search.execute", "database.search.rebuild", "database.stats.read", "database.private.access",
		"knowledge.info", "knowledge.profiles.list", "knowledge.resolve", "knowledge.deindex.status", "knowledge.decay.info",
		"knowledge.decay_profile.list", "knowledge.decay_profile.create",
		"knowledge.decay_profile.update", "knowledge.decay_profile.delete",
		"knowledge.decay_profile.bind", "knowledge.decay_profile.binding.update",
		"knowledge.promotion_profile.list", "knowledge.promotion_profile.create",
		"knowledge.promotion_profile.update", "knowledge.promotion_profile.delete",
		"knowledge.promotion_policy.list", "knowledge.promotion_policy.create",
		"knowledge.promotion_policy.update", "knowledge.promotion_policy.definition.update",
		"knowledge.promotion_policy.delete",
	} {
		actions = append(actions, auth.FineGrainedAction{ID: id, ResourceKind: auth.FineGrainedDatabase})
	}
	for _, id := range []string{
		"embedding.provider.update", "embedding.model.update", "embedding.worker.settings.update", "database.config.update",
	} {
		actions = append(actions, auth.FineGrainedAction{ID: id, ResourceKind: auth.FineGrainedDatabase, Property: true})
	}
	for _, id := range []string{
		"dbms.api.graphql", "dbms.api.mcp", "dbms.heimdall.status.read",
		"dbms.heimdall.chat", "dbms.heimdall.actions.discover", "dbms.heimdall.models.list", "dbms.heimdall.events.read", "dbms.heimdall.conversations",
		"dbms.service.status.read", "dbms.service.metrics.read",
		"dbms.admin.stats", "dbms.admin.config", "dbms.backup.create", "dbms.backup.restore",
		"dbms.gpu.status", "dbms.gpu.enable", "dbms.gpu.disable", "dbms.gpu.test",
		"dbms.gdpr.self.export", "dbms.gdpr.self.delete", "dbms.gdpr.user.export", "dbms.gdpr.user.delete",
	} {
		actions = append(actions, auth.FineGrainedAction{ID: id, ResourceKind: auth.FineGrainedDBMS})
	}
	for _, name := range []string{mcp.ToolStore, mcp.ToolRecall, mcp.ToolDiscover, mcp.ToolLink, mcp.ToolTasks} {
		actions = append(actions, auth.FineGrainedAction{ID: "mcp.invoke." + name, ResourceKind: auth.FineGrainedDatabase})
	}

	names := heimdall.ListHeimdallActions()
	slices.Sort(names)
	for _, name := range names {
		actions = append(actions, auth.FineGrainedAction{
			ID: "heimdall.invoke." + name, ResourceKind: auth.FineGrainedDatabase,
		})
	}
	return actions
}
