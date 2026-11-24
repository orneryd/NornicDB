package server

import (
	"context"
	"fmt"
	"net/http"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
)

func (s *Server) initializeFineGrainedRuntime(ctx context.Context) error {
	if !s.isRBACEnforced() || s.roleStore == nil {
		return fmt.Errorf("fine-grained access control requires authentication and the system role store")
	}
	actions := fineGrainedCatalogActions()
	catalog, revision, err := s.roleStore.PrepareFineGrainedCatalog(ctx)
	if err != nil {
		return err
	}
	if revision == "" {
		catalog = auth.DefaultFineGrainedCatalog(actions)
		if err := s.roleStore.ReplaceFineGrainedCatalog(ctx, revision, actions, catalog); err != nil {
			return err
		}
	}
	runtime := auth.NewFineGrainedRuntime(s.roleStore, actions, func(name string) string {
		resolved, err := s.dbManager.ResolveDatabase(name)
		if err != nil {
			return ""
		}
		id, _ := s.dbManager.DatabaseIdentity(resolved)
		return id
	}, s.dbManager.DefaultDatabaseName)
	runtime.SetAuthRuleCompilerContext(cypher.CompileFineGrainedAuthRuleConditionContext)
	if s.processConfig != nil {
		runtime.SetNativeTagAuthorization(s.processConfig.Auth.FGACNativeTags)
	}
	runtime.SetPrivateDatabaseOwner(func(name string) string {
		resolved, err := s.dbManager.ResolveDatabase(name)
		if err != nil {
			return ""
		}
		return s.dbManager.PrivateDatabaseOwner(resolved)
	})
	runtime.SetSecurityDirectory(s.auth, auth.RoleMutationStores{
		Allowlist: s.allowlistStore, Privileges: s.privilegesStore, Entitlements: s.roleEntitlementsStore, Users: s.auth,
	})
	if err := runtime.ValidateAdministratorContext(ctx, catalog); err != nil {
		return err
	}
	s.fineGrainedRuntime = runtime
	return nil
}

// FineGrainedAdmission returns the shared admission callback used by Bolt.
// A nil callback keeps the session on the existing RBAC principal.
func (s *Server) FineGrainedAdmission() func(context.Context, []string) (*auth.FineGrainedRequest, error) {
	if s.fineGrainedRuntime == nil {
		return nil
	}
	return s.fineGrainedRuntime.Admit
}

func (s *Server) principalAccessForRequest(ctx context.Context, claims *auth.JWTClaims) cypher.PrincipalAccess {
	if request := auth.FineGrainedRequestFromContext(ctx); request != nil {
		return cypher.FineGrainedCypherAccess(s.dbManager, request)
	}
	return s.principalAccess(claims)
}

func (s *Server) canAccessGraphForRequest(r *http.Request, graph string) bool {
	return s.principalAccessForRequest(r.Context(), getClaims(r)).Allows(graph, "access")
}

func fineGrainedServerAction(route string) string {
	switch route {
	case "/admin/stats":
		return "dbms.admin.stats"
	case "/admin/config":
		return "dbms.admin.config"
	case "/admin/backup":
		return "dbms.backup.create"
	case "/admin/restore":
		return "dbms.backup.restore"
	case "/admin/databases/imports/publish":
		return publishPreparedDatabaseAction
	case "/admin/gpu/status":
		return "dbms.gpu.status"
	case "/admin/gpu/enable":
		return "dbms.gpu.enable"
	case "/admin/gpu/disable":
		return "dbms.gpu.disable"
	case "/admin/gpu/test":
		return "dbms.gpu.test"
	default:
		return ""
	}
}

func fineGrainedHTTPAllowed(r *http.Request, request *auth.FineGrainedRequest) bool {
	if action := fineGrainedServerAction(r.Pattern); action != "" {
		return request.AllowsDBMS(action, "")
	}
	switch r.Pattern {
	case "/auth/fgac/effective", "/auth/me", "/auth/password", "/auth/profile", "/auth/api-token", "/auth/logout",
		"/admin/retention/policies", "/admin/retention/policies/{id}", "/admin/retention/policies/defaults",
		"/admin/retention/holds", "/admin/retention/holds/{id}", "/admin/retention/erasures",
		"/admin/retention/erasures/{id}/process", "/admin/retention/sweep", "/admin/retention/status",
		"/gdpr/export", "/gdpr/delete", "/nornicdb/decay",
		"/db/", "/db/{database}/query/v2", "/db/{database}/query/v2/tx",
		"/db/{database}/query/v2/tx/{transaction}", "/db/{database}/query/v2/tx/{transaction}/commit", "/nornicdb/embed/trigger", "/nornicdb/embed/clear", "/nornicdb/embed/stats",
		"/nornicdb/embed/failures", "/nornicdb/embed/retry-failures", "/admin/databases/",
		"/nornicdb/search", "/nornicdb/similar", "/nornicdb/search/rebuild",
		"/nornicdb/graph/{database}/neighborhood", "/nornicdb/graph/{database}/expand",
		"/nornicdb/graph/{database}/path", "/nornicdb/graph/{database}/temporal", "/nornicdb/graph/{database}/diff":
		return true
	case "/status":
		return request.AllowsDBMS("dbms.service.status.read", "")
	case "/metrics":
		return request.AllowsDBMS("dbms.service.metrics.read", "")
	case "/graphql", "/graphql/playground":
		return request.AllowsDBMS("dbms.api.graphql", "")
	case "/mcp", "/mcp/initialize", "/mcp/tools/list", "/mcp/tools/call",
		"/mcp/{database}", "/mcp/{database}/initialize", "/mcp/{database}/tools/list", "/mcp/{database}/tools/call":
		return request.AllowsDBMS("dbms.api.mcp", "")
	case "/v1/models":
		return request.AllowsDBMS("dbms.heimdall.models.list", "")
	case "/api/bifrost/status":
		return request.AllowsDBMS("dbms.heimdall.status.read", "")
	case "/api/bifrost/events":
		return request.AllowsDBMS("dbms.heimdall.events.read", "")
	case "/api/bifrost/chat/completions", "/v1/chat/completions", "/api/bifrost/autocomplete":
		return request.AllowsDBMS("dbms.heimdall.chat", "")
	case "/api/bifrost/workspace", "/api/bifrost/conversations", "/api/bifrost/conversations/":
		return request.AllowsDBMS("dbms.heimdall.conversations", "")
	case "/auth/fgac/catalog", "/auth/fgac/entity", "/auth/entitlements", "/auth/access/databases", "/auth/access/privileges",
		"/auth/role-entitlements", "/admin/databases/config/keys":
		return request.AllowsDBMS("dbms.security.policy.manage", "")
	case "/auth/roles", "/auth/roles/":
		switch r.Method {
		case http.MethodGet:
			return request.AllowsDBMS("dbms.role.show", "")
		case http.MethodPost:
			return request.AllowsDBMS("dbms.role.create", "")
		case http.MethodPut, http.MethodPatch:
			return request.AllowsDBMS("dbms.role.rename", "")
		case http.MethodDelete:
			return request.AllowsDBMS("dbms.role.drop", "")
		}
	case "/auth/users", "/auth/users/":
		switch r.Method {
		case http.MethodGet:
			return request.AllowsDBMS("dbms.user.show", "")
		case http.MethodPost:
			return request.AllowsDBMS("dbms.user.create", "")
		case http.MethodPut, http.MethodPatch:
			return request.AllowsDBMS("dbms.user.alter", "") &&
				request.AllowsDBMS("dbms.role.assign", "") && request.AllowsDBMS("dbms.role.remove", "")
		case http.MethodDelete:
			return request.AllowsDBMS("dbms.user.drop", "")
		}
	}
	return false
}
