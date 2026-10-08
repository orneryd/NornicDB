package server

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/security"
)

// =============================================================================
// Router Setup
// =============================================================================

// routeSpec declares one authenticated endpoint: path, required permission
// and handler. registerRouteTable wires every entry through withAuth, so an
// endpoint can never be registered without its permission check.
type routeSpec struct {
	path    string
	perm    auth.Permission
	handler http.HandlerFunc
}

// registerRouteTable registers each authenticated endpoint on mux. The
// NornicDB, admin, GDPR and retention registrars are route tables over this
// helper, so the path/permission/handler shape cannot drift between them.
func (s *Server) registerRouteTable(mux *http.ServeMux, routes []routeSpec) {
	for _, route := range routes {
		mux.HandleFunc(route.path, s.withAuth(route.handler, route.perm))
	}
}

func (s *Server) buildRouter() http.Handler {
	mux := http.NewServeMux()

	uiHandler := s.registerUIRoutes(mux)
	s.registerNeo4jRoutes(mux, uiHandler)
	s.registerHealthRoutes(mux)
	s.registerAuthRoutes(mux)
	s.registerNornicDBRoutes(mux)
	s.registerAdminRoutes(mux)
	s.registerRetentionRoutes(mux)
	s.registerGDPRRoutes(mux)
	s.registerMCPRoutes(mux)
	s.registerHeimdallRoutes(mux)
	s.registerGraphQLRoutes(mux)

	return s.wrapWithMiddleware(mux)
}

func (s *Server) registerUIRoutes(mux *http.ServeMux) *uiHandler {
	// ==========================================================================
	// UI Browser (if enabled and not in headless mode)
	// ==========================================================================
	if s.config.Headless {
		s.logEvent(context.Background(), slog.LevelInfo, localization.ServerUIHeadlessEvent())
		return nil
	}

	uiHandler, uiErr := newUIHandler(s.config.BasePath)
	if uiErr != nil {
		s.logEvent(context.Background(), slog.LevelWarn, localization.ServerUIInitializationFailedEvent(uiErr))
		return nil
	}
	if uiHandler == nil {
		return nil
	}

	s.logEvent(context.Background(), slog.LevelInfo, localization.ServerUIEnabledEvent("/"))

	// Serve UI assets
	mux.Handle("/assets/", uiHandler)
	mux.HandleFunc("/favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		uiHandler.ServeHTTP(w, r)
	})
	mux.HandleFunc("/nornicdb.svg", func(w http.ResponseWriter, r *http.Request) {
		uiHandler.ServeHTTP(w, r)
	})

	// UI routes (SPA)
	mux.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		uiHandler.ServeHTTP(w, r)
	})
	mux.HandleFunc("/security", func(w http.ResponseWriter, r *http.Request) {
		uiHandler.ServeHTTP(w, r)
	})
	mux.HandleFunc("/security/knowledge-policies", func(w http.ResponseWriter, r *http.Request) {
		uiHandler.ServeHTTP(w, r)
	})

	// Auth config endpoint for UI
	mux.HandleFunc("/auth/config", s.handleAuthConfig)

	return uiHandler
}

func (s *Server) registerNeo4jRoutes(mux *http.ServeMux, uiHandler *uiHandler) {
	// ==========================================================================
	// Neo4j-Compatible Endpoints (for driver/browser compatibility)
	// ==========================================================================

	// Discovery endpoint (no auth required) - Neo4j compatible
	// Also serves UI for browser requests (unless headless)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Serve UI for browser requests (SPA) unless headless.
		// This enables deep links like /help or /security/admin to render correctly
		// instead of falling through to the Neo4j discovery JSON.
		if uiHandler != nil && isUIRequest(r) {
			uiHandler.ServeHTTP(w, r)
			return
		}
		// Otherwise serve Neo4j discovery JSON
		s.handleDiscovery(w, r)
	})

	// Neo4j HTTP API - Transaction endpoints (database-specific)
	// Pattern: /db/{databaseName}/tx/commit for implicit transactions
	// Pattern: /db/{databaseName}/tx for explicit transaction creation
	// Pattern: /db/{databaseName}/tx/{txId} for transaction operations
	// Pattern: /db/{databaseName}/tx/{txId}/commit for transaction commit
	mux.HandleFunc("/db/", s.withAuth(s.handleDatabaseEndpoint, auth.PermRead))
}

func (s *Server) registerHealthRoutes(mux *http.ServeMux) {
	// ==========================================================================
	// Health/Status/Metrics Endpoints
	// ==========================================================================
	// Health check is public (required for load balancers/k8s probes)
	mux.HandleFunc("/health", s.handleHealth)
	// Status and metrics require authentication to prevent information disclosure
	// These expose node counts, uptime, request stats that aid reconnaissance
	mux.HandleFunc("/status", s.withAuth(s.handleStatus, auth.PermRead))
	mux.HandleFunc("/metrics", s.withAuth(s.handleMetrics, auth.PermRead)) // Prometheus-compatible metrics
}

func (s *Server) registerAuthRoutes(mux *http.ServeMux) {
	// ==========================================================================
	// Authentication Endpoints (NornicDB additions)
	// ==========================================================================
	mux.HandleFunc("/auth/token", s.handleToken)
	mux.HandleFunc("/auth/logout", s.handleLogout)

	s.registerRouteTable(mux, []routeSpec{
		{"/auth/me", auth.PermRead, s.handleMe},
		{"/auth/password", auth.PermRead, s.handleChangePassword},     // Users can change their own password
		{"/auth/profile", auth.PermRead, s.handleUpdateProfile},       // Users can update their own profile
		{"/auth/api-token", auth.PermAdmin, s.handleGenerateAPIToken}, // Admin only - generate API tokens
	})

	// OAuth endpoints
	mux.HandleFunc("/auth/oauth/redirect", s.handleOAuthRedirect)
	mux.HandleFunc("/auth/oauth/callback", s.handleOAuthCallback)

	// User management, roles, database access and entitlements.
	s.registerRouteTable(mux, []routeSpec{
		{"/auth/users", auth.PermUserManage, s.handleUsers},
		{"/auth/users/", auth.PermUserManage, s.handleUserByID},
		{"/auth/roles", auth.PermAdmin, s.handleRoles},
		{"/auth/roles/", auth.PermAdmin, s.handleRoleByID},
		{"/auth/access/databases", auth.PermAdmin, s.handleAccessDatabases},
		{"/auth/access/privileges", auth.PermAdmin, s.handleAccessPrivileges},
		{"/auth/entitlements", auth.PermRead, s.handleEntitlements},
		{"/auth/role-entitlements", auth.PermAdmin, s.handleRoleEntitlements},
	})
}

func (s *Server) registerNornicDBRoutes(mux *http.ServeMux) {
	// ==========================================================================
	// NornicDB Extension Endpoints (additional features)
	// ==========================================================================
	s.registerRouteTable(mux, []routeSpec{
		// Vector search (NornicDB-specific)
		{"/nornicdb/search", auth.PermRead, s.handleSearch},
		{"/nornicdb/similar", auth.PermRead, s.handleSimilar},
		{"/nornicdb/graph/{database}/neighborhood", auth.PermRead, s.handleGraphNeighborhood},
		{"/nornicdb/graph/{database}/expand", auth.PermRead, s.handleGraphExpand},
		{"/nornicdb/graph/{database}/path", auth.PermRead, s.handleGraphPath},
		{"/nornicdb/graph/{database}/temporal", auth.PermRead, s.handleGraphTemporal},
		{"/nornicdb/graph/{database}/diff", auth.PermRead, s.handleGraphDiff},

		// Memory decay (NornicDB-specific)
		{"/nornicdb/decay", auth.PermRead, s.handleDecay},

		// Embedding control (NornicDB-specific)
		{"/nornicdb/embed/trigger", auth.PermWrite, s.handleEmbedTrigger},
		{"/nornicdb/embed/stats", auth.PermRead, s.handleEmbedStats},
		{"/nornicdb/embed/failures", auth.PermRead, s.handleEmbedFailures},
		{"/nornicdb/embed/retry-failures", auth.PermWrite, s.handleEmbedFailureRetry},
		{"/nornicdb/embed/clear", auth.PermAdmin, s.handleEmbedClear},
		{"/nornicdb/search/rebuild", auth.PermWrite, s.handleSearchRebuild},
	})
}

func (s *Server) registerAdminRoutes(mux *http.ServeMux) {
	// ==========================================================================
	// Admin endpoints (NornicDB-specific)
	// ==========================================================================
	s.registerRouteTable(mux, []routeSpec{
		{"/admin/stats", auth.PermAdmin, s.handleAdminStats},
		{"/admin/config", auth.PermAdmin, s.handleAdminConfig},
		{"/admin/backup", auth.PermAdmin, s.handleBackup},
		{"/admin/restore", auth.PermAdmin, s.handleRestore},

		// GPU control endpoints (NornicDB-specific)
		{"/admin/gpu/status", auth.PermAdmin, s.handleGPUStatus},
		{"/admin/gpu/enable", auth.PermAdmin, s.handleGPUEnable},
		{"/admin/gpu/disable", auth.PermAdmin, s.handleGPUDisable},
		{"/admin/gpu/test", auth.PermAdmin, s.handleGPUTest},

		// Per-database config overrides (admin only)
		{"/admin/databases/config/keys", auth.PermAdmin, s.handleDbConfigKeys},
		{"/admin/databases/", auth.PermAdmin, s.handleDbConfigPrefix},
	})
}

func (s *Server) registerGDPRRoutes(mux *http.ServeMux) {
	// ==========================================================================
	// GDPR compliance endpoints (NornicDB-specific)
	// ==========================================================================
	s.registerRouteTable(mux, []routeSpec{
		{"/gdpr/export", auth.PermRead, s.handleGDPRExport},
		{"/gdpr/delete", auth.PermDelete, s.handleGDPRDelete},
	})
}

func (s *Server) registerMCPRoutes(mux *http.ServeMux) {
	// ==========================================================================
	// MCP Tool Endpoints (LLM-native interface)
	// ==========================================================================
	// Register MCP routes on the same server (port 7474)
	// Routes: /mcp, /mcp/initialize, /mcp/tools/list, /mcp/tools/call, /mcp/health
	// plus database-pinned variants /mcp/{database}, /mcp/{database}/initialize,
	// /mcp/{database}/tools/list and /mcp/{database}/tools/call, where the URL
	// database becomes the tool's database argument and cannot be overridden by
	// the payload. All MCP endpoints require authentication (PermRead minimum).
	if s.mcpServer == nil {
		return
	}

	serveMCP := func(w http.ResponseWriter, r *http.Request) {
		r = s.withBifrostRBAC(r)
		s.mcpServer.ServeHTTP(w, r)
	}

	// Wrap MCP endpoints with auth - MCP is a powerful API that allows full DB access
	mux.HandleFunc("/mcp", s.withAuth(serveMCP, auth.PermRead))
	mux.HandleFunc("/mcp/initialize", s.withAuth(serveMCP, auth.PermRead))
	mux.HandleFunc("/mcp/tools/list", s.withAuth(serveMCP, auth.PermRead))
	mux.HandleFunc("/mcp/tools/call", s.withAuth(serveMCP, auth.PermRead))
	mux.HandleFunc("/mcp/health", s.handleHealth) // Health check can remain public

	// Database-pinned variants. ServeHTTP extracts the database from the path.
	mux.HandleFunc("/mcp/{database}", s.withAuth(serveMCP, auth.PermRead))
	mux.HandleFunc("/mcp/{database}/initialize", s.withAuth(serveMCP, auth.PermRead))
	mux.HandleFunc("/mcp/{database}/tools/list", s.withAuth(serveMCP, auth.PermRead))
	mux.HandleFunc("/mcp/{database}/tools/call", s.withAuth(serveMCP, auth.PermRead))
}

func (s *Server) registerHeimdallRoutes(mux *http.ServeMux) {
	// ==========================================================================
	// Heimdall AI Assistant Endpoints (Bifrost chat interface)
	// ==========================================================================
	// Routes: /api/bifrost/status, /api/bifrost/chat/completions, /api/bifrost/autocomplete, /api/bifrost/events
	// All Bifrost endpoints require authentication (PermRead minimum)
	serveHeimdall := func(w http.ResponseWriter, r *http.Request) {
		if s.config != nil && s.config.Features != nil && !s.config.Features.HeimdallEnabled {
			s.writeLocalizedError(w, r, http.StatusServiceUnavailable, localization.HeimdallDisabled(), nil)
			return
		}
		handler := s.getHeimdallHandler()
		if handler == nil {
			s.writeLocalizedError(w, r, http.StatusServiceUnavailable, localization.HeimdallInitializing(), nil)
			return
		}
		r = s.withBifrostRBAC(r)
		handler.ServeHTTP(w, r)
	}

	// Status endpoint - read access required
	mux.HandleFunc("/api/bifrost/status", s.withAuth(func(w http.ResponseWriter, r *http.Request) {
		serveHeimdall(w, r)
	}, auth.PermRead))
	// OpenAI-compatible models endpoint - read access required
	mux.HandleFunc("/v1/models", s.withAuth(func(w http.ResponseWriter, r *http.Request) {
		serveHeimdall(w, r)
	}, auth.PermRead))
	// Chat completions - write access required (modifies state/generates content)
	mux.HandleFunc("/api/bifrost/chat/completions", s.withAuth(func(w http.ResponseWriter, r *http.Request) {
		serveHeimdall(w, r)
	}, auth.PermWrite))
	// OpenAI-compatible chat completions alias - write access required
	mux.HandleFunc("/v1/chat/completions", s.withAuth(func(w http.ResponseWriter, r *http.Request) {
		serveHeimdall(w, r)
	}, auth.PermWrite))
	// Autocomplete - read access required (queries schema, generates suggestions)
	mux.HandleFunc("/api/bifrost/autocomplete", s.withAuth(func(w http.ResponseWriter, r *http.Request) {
		serveHeimdall(w, r)
	}, auth.PermRead))
	// SSE events - read access required
	mux.HandleFunc("/api/bifrost/events", s.withAuth(func(w http.ResponseWriter, r *http.Request) {
		serveHeimdall(w, r)
	}, auth.PermRead))
}

func (s *Server) registerGraphQLRoutes(mux *http.ServeMux) {
	// ==========================================================================
	// GraphQL API Endpoints
	// ==========================================================================
	// Routes: /graphql (query/mutation), /graphql/playground (GraphQL IDE)
	// GraphQL provides a flexible query language for accessing NornicDB
	if s.graphqlHandler == nil {
		return
	}

	// GraphQL endpoint - read access required; enrich request with RBAC for per-DB enforcement
	mux.HandleFunc("/graphql", s.withAuth(func(w http.ResponseWriter, r *http.Request) {
		r = s.withBifrostRBAC(r)
		if os.Getenv("NORNICDB_TRACE_GRAPHQL") != "" {
			start := time.Now()
			s.graphqlHandler.ServeHTTP(w, r)
			s.logEvent(r.Context(), slog.LevelDebug, localization.ServerGraphQLRequestEvent(r.Method, r.URL.Path, time.Since(start)))
			return
		}
		s.graphqlHandler.ServeHTTP(w, r)
	}, auth.PermRead))

	// GraphQL Playground - interactive IDE (read access required)
	if !s.config.Headless {
		mux.HandleFunc("/graphql/playground", s.withAuth(func(w http.ResponseWriter, r *http.Request) {
			r = s.withBifrostRBAC(r)
			s.graphqlHandler.Playground().ServeHTTP(w, r)
		}, auth.PermRead))
	}
	s.logEvent(context.Background(), slog.LevelInfo, localization.ServerGraphQLEnabledEvent("/graphql"))
}

func (s *Server) wrapWithMiddleware(next http.Handler) http.Handler {
	// Wrap with middleware (order matters: outermost runs first)
	// Security middleware validates all tokens, URLs, and headers FIRST
	securityMiddleware := security.NewSecurityMiddleware()
	securityMiddleware.SetLocalizer(s.localizer)
	handler := securityMiddleware.ValidateRequest(next)
	handler = s.corsMiddleware(handler)
	handler = s.rateLimitMiddleware(handler) // Rate limit after CORS preflight
	handler = s.requestTimeoutMiddleware(handler)
	handler = s.loggingMiddleware(handler)
	handler = s.recoveryMiddleware(handler)
	handler = s.metricsMiddleware(handler)
	handler = s.localizationMiddleware(handler)
	// Base path middleware runs FIRST (outermost) to strip prefix before routing
	handler = s.basePathMiddleware(handler)
	// Authenticate proxy metadata before base-path and other middleware consume it.
	handler = s.trustedProxyMiddleware(handler)

	return handler
}
