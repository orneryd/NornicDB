package server

import (
	"net/http"

	// _ "net/http/pprof" // Register pprof handlers
	"os"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/security"
)

// =============================================================================
// Router Setup
// =============================================================================

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
	// s.registerDebugRoutes(mux) // Pprof endpoints

	return s.wrapWithMiddleware(mux)
}

func (s *Server) registerUIRoutes(mux *http.ServeMux) *uiHandler {
	// ==========================================================================
	// UI Browser (if enabled and not in headless mode)
	// ==========================================================================
	if s.config.Headless {
		s.log.Info("headless mode: UI disabled")
		return nil
	}

	SetUIBasePath(s.config.BasePath)
	uiHandler, uiErr := newUIHandler()
	if uiErr != nil {
		s.log.Warn("UI initialization failed", "error", uiErr)
		return nil
	}
	if uiHandler == nil {
		return nil
	}

	s.log.Info("UI browser enabled", "route", "/")

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
	mux.HandleFunc("/auth/me", s.withAuth(s.handleMe, auth.PermRead))
	mux.HandleFunc("/auth/password", s.withAuth(s.handleChangePassword, auth.PermRead))     // Users can change their own password
	mux.HandleFunc("/auth/profile", s.withAuth(s.handleUpdateProfile, auth.PermRead))       // Users can update their own profile
	mux.HandleFunc("/auth/api-token", s.withAuth(s.handleGenerateAPIToken, auth.PermAdmin)) // Admin only - generate API tokens

	// OAuth endpoints
	mux.HandleFunc("/auth/oauth/redirect", s.handleOAuthRedirect)
	mux.HandleFunc("/auth/oauth/callback", s.handleOAuthCallback)

	// User management (admin only)
	mux.HandleFunc("/auth/users", s.withAuth(s.handleUsers, auth.PermUserManage))
	mux.HandleFunc("/auth/users/", s.withAuth(s.handleUserByID, auth.PermUserManage))

	// User-defined roles (admin only)
	mux.HandleFunc("/auth/roles", s.withAuth(s.handleRoles, auth.PermAdmin))
	mux.HandleFunc("/auth/roles/", s.withAuth(s.handleRoleByID, auth.PermAdmin))

	// Per-database access allowlist (admin only, Phase 3 RBAC)
	mux.HandleFunc("/auth/access/databases", s.withAuth(s.handleAccessDatabases, auth.PermAdmin))
	// Per-database read/write privileges (admin only, Phase 4 RBAC)
	mux.HandleFunc("/auth/access/privileges", s.withAuth(s.handleAccessPrivileges, auth.PermAdmin))
	// Canonical list of entitlements (for UI and docs); read access sufficient
	mux.HandleFunc("/auth/entitlements", s.withAuth(s.handleEntitlements, auth.PermRead))
	// Per-role global entitlements (admin only); GET returns role→entitlements, PUT sets one role's entitlements
	mux.HandleFunc("/auth/role-entitlements", s.withAuth(s.handleRoleEntitlements, auth.PermAdmin))
}

func (s *Server) registerNornicDBRoutes(mux *http.ServeMux) {
	// ==========================================================================
	// NornicDB Extension Endpoints (additional features)
	// ==========================================================================

	// Vector search (NornicDB-specific)
	mux.HandleFunc("/nornicdb/search", s.withAuth(s.handleSearch, auth.PermRead))
	mux.HandleFunc("/nornicdb/similar", s.withAuth(s.handleSimilar, auth.PermRead))
	mux.HandleFunc("/nornicdb/graph/{database}/neighborhood", s.withAuth(s.handleGraphNeighborhood, auth.PermRead))
	mux.HandleFunc("/nornicdb/graph/{database}/expand", s.withAuth(s.handleGraphExpand, auth.PermRead))
	mux.HandleFunc("/nornicdb/graph/{database}/path", s.withAuth(s.handleGraphPath, auth.PermRead))
	mux.HandleFunc("/nornicdb/graph/{database}/temporal", s.withAuth(s.handleGraphTemporal, auth.PermRead))
	mux.HandleFunc("/nornicdb/graph/{database}/diff", s.withAuth(s.handleGraphDiff, auth.PermRead))

	// Memory decay (NornicDB-specific)
	mux.HandleFunc("/nornicdb/decay", s.withAuth(s.handleDecay, auth.PermRead))

	// Embedding control (NornicDB-specific)
	mux.HandleFunc("/nornicdb/embed/trigger", s.withAuth(s.handleEmbedTrigger, auth.PermWrite))
	mux.HandleFunc("/nornicdb/embed/stats", s.withAuth(s.handleEmbedStats, auth.PermRead))
	mux.HandleFunc("/nornicdb/embed/clear", s.withAuth(s.handleEmbedClear, auth.PermAdmin))
	mux.HandleFunc("/nornicdb/search/rebuild", s.withAuth(s.handleSearchRebuild, auth.PermWrite))
}

func (s *Server) registerAdminRoutes(mux *http.ServeMux) {
	// ==========================================================================
	// Admin endpoints (NornicDB-specific)
	// ==========================================================================
	mux.HandleFunc("/admin/stats", s.withAuth(s.handleAdminStats, auth.PermAdmin))
	mux.HandleFunc("/admin/config", s.withAuth(s.handleAdminConfig, auth.PermAdmin))
	mux.HandleFunc("/admin/backup", s.withAuth(s.handleBackup, auth.PermAdmin))

	// GPU control endpoints (NornicDB-specific)
	mux.HandleFunc("/admin/gpu/status", s.withAuth(s.handleGPUStatus, auth.PermAdmin))
	mux.HandleFunc("/admin/gpu/enable", s.withAuth(s.handleGPUEnable, auth.PermAdmin))
	mux.HandleFunc("/admin/gpu/disable", s.withAuth(s.handleGPUDisable, auth.PermAdmin))
	mux.HandleFunc("/admin/gpu/test", s.withAuth(s.handleGPUTest, auth.PermAdmin))

	// Per-database config overrides (admin only)
	mux.HandleFunc("/admin/databases/config/keys", s.withAuth(s.handleDbConfigKeys, auth.PermAdmin))
	mux.HandleFunc("/admin/databases/", s.withAuth(s.handleDbConfigPrefix, auth.PermAdmin))
}

func (s *Server) registerGDPRRoutes(mux *http.ServeMux) {
	// ==========================================================================
	// GDPR compliance endpoints (NornicDB-specific)
	// ==========================================================================
	mux.HandleFunc("/gdpr/export", s.withAuth(s.handleGDPRExport, auth.PermRead))
	mux.HandleFunc("/gdpr/delete", s.withAuth(s.handleGDPRDelete, auth.PermDelete))
}

func (s *Server) registerMCPRoutes(mux *http.ServeMux) {
	// ==========================================================================
	// MCP Tool Endpoints (LLM-native interface)
	// ==========================================================================
	// Register MCP routes on the same server (port 7474)
	// Routes: /mcp, /mcp/initialize, /mcp/tools/list, /mcp/tools/call, /mcp/health
	// All MCP endpoints require authentication (PermRead minimum for tool calls)
	if s.mcpServer == nil {
		return
	}

	// Wrap MCP endpoints with auth - MCP is a powerful API that allows full DB access
	mux.HandleFunc("/mcp", s.withAuth(func(w http.ResponseWriter, r *http.Request) {
		s.mcpServer.ServeHTTP(w, r)
	}, auth.PermWrite))
	mux.HandleFunc("/mcp/initialize", s.withAuth(func(w http.ResponseWriter, r *http.Request) {
		s.mcpServer.ServeHTTP(w, r)
	}, auth.PermRead))
	mux.HandleFunc("/mcp/tools/list", s.withAuth(func(w http.ResponseWriter, r *http.Request) {
		s.mcpServer.ServeHTTP(w, r)
	}, auth.PermRead))
	mux.HandleFunc("/mcp/tools/call", s.withAuth(func(w http.ResponseWriter, r *http.Request) {
		s.mcpServer.ServeHTTP(w, r)
	}, auth.PermWrite))
	mux.HandleFunc("/mcp/health", s.handleHealth) // Health check can remain public
}

func (s *Server) registerHeimdallRoutes(mux *http.ServeMux) {
	// ==========================================================================
	// Heimdall AI Assistant Endpoints (Bifrost chat interface)
	// ==========================================================================
	// Routes: /api/bifrost/status, /api/bifrost/chat/completions, /api/bifrost/autocomplete, /api/bifrost/events
	// All Bifrost endpoints require authentication (PermRead minimum)
	serveHeimdall := func(w http.ResponseWriter, r *http.Request) {
		if s.config != nil && s.config.Features != nil && !s.config.Features.HeimdallEnabled {
			s.writeError(w, http.StatusServiceUnavailable, "Heimdall is disabled by configuration", nil)
			return
		}
		handler := s.getHeimdallHandler()
		if handler == nil {
			s.writeError(w, http.StatusServiceUnavailable, "Heimdall is initializing, please try again shortly", nil)
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
			s.log.Debug("graphql request",
				"subsystem", "graphql",
				"method", r.Method,
				"path", r.URL.Path,
				"duration", time.Since(start),
			)
			return
		}
		s.graphqlHandler.ServeHTTP(w, r)
	}, auth.PermRead))

	// GraphQL Playground - interactive IDE (read access required)
	mux.HandleFunc("/graphql/playground", s.withAuth(func(w http.ResponseWriter, r *http.Request) {
		r = s.withBifrostRBAC(r)
		s.graphqlHandler.Playground().ServeHTTP(w, r)
	}, auth.PermRead))
	s.log.Info("graphql API enabled", "route", "/graphql")
}

// registerDebugRoutes registers pprof profiling endpoints
// func (s *Server) registerDebugRoutes(mux *http.ServeMux) {
// 	// ==========================================================================
// 	// Debug/Profiling Endpoints (development/testing only)
// 	// ==========================================================================
// 	if !s.config.EnablePprof {
// 		return
// 	}
//
// 	// Register pprof handlers at /debug/pprof/*
// 	// These are provided by net/http/pprof package
// 	mux.HandleFunc("/debug/pprof/", func(w http.ResponseWriter, r *http.Request) {
// 		http.DefaultServeMux.ServeHTTP(w, r)
// 	})
// 	mux.HandleFunc("/debug/pprof/cmdline", func(w http.ResponseWriter, r *http.Request) {
// 		http.DefaultServeMux.ServeHTTP(w, r)
// 	})
// 	mux.HandleFunc("/debug/pprof/profile", func(w http.ResponseWriter, r *http.Request) {
// 		http.DefaultServeMux.ServeHTTP(w, r)
// 	})
// 	mux.HandleFunc("/debug/pprof/symbol", func(w http.ResponseWriter, r *http.Request) {
// 		http.DefaultServeMux.ServeHTTP(w, r)
// 	})
// 	mux.HandleFunc("/debug/pprof/trace", func(w http.ResponseWriter, r *http.Request) {
// 		http.DefaultServeMux.ServeHTTP(w, r)
// 	})
//
// 	// (would log: pprof profiling enabled at /debug/pprof/)
// }

func (s *Server) wrapWithMiddleware(next http.Handler) http.Handler {
	// Wrap with middleware (order matters: outermost runs first)
	// Security middleware validates all tokens, URLs, and headers FIRST
	securityMiddleware := security.NewSecurityMiddleware()
	handler := securityMiddleware.ValidateRequest(next)
	handler = s.corsMiddleware(handler)
	handler = s.rateLimitMiddleware(handler) // Rate limit after CORS preflight
	handler = s.requestTimeoutMiddleware(handler)
	handler = s.loggingMiddleware(handler)
	handler = s.recoveryMiddleware(handler)
	handler = s.metricsMiddleware(handler)
	// Base path middleware runs FIRST (outermost) to strip prefix before routing
	handler = s.basePathMiddleware(handler)

	return handler
}
