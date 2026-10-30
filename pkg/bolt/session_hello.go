package bolt

import (
	"context"
	"errors"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/localization"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"log/slog"
	"strings"
)

// handleHello handles the HELLO message with authentication.
// Neo4j HELLO message format:
//
//	HELLO { user_agent: String, scheme: String, principal: String, credentials: String, ... }
//
// Authentication schemes:
//   - "none": No-auth access when auth is disabled, or implicit WebSocket bearer
//   - "basic": Username/password authentication
//   - "bearer": JWT token authentication (credentials contains the token)
//
// For cluster authentication with shared JWT:
//   - All nodes share the same JWT secret (NORNICDB_JWT_SECRET)
//   - Generate a cluster token: POST /api/v1/auth/cluster-token
//   - Connect using bearer scheme with the token as credentials
//
// Server-to-server clustering uses the same auth mechanism with service accounts.
func (s *Session) handleHello(data []byte) error {
	defer s.publishConnectionListing()
	s.utcPatchNegotiated = false
	// Plan 04-02 D-11 / D-05e auth-attempts crosswire: observe a single
	// auth attempt per HELLO message at the function-exit chokepoint.
	// Result enum closed (success | failure | denied):
	//   - success: handshake completed and session.authenticated = true
	//   - failure: auth credentials rejected by the authenticator
	//   - denied:  request lacked auth when RequireAuth was true (or
	//              an unsupported scheme was offered).
	// No-op until Plan 04-06 wires the AuthMetrics bag.
	wasAuthenticatedBefore := s.authenticated
	defer func() {
		if s.server == nil {
			return
		}
		var result string
		switch {
		case s.authenticated && !wasAuthenticatedBefore:
			result = "success"
		case !s.authenticated:
			// Either auth failed (failure) or auth was required but
			// rejected the request before any credentials could
			// validate (denied). The two are bucketed as "failure" /
			// "denied" by the Phase 5 K8s-side aggregation; the Bolt
			// path classifies based on whether the failure path took a
			// scheme decision (failure) vs. a require-auth gate (denied).
			// Keep the call site simple: any non-success outcome is
			// "failure"; "denied" is reserved for explicit rejection
			// without credential evaluation.
			result = "failure"
		default:
			// Already authenticated (re-HELLO) — no new attempt.
			return
		}
		s.server.observeAuthAttempt(result)
	}()

	// Parse HELLO message to extract authentication details
	authParams, err := s.parseHelloAuth(data)
	if err != nil {
		if errors.Is(err, errDateTimeSignature) {
			return err
		}
		return s.sendFailure("Neo.ClientError.Request.Invalid", fmt.Sprintf("Failed to parse HELLO: %v", err))
	}

	// Check if authentication is required
	if s.server != nil && s.server.config.Authenticator != nil {
		scheme := authParams["scheme"]
		principal := authParams["principal"]
		credentials := authParams["credentials"]

		// Handle anonymous auth
		if scheme == "none" || scheme == "" {
			// Cookie-as-implicit-bearer: if the WS upgrade carried a
			// nornicdb_token cookie that validated, treat scheme=none as
			// scheme=bearer with that token. This lets first-party browser
			// clients (the NornicDB UI) connect via neo4j.auth.none() and
			// have the same-origin cookie carry the credential. HELLO
			// bearer always wins over the cookie because we only enter
			// this branch when the driver explicitly chose scheme=none.
			s.server.logEvent(context.Background(), slog.LevelDebug,
				localization.BoltHelloSchemeNoneEvent(s.implicitBearer != "", s.server.config.AllowAnonymous))
			cookieAuthed := false
			if s.implicitBearer != "" {
				result, err := s.server.config.Authenticator.Authenticate("bearer", "", s.implicitBearer)
				if err == nil {
					s.authenticated = true
					s.authResult = result
					s.forwardedAuthHeader = "Bearer " + s.implicitBearer
					cookieAuthed = true
				} else {
					// Logging at warn keeps the operator-visible signal
					// without leaking the token; we do NOT short-circuit
					// to a failure response — the driver still has the
					// AllowAnonymous fallback below.
					remoteAddr := "unknown"
					if s.conn != nil {
						remoteAddr = s.conn.RemoteAddr().String()
					}
					s.server.logEvent(context.Background(), slog.LevelWarn,
						localization.BoltWebSocketCookieBearerRejectedEvent(remoteAddr, err))
				}
			}
			if !cookieAuthed {
				return s.sendLocalizedFailure("Neo.ClientError.Security.Unauthorized", localization.BoltAuthenticationRequired())
			}
		} else if scheme == "basic" {
			// Authenticate with provided credentials
			result, err := s.server.config.Authenticator.Authenticate(scheme, principal, credentials)
			if err != nil {
				remoteAddr := "unknown"
				if s.conn != nil {
					remoteAddr = s.conn.RemoteAddr().String()
				}
				s.server.logEvent(context.Background(), slog.LevelWarn,
					localization.BoltBasicAuthenticationFailedEvent(principal, remoteAddr, err))
				return s.sendLocalizedFailure("Neo.ClientError.Security.Unauthorized", localization.BoltInvalidCredentials())
			}
			s.authenticated = true
			s.authResult = result
			s.forwardedAuthHeader = ""
		} else if scheme == "bearer" {
			// JWT token authentication - used for cluster inter-node auth
			result, err := s.server.config.Authenticator.Authenticate(scheme, principal, credentials)
			if err != nil {
				remoteAddr := "unknown"
				if s.conn != nil {
					remoteAddr = s.conn.RemoteAddr().String()
				}
				s.server.logEvent(context.Background(), slog.LevelWarn,
					localization.BoltBearerAuthenticationFailedEvent(remoteAddr, err))
				return s.sendLocalizedFailure("Neo.ClientError.Security.Unauthorized", localization.BoltInvalidOrExpiredToken())
			}
			s.authenticated = true
			s.authResult = result
			if strings.TrimSpace(credentials) != "" {
				s.forwardedAuthHeader = "Bearer " + credentials
			}
		} else {
			return s.sendLocalizedFailure("Neo.ClientError.Security.Unauthorized", localization.BoltUnsupportedAuthScheme(scheme))
		}
	} else if s.server != nil && s.server.config.RequireAuth {
		// Auth required but no authenticator configured - reject all
		return s.sendLocalizedFailure("Neo.ClientError.Security.Unauthorized", localization.BoltAuthenticationNotConfigured())
	} else {
		// No auth configured - allow all (development mode, canonical role from auth)
		s.authenticated = true
		s.authResult = &BoltAuthResult{
			Authenticated: true,
			Username:      "anonymous",
			PrincipalID:   auth.PrincipalID(nil),
			Roles:         []string{string(auth.RoleAdmin)},
		}
		s.forwardedAuthHeader = ""
	}

	// Extract and validate database name
	dbName := authParams["database"]
	if dbName == "" && s.server != nil && s.server.dbManager != nil {
		// Use default database if not specified
		dbName = s.server.dbManager.DefaultDatabaseName()
	}

	// A database the client names: access first, so a hidden one answers as an
	// absent one (databaseDenied).
	if authParams["database"] != "" && s.databaseDenied(dbName) {
		return s.sendLocalizedFailure("Neo.ClientError.Security.Forbidden", localization.DatabaseAccessDenied(dbName))
	}

	// Validate database exists (if dbManager is configured).
	// Use constituentAwareExists to accept dotted composite.alias references.
	if dbName != "" && s.server != nil && s.server.dbManager != nil {
		if !constituentAwareExists(s.server.dbManager, dbName) {
			return s.sendLocalizedFailure("Neo.ClientError.Database.DatabaseNotFound", localization.DatabaseNotFound(dbName))
		}
	}
	if err := s.bindDatabaseConnection(dbName); err != nil {
		return s.sendFailure("Neo.TransientError.General.DatabaseUnavailable", err.Error())
	}

	// Store database for this session
	s.database = dbName

	// SEC-03 / TRC-13: enrich session span with auth outcome (never credentials).
	if s.spanCtx != nil {
		if span := trace.SpanFromContext(s.spanCtx); span.IsRecording() {
			span.SetAttributes(
				attribute.String("bolt.user", s.authResult.Username),
				attribute.String("bolt.database", dbName),
			)
		}
	}

	if s.server != nil && s.server.config.LogQueries {
		remoteAddr := "unknown"
		if s.conn != nil {
			remoteAddr = s.conn.RemoteAddr().String()
		}
		// D-10a: drop the "[BOLT]" bracket — component=bolt is already
		// baked in via .With at ctor time. D-03a: any "credentials" attr
		// would be auto-redacted by the redactingHandler chain (it is in
		// DefaultRedactKeys); we deliberately do NOT log credentials
		// here, but the redaction guard remains in force as a defense
		// in depth.
		s.server.logEvent(context.Background(), slog.LevelInfo,
			localization.BoltHelloEvent(remoteAddr, s.authResult.Username, s.authResult.Roles, dbName))
	}

	return s.sendSuccess(map[string]any{
		"server":        s.server.config.serverAnnouncement(),
		"connection_id": "nornic-1",
		"hints":         map[string]any{},
		"patch_bolt":    []any{"utc"},
	})
}

func (s *Session) bindDatabaseConnection(databaseName string) error {
	if s == nil || s.server == nil || databaseName == "" || s.server.databaseConnectionAdmit == nil {
		return nil
	}
	owner := databaseName
	if s.server.databaseConnectionResolve != nil {
		resolved, err := s.server.databaseConnectionResolve(databaseName)
		if err != nil {
			return err
		}
		owner = resolved
	}
	if owner == s.admittedDatabase {
		return nil
	}
	if err := s.server.databaseConnectionAdmit(owner); err != nil {
		return err
	}
	previous := s.admittedDatabase
	s.admittedDatabase = owner
	if previous != "" && s.server.databaseConnectionRelease != nil {
		s.server.databaseConnectionRelease(previous)
	}
	return nil
}

func (s *Session) releaseDatabaseConnection() {
	if s == nil || s.server == nil || s.admittedDatabase == "" {
		return
	}
	databaseName := s.admittedDatabase
	s.admittedDatabase = ""
	if s.server.databaseConnectionRelease != nil {
		s.server.databaseConnectionRelease(databaseName)
	}
}

// parseHelloAuth parses authentication parameters and database from a HELLO message.
// Returns a map with keys: scheme, principal, credentials, database
func (s *Session) parseHelloAuth(data []byte) (map[string]string, error) {
	result := map[string]string{
		"scheme":      "",
		"principal":   "",
		"credentials": "",
		"database":    "",
	}

	if len(data) == 0 {
		return result, nil
	}

	// HELLO is a structure: [extra: Map]
	// First byte is marker for structure
	marker := data[0]

	// Check for tiny struct marker (0xB0-0xBF = struct with 0-15 fields)
	// HELLO has signature 0x01 and one field (the extra map)
	offset := 0
	if marker >= 0xB0 && marker <= 0xBF {
		offset = 2 // Skip struct marker and signature byte
	} else {
		// Try to find the map directly
		offset = 0
	}

	if offset >= len(data) {
		return result, nil
	}

	// Parse the extra map
	extraMap, _, err := decodePackStreamMap(data, offset, s.useUTCDateTimeStructs())
	if err != nil {
		return result, fmt.Errorf("failed to decode HELLO extra map: %w", err)
	}

	// Extract auth fields
	if scheme, ok := extraMap["scheme"].(string); ok {
		result["scheme"] = scheme
	}
	if principal, ok := extraMap["principal"].(string); ok {
		result["principal"] = principal
	}
	if credentials, ok := extraMap["credentials"].(string); ok {
		result["credentials"] = credentials
	}
	if userAgent, ok := extraMap["user_agent"].(string); ok {
		s.userAgent = userAgent
	}

	// Extract database parameter (Neo4j 4.x multi-database support)
	if db, ok := extraMap["db"].(string); ok {
		result["database"] = db
	} else if db, ok := extraMap["database"].(string); ok {
		// Some drivers use "database" instead of "db"
		result["database"] = db
	}
	s.utcPatchNegotiated = helloExtraRequestsUTCPatch(extraMap)

	return result, nil
}

func helloExtraRequestsUTCPatch(extraMap map[string]any) bool {
	raw, ok := extraMap["patch_bolt"]
	if !ok {
		return false
	}
	items, ok := raw.([]any)
	if !ok {
		return false
	}
	for _, item := range items {
		if token, ok := item.(string); ok && strings.EqualFold(strings.TrimSpace(token), "utc") {
			return true
		}
	}
	return false
}
