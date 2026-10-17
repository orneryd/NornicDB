package bolt

import (
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// User impersonation (Neo4j's imp_user, Bolt 4.4+): a BEGIN, an auto-commit
// RUN or a ROUTE may name a user whose complete security context the
// transaction runs in: that user's current roles and privileges, not a
// union with the authenticated user's. The authenticated user needs the
// IMPERSONATE privilege for that user (auth.ImpersonatePrivilegeStore), and
// a request without it fails before anything runs. While a session
// impersonates (Session.impersonated), every permission the session
// resolves (principalAccess, accessMode, withSessionIdentity) is the
// impersonated user's; the authenticated user stays in the request identity
// (cypher.RequestIdentity.ImpersonatedBy) for attribution, and the executor
// refuses updating administration commands (cypher.AuthorizeQuery).

const impersonatedUserField = "imp_user"

// impersonationFailure is a refused imp_user: the Neo4j status code and the
// message the client gets.
type impersonationFailure struct {
	code    string
	message localization.Message
}

// SetImpersonationResolver sets how the server decides whether a principal
// may impersonate a user: its roles, whether it holds the admin entitlement,
// and the user. Without one, no one may impersonate (secure default).
func (s *Server) SetImpersonationResolver(resolver func(roles []string, admin bool, user string) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.impersonationAllowed = resolver
}

// ImpersonationAuthResult is the auth result a transaction impersonating
// username runs with: the account's current roles and effective
// permissions. A user that doesn't exist or is disabled is an error.
func (a *AuthenticatorAdapter) ImpersonationAuthResult(username string) (*BoltAuthResult, error) {
	claims, err := a.auth.ImpersonationClaims(username)
	if err != nil {
		return nil, err
	}
	result := &BoltAuthResult{Authenticated: true, Username: claims.Username, PrincipalID: auth.PrincipalID(claims), Roles: claims.Roles}
	if a.getEffectivePermissions != nil {
		result.Permissions = a.getEffectivePermissions(claims.Roles)
	}
	return result, nil
}

// executingAuth is the auth result whose privileges the session's current
// statement or transaction runs with: the impersonated user's while it
// impersonates one, else the authenticated user's.
func (s *Session) executingAuth() *BoltAuthResult {
	if s.impersonated != nil {
		return s.impersonated
	}
	return s.authResult
}

// resolveImpersonation reads imp_user from a BEGIN's, auto-commit RUN's or
// ROUTE's extra dictionary and returns the auth result to run as: nil for
// no impersonation (no imp_user, null, a Bolt version before 4.4, which
// doesn't define it, or the authenticated user itself), or the failure to
// send, in the order Neo4j checks: a malformed field, a server with
// authentication disabled (ArgumentError), an authenticator that can't
// resolve users, and then the privilege. A missing or disabled user is
// refused exactly as one the authenticated user may not impersonate, so a
// request can't learn which users exist.
func (s *Session) resolveImpersonation(extra map[string]any) (*BoltAuthResult, *impersonationFailure) {
	raw, present := extra[impersonatedUserField]
	if !present || raw == nil || s.version < BoltV4_4 {
		return nil, nil
	}
	user, isString := raw.(string)
	if !isString {
		return nil, &impersonationFailure{code: "Neo.ClientError.Request.Invalid", message: localization.BoltImpersonatedUserInvalid()}
	}
	if s.authenticationDisabled() {
		return nil, &impersonationFailure{code: "Neo.ClientError.Statement.ArgumentError", message: localization.BoltImpersonationAuthDisabled()}
	}
	resolver, supported := s.server.config.Authenticator.(interface {
		ImpersonationAuthResult(username string) (*BoltAuthResult, error)
	})
	if !supported {
		return nil, &impersonationFailure{code: "Neo.ClientError.Statement.ArgumentError", message: localization.BoltImpersonationUnsupported()}
	}
	forbidden := &impersonationFailure{code: "Neo.ClientError.Security.Forbidden", message: localization.BoltImpersonationForbidden(user)}
	if s.authResult == nil {
		return nil, forbidden
	}
	if user == s.authResult.Username {
		return nil, nil
	}
	s.server.mu.RLock()
	allowed := s.server.impersonationAllowed
	s.server.mu.RUnlock()
	if allowed == nil || !allowed(s.authResult.Roles, s.authResult.HasPermission(string(auth.PermAdmin)), user) {
		return nil, forbidden
	}
	target, err := resolver.ImpersonationAuthResult(user)
	if err != nil || target == nil {
		return nil, forbidden
	}
	return target, nil
}

// routeExtra is a ROUTE message's extra dictionary (Bolt 4.4+: db and
// imp_user), or nil when it has none (Bolt 4.3 sends a database name in its
// place) or can't be read.
func routeExtra(data []byte) map[string]any {
	if len(data) == 0 {
		return nil
	}
	offset := 0
	if _, n, err := decodePackStreamMap(data, offset); err == nil {
		offset += n
	} else {
		return nil
	}
	if _, n, err := decodePackStreamList(data, offset); err == nil {
		offset += n
	} else {
		return nil
	}
	if offset >= len(data) {
		return nil
	}
	extra, _, err := decodePackStreamMap(data, offset)
	if err != nil {
		return nil
	}
	return extra
}

// forwardedAuth is the credential forwarded to remote constituents: the
// authenticated user's, but none while the session impersonates a user,
// whose identity it isn't. Impersonation never reaches a remote graph
// (remoteImpersonationFailure, and the executor checks every graph a
// statement selects); without the credential, nothing can run
// there as the authenticated user in the impersonated user's place.
func (s *Session) forwardedAuth() string {
	if s.impersonated != nil {
		return ""
	}
	return s.forwardedAuthHeader
}

// remoteImpersonationFailure is the refusal of graph while impersonating a
// user, when graph is (or includes) a remote constituent
// (cypher.GraphIsRemote), checked before any executor or storage for it is
// opened: as Neo4j documents for remote database aliases, a user can't be
// impersonated on the remote database. It is nil otherwise.
func (s *Session) remoteImpersonationFailure(impersonated *BoltAuthResult, graph string) *impersonationFailure {
	if impersonated == nil || !cypher.GraphIsRemote(s.cypherDatabaseManager(), graph) {
		return nil
	}
	return &impersonationFailure{code: "Neo.ClientError.Security.Forbidden", message: localization.CypherAdminImpersonationRemoteDenied(graph)}
}

// databaseAccessDenied is Neo4j's refusal of a database for who, the
// executing principal (the impersonated user while impersonating), named
// with its roles; no principal is a user with no name and no roles.
func databaseAccessDenied(who *BoltAuthResult) localization.Message {
	if who == nil {
		return localization.DatabaseAccessDenied("", nil)
	}
	return localization.DatabaseAccessDenied(who.Username, who.Roles)
}
