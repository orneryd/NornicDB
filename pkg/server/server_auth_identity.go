package server

import (
	"net/http"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// selfServiceClaims preserves the guard order before a request body is decoded.
func (s *Server) selfServiceClaims(w http.ResponseWriter, r *http.Request, method string, methodMessage localization.Message) *auth.JWTClaims {
	if r.Method != method {
		s.writeLocalizedError(w, r, http.StatusMethodNotAllowed, methodMessage, ErrMethodNotAllowed)
		return nil
	}
	if s.auth == nil {
		s.writeAuthenticationNotConfigured(w, r)
		return nil
	}
	claims := getClaims(r)
	if claims == nil {
		s.writeNotAuthenticated(w, r)
	}
	return claims
}

// readSelfServiceActor decodes first, then resolves the authenticated username.
// Missing usernames retain the established subject-ID lookup and error shape.
func (s *Server) readSelfServiceActor(w http.ResponseWriter, r *http.Request, body any, claims *auth.JWTClaims) (string, bool) {
	if err := s.readJSON(r, body); err != nil {
		s.writeInvalidRequestBody(w, r)
		return "", false
	}
	username := claims.Username
	if username == "" {
		user, err := s.auth.GetUserByID(claims.Sub)
		if err != nil {
			s.writeUserNotFound(w, r)
			return "", false
		}
		username = user.Username
	}
	return username, true
}

// authRoleValues preserves the exact role names and the old non-nil empty
// projection. Optional updates retain their nil-as-no-change policy at the caller.
func authRoleValues(names []string) []auth.Role {
	roles := make([]auth.Role, len(names))
	for i, name := range names {
		roles[i] = auth.Role(name)
	}
	return roles
}
