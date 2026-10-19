package server

import "github.com/orneryd/nornicdb/pkg/auth"

// databaseReadPredicate binds only the principal inputs for one aggregate read.
// Every invocation uses databaseAccessForRoles, the existing selection/read
// owner. Allowlist and permission edits remain live; no decision is cached.
func (s *Server) databaseReadPredicate(claims *auth.JWTClaims) func(string) bool {
	var roles []string
	if claims != nil {
		roles = claims.Roles
	}
	mode := s.getDatabaseAccessMode(claims)
	return func(database string) bool {
		_, access := s.databaseAccessForRoles(roles, database, mode, false)
		return access.Read
	}
}
