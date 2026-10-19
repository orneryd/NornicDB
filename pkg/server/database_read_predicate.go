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

// databaseReadCoverage distinguishes admission to a logical database from
// physical local counts. A remote-only composite can have readable metrics
// while contributing no local nodes. Both decisions use the same live owner.
func (s *Server) databaseReadCoverage(canRead func(string) bool, includeSystem, anyReadable bool) (bool, bool) {
	allReadable := true
	for _, database := range s.dbManager.ListDatabaseNames() {
		if !includeSystem && database == "system" {
			continue
		}
		readable := canRead(database)
		anyReadable = anyReadable || readable
		allReadable = allReadable && readable
		if anyReadable && !allReadable {
			return true, false
		}
	}
	return anyReadable, allReadable
}
