package bolt

import "github.com/orneryd/nornicdb/pkg/cypher"

// collectionAccessHooks are the server's sources for collection access,
// embedded in Server.
type collectionAccessHooks struct {
	// roleAdministration is the server's role owner the Cypher role commands
	// run through (SetRoleAdministration); nil leaves them unsupported.
	roleAdministration func() cypher.RoleAdministration
	// graphVisibility is a set of roles' graph visibility source
	// (SetGraphVisibilityResolver); nil restricts no one.
	graphVisibility func(roles []string, database string) (*cypher.GraphVisibility, error)
}

// SetRoleAdministration sets the server's role owner the Cypher role
// commands run through, read when a session builds its identity, so a
// session sees the role store the server has loaded by then.
func (s *Server) SetRoleAdministration(roles func() cypher.RoleAdministration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.roleAdministration = roles
}

// SetGraphVisibilityResolver sets how a session's roles resolve to their
// graph visibility (the HTTP server's GraphVisibilityForRoles), so a login
// restricted by collection rules sees the same graph on Bolt as on HTTP.
func (s *Server) SetGraphVisibilityResolver(resolver func(roles []string, database string) (*cypher.GraphVisibility, error)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.graphVisibility = resolver
}
