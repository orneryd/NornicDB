package server

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
)

// serverRoleAdministration is the server's role owner for the Cypher role
// commands (cypher.RoleAdministration): the same role store and role
// mutations the HTTP role API uses, so a role change moves every record the
// role owns and publishes the server's loaded caches after commit, whichever
// route made it.
type serverRoleAdministration struct {
	s *Server
}

// RoleAdministration is the server's role owner, or nil when it has no role
// or user store (the role commands are then unsupported).
func (s *Server) RoleAdministration() cypher.RoleAdministration {
	if s == nil || s.roleStore == nil || s.auth == nil {
		return nil
	}
	return serverRoleAdministration{s: s}
}

func (a serverRoleAdministration) mutationStores() auth.RoleMutationStores {
	return auth.RoleMutationStores{
		Allowlist: a.s.allowlistStore, Privileges: a.s.privilegesStore,
		Entitlements: a.s.roleEntitlementsStore, Users: a.s.auth,
	}
}

func (a serverRoleAdministration) Roles() []cypher.RoleListing {
	members := make(map[string][]string)
	for _, user := range a.s.auth.ListUsers() {
		if user == nil {
			continue
		}
		for _, role := range user.Roles {
			name := auth.NormalizeRoleName(string(role))
			members[name] = append(members[name], user.Username)
		}
	}
	names := a.s.roleStore.AllRoles()
	listings := make([]cypher.RoleListing, 0, len(names))
	for _, name := range names {
		listings = append(listings, cypher.RoleListing{Name: name, Members: members[auth.NormalizeRoleName(name)]})
	}
	return listings
}

func (a serverRoleAdministration) CreateRole(ctx context.Context, name string) error {
	return a.s.roleStore.CreateRole(ctx, name)
}

func (a serverRoleAdministration) DropRole(ctx context.Context, name string) error {
	return a.s.roleStore.DeleteRoleWithStores(ctx, name, a.mutationStores())
}

func (a serverRoleAdministration) RenameRole(ctx context.Context, from, to string) error {
	return a.s.roleStore.RenameRoleWithStores(ctx, from, to, a.mutationStores())
}

func (a serverRoleAdministration) RoleExists(name string) bool {
	return a.s.roleStore.Exists(name)
}

func (a serverRoleAdministration) UserRoles(user string) ([]string, error) {
	stored, err := a.s.auth.GetUser(user)
	if err != nil {
		return nil, err
	}
	roles := make([]string, len(stored.Roles))
	for i, role := range stored.Roles {
		roles[i] = string(role)
	}
	return roles, nil
}

func (a serverRoleAdministration) ChangeUserRoles(ctx context.Context, users []string, change func(current []string) ([]string, bool), skipMissing bool) error {
	return a.s.auth.ModifyUsersRoles(ctx, users, func(current []auth.Role) ([]auth.Role, bool) {
		names := make([]string, len(current))
		for i, role := range current {
			names[i] = string(role)
		}
		next, changed := change(names)
		roles := make([]auth.Role, len(next))
		for i, role := range next {
			roles[i] = auth.Role(role)
		}
		return roles, changed
	}, skipMissing)
}
