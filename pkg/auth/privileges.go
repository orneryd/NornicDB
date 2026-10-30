// Package auth: per-database read/write privileges (Phase 4 RBAC).
//
// Privileges are stored in the system database as _DbPrivilege nodes.
// ResolveDatabaseAccess resolves read/write from the matrix within the roles' global
// entitlements; when no entry exists, from the entitlements alone.
// See docs/plans/per-database-rbac-neo4j-style.md §4.4.

package auth

import (
	"context"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	dbPrivLabel   = "_DbPrivilege"
	dbPrivSystems = "_System"
	dbPrivPrefix  = "db_priv:"
)

// DbPrivilege holds read/write for one (role, database).
type DbPrivilege struct {
	Read  bool
	Write bool
}

// PrivilegesStore persists and resolves per (role, database) read/write.
type PrivilegesStore struct {
	storage storage.Engine
	mu      sync.RWMutex
	// role -> dbName -> { Read, Write }
	matrix map[string]map[string]DbPrivilege
}

// NewPrivilegesStore creates a store that reads/writes _DbPrivilege nodes.
func NewPrivilegesStore(systemStorage storage.Engine) *PrivilegesStore {
	return &PrivilegesStore{storage: systemStorage, matrix: make(map[string]map[string]DbPrivilege)}
}

// Load reads all _DbPrivilege nodes from storage into memory.
func (p *PrivilegesStore) Load(ctx context.Context) error {
	m := make(map[string]map[string]DbPrivilege)
	err := storage.StreamNodesWithFallback(ctx, p.storage, 1000, func(n *storage.Node) error {
		for _, l := range n.Labels {
			if l == dbPrivLabel {
				role, dbName := roleDbFromNodeID(string(n.ID))
				if role == "" || dbName == "" {
					return nil
				}
				priv := privFromProperties(n.Properties)
				if m[role] == nil {
					m[role] = make(map[string]DbPrivilege)
				}
				m[role][dbName] = priv
				break
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.matrix = m
	p.mu.Unlock()
	return nil
}

// Granted is the read and write the privileges matrix records for roles on
// database, the union of their entries, and whether any of the roles has an
// entry there. A nil store records none.
func (p *PrivilegesStore) Granted(roles []string, database string) (ResolvedAccess, bool) {
	if p == nil {
		return ResolvedAccess{}, false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	var access ResolvedAccess
	found := false
	for _, role := range roles {
		if priv, ok := p.matrix[NormalizeRoleName(role)][database]; ok {
			found = true
			access.Read = access.Read || priv.Read
			access.Write = access.Write || priv.Write
		}
	}
	return access, found
}

// Resolve preserves the legacy database-access contract: explicit matrix grants
// win, otherwise built-in role permissions supply the defaults. For example,
// store.Resolve([]string{"viewer"}, "reports") returns that role's matrix grant
// when present, even if it differs from the role's global entitlements.
//
// Deprecated: production authorization should use ResolveDatabaseAccess with
// the stored entitlement policy; Resolve intentionally retains legacy semantics.
func (p *PrivilegesStore) Resolve(principalRoles []string, dbName string) ResolvedAccess {
	if granted, found := p.Granted(principalRoles, dbName); found {
		return granted
	}
	var access ResolvedAccess
	for _, permission := range PermissionsForRoles(principalRoles, nil) {
		switch permission {
		case string(PermRead):
			access.Read = true
		case string(PermWrite), string(PermAdmin):
			access.Write = true
		}
	}
	return access
}

// ResolveDatabaseAccess is the one resolver of a principal's read and write
// on a database, which HTTP, Bolt, gRPC, GraphQL, MCP and plugins all use
// (through cypher.PrincipalAccess or the server's resolved-access
// resolver). The roles' global entitlements (PermissionsForRoles: stored
// overrides, else the built-in roles' defaults) gate it: the read
// entitlement is what lets a principal use a database at all, and write
// needs the write entitlement too. Within that, the privileges matrix
// decides when any of the roles has an entry for the database; otherwise
// the entitlements decide alone. So removing a role's write entitlement
// stops its writes on every database at once.
func ResolveDatabaseAccess(roles []string, database string, privileges *PrivilegesStore, entitlements *RoleEntitlementsStore) ResolvedAccess {
	if !RolesAllow(roles, entitlements, string(PermRead)) {
		return ResolvedAccess{}
	}
	write := RolesAllow(roles, entitlements, string(PermWrite))
	if granted, found := privileges.Granted(roles, database); found {
		return ResolvedAccess{Read: granted.Read, Write: granted.Write && write}
	}
	return ResolvedAccess{Read: true, Write: write}
}

// DatabaseReadAllowed checks only read, without constructing unused write
// capabilities. It uses the same global entitlement and matrix owners.
func DatabaseReadAllowed(roles []string, database string, privileges *PrivilegesStore, entitlements *RoleEntitlementsStore) bool {
	if !RolesAllow(roles, entitlements, string(PermRead)) {
		return false
	}
	if granted, found := privileges.Granted(roles, database); found {
		return granted.Read
	}
	return true
}

// Matrix returns a copy of the full matrix for GET API.
func (p *PrivilegesStore) Matrix() []struct {
	Role     string `json:"role"`
	Database string `json:"database"`
	Read     bool   `json:"read"`
	Write    bool   `json:"write"`
} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var out []struct {
		Role     string `json:"role"`
		Database string `json:"database"`
		Read     bool   `json:"read"`
		Write    bool   `json:"write"`
	}
	for role, perDb := range p.matrix {
		for dbName, priv := range perDb {
			out = append(out, struct {
				Role     string `json:"role"`
				Database string `json:"database"`
				Read     bool   `json:"read"`
				Write    bool   `json:"write"`
			}{Role: role, Database: dbName, Read: priv.Read, Write: priv.Write})
		}
	}
	return out
}

// SavePrivilege persists one (role, database, read, write) and refreshes
// in-memory, through the role-record owner (WithRoleRecords). The role must
// exist (ErrRoleNotFound).
func (p *PrivilegesStore) SavePrivilege(ctx context.Context, role, dbName string, read, write bool) error {
	role = NormalizeRoleName(role)
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := WithRoleRecords(ctx, p.storage, []string{role}, func(view storage.Engine) error {
		return putDbPrivilege(view, role, dbName, read, write)
	}); err != nil {
		return err
	}
	if p.matrix[role] == nil {
		p.matrix[role] = make(map[string]DbPrivilege)
	}
	p.matrix[role][dbName] = DbPrivilege{Read: read, Write: write}
	return nil
}

// putDbPrivilege stores one database privilege record in view.
func putDbPrivilege(view storage.Engine, role, dbName string, read, write bool) error {
	return putRoleRecord(view, &storage.Node{
		ID:     storage.NodeID(dbPrivPrefix + role + ":" + dbName),
		Labels: []string{dbPrivLabel, dbPrivSystems},
		Properties: map[string]any{
			"role":     role,
			"database": dbName,
			"read":     read,
			"write":    write,
		},
	}, false)
}

// PutMatrix replaces the stored matrix with the given list (for PUT
// /auth/access/privileges): it deletes every _DbPrivilege node and creates
// one per entry, in one transaction through the role-record owner, so the
// matrix changes entirely or not at all, and every entry's role must exist.
func (p *PrivilegesStore) PutMatrix(ctx context.Context, entries []struct {
	Role     string `json:"role"`
	Database string `json:"database"`
	Read     bool   `json:"read"`
	Write    bool   `json:"write"`
}) error {
	roles := make([]string, 0, len(entries))
	for _, e := range entries {
		roles = append(roles, e.Role)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	matrix := make(map[string]map[string]DbPrivilege)
	err := WithRoleRecords(ctx, p.storage, roles, func(view storage.Engine) error {
		nodes, err := view.GetNodesByLabel(dbPrivLabel)
		if err != nil {
			return err
		}
		for _, node := range nodes {
			role, database := roleDbFromNodeID(string(node.ID))
			if role == "" || database == "" {
				continue
			}
			if err := view.DeleteNode(node.ID); err != nil {
				return err
			}
		}
		for _, e := range entries {
			role := NormalizeRoleName(e.Role)
			if err := putDbPrivilege(view, role, e.Database, e.Read, e.Write); err != nil {
				return err
			}
			if matrix[role] == nil {
				matrix[role] = make(map[string]DbPrivilege)
			}
			matrix[role][e.Database] = DbPrivilege{Read: e.Read, Write: e.Write}
		}
		return nil
	})
	if err != nil {
		return err
	}
	p.matrix = matrix
	return nil
}

func roleDbFromNodeID(id string) (role, dbName string) {
	if !strings.HasPrefix(id, dbPrivPrefix) {
		return "", ""
	}
	rest := id[len(dbPrivPrefix):]
	idx := strings.Index(rest, ":")
	if idx < 0 {
		return "", ""
	}
	return rest[:idx], rest[idx+1:]
}

func privFromProperties(prop map[string]any) DbPrivilege {
	var read, write bool
	if v, ok := prop["read"].(bool); ok {
		read = v
	}
	if v, ok := prop["write"].(bool); ok {
		write = v
	}
	// Support JSON-unmarshalled bool from string storage
	if v, ok := prop["read"].(float64); ok {
		read = v != 0
	}
	if v, ok := prop["write"].(float64); ok {
		write = v != 0
	}
	return DbPrivilege{Read: read, Write: write}
}
