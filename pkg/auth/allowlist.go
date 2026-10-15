// Package auth: per-database allowlist storage and allowlist-based DatabaseAccessMode.
//
// Allowlist is stored in the system database as nodes with label _RoleDbAccess.
// Seed populates admin, editor, viewer with access to all databases so the default admin works.
// See docs/plans/per-database-rbac-neo4j-style.md §4.3.

package auth

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	roleDbAccessLabel   = "_RoleDbAccess"
	roleDbAccessPrefix  = "role_db_access:"
	roleDbAccessSystems = "_System"
)

// Built-in role names seeded with allowlist (same as Role constants).
var builtinRoleNames = []string{string(RoleAdmin), string(RoleEditor), string(RoleViewer)}

// AllowlistStore loads and saves role→databases allowlist in the system database.
type AllowlistStore struct {
	storage storage.Engine
	mu      sync.RWMutex
	// role → list of database names (nil or empty entry means "all databases")
	allowlist map[string][]string
}

// NewAllowlistStore creates a store that reads/writes allowlist to the given system storage.
func NewAllowlistStore(systemStorage storage.Engine) *AllowlistStore {
	return &AllowlistStore{storage: systemStorage, allowlist: make(map[string][]string)}
}

// Load reads the full allowlist from storage into memory. Call at startup and after PUT.
func (a *AllowlistStore) Load(ctx context.Context) error {
	m := make(map[string][]string)
	err := storage.StreamNodesWithFallback(ctx, a.storage, 1000, func(n *storage.Node) error {
		hasLabel := false
		for _, l := range n.Labels {
			if l == roleDbAccessLabel {
				hasLabel = true
				break
			}
		}
		if !hasLabel {
			return nil
		}
		role := roleFromNodeID(string(n.ID))
		if role == "" {
			return nil
		}
		databases := databasesFromProperties(n.Properties)
		m[role] = databases
		return nil
	})
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.allowlist = m
	a.mu.Unlock()
	return nil
}

// Allowlist returns a copy of the current allowlist (role → databases). Nil or empty slice means "all databases".
func (a *AllowlistStore) Allowlist() map[string][]string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make(map[string][]string, len(a.allowlist))
	for k, v := range a.allowlist {
		if v != nil {
			out[k] = append([]string(nil), v...)
		} else {
			out[k] = nil
		}
	}
	return out
}

// SaveRoleDatabases persists one role's database list and refreshes the
// in-memory allowlist. The role must exist (ErrRoleNotFound).
func (a *AllowlistStore) SaveRoleDatabases(ctx context.Context, role string, databases []string) error {
	return a.SaveMappings(ctx, map[string][]string{role: databases})
}

// SaveMappings persists a batch in one transaction through the role-record
// owner (WithRoleRecords): every role's list is stored, or none when one
// fails or names a role that doesn't exist. The cache changes only after
// commit.
func (a *AllowlistStore) SaveMappings(ctx context.Context, mappings map[string][]string) error {
	roles := make([]string, 0, len(mappings))
	normalized := make(map[string][]string, len(mappings))
	for role, databases := range mappings {
		roles = append(roles, role)
		normalized[NormalizeRoleName(role)] = databases
	}
	mappings = normalized
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := WithRoleRecords(ctx, a.storage, roles, func(view storage.Engine) error {
		for role, databases := range mappings {
			databasesJSON, _ := json.Marshal(databases)
			node := &storage.Node{
				ID:     storage.NodeID(roleDbAccessPrefix + role),
				Labels: []string{roleDbAccessLabel, roleDbAccessSystems},
				Properties: map[string]any{
					"role":      role,
					"databases": string(databasesJSON),
				},
			}
			if err := putRoleRecord(view, node, false); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if a.allowlist == nil {
		a.allowlist = make(map[string][]string)
	}
	for role, databases := range mappings {
		a.allowlist[role] = append([]string(nil), databases...)
	}
	return nil
}

// SeedIfEmpty creates default allowlist entries for admin, editor, viewer if no allowlist data exists.
// Built-in roles are seeded with an empty list so they have access to all databases (including
// dynamically created ones). databaseNames is unused but kept for API compatibility; callers may
// pass the current db list from dbManager for logging or future use.
// Call after Load() so in-memory state is correct; or call on fresh DB before any Load().
func (a *AllowlistStore) SeedIfEmpty(ctx context.Context, databaseNames []string) error {
	hasStored, err := a.HasAllowlistData(ctx)
	if err != nil || hasStored {
		return err
	}
	// Empty list means "all databases" in NewAllowlistDatabaseAccessMode, so dynamically
	// created databases (e.g. test_db_a) are accessible without re-seeding.
	mappings := make(map[string][]string)
	for _, role := range builtinRoleNames {
		mappings[role] = []string{}
	}
	return a.SaveMappings(ctx, mappings)
}

// HasAllowlistData returns true if any allowlist entry exists in storage (for seed skip).
func (a *AllowlistStore) HasAllowlistData(ctx context.Context) (bool, error) {
	var found bool
	err := storage.StreamNodesWithFallback(ctx, a.storage, 100, func(n *storage.Node) error {
		for _, l := range n.Labels {
			if l == roleDbAccessLabel {
				found = true
				return storage.ErrIterationStopped
			}
		}
		return nil
	})
	return found, err
}

func roleFromNodeID(id string) string {
	if strings.HasPrefix(id, roleDbAccessPrefix) {
		return id[len(roleDbAccessPrefix):]
	}
	return ""
}

func databasesFromProperties(p map[string]any) []string {
	if p == nil {
		return nil
	}
	s, ok := p["databases"].(string)
	if !ok {
		return nil
	}
	var out []string
	_ = json.Unmarshal([]byte(s), &out)
	return out
}

// allowlistDBAccessMode implements DatabaseAccessMode from allowlist + principal roles.
type allowlistDBAccessMode struct {
	allowlist map[string][]string // role → databases (nil/empty = all)
	roles     []string            // principal's roles
}

// NewAllowlistDatabaseAccessMode returns a DatabaseAccessMode for the given allowlist and principal roles.
// For each role: if the role has no allowlist (or empty), treat as "all databases"; else allow only listed DBs.
// CanAccessDatabase(dbName) is true iff at least one of the principal's roles allows the database.
func NewAllowlistDatabaseAccessMode(allowlist map[string][]string, principalRoles []string) DatabaseAccessMode {
	if allowlist == nil || len(principalRoles) == 0 {
		return DenyAllDatabaseAccessMode
	}
	return &allowlistDBAccessMode{allowlist: allowlist, roles: principalRoles}
}

func (m *allowlistDBAccessMode) CanSeeDatabase(dbName string) bool {
	return m.canAccess(dbName)
}

func (m *allowlistDBAccessMode) CanAccessDatabase(dbName string) bool {
	return m.canAccess(dbName)
}

func (m *allowlistDBAccessMode) canAccess(dbName string) bool {
	for _, role := range m.roles {
		role = NormalizeRoleName(role)
		dbs, ok := m.allowlist[role]
		if !ok {
			// No allowlist for this role → "all databases"
			return true
		}
		if len(dbs) == 0 {
			return true
		}
		for _, d := range dbs {
			if d == dbName {
				return true
			}
		}
	}
	return false
}
