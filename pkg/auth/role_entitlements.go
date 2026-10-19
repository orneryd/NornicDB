// Package auth: per-role global entitlements (permissions) storage.
//
// Role entitlements are stored in the system database as _RoleEntitlement nodes.
// Built-in roles (admin, editor, viewer) use RolePermissions when no override
// is stored; user-defined roles use stored entitlements or have none.
// See docs/security/entitlements.md.

package auth

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	roleEntitlementLabel   = "_RoleEntitlement"
	roleEntitlementSystems = "_System"
	roleEntitlementPrefix  = "role_entitlement:"
)

// RoleEntitlementsStore loads and saves role→entitlement IDs (global permissions) in the system database.
type RoleEntitlementsStore struct {
	storage storage.Engine
	mu      sync.RWMutex
	// role → list of entitlement IDs (e.g. "read", "write", "admin")
	entitlements map[string][]string
}

// NewRoleEntitlementsStore creates a store that reads/writes role entitlements to the given system storage.
func NewRoleEntitlementsStore(systemStorage storage.Engine) *RoleEntitlementsStore {
	return &RoleEntitlementsStore{storage: systemStorage, entitlements: make(map[string][]string)}
}

// Load reads all _RoleEntitlement nodes from storage into memory. Call at startup and after PUT.
func (r *RoleEntitlementsStore) Load(ctx context.Context) error {
	m := make(map[string][]string)
	err := storage.StreamNodesWithFallback(ctx, r.storage, 1000, func(n *storage.Node) error {
		for _, l := range n.Labels {
			if l != roleEntitlementLabel {
				continue
			}
			role := roleFromEntitlementNodeID(string(n.ID))
			if role == "" {
				return nil
			}
			ids := entitlementIDsFromProperties(n.Properties)
			m[role] = ids
			return nil
		}
		return nil
	})
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.entitlements = m
	r.mu.Unlock()
	return nil
}

func roleFromEntitlementNodeID(id string) string {
	if !strings.HasPrefix(id, roleEntitlementPrefix) {
		return ""
	}
	return NormalizeRoleName(id[len(roleEntitlementPrefix):])
}

func entitlementIDsFromProperties(prop map[string]any) []string {
	v, ok := prop["entitlements"]
	if !ok || v == nil {
		return nil
	}
	switch raw := v.(type) {
	case string:
		var out []string
		_ = json.Unmarshal([]byte(raw), &out)
		return out
	case []any:
		out := make([]string, 0, len(raw))
		for _, x := range raw {
			if s, ok := x.(string); ok && s != "" {
				out = append(out, strings.ToLower(strings.TrimSpace(s)))
			}
		}
		return out
	default:
		return nil
	}
}

// Get returns the stored entitlement IDs for a role. Returns nil if no override is stored.
// For permission resolution use PermissionsForRole so built-in roles use RolePermissions when not overridden.
func (r *RoleEntitlementsStore) Get(role string) []string {
	role = NormalizeRoleName(role)
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids, ok := r.entitlements[role]
	if !ok || len(ids) == 0 {
		return nil
	}
	out := make([]string, len(ids))
	copy(out, ids)
	return out
}

// Set persists one role's entitlement IDs and refreshes in-memory. Empty list
// removes the override (a built-in role then uses RolePermissions). The role
// must exist (ErrRoleNotFound).
func (r *RoleEntitlementsStore) Set(ctx context.Context, role string, entitlementIDs []string) error {
	return r.SetMappings(ctx, map[string][]string{role: entitlementIDs})
}

// SetMappings persists every role's entitlement IDs in one transaction
// through the role-record owner (WithRoleRecords): all of them are stored,
// or, when one fails or names a role that doesn't exist, none. The cache
// changes only after commit.
func (r *RoleEntitlementsStore) SetMappings(ctx context.Context, mappings map[string][]string) error {
	normalized := make(map[string][]string, len(mappings))
	roles := make([]string, 0, len(mappings))
	for role, ids := range mappings {
		role = NormalizeRoleName(role)
		if role == "" {
			return ErrInvalidRoleName
		}
		normalized[role] = ids
		roles = append(roles, role)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := WithRoleRecords(ctx, r.storage, roles, func(view storage.Engine) error {
		for role, ids := range normalized {
			entitlementsJSON, _ := json.Marshal(ids)
			node := &storage.Node{
				ID:     storage.NodeID(roleEntitlementPrefix + role),
				Labels: []string{roleEntitlementLabel, roleEntitlementSystems},
				Properties: map[string]any{
					"role":         role,
					"entitlements": string(entitlementsJSON),
				},
			}
			if err := putRoleRecord(view, node, len(ids) == 0); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	for role, ids := range normalized {
		if len(ids) == 0 {
			delete(r.entitlements, role)
		} else {
			r.entitlements[role] = append([]string(nil), ids...)
		}
	}
	return nil
}

// All returns role→entitlement IDs for all roles that have a stored override.
// For full role→entitlements (including built-in defaults) the server merges with RolePermissions.
func (r *RoleEntitlementsStore) All() map[string][]string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string][]string, len(r.entitlements))
	for k, v := range r.entitlements {
		out[k] = append([]string(nil), v...)
	}
	return out
}

// PermissionsForRole returns the effective entitlement IDs (permission strings) for a single role.
// If store is nil or has no override for role, built-in roles use RolePermissions; user-defined return nil.
func PermissionsForRole(role string, store *RoleEntitlementsStore) []string {
	if store != nil {
		store.mu.RLock()
		defer store.mu.RUnlock()
	}
	ids, builtin := effectiveRoleEntitlements(role, store)
	if len(ids) > 0 {
		return append([]string(nil), ids...)
	}
	if len(builtin) == 0 {
		return nil
	}
	ids = make([]string, len(builtin))
	for i, p := range builtin {
		ids[i] = string(p)
	}
	return ids
}

// effectiveRoleEntitlements owns override versus built-in fallback. The caller
// holds store.mu when store is non-nil; returned slices must not escape it.
func effectiveRoleEntitlements(role string, store *RoleEntitlementsStore) ([]string, []Permission) {
	role = NormalizeRoleName(role)
	if store != nil {
		if ids := store.entitlements[role]; len(ids) > 0 {
			return ids, nil
		}
	}
	return nil, RolePermissions[Role(role)]
}

// EntitlementsAllow is the one check of a global entitlement: entitlements
// (IDs, from PermissionsForRoles) hold permission, or admin, which holds
// every permission. HTTP, Bolt and the "is admin" checks all decide through
// it.
func EntitlementsAllow(entitlements []string, permission string) bool {
	permission = strings.ToLower(strings.TrimSpace(permission))
	for _, id := range entitlements {
		if id == permission || id == string(PermAdmin) {
			return true
		}
	}
	return false
}

// PermissionsForRoles returns the union of effective entitlement IDs for the given roles.
// Used by server hasPermission and GetEffectivePermissions.
func PermissionsForRoles(roles []string, store *RoleEntitlementsStore) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, role := range roles {
		for _, id := range PermissionsForRole(role, store) {
			if _, ok := seen[id]; !ok {
				seen[id] = struct{}{}
				out = append(out, id)
			}
		}
	}
	return out
}

// RolesAllow tests the effective entitlement union without materializing it.
// Stored overrides and built-in fallback are identical to PermissionsForRoles.
func RolesAllow(roles []string, store *RoleEntitlementsStore, permission string) bool {
	if store != nil {
		store.mu.RLock()
		defer store.mu.RUnlock()
	}
	permission = strings.ToLower(strings.TrimSpace(permission))
	for _, role := range roles {
		ids, builtin := effectiveRoleEntitlements(role, store)
		if len(ids) > 0 {
			if EntitlementsAllow(ids, permission) {
				return true
			}
			continue
		}
		for _, id := range builtin {
			if string(id) == permission || id == PermAdmin {
				return true
			}
		}
	}
	return false
}
