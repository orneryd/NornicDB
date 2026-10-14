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
	return strings.ToLower(strings.TrimSpace(id[len(roleEntitlementPrefix):]))
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
	role = strings.ToLower(strings.TrimSpace(role))
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

// Set persists one role's entitlement IDs and refreshes in-memory. Empty list removes override (built-in will use RolePermissions).
func (r *RoleEntitlementsStore) Set(ctx context.Context, role string, entitlementIDs []string) error {
	role = strings.ToLower(strings.TrimSpace(role))
	if role == "" {
		return ErrInvalidRoleName
	}
	nodeID := storage.NodeID(roleEntitlementPrefix + role)
	entitlementsJSON, _ := json.Marshal(entitlementIDs)
	node := &storage.Node{
		ID:     nodeID,
		Labels: []string{roleEntitlementLabel, roleEntitlementSystems},
		Properties: map[string]any{
			"role":         role,
			"entitlements": string(entitlementsJSON),
		},
	}
	existing, err := r.storage.GetNode(nodeID)
	if err == storage.ErrNotFound {
		if len(entitlementIDs) == 0 {
			return nil
		}
		_, err = r.storage.CreateNode(node)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		node.CreatedAt = existing.CreatedAt
		if len(entitlementIDs) == 0 {
			if err := r.storage.DeleteNode(nodeID); err != nil {
				return err
			}
		} else {
			err = r.storage.UpdateNode(node)
			if err != nil {
				return err
			}
		}
	}
	r.mu.Lock()
	if len(entitlementIDs) == 0 {
		delete(r.entitlements, role)
	} else {
		r.entitlements[role] = append([]string(nil), entitlementIDs...)
	}
	r.mu.Unlock()
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
	role = strings.ToLower(strings.TrimSpace(role))
	if store != nil {
		if ids := store.Get(role); len(ids) > 0 {
			return ids
		}
	}
	// Built-in: use RolePermissions
	perms, ok := RolePermissions[Role(role)]
	if !ok || len(perms) == 0 {
		return nil
	}
	ids := make([]string, len(perms))
	for i, p := range perms {
		ids[i] = string(p)
	}
	return ids
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
