// Package auth: user-defined roles storage (system DB).
//
// User-defined roles are stored as _Role nodes. Built-in roles (admin, editor, viewer)
// are not stored but appear in the full role list. See docs/plans/per-database-rbac-neo4j-style.md §4.2.

package auth

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	roleLabel   = "_Role"
	roleSystems = "_System"
	rolePrefix  = "role:"
)

// RoleStore persists user-defined role names in the system database.
type RoleStore struct {
	storage storage.Engine
	mu      sync.RWMutex
	roles   map[string]struct{} // user-defined role names (set)
}

// NewRoleStore creates a store that reads/writes _Role nodes in the given system storage.
func NewRoleStore(systemStorage storage.Engine) *RoleStore {
	return &RoleStore{storage: systemStorage, roles: make(map[string]struct{})}
}

// Load reads all _Role nodes from storage into memory.
func (r *RoleStore) Load(ctx context.Context) error {
	m := make(map[string]struct{})
	err := storage.StreamNodesWithFallback(ctx, r.storage, 1000, func(n *storage.Node) error {
		for _, l := range n.Labels {
			if l == roleLabel {
				name := roleNameFromNodeID(string(n.ID))
				if name != "" {
					m[name] = struct{}{}
				}
				break
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.roles = m
	r.mu.Unlock()
	return nil
}

// AllRoles returns built-in role names plus all user-defined role names.
func (r *RoleStore) AllRoles() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(builtinRoleNames)+len(r.roles))
	out = append(out, builtinRoleNames...)
	for name := range r.roles {
		out = append(out, name)
	}
	return out
}

// IsBuiltin returns true if the role name is a built-in (admin, editor, viewer).
func IsBuiltinRole(name string) bool {
	n := NormalizeRoleName(name)
	for _, b := range builtinRoleNames {
		if b == n {
			return true
		}
	}
	return false
}

// CreateRole creates a user-defined role. Fails if name is built-in or already exists.
func (r *RoleStore) CreateRole(ctx context.Context, name string) error {
	name = NormalizeRoleName(name)
	if name == "" {
		return ErrInvalidRoleName
	}
	if IsBuiltinRole(name) {
		return ErrRoleExists
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.roles[name]; exists {
		return ErrRoleExists
	}
	nodeID := storage.NodeID(rolePrefix + name)
	node := &storage.Node{
		ID:     nodeID,
		Labels: []string{roleLabel, roleSystems},
		Properties: map[string]any{
			"name": name,
		},
	}
	_, err := r.storage.CreateNode(node)
	if err != nil {
		return err
	}
	r.roles[name] = struct{}{}
	return nil
}

// DeleteRole removes the role, its grants and stored user assignments atomically.
// Servers use DeleteRoleWithStores to publish their loaded caches after commit.
func (r *RoleStore) DeleteRole(ctx context.Context, name string) error {
	return r.DeleteRoleWithStores(ctx, name, RoleMutationStores{})
}

// RenameRole moves the role and all its stored grants and assignments atomically.
// Servers use RenameRoleWithStores to publish their loaded caches after commit.
func (r *RoleStore) RenameRole(ctx context.Context, oldName, newName string) error {
	return r.RenameRoleWithStores(ctx, oldName, newName, RoleMutationStores{})
}

// Exists returns true if the role exists (built-in or user-defined).
func (r *RoleStore) Exists(name string) bool {
	name = NormalizeRoleName(name)
	if IsBuiltinRole(name) {
		return true
	}
	r.mu.RLock()
	_, ok := r.roles[name]
	r.mu.RUnlock()
	return ok
}

func roleNameFromNodeID(id string) string {
	if strings.HasPrefix(id, rolePrefix) {
		return id[len(rolePrefix):]
	}
	return ""
}

// Role store errors.
var (
	ErrInvalidRoleName         = errors.New("invalid role name")
	ErrRoleExists              = errors.New("role already exists")
	ErrRoleNotFound            = errors.New("role not found")
	ErrCannotDeleteBuiltinRole = errors.New("cannot delete or rename built-in role")
)

// LockRoleForGrant reads and writes the role record in the grant transaction.
// Role deletion writes that same record, so their commits serialize: neither
// can commit from a snapshot that missed the other's role or grant changes.
func (r *RoleStore) LockRoleForGrant(ctx context.Context, name string) (bool, error) {
	name = NormalizeRoleName(name)
	if IsBuiltinRole(name) {
		return true, nil
	}
	node, err := r.storage.GetNode(storage.NodeID(rolePrefix + name))
	if errors.Is(err, storage.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	for _, label := range node.Labels {
		if label == roleLabel {
			return true, r.storage.UpdateNode(node)
		}
	}
	return false, nil
}
