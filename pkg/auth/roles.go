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
	n := strings.ToLower(strings.TrimSpace(name))
	for _, b := range builtinRoleNames {
		if b == n {
			return true
		}
	}
	return false
}

// CreateRole creates a user-defined role. Fails if name is built-in or already exists.
func (r *RoleStore) CreateRole(ctx context.Context, name string) error {
	name = strings.ToLower(strings.TrimSpace(name))
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

// DeleteRole removes a user-defined role. Fails if name is built-in or does not exist.
// Caller must ensure no user has this role (e.g. check auth.ListUsers()).
func (r *RoleStore) DeleteRole(ctx context.Context, name string) error {
	name = strings.ToLower(strings.TrimSpace(name))
	if IsBuiltinRole(name) {
		return ErrCannotDeleteBuiltinRole
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.roles[name]; !exists {
		return ErrRoleNotFound
	}
	// The role's procedure grants go first, so a role created later under
	// this name doesn't inherit them.
	if err := NewProcedurePrivilegeStore(r.storage).revokeRole(name); err != nil {
		return err
	}
	nodeID := storage.NodeID(rolePrefix + name)
	if err := r.storage.DeleteNode(nodeID); err != nil {
		return err
	}
	delete(r.roles, name)
	return nil
}

// RenameRole renames a user-defined role (oldName -> newName). Fails if old is built-in,
// new is built-in or exists, or old does not exist. Does not update user nodes or allowlist;
// caller should update those or document that rename is for the role list only and
// allowlist/user assignments use the new name going forward.
func (r *RoleStore) RenameRole(ctx context.Context, oldName, newName string) error {
	oldName = strings.ToLower(strings.TrimSpace(oldName))
	newName = strings.ToLower(strings.TrimSpace(newName))
	if IsBuiltinRole(oldName) {
		return ErrCannotDeleteBuiltinRole
	}
	if IsBuiltinRole(newName) || newName == "" {
		return ErrRoleExists
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.roles[oldName]; !exists {
		return ErrRoleNotFound
	}
	if _, exists := r.roles[newName]; exists {
		return ErrRoleExists
	}
	oldID := storage.NodeID(rolePrefix + oldName)
	newNode := &storage.Node{
		ID:     storage.NodeID(rolePrefix + newName),
		Labels: []string{roleLabel, roleSystems},
		Properties: map[string]any{
			"name": newName,
		},
	}
	if _, err := r.storage.CreateNode(newNode); err != nil {
		return err
	}
	if err := r.storage.DeleteNode(oldID); err != nil {
		_ = r.storage.DeleteNode(newNode.ID)
		return err
	}
	delete(r.roles, oldName)
	r.roles[newName] = struct{}{}
	return nil
}

// Exists returns true if the role exists (built-in or user-defined).
func (r *RoleStore) Exists(name string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
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
