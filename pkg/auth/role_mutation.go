package auth

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// RoleRenameStores are the loaded caches that must follow a role rename.
// All supplied stores must use the role store's system database.
type RoleRenameStores struct {
	Allowlist    *AllowlistStore
	Privileges   *PrivilegesStore
	Entitlements *RoleEntitlementsStore
	Users        *Authenticator
}

// RenameRoleWithUsers retains the caller's loaded allowlist and user caches.
func (r *RoleStore) RenameRoleWithUsers(ctx context.Context, oldName, newName string, allowlist *AllowlistStore, users *Authenticator) error {
	return r.RenameRoleWithStores(ctx, oldName, newName, RoleRenameStores{Allowlist: allowlist, Users: users})
}

// RenameRoleWithStores moves every persisted role record and user assignment in
// one system-store transaction. Loaded caches change only after commit.
func (r *RoleStore) RenameRoleWithStores(ctx context.Context, oldName, newName string, stores RoleRenameStores) error {
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
	allowlist, privileges, entitlements, users := stores.Allowlist, stores.Privileges, stores.Entitlements, stores.Users
	if allowlist != nil {
		allowlist.mu.Lock()
		defer allowlist.mu.Unlock()
	}
	if privileges != nil {
		privileges.mu.Lock()
		defer privileges.mu.Unlock()
	}
	if entitlements != nil {
		entitlements.mu.Lock()
		defer entitlements.mu.Unlock()
	}
	if users != nil {
		users.mu.Lock()
		defer users.mu.Unlock()
	}
	var stagedRoles *RoleStore
	var stagedAllowlist *AllowlistStore
	var stagedPrivileges *PrivilegesStore
	var stagedEntitlements *RoleEntitlementsStore
	var stagedUsers map[string]*User
	err := WithStoreTransaction(ctx, r.storage, func(view storage.Engine) error {
		stagedRoles = NewRoleStore(view)
		if err := stagedRoles.Load(ctx); err != nil {
			return err
		}
		if !stagedRoles.Exists(oldName) {
			return ErrRoleNotFound
		}
		if stagedRoles.Exists(newName) {
			return ErrRoleExists
		}
		if allowlist != nil && !sameSystemStore(r.storage, allowlist.storage) {
			return storage.ErrCrossNamespaceTransaction
		}
		if privileges != nil && !sameSystemStore(r.storage, privileges.storage) {
			return storage.ErrCrossNamespaceTransaction
		}
		if entitlements != nil && !sameSystemStore(r.storage, entitlements.storage) {
			return storage.ErrCrossNamespaceTransaction
		}
		if users != nil && !sameSystemStore(r.storage, users.storage) {
			return storage.ErrCrossNamespaceTransaction
		}
		newNode := &storage.Node{ID: storage.NodeID(rolePrefix + newName), Labels: []string{roleLabel, roleSystems}, Properties: map[string]any{"name": newName}}
		if _, err := view.CreateNode(newNode); err != nil {
			return err
		}
		if err := view.DeleteNode(storage.NodeID(rolePrefix + oldName)); err != nil {
			return err
		}
		delete(stagedRoles.roles, oldName)
		stagedRoles.roles[newName] = struct{}{}
		if err := renameRoleRecords(view, oldName, newName); err != nil {
			return err
		}
		var err error
		stagedUsers, err = renameRoleAssignments(view, oldName, newName)
		if err != nil {
			return err
		}
		if allowlist != nil {
			stagedAllowlist = NewAllowlistStore(view)
			if err := stagedAllowlist.Load(ctx); err != nil {
				return err
			}
		}
		if privileges != nil {
			stagedPrivileges = NewPrivilegesStore(view)
			if err := stagedPrivileges.Load(ctx); err != nil {
				return err
			}
		}
		if entitlements != nil {
			stagedEntitlements = NewRoleEntitlementsStore(view)
			if err := stagedEntitlements.Load(ctx); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	r.roles = stagedRoles.roles
	if allowlist != nil {
		allowlist.allowlist = stagedAllowlist.allowlist
	}
	if privileges != nil {
		privileges.matrix = stagedPrivileges.matrix
	}
	if entitlements != nil {
		entitlements.entitlements = stagedEntitlements.entitlements
	}
	if users != nil {
		for username, user := range stagedUsers {
			var oldRoles []Role
			if old := users.users[username]; old != nil {
				oldRoles = old.Roles
			}
			users.users[username] = user
			users.logAudit(AuditEvent{EventType: "role_change", Username: username, UserID: user.ID, Success: true,
				Details: fmt.Sprintf("roles changed from %v to %v", oldRoles, user.Roles)})
		}
	}
	return nil
}

// renameRoleRecords preserves each record's properties, including immutable
// grants, while changing the canonical ID and role property together.
func renameRoleRecords(view storage.Engine, oldName, newName string) error {
	for _, label := range []string{procedurePrivilegeLabel, dbPrivLabel, roleDbAccessLabel, roleEntitlementLabel} {
		nodes, err := view.GetNodesByLabel(label)
		if err != nil {
			return err
		}
		for _, node := range nodes {
			var newID storage.NodeID
			switch label {
			case procedurePrivilegeLabel:
				grant, valid := procedurePrivilegeNode(node)
				if !valid || grant.Role != normalizeGrantRole(oldName) {
					continue
				}
				grant.Role = normalizeGrantRole(newName)
				newID = procedurePrivilegeNodeID(grant)
			case dbPrivLabel:
				role, database := roleDbFromNodeID(string(node.ID))
				if role != oldName || database == "" {
					continue
				}
				newID = storage.NodeID(dbPrivPrefix + newName + ":" + database)
			case roleDbAccessLabel:
				if roleFromNodeID(string(node.ID)) != oldName {
					continue
				}
				newID = storage.NodeID(roleDbAccessPrefix + newName)
			case roleEntitlementLabel:
				if roleFromEntitlementNodeID(string(node.ID)) != oldName {
					continue
				}
				newID = storage.NodeID(roleEntitlementPrefix + newName)
			}
			if _, err := view.GetNode(newID); err != storage.ErrNotFound {
				if err != nil {
					return err
				}
				return ErrRoleExists
			}
			copy := *node
			copy.ID = newID
			copy.Properties = make(map[string]any, len(node.Properties))
			for key, value := range node.Properties {
				copy.Properties[key] = value
			}
			role := newName
			if label == procedurePrivilegeLabel {
				role = normalizeGrantRole(newName)
			}
			copy.Properties["role"] = role
			if _, err := view.CreateNode(&copy); err != nil {
				return err
			}
			if err := view.DeleteNode(node.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

func renameRoleAssignments(view storage.Engine, oldName, newName string) (map[string]*User, error) {
	nodes, err := view.GetNodesByLabel("_User")
	if err != nil {
		return nil, err
	}
	staged := &Authenticator{storage: view}
	changedUsers := make(map[string]*User)
	for _, node := range nodes {
		user, _ := staged.userFromNode(node)
		if node.ID != storage.NodeID("user:"+user.Username) {
			continue
		}
		changed := false
		for i, role := range user.Roles {
			if strings.EqualFold(string(role), oldName) {
				user.Roles[i] = Role(newName)
				changed = true
			}
		}
		if !changed {
			continue
		}
		user.UpdatedAt = time.Now()
		if err := staged.saveUserInTransaction(user); err != nil {
			return nil, err
		}
		changedUsers[user.Username] = user
	}
	return changedUsers, nil
}

func sameSystemStore(a, b storage.Engine) bool {
	if storage.UnwrapEngine(a) != storage.UnwrapEngine(b) {
		return false
	}
	namespace := func(engine storage.Engine) string {
		if scoped, ok := engine.(interface{ Namespace() string }); ok {
			return scoped.Namespace()
		}
		return ""
	}
	return namespace(a) == namespace(b)
}
