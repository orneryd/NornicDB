package auth

import (
	"context"
	"fmt"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// RoleMutationStores are the loaded caches that must follow a role change.
// All supplied stores must use the role store's system database.
type RoleMutationStores struct {
	Allowlist    *AllowlistStore
	Privileges   *PrivilegesStore
	Entitlements *RoleEntitlementsStore
	Users        *Authenticator
}

// RenameRoleWithUsers retains the caller's loaded allowlist and user caches.
func (r *RoleStore) RenameRoleWithUsers(ctx context.Context, oldName, newName string, allowlist *AllowlistStore, users *Authenticator) error {
	return r.RenameRoleWithStores(ctx, oldName, newName, RoleMutationStores{Allowlist: allowlist, Users: users})
}

// RenameRoleWithStores moves every persisted role record and user assignment in
// one system-store transaction. Loaded caches change only after commit.
func (r *RoleStore) RenameRoleWithStores(ctx context.Context, oldName, newName string, stores RoleMutationStores) error {
	return r.changeRole(ctx, oldName, newName, false, stores)
}

// DeleteRoleWithStores removes every role-owned record and user assignment in
// the same transaction owner as rename, publishing loaded caches after commit.
func (r *RoleStore) DeleteRoleWithStores(ctx context.Context, name string, stores RoleMutationStores) error {
	return r.changeRole(ctx, name, "", true, stores)
}

func (r *RoleStore) changeRole(ctx context.Context, oldName, newName string, remove bool, stores RoleMutationStores) error {
	oldName = NormalizeRoleName(oldName)
	newName = NormalizeRoleName(newName)
	if IsBuiltinRole(oldName) {
		return ErrCannotDeleteBuiltinRole
	}
	if !remove && (IsBuiltinRole(newName) || newName == "") {
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
		if !remove && stagedRoles.Exists(newName) {
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
		records, err := roleOwnedRecords(view, oldName, newName)
		if err != nil {
			return err
		}
		stagedUsers, err = applyRoleRecords(ctx, view, records, oldName, newName, remove)
		if err != nil {
			return err
		}
		delete(stagedRoles.roles, oldName)
		if !remove {
			stagedRoles.roles[newName] = struct{}{}
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

type roleOwnedRecord struct {
	node            *storage.Node
	replacementID   storage.NodeID
	replacementRole string
	roleProperty    string
	user            *User // assignments share a user record; the user itself is retained
}

// roleOwnedRecords is the single enumerator for deletion and rename. Adding a
// record type here includes it in both operations. The role node is last, so
// its removal and every dependent record change commit together.
func roleOwnedRecords(view storage.Engine, oldName, newName string) ([]roleOwnedRecord, error) {
	var records []roleOwnedRecord
	for _, label := range []string{procedurePrivilegeLabel, dbPrivLabel, roleDbAccessLabel, roleEntitlementLabel, "_User", roleLabel} {
		nodes, err := view.GetNodesByLabel(label)
		if err != nil {
			return nil, err
		}
		for _, node := range nodes {
			record := roleOwnedRecord{node: node, replacementRole: newName, roleProperty: "role"}
			switch label {
			case procedurePrivilegeLabel:
				grant, valid := procedurePrivilegeNode(node)
				if !valid || grant.Role != NormalizeRoleName(oldName) {
					continue
				}
				grant.Role = NormalizeRoleName(newName)
				record.replacementID = procedurePrivilegeNodeID(grant)
				record.replacementRole = grant.Role
			case dbPrivLabel:
				role, database := roleDbFromNodeID(string(node.ID))
				if role != oldName || database == "" {
					continue
				}
				record.replacementID = storage.NodeID(dbPrivPrefix + newName + ":" + database)
			case roleDbAccessLabel:
				if roleFromNodeID(string(node.ID)) != oldName {
					continue
				}
				record.replacementID = storage.NodeID(roleDbAccessPrefix + newName)
			case roleEntitlementLabel:
				if roleFromEntitlementNodeID(string(node.ID)) != oldName {
					continue
				}
				record.replacementID = storage.NodeID(roleEntitlementPrefix + newName)
			case "_User":
				user := mapUserRecord(loadUserRecord, nil, node).User
				if node.ID != storage.NodeID("user:"+user.Username) {
					continue
				}
				found := false
				for _, role := range user.Roles {
					if NormalizeRoleName(string(role)) == oldName {
						found = true
						break
					}
				}
				if !found {
					continue
				}
				record.user = user
			case roleLabel:
				if roleNameFromNodeID(string(node.ID)) != oldName {
					continue
				}
				record.replacementID = storage.NodeID(rolePrefix + newName)
				record.roleProperty = "name"
			}
			records = append(records, record)
		}
	}
	return records, nil
}

// applyRoleRecords uses exactly the enumerated records for either operation.
// Rename preserves properties, including immutable grants. Deletion removes
// owned nodes and detaches the role from shared user records.
func applyRoleRecords(ctx context.Context, view storage.Engine, records []roleOwnedRecord, oldName, newName string, remove bool) (map[string]*User, error) {
	changedUsers := make(map[string]*User)
	for _, record := range records {
		if user := record.user; user != nil {
			roles := make([]Role, 0, len(user.Roles))
			for _, role := range user.Roles {
				if NormalizeRoleName(string(role)) == oldName {
					if !remove {
						roles = append(roles, Role(newName))
					}
				} else {
					roles = append(roles, role)
				}
			}
			user.Roles = roles
			user.UpdatedAt = time.Now()
			if err := (&Authenticator{storage: view}).saveUser(ctx, user, view); err != nil {
				return nil, err
			}
			changedUsers[user.Username] = user
			continue
		}
		if !remove {
			if _, err := view.GetNode(record.replacementID); err != storage.ErrNotFound {
				if err != nil {
					return nil, err
				}
				return nil, ErrRoleExists
			}
			copy := *record.node
			copy.ID = record.replacementID
			copy.Properties = make(map[string]any, len(record.node.Properties))
			for key, value := range record.node.Properties {
				copy.Properties[key] = value
			}
			copy.Properties[record.roleProperty] = record.replacementRole
			if _, err := view.CreateNode(&copy); err != nil {
				return nil, err
			}
		}
		if err := view.DeleteNode(record.node.ID); err != nil {
			return nil, err
		}
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
