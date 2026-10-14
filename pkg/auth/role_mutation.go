package auth

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// RenameRoleWithUsers renames the role, its allowlist and all user assignments
// in one system-store transaction. The stores' caches change only after commit.
func (r *RoleStore) RenameRoleWithUsers(ctx context.Context, oldName, newName string, allowlist *AllowlistStore, users *Authenticator) error {
	oldName = strings.ToLower(strings.TrimSpace(oldName))
	newName = strings.ToLower(strings.TrimSpace(newName))
	r.mu.Lock()
	defer r.mu.Unlock()
	if allowlist != nil {
		allowlist.mu.Lock()
		defer allowlist.mu.Unlock()
	}
	if users != nil {
		users.mu.Lock()
		defer users.mu.Unlock()
	}
	var stagedRoles *RoleStore
	var stagedAllowlist *AllowlistStore
	stagedUsers := make(map[string]*User)
	err := WithStoreTransaction(ctx, r.storage, func(view storage.Engine) error {
		stagedRoles = NewRoleStore(view)
		if err := stagedRoles.Load(ctx); err != nil {
			return err
		}
		if err := stagedRoles.RenameRole(ctx, oldName, newName); err != nil {
			return err
		}
		if allowlist != nil {
			if !sameSystemStore(r.storage, allowlist.storage) {
				return storage.ErrCrossNamespaceTransaction
			}
			stagedAllowlist = NewAllowlistStore(view)
			if err := stagedAllowlist.Load(ctx); err != nil {
				return err
			}
			if err := stagedAllowlist.RenameRoleInAllowlist(ctx, oldName, newName); err != nil {
				return err
			}
		}
		if users != nil {
			for username, user := range users.users {
				copy := cloneStoredUser(user)
				changed := false
				for i, role := range copy.Roles {
					if strings.EqualFold(string(role), oldName) {
						copy.Roles[i] = Role(newName)
						changed = true
					}
				}
				if changed {
					if !sameSystemStore(r.storage, users.storage) {
						return storage.ErrCrossNamespaceTransaction
					}
					copy.UpdatedAt = time.Now()
					staged := &Authenticator{storage: view}
					if err := staged.saveUserInTransaction(copy); err != nil {
						return err
					}
					stagedUsers[username] = copy
				}
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
	for username, user := range stagedUsers {
		oldRoles := users.users[username].Roles
		users.users[username] = user
		users.logAudit(AuditEvent{
			EventType: "role_change", Username: username, UserID: user.ID, Success: true,
			Details: fmt.Sprintf("roles changed from %v to %v", oldRoles, user.Roles),
		})
	}
	return nil
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
