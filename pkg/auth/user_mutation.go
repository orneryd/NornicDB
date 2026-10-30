package auth

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// UpdateRolesAndStatus applies the requested roles and account status together.
// A nil argument leaves that field unchanged. Existing user mutations stage
// their writes in the shared transaction; caches and audit events publish
// only after the whole request commits.
func (a *Authenticator) UpdateRolesAndStatus(ctx context.Context, username string, roles []Role, disabled *bool) error {
	if roles == nil && disabled == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	user, exists := a.users[username]
	if !exists {
		return ErrUserNotFound
	}
	var updated *User
	var events []AuditEvent
	err := WithStoreTransaction(ctx, a.storage, func(view storage.Engine) error {
		staged := &Authenticator{
			storage: view, users: map[string]*User{username: cloneStoredUser(user)},
			auditLog: func(event AuditEvent) { events = append(events, event) },
		}
		if roles != nil {
			if err := staged.UpdateRoles(username, roles); err != nil {
				return err
			}
		}
		if disabled != nil {
			var err error
			if *disabled {
				err = staged.ChangeUserState(username, UserDisable)
			} else {
				err = staged.ChangeUserState(username, UserEnable)
			}
			if err != nil {
				return err
			}
		}
		updated = staged.users[username]
		return nil
	})
	if err != nil {
		return err
	}
	a.users[username] = updated
	for _, event := range events {
		a.logAudit(event)
	}
	return nil
}
