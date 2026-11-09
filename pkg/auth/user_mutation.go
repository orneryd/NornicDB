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
	return a.writeUserRecords(ctx, func() (userRecordChange, error) {
		user, exists := a.storedUser(username)
		if !exists {
			return userRecordChange{}, ErrUserNotFound
		}
		var updated *User
		var events []AuditEvent
		return userRecordChange{
			persist: func(ctx context.Context) error {
				return WithStoreTransaction(ctx, a.storage, func(view storage.Engine) error {
					staged := &Authenticator{
						storage: view, users: map[string]*User{username: cloneStoredUser(user)},
						auditLog: func(event AuditEvent) { events = append(events, event) },
					}
					if roles != nil {
						if err := staged.UpdateRoles(ctx, username, roles); err != nil {
							return err
						}
					}
					if disabled != nil {
						var err error
						if *disabled {
							err = staged.ChangeUserState(ctx, username, UserDisable)
						} else {
							err = staged.ChangeUserState(ctx, username, UserEnable)
						}
						if err != nil {
							return err
						}
					}
					updated = staged.users[username]
					return nil
				})
			},
			publish: func() {
				a.users[username] = updated
				for _, event := range events {
					a.logAudit(event)
				}
			},
		}, nil
	})
}
