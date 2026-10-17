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
				err = staged.DisableUser(username)
			} else {
				err = staged.EnableUser(username)
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

// ModifyUsersRoles changes the roles of every user in usernames with change,
// which gets a user's current roles and returns its new ones and whether
// they differ. Reading, changing and writing happen under the lock every
// user mutation takes, in one store transaction: a concurrent role change
// is either wholly before (and change sees its result) or wholly after, so
// none is lost; every changed user is written or none is; and the user
// cache and audit events publish only after the whole set commits. A
// missing user is ErrUserNotFound, before anything is written, unless
// skipMissing.
func (a *Authenticator) ModifyUsersRoles(ctx context.Context, usernames []string, change func(current []Role) ([]Role, bool), skipMissing bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	var present []string
	for _, username := range usernames {
		if _, exists := a.users[username]; exists {
			present = append(present, username)
		} else if !skipMissing {
			return ErrUserNotFound
		}
	}
	if len(present) == 0 {
		return nil
	}
	var staged *Authenticator
	var events []AuditEvent
	err := WithStoreTransaction(ctx, a.storage, func(view storage.Engine) error {
		events = events[:0]
		staged = &Authenticator{
			storage: view, users: make(map[string]*User, len(present)),
			auditLog: func(event AuditEvent) { events = append(events, event) },
		}
		for _, username := range present {
			user, seen := staged.users[username]
			if !seen {
				user = cloneStoredUser(a.users[username])
				staged.users[username] = user
			}
			next, changed := change(append([]Role(nil), user.Roles...))
			if !changed {
				continue
			}
			if err := staged.UpdateRoles(username, next); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for username, user := range staged.users {
		a.users[username] = user
	}
	for _, event := range events {
		a.logAudit(event)
	}
	return nil
}
