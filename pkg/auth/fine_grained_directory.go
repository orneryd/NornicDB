package auth

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// SetSecurityDirectory attaches the server's authoritative role and user
// stores before the runtime is published to request handlers.
func (runtime *FineGrainedRuntime) SetSecurityDirectory(users *Authenticator, stores RoleMutationStores) {
	stores.Users = users
	if users != nil && users.storage != nil && users.fgacUsers == nil {
		provider, _ := users.storage.(storage.GraphMutationVersionProvider)
		users.fgacUsers = &fineGrainedUsers{provider: provider}
	}
	runtime.directory = stores
}

// RoleNames returns the same role catalog used by HTTP administration.
func (runtime *FineGrainedRuntime) RoleNames() []string {
	roles := ReadRoleCatalog(runtime.roles, nil).RoleNames
	if !slices.Contains(roles, FineGrainedPublicRole) {
		roles = append(roles, FineGrainedPublicRole)
	}
	return roles
}

// CreateRole creates a role in the shared security directory.
func (runtime *FineGrainedRuntime) CreateRole(ctx context.Context, role string) error {
	if NormalizeRoleName(role) == FineGrainedPublicRole {
		return ErrRoleExists
	}
	return runtime.roles.CreateRole(ctx, role)
}

// CreateRoleCopy creates a role that starts with the persisted privileges and
// assignments of another role. The role record and every copied catalog
// record commit in one transaction through the role-record owners, so a
// concurrent delete or rename of the source cannot leave a half-copied role.
// A source role that does not exist refuses the statement.
func (runtime *FineGrainedRuntime) CreateRoleCopy(ctx context.Context, role, source string) error {
	store := runtime.roles
	name := NormalizeRoleName(role)
	if name == FineGrainedPublicRole || IsBuiltinRole(name) {
		return ErrRoleExists
	}
	if name == "" {
		return ErrInvalidRoleName
	}
	from := NormalizeRoleName(source)
	if from == "" {
		return ErrInvalidRoleName
	}
	if err := store.writes.lock(ctx); err != nil {
		return err
	}
	defer store.writes.unlock()
	if store.Exists(name) {
		return ErrRoleExists
	}
	if from != FineGrainedPublicRole && !store.Exists(from) {
		return &RoleNotFoundError{Role: source}
	}
	locked := []string{from}
	if from == FineGrainedPublicRole {
		// PUBLIC has no stored role record and is never renamed or dropped.
		locked = nil
	}
	if err := withFineGrainedRoleRecords(ctx, store.storage, locked, func(view storage.Engine) error {
		if _, err := view.GetNode(ctx, storage.NodeID(rolePrefix+name)); err == nil {
			return ErrRoleExists
		} else if !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		catalog, _, err := readFineGrainedCatalog(ctx, view)
		if err != nil {
			return err
		}
		copied := false
		for _, privilege := range slices.Clone(catalog.Privileges) {
			if NormalizeRoleName(privilege.Role) != from {
				continue
			}
			privilege.Role = name
			catalog.Privileges = append(catalog.Privileges, privilege)
			copied = true
		}
		if ids, ok := catalog.Assignments[from]; ok {
			catalog.Assignments[name] = slices.Clone(ids)
			copied = true
		}
		record := &storage.Node{
			ID:         storage.NodeID(rolePrefix + name),
			Labels:     []string{roleLabel, roleSystems},
			Properties: map[string]any{"name": name},
		}
		if err := putRoleRecord(ctx, view, record, false); err != nil {
			return err
		}
		if !copied {
			return nil
		}
		return writeFineGrainedCatalog(ctx, view, catalog)
	}); err != nil {
		return err
	}
	store.mu.Lock()
	store.roles[name] = struct{}{}
	store.mu.Unlock()
	return nil
}

// RenameRole renames a role and all security records that reference it.
func (runtime *FineGrainedRuntime) RenameRole(ctx context.Context, role, name string) error {
	if NormalizeRoleName(role) == FineGrainedPublicRole || NormalizeRoleName(name) == FineGrainedPublicRole {
		return ErrCannotDeleteBuiltinRole
	}
	stores := runtime.directory
	stores.fineGrained = true
	return runtime.roles.RenameRoleWithStores(ctx, role, name, stores)
}

// DropRole removes a role, its privileges and its user memberships.
func (runtime *FineGrainedRuntime) DropRole(ctx context.Context, role string) error {
	if NormalizeRoleName(role) == FineGrainedPublicRole {
		return ErrCannotDeleteBuiltinRole
	}
	stores := runtime.directory
	stores.fineGrained = true
	return runtime.roles.DeleteRoleWithStores(ctx, role, stores)
}

// FineGrainedAccount is the account state one CREATE USER statement declares.
type FineGrainedAccount struct {
	Name                   string
	Password               string
	PasswordChangeRequired bool
	Suspended              bool
	HomeDatabase           string
	Providers              map[string]string
}

// CreateUser creates a native user in the directory used by authentication.
func (runtime *FineGrainedRuntime) CreateUser(ctx context.Context, name, password string, passwordChangeRequired bool) error {
	return runtime.CreateUserAccount(ctx, FineGrainedAccount{Name: name, Password: password, PasswordChangeRequired: passwordChangeRequired})
}

// CreateUserAccount creates one native account with every declared clause
// applied by the single user-record write: the password, its change
// requirement, the login state and the home database publish together.
func (runtime *FineGrainedRuntime) CreateUserAccount(ctx context.Context, account FineGrainedAccount) error {
	directory := runtime.directory.Users
	if directory == nil {
		return ErrFineGrainedForbidden
	}
	_, err := directory.createUserRecord(ctx, account.Name, account.Password, []Role{}, userCreateRecord{
		passwordChangeRequired: account.PasswordChangeRequired,
		disabled:               account.Suspended,
		homeDatabase:           account.HomeDatabase,
		providers:              account.Providers,
	})
	return err
}

// DeleteUser deletes a native user and invalidates their authenticated state.
func (runtime *FineGrainedRuntime) DeleteUser(ctx context.Context, name string) error {
	if runtime.directory.Users == nil {
		return ErrFineGrainedForbidden
	}
	return runtime.directory.Users.DeleteUser(ctx, name)
}

// RenameUser changes the stored login name and removes the old key atomically.
// The stable account ID and its role assignments stay with the account.
func (runtime *FineGrainedRuntime) RenameUser(ctx context.Context, username, next string) error {
	directory := runtime.directory.Users
	if directory == nil {
		return ErrFineGrainedForbidden
	}
	return directory.writeUserRecords(ctx, func() (userRecordChange, error) {
		current, found := directory.storedUser(username)
		if !found {
			return userRecordChange{}, ErrUserNotFound
		}
		if _, exists := directory.storedUser(next); exists {
			return userRecordChange{}, ErrUserExists
		}
		user := cloneStoredUser(current)
		user.Username, user.UpdatedAt = next, time.Now()
		return userRecordChange{
			persist: func(ctx context.Context) error {
				return withFineGrainedStoreTransaction(ctx, directory.storage, func(view storage.Engine) error {
					if err := directory.saveUser(ctx, user, view); err != nil {
						return err
					}
					return view.DeleteNode(ctx, storage.NodeID("user:"+username))
				})
			},
			publish: func() {
				delete(directory.users, username)
				directory.users[next] = user
				directory.notifyPasswordChange(username)
				directory.logAudit(AuditEvent{EventType: "user_rename", Username: next, UserID: user.ID, Success: true})
			},
		}, nil
	})
}

// FineGrainedUserAlteration is one ALTER USER statement's clauses. A nil field
// is a clause the statement did not write, so that part of the account keeps
// its stored value.
type FineGrainedUserAlteration struct {
	Password               *string
	PasswordChangeRequired *bool
	Suspended              *bool
	HomeDatabase           *string
	Providers              *FineGrainedProviderChange
}

// AlterUser applies every declared ALTER USER clause in one user-record
// transaction, so no reader observes a partly altered account.
func (runtime *FineGrainedRuntime) AlterUser(ctx context.Context, username string, change FineGrainedUserAlteration) error {
	directory := runtime.directory.Users
	if directory == nil {
		return ErrFineGrainedForbidden
	}
	operation, eventType := change.audit()
	var complete func()
	if change.Password != nil || change.Providers.removesNative() {
		complete = func() { directory.notifyPasswordChange(username) }
	}
	return directory.mutateStoredUserWithProviders(ctx, username, operation, func(user *User) (AuditEvent, error) {
		if change.Providers.removesNative() {
			user.PasswordHash = ""
			user.PasswordChangeRequired = false
		}
		if change.Password != nil {
			// A statement that omits CHANGE [NOT] REQUIRED keeps the stored
			// requirement instead of clearing it.
			if _, err := directory.replaceNativePassword(user, *change.Password, user.PasswordChangeRequired); err != nil {
				return AuditEvent{}, err
			}
		}
		if change.PasswordChangeRequired != nil {
			user.PasswordChangeRequired = *change.PasswordChangeRequired
		}
		if change.Suspended != nil {
			state := UserEnable
			if *change.Suspended {
				state = UserDisable
			}
			applyUserState(user, state)
		}
		if change.HomeDatabase != nil {
			user.HomeDatabase = *change.HomeDatabase
		}
		return AuditEvent{EventType: eventType}, nil
	}, complete, change.Providers)
}

// audit is the operation name and audit event of an alteration: a single
// clause keeps the directory event it has always written, while a combination
// of clauses shares one user_alter event.
func (change FineGrainedUserAlteration) audit() (string, string) {
	written := 0
	for _, declared := range []bool{change.Password != nil, change.PasswordChangeRequired != nil, change.Suspended != nil, change.HomeDatabase != nil, change.Providers != nil} {
		if declared {
			written++
		}
	}
	if written != 1 {
		return "user administration", "user_alter"
	}
	switch {
	case change.Password != nil:
		return "native password administration", "password_change"
	case change.PasswordChangeRequired != nil:
		return "native password administration", "password_change_requirement"
	case change.Suspended != nil:
		if *change.Suspended {
			return "user disable", "user_disable"
		}
		return "user enable", "user_enable"
	default:
		return "home database change", "user_home_change"
	}
}

// SetUserPassword updates a native secret or its change-required state.
func (runtime *FineGrainedRuntime) SetUserPassword(ctx context.Context, username string, password *string, changeRequired bool) error {
	return runtime.AlterUser(ctx, username, FineGrainedUserAlteration{Password: password, PasswordChangeRequired: &changeRequired})
}

// ChangeOwnPassword uses the same verified current-password mutation as HTTP.
func (runtime *FineGrainedRuntime) ChangeOwnPassword(ctx context.Context, username, current, replacement string) error {
	if runtime.directory.Users == nil || username == "" {
		return ErrFineGrainedForbidden
	}
	return runtime.directory.Users.ChangePassword(ctx, username, current, replacement)
}

// SetUserHome changes the account-specific database selected when no database
// is named. An empty name restores selection of the installation default.
func (runtime *FineGrainedRuntime) SetUserHome(ctx context.Context, username, database string) error {
	return runtime.AlterUser(ctx, username, FineGrainedUserAlteration{HomeDatabase: &database})
}

// SetUserSuspended applies the security directory's normal login state change.
func (runtime *FineGrainedRuntime) SetUserSuspended(ctx context.Context, username string, suspended bool) error {
	return runtime.AlterUser(ctx, username, FineGrainedUserAlteration{Suspended: &suspended})
}

func (runtime *FineGrainedRuntime) activeNativeUser(username string) *User {
	directory := runtime.directory.Users
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	user := directory.users[username]
	if user == nil || user.Disabled || time.Now().Before(user.LockedUntil) {
		return nil
	}
	safe := directory.copyUserSafe(user)
	safe.ID, safe.SecurityTags = user.ID, slices.Clone(user.SecurityTags)
	return safe
}

// Users returns the directory's public user records for SHOW ROLE WITH USERS.
func (runtime *FineGrainedRuntime) Users() []*User {
	if runtime.directory.Users == nil {
		return nil
	}
	return runtime.directory.Users.ListUsers()
}

// SetRoleMembership changes every cited user in the same system transaction.
// The role and user write owners serialize it with other directory mutations.
func (runtime *FineGrainedRuntime) SetRoleMembership(ctx context.Context, users, roles []string, assign bool) error {
	directory := runtime.directory.Users
	if directory == nil {
		return ErrFineGrainedForbidden
	}
	if err := runtime.roles.writes.lock(ctx); err != nil {
		return err
	}
	defer runtime.roles.writes.unlock()
	if !sameSystemStore(runtime.roles.storage, directory.storage) {
		return storage.ErrCrossNamespaceTransaction
	}
	return directory.writeUserRecords(ctx, func() (userRecordChange, error) {
		staged := make(map[string]*User, len(users))
		for _, name := range users {
			current, found := directory.storedUser(name)
			if !found {
				return userRecordChange{}, ErrUserNotFound
			}
			user := cloneStoredUser(current)
			for _, name := range roles {
				role := Role(NormalizeRoleName(name))
				if assign {
					if !slices.Contains(user.Roles, role) {
						user.Roles = append(user.Roles, role)
					}
				} else {
					user.Roles = slices.DeleteFunc(user.Roles, func(value Role) bool {
						return NormalizeRoleName(string(value)) == string(role)
					})
				}
			}
			user.UpdatedAt = time.Now()
			staged[user.Username] = user
		}
		return userRecordChange{
			persist: func(ctx context.Context) error {
				return withFineGrainedRoleRecords(ctx, runtime.roles.storage, roles, func(view storage.Engine) error {
					for _, user := range staged {
						if err := directory.saveUser(ctx, user, view); err != nil {
							return err
						}
					}
					return nil
				})
			},
			publish: func() {
				for name, user := range staged {
					directory.users[name] = user
					directory.logAudit(AuditEvent{EventType: "role_change", Username: name, UserID: user.ID, Success: true})
				}
			},
		}, nil
	})
}

// Impersonate resolves an authorized, active native account and admits its
// roles. The actor's grants never enter the impersonated request.
func (runtime *FineGrainedRuntime) Impersonate(ctx context.Context, actor *FineGrainedRequest, username string) (*FineGrainedRequest, *User, error) {
	if runtime.directory.Users == nil || actor.PasswordChangeRequired || !actor.AllowsDBMS("dbms.user.impersonate", username) {
		return nil, nil, ErrFineGrainedForbidden
	}
	if err := runtime.directory.Users.refreshFineGrainedUsers(ctx); err != nil {
		return nil, nil, err
	}
	user := runtime.activeNativeUser(username)
	if user == nil {
		return nil, nil, ErrFineGrainedImpersonationTarget
	}
	roles := make([]string, len(user.Roles))
	for i, role := range user.Roles {
		roles[i] = string(role)
	}
	ctx = context.WithValue(ctx, fineGrainedProviderSessionKey{}, (*FineGrainedProviderSession)(nil))
	ctx = WithRequestIdentity(ctx, RequestIdentity{ID: "sub:" + user.ID, Username: user.Username})
	request, err := runtime.Admit(ctx, roles)
	if err != nil {
		return nil, nil, err
	}
	// Credential expiry applies to the actor's login. The selected account
	// supplies authorization and has no credential exchange in this request.
	request.PasswordChangeRequired = false
	request.impersonated = actor.identity.Username != user.Username
	return request, user, nil
}
