package auth

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// FineGrainedProviderChange is a single ALTER USER provider transition.
// Removal is applied before new bindings are assigned.
type FineGrainedProviderChange struct {
	Set       map[string]string
	Remove    []string
	RemoveAll bool
}

func (change *FineGrainedProviderChange) removesNative() bool {
	return change != nil && (change.RemoveAll || slices.Contains(change.Remove, "native"))
}

// ErrFineGrainedProviderRequired reports an account alteration that would
// leave the account without any authentication provider.
var ErrFineGrainedProviderRequired = errors.New("a user requires at least one auth provider")

func (a *Authenticator) persistUserAndProviders(user *User, change FineGrainedProviderChange) func(context.Context) error {
	return func(ctx context.Context) error {
		return withFineGrainedStoreTransaction(ctx, a.storage, func(view storage.Engine) error {
			providers := make(map[string]string)
			if !change.RemoveAll {
				current, err := readFineGrainedProviderBindings(ctx, view, user.ID)
				if err != nil {
					return err
				}
				for name, subject := range current {
					providers[name] = subject
				}
				for _, name := range change.Remove {
					delete(providers, name)
				}
			}
			for name, subject := range change.Set {
				if name == "native" || strings.TrimSpace(name) == "" || strings.TrimSpace(subject) == "" {
					return ErrInvalidFineGrainedPolicy
				}
				if err := retireOrphanedProviderBinding(ctx, view, user.ID, name, subject); err != nil {
					return err
				}
				providers[name] = subject
			}
			if user.PasswordHash == "" && len(providers) == 0 {
				return ErrFineGrainedProviderRequired
			}
			if err := writeFineGrainedProviderBindings(ctx, view, user.ID, providers); err != nil {
				return err
			}
			return a.saveUser(ctx, user, view)
		})
	}
}

// An RBAC deletion leaves FGAC sidecars dormant. During a future binding
// write, retire the old sidecar only after the system view proves its account
// ID is absent.
func retireOrphanedProviderBinding(ctx context.Context, view storage.Engine, userID, provider, subject string) error {
	owner, err := fineGrainedProviderUserID(ctx, view, provider, subject)
	if errors.Is(err, storage.ErrNotFound) || err == nil && owner == userID {
		return nil
	}
	if err != nil {
		return err
	}
	exists := false
	if err := view.StreamNodesWithOptions(ctx, storage.StreamNodesOptions{
		Label: "_User", Projection: []string{"id"}, PropertyStringEquals: map[string]string{"id": owner},
	}, func(node *storage.Node) error {
		if node.Properties["id"] == owner && slices.Contains(node.Labels, "_User") {
			exists = true
			return storage.ErrIterationStopped
		}
		return nil
	}); err != nil {
		return err
	}
	if exists {
		return errFineGrainedProviderSubjectBound
	}
	return writeFineGrainedProviderBindings(ctx, view, owner, nil)
}

// FineGrainedUserProvider is the authorized SHOW USERS WITH AUTH row. The
// native row describes account state without including credential material.
type FineGrainedUserProvider struct {
	User                   *User
	Name                   string
	Subject                string
	HasNative              bool
	PasswordChangeRequired bool
}

// UserProviders returns the configured providers of the current user
// directory for an administrator authorized to list users.
func (runtime *FineGrainedRuntime) UserProviders(ctx context.Context) ([]FineGrainedUserProvider, error) {
	directory := runtime.directory.Users
	if directory == nil {
		return nil, ErrFineGrainedForbidden
	}
	if err := directory.refreshFineGrainedUsers(ctx); err != nil {
		return nil, err
	}
	directory.mu.RLock()
	users := make([]*User, 0, len(directory.users))
	for _, user := range directory.users {
		users = append(users, user)
	}
	directory.mu.RUnlock()

	slices.SortFunc(users, func(a, b *User) int { return strings.Compare(a.Username, b.Username) })
	var providers []FineGrainedUserProvider
	for _, user := range users {
		bindings, err := readFineGrainedProviderBindings(ctx, directory.storage, user.ID)
		if err != nil {
			return nil, err
		}
		safe := directory.copyUserSafe(user)
		names := make([]string, 0, len(bindings)+1)
		for name := range bindings {
			names = append(names, name)
		}
		native := user.PasswordHash != ""
		if native {
			names = append(names, "native")
		}
		slices.Sort(names)
		for _, name := range names {
			providers = append(providers, FineGrainedUserProvider{
				User: safe, Name: name, Subject: bindings[name], HasNative: native, PasswordChangeRequired: user.PasswordChangeRequired,
			})
		}
	}
	return providers, nil
}

func (a *Authenticator) fineGrainedUserForProvider(ctx context.Context, provider, subject string) (*User, error) {
	if a.fgacUsers == nil || strings.TrimSpace(provider) == "" || strings.TrimSpace(subject) == "" {
		return nil, ErrInvalidCredentials
	}
	if err := a.refreshFineGrainedUsers(ctx); err != nil {
		return nil, err
	}
	userID, err := fineGrainedProviderUserID(ctx, a.storage, provider, subject)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, ErrInvalidCredentials
	}
	if err != nil {
		return nil, err
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, user := range a.users {
		if user.ID == userID && !user.Disabled && !time.Now().Before(user.LockedUntil) {
			result := a.copyUserSafe(user)
			result.ID = user.ID
			return result, nil
		}
	}
	return nil, ErrInvalidCredentials
}
