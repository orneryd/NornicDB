package auth

import (
	"context"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// fineGrainedUsers tracks the system namespace revision observed while the
// authoritative user records were loaded. It exists only in FGAC mode.
type fineGrainedUsers struct {
	provider storage.GraphMutationVersionProvider
	version  atomic.Uint64
}

func (a *Authenticator) refreshFineGrainedUsers(ctx context.Context) error {
	directory := a.fgacUsers
	if directory == nil {
		return nil
	}
	if directory.provider != nil {
		version, valid := directory.provider.GraphMutationVersion()
		if !valid {
			return ErrFineGrainedForbidden
		}
		if directory.version.Load() == version+1 {
			return nil
		}
	}
	if err := a.recordOrder.lock(ctx); err != nil {
		return err
	}
	defer a.recordOrder.unlock()
	return a.refreshFineGrainedUsersOrdered(ctx)
}

// Refresh under the normal publication order so an in-process user mutation
// cannot replace the cache with records from an earlier snapshot.
func (a *Authenticator) refreshFineGrainedUsersOrdered(ctx context.Context) error {
	return a.reloadUsersOrdered(ctx, false)
}

// ReloadUsersAfterRestore replaces the live account directory with the
// restored system records. The record-order boundary prevents a concurrent
// account mutation from publishing an older directory after this reload.
func (a *Authenticator) ReloadUsersAfterRestore(ctx context.Context) error {
	if a == nil {
		return nil
	}
	if err := a.recordOrder.lock(ctx); err != nil {
		return err
	}
	defer a.recordOrder.unlock()
	if err := a.reloadUsersOrdered(ctx, true); err != nil {
		// The storage replacement has committed. Keep no pre-restore
		// principal authorized if the new directory cannot be read.
		a.publishUserRecords(func() {
			for name := range a.users {
				a.notifyPasswordChange(name)
			}
			a.users = make(map[string]*User)
		})
		return err
	}
	return nil
}

func (a *Authenticator) reloadUsersOrdered(ctx context.Context, force bool) error {
	directory := a.fgacUsers
	var version uint64
	if directory != nil && directory.provider != nil {
		var valid bool
		version, valid = directory.provider.GraphMutationVersion()
		if !valid {
			return ErrFineGrainedForbidden
		}
		if !force && directory.version.Load() == version+1 {
			return nil
		}
	}

	users, err := a.readStoredUsers(ctx)
	if err != nil {
		return err
	}
	if directory != nil && directory.provider != nil {
		after, valid := directory.provider.GraphMutationVersion()
		if !valid || after != version {
			return ErrFineGrainedForbidden
		}
	}
	a.publishUserRecords(func() {
		for name, previous := range a.users {
			current := users[name]
			if force || current == nil || current.ID != previous.ID || current.PasswordHash != previous.PasswordHash ||
				current.Disabled != previous.Disabled || !current.LockedUntil.Equal(previous.LockedUntil) {
				a.notifyPasswordChange(name)
			}
		}
		a.users = users
	})
	if directory != nil {
		directory.version.Store(version + 1)
	}
	return nil
}

// refreshFineGrainedRoles refreshes the name cache from the same system
// namespace used by policy admission and returns the observed revision.
func (runtime *FineGrainedRuntime) refreshFineGrainedRoles(ctx context.Context) (uint64, error) {
	roles := runtime.roles
	provider, ok := roles.storage.(storage.GraphMutationVersionProvider)
	if !ok {
		return 0, nil
	}
	version, valid := provider.GraphMutationVersion()
	if !valid {
		return 0, ErrFineGrainedForbidden
	}
	if runtime.rolesVersion.Load() == version+1 {
		return version, nil
	}
	if err := roles.writes.lock(ctx); err != nil {
		return 0, err
	}
	defer roles.writes.unlock()
	version, valid = provider.GraphMutationVersion()
	if !valid {
		return 0, ErrFineGrainedForbidden
	}
	if runtime.rolesVersion.Load() == version+1 {
		return version, nil
	}
	if err := roles.Load(ctx); err != nil {
		return 0, err
	}
	after, valid := provider.GraphMutationVersion()
	if !valid || after != version {
		return 0, ErrFineGrainedForbidden
	}
	runtime.rolesVersion.Store(version + 1)
	return version, nil
}

// CurrentBasicClaims validates a cached Basic authentication against the
// refreshed directory. If refresh invalidated the cache, the caller checks
// the credentials again through Authenticate.
func (a *Authenticator) CurrentBasicClaims(claims *JWTClaims, cache *BasicAuthCache, generation uint64) (*JWTClaims, error) {
	return a.CurrentBasicClaimsContext(context.Background(), claims, cache, generation)
}

// CurrentBasicClaimsContext verifies cached Basic claims with the caller's lifetime.
func (a *Authenticator) CurrentBasicClaimsContext(ctx context.Context, claims *JWTClaims, cache *BasicAuthCache, generation uint64) (*JWTClaims, error) {
	current, err := a.CurrentClaimsContext(ctx, claims)
	if err != nil {
		return nil, err
	}
	if a.fgacUsers != nil && cache.Generation() != generation {
		return nil, ErrInvalidCredentials
	}
	return current, nil
}
