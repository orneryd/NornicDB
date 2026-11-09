package auth

import (
	"context"
	"sync"
)

// User-record writes: one owner for every change of a stored user record,
// whether it comes from a login's bookkeeping, an account mutation, creation,
// deletion, the admin role/status update or a role rename or removal.
//
// writeUserRecords orders the change after every other one (recordOrder) and
// performs its storage I/O with no Authenticator lock held. mu is taken only
// to publish the outcome, so token and cached-credential checks
// (CurrentClaims, GetUser, ...) never wait for storage. Published records are
// never modified in place: a writer clones the record it changes. With every
// writer ordered here, the record a change was staged from is still the live
// one when the change publishes.

// recordWriteOrder is the ordering lock of writeUserRecords. The zero value is
// ready to use; lock honours the caller's context.
type recordWriteOrder struct {
	once sync.Once
	slot chan struct{}
}

func (o *recordWriteOrder) lock(ctx context.Context) error {
	o.once.Do(func() { o.slot = make(chan struct{}, 1) })
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case o.slot <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (o *recordWriteOrder) unlock() { <-o.slot }

// userRecordChange is what one writer stages.
type userRecordChange struct {
	// persist writes the stored records; it runs with no Authenticator lock.
	persist func(context.Context) error
	// publish updates the live records and records the success audit. It runs
	// under mu after persist succeeded, or, for best-effort bookkeeping,
	// before persist.
	publish func()
	// bestEffort publishes first and persists after, ignoring a storage
	// refusal: a login's bookkeeping never turns an accepted credential into
	// a failed login, and its lockout counting does not depend on storage.
	bestEffort bool
}

// writeUserRecords runs one user-record change through the ordered boundary.
// stage runs ordered and with no lock held: it reads the live records through
// storedUser, may hash or compare passwords, and returns the change, or an
// error that leaves everything unchanged.
func (a *Authenticator) writeUserRecords(ctx context.Context, stage func() (userRecordChange, error)) error {
	if err := a.recordOrder.lock(ctx); err != nil {
		return err
	}
	defer a.recordOrder.unlock()
	change, err := stage()
	if err != nil {
		return err
	}
	if change.bestEffort {
		a.publishUserRecords(change.publish)
		if change.persist != nil {
			_ = change.persist(ctx)
		}
		return nil
	}
	if change.persist != nil {
		if err := change.persist(ctx); err != nil {
			return err
		}
	}
	a.publishUserRecords(change.publish)
	return nil
}

func (a *Authenticator) publishUserRecords(publish func()) {
	if publish == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	publish()
}

// storedUser returns the live record of username. The record is immutable;
// a writer clones it before changing it.
func (a *Authenticator) storedUser(username string) (*User, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	user, ok := a.users[username]
	return user, ok
}

// audit records event under mu, for outcomes decided outside the lock.
func (a *Authenticator) audit(event AuditEvent) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.logAudit(event)
}
