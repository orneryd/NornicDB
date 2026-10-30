package txsession

import (
	"context"
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type managerBeginGateEngine struct {
	storage.Engine
	beginner interface {
		BeginTransaction() (*storage.BadgerTransaction, error)
	}
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (e *managerBeginGateEngine) BeginTransaction() (*storage.BadgerTransaction, error) {
	e.once.Do(func() { close(e.entered) })
	<-e.release
	return e.beginner.BeginTransaction()
}

func TestManagerTouchAfterCommitDoesNotRestartExpiry(t *testing.T) {
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { _ = store.Close() })
	manager := NewManager(time.Hour, func(string) (*cypher.StorageExecutor, error) {
		return cypher.NewStorageExecutor(storage.NewNamespacedEngine(store, "nornic")), nil
	})
	t.Cleanup(func() { _ = manager.Close() })
	session, err := manager.Open(context.Background(), "nornic")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		session.mu.Lock()
		manager.stopExpiryTimerLocked(session)
		session.mu.Unlock()
	})
	if _, err := manager.CommitAndDelete(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	manager.Touch(session)
	session.mu.Lock()
	timer := session.expiryTimer
	session.mu.Unlock()
	if timer != nil {
		t.Fatal("Touch restarted the expiry timer after commit removed the session")
	}
	if _, ok := manager.Get(session.ID); ok {
		t.Fatal("committed session was re-registered")
	}
}

func TestManagerCloseRollsBackSessionsAndStopsTimers(t *testing.T) {
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { _ = store.Close() })
	manager := NewManager(time.Hour, func(string) (*cypher.StorageExecutor, error) {
		return cypher.NewStorageExecutor(storage.NewNamespacedEngine(store, "nornic")), nil
	})
	t.Cleanup(func() { _ = manager.Close() })
	session, err := manager.Open(context.Background(), "nornic")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ExecuteInSession(context.Background(), session, "CREATE (:ManagerCloseLifecycle {id: 'nornic:manager-close-test'})", nil); err != nil {
		t.Fatal(err)
	}

	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := manager.Get(session.ID); ok {
		t.Fatal("Close left the transaction registered")
	}
	session.mu.Lock()
	timer, expired := session.expiryTimer, session.expired
	session.mu.Unlock()
	if timer != nil || !expired {
		t.Fatalf("Close left the timer/session active: timer=%v expired=%t", timer != nil, expired)
	}

	check := cypher.NewStorageExecutor(storage.NewNamespacedEngine(store, "nornic"))
	result, err := check.Execute(context.Background(), "MATCH (n:ManagerCloseLifecycle) RETURN count(n) AS count", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != int64(0) {
		t.Fatalf("Close did not roll back the open transaction: rows=%#v", result.Rows)
	}
	if err := manager.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
	if _, err := manager.Open(context.Background(), "nornic"); err == nil {
		t.Fatal("closed manager accepted a new transaction")
	}
}

func TestManagerOpenRacingCloseRollsBackUnregisteredTransaction(t *testing.T) {
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { _ = store.Close() })
	engine := &managerBeginGateEngine{
		Engine:   store,
		beginner: store,
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseBegin := func() { releaseOnce.Do(func() { close(engine.release) }) }
	t.Cleanup(releaseBegin)
	manager := NewManager(time.Hour, nil)
	t.Cleanup(func() { _ = manager.Close() })
	executor := cypher.NewStorageExecutor(engine)
	opened := make(chan error, 1)
	go func() {
		_, err := manager.OpenWithExecutorForOwner(context.Background(), "nornic", executor, "")
		opened <- err
	}()
	select {
	case <-engine.entered:
	case <-time.After(3 * time.Second):
		releaseBegin()
		t.Fatal("BEGIN did not reach the injected gate")
	}
	if err := manager.Close(); err != nil {
		t.Fatalf("close with no registered sessions failed: %v", err)
	}
	releaseBegin()
	select {
	case err := <-opened:
		if err == nil || !strings.Contains(err.Error(), "transaction manager is closed") {
			t.Fatalf("open racing Close returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("open did not roll back after Close rejected registration")
	}
}

func TestManagerAcquireExpiredSessionRejectsAndExpires(t *testing.T) {
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { _ = store.Close() })
	manager := NewManager(time.Hour, func(string) (*cypher.StorageExecutor, error) {
		return cypher.NewStorageExecutor(storage.NewNamespacedEngine(store, "nornic")), nil
	})
	t.Cleanup(func() { _ = manager.Close() })
	session, err := manager.Open(context.Background(), "nornic")
	if err != nil {
		t.Fatal(err)
	}
	expires := session.ExpiresAt()
	manager.nowFunc = func() time.Time { return expires.Add(time.Nanosecond) }
	if lease, ok := manager.AcquireForOwner(session.ID, ""); ok || lease != nil {
		t.Fatal("AcquireForOwner accepted an expired session")
	}
	if _, ok := manager.Get(session.ID); ok {
		t.Fatal("expired session remained registered")
	}
	if _, err := manager.ExecuteInSession(context.Background(), session, "RETURN 1", nil); err == nil {
		t.Fatal("ExecuteInSession accepted an expired session")
	}
	if _, err := manager.CommitAndDelete(context.Background(), session); err == nil {
		t.Fatal("CommitAndDelete accepted an expired session")
	}
	if err := manager.RollbackAndDelete(context.Background(), session); err != nil {
		t.Fatalf("RollbackAndDelete of an expired session failed: %v", err)
	}
	session.mu.Lock()
	timer, expired := session.expiryTimer, session.expired
	session.mu.Unlock()
	if timer != nil || !expired {
		t.Fatalf("expiry did not retire the session: timer=%v expired=%t", timer != nil, expired)
	}
}

func TestManagerDeleteRemovesSessionAndStopsTimer(t *testing.T) {
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { _ = store.Close() })
	manager := NewManager(time.Hour, func(string) (*cypher.StorageExecutor, error) {
		return cypher.NewStorageExecutor(storage.NewNamespacedEngine(store, "nornic")), nil
	})
	t.Cleanup(func() { _ = manager.Close() })
	session, err := manager.Open(context.Background(), "nornic")
	if err != nil {
		t.Fatal(err)
	}
	manager.Delete(session.ID)
	if _, ok := manager.Get(session.ID); ok {
		t.Fatal("Delete left the session registered")
	}
	session.mu.Lock()
	timer := session.expiryTimer
	session.mu.Unlock()
	if timer != nil {
		t.Fatal("Delete left the session expiry timer active")
	}
	_, _ = session.Executor.Execute(context.Background(), "ROLLBACK", nil)
}

func TestManagerExpiryIgnoresStaleAndRemovedSessions(t *testing.T) {
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { _ = store.Close() })
	manager := NewManager(time.Hour, func(string) (*cypher.StorageExecutor, error) {
		return cypher.NewStorageExecutor(storage.NewNamespacedEngine(store, "nornic")), nil
	})
	t.Cleanup(func() { _ = manager.Close() })
	session, err := manager.Open(context.Background(), "nornic")
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	staleGeneration := session.expiryGeneration
	session.mu.Unlock()
	manager.Touch(session)
	manager.expireSession(session, staleGeneration)
	if _, ok := manager.Get(session.ID); !ok {
		t.Fatal("stale expiry generation removed an active session")
	}

	session.mu.Lock()
	manager.stopExpiryTimerLocked(session)
	generation := session.expiryGeneration
	timer := session.expiryTimer
	session.mu.Unlock()
	manager.mu.Lock()
	delete(manager.sessions, session.ID)
	manager.mu.Unlock()
	manager.expireSession(session, generation)
	session.mu.Lock()
	timer = session.expiryTimer
	session.mu.Unlock()
	if timer != nil {
		t.Fatal("expiry for a removed session left a timer active")
	}
	_, _ = session.Executor.Execute(context.Background(), "ROLLBACK", nil)
}

func TestManagerCloseContextHonorsDeadlineAndRollsBackAfterLease(t *testing.T) {
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { _ = store.Close() })
	manager := NewManager(time.Hour, func(string) (*cypher.StorageExecutor, error) {
		return cypher.NewStorageExecutor(storage.NewNamespacedEngine(store, "nornic")), nil
	})
	t.Cleanup(func() { _ = manager.Close() })
	session, err := manager.Open(context.Background(), "nornic")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ExecuteInSession(context.Background(), session, "CREATE (:CloseDeadline {id: 'abandoned'})", nil); err != nil {
		t.Fatal(err)
	}
	session.requestMu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = manager.CloseContext(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CloseContext returned %v, want deadline exceeded", err)
	}
	if _, ok := manager.Get(session.ID); ok {
		t.Fatal("deadline close left the leased session registered")
	}
	session.mu.Lock()
	timer, expired := session.expiryTimer, session.expired
	session.mu.Unlock()
	if timer != nil || !expired {
		t.Fatalf("deadline close left timer/session active: timer=%v expired=%t", timer != nil, expired)
	}
	session.requestMu.Unlock()

	deadline := time.After(3 * time.Second)
	for {
		session.mu.Lock()
		completed := session.completed
		session.mu.Unlock()
		if completed {
			break
		}
		select {
		case <-deadline:
			t.Fatal("abandoned session was not rolled back after its lease was released")
		case <-time.After(time.Millisecond):
		}
	}
	check := cypher.NewStorageExecutor(storage.NewNamespacedEngine(store, "nornic"))
	result, err := check.Execute(context.Background(), "MATCH (n:CloseDeadline) RETURN count(n) AS count", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != int64(0) {
		t.Fatalf("abandoned transaction was not rolled back: rows=%#v", result.Rows)
	}
}

func TestManagerCloseContextReturnsRollbackErrors(t *testing.T) {
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { _ = store.Close() })
	manager := NewManager(time.Hour, func(string) (*cypher.StorageExecutor, error) {
		return cypher.NewStorageExecutor(storage.NewNamespacedEngine(store, "nornic")), nil
	})
	t.Cleanup(func() { _ = manager.Close() })
	session, err := manager.Open(context.Background(), "nornic")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Executor.Execute(context.Background(), "COMMIT", nil); err != nil {
		t.Fatal(err)
	}
	if err := manager.CloseContext(context.Background()); err == nil || !strings.Contains(err.Error(), "rollback explicit transaction") {
		t.Fatalf("CloseContext returned %v, want rollback error", err)
	}
}

func TestManagerCloseContextMarksTerminalSessionsComplete(t *testing.T) {
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { _ = store.Close() })
	manager := NewManager(time.Hour, func(string) (*cypher.StorageExecutor, error) {
		return cypher.NewStorageExecutor(storage.NewNamespacedEngine(store, "nornic")), nil
	})
	t.Cleanup(func() { _ = manager.Close() })
	session, err := manager.Open(context.Background(), "nornic")
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	session.terminalErr = errors.New("terminal lifecycle failure")
	session.mu.Unlock()
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	completed := session.completed
	session.mu.Unlock()
	if !completed {
		t.Fatal("Close did not mark a terminal session complete")
	}
	_, _ = session.Executor.Execute(context.Background(), "ROLLBACK", nil)
}

// The lifecycle owner rejects a nil context loudly instead of substituting
// one: a caller's missing deadline is a programming error, not a default.
func TestManagerCloseContextRejectsNilContext(t *testing.T) {
	manager := NewManager(time.Hour, nil)
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	})
	defer func() {
		if recover() == nil {
			t.Fatal("CloseContext(nil) did not reject the nil context")
		}
	}()
	_ = manager.CloseContext(nil) // panics before returning
}

func TestManagerCloseContextDoesNotWaitForSessionStateLock(t *testing.T) {
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { _ = store.Close() })
	manager := NewManager(time.Hour, func(string) (*cypher.StorageExecutor, error) {
		return cypher.NewStorageExecutor(storage.NewNamespacedEngine(store, "nornic")), nil
	})
	t.Cleanup(func() { _ = manager.Close() })
	session, err := manager.Open(context.Background(), "nornic")
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	locked := true
	t.Cleanup(func() {
		if locked {
			session.mu.Unlock()
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := manager.CloseContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CloseContext returned %v, want deadline exceeded", err)
	}
	session.mu.Unlock()
	locked = false

	deadline := time.After(3 * time.Second)
	for {
		session.mu.Lock()
		completed := session.completed
		timer := session.expiryTimer
		session.mu.Unlock()
		if completed && timer == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("session cleanup did not stop its timer and finish rollback")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestLeaseReleaseAndExpiresAtAreNilSafe(t *testing.T) {
	var lease *Lease
	lease.Release()
	(&Lease{}).Release()
	var session *Session
	if !session.ExpiresAt().IsZero() {
		t.Fatal("nil session returned a non-zero expiry")
	}
}
