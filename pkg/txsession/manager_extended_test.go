package txsession

import (
	"context"
	"errors"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestSetTerminalErrorObserver(t *testing.T) {
	mgr := NewManager(time.Second, newExecutorFactory(t))

	var observed struct {
		mu      sync.Mutex
		session *Session
		err     error
	}

	mgr.SetTerminalErrorObserver(func(s *Session, err error) {
		observed.mu.Lock()
		observed.session = s
		observed.err = err
		observed.mu.Unlock()
	})

	// Create a session with a lifecycle controller that will trigger a terminal error
	engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	if err != nil {
		t.Fatalf("failed to create badger engine: %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	controller := &lifecycleControllerStub{}
	engine.SetLifecycleController(controller)
	exec := cypher.NewStorageExecutor(engine)

	session, err := mgr.OpenWithExecutorForOwner(context.Background(), "neo4j", exec, "")
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}

	// Trigger graceful expiry
	controller.mu.Lock()
	controller.gracefulExpire = true
	controller.mu.Unlock()

	_, err = mgr.ExecuteInSession(context.Background(), session, "CREATE (n:Doc {name:'x'})", nil)
	if !errors.Is(err, storage.ErrMVCCSnapshotGracefulCancel) {
		t.Fatalf("expected graceful cancel, got %v", err)
	}

	// Observer should have been called
	observed.mu.Lock()
	if observed.session == nil || observed.session.ID != session.ID {
		t.Fatalf("observer should have been called with the session")
	}
	if !errors.Is(observed.err, storage.ErrMVCCSnapshotGracefulCancel) {
		t.Fatalf("observer should have been called with terminal error, got %v", observed.err)
	}
	observed.mu.Unlock()

	_ = mgr.RollbackAndDelete(context.Background(), session)
}

func TestManagerTerminalLifecycleScenarios(t *testing.T) {
	for _, scenario := range []struct {
		name         string
		statements   []string
		checkExecute bool
		commit       bool
		checkDeleted bool
	}{
		{name: "commit replays terminal error", statements: []string{"CREATE (n:X)"}, commit: true, checkDeleted: true},
		{name: "rollback after terminal succeeds", statements: []string{"CREATE (n:X)"}},
		{name: "execute replays terminal error", statements: []string{"CREATE (n:Doc {name:'expired'})", "CREATE (n:Doc {name:'expired-again'})"}, checkExecute: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
			if engine != nil {
				fixtureowner.Own(t, engine)
			}
			if err != nil {
				t.Fatalf("failed to create badger engine: %v", err)
			}
			t.Cleanup(func() { _ = engine.Close() })
			controller := &lifecycleControllerStub{}
			engine.SetLifecycleController(controller)
			exec := cypher.NewStorageExecutor(engine)
			mgr := NewManager(time.Second, nil)
			session, err := mgr.OpenWithExecutorForOwner(context.Background(), "neo4j", exec, "")
			if err != nil {
				t.Fatalf("open with executor failed: %v", err)
			}
			controller.mu.Lock()
			controller.gracefulExpire = true
			controller.mu.Unlock()
			for index, statement := range scenario.statements {
				_, err = mgr.ExecuteInSession(context.Background(), session, statement, nil)
				if scenario.checkExecute && !errors.Is(err, storage.ErrMVCCSnapshotGracefulCancel) {
					t.Fatalf("expected graceful snapshot cancel at execution %d, got %v", index+1, err)
				}
			}
			if scenario.commit {
				_, err = mgr.CommitAndDelete(context.Background(), session)
				if !errors.Is(err, storage.ErrMVCCSnapshotGracefulCancel) {
					t.Fatalf("expected terminal error on commit, got %v", err)
				}
			} else if err = mgr.RollbackAndDelete(context.Background(), session); err != nil {
				t.Fatalf("rollback after terminal error should succeed, got %v", err)
			}
			if scenario.checkDeleted {
				if _, ok := mgr.Get(session.ID); ok {
					t.Fatalf("expected session deleted after failed commit")
				}
			}
		})
	}
}

func TestIsTerminalLifecycleError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		terminal bool
	}{
		{"nil error", nil, false},
		{"regular error", fmt.Errorf("something went wrong"), false},
		{"graceful cancel sentinel", storage.ErrMVCCSnapshotGracefulCancel, true},
		{"hard expired sentinel", storage.ErrMVCCSnapshotHardExpired, true},
		{"wrapped graceful cancel", fmt.Errorf("query failed: %w", storage.ErrMVCCSnapshotGracefulCancel), true},
		{"wrapped hard expired", fmt.Errorf("query failed: %w", storage.ErrMVCCSnapshotHardExpired), true},
		{"string match graceful", fmt.Errorf("snapshot cancelled due to resource pressure"), true},
		{"string match hard", fmt.Errorf("snapshot forcibly expired due to critical resource pressure"), true},
		{"case insensitive", fmt.Errorf("Snapshot Cancelled Due To Resource Pressure"), true},
		{"unrelated error", fmt.Errorf("connection refused"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isTerminalLifecycleError(tt.err)
			if got != tt.terminal {
				t.Fatalf("isTerminalLifecycleError(%v) = %v, want %v", tt.err, got, tt.terminal)
			}
		})
	}
}

func TestRememberTerminalErrorLocked_NilSession(t *testing.T) {
	mgr := NewManager(time.Second, nil)
	err, notify := mgr.rememberTerminalErrorLocked(nil, fmt.Errorf("some error"))
	if err == nil || notify {
		t.Fatalf("nil session should return error without notification, got err=%v notify=%v", err, notify)
	}
}

func TestRememberTerminalErrorLocked_NilError(t *testing.T) {
	mgr := NewManager(time.Second, nil)
	session := &Session{ID: "test"}
	err, notify := mgr.rememberTerminalErrorLocked(session, nil)
	if err != nil || notify {
		t.Fatalf("nil error should return nil without notification, got err=%v notify=%v", err, notify)
	}
}

func TestRememberTerminalErrorLocked_NonTerminal(t *testing.T) {
	mgr := NewManager(time.Second, nil)
	session := &Session{ID: "test"}
	regularErr := fmt.Errorf("syntax error")
	err, notify := mgr.rememberTerminalErrorLocked(session, regularErr)
	if err != regularErr || notify {
		t.Fatalf("non-terminal error should pass through without notification, got err=%v notify=%v", err, notify)
	}
	// Session should not have terminal error set
	if session.terminalErr != nil {
		t.Fatalf("non-terminal error should not be stored on session")
	}
}

func TestRememberTerminalErrorLocked_AlreadyTerminal(t *testing.T) {
	mgr := NewManager(time.Second, nil)
	firstErr := storage.ErrMVCCSnapshotGracefulCancel
	session := &Session{ID: "test", terminalErr: firstErr}
	secondErr := storage.ErrMVCCSnapshotHardExpired
	err, notify := mgr.rememberTerminalErrorLocked(session, secondErr)
	if err != firstErr || notify {
		t.Fatalf("already-terminal session should return first error without re-notification, got err=%v notify=%v", err, notify)
	}
}

func TestNotifyTerminalError_NoObserver(t *testing.T) {
	mgr := NewManager(time.Second, nil)
	session := &Session{ID: "test"}
	// Should not panic with no observer set
	mgr.notifyTerminalError(session, fmt.Errorf("error"), true)
}

func TestNotifyTerminalError_NotifyFalse(t *testing.T) {
	mgr := NewManager(time.Second, nil)
	called := false
	mgr.SetTerminalErrorObserver(func(s *Session, err error) {
		called = true
	})
	session := &Session{ID: "test"}
	mgr.notifyTerminalError(session, fmt.Errorf("error"), false)
	if called {
		t.Fatalf("observer should not be called when notify is false")
	}
}

func TestOpenWithExecutorForOwner_OwnerTrimmed(t *testing.T) {
	mgr := NewManager(time.Second, newExecutorFactory(t))

	store := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = store.Close() })
	exec := cypher.NewStorageExecutor(store)

	session, err := mgr.OpenWithExecutorForOwner(context.Background(), "neo4j", exec, "  user:alice  ")
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	if session.Owner != "user:alice" {
		t.Fatalf("owner should be trimmed, got %q", session.Owner)
	}

	// Lookup with trimmed owner
	if _, ok := mgr.GetForOwner(session.ID, "  user:alice  "); !ok {
		t.Fatalf("should find session with whitespace-padded owner")
	}

	_ = mgr.RollbackAndDelete(context.Background(), session)
}
