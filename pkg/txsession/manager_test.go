package txsession

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type lifecycleControllerStub struct {
	mu             sync.Mutex
	gracefulExpire bool
	hardExpire     bool
}

func (s *lifecycleControllerStub) RegisterSnapshotReader(info storage.SnapshotReaderInfo) func() {
	_ = info
	return func() {}
}

func (s *lifecycleControllerStub) LifecycleStatus() map[string]interface{} {
	return map[string]interface{}{"enabled": true}
}

func (s *lifecycleControllerStub) TriggerPruneNow(ctx context.Context) error {
	_ = ctx
	return nil
}

func (s *lifecycleControllerStub) PauseLifecycle() {}

func (s *lifecycleControllerStub) ResumeLifecycle() {}

func (s *lifecycleControllerStub) AcquireSnapshotReader(info storage.SnapshotReaderInfo) (func(), error) {
	_ = info
	return func() {}, nil
}

func (s *lifecycleControllerStub) EvaluateSnapshotReader(info storage.SnapshotReaderInfo) (bool, bool) {
	_ = info
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gracefulExpire, s.hardExpire
}

func (s *lifecycleControllerStub) RunPruneNow(ctx context.Context, opts storage.MVCCPruneOptions) (int64, error) {
	_ = ctx
	_ = opts
	return 0, nil
}

func (s *lifecycleControllerStub) StartLifecycle(ctx context.Context) { _ = ctx }

func (s *lifecycleControllerStub) StopLifecycle() {}

func (s *lifecycleControllerStub) IsLifecycleEnabled() bool { return true }

func (s *lifecycleControllerStub) IsLifecycleRunning() bool { return false }

func (s *lifecycleControllerStub) ReaderRegistry() storage.SnapshotReaderRegistry { return nil }

func newExecutorFactory(t *testing.T) ExecutorFactory {
	t.Helper()
	return func(_ string) (*cypher.StorageExecutor, error) {
		store := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = store.Close() })
		return cypher.NewStorageExecutor(store), nil
	}
}

func TestManagerOpenErrors(t *testing.T) {
	mgr := NewManager(time.Second, nil)
	if _, err := mgr.Open(context.Background(), "neo4j"); err == nil {
		t.Fatalf("expected error when factory is nil")
	}

	mgr = NewManager(time.Second, func(_ string) (*cypher.StorageExecutor, error) {
		return nil, fmt.Errorf("factory failed")
	})
	if _, err := mgr.Open(context.Background(), "neo4j"); err == nil {
		t.Fatalf("expected factory error")
	}
}

func TestManagerLifecycle_ExecuteCommitAndDelete(t *testing.T) {
	mgr := NewManager(time.Second, newExecutorFactory(t))
	baseTime := time.Unix(1700000000, 0)
	mgr.lastID = 41
	mgr.nowFunc = func() time.Time { return baseTime }

	session, err := mgr.Open(context.Background(), "neo4j")
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	if session.ID != "42" {
		t.Fatalf("unexpected session id: %s", session.ID)
	}
	if !session.Expires.Equal(baseTime.Add(time.Second)) {
		t.Fatalf("unexpected expiry: %v", session.Expires)
	}

	if _, ok := mgr.Get("42"); !ok {
		t.Fatalf("expected session to be retrievable")
	}

	result, err := mgr.ExecuteInSession(context.Background(), session, "RETURN 1 AS one", nil)
	if err != nil {
		t.Fatalf("execute in session failed: %v", err)
	}
	if len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != int64(1) {
		t.Fatalf("unexpected execute result in session: %#v", result.Rows)
	}

	mgr.nowFunc = func() time.Time { return baseTime.Add(5 * time.Second) }
	mgr.Touch(session)
	if !session.Expires.Equal(baseTime.Add(6 * time.Second)) {
		t.Fatalf("touch should refresh expiration, got %v", session.Expires)
	}

	if _, err := mgr.CommitAndDelete(context.Background(), session); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if _, ok := mgr.Get("42"); ok {
		t.Fatalf("expected session deleted after commit")
	}

	mgr.Delete("42") // no-op path
}

func TestManagerLifecycle_RollbackAndErrorGuards(t *testing.T) {
	mgr := NewManager(time.Second, newExecutorFactory(t))
	session, err := mgr.Open(context.Background(), "neo4j")
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}

	if _, err := mgr.ExecuteInSession(context.Background(), nil, "RETURN 1", nil); err == nil {
		t.Fatalf("expected nil session execute to fail")
	}
	if _, err := mgr.CommitAndDelete(context.Background(), nil); err == nil {
		t.Fatalf("expected nil session commit to fail")
	}
	if err := mgr.RollbackAndDelete(context.Background(), nil); err == nil {
		t.Fatalf("expected nil session rollback to fail")
	}

	if _, err := mgr.ExecuteInSession(context.Background(), session, "RETURN 2 AS two", nil); err != nil {
		t.Fatalf("execute in session failed: %v", err)
	}

	if err := mgr.RollbackAndDelete(context.Background(), session); err != nil {
		t.Fatalf("rollback failed: %v", err)
	}
	if _, ok := mgr.Get(session.ID); ok {
		t.Fatalf("expected session deleted after rollback")
	}
}

func TestManagerOpenWithExecutor(t *testing.T) {
	mgr := NewManager(time.Second, newExecutorFactory(t))

	store := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = store.Close() })
	exec := cypher.NewStorageExecutor(store)

	session, err := mgr.OpenWithExecutor(context.Background(), "neo4j", exec)
	if err != nil {
		t.Fatalf("open with executor failed: %v", err)
	}
	if session == nil || session.Executor == nil {
		t.Fatalf("expected non-nil session and executor")
	}
	if session.Database != "neo4j" {
		t.Fatalf("unexpected session database: %s", session.Database)
	}
	if _, ok := mgr.Get(session.ID); !ok {
		t.Fatalf("expected session to be tracked")
	}

	if err := mgr.RollbackAndDelete(context.Background(), session); err != nil {
		t.Fatalf("rollback failed: %v", err)
	}
	if _, ok := mgr.Get(session.ID); ok {
		t.Fatalf("expected session deleted after rollback")
	}
}

func TestManagerOpenWithExecutorErrors(t *testing.T) {
	mgr := NewManager(time.Second, newExecutorFactory(t))

	if _, err := mgr.OpenWithExecutor(context.Background(), "neo4j", nil); err == nil {
		t.Fatalf("expected nil executor error")
	}
}

func TestManagerOwnerBoundSessions(t *testing.T) {
	mgr := NewManager(time.Second, newExecutorFactory(t))

	session, err := mgr.OpenForOwner(context.Background(), "neo4j", "user:alice")
	if err != nil {
		t.Fatalf("open for owner failed: %v", err)
	}

	if got, ok := mgr.GetForOwner(session.ID, "user:alice"); !ok || got == nil {
		t.Fatalf("expected owner-bound session lookup to succeed")
	}
	if got, ok := mgr.GetForOwner(session.ID, "user:bob"); ok || got != nil {
		t.Fatalf("expected mismatched owner lookup to fail")
	}
	if got, ok := mgr.GetForOwner(session.ID, ""); ok || got != nil {
		t.Fatalf("expected empty owner lookup to fail for owner-bound session")
	}

	if err := mgr.RollbackAndDelete(context.Background(), session); err != nil {
		t.Fatalf("rollback failed: %v", err)
	}
}

func TestManagerNewManagerDefaultTTLAndTouchNil(t *testing.T) {
	mgr := NewManager(0, newExecutorFactory(t))
	if mgr.ttl != 30*time.Second {
		t.Fatalf("expected default ttl 30s, got %v", mgr.ttl)
	}
	// No panic path.
	mgr.Touch(nil)
}

func TestManagerOpenWithExecutorCompositeBeginSupported(t *testing.T) {
	mgr := NewManager(time.Second, newExecutorFactory(t))

	// CompositeEngine explicit BEGIN is supported via Fabric transaction context.
	c := storage.NewCompositeEngine(
		map[string]storage.Engine{"a": storage.NewMemoryEngine()},
		map[string]string{"a": "a"},
		map[string]string{"a": "read_write"},
	)
	exec := cypher.NewStorageExecutor(c)

	session, err := mgr.OpenWithExecutor(context.Background(), "neo4j", exec)
	if err != nil {
		t.Fatalf("expected composite begin to succeed, got: %v", err)
	}
	if session == nil {
		t.Fatalf("expected non-nil session")
	}
	_ = mgr.RollbackAndDelete(context.Background(), session)
}

func TestManagerExecuteInSession_ReplaysTerminalLifecycleError(t *testing.T) {
	engine, err := storage.NewBadgerEngineInMemory()
	if err != nil {
		t.Fatalf("failed to create badger engine: %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	controller := &lifecycleControllerStub{}
	engine.SetLifecycleController(controller)
	exec := cypher.NewStorageExecutor(engine)
	mgr := NewManager(time.Second, nil)
	session, err := mgr.OpenWithExecutor(context.Background(), "neo4j", exec)
	if err != nil {
		t.Fatalf("open with executor failed: %v", err)
	}

	controller.mu.Lock()
	controller.gracefulExpire = true
	controller.mu.Unlock()

	_, err = mgr.ExecuteInSession(context.Background(), session, "CREATE (n:Doc {name:'expired'})", nil)
	if !errors.Is(err, storage.ErrMVCCSnapshotGracefulCancel) {
		t.Fatalf("expected graceful snapshot cancel, got %v", err)
	}
	_, err = mgr.ExecuteInSession(context.Background(), session, "CREATE (n:Doc {name:'expired-again'})", nil)
	if !errors.Is(err, storage.ErrMVCCSnapshotGracefulCancel) {
		t.Fatalf("expected repeated graceful snapshot cancel, got %v", err)
	}
	if rollbackErr := mgr.RollbackAndDelete(context.Background(), session); rollbackErr != nil {
		t.Fatalf("rollback after terminal error should succeed, got %v", rollbackErr)
	}
}
