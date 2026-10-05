package txsession

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// ExecutorFactory creates a fresh executor scoped to a database.
// Each explicit transaction gets its own executor instance.
type ExecutorFactory func(dbName string) (*cypher.StorageExecutor, error)

// Session stores explicit transaction state shared across transports.
type Session struct {
	ID       string
	Database string
	Owner    string
	Executor *cypher.StorageExecutor
	Expires  time.Time

	mu          sync.Mutex
	terminalErr error
}

// Manager tracks explicit transaction sessions and lifecycle operations.
type Manager struct {
	mu       sync.RWMutex
	sessions map[string]*Session

	ttl     time.Duration
	nowFunc func() time.Time
	// lastID is the most recently issued transaction ID, guarded by mu. IDs
	// count up from the manager's creation time, so every session gets its
	// own ID however close together sessions open, and IDs issued after a
	// restart don't repeat ones issued before it.
	lastID uint64

	factory               ExecutorFactory
	terminalErrorObserver func(*Session, error)
}

func NewManager(ttl time.Duration, factory ExecutorFactory) *Manager {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &Manager{
		sessions: make(map[string]*Session),
		ttl:      ttl,
		nowFunc:  time.Now,
		lastID:   uint64(time.Now().UnixNano()),
		factory:  factory,
	}
}

func (m *Manager) Open(ctx context.Context, dbName string) (*Session, error) {
	return m.OpenForOwner(ctx, dbName, "")
}

func (m *Manager) OpenForOwner(ctx context.Context, dbName string, owner string) (*Session, error) {
	if m.factory == nil {
		return nil, fmt.Errorf("transaction manager factory is not configured")
	}
	executor, err := m.factory(dbName)
	if err != nil {
		return nil, err
	}

	return m.OpenWithExecutorForOwner(ctx, dbName, executor, owner)
}

// OpenWithExecutor opens a transaction session using a pre-built executor.
// This is used when executor construction depends on request context (for example
// forwarding auth to remote composite constituents).
func (m *Manager) OpenWithExecutor(ctx context.Context, dbName string, executor *cypher.StorageExecutor) (*Session, error) {
	return m.OpenWithExecutorForOwner(ctx, dbName, executor, "")
}

func (m *Manager) OpenWithExecutorForOwner(ctx context.Context, dbName string, executor *cypher.StorageExecutor, owner string) (*Session, error) {
	if executor == nil {
		return nil, fmt.Errorf("transaction executor is not available")
	}

	if _, err := executor.Execute(cypher.WithTransactionControl(ctx), "BEGIN", nil); err != nil {
		return nil, err
	}

	now := m.nowFunc()
	session := &Session{
		Database: dbName,
		Owner:    strings.TrimSpace(owner),
		Executor: executor,
		Expires:  now.Add(m.ttl),
	}

	m.mu.Lock()
	m.lastID++
	session.ID = strconv.FormatUint(m.lastID, 10)
	m.sessions[session.ID] = session
	m.mu.Unlock()

	return session, nil
}

func (m *Manager) Get(txID string) (*Session, bool) {
	return m.GetForOwner(txID, "")
}

func (m *Manager) GetForOwner(txID string, owner string) (*Session, bool) {
	m.mu.RLock()
	session, ok := m.sessions[txID]
	m.mu.RUnlock()
	if !ok || session == nil {
		return nil, false
	}
	boundOwner := strings.TrimSpace(session.Owner)
	if boundOwner == "" {
		return session, true
	}
	if strings.TrimSpace(owner) == "" {
		return nil, false
	}
	if boundOwner != strings.TrimSpace(owner) {
		return nil, false
	}
	return session, ok
}

func (m *Manager) Delete(txID string) {
	m.mu.Lock()
	delete(m.sessions, txID)
	m.mu.Unlock()
}

func (m *Manager) SetTerminalErrorObserver(observer func(*Session, error)) {
	m.mu.Lock()
	m.terminalErrorObserver = observer
	m.mu.Unlock()
}

func (m *Manager) Touch(session *Session) {
	if session == nil {
		return
	}
	session.Expires = m.nowFunc().Add(m.ttl)
}

func (m *Manager) ExecuteInSession(ctx context.Context, session *Session, query string, params map[string]interface{}) (*cypher.ExecuteResult, error) {
	if session == nil || session.Executor == nil {
		return nil, fmt.Errorf("transaction session is not available")
	}
	session.mu.Lock()
	if session.terminalErr != nil {
		err := session.terminalErr
		session.mu.Unlock()
		return nil, err
	}

	result, err := session.Executor.Execute(ctx, query, params)
	if err != nil {
		terminalErr, notify := m.rememberTerminalErrorLocked(session, err)
		session.mu.Unlock()
		m.notifyTerminalError(session, terminalErr, notify)
		return result, terminalErr
	}
	session.Expires = m.nowFunc().Add(m.ttl)
	session.mu.Unlock()
	return result, nil
}

func (m *Manager) CommitAndDelete(ctx context.Context, session *Session) (*cypher.ExecuteResult, error) {
	if session == nil || session.Executor == nil {
		return nil, fmt.Errorf("transaction session is not available")
	}
	defer m.Delete(session.ID)
	session.mu.Lock()
	if session.terminalErr != nil {
		err := session.terminalErr
		session.mu.Unlock()
		return nil, err
	}

	result, err := session.Executor.Execute(cypher.WithTransactionControl(ctx), "COMMIT", nil)
	if err != nil {
		terminalErr, notify := m.rememberTerminalErrorLocked(session, err)
		session.mu.Unlock()
		m.notifyTerminalError(session, terminalErr, notify)
		return nil, terminalErr
	}
	session.mu.Unlock()
	return result, nil
}

func (m *Manager) RollbackAndDelete(ctx context.Context, session *Session) error {
	if session == nil || session.Executor == nil {
		return fmt.Errorf("transaction session is not available")
	}
	defer m.Delete(session.ID)
	session.mu.Lock()
	if session.terminalErr != nil {
		session.mu.Unlock()
		return nil
	}

	_, err := session.Executor.Execute(cypher.WithTransactionControl(ctx), "ROLLBACK", nil)
	session.mu.Unlock()
	return err
}

func (m *Manager) rememberTerminalErrorLocked(session *Session, err error) (error, bool) {
	if session == nil || err == nil {
		return err, false
	}
	if session.terminalErr != nil {
		return session.terminalErr, false
	}
	if !isTerminalLifecycleError(err) {
		return err, false
	}
	session.terminalErr = err
	return session.terminalErr, true
}

func (m *Manager) notifyTerminalError(session *Session, err error, notify bool) {
	if !notify || err == nil || session == nil {
		return
	}
	m.mu.RLock()
	observer := m.terminalErrorObserver
	m.mu.RUnlock()
	if observer != nil {
		observer(session, err)
	}
}

func isTerminalLifecycleError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, storage.ErrMVCCSnapshotGracefulCancel) || errors.Is(err, storage.ErrMVCCSnapshotHardExpired) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "snapshot cancelled due to resource pressure") ||
		strings.Contains(message, "snapshot forcibly expired due to critical resource pressure")
}
