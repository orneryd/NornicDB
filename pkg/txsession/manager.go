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
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/txphase"
)

// ExecutorFactory creates a fresh executor scoped to a database.
// Each explicit transaction gets its own executor instance.
type ExecutorFactory func(dbName string) (*cypher.StorageExecutor, error)

// Session cleanup is bounded independently of an HTTP request and of idle TTL.
const terminalCleanupTimeout = 30 * time.Second

// Session stores explicit transaction state shared across transports.
type Session struct {
	ID       string
	Database string
	Owner    string
	Executor *cypher.StorageExecutor
	Expires  time.Time

	mu             sync.Mutex
	terminalErr    error
	closed         bool
	lifetimeCancel context.CancelFunc
	idleTimer      *time.Timer
}

func (s *Session) Expiration() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Expires
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
	lifetimeCtx           context.Context
}

func NewManager(ttl time.Duration, factory ExecutorFactory) *Manager {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &Manager{
		sessions:    make(map[string]*Session),
		ttl:         ttl,
		nowFunc:     time.Now,
		lastID:      uint64(time.Now().UnixNano()),
		factory:     factory,
		lifetimeCtx: context.Background(),
	}
}

// SetLifetimeContext selects the server owner for newly opened transactions.
// Individual HTTP requests carry values and cancellation for statements only.
func (m *Manager) SetLifetimeContext(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lifetimeCtx = ctx
}

func (m *Manager) Open(ctx context.Context, dbName string) (*Session, error) {
	return m.OpenForOwner(ctx, dbName, "")
}

func (m *Manager) OpenForOwner(ctx context.Context, dbName string, owner string) (_ *Session, retErr error) {
	defer func() { retErr = nornicerrors.TransactionControlFailure(nornicerrors.TerminalBegin, retErr) }()
	if m.factory == nil {
		return nil, fmt.Errorf("transaction manager factory is not configured")
	}
	executor, err := m.factory(dbName)
	if err != nil {
		return nil, err
	}

	return m.OpenWithExecutorForOwner(ctx, dbName, executor, owner)
}

// OpenWithExecutorForOwner opens a transaction session using a pre-built
// executor and its principal identity. An empty owner denotes an unowned
// session. For example, an HTTP transaction supplies its authenticated owner;
// internal callers can supply "". Request-dependent remote credentials remain
// on the supplied executor, while the manager owns transaction lifetime.
func (m *Manager) OpenWithExecutorForOwner(ctx context.Context, dbName string, executor *cypher.StorageExecutor, owner string) (_ *Session, retErr error) {
	// A failure to open is the transaction's BEGIN failure, classified here.
	defer func() { retErr = nornicerrors.TransactionControlFailure(nornicerrors.TerminalBegin, retErr) }()
	if executor == nil {
		return nil, fmt.Errorf("transaction executor is not available")
	}

	m.mu.RLock()
	serverLifetime := m.lifetimeCtx
	m.mu.RUnlock()
	lifetime, cancel := context.WithCancel(serverLifetime)
	beginCtx := fabric.WithTransactionLifetime(ctx, lifetime)
	if _, err := executor.Execute(cypher.WithTransactionControl(beginCtx), "BEGIN", nil); err != nil {
		cancel()
		return nil, err
	}

	now := m.nowFunc()
	session := &Session{
		Database:       dbName,
		Owner:          strings.TrimSpace(owner),
		Executor:       executor,
		Expires:        now.Add(m.ttl),
		lifetimeCancel: cancel,
	}

	session.mu.Lock()
	m.mu.Lock()
	m.lastID++
	session.ID = strconv.FormatUint(m.lastID, 10)
	m.sessions[session.ID] = session
	session.idleTimer = time.AfterFunc(m.ttl, func() { m.expire(session) })
	m.mu.Unlock()
	session.mu.Unlock()

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
	session.mu.Lock()
	defer session.mu.Unlock()
	if m.expiredLocked(session) {
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
	m.mu.RLock()
	session := m.sessions[txID]
	m.mu.RUnlock()
	if session != nil {
		_ = m.RollbackAndDelete(context.Background(), session)
	}
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
	session.mu.Lock()
	defer session.mu.Unlock()
	if !m.expiredLocked(session) {
		m.touchLocked(session)
	}
}

func (m *Manager) touchLocked(session *Session) {
	if session.closed {
		return
	}
	session.Expires = m.nowFunc().Add(m.ttl)
	if session.idleTimer != nil {
		session.idleTimer.Reset(m.ttl)
	}
}

func (m *Manager) ExecuteInSession(ctx context.Context, session *Session, query string, params map[string]interface{}) (*cypher.ExecuteResult, error) {
	if session == nil {
		return nil, fmt.Errorf("transaction session is not available")
	}
	var terminalErr error
	var notify, resumeIdle bool
	session.mu.Lock()
	defer func() {
		// A panicking operation still releases its lease. Resume the paused idle
		// timer at its existing deadline rather than granting a new lifetime.
		if resumeIdle && !session.closed && session.idleTimer != nil {
			session.idleTimer.Reset(session.Expires.Sub(m.nowFunc()))
		}
		session.mu.Unlock()
		m.notifyTerminalError(session, terminalErr, notify)
	}()
	if m.expiredLocked(session) {
		return nil, fmt.Errorf("transaction session has expired or closed")
	}
	if session.terminalErr != nil {
		err := session.terminalErr
		return nil, err
	}
	if session.idleTimer != nil {
		session.idleTimer.Stop()
		resumeIdle = true
	}

	result, err := session.Executor.Execute(ctx, query, params)
	m.touchLocked(session)
	resumeIdle = false
	if err != nil {
		terminalErr, notify = m.rememberTerminalErrorLocked(session, err)
		return result, terminalErr
	}
	return result, nil
}

func (m *Manager) CommitAndDelete(ctx context.Context, session *Session) (_ *cypher.ExecuteResult, retErr error) {
	// Every failure returned here is this COMMIT's outcome, classified here; a
	// remembered statement failure keeps its own status.
	defer func() { retErr = nornicerrors.TransactionControlFailure(nornicerrors.TerminalCommit, retErr) }()
	if session == nil {
		return nil, fmt.Errorf("transaction session is not available")
	}
	var terminalErr error
	var notify bool
	session.mu.Lock()
	defer func() { session.mu.Unlock(); m.notifyTerminalError(session, terminalErr, notify) }()
	if m.expiredLocked(session) {
		return nil, fmt.Errorf("transaction session has expired or closed")
	}
	commitCtx, admissionErr := txphase.CommitContext(ctx)
	if admissionErr != nil {
		// Author: Codex, NOR-180. The existing terminal owner has already
		// returned its outcome; keep it without changing admission or its wait.
		_, cleanupErr := m.endLocked(ctx, session, "ROLLBACK")
		if cleanupErr != nil {
			return nil, nornicerrors.CombineTerminalOutcomes(admissionErr, cleanupErr)
		}
		return nil, admissionErr
	}
	result, err := m.endLocked(commitCtx, session, "COMMIT")
	if err != nil {
		terminalErr, notify = m.rememberTerminalErrorLocked(session, err)
		return nil, terminalErr
	}
	return result, nil
}

func (m *Manager) RollbackAndDelete(ctx context.Context, session *Session) (retErr error) {
	defer func() { retErr = nornicerrors.TransactionControlFailure(nornicerrors.TerminalRollback, retErr) }()
	if session == nil {
		return fmt.Errorf("transaction session is not available")
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	_, err := m.endLocked(ctx, session, "ROLLBACK")
	return err
}

// endLocked is the only session terminal owner. The operation mutex arbitrates
// expiry, explicit endings and active statements. No manager lock is held while
// waiting. A contextless storage rollback may outlive the wait; it retains its
// private executor, while the expired session is retired at the cleanup bound.
func (m *Manager) endLocked(ctx context.Context, session *Session, command string) (*cypher.ExecuteResult, error) {
	if session.closed {
		return nil, nil
	}
	session.closed = true
	if session.idleTimer != nil {
		session.idleTimer.Stop()
	}
	defer func() {
		m.mu.Lock()
		delete(m.sessions, session.ID)
		m.mu.Unlock()
		session.lifetimeCancel()
		session.Executor = nil
	}()
	if session.terminalErr != nil {
		if command == "COMMIT" {
			return nil, session.terminalErr
		}
		return nil, nil
	}
	if command == "COMMIT" {
		// Storage stops observing cancellation once write application begins.
		// Wait for its real result instead of retiring an in-flight commit.
		return session.Executor.Execute(cypher.WithTransactionControl(context.WithoutCancel(ctx)), command, nil)
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), terminalCleanupTimeout)
	defer cancel()
	stop := context.AfterFunc(cleanup, session.lifetimeCancel)
	defer stop()
	type terminalResult struct {
		result *cypher.ExecuteResult
		err    error
	}
	executor := session.Executor
	done := make(chan terminalResult, 1)
	go func() {
		outcome := terminalResult{}
		defer func() {
			if cause := recover(); cause != nil {
				if err, ok := cause.(error); ok {
					outcome.err = fmt.Errorf("transaction rollback panicked: %w", err)
				} else {
					outcome.err = fmt.Errorf("transaction rollback panicked: %v", cause)
				}
			}
			done <- outcome
		}()
		outcome.result, outcome.err = executor.Execute(cypher.WithTransactionControl(cleanup), command, nil)
	}()
	select {
	case outcome := <-done:
		return outcome.result, outcome.err
	case <-cleanup.Done():
		return nil, cleanup.Err()
	}
}

func (m *Manager) expiredLocked(session *Session) bool {
	if session.closed {
		return true
	}
	if !m.nowFunc().Before(session.Expires) {
		_, _ = m.endLocked(context.Background(), session, "ROLLBACK")
		return true
	}
	return false
}

func (m *Manager) expire(session *Session) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if !m.expiredLocked(session) {
		// A timer already firing may have waited behind an active statement.
		// That statement refreshed the idle deadline; it must not be expired by
		// the obsolete firing.
		session.idleTimer.Reset(session.Expires.Sub(m.nowFunc()))
	}
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
