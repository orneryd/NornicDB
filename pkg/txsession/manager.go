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

	mu               sync.Mutex
	requestMu        sync.Mutex
	terminalErr      error
	expiryTimer      *time.Timer
	expiryGeneration uint64
	expired          bool
	completed        bool
}

// Manager tracks explicit transaction sessions and lifecycle operations.
type Manager struct {
	mu       sync.RWMutex
	sessions map[string]*Session
	closed   bool // guarded by mu

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

// Lease serializes one request's use of a session against expiry and other
// requests. Release it when the request has finished using Session.
type Lease struct {
	Session *Session
	once    sync.Once
}

// Release ends the request's exclusive use of the leased session.
func (l *Lease) Release() {
	if l == nil || l.Session == nil {
		return
	}
	l.once.Do(func() { l.Session.requestMu.Unlock() })
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
	m.mu.RLock()
	closed := m.closed
	m.mu.RUnlock()
	if closed {
		return nil, fmt.Errorf("transaction manager is closed")
	}

	if _, err := executor.Execute(cypher.WithTransactionControl(ctx), "BEGIN", nil); err != nil {
		return nil, err
	}

	session := &Session{
		Database: dbName,
		Owner:    strings.TrimSpace(owner),
		Executor: executor,
	}

	session.mu.Lock()
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		session.mu.Unlock()
		_, rollbackErr := executor.Execute(cypher.WithTransactionControl(context.Background()), "ROLLBACK", nil)
		return nil, errors.Join(fmt.Errorf("transaction manager is closed"), rollbackErr)
	}
	m.lastID++
	session.ID = strconv.FormatUint(m.lastID, 10)
	m.sessions[session.ID] = session
	m.mu.Unlock()
	m.resetExpiryTimerLocked(session)
	session.mu.Unlock()

	return session, nil
}

func (m *Manager) Get(txID string) (*Session, bool) {
	return m.GetForOwner(txID, "")
}

func (m *Manager) GetForOwner(txID string, owner string) (*Session, bool) {
	m.mu.RLock()
	session, ok := m.sessions[txID]
	closed := m.closed
	m.mu.RUnlock()
	if closed || !ok || session == nil {
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

// AcquireForOwner leases an active session to one request. Expiry is checked
// after the request has decoded its body, so a transaction that expires while
// the body is being read is reported as not found. The lease keeps an accepted
// request serialized with expiry and other session operations until Release.
func (m *Manager) AcquireForOwner(txID, owner string) (*Lease, bool) {
	session, ok := m.GetForOwner(txID, owner)
	if !ok || session == nil {
		return nil, false
	}
	session.requestMu.Lock()
	session.mu.Lock()
	m.mu.RLock()
	current := !m.closed && m.sessions[txID] == session
	m.mu.RUnlock()
	expired := session.expired || !m.nowFunc().Before(session.Expires)
	generation := session.expiryGeneration
	session.mu.Unlock()
	if !current || expired {
		session.requestMu.Unlock()
		if current && expired {
			m.expireSession(session, generation)
		}
		return nil, false
	}
	return &Lease{Session: session}, true
}

func (m *Manager) Delete(txID string) {
	m.mu.RLock()
	session := m.sessions[txID]
	m.mu.RUnlock()
	if session == nil {
		return
	}
	session.requestMu.Lock()
	session.mu.Lock()
	m.mu.Lock()
	if m.sessions[txID] == session {
		delete(m.sessions, txID)
	}
	m.mu.Unlock()
	m.stopExpiryTimerLocked(session)
	session.mu.Unlock()
	session.requestMu.Unlock()
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
	m.mu.RLock()
	registered := !m.closed && m.sessions[session.ID] == session
	m.mu.RUnlock()
	if registered && !session.expired {
		m.resetExpiryTimerLocked(session)
	}
	session.mu.Unlock()
}

// Close rejects new sessions, rolls back every remaining transaction, removes
// it from the manager and stops its expiry timer. It is safe to call repeatedly.
func (m *Manager) Close() error {
	return m.CloseContext(context.Background())
}

// CloseContext rejects new sessions and drains their active requests before
// rolling back. If ctx expires while a request still owns a session lease, the
// session is abandoned from the manager immediately and a cleanup goroutine
// finishes its rollback after that lease is released.
func (m *Manager) CloseContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	sessions := make([]*Session, 0, len(m.sessions))
	for _, session := range m.sessions {
		sessions = append(sessions, session)
	}
	m.sessions = make(map[string]*Session)
	m.mu.Unlock()

	closeResults := make(chan error, len(sessions))
	for _, session := range sessions {
		// Stop timers without waiting for a request lease. Most response writes
		// retain requestMu after releasing session.mu, so this also prevents an
		// idle timer from outliving a deadline-bounded shutdown.
		if session.mu.TryLock() {
			m.stopExpiryTimerLocked(session)
			session.expired = true
			session.mu.Unlock()
		} else {
			go func(session *Session) {
				session.mu.Lock()
				m.stopExpiryTimerLocked(session)
				session.expired = true
				session.mu.Unlock()
			}(session)
		}

		go func(session *Session) {
			session.requestMu.Lock()
			session.mu.Lock()
			m.stopExpiryTimerLocked(session)
			session.expired = true
			var terminalErr error
			var notify bool
			var closeErr error
			if !session.completed && session.terminalErr == nil && session.Executor != nil {
				_, err := session.Executor.Execute(cypher.WithTransactionControl(context.Background()), "ROLLBACK", nil)
				terminalErr, notify = m.rememberTerminalErrorLocked(session, err)
				if err == nil {
					session.completed = true
				} else {
					closeErr = fmt.Errorf("rollback explicit transaction %s: %w", session.ID, err)
				}
			} else if session.terminalErr != nil {
				session.completed = true
			}
			session.mu.Unlock()
			session.requestMu.Unlock()
			m.notifyTerminalError(session, terminalErr, notify)
			closeResults <- closeErr
		}(session)
	}

	var closeErr error
	for remaining := len(sessions); remaining > 0; remaining-- {
		select {
		case err := <-closeResults:
			closeErr = errors.Join(closeErr, err)
		case <-ctx.Done():
			return errors.Join(closeErr, ctx.Err())
		}
	}
	return closeErr
}

// ExpiresAt returns the expiry timestamp under the session lock.
func (session *Session) ExpiresAt() time.Time {
	if session == nil {
		return time.Time{}
	}
	session.mu.Lock()
	expires := session.Expires
	session.mu.Unlock()
	return expires
}

func (m *Manager) resetExpiryTimerLocked(session *Session) {
	if session.expiryTimer != nil {
		session.expiryTimer.Stop()
	}
	session.expiryGeneration++
	generation := session.expiryGeneration
	session.Expires = m.nowFunc().Add(m.ttl)
	session.expiryTimer = time.AfterFunc(m.ttl, func() {
		m.expireSession(session, generation)
	})
}

func (m *Manager) stopExpiryTimerLocked(session *Session) {
	session.expiryGeneration++
	if session.expiryTimer != nil {
		session.expiryTimer.Stop()
		session.expiryTimer = nil
	}
}

func (m *Manager) expireSession(session *Session, generation uint64) {
	session.requestMu.Lock()
	defer session.requestMu.Unlock()

	session.mu.Lock()
	if session.expiryGeneration != generation {
		session.mu.Unlock()
		return
	}
	session.expiryTimer = nil

	m.mu.Lock()
	current := m.sessions[session.ID] == session
	if current {
		delete(m.sessions, session.ID)
	}
	m.mu.Unlock()
	if !current {
		m.stopExpiryTimerLocked(session)
		session.mu.Unlock()
		return
	}

	session.expiryGeneration++
	session.expired = true
	var terminalErr error
	var notify bool
	if session.Executor != nil && session.terminalErr == nil {
		_, err := session.Executor.Execute(cypher.WithTransactionControl(context.Background()), "ROLLBACK", nil)
		terminalErr, notify = m.rememberTerminalErrorLocked(session, err)
		if err == nil {
			session.completed = true
		}
	}
	session.mu.Unlock()
	m.notifyTerminalError(session, terminalErr, notify)
}

func (m *Manager) ExecuteInSession(ctx context.Context, session *Session, query string, params map[string]interface{}) (*cypher.ExecuteResult, error) {
	if session == nil || session.Executor == nil {
		return nil, fmt.Errorf("transaction session is not available")
	}
	session.mu.Lock()
	if session.expired {
		session.mu.Unlock()
		return nil, fmt.Errorf("transaction session has expired")
	}
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
	m.resetExpiryTimerLocked(session)
	session.mu.Unlock()
	return result, nil
}

func (m *Manager) CommitAndDelete(ctx context.Context, session *Session) (*cypher.ExecuteResult, error) {
	if session == nil || session.Executor == nil {
		return nil, fmt.Errorf("transaction session is not available")
	}
	session.mu.Lock()
	if session.expired {
		m.deleteSessionLocked(session)
		session.mu.Unlock()
		return nil, fmt.Errorf("transaction session has expired")
	}
	if session.terminalErr != nil {
		err := session.terminalErr
		session.completed = true
		m.deleteSessionLocked(session)
		session.mu.Unlock()
		return nil, err
	}

	result, err := session.Executor.Execute(cypher.WithTransactionControl(ctx), "COMMIT", nil)
	if err != nil {
		terminalErr, notify := m.rememberTerminalErrorLocked(session, err)
		m.deleteSessionLocked(session)
		session.mu.Unlock()
		m.notifyTerminalError(session, terminalErr, notify)
		return nil, terminalErr
	}
	session.completed = true
	m.deleteSessionLocked(session)
	session.mu.Unlock()
	return result, nil
}

func (m *Manager) RollbackAndDelete(ctx context.Context, session *Session) error {
	if session == nil || session.Executor == nil {
		return fmt.Errorf("transaction session is not available")
	}
	session.mu.Lock()
	if session.expired {
		m.deleteSessionLocked(session)
		session.mu.Unlock()
		return nil
	}
	if session.terminalErr != nil {
		session.completed = true
		m.deleteSessionLocked(session)
		session.mu.Unlock()
		return nil
	}

	_, err := session.Executor.Execute(cypher.WithTransactionControl(ctx), "ROLLBACK", nil)
	if err == nil {
		session.completed = true
	}
	m.deleteSessionLocked(session)
	session.mu.Unlock()
	return err
}

func (m *Manager) deleteSessionLocked(session *Session) {
	m.mu.Lock()
	if m.sessions[session.ID] == session {
		delete(m.sessions, session.ID)
	}
	m.mu.Unlock()
	m.stopExpiryTimerLocked(session)
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
