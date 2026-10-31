package txsession

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/errors/compose"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/txphase"
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
	expiryTimer      *lifecycle.OwnedTimer
	expiryGeneration uint64
	expired          bool
	completed        bool
}

// Manager tracks explicit transaction sessions and lifecycle operations.
type Manager struct {
	mu       sync.RWMutex
	sessions map[string]*Session
	// work owns admission, every opening/cleanup/timer completion and their
	// outcomes. Its admission closes under mu with the session snapshot.
	work *lifecycle.WorkLifetime

	ttl          time.Duration
	nowFunc      func() time.Time
	armAfterFunc func(time.Duration, func()) *time.Timer
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
	Session   *Session
	manager   *Manager
	once      sync.Once
	retention *leaseRetention
}

type leaseRetention struct {
	refs atomic.Int32
	// work is the request's ticket in the manager's lifetime: WaitClosed and
	// Shutdown wait for every leased request, including its terminal observer
	// and response publication, until the last share is released.
	work func()
	// panicked keeps the session's existing idle deadline: an operation that
	// panicked did not complete, so its release grants no new lifetime.
	panicked atomic.Bool
}

// newLease enrolls the request in the manager's lifetime. It is refused once
// the manager is closing; the caller then still owns session.requestMu.
func (m *Manager) newLease(session *Session) (*Lease, bool) {
	_, work, accepted := m.work.Acquire(context.Background())
	if !accepted {
		return nil, false
	}
	retention := &leaseRetention{work: work}
	retention.refs.Store(1)
	return &Lease{Session: session, manager: m, retention: retention}, true
}

// requestLease leases a just-opened session, which holds requestMu.
func (m *Manager) requestLease(session *Session) (*Lease, error) {
	lease, accepted := m.newLease(session)
	if !accepted {
		session.requestMu.Unlock()
		return nil, fmt.Errorf("transaction manager is closed")
	}
	return lease, nil
}

// Retain transfers another share of this accepted request's ownership to its
// response publisher. Each share is released on actual completion. A timeout
// can end one waiter while its handler or buffered publication is still active.
// Retain must be called before releasing the source share.
func (l *Lease) Retain() *Lease {
	if l == nil || l.retention == nil {
		return nil
	}
	for refs := l.retention.refs.Load(); refs > 0; refs = l.retention.refs.Load() {
		if l.retention.refs.CompareAndSwap(refs, refs+1) {
			return &Lease{Session: l.Session, manager: l.manager, retention: l.retention}
		}
	}
	return nil
}

// Release ends the request's exclusive use of the leased session.
func (l *Lease) Release() {
	if l == nil || l.Session == nil {
		return
	}
	l.once.Do(func() {
		if l.retention != nil && l.retention.refs.Add(-1) != 0 {
			return
		}
		// Idle time starts after the actual request, including its response.
		if l.manager != nil && (l.retention == nil || !l.retention.panicked.Load()) {
			l.manager.touch(l.Session, true)
		}
		l.Session.requestMu.Unlock()
		if l.retention != nil && l.retention.work != nil {
			l.retention.work()
		}
	})
}

// NewManager binds its optional lifetime parent once, before any opening work.
// Start/transport contexts must never replace this owner after enrollment.
func NewManager(ttl time.Duration, factory ExecutorFactory, parents ...context.Context) *Manager {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	parent := context.Background()
	if len(parents) > 0 && parents[0] != nil {
		parent = parents[0]
	}
	return &Manager{
		work:         lifecycle.NewWorkLifetime(parent),
		sessions:     make(map[string]*Session),
		ttl:          ttl,
		nowFunc:      time.Now,
		armAfterFunc: time.AfterFunc,
		lastID:       uint64(time.Now().UnixNano()),
		factory:      factory,
	}
}

func (m *Manager) Open(ctx context.Context, dbName string) (*Session, error) {
	return m.OpenForOwner(ctx, dbName, "")
}

func (m *Manager) OpenForOwner(ctx context.Context, dbName string, owner string) (*Session, error) {
	return m.openForOwner(ctx, dbName, owner, false)
}

// OpenRequestForOwner opens and leases in one admission operation. HTTP and
// other request transports must retain this lease through response publication;
// acquiring a lease after OpenForOwner leaves an idle-expiry gap.
func (m *Manager) OpenRequestForOwner(ctx context.Context, dbName, owner string) (*Lease, error) {
	session, err := m.openForOwner(ctx, dbName, owner, true)
	if err != nil {
		return nil, err
	}
	return m.requestLease(session)
}

func (m *Manager) openForOwner(ctx context.Context, dbName string, owner string, leased bool) (_ *Session, retErr error) {
	// A failure to open is the transaction's BEGIN failure, classified here.
	defer func() { retErr = nornicerrors.TransactionControlFailure(nornicerrors.TerminalBegin, retErr) }()
	if m.factory == nil {
		return nil, fmt.Errorf("transaction manager factory is not configured")
	}
	_, release, accepted := m.work.Acquire(context.Background())
	if !accepted {
		return nil, fmt.Errorf("transaction manager is closed")
	}
	defer release()
	executor, err := m.factory(dbName)
	if err != nil {
		return nil, err
	}

	return m.openWithExecutorForOwner(ctx, dbName, executor, owner, leased)
}

func (m *Manager) OpenWithExecutorForOwner(ctx context.Context, dbName string, executor *cypher.StorageExecutor, owner string) (*Session, error) {
	return m.openWithExecutorForOwner(ctx, dbName, executor, owner, false)
}

// OpenRequestWithExecutorForOwner is the request opening contract for executors
// built with transport-specific authentication. It has the same lease lifetime
// as OpenRequestForOwner, including internal one-shot and fallback opens.
func (m *Manager) OpenRequestWithExecutorForOwner(ctx context.Context, dbName string, executor *cypher.StorageExecutor, owner string) (*Lease, error) {
	session, err := m.openWithExecutorForOwner(ctx, dbName, executor, owner, true)
	if err != nil {
		return nil, err
	}
	return m.requestLease(session)
}

func (m *Manager) openWithExecutorForOwner(ctx context.Context, dbName string, executor *cypher.StorageExecutor, owner string, leased bool) (_ *Session, retErr error) {
	defer func() { retErr = nornicerrors.TransactionControlFailure(nornicerrors.TerminalBegin, retErr) }()
	if executor == nil {
		return nil, fmt.Errorf("transaction executor is not available")
	}
	_, release, accepted := m.work.Acquire(context.Background())
	if !accepted {
		return nil, fmt.Errorf("transaction manager is closed")
	}
	defer release()

	beginCtx := fabric.WithTransactionLifetime(ctx, m.work.Context())
	if _, err := executor.Execute(cypher.WithTransactionControl(beginCtx), "BEGIN", nil); err != nil {
		return nil, err
	}

	session := &Session{
		Database: dbName,
		Owner:    strings.TrimSpace(owner),
		Executor: executor,
	}
	if leased {
		session.requestMu.Lock()
	}

	session.mu.Lock()
	m.mu.Lock()
	if m.work.Closing() {
		m.mu.Unlock()
		session.mu.Unlock()
		if leased {
			session.requestMu.Unlock()
		}
		_, rollbackErr := m.executeTerminal(context.Background(), session, "ROLLBACK")
		m.work.RecordError(fmt.Sprintf("rollback rejected BEGIN for database %s", dbName), rollbackErr)
		return nil, compose.Outcomes(fmt.Errorf("transaction manager is closed"), rollbackErr)
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
	closed := m.work.Closing()
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
	current := !m.work.Closing() && m.sessions[txID] == session
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
	lease, accepted := m.newLease(session)
	if !accepted {
		session.requestMu.Unlock()
		return nil, false
	}
	return lease, true
}

// Delete retires a forgotten session through the same terminal owner. Its
// legacy void API retains cleanup errors for WaitClosed.
func (m *Manager) Delete(txID string) {
	m.mu.Lock()
	session := m.sessions[txID]
	release, accepted := func() {}, false
	if session != nil {
		_, release, accepted = m.work.Acquire(context.Background())
	}
	m.mu.Unlock()
	if !accepted {
		return
	}
	defer release()
	session.requestMu.Lock()
	defer session.requestMu.Unlock()
	session.mu.Lock()
	rollback := !session.completed && session.terminalErr == nil && session.Executor != nil
	session.mu.Unlock()
	completed := session.Executor == nil || !session.Executor.HasActiveTransaction()
	if rollback && !completed {
		_, err := m.executeTerminal(context.Background(), session, "ROLLBACK")
		completed = !session.Executor.HasActiveTransaction()
		m.work.RecordError(fmt.Sprintf("rollback deleted explicit transaction %s", session.ID), err)
	}
	session.mu.Lock()
	session.completed = session.completed || completed
	m.deleteSessionLocked(session)
	session.mu.Unlock()
}

func (m *Manager) SetTerminalErrorObserver(observer func(*Session, error)) {
	m.mu.Lock()
	m.terminalErrorObserver = observer
	m.mu.Unlock()
}

func (m *Manager) Touch(session *Session) {
	m.touch(session, false)
}

// A retained request starts its next idle interval on actual completion. An
// unleased Touch cannot revive an idle interval that has already ended.
func (m *Manager) touch(session *Session, requestCompleted bool) {
	if session == nil {
		return
	}
	session.mu.Lock()
	m.mu.RLock()
	registered := !m.work.Closing() && m.sessions[session.ID] == session
	m.mu.RUnlock()
	expire := registered && !session.expired && !requestCompleted && !m.nowFunc().Before(session.Expires)
	generation := session.expiryGeneration
	if registered && !session.expired && !expire {
		m.resetExpiryTimerLocked(session)
	}
	session.mu.Unlock()
	if expire && session.requestMu.TryLock() {
		defer session.requestMu.Unlock()
		m.expireSessionUnderLease(session, generation)
	}
}

// Close rejects new sessions, rolls back every remaining transaction, removes
// it from the manager and stops its expiry timer. It is safe to call repeatedly.
func (m *Manager) Close() error {
	return m.CloseContext(context.Background())
}

// CloseContext rejects new sessions, rolls back every registered session once
// its active request has released it, and joins the manager's actual
// completion: rollbacks, leased requests, expiry timers and terminal
// observers. Every caller, first or repeated, joins the same completion and
// receives the same outcomes; ctx bounds only this caller's wait.
func (m *Manager) CloseContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.beginClose()
	return m.work.Shutdown(ctx, nil)
}

// beginClose runs once: it enrolls each registered session's rollback before
// sealing admission, in the same critical section that removes the sessions
// from the map, then signals cancellation and launches the rollbacks.
func (m *Manager) beginClose() {
	m.mu.Lock()
	if m.work.Closing() {
		m.mu.Unlock()
		return
	}
	sessions := make([]*Session, 0, len(m.sessions))
	releases := make([]func(), 0, len(m.sessions))
	for _, session := range m.sessions {
		_, release, _ := m.work.Acquire(context.Background())
		sessions = append(sessions, session)
		releases = append(releases, release)
	}
	m.sessions = make(map[string]*Session)
	m.work.CloseAdmission()
	m.mu.Unlock()
	m.work.Cancel()

	for i, session := range sessions {
		// Stop timers without waiting for a request lease. Most response writes
		// retain requestMu after releasing session.mu, so this also prevents an
		// idle timer from outliving a deadline-bounded shutdown.
		if session.mu.TryLock() {
			m.stopExpiryTimerLocked(session)
			session.expired = true
			session.mu.Unlock()
		}

		go func(session *Session, release func()) {
			defer release()
			session.requestMu.Lock()
			session.mu.Lock()
			m.stopExpiryTimerLocked(session)
			session.expired = true
			var terminalErr error
			var notify bool
			var closeErr error
			if !session.completed && session.terminalErr == nil && session.Executor != nil {
				session.mu.Unlock()
				_, err := m.executeTerminal(context.Background(), session, "ROLLBACK")
				completed := !session.Executor.HasActiveTransaction()
				terminal := isTerminalLifecycleError(err)
				if err != nil {
					closeErr = fmt.Errorf("rollback explicit transaction %s: %w", session.ID, err)
				}
				session.mu.Lock()
				terminalErr, notify = m.rememberTerminalErrorLocked(session, err, terminal)
				session.completed = completed
			} else if session.terminalErr != nil {
				session.completed = true
			}
			session.mu.Unlock()
			session.requestMu.Unlock()
			m.notifyTerminalError(session, terminalErr, notify)
			m.work.RecordError("", closeErr)
		}(session, releases[i])
	}
}

// WaitClosed waits for actual cleanup completion after Close/CloseContext starts,
// including expiry workers already absent from sessions and terminal observers.
// It does not initiate close.
func (m *Manager) WaitClosed(ctx context.Context) error {
	return m.work.WaitClosed(ctx)
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
	m.stopExpiryTimerLocked(session)
	generation := session.expiryGeneration
	session.Expires = m.nowFunc().Add(m.ttl)
	// The timer is enrolled in the manager's lifetime before it can run and is
	// released only by a successful Stop or by its callback's actual return.
	session.expiryTimer = lifecycle.AfterFuncOwned(m.work.Context(), m.armAfterFunc, m.ttl, m.retainWork, func() {
		m.expireSession(session, generation)
	})
}

// retainWork enrolls one unit of manager work, such as an armed expiry timer.
func (m *Manager) retainWork() (func(), bool) {
	_, release, accepted := m.work.Acquire(context.Background())
	return release, accepted
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
	m.expireSessionUnderLease(session, generation)
}

// The timer, admission and idle Touch paths share this retirement operation.
// Its caller owns requestMu; an active request renews only when it completes.
func (m *Manager) expireSessionUnderLease(session *Session, generation uint64) {
	session.mu.Lock()
	if session.expiryGeneration != generation {
		session.mu.Unlock()
		return
	}
	m.mu.Lock()
	current := !m.work.Closing() && m.sessions[session.ID] == session
	release := func() {}
	if current {
		_, release, current = m.work.Acquire(m.work.Context())
		delete(m.sessions, session.ID)
	}
	m.mu.Unlock()
	if !current {
		m.stopExpiryTimerLocked(session)
		session.mu.Unlock()
		return
	}
	defer release()

	// Retain the retirement above before stopping a still-armed timer. A
	// clock/admission expiry can precede its native firing; that timer's credit
	// must be released without opening a gap in the manager's completion join.
	m.stopExpiryTimerLocked(session)
	session.expired = true
	var terminalErr error
	var notify bool
	if session.Executor != nil && session.terminalErr == nil {
		session.mu.Unlock()
		// The expiry is the manager's accepted work: its ROLLBACK keeps the
		// owner's values but not its cancellation, so a concurrent stop joins
		// it instead of abandoning the transaction mid-rollback.
		_, err := m.executeTerminal(context.WithoutCancel(m.work.Context()), session, "ROLLBACK")
		completed := !session.Executor.HasActiveTransaction()
		m.work.RecordError(fmt.Sprintf("rollback expired explicit transaction %s", session.ID), err)
		terminal := isTerminalLifecycleError(err)
		session.mu.Lock()
		terminalErr, notify = m.rememberTerminalErrorLocked(session, err, terminal)
		session.completed = completed
	}
	if session.completed {
		session.Executor = nil
	}
	session.mu.Unlock()
	m.notifyTerminalError(session, terminalErr, notify)
}

// operationLease keeps the legacy Session API on the same admission owner as
// request transports. A transport already holding a Lease uses its methods;
// it must not acquire a second exclusive operation on that same session.
func (m *Manager) operationLease(session *Session) (*Lease, error) {
	if session == nil {
		return nil, fmt.Errorf("transaction session is not available")
	}
	lease, accepted := m.AcquireForOwner(session.ID, session.Owner)
	if !accepted {
		return nil, fmt.Errorf("transaction session has expired or closed")
	}
	if lease.Session != session {
		lease.Release()
		return nil, fmt.Errorf("transaction session is not available")
	}
	return lease, nil
}

// operate runs one operation within this request's admission. A panic still
// propagates; the lease's release then keeps the existing idle deadline.
func (l *Lease) operate(op func(*Manager) error) error {
	if l == nil || l.manager == nil || l.Session == nil || l.retention == nil || l.retention.refs.Load() <= 0 {
		return fmt.Errorf("transaction request lease is not available")
	}
	returned := false
	defer func() {
		if !returned {
			l.retention.panicked.Store(true)
		}
	}()
	err := op(l.manager)
	returned = true
	return err
}

// ExecuteInSession retains a legacy session operation through actual return.
// Transports holding an opening/request Lease use Lease.ExecuteInSession.
func (m *Manager) ExecuteInSession(ctx context.Context, session *Session, query string, params map[string]interface{}) (*cypher.ExecuteResult, error) {
	lease, err := m.operationLease(session)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	return lease.ExecuteInSession(ctx, query, params)
}

// ExecuteInSession executes within this already-admitted request. Retain the
// lease through response publication, and release it only on actual completion.
func (l *Lease) ExecuteInSession(ctx context.Context, query string, params map[string]interface{}) (result *cypher.ExecuteResult, err error) {
	err = l.operate(func(manager *Manager) error {
		result, err = manager.executeInSession(ctx, l.Session, query, params)
		return err
	})
	return result, err
}

func (m *Manager) executeInSession(ctx context.Context, session *Session, query string, params map[string]interface{}) (*cypher.ExecuteResult, error) {
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

	session.mu.Unlock()
	result, err := session.Executor.Execute(ctx, query, params)
	if err != nil {
		terminal := isTerminalLifecycleError(err)
		session.mu.Lock()
		terminalErr, notify := m.rememberTerminalErrorLocked(session, err, terminal)
		session.mu.Unlock()
		m.notifyTerminalError(session, terminalErr, notify)
		return result, terminalErr
	}
	m.touch(session, true)
	return result, nil
}

// CommitAndDelete admits and joins a legacy session terminal operation.
// Transports already holding a request Lease use Lease.CommitAndDelete.
func (m *Manager) CommitAndDelete(ctx context.Context, session *Session) (*cypher.ExecuteResult, error) {
	lease, err := m.operationLease(session)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	return lease.CommitAndDelete(ctx)
}

// CommitAndDelete commits within this request's existing admission. Release
// the lease after response publication; Delete would reacquire the same lease.
func (l *Lease) CommitAndDelete(ctx context.Context) (result *cypher.ExecuteResult, err error) {
	err = l.operate(func(manager *Manager) error {
		result, err = manager.commitAndDelete(ctx, l.Session)
		return err
	})
	return result, err
}

func (m *Manager) commitAndDelete(ctx context.Context, session *Session) (_ *cypher.ExecuteResult, retErr error) {
	// Every failure returned here is this COMMIT's outcome, classified here; a
	// remembered statement failure keeps its own status.
	defer func() { retErr = nornicerrors.TransactionControlFailure(nornicerrors.TerminalCommit, retErr) }()
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

	session.mu.Unlock()
	commitCtx, admissionErr := txphase.CommitContext(ctx)
	if admissionErr != nil {
		if rollbackErr := m.rollbackAndDelete(ctx, session); rollbackErr != nil {
			return nil, nornicerrors.CombineTerminalOutcomes(admissionErr, rollbackErr)
		}
		return nil, admissionErr
	}
	// txphase owns HTTP admission. After admission, preserve the release
	// contract: COMMIT returns its real result independently of request cancel.
	result, err := m.executeTerminal(context.WithoutCancel(commitCtx), session, "COMMIT")
	completed := !session.Executor.HasActiveTransaction()
	terminal := isTerminalLifecycleError(err)
	session.mu.Lock()
	session.completed = completed
	if err != nil {
		terminalErr, notify := m.rememberTerminalErrorLocked(session, err, terminal)
		m.deleteSessionLocked(session)
		session.mu.Unlock()
		m.notifyTerminalError(session, terminalErr, notify)
		return nil, terminalErr
	}
	m.deleteSessionLocked(session)
	session.mu.Unlock()
	return result, nil
}

// RollbackAndDelete admits and joins a legacy session rollback. An already
// retired session remains an idempotent no-op; WaitClosed joins late cleanup.
func (m *Manager) RollbackAndDelete(ctx context.Context, session *Session) error {
	if session == nil {
		return fmt.Errorf("transaction session is not available")
	}
	session.mu.Lock()
	retired := session.expired || session.completed
	session.mu.Unlock()
	if retired {
		return nil
	}
	lease, err := m.operationLease(session)
	if err != nil {
		session.mu.Lock()
		retired = session.expired || session.completed
		session.mu.Unlock()
		if retired {
			return nil
		}
		return err
	}
	defer lease.Release()
	return lease.RollbackAndDelete(ctx)
}

// RollbackAndDelete rolls back within the existing request admission. Keep the
// lease until the request and its response publication have actually completed.
func (l *Lease) RollbackAndDelete(ctx context.Context) error {
	return l.operate(func(manager *Manager) error { return manager.rollbackAndDelete(ctx, l.Session) })
}

func (m *Manager) rollbackAndDelete(ctx context.Context, session *Session) (retErr error) {
	defer func() { retErr = nornicerrors.TransactionControlFailure(nornicerrors.TerminalRollback, retErr) }()
	if session == nil {
		return fmt.Errorf("transaction session is not available")
	}
	session.mu.Lock()
	if session.expired {
		m.deleteSessionLocked(session)
		session.mu.Unlock()
		return nil
	}
	if session.Executor == nil {
		session.mu.Unlock()
		return fmt.Errorf("transaction session is not available")
	}
	if session.terminalErr != nil {
		session.completed = true
		m.deleteSessionLocked(session)
		session.mu.Unlock()
		return nil
	}

	session.mu.Unlock()
	_, err := m.executeTerminal(context.WithoutCancel(ctx), session, "ROLLBACK")
	completed := !session.Executor.HasActiveTransaction()
	m.work.RecordError(fmt.Sprintf("rollback explicit transaction %s", session.ID), err)
	session.mu.Lock()
	session.completed = completed
	m.deleteSessionLocked(session)
	session.mu.Unlock()
	return err
}

// executeTerminal runs under an admitted opening/cleanup or request lease, outside
// host state locks. A rejected COMMIT can return before its executor enters
// the terminal operation; that active transaction still needs actual cleanup.
// HasActiveTransaction is the executor's canonical truth, also consumed by Bolt.
func (m *Manager) executeTerminal(ctx context.Context, session *Session, command string) (*cypher.ExecuteResult, error) {
	if command == "ROLLBACK" {
		return nil, rollbackOutcome(ctx, session.Executor)
	}
	result, err := session.Executor.Execute(cypher.WithTransactionControl(ctx), command, nil)
	if err != nil && command == "COMMIT" && session.Executor.HasActiveTransaction() {
		rollbackErr := rollbackOutcome(context.WithoutCancel(ctx), session.Executor)
		m.work.RecordError(fmt.Sprintf("rollback after failed commit of explicit transaction %s", session.ID), rollbackErr)
		err = compose.Outcomes(err, rollbackErr)
	}
	return result, err
}

// rollbackOutcome reports a panicking rollback as its outcome. Rollback also
// runs from expiry timers and cleanup, where a panic would end the process.
func rollbackOutcome(ctx context.Context, executor *cypher.StorageExecutor) (err error) {
	defer func() {
		if cause := recover(); cause != nil {
			if causeErr, ok := cause.(error); ok {
				err = fmt.Errorf("transaction rollback panicked: %w", causeErr)
			} else {
				err = fmt.Errorf("transaction rollback panicked: %v", cause)
			}
		}
	}()
	_, err = executor.Execute(cypher.WithTransactionControl(ctx), "ROLLBACK", nil)
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

func (m *Manager) rememberTerminalErrorLocked(session *Session, err error, terminal bool) (error, bool) {
	if session == nil || err == nil {
		return err, false
	}
	if session.terminalErr != nil {
		return session.terminalErr, false
	}
	if !terminal {
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
