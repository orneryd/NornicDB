package server

import (
	"context"
	"net/http"
	"sync"

	"github.com/orneryd/nornicdb/pkg/errors/compose"
	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/txsession"
	"github.com/orneryd/nornicdb/pkg/util"
)

// serverShutdown is the server's use of its one lifecycle owner: admission,
// actual work completion and resource retirement. A caller's deadline only
// bounds waiting for this owner; it never retires a resource or revokes work
// that has already been accepted. Each producer registers its stop signal with
// the owner where it publishes its handle; retire runs after quiescence.
type serverShutdown struct {
	mu                 sync.Mutex
	work               lifecycle.WorkLifetime
	budget             context.Context // the first Stop caller's bound for transport drains
	unpublishedRouters []*heimdallDBRouter
	dbRelease          func()
	stopOnDBClose      func() bool
}

// stopBudget is the first stop caller's context; producers' drains use it.
func (d *serverShutdown) stopBudget() context.Context {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.budget == nil {
		return context.Background()
	}
	return d.budget
}

// Both the handler and TimeoutHandler's outer publisher consume the same lease.
func (s *Server) retainTransactionResponse(ctx context.Context, lease *txsession.Lease) {
	share := lease.Retain()
	if err := s.shutdown.work.OnRequestComplete(ctx, share.Release); err != nil {
		share.Release()
		s.shutdown.work.RecordError("transaction response", err)
	}
}

func (s *Server) runBackground(ctx context.Context, fn func(context.Context)) bool {
	ctx, release, ok := s.shutdown.work.Acquire(ctx)
	if !ok {
		return false
	}
	ctx = context.WithoutCancel(ctx)
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.shutdown.work.Context(), cancel)
	go func() { defer release(); defer cancel(); defer stop(); fn(ctx) }()
	return true
}

func (s *Server) workMiddleware(next http.Handler) http.Handler { return s.shutdown.work.Handler(next) }

// beginShutdown starts the owner's one stop. The first caller's context is the
// drain budget; it reports whether this call was that first caller.
func (s *Server) beginShutdown(ctx context.Context) bool {
	d := &s.shutdown
	d.mu.Lock()
	first := d.work.CloseAdmission()
	if first {
		d.budget = ctx
	}
	d.mu.Unlock()
	d.work.BeginStop(s.retire)
	return first
}

// retire is the owner's dispose. It runs once, after every accepted handler,
// detached child, initializer, RPC and producer stop signal actually returned.
func (s *Server) retire() error {
	d := &s.shutdown
	var err error
	if s.txSessions != nil {
		err = compose.Outcomes(err, s.txSessions.WaitClosed(context.Background()))
	}
	err = compose.Outcomes(err, s.disposeHeimdallPlugins())
	d.mu.Lock()
	routers := d.unpublishedRouters
	d.unpublishedRouters = nil
	d.mu.Unlock()
	for _, router := range routers {
		router.Close()
	}
	s.lifecycleMu.Lock()
	pending := s.pendingListener
	s.pendingListener = nil
	s.lifecycleMu.Unlock()
	if pending != nil {
		err = compose.Outcomes(err, pending.Close())
	}
	s.statementCacheIndex.Close()
	if s.executorCache != nil {
		s.executorCache.Clear()
	}
	if s.rateLimiter != nil {
		s.rateLimiter.Stop()
	}
	if s.slowQueryFile != nil {
		err = compose.Outcomes(err, s.slowQueryFile.Close())
	}
	if d.stopOnDBClose != nil {
		d.stopOnDBClose()
	}
	// The canonical DB lifetime retains borrowed executors and storage until
	// every server user and teardown step above has actually finished.
	if d.dbRelease != nil {
		d.dbRelease()
	}
	return err
}

// WaitStopped waits for actual completion, including outcomes produced after a
// Stop deadline. It does not initiate shutdown.
func (s *Server) WaitStopped(ctx context.Context) error {
	return s.shutdown.work.WaitStopped(ctx)
}

// stopHTTPServer is the HTTP transport's stop signal: it cancels request
// contexts and drains the server within the first stop caller's budget.
func (s *Server) stopHTTPServer(server *http.Server, cancelRequests context.CancelFunc) func() error {
	return func() error {
		cancelRequests()
		return util.ShutdownHTTPServer(s.shutdown.stopBudget(), server)
	}
}
