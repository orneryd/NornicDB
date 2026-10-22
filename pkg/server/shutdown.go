package server

import (
	"context"
	"errors"
	"net/http"
	"sync"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/txsession"
	"github.com/orneryd/nornicdb/pkg/util"
)

// serverShutdown owns admission, actual work completion and resource retirement.
// A caller's deadline only bounds waiting for this owner; it never retires a
// resource or revokes work that has already been accepted.
type serverShutdown struct {
	initOnce, driverOnce sync.Once
	mu                   sync.Mutex
	closing              bool
	work                 util.WorkLifetime
	signal, done         chan struct{}
	budget               context.Context
	err                  error
	workContext          context.Context
	cancelWork           context.CancelFunc
	unpublishedRouters   []*heimdallDBRouter
	constructionDone     chan struct{}
	dbRelease            func()
}

func (d *serverShutdown) init() {
	d.initOnce.Do(func() {
		d.signal = make(chan struct{})
		d.done = make(chan struct{})
		d.workContext = d.work.Context()
		d.cancelWork = d.work.Cancel
	})
}
func (d *serverShutdown) acquire(ctx context.Context) (context.Context, func(), bool) {
	d.init()
	return d.work.Acquire(ctx)
}

// Both the handler and TimeoutHandler's outer publisher consume the same lease.
func retainTransactionResponse(ctx context.Context, lease *txsession.Lease) {
	share := lease.Retain()
	if !util.OnRequestComplete(ctx, share.Release) {
		share.Release()
	}
}

func (s *Server) runBackground(ctx context.Context, fn func(context.Context)) bool {
	ctx, release, ok := s.shutdown.acquire(ctx)
	if !ok {
		return false
	}
	ctx = context.WithoutCancel(ctx)
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.shutdown.workContext, cancel)
	go func() { defer release(); defer cancel(); defer stop(); fn(ctx) }()
	return true
}

func (s *Server) workMiddleware(next http.Handler) http.Handler { return s.shutdown.work.Handler(next) }
func retainServerHandler(next http.Handler, r *http.Request) http.Handler {
	return util.RetainHandler(next, r)
}

func (s *Server) beginShutdown(ctx context.Context) bool {
	s.shutdown.init()
	d := &s.shutdown
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closing {
		return false
	}
	d.closing = true
	s.closed.Store(true)
	d.budget = ctx
	d.work.CloseAdmission()
	close(d.signal)
	return true
}

func (s *Server) startShutdownDriver() {
	s.shutdown.init()
	s.shutdown.driverOnce.Do(func() {
		run := func() {
			if s.shutdown.dbRelease != nil {
				defer s.shutdown.dbRelease()
			}
			if s.db != nil {
				select {
				case <-s.shutdown.signal:
				case <-s.db.LifetimeContext().Done():
					s.beginShutdown(context.Background())
				}
			} else {
				<-s.shutdown.signal
			}
			s.finishShutdown()
		}
		// The canonical DB lifetime retains borrowed executors/storage until all
		// server work and teardown callbacks have actually finished, even when the
		// Stop caller timed out or DB.Close was called without Stop.
		if s.shutdown.dbRelease != nil || s.db == nil {
			go run()
		} else if !s.db.RunBackgroundTask(run) {
			// Only incomplete, directly assembled internal fixtures reach this
			// branch. New refuses DB admission before constructing any component.
			s.shutdown.addError(nornicdb.ErrClosed)
			s.beginShutdown(context.Background())
			go run()
		}
	})
}

func (d *serverShutdown) addError(err error) {
	if err == nil {
		return
	}
	d.mu.Lock()
	d.err = errors.Join(d.err, err)
	d.mu.Unlock()
}
func (d *serverShutdown) result() error { d.mu.Lock(); defer d.mu.Unlock(); return d.err }

func (s *Server) finishShutdown() {
	d := &s.shutdown
	d.cancelWork()
	pluginStopped := make(chan error, 1)
	go func() { pluginStopped <- s.signalHeimdallPlugins() }()
	if d.constructionDone != nil {
		<-d.constructionDone
	}
	// Start publishes all transport handles under lifecycleMu. This lock and all
	// subsequent locks/callbacks belong to the eventual owner, never Stop's caller.
	s.lifecycleMu.Lock()
	cancelRequests, httpServer := s.cancelRequestsCtx, s.httpServer
	s.lifecycleMu.Unlock()
	d.mu.Lock()
	ctx := d.budget
	d.mu.Unlock()
	if cancelRequests != nil {
		cancelRequests()
	}
	if s.embedderInitCancel != nil {
		s.embedderInitCancel()
	}

	// Signal EVERY producer before joining work: a plugin's Stop may be what
	// lets accepted actions finish. Signal callbacks run concurrently so one
	// component's join cannot withhold another component's stop signal.
	grpcDone := make(chan struct{})
	go func() { defer close(grpcDone); s.stopQdrantGRPC() }()
	txClosed, httpClosed := make(chan error, 1), make(chan error, 1)
	go func() {
		var err error
		if s.txSessions != nil {
			err = s.txSessions.CloseContext(ctx)
		}
		txClosed <- err
	}()
	go func() {
		var err error
		if httpServer != nil {
			err = util.ShutdownHTTPServer(ctx, httpServer)
		}
		httpClosed <- err
	}()

	// Join. Transport cancellation and an empty session map are not completion. Retain
	// every resource until accepted handlers, detached children, initializers,
	// RPCs, and manager rollback/observer workers have really returned.
	<-d.work.Quiescent()
	if s.embedderInitDone != nil {
		<-s.embedderInitDone
	}
	if s.txSessions != nil {
		d.addError(s.txSessions.WaitClosed(context.Background()))
	}
	d.addError(<-txClosed)
	d.addError(<-httpClosed)
	<-grpcDone
	d.addError(<-pluginStopped)

	// Dispose only after both producer-stop callbacks and actual users finish.
	d.addError(s.disposeHeimdallPlugins())
	for _, router := range d.unpublishedRouters {
		router.Close()
	}
	if s.pendingListener != nil {
		d.addError(s.pendingListener.Close())
	}
	s.statementCacheIndex.Close()
	if s.executorCache != nil {
		s.executorCache.Clear()
	}
	if s.rateLimiter != nil {
		s.rateLimiter.Stop()
	}
	if s.slowQueryFile != nil {
		d.addError(s.slowQueryFile.Close())
	}
	close(d.done)
}

// WaitStopped waits for actual completion, including outcomes produced after a
// Stop deadline. It does not initiate shutdown. Repeated Stop remains a no-op.
func (s *Server) WaitStopped(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.shutdown.init()
	select {
	case <-s.shutdown.done:
		return s.shutdown.result()
	case <-ctx.Done():
		return errors.Join(s.shutdown.result(), ctx.Err())
	}
}
