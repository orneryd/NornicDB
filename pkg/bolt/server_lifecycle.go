package bolt

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/errors/compose"
	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/util"
)

// lifetime is initialized before a start site can run. A configured TLS
// constructor and its server consume this same admission/completion owner.
// Its closing state is the server's only closed state.
func (s *Server) lifetime() *lifecycle.WorkLifetime {
	if w := s.work.Load(); w != nil {
		return w
	}
	s.work.CompareAndSwap(nil, &lifecycle.WorkLifetime{})
	return s.work.Load()
}

// errServerClosed reports that a start attempt observed the server's one stop
// instead of serving. The attempt was cancelled, not failed: the caller that
// began the stop owns its outcome and its join.
var errServerClosed = errors.New("bolt server is closed")

func (s *Server) beginServe() (context.Context, func(), error) {
	w := s.lifetime()
	ctx, release, accepted := w.Acquire(w.Context())
	if !accepted {
		return nil, nil, errServerClosed
	}
	s.lifecycleMu.Lock()
	if w.Closing() {
		s.lifecycleMu.Unlock()
		release()
		return nil, nil, errServerClosed
	}
	if s.serving {
		s.lifecycleMu.Unlock()
		release()
		return nil, nil, fmt.Errorf("bolt server is already serving")
	}
	s.serving = true
	s.lifecycleMu.Unlock()
	return ctx, func() {
		s.lifecycleMu.Lock()
		s.serving = false
		s.lifecycleMu.Unlock()
		release()
	}, nil
}

// Every start path enrolls before its worker or listener factory can run.
// The worker signals failed startup but never joins its own retained frame,
// and a start that observes another caller's stop returns that cancelled
// attempt without joining a close this frame does not own.
func (s *Server) runServing(ctx context.Context, listen func(context.Context) (net.Listener, error)) (error, bool) {
	if err := ctx.Err(); err != nil {
		return err, false
	}
	native, release, err := s.beginServe()
	if err != nil {
		return err, false
	}
	w := s.lifetime()
	result := make(chan error, 1)
	go func() {
		defer release()
		listener, err := listen(native)
		if err == nil {
			err = s.serveListener(native, listener)
		}
		if native.Err() != nil && util.ErrorIsOnly(err, native.Err()) {
			err = nil
		}
		// A start attempt that observes the server's own stop is a cancelled
		// attempt, not a service failure: the stop already belongs to the
		// caller that began it, and those outcomes are that caller's to join.
		if err != nil && !errors.Is(err, errServerClosed) {
			w.RecordError("Bolt serve", err)
			s.BeginShutdown()
		}
		result <- err
	}()
	select {
	case err := <-result:
		if err == nil {
			return nil, true
		}
		if errors.Is(err, errServerClosed) {
			// Join no close this frame does not own: the stop it observed is
			// already in progress, and its initiator waits for this frame.
			return err, true
		}
		return compose.Outcomes(err, s.WaitStopped(ctx)), true
	case <-ctx.Done():
		s.BeginShutdown()
		return ctx.Err(), true
	}
}

// Close stops and joins every accepted native lifetime before returning.
func (s *Server) Close() error { return s.Shutdown(context.Background()) }

// BeginShutdown closes admission and signals the same owner used by native
// constructors and workers. A supervisor signals all siblings before joining
// any one of them. The returned channel closes after disposal and publication.
func (s *Server) BeginShutdown() <-chan struct{} {
	return s.lifetime().BeginStop(s.retire)
}

// retire runs once, after every accepted connection and serve loop returned.
func (s *Server) retire() error {
	s.statementCacheIndex.Close()
	s.executorsMu.Lock()
	s.executors = nil
	s.executorsMu.Unlock()
	s.fallbackStatementCaches().Dispose()
	return nil
}

// Shutdown bounds only the caller; every caller joins the same actual stop.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.lifetime().Shutdown(ctx, s.retire)
}

// WaitStopped observes actual retirement without initiating another shutdown.
func (s *Server) WaitStopped(ctx context.Context) error {
	return s.lifetime().WaitStopped(ctx)
}

func (s *Server) startConnection(ctx context.Context, conn net.Conn) {
	w := s.lifetime()
	ctx, release, accepted := w.Acquire(ctx)
	if !accepted {
		if err := conn.Close(); !util.ErrorIsOnly(err, net.ErrClosed) {
			w.RecordError("close rejected Bolt connection", err)
		}
		return
	}
	conn = s.ownConnection(conn)
	go func() { defer release(); s.handleConnectionOwned(ctx, conn) }()
}

// Keep the synchronous private entry used by embedded package consumers. The
// serving path enrolls before its go statement through startConnection.
func (s *Server) handleConnection(conn net.Conn) {
	w := s.lifetime()
	ctx, release, accepted := w.Acquire(w.Context())
	if !accepted {
		if err := conn.Close(); !util.ErrorIsOnly(err, net.ErrClosed) {
			w.RecordError("close rejected Bolt connection", err)
		}
		return
	}
	defer release()
	s.handleConnectionOwned(ctx, s.ownConnection(conn))
}

// lifetimeConn keeps protocol budgets subordinate to the immutable service
// lifetime. Recheck after the foreign setter: an already-admitted setter can
// return after cancellation installed its stop deadline.
type lifetimeConn struct {
	net.Conn
	owner   *lifecycle.WorkLifetime
	closing atomic.Bool
}

func (s *Server) ownConnection(conn net.Conn) net.Conn {
	if tcp, ok := conn.(*net.TCPConn); ok {
		if err := tcp.SetNoDelay(true); !util.ErrorIsOnly(err, net.ErrClosed) {
			s.lifetime().RecordError("configure Bolt TCP transport", err)
		}
	}
	return &lifetimeConn{Conn: conn, owner: s.lifetime()}
}

func (c *lifetimeConn) setDeadline(d time.Time, writeOnly bool, set func(time.Time) error) error {
	err := set(d)
	// TLS/WS disposal may set a bounded write deadline for its closing frame.
	// Read budgets remain sealed, and the physical Close still ends all I/O.
	if c.owner.Context().Err() != nil && (!writeOnly || !c.closing.Load()) {
		err = compose.Outcomes(err, set(time.Now()))
	}
	if !util.ErrorIsOnly(err, net.ErrClosed) {
		c.owner.RecordError("set Bolt transport deadline", err)
	}
	return err
}
func (c *lifetimeConn) SetDeadline(d time.Time) error {
	return c.setDeadline(d, false, c.Conn.SetDeadline)
}
func (c *lifetimeConn) SetReadDeadline(d time.Time) error {
	return c.setDeadline(d, false, c.Conn.SetReadDeadline)
}
func (c *lifetimeConn) SetWriteDeadline(d time.Time) error {
	return c.setDeadline(d, true, c.Conn.SetWriteDeadline)
}

// Signal I/O interruption without bypassing the handler's outer TLS/WS Close.
// The caller retains its connection ticket until this callback really returns.
func (s *Server) interruptConnection(conn net.Conn) func() {
	w := s.lifetime()
	return lifecycle.AfterFuncJoined(w.Context(), func() {
		if err := conn.SetDeadline(time.Now()); !util.ErrorIsOnly(err, net.ErrClosed) {
			w.RecordError("interrupt Bolt connection", err)
		}
	})
}

func (s *Session) closeConnection() {
	if s.closeTransport != nil {
		s.closeTransport()
		return
	}
	if s.conn != nil {
		err := s.conn.Close()
		if s.server != nil && !util.ErrorIsOnly(err, net.ErrClosed) {
			s.server.lifetime().RecordError("close Bolt connection", err)
		}
	}
}

// parentContext is the session's lifetime for the work its messages start:
// the connection context (cancelled on disconnect and by the server's stop),
// else the session span, else the Bolt server's lifetime. Every session
// belongs to a server.
func (s *Session) parentContext() context.Context {
	if s.connCtx != nil {
		return s.connCtx
	}
	if s.spanCtx != nil {
		return s.spanCtx
	}
	return s.server.lifetime().Context()
}
