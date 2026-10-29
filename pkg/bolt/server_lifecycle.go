package bolt

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/util"
)

// lifetime is initialized before a start site can run. A configured TLS
// constructor and its server consume this same admission/completion owner.
func (s *Server) lifetime() *util.WorkLifetime {
	s.lifecycleMu.Lock()
	if s.work == nil {
		s.work = &util.WorkLifetime{}
	}
	if s.stopDone == nil {
		s.stopDone = make(chan struct{})
	}
	w := s.work
	s.lifecycleMu.Unlock()
	return w
}

func (s *Server) beginServe() (context.Context, func(), error) {
	w := s.lifetime()
	ctx, release, accepted := w.Acquire(w.Context())
	if !accepted {
		return nil, nil, fmt.Errorf("bolt server is closed")
	}
	s.lifecycleMu.Lock()
	if s.closed.Load() || s.serving {
		s.lifecycleMu.Unlock()
		release()
		return nil, nil, fmt.Errorf("bolt server is closed or already serving")
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
// The worker signals failed startup but never joins its own retained frame.
func (s *Server) runServing(ctx context.Context, listen func(context.Context) (net.Listener, error)) (error, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
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
		if err != nil {
			w.RecordError("Bolt serve", err)
			s.BeginShutdown()
		}
		result <- err
	}()
	select {
	case err := <-result:
		if err != nil {
			return errors.Join(err, s.WaitStopped(ctx)), true
		}
		return nil, true
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
	w := s.lifetime()
	s.lifecycleMu.Lock()
	done := s.stopDone
	if s.stopStarted {
		s.lifecycleMu.Unlock()
		return done
	}
	s.stopStarted = true
	s.closed.Store(true)
	s.lifecycleMu.Unlock()
	w.CloseAdmission()
	w.Cancel()
	// This is the retirement driver, joined through stopDone, not an admitted
	// user of the group it must drain. No host lock surrounds foreign code.
	go func() {
		if err := s.closeListener(); !util.ErrorIsOnly(err, net.ErrClosed) {
			w.RecordError("close Bolt listener", err)
		}
		<-w.Quiescent()
		s.statementCacheIndex.Close()
		s.executorsMu.Lock()
		s.executors = nil
		s.executorsMu.Unlock()
		s.fallbackStatementCaches().Dispose()
		err := w.Err()
		s.lifecycleMu.Lock()
		s.stopErr = err
		close(s.stopDone)
		s.lifecycleMu.Unlock()
	}()
	return done
}

// Shutdown bounds only the caller; the driver retains every actual completion.
func (s *Server) Shutdown(ctx context.Context) error {
	s.BeginShutdown()
	return s.WaitStopped(ctx)
}

// WaitStopped observes actual retirement without initiating another shutdown.
func (s *Server) WaitStopped(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	w := s.lifetime()
	s.lifecycleMu.Lock()
	done := s.stopDone
	s.lifecycleMu.Unlock()
	select {
	case <-done:
		s.lifecycleMu.Lock()
		err := s.stopErr
		s.lifecycleMu.Unlock()
		return err
	case <-ctx.Done():
		return errors.Join(w.Err(), ctx.Err())
	}
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
	owner   *util.WorkLifetime
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
		err = errors.Join(err, set(time.Now()))
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
	done := make(chan struct{})
	stop := context.AfterFunc(w.Context(), func() {
		defer close(done)
		if err := conn.SetDeadline(time.Now()); !util.ErrorIsOnly(err, net.ErrClosed) {
			w.RecordError("interrupt Bolt connection", err)
		}
	})
	return func() {
		if !stop() {
			<-done
		}
	}
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
