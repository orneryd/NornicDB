package bolt

// Author: Codex. Public transport-budget class schedule at the pushed native
// implementation: a still-admitted budget writer must not undo lifetime stop.
import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type classBudgetListener struct {
	net.Listener
	entered, signal, release chan struct{}
	budgetOnce, signalOnce   sync.Once
}
type classBudgetConn struct {
	net.Conn
	owner *classBudgetListener
}

func (l *classBudgetListener) Accept() (net.Conn, error) {
	c, e := l.Listener.Accept()
	if e != nil {
		return nil, e
	}
	return &classBudgetConn{Conn: c, owner: l}, nil
}
func (c *classBudgetConn) SetReadDeadline(d time.Time) error {
	if !d.IsZero() && time.Until(d) > time.Minute {
		c.owner.budgetOnce.Do(func() { close(c.owner.entered) })
		<-c.owner.release
	}
	return c.Conn.SetReadDeadline(d)
}
func (c *classBudgetConn) SetDeadline(d time.Time) error {
	err := c.Conn.SetDeadline(d)
	if !d.IsZero() && time.Until(d) < time.Second {
		c.owner.signalOnce.Do(func() { close(c.owner.signal) })
	}
	return err
}

func TestOwnerClassNativeStopOwnsTransportBudgets(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	l := &classBudgetListener{Listener: ln, entered: make(chan struct{}), signal: make(chan struct{}), release: gate}
	cfg := DefaultConfig()
	cfg.BoltSniffTimeout = time.Hour
	s := New(cfg, &mockExecutor{})
	served := make(chan error, 1)
	go func() { served <- s.Serve(l) }()
	c, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { release(); _ = c.Close(); _ = s.Close() })
	select {
	case <-l.entered:
	case <-time.After(time.Second):
		t.Fatal("public sniff budget was not applied")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, s.Shutdown(ctx), context.DeadlineExceeded)
	select {
	case <-l.signal:
	case <-time.After(time.Second):
		t.Fatal("lifetime stop did not signal native I/O")
	}
	// The native stop deadline is now installed. Let the earlier writer resume.
	release()
	joined, joinCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer joinCancel()
	if err := s.WaitStopped(joined); err != nil {
		t.Errorf("an admitted transport budget replaced lifetime stop: %v", err)
	}
	_ = c.Close() // controlled escape for the old implementation only
	finish, finishCancel := context.WithTimeout(context.Background(), time.Second)
	defer finishCancel()
	require.NoError(t, s.WaitStopped(finish))
	select {
	case <-served:
	case <-finish.Done():
		t.Fatal("Serve did not finish after native escape")
	}
}
