package bolt

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type startupLogGate struct {
	entered, release chan struct{}
	once             sync.Once
}

func (h *startupLogGate) Enabled(context.Context, slog.Level) bool { return true }
func (h *startupLogGate) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *startupLogGate) WithGroup(string) slog.Handler            { return h }
func (h *startupLogGate) Handle(context.Context, slog.Record) error {
	h.once.Do(func() { close(h.entered); <-h.release })
	return nil
}

type shutdownListenerGate struct {
	net.Listener
	entered, release chan struct{}
	calls            atomic.Int32
}

func (l *shutdownListenerGate) Close() error {
	if l.calls.Add(1) == 1 {
		close(l.entered)
		<-l.release
	}
	return l.Listener.Close()
}

func TestCloseDuringDiscoveryStartupClosesListenerOnce(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	logGate := &startupLogGate{entered: make(chan struct{}), release: make(chan struct{})}
	closeGate := &shutdownListenerGate{Listener: listener, entered: make(chan struct{}), release: make(chan struct{})}
	var releaseLog, releaseClose sync.Once
	defer releaseClose.Do(func() { close(closeGate.release) })
	defer releaseLog.Do(func() { close(logGate.release) })
	server := New(&Config{Logger: slog.New(logGate)}, &mockExecutor{})
	served, closed := make(chan error, 1), make(chan error, 1)
	go func() { served <- server.Serve(closeGate) }()
	select {
	case <-logGate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not publish listener")
	}
	go func() { closed <- server.Close() }()
	select {
	case <-closeGate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not reach listener")
	}
	releaseLog.Do(func() { close(logGate.release) })
	select {
	case err := <-served:
		require.ErrorContains(t, err, "closed")
	case <-time.After(5 * time.Second):
		t.Fatal("startup did not observe shutdown")
	}
	releaseClose.Do(func() { close(closeGate.release) })
	require.NoError(t, <-closed)
	require.EqualValues(t, 1, closeGate.calls.Load(), "startup and shutdown must transfer listener ownership")
}
