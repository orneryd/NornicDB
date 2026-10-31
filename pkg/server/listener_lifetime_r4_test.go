package server

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type r4OwnedHTTPListener struct {
	net.Listener
	err       error
	closed    chan struct{}
	once      sync.Once
	writeGate *stopR2Gate
}

type r4AddressProbeListener struct {
	net.Listener
	probe      func()
	enabled    atomic.Bool
	accepting  chan struct{}
	acceptOnce sync.Once
}

func (l *r4AddressProbeListener) Accept() (net.Conn, error) {
	l.acceptOnce.Do(func() { close(l.accepting) })
	return l.Listener.Accept()
}

func (l *r4AddressProbeListener) Addr() net.Addr {
	if l.enabled.CompareAndSwap(true, false) {
		l.probe()
	}
	return l.Listener.Addr()
}

func TestServerStopR4ListenerAddressReentry(t *testing.T) {
	s, _ := setupTestServerWithConfig(t, func(c *Config) { c.Address = "127.0.0.1"; c.Port = 0; c.MCPEnabled = false })
	read := make(chan error, 1)
	accepting := make(chan struct{})
	var listener *r4AddressProbeListener
	s.listenNative = func(network, addr string) (net.Listener, error) {
		native, e := net.Listen(network, addr)
		if e != nil {
			return nil, e
		}
		listener = &r4AddressProbeListener{Listener: native, accepting: accepting, probe: func() {
			go func() { read <- s.Start() }()
			select {
			case e := <-read:
				read <- e
			case <-time.After(100 * time.Millisecond):
				t.Error("native Addr callback could not reenter public Start while host state was locked")
			}
		}}
		return listener, nil
	}
	require.NoError(t, s.Start())
	stopR2Await(t, accepting)
	listener.enabled.Store(true)
	require.NotEmpty(t, s.Addr())
	select {
	case e := <-read:
		require.ErrorContains(t, e, "already started")
	case <-time.After(time.Second):
		t.Fatal("reentrant Start did not unwind")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, s.Stop(ctx))
}

func (l *r4OwnedHTTPListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &r4HeldHTTPWriteConn{Conn: c, gate: l.writeGate}, nil
}

type r4HeldHTTPWriteConn struct {
	net.Conn
	gate *stopR2Gate
}

func (c *r4HeldHTTPWriteConn) Write(data []byte) (int, error) {
	c.gate.block()
	return c.Conn.Write(data)
}
func (c *r4HeldHTTPWriteConn) Close() error { err := c.Conn.Close(); c.gate.open(); return err }
func (l *r4OwnedHTTPListener) Close() error {
	err := l.Listener.Close()
	l.once.Do(func() { close(l.closed) })
	return errors.Join(err, l.err)
}

func TestServerStopR4ListenerHandoffOutcome(t *testing.T) {
	s, a := setupTestServerWithConfig(t, func(c *Config) { c.Address = "127.0.0.1"; c.Port = 0; c.MCPEnabled = false })
	gate := newStopR2Gate(t)
	defer gate.open()
	failure := errors.New("r4 HTTP graceful listener close")
	closed := make(chan struct{})
	s.listenNative = func(network, addr string) (net.Listener, error) {
		l, e := net.Listen(network, addr)
		if e != nil {
			return nil, e
		}
		return &r4OwnedHTTPListener{Listener: l, err: failure, closed: closed, writeGate: gate}, nil
	}
	require.NoError(t, s.Start())
	token := getAuthToken(t, a, "admin")
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		req, e := http.NewRequest(http.MethodGet, "http://"+s.Addr()+"/health", nil)
		if e != nil {
			return
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, e := http.DefaultClient.Do(req)
		if e == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}()
	stopR2Await(t, gate.entered)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- s.Stop(ctx) }()
	stopR2Await(t, closed)
	require.NoError(t, ctx.Err(), "listener must close during graceful phase, before the deadline")
	require.ErrorIs(t, <-stopped, context.DeadlineExceeded)
	gate.open()
	stopR2Await(t, requestDone)
	final, end := context.WithTimeout(context.Background(), 2*time.Second)
	defer end()
	require.ErrorIs(t, s.WaitStopped(final), failure, "SDK drain deadline replaced the owned listener's outcome")
}
