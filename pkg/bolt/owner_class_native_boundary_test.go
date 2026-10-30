package bolt

// Author: Codex. Public native sibling-protocol boundary evidence only.
// No product implementation is changed by this release overlay.
import (
	"errors"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type classNativeListener struct {
	net.Listener
	waiting chan struct{}
}
type classNativeConn struct {
	net.Conn
	wrote   atomic.Bool
	once    sync.Once
	waiting chan struct{}
}

func (l *classNativeListener) Accept() (net.Conn, error) {
	c, e := l.Listener.Accept()
	if e != nil {
		return nil, e
	}
	return &classNativeConn{Conn: c, waiting: l.waiting}, nil
}
func (c *classNativeConn) Read(p []byte) (int, error) {
	if c.wrote.Load() {
		c.once.Do(func() { close(c.waiting) })
	}
	return c.Conn.Read(p)
}
func (c *classNativeConn) Write(p []byte) (int, error) {
	n, e := c.Conn.Write(p)
	if n > 0 {
		c.wrote.Store(true)
	}
	return n, e
}

func TestOwnerClassNativeBoltCloseAcceptedConnection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	observed := &classNativeListener{Listener: ln, waiting: make(chan struct{})}
	s := New(DefaultConfig(), &mockExecutor{})
	defer s.Close()
	served := make(chan error, 1)
	go func() { served <- s.Serve(observed) }()
	c, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.SetDeadline(time.Now().Add(2*time.Second)))
	_, err = c.Write([]byte{0x60, 0x60, 0xb0, 0x17, 0, 0, 4, 4, 0, 0, 4, 3, 0, 0, 4, 2, 0, 0, 4, 1})
	require.NoError(t, err)
	var version [4]byte
	_, err = io.ReadFull(c, version[:])
	require.NoError(t, err)
	select {
	case <-observed.waiting:
	case <-time.After(time.Second):
		t.Fatal("accepted connection did not await its next protocol message")
	}
	require.NoError(t, s.Close())
	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("listener Serve did not finish")
	}
	require.NoError(t, c.SetReadDeadline(time.Now().Add(200*time.Millisecond)))
	var b [1]byte
	_, err = c.Read(b[:])
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		t.Fatal("Close returned while the accepted protocol connection remained open")
	}
	require.Error(t, err, "closed server left an accepted connection usable")
}

func classNativeRotators() int {
	b := make([]byte, 1<<20)
	n := runtime.Stack(b, true)
	return strings.Count(string(b[:n]), "(*certRotator).runRotation(")
}
func TestOwnerClassNativeTLSConstructorHasNoRetirement(t *testing.T) {
	before := classNativeRotators()
	cert, key := generateSelfSignedPEM(t, "lifetime-native-control")
	certPath, keyPath := writeCertKey(t, t.TempDir(), cert, key)
	for range 2 {
		cfg, err := LoadTLSConfig(certPath, keyPath)
		require.NoError(t, err)
		config := DefaultConfig()
		config.TLSConfig = cfg
		s := New(config, &mockExecutor{})
		require.NoError(t, s.Close())
	}
	deadline := time.Now().Add(time.Second)
	for classNativeRotators() < before+2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	// Both constructors have returned, both service owners have closed, and
	// tls.Config exposes no supported cancellation/join/disposal handle.
	require.Equal(t, before, classNativeRotators(), "native TLS construction retained polling workers without a retirement owner")
}
