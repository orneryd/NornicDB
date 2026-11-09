package bolt

// Author: Codex. Public class controls for the shared native service owner.
import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/stretchr/testify/require"
)

func TestOwnerClassNativeOwnedTLSCompletion(t *testing.T) {
	for _, withServer := range []bool{false, true} {
		t.Run(map[bool]string{false: "constructor", true: "service"}[withServer], func(t *testing.T) {
			before := classNativeRotators()
			cert, key := generateSelfSignedPEM(t, "owned-native-lifetime")
			certPath, keyPath := writeCertKey(t, t.TempDir(), cert, key)
			owner := &lifecycle.WorkLifetime{}
			t.Cleanup(func() { _ = owner.Close() })
			cfg, err := LoadTLSConfigForLifetime(owner, certPath, keyPath, "", ClientAuthNone)
			require.NoError(t, err)
			deadline := time.Now().Add(time.Second)
			for classNativeRotators() < before+1 && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			require.Equal(t, before+1, classNativeRotators(), "owned polling did not start before retirement")
			if withServer {
				config := DefaultConfig()
				config.Lifetime = owner
				config.TLSConfig = cfg
				s := New(config, &mockExecutor{})
				require.NoError(t, s.Close())
			} else {
				require.NoError(t, owner.Close())
			}
			require.Equal(t, before, classNativeRotators(), "actual polling completion was not joined")
		})
	}
}

func TestOwnerClassNativeStartFailureReleasesItsFrame(t *testing.T) {
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer blocker.Close()
	before := classNativeRotators()
	cert, key := generateSelfSignedPEM(t, "native-start-failure")
	certPath, keyPath := writeCertKey(t, t.TempDir(), cert, key)
	owner := &lifecycle.WorkLifetime{}
	t.Cleanup(func() { _ = owner.Close() })
	tlsCfg, err := LoadTLSConfigForLifetime(owner, certPath, keyPath, "", ClientAuthNone)
	require.NoError(t, err)
	cfg := DefaultConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = blocker.Addr().(*net.TCPAddr).Port
	cfg.Lifetime = owner
	cfg.TLSConfig = tlsCfg
	s := New(cfg, &mockExecutor{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = s.Run(ctx)
	require.Error(t, err)
	require.NotErrorIs(t, err, context.DeadlineExceeded, "failed start waited on its own retention")
	join, joinCancel := context.WithTimeout(context.Background(), time.Second)
	defer joinCancel()
	actual := s.WaitStopped(join)
	require.Error(t, actual)
	require.ErrorIs(t, err, actual)
	require.Equal(t, before, classNativeRotators(), "failed startup left its constructed TLS work alive")
}

func TestOwnerClassNativeRunCallerCancellation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Port = 0
	s := New(cfg, &mockExecutor{})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, s.Run(ctx), context.DeadlineExceeded)
	join, joinCancel := context.WithTimeout(context.Background(), time.Second)
	defer joinCancel()
	require.NoError(t, s.WaitStopped(join))
}

type classOutcomeListener struct {
	*classNativeListener
	failure           error
	connectionFailure error
	held, release     chan struct{}
	onClose           func()
	once              sync.Once
	connClosed        chan struct{}
}

func (l *classOutcomeListener) Accept() (net.Conn, error) {
	c, e := l.classNativeListener.Accept()
	if e != nil {
		return nil, e
	}
	return &classOutcomeConn{Conn: c, owner: l}, nil
}
func (l *classOutcomeListener) Close() error {
	var err error
	l.once.Do(func() {
		if l.onClose != nil {
			l.onClose()
		}
		err = errors.Join(l.failure, l.Listener.Close())
	})
	return err
}

type classOutcomeConn struct {
	net.Conn
	owner  *classOutcomeListener
	once   sync.Once
	result error
}

func (c *classOutcomeConn) Close() error {
	c.once.Do(func() {
		err := c.Conn.Close()
		if c.owner.onClose != nil {
			c.owner.onClose()
		}
		close(c.owner.held)
		<-c.owner.release
		c.result = errors.Join(c.owner.connectionFailure, err)
		close(c.owner.connClosed)
	})
	return c.result
}

func TestOwnerClassNativeDeadlineRetainsSiblingOutcomes(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := New(DefaultConfig(), &mockExecutor{})
	a, b := errors.New("native listener close A"), errors.New("native connection close B")
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	l := &classOutcomeListener{classNativeListener: &classNativeListener{Listener: ln, waiting: make(chan struct{})}, failure: a, connectionFailure: b, held: make(chan struct{}), release: gate, connClosed: make(chan struct{}), onClose: func() { _ = s.ConnectionListings() }}
	served := make(chan error, 1)
	go func() { served <- s.Serve(l) }()
	c, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.SetDeadline(time.Now().Add(time.Second)))
	_, err = c.Write([]byte{0x60, 0x60, 0xb0, 0x17, 0, 0, 4, 4, 0, 0, 4, 3, 0, 0, 4, 2, 0, 0, 4, 1})
	require.NoError(t, err)
	var version [4]byte
	_, err = io.ReadFull(c, version[:])
	require.NoError(t, err)
	select {
	case <-l.waiting:
	case <-time.After(time.Second):
		t.Fatal("accepted protocol connection did not reach its native wait")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- s.Shutdown(ctx) }()
	select {
	case <-l.held:
	case <-time.After(time.Second):
		t.Fatal("foreign connection close did not enter")
	}
	select {
	case err := <-stopped:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("Shutdown exceeded its caller deadline")
	}
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer joinCancel()
	require.ErrorIs(t, s.WaitStopped(joinCtx), context.DeadlineExceeded, "caller timeout declared retained close completed")
	release()
	actual := s.WaitStopped(context.Background())
	require.ErrorIs(t, actual, a)
	require.ErrorIs(t, actual, b)
	require.ErrorIs(t, s.Close(), a)
	require.ErrorIs(t, s.Close(), b)
	select {
	case <-served:
	case <-time.After(time.Second):
		t.Fatal("Serve was not joined")
	}
}
