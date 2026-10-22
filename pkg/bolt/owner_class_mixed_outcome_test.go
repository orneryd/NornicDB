package bolt

// Author: Codex. A known close status must not hide a sibling cleanup failure.
import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type classMixedListener struct {
	net.Listener
	entered              chan struct{}
	enterOnce, closeOnce sync.Once
	failure, errorResult error
}

func (l *classMixedListener) Accept() (net.Conn, error) {
	l.enterOnce.Do(func() { close(l.entered) })
	return l.Listener.Accept()
}
func (l *classMixedListener) Close() error {
	l.closeOnce.Do(func() { l.errorResult = errors.Join(net.ErrClosed, l.failure, l.Listener.Close()) })
	return l.errorResult
}
func TestOwnerClassNativeMixedCloseOutcomes(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	failure := errors.New("native close sibling must survive known status")
	l := &classMixedListener{Listener: ln, entered: make(chan struct{}), failure: failure}
	s := New(DefaultConfig(), &mockExecutor{})
	served := make(chan error, 1)
	go func() { served <- s.Serve(l) }()
	select {
	case <-l.entered:
	case <-time.After(time.Second):
		t.Fatal("real Serve did not enter listener admission")
	}
	err = s.Close()
	require.ErrorIs(t, err, failure, "benign close status hid a real sibling outcome")
	select {
	case <-served:
	case <-time.After(time.Second):
		t.Fatal("Serve did not retire")
	}
}
