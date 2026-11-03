package qdrantgrpc

import (
	"context"
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type r4OwnedGRPCListener struct {
	net.Listener
	failure  error
	accepted chan struct{}
	once     sync.Once
}

type r4GRPCAddressProbeListener struct {
	net.Listener
	enabled    atomic.Bool
	probe      func()
	accepting  chan struct{}
	acceptOnce sync.Once
}

func (l *r4GRPCAddressProbeListener) Accept() (net.Conn, error) {
	l.acceptOnce.Do(func() { close(l.accepting) })
	return l.Listener.Accept()
}
func (l *r4GRPCAddressProbeListener) Addr() net.Addr {
	if l.enabled.CompareAndSwap(true, false) {
		l.probe()
	}
	return l.Listener.Addr()
}

func TestQdrantR4ListenerAddressReentry(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	defer base.Close()
	s, e := NewServer(cfg, newTestCollectionStore(t), base, nil, nil)
	require.NoError(t, e)
	read := make(chan error, 1)
	accepting := make(chan struct{})
	var listener *r4GRPCAddressProbeListener
	s.listenNative = func(network, addr string) (net.Listener, error) {
		native, e := net.Listen(network, addr)
		if e != nil {
			return nil, e
		}
		listener = &r4GRPCAddressProbeListener{Listener: native, accepting: accepting, probe: func() {
			go func() { read <- s.RegisterAdditionalServices(func(*grpc.Server) {}) }()
			select {
			case e := <-read:
				read <- e
			case <-time.After(100 * time.Millisecond):
				t.Error("native Addr callback could not reenter public registration while host state was locked")
			}
		}}
		return listener, nil
	}
	require.NoError(t, s.Start())
	select {
	case <-accepting:
	case <-time.After(time.Second):
		t.Fatal("native gRPC Accept not reached")
	}
	listener.enabled.Store(true)
	require.NotEmpty(t, s.Addr())
	select {
	case e := <-read:
		require.Error(t, e)
	case <-time.After(time.Second):
		t.Fatal("reentrant registration did not unwind")
	}
	s.Stop()
}

func (l *r4OwnedGRPCListener) Accept() (net.Conn, error) {
	l.once.Do(func() { close(l.accepted) })
	return l.Listener.Accept()
}
func (l *r4OwnedGRPCListener) Close() error { return errors.Join(l.Listener.Close(), l.failure) }

func TestQdrantR4ListenerHandoffOutcome(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	defer base.Close()
	s, e := NewServer(cfg, newTestCollectionStore(t), base, nil, nil)
	require.NoError(t, e)
	failure := errors.New("r4 gRPC graceful listener close")
	accepted := make(chan struct{})
	s.listenNative = func(network, addr string) (net.Listener, error) {
		l, e := net.Listen(network, addr)
		if e != nil {
			return nil, e
		}
		return &r4OwnedGRPCListener{Listener: l, failure: failure, accepted: accepted}, nil
	}
	require.NoError(t, s.Start())
	select {
	case <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("gRPC Serve did not enter native listener")
	}
	s.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var outcome error
	if waiter, ok := any(s).(interface{ WaitStopped(context.Context) error }); ok {
		outcome = waiter.WaitStopped(ctx)
	}
	require.ErrorIs(t, outcome, failure, "gRPC's void stop API lost the owned listener's outcome")
}
