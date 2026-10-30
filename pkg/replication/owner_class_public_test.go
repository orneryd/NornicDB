package replication

// Author: Codex. Public owner-class probe, identical on release and head.

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

type r4RetirementTransport struct {
	*MockTransport
	entered   chan struct{}
	stopped   chan struct{}
	enterOnce sync.Once
	stopOnce  sync.Once
	failure   error
}

func (tr *r4RetirementTransport) Listen(ctx context.Context, addr string, handler ConnectionHandler) error {
	tr.enterOnce.Do(func() { close(tr.entered) })
	return tr.MockTransport.Listen(ctx, addr, handler)
}
func (tr *r4RetirementTransport) Close() error {
	tr.stopOnce.Do(func() { close(tr.stopped) })
	return errors.Join(tr.failure, tr.MockTransport.Close())
}

// The only escape closes the transport after recording that Shutdown failed to
// signal it. Production entry, constructor, Start and Shutdown remain public.
func TestR4ReplicationRetirementPublic(t *testing.T) {
	for _, mode := range []ReplicationMode{ModeRaft, ModeHAStandby} {
		t.Run(string(mode), func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Mode = mode
			cfg.NodeID = "r4-retirement"
			cfg.AdvertiseAddr = "127.0.0.1:17000"
			cfg.Raft.Bootstrap = true
			cfg.HAStandby.Role = "standby"
			cfg.HAStandby.AutoFailover = false
			cfg.HAStandby.PeerAddr = "127.0.0.1:17001"
			owner, err := NewReplicator(cfg, NewMockStorage())
			require.NoError(t, err)
			failure := errors.New("r4 transport close outcome")
			tr := &r4RetirementTransport{MockTransport: NewMockTransport(), entered: make(chan struct{}), stopped: make(chan struct{}), failure: failure}
			owner.(interface{ SetTransport(Transport) }).SetTransport(tr)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			require.NoError(t, owner.Start(ctx))
			select {
			case <-tr.entered:
			case <-time.After(time.Second):
				t.Fatal("public Start did not enter transport Listen")
			}
			result := make(chan error, 1)
			go func() { result <- owner.Shutdown() }()
			select {
			case <-tr.stopped:
			case <-time.After(250 * time.Millisecond):
				t.Error("Shutdown joined the transport listener before signaling Close")
				require.NoError(t, tr.MockTransport.Close()) // controlled escape, not the owner under test
			}
			select {
			case err := <-result:
				require.ErrorIs(t, err, failure, "public Shutdown discarded the transport outcome")
			case <-time.After(time.Second):
				t.Fatal("Shutdown did not complete after transport escape")
			}
		})
	}
}
