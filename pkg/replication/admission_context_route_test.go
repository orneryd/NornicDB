package replication

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Author: #1667 implementation lane (Codex). A transparent public unwrap seam
// pauses actual native frame discovery, so cancellation remains observable even
// when a legitimate timeout/lifetime context is derived from the caller.
type admissionFrameBarrier struct {
	armed   atomic.Bool
	once    sync.Once
	entered chan struct{}
	resume  chan struct{}
}

type admissionFrameWrapper struct {
	storage.Engine
	barrier *admissionFrameBarrier
}

type admissionEncodingContext struct {
	context.Context
	cancel context.CancelFunc
	hits   int
}

func (c *admissionEncodingContext) Err() error {
	var pcs [16]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Function, "/pkg/replication.") || strings.Contains(frame.Function, "/pkg/storage.") {
			if strings.HasPrefix(frame.Function, "github.com/orneryd/nornicdb/pkg/replication.encodeBulkEntityPayloadWithContext[") {
				c.hits++
				if c.hits == 2 {
					c.cancel()
				}
			}
			break
		}
		if !more {
			break
		}
	}
	return c.Context.Err()
}

func (w *admissionFrameWrapper) GetInnerEngine() storage.Engine {
	if w.barrier.armed.Load() {
		var pcs [16]uintptr
		n := runtime.Callers(2, pcs[:])
		frames := runtime.CallersFrames(pcs[:n])
		for {
			frame, more := frames.Next()
			if strings.Contains(frame.Function, "/pkg/storage.") {
				if strings.HasSuffix(frame.Function, ".BeginEntityTransactionWithContext") {
					w.barrier.once.Do(func() { close(w.barrier.entered); <-w.barrier.resume })
				}
				break
			}
			if !more {
				break
			}
		}
	}
	return w.Engine
}

func admissionReplicatorFixture(t *testing.T, mode ReplicationMode, lifetime context.Context) (*storage.BadgerEngine, *StorageAdapter, Replicator, *admissionFrameBarrier) {
	t.Helper()
	b, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	barrier := &admissionFrameBarrier{entered: make(chan struct{}), resume: make(chan struct{})}
	var wrapped storage.Engine = b
	for i := 0; i < 20; i++ {
		wrapped = &admissionFrameWrapper{Engine: wrapped, barrier: barrier}
	}
	adapter, err := NewStorageAdapterWithWAL(wrapped, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, adapter.Close()) })
	cfg := DefaultConfig()
	cfg.Mode, cfg.NodeID = mode, "admission-context-"+string(mode)
	cfg.BindAddr, cfg.AdvertiseAddr = "127.0.0.1:0", "127.0.0.1:0"
	cfg.ReplicationSecret, cfg.DataDir = strings.Repeat("x", 32), t.TempDir()
	cfg.Raft.Bootstrap = true
	cfg.MultiRegion.RegionID, cfg.MultiRegion.LocalCluster = "admission-local", cfg.Raft
	cfg.HAStandby.Role, cfg.HAStandby.PeerAddr, cfg.HAStandby.SyncMode = "primary", "127.0.0.1:1", SyncAsync
	cfg.HAStandby.ReconnectInterval = 20 * time.Millisecond
	r, err := NewReplicator(cfg, adapter)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Shutdown()) })
	require.NoError(t, r.Start(lifetime))
	return b, adapter, r, barrier
}

func admissionNodeCommand(t *testing.T, bulk bool) *Command {
	t.Helper()
	count := 1
	if bulk {
		count = 20
	}
	encoded := make([][]byte, count)
	for i := range encoded {
		var err error
		encoded[i], err = storage.EncodeNodePayload(&storage.Node{ID: storage.NodeID(fmt.Sprintf("test:forward%d", i)), Labels: []string{"ContextCandidate"}})
		require.NoError(t, err)
	}
	if !bulk {
		return &Command{Type: CmdCreateNode, Data: encoded[0], Timestamp: time.Now()}
	}
	data, err := storage.EncodeCommandPayload(encoded)
	require.NoError(t, err)
	return &Command{Type: CmdBulkCreateNodes, Data: data, Timestamp: time.Now()}
}

func TestReplicationAdmissionContextRoute(t *testing.T) {
	for _, source := range []string{"request", "shutdown"} {
		for _, mode := range []ReplicationMode{ModeStandalone, ModeHAStandby, ModeRaft, ModeMultiRegion} {
			for _, bulk := range []bool{false, true} {
				if source == "shutdown" && !bulk {
					continue
				}
				t.Run(fmt.Sprintf("%s/%s/bulk=%v", source, mode, bulk), func(t *testing.T) {
					parent, cancel := context.WithCancel(context.Background())
					defer cancel()
					b, adapter, r, barrier := admissionReplicatorFixture(t, mode, context.Background())
					before, err := adapter.GetWALPosition()
					require.NoError(t, err)
					command := admissionNodeCommand(t, bulk)
					barrier.armed.Store(true)
					result := make(chan error, 1)
					go func() {
						if source == "request" {
							engine := NewReplicatedEngine(b, r, 3*time.Second)
							count := 1
							if bulk {
								count = 20
							}
							input := make([]*storage.Node, count)
							for i := range input {
								input[i] = &storage.Node{ID: storage.NodeID(fmt.Sprintf("test:forward%d", i)), Labels: []string{"ContextCandidate"}}
							}
							if bulk {
								result <- storage.BulkCreateNodesWithContext(parent, engine, input)
							} else {
								_, err := storage.CreateNodeWithContext(parent, engine, input[0])
								result <- err
							}
							return
						}
						// A no-request command consumes the replicator owner lifetime.
						result <- r.Apply(command, 3*time.Second)
					}()
					select {
					case <-barrier.entered:
					case <-time.After(5 * time.Second):
						close(barrier.resume)
						t.Fatal("public command did not enter native frame preparation")
					}
					var stopped chan error
					if source == "shutdown" {
						stopped = make(chan error, 1)
						go func() { stopped <- r.Shutdown() }()
						if owner, ok := r.(interface{ LifetimeContext() context.Context }); ok {
							select {
							case <-owner.LifetimeContext().Done():
							case <-time.After(5 * time.Second):
								close(barrier.resume)
								t.Fatal("Shutdown did not cancel the retained lifetime")
							}
						} else {
							// Old-product baseline has no accessor: use completion of
							// the actual public Shutdown, never a made-up cancel root.
							select {
							case stopErr := <-stopped:
								require.NoError(t, stopErr)
								stopped = nil
							case <-time.After(5 * time.Second):
								close(barrier.resume)
								t.Fatal("old public Shutdown did not complete")
							}
						}
					} else {
						cancel()
					}
					close(barrier.resume)
					select {
					case err = <-result:
					case <-time.After(5 * time.Second):
						t.Fatal("public apply did not settle after cancellation")
					}
					if stopped != nil {
						select {
						case stopErr := <-stopped:
							require.NoError(t, stopErr)
						case <-time.After(5 * time.Second):
							t.Fatal("Shutdown did not join")
						}
					}
					nodes, readErr := b.GetStoredNodesByLabel("ContextCandidate")
					require.NoError(t, readErr)
					after, readErr := adapter.GetWALPosition()
					require.NoError(t, readErr)
					t.Logf("source=%s mode=%s bulk=%v apply=%v candidates=%d WAL=%d->%d", source, mode, bulk, err, len(nodes), before, after)
					require.ErrorIs(t, err, context.Canceled)
					require.Empty(t, nodes)
					require.Equal(t, before, after, "canceled preparation must not be accepted into replay")
				})
			}
		}
	}
}
