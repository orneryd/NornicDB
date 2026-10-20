package storage

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestNamespaceRetiringWALWritesNeverEnterJournal(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	base := NewMemoryEngine()
	wal, err := NewWAL(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)
	engine := NewWALEngine(base, wal)
	defer engine.Close()
	_, err = engine.CreateNode(&Node{ID: "wal_fence_a:seed"})
	require.NoError(t, err)
	entered, resume := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-resume:
		default:
			close(resume)
		}
	}()
	op := NewNamespaceDrop(func() error { close(entered); <-resume; return nil })
	result := make(chan error, 1)
	go func() {
		_, _, err := engine.DeleteByPrefixContext(WithNamespaceDrop(context.Background(), op), "wal_fence_a:")
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("DROP did not reach committed completion")
	}
	writes := []struct {
		name string
		run  func() error
	}{
		{"create-node", func() error { _, err := engine.CreateNode(&Node{ID: "wal_fence_a:ghost"}); return err }},
		{"bulk-create-node", func() error { return engine.BulkCreateNodes([]*Node{{ID: "wal_fence_a:bulk_ghost"}}) }},
		{"update-node", func() error { return engine.UpdateNode(&Node{ID: "wal_fence_a:seed"}) }},
		{"delete-node", func() error { return engine.DeleteNode("wal_fence_a:seed") }},
		{"embedding-sidecar", func() error { return engine.UpdateNodeEmbeddingSidecar(&Node{ID: "wal_fence_a:seed"}) }},
		{"create-edge", func() error {
			return engine.CreateEdge(&Edge{ID: "wal_fence_a:edge", StartNode: "wal_fence_a:seed", EndNode: "wal_fence_a:seed", Type: "FENCE"})
		}},
		{"update-edge", func() error {
			return engine.UpdateEdge(&Edge{ID: "wal_fence_a:edge", StartNode: "wal_fence_a:seed", EndNode: "wal_fence_a:seed", Type: "FENCE"})
		}},
		{"delete-edge", func() error { return engine.DeleteEdge("wal_fence_a:edge") }},
		{"bulk-create-edge", func() error {
			return engine.BulkCreateEdges([]*Edge{{ID: "wal_fence_a:bulk_edge", StartNode: "wal_fence_a:seed", EndNode: "wal_fence_a:seed", Type: "FENCE"}})
		}},
		{"bulk-delete-node", func() error { return engine.BulkDeleteNodes([]NodeID{"wal_fence_a:seed"}) }},
		{"bulk-delete-edge", func() error { return engine.BulkDeleteEdges([]EdgeID{"wal_fence_a:edge"}) }},
	}
	for _, write := range writes {
		t.Run(write.name, func(t *testing.T) {
			before := wal.Sequence()
			require.ErrorIs(t, write.run(), ErrConflict)
			require.Equal(t, before, wal.Sequence(), "a failed retiring-namespace mutation must never be replayed after DROP")
		})
	}
	_, err = engine.CreateNode(&Node{ID: "wal_fence_b:peer"})
	require.NoError(t, err)
	close(resume)
	require.NoError(t, <-result)
}

type namespaceJournalApplyGate struct {
	*MemoryEngine
	entered, resume chan struct{}
}

func (g *namespaceJournalApplyGate) CreateNode(node *Node) (NodeID, error) {
	if node.ID == "wal_frontier_a:old" {
		close(g.entered)
		<-g.resume
	}
	return g.MemoryEngine.CreateNode(node)
}

func TestNamespaceWALPreparationHasTargetProgressBoundary(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, cancelPreparation := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%v", cancelPreparation), func(t *testing.T) {
			base := &namespaceJournalApplyGate{MemoryEngine: NewMemoryEngine(), entered: make(chan struct{}), resume: make(chan struct{})}
			wal, err := NewWAL(t.TempDir(), &WALConfig{SyncMode: "none"})
			require.NoError(t, err)
			engine := NewWALEngine(base, wal)
			defer engine.Close()
			var resumeOnce sync.Once
			resume := func() { resumeOnce.Do(func() { close(base.resume) }) }
			defer resume()
			writer := make(chan error, 1)
			go func() { _, err := engine.CreateNode(&Node{ID: "wal_frontier_a:old"}); writer <- err }()
			select {
			case <-base.entered:
			case <-time.After(time.Second):
				t.Fatal("native WAL writer did not enter its admitted publication")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			dropped := make(chan error, 1)
			started := time.Now()
			go func() { _, _, err := engine.DeleteByPrefixContext(ctx, "wal_frontier_a:"); dropped <- err }()
			select {
			case err := <-dropped:
				t.Fatalf("DROP did not settle its admitted writer: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			stop := make(chan struct{})
			var stopOnce sync.Once
			stopWrites := func() { stopOnce.Do(func() { close(stop) }) }
			defer stopWrites()
			peerDone := make(chan error, 1)
			peerReady := make(chan struct{})
			var acknowledged atomic.Int64
			go func() {
				for i := 0; ; i++ {
					select {
					case <-stop:
						peerDone <- nil
						return
					default:
					}
					_, err := engine.CreateNode(&Node{ID: NodeID(fmt.Sprintf("wal_frontier_b:%d", i))})
					if err != nil {
						peerDone <- err
						return
					}
					acknowledged.Add(1)
					if i == 0 {
						close(peerReady)
					}
				}
			}()
			select {
			case <-peerReady:
			case err := <-peerDone:
				t.Fatalf("peer writer failed: %v", err)
			case <-time.After(time.Second):
				t.Fatal("peer writer blocked behind another namespace's preparation")
			}
			if cancelPreparation {
				cancel()
				select {
				case err := <-dropped:
					require.ErrorIs(t, err, context.Canceled)
				case <-time.After(time.Second):
					t.Fatal("preparation ignored cancellation while settling a writer")
				}
				resume()
				require.NoError(t, <-writer)
				_, err = base.GetNode("wal_frontier_a:old")
				require.NoError(t, err, "aborted preparation changes no acknowledged data")
			} else {
				resume()
				require.NoError(t, <-writer)
				select {
				case err := <-dropped:
					require.NoError(t, err)
				case <-time.After(2 * time.Second):
					t.Fatal("DROP waited for unrelated journal writers")
				}
				t.Logf("DROP with continuous native-WAL peer writes: %s; peer ACKs=%d; writers-stopped=false", time.Since(started), acknowledged.Load())
				_, err = base.GetNode("wal_frontier_a:old")
				require.ErrorIs(t, err, ErrNotFound)
			}
			stopWrites()
			require.NoError(t, <-peerDone)
			count, err := NewNamespacedEngine(engine, "wal_frontier_b").NodeCount()
			require.NoError(t, err)
			require.Equal(t, acknowledged.Load(), count)
		})
	}
}
