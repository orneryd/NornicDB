package storage

import (
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// The oracle retains order AND multiplicity. Revisions alone are not a set:
// delivery 2,1,1 must disagree with commits 1,2.
type wbEventHistory struct {
	mu     sync.Mutex
	events []string
}

func (h *wbEventHistory) add(event string) {
	h.mu.Lock()
	h.events = append(h.events, event)
	h.mu.Unlock()
}

func (h *wbEventHistory) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.events)
}

func wbHistoryCompare(commits, events []string) error {
	if !slices.Equal(commits, events) {
		return fmt.Errorf("commit/event history mismatch: commits=%v events=%v", commits, events)
	}
	return nil
}

func wbObserveHistory(b *BadgerEngine, crossEntity ...bool) *wbEventHistory {
	h := &wbEventHistory{}
	edgeEvent := func(kind string, e *Edge) string {
		event := fmt.Sprintf("%s:%s:%v", kind, e.ID, e.Properties["rev"])
		if len(crossEntity) > 0 && crossEntity[0] {
			event += fmt.Sprintf(":%s>%s:suppressed=%v", e.StartNode, e.EndNode, e.VisibilitySuppressed)
		}
		return event
	}
	b.OnNodeCreated(func(n *Node) { h.add(fmt.Sprintf("create:%s:%v", n.ID, n.Properties["rev"])) })
	b.OnNodeUpdated(func(n *Node) { h.add(fmt.Sprintf("update:%s:%v", n.ID, n.Properties["rev"])) })
	b.OnNodeDeleted(func(id NodeID) { h.add("delete:" + string(id)) })
	b.OnEdgeCreated(func(e *Edge) { h.add(edgeEvent("edge-create", e)) })
	b.OnEdgeUpdated(func(e *Edge) { h.add(edgeEvent("edge-update", e)) })
	b.OnEdgeDeleted(func(id EdgeID) { h.add("edge-delete:" + string(id)) })
	b.OnNamespaceReset(func(prefix string) { h.add("reset:" + prefix) })
	return h
}

// Pause the first physical commit at the existing tail hook, then let a
// related public writer run. Correct ordering may exclude that writer until
// reservation; both branches must produce the exact same commit history.
func TestDispatcherEveryProducerReservesInCommitOrder(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, producer := range []string{"direct", "explicit", "manual-node", "manual-edge", "decay-node", "decay-edge"} {
			t.Run(fmt.Sprintf("buffered=%v/%s", buffered, producer), func(t *testing.T) {
				b := wbListenerEngine(t, buffered)
				defer b.Close()
				for _, id := range []NodeID{"test:a", "test:b"} {
					_, err := b.CreateNode(&Node{ID: id, Properties: map[string]any{"rev": int64(0)}})
					require.NoError(t, err)
				}
				require.NoError(t, b.CreateEdge(&Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: map[string]any{"rev": int64(0)}}))
				b.WaitForListeners()
				h := wbObserveHistory(b)
				if producer == "decay-node" || producer == "decay-edge" {
					// Clearing a persisted suppressed body is a real helper UPDATE.
					b.decayEnabled = true
					b.revealAll.Store(true)
					if producer == "decay-node" {
						n, err := b.GetNode("test:a")
						require.NoError(t, err)
						n.VisibilitySuppressed = true
						require.NoError(t, b.UpdateNode(n))
					} else {
						e, err := b.GetEdge("test:e")
						require.NoError(t, err)
						e.VisibilitySuppressed = true
						require.NoError(t, b.UpdateEdge(e))
					}
					b.WaitForListeners()
					h.mu.Lock()
					h.events = nil
					h.mu.Unlock()
				}
				entered, resume := make(chan struct{}), make(chan struct{})
				prior, _ := b.listenerFrontier()
				var first atomic.Bool
				var reserved atomic.Bool
				restore := setCommitTailHook(func() {
					if first.CompareAndSwap(false, true) {
						current, _ := b.listenerFrontier()
						reserved.Store(current != prior)
						close(entered)
						<-resume
					}
				})
				defer restore()
				var once sync.Once
				unblock := func() { once.Do(func() { close(resume) }) }
				defer unblock()
				a := make(chan error, 1)
				go func() {
					switch producer {
					case "manual-node":
						a <- b.SetNodeManuallySuppressed("test:a", true)
					case "manual-edge":
						a <- b.SetEdgeManuallySuppressed("test:e", true)
					case "decay-node":
						_, err := b.EnqueueDeindexIfSuppressed("test:a", false)
						a <- err
					case "decay-edge":
						_, err := b.EnqueueDeindexIfSuppressed("test:e", true)
						a <- err
					case "explicit":
						a <- wbListenerTx(b, false, func(tx *BadgerTransaction) error {
							return tx.UpdateNode(&Node{ID: "test:a", Properties: map[string]any{"rev": int64(1)}})
						})
					default:
						a <- b.UpdateNode(&Node{ID: "test:a", Properties: map[string]any{"rev": int64(1)}})
					}
				}()
				select {
				case <-entered:
				case <-time.After(3 * time.Second):
					t.Fatal("producer did not physically commit")
				}
				peer := make(chan error, 1)
				edge := producer == "manual-edge" || producer == "decay-edge"
				helper := producer != "direct" && producer != "explicit"
				go func() {
					if helper {
						if edge {
							peer <- b.DeleteEdge("test:e")
						} else {
							peer <- b.DeleteNode("test:a")
						}
					} else {
						peer <- b.UpdateNode(&Node{ID: "test:a", Properties: map[string]any{"rev": int64(2)}})
					}
				}()
				peerDone := false
				select {
				case err := <-peer:
					require.NoError(t, err)
					peerDone = true
				case <-time.After(150 * time.Millisecond):
				}
				unblock()
				require.NoError(t, <-a)
				if !peerDone {
					require.NoError(t, <-peer)
				}
				b.WaitForListeners()
				require.True(t, reserved.Load(), "the physical commit must reserve before its post-commit tail")
				want := []string{"update:test:a:1", "update:test:a:2"}
				if helper {
					if edge {
						want = []string{"edge-update:test:e:0", "edge-delete:test:e"}
					} else {
						want = []string{"update:test:a:0", "edge-delete:test:e", "delete:test:a"}
					}
				}
				require.NoError(t, wbHistoryCompare(want, h.snapshot()))
			})
		}
	}
}

// Plant the precise defect: persist revision 1, let public revision 2 reserve,
// and only then reserve revision 1 twice. The normal control must reject it;
// the opt-in mutant asks the model to accept it and must fail in the lab.
func TestWriteBehindHistoryNegativeControl(t *testing.T) {
	b := wbListenerEngine(t, false)
	defer b.Close()
	_, err := b.CreateNode(&Node{ID: "test:mutant", Properties: map[string]any{"rev": int64(0)}})
	require.NoError(t, err)
	h := wbObserveHistory(b)
	old := &Node{ID: "test:mutant", Properties: map[string]any{"rev": int64(1)}}
	require.NoError(t, b.withUpdate(singleNodeCacheWrites(old.ID), func(tx *badger.Txn) error {
		data, _, err := b.encodeNodeInTxn(tx, "test", old)
		if err != nil {
			return err
		}
		return tx.Set(nodeKey(old.ID), data)
	}))
	require.NoError(t, b.UpdateNode(&Node{ID: old.ID, Properties: map[string]any{"rev": int64(2)}}))
	b.notifyNodeUpdated(old)
	b.notifyNodeUpdated(old)
	b.WaitForListeners()
	err = wbHistoryCompare([]string{"update:test:mutant:1", "update:test:mutant:2"}, h.snapshot())
	if os.Getenv("NORNIC_WB_HISTORY_MUTANT") == "1" {
		require.NoError(t, err)
		return
	}
	require.ErrorContains(t, err, "commit/event history mismatch")
	require.Equal(t, []string{"update:test:mutant:2", "update:test:mutant:1", "update:test:mutant:1"}, h.snapshot())
}
