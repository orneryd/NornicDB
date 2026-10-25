package storage

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// Avoid an accidental stripe collision hiding primary-entity-only ordering.
func wbCrossIDs(prefix string) (NodeID, NodeID, NodeID, EdgeID) {
	var used orderStripeSet
	var ids []string
	for i := 0; len(ids) < 4; i++ {
		id := fmt.Sprintf("cross:%s-%d", prefix, i)
		var s orderStripeSet
		s.add(id)
		collision := false
		for j := range s {
			collision = collision || used[j]&s[j] != 0
		}
		if !collision {
			ids = append(ids, id)
			for j := range s {
				used[j] |= s[j]
			}
		}
	}
	return NodeID(ids[0]), NodeID(ids[1]), NodeID(ids[2]), EdgeID(ids[3])
}

func wbAssertOrdered(t *testing.T, b *BadgerEngine, ids ...string) {
	t.Helper()
	for _, id := range ids {
		var stripes orderStripeSet
		stripes.add(id)
		for word, mask := range stripes {
			for bit := 0; bit < 64; bit++ {
				if mask&(uint64(1)<<bit) != 0 {
					lock := &b.entityOrder[word*64+bit]
					if lock.TryLock() {
						lock.Unlock()
						t.Errorf("affected entity %s is unordered before reservation", id)
					}
				}
			}
		}
	}
}

func wbCrossSchedule(t *testing.T, buffered bool, producer string, seed int64) {
	t.Helper()
	b := wbListenerEngine(t, buffered)
	defer b.Close()
	a, z, c, id := wbCrossIDs(fmt.Sprintf("%d", seed))
	for _, n := range []NodeID{a, z, c} {
		_, err := b.CreateNode(&Node{ID: n, Properties: map[string]any{"rev": int64(0)}})
		require.NoError(t, err)
	}
	e := &Edge{ID: id, StartNode: a, EndNode: z, Type: "R", Properties: map[string]any{"rev": int64(0)}}
	if seed%2 != 0 {
		e.StartNode, e.EndNode = e.EndNode, e.StartNode
	}
	require.NoError(t, b.CreateEdge(e))
	if producer == "decay" {
		b.decayEnabled = true
		b.revealAll.Store(true)
		e.VisibilitySuppressed = true
		require.NoError(t, b.UpdateEdge(e))
	}
	b.WaitForListeners()
	h := wbObserveHistory(b, true)
	entered, resume := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	defer unblock()
	b.beforeCommitReservation = func(ops []Operation) {
		for _, op := range ops {
			if op.Type == OpUpdateEdge && op.Edge != nil && op.Edge.ID == id && first.CompareAndSwap(false, true) {
				close(entered)
				<-resume
			}
		}
	}
	done := make(chan error, 1)
	go func() {
		switch producer {
		case "manual":
			done <- b.SetEdgeManuallySuppressed(id, true)
		case "decay":
			_, err := b.EnqueueDeindexIfSuppressed(string(id), true)
			done <- err
		default:
			next := copyEdge(e)
			next.Properties["rev"] = int64(1)
			if producer == "move" || producer == "direct-move" {
				if next.StartNode == a {
					next.StartNode = c
				} else {
					next.EndNode = c
				}
			}
			if producer == "direct" || producer == "direct-move" {
				done <- b.UpdateEdge(next)
			} else {
				done <- wbListenerTx(b, producer == "implicit", func(tx *BadgerTransaction) error { return tx.UpdateEdge(next) })
			}
		}
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("producer did not reach reservation")
	}
	wbAssertOrdered(t, b, string(a), string(z), string(id))
	if producer == "move" || producer == "direct-move" {
		wbAssertOrdered(t, b, string(c))
	}
	peer := make(chan error, 1)
	go func() {
		if producer == "move" {
			peer <- wbListenerTx(b, false, func(tx *BadgerTransaction) error { return tx.DeleteNode(a) })
		} else {
			peer <- b.DeleteNode(a)
		}
	}()
	finished := false
	select {
	case err := <-peer:
		require.NoError(t, err)
		finished = true
	case <-time.After(75 * time.Millisecond):
	}
	unblock()
	require.NoError(t, <-done)
	if !finished {
		require.NoError(t, <-peer)
	}
	b.WaitForListeners()
	rev := int64(1)
	if producer == "manual" || producer == "decay" {
		rev = 0
	}
	start, end := e.StartNode, e.EndNode
	if producer == "move" || producer == "direct-move" {
		if start == a {
			start = c
		} else {
			end = c
		}
	}
	want := []string{fmt.Sprintf("edge-update:%s:%d:%s>%s:suppressed=false", id, rev, start, end)}
	if producer != "move" && producer != "direct-move" {
		want = append(want, "edge-delete:"+string(id))
	}
	want = append(want, "delete:"+string(a))
	require.NoError(t, wbHistoryCompare(want, h.snapshot()))
	require.False(t, finished, "related producer crossed the physical commit/reservation boundary")
	if producer == "move" || producer == "direct-move" {
		stored, err := b.GetStoredEdge(id)
		require.NoError(t, err)
		require.True(t, stored.StartNode == c || stored.EndNode == c)
	} else {
		_, err := b.GetStoredEdge(id)
		require.ErrorIs(t, err, ErrNotFound)
	}
}

func TestDispatcherCascadeHelperOrder(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, producer := range []string{"manual", "decay"} {
			t.Run(fmt.Sprintf("buffered=%v/%s", buffered, producer), func(t *testing.T) { wbCrossSchedule(t, buffered, producer, 1) })
		}
	}
}

// NOR-169: move off a while a second transaction detach-deletes a. The move
// must retain the old endpoint's order even after its physical adjacency moved.
func TestDispatcherNOR169MoveDetach(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprintf("buffered=%v", buffered), func(t *testing.T) { wbCrossSchedule(t, buffered, "move", 169) })
	}
}

func TestDispatcherCascadeAndRelabelAffectedSet(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, route := range []string{"direct", "explicit", "implicit"} {
			for _, relabel := range []bool{false, true} {
				t.Run(fmt.Sprintf("buffered=%v/%s/relabel=%v", buffered, route, relabel), func(t *testing.T) {
					b := wbListenerEngine(t, buffered)
					defer b.Close()
					a, z, c, id := wbCrossIDs("multi")
					for _, n := range []NodeID{a, z, c} {
						_, err := b.CreateNode(&Node{ID: n, Labels: []string{"Old"}})
						require.NoError(t, err)
					}
					other, loop := EdgeID(string(id)+"-incoming"), EdgeID(string(id)+"-loop")
					for _, e := range []*Edge{{ID: id, StartNode: a, EndNode: z, Type: "R"}, {ID: other, StartNode: c, EndNode: a, Type: "R"}, {ID: loop, StartNode: a, EndNode: a, Type: "R"}} {
						require.NoError(t, b.CreateEdge(e))
					}
					require.NoError(t, b.SetEdgeManuallySuppressed(other, true))
					b.WaitForListeners()
					entered, resume := make(chan struct{}), make(chan struct{})
					var once sync.Once
					unblock := func() { once.Do(func() { close(resume) }) }
					defer unblock()
					b.beforeCommitReservation = func(ops []Operation) {
						for _, op := range ops {
							if (op.Type == OpDeleteNode && op.NodeID == a) || (relabel && op.Type == OpUpdateNode && op.Node != nil && op.Node.ID == a) {
								close(entered)
								<-resume
							}
						}
					}
					done := make(chan error, 1)
					go func() {
						next := &Node{ID: a, Labels: []string{"New"}}
						if route == "direct" {
							if relabel {
								done <- b.UpdateNode(next)
							} else {
								done <- b.DeleteNode(a)
							}
						} else {
							done <- wbListenerTx(b, route == "implicit", func(tx *BadgerTransaction) error {
								if relabel {
									return tx.UpdateNode(next)
								}
								return tx.DeleteNode(a)
							})
						}
					}()
					select {
					case <-entered:
					case <-time.After(3 * time.Second):
						t.Fatal("node producer did not reach reservation")
					}
					wbAssertOrdered(t, b, string(a), string(z), string(c), string(id), string(other), string(loop))
					unblock()
					require.NoError(t, <-done)
					require.NoError(t, b.LandWriteBehind())
					b.WaitForListeners()
					for _, eid := range []EdgeID{id, other, loop} {
						_, err := b.GetStoredEdge(eid)
						if relabel {
							require.NoError(t, err)
						} else {
							require.ErrorIs(t, err, ErrNotFound)
						}
					}
				})
			}
		}
	}
}

// Generate the cross product of producers, endpoint orientation/IDs and
// buffering; shuffle each seed so omissions cannot be hidden by random choice.
func TestWriteBehindModelCrossEntityHistory(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, seed := range wbModelSeeds(t) {
			t.Run(fmt.Sprintf("buffered=%v/seed=%d", buffered, seed), func(t *testing.T) {
				producers := []string{"manual", "decay", "direct", "explicit", "implicit", "move", "direct-move"}
				rng := rand.New(rand.NewSource(seed))
				rng.Shuffle(len(producers), func(i, j int) { producers[i], producers[j] = producers[j], producers[i] })
				for _, producer := range producers {
					t.Run(producer, func(t *testing.T) { wbCrossSchedule(t, buffered, producer, seed) })
					t.Run(producer+"/delete-first", func(t *testing.T) { wbCrossDeleteFirst(t, buffered, producer, seed) })
				}
			})
		}
	}
}

// In the opposite schedule the relationship is already physically absent, so
// a helper cannot discover its old endpoints. The cascade must still hold e.
func wbCrossDeleteFirst(t *testing.T, buffered bool, producer string, seed int64) {
	t.Helper()
	b := wbListenerEngine(t, buffered)
	defer b.Close()
	a, z, c, id := wbCrossIDs(fmt.Sprintf("delete-%d", seed))
	for _, n := range []NodeID{a, z, c} {
		_, err := b.CreateNode(&Node{ID: n})
		require.NoError(t, err)
	}
	e := &Edge{ID: id, StartNode: a, EndNode: z, Type: "R", Properties: map[string]any{"rev": int64(0)}}
	if seed%2 != 0 {
		e.StartNode, e.EndNode = e.EndNode, e.StartNode
	}
	require.NoError(t, b.CreateEdge(e))
	b.decayEnabled = true
	b.revealAll.Store(true)
	b.WaitForListeners()
	h := wbObserveHistory(b, true)
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	defer unblock()
	b.beforeCommitReservation = func(ops []Operation) {
		for _, op := range ops {
			if op.Type == OpDeleteNode && op.NodeID == a {
				close(entered)
				<-resume
			}
		}
	}
	done := make(chan error, 1)
	go func() { done <- b.DeleteNode(a) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("cascade did not reach reservation")
	}
	peer := make(chan error, 1)
	go func() {
		switch producer {
		case "manual":
			peer <- b.SetEdgeManuallySuppressed(id, true)
		case "decay":
			_, err := b.EnqueueDeindexIfSuppressed(string(id), true)
			peer <- err
		default:
			next := copyEdge(e)
			next.Properties["rev"] = int64(1)
			if producer == "move" || producer == "direct-move" {
				if next.StartNode == a {
					next.StartNode = c
				} else {
					next.EndNode = c
				}
			}
			if producer == "direct" || producer == "direct-move" {
				peer <- b.UpdateEdge(next)
			} else {
				peer <- wbListenerTx(b, producer == "implicit", func(tx *BadgerTransaction) error { return tx.UpdateEdge(next) })
			}
		}
	}()
	finished := false
	var peerErr error
	select {
	case peerErr = <-peer:
		finished = true
	case <-time.After(75 * time.Millisecond):
	}
	unblock()
	require.NoError(t, <-done)
	if !finished {
		peerErr = <-peer
	}
	if producer == "decay" {
		require.NoError(t, peerErr)
	} else {
		require.Error(t, peerErr)
	}
	require.NoError(t, b.LandWriteBehind())
	b.WaitForListeners()
	require.NoError(t, wbHistoryCompare([]string{"edge-delete:" + string(id), "delete:" + string(a)}, h.snapshot()))
	// Transaction staging can observe the missing body and reject before a
	// commit is attempted. Direct helpers enter the order before reading it.
	if producer == "manual" || producer == "decay" || producer == "direct" || producer == "direct-move" {
		require.False(t, finished, "cascade failed to exclude a relationship writer")
	}
}

// The same exact-sequence oracle rejects a real primary-only producer. This
// deliberately bypasses the public helper's affected set, committing its
// suppression change while holding only e, then pausing before reservation.
func TestWriteBehindCrossEntityNegativeControl(t *testing.T) {
	b := wbListenerEngine(t, false)
	defer b.Close()
	a, z, _, id := wbCrossIDs("mutant")
	for _, n := range []NodeID{a, z} {
		_, err := b.CreateNode(&Node{ID: n})
		require.NoError(t, err)
	}
	require.NoError(t, b.CreateEdge(&Edge{ID: id, StartNode: a, EndNode: z, Type: "R", Properties: map[string]any{"rev": int64(0)}}))
	b.WaitForListeners()
	h := wbObserveHistory(b, true)
	var stripes orderStripeSet
	stripes.add(string(id))
	b.entityOrder.lock(&stripes, true)
	defer b.entityOrder.unlock(&stripes, true)
	var edge *Edge
	require.NoError(t, b.setManualSuppression(manualSuppressionEdge, string(id), edgeKey(id), true, nil))
	// Snapshot is the committed producer image, independent of later deletion.
	var err error
	edge, err = b.GetStoredEdge(id)
	require.NoError(t, err)
	// The mutant cascade is also primary-only, as on the reviewed head. Run
	// its real physical operation without calling the corrected public owner.
	peer := make(chan error, 1)
	go func() {
		var primary orderStripeSet
		primary.add(string(a))
		b.entityOrder.lock(&primary, true)
		defer b.entityOrder.unlock(&primary, true)
		var deleted []*Edge
		err := b.withUpdate(nodeDeleteCacheWrites(a, &deleted), func(txn *badger.Txn) error {
			_, _, edges, _, err := b.deleteNodeInTxn(txn, a)
			deleted = edges
			return err
		})
		if err == nil {
			b.completeCommitNotification(b.reserveCommitOperations([]Operation{{Type: OpDeleteNode, NodeID: a, DeletedEdgeIDs: []EdgeID{id}}}))
		}
		peer <- err
	}()
	require.NoError(t, <-peer)
	b.completeCommitNotification(b.reserveCommitOperations([]Operation{{Type: OpUpdateEdge, Edge: edge}}))
	b.WaitForListeners()
	update := fmt.Sprintf("edge-update:%s:0:%s>%s:suppressed=false", id, a, z)
	want := []string{update, "edge-delete:" + string(id), "delete:" + string(a)}
	err = wbHistoryCompare(want, h.snapshot())
	require.Equal(t, []string{"edge-delete:" + string(id), "delete:" + string(a), update}, h.snapshot())
	if os.Getenv("NORNIC_WB_CROSS_ENTITY_MUTANT") == "1" {
		require.NoError(t, err)
	} else {
		require.ErrorContains(t, err, "commit/event history mismatch")
	}
}
