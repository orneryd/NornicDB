package storage

import (
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A deleting commit already visible before begin, still descheduled in its
// commit tail, gives the writer ordinary not-found semantics.
func TestVanishedWriteTargetVisibleDeletionBeforeBegin(t *testing.T) {
	for _, syncWrites := range []bool{false, true} {
		for _, producer := range []string{"transaction", "direct"} {
			t.Run(fmt.Sprintf("sync=%t/%s", syncWrites, producer), func(t *testing.T) {
				b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour, SyncWrites: syncWrites})
				require.NoError(t, err)
				defer b.Close()
				for _, id := range []NodeID{"test:a", "test:b"} {
					_, err = b.CreateNode(&Node{ID: id})
					require.NoError(t, err)
				}
				require.NoError(t, b.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R"}))
				commit := func() error { return b.DeleteNode("test:a") }
				if producer == "transaction" {
					peer, err := b.BeginTransaction()
					require.NoError(t, err)
					require.NoError(t, peer.DeleteNode("test:a"))
					commit = peer.Commit
				}
				reached, resume := make(chan struct{}), make(chan struct{})
				var once sync.Once
				release := func() { once.Do(func() { close(resume) }) }
				defer release()
				restore := setCommitTailHook(func() {
					close(reached)
					<-resume
				})
				defer restore()
				completed := make(chan error, 1)
				go func() { completed <- commit() }()
				select {
				case <-reached:
				case <-time.After(15 * time.Second):
					t.Fatal("deleting commit did not reach its tail")
				}
				head, err := b.GetNodeCurrentHead("test:a")
				require.NoError(t, err)
				require.True(t, head.Tombstoned)
				_, err = b.getNodeSnapshot("test:a", true)
				require.ErrorIs(t, err, ErrNotFound, "the deletion is visible before the writer begins")
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				require.NoError(t, tx.SetImplicit(true))
				require.NoError(t, tx.SetNamespace("test"))
				release()
				require.NoError(t, <-completed)
				writes := []struct {
					name string
					run  func() error
				}{
					{"node", func() error { return tx.UpdateNode(&Node{ID: "test:a"}) }},
					{"observed", func() error { return tx.UpdateNodeObserved(&Node{ID: "test:a"}, &Node{ID: "test:a"}) }},
					{"relationship", func() error {
						return tx.UpdateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R"})
					}},
					{"endpoint", func() error {
						return tx.CreateEdge(&Edge{ID: "test:new", StartNode: "test:b", EndNode: "test:a", Type: "R"})
					}},
				}
				for _, w := range writes {
					if err := w.run(); !errors.Is(err, ErrNotFound) || errors.Is(err, ErrConflict) {
						t.Errorf("%s: deleted before begin, want not found, got %v", w.name, err)
					}
				}
			})
		}
	}
}

// A deletion that allocated its version, and sampled its commit time, before
// the writer began but published only after BeginTransaction returned is a
// concurrent modification, whatever its timestamp says.
func TestVanishedWriteTargetDeletionAllocatedBeforeBegin(t *testing.T) {
	for _, producer := range []string{"transaction", "direct"} {
		t.Run(producer, func(t *testing.T) {
			b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour})
			require.NoError(t, err)
			defer b.Close()
			state, err := b.namespaceMVCC("test")
			require.NoError(t, err)
			for i := 0; i < 12; i++ {
				id := NodeID(fmt.Sprintf("test:sample%d", i))
				_, err := b.CreateNode(&Node{ID: id})
				require.NoError(t, err)
				commit := func() error { return b.DeleteNode(id) }
				if producer == "transaction" {
					peer, err := b.BeginTransaction()
					require.NoError(t, err)
					require.NoError(t, peer.DeleteNode(id))
					commit = peer.Commit
				}
				before := state.seq.Load()
				b.db.commitGate.Lock()
				var gateOnce sync.Once
				unlock := func() { gateOnce.Do(b.db.commitGate.Unlock) }
				finished := make(chan error, 1)
				go func() { finished <- commit() }()
				deadline := time.Now().Add(10 * time.Second)
				for state.seq.Load() == before && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
				require.Greater(t, state.seq.Load(), before, "the deletion did not allocate")
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetImplicit(true))
				require.NoError(t, tx.SetNamespace("test"))
				unlock()
				require.NoError(t, <-finished)
				err = tx.UpdateNodeObserved(&Node{ID: id}, &Node{ID: id})
				if err == nil {
					err = tx.Commit()
				}
				require.ErrorIs(t, err, ErrConflict, "%s published after BeginTransaction returned", id)
				_ = tx.Rollback()
			}
		})
	}
}

// A transaction pins the write-behind generations it saw at begin: their
// images and tombstones keep answering after flush and retirement, their
// memory leases wait for the last pin, and generations created later are
// never pinned.
func TestBeginOverlayPinsRetiredGenerations(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	defer b.Close()
	_, err = b.CreateNode(&Node{ID: "test:gone"})
	require.NoError(t, err)
	implicitCommit(t, b, func(tx *BadgerTransaction) error {
		_, err := tx.CreateNode(&Node{ID: "test:seed"})
		return err
	})
	implicitCommit(t, b, func(tx *BadgerTransaction) error { return tx.DeleteNode("test:gone") })
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	pinned := tx.overlayAtBegin.active
	implicitCommit(t, b, func(peer *BadgerTransaction) error { return peer.DeleteNode("test:seed") })
	require.NoError(t, b.FlushWriteBehind())
	require.True(t, pinned.queueRetired.Load(), "the generation was flushed and left the queue")
	require.False(t, pinned.leasesReleased.Load(), "a pinned generation keeps its leases")
	b.writeBehind.mu.RLock()
	later := b.writeBehind.active
	b.writeBehind.mu.RUnlock()
	require.NotSame(t, pinned, later)
	require.Zero(t, later.pins.Load(), "generations created after begin are not pinned")
	require.ErrorIs(t, tx.UpdateNode(&Node{ID: "test:seed"}), ErrConflict, "visible at begin, deleted after it")
	require.ErrorIs(t, tx.UpdateNode(&Node{ID: "test:gone"}), ErrNotFound, "deleted before begin")
	require.NoError(t, tx.Rollback())
	require.Zero(t, pinned.pins.Load())
	require.True(t, pinned.leasesReleased.Load(), "the last pin releases a retired generation")
}

// A generated mix of buffered, synchronous transaction and direct engine
// creates and deletes, with transactions beginning and flushes landing in
// between, checked against a reference model: a target deleted while a
// transaction was open conflicts exactly when the model had it live at that
// transaction's begin.
func TestVanishedWriteTargetSnapshotModel(t *testing.T) {
	for _, syncWrites := range []bool{false, true} {
		for seedIndex := 0; seedIndex < 4; seedIndex++ {
			seed := time.Now().UnixNano() + int64(seedIndex)
			t.Run(fmt.Sprintf("sync=%t/seed=%d", syncWrites, seed), func(t *testing.T) {
				rng := rand.New(rand.NewSource(seed))
				b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour, SyncWrites: syncWrites})
				require.NoError(t, err)
				defer b.Close()
				type modelTx struct {
					tx    *BadgerTransaction
					begin map[NodeID]bool
				}
				live := map[NodeID]bool{}
				persisted := map[NodeID]bool{} // in Badger, not only in the buffer
				var everCreated []NodeID
				var open []*modelTx
				next := 0
				buffered := func(write func(*BadgerTransaction) error) {
					implicitCommit(t, b, write)
				}
				synchronous := func(write func(*BadgerTransaction) error) {
					tx, err := b.BeginTransaction()
					require.NoError(t, err)
					require.NoError(t, write(tx))
					require.NoError(t, tx.Commit())
				}
				for step := 0; step < 160; step++ {
					switch op := rng.Intn(10); {
					case op < 3: // create
						id := NodeID(fmt.Sprintf("test:m%d", next))
						next++
						switch rng.Intn(3) {
						case 0:
							buffered(func(tx *BadgerTransaction) error {
								_, err := tx.CreateNode(&Node{ID: id})
								return err
							})
						case 1:
							synchronous(func(tx *BadgerTransaction) error {
								_, err := tx.CreateNode(&Node{ID: id})
								return err
							})
							persisted[id] = true
						default:
							_, err := b.CreateNode(&Node{ID: id})
							require.NoError(t, err)
							persisted[id] = true
						}
						live[id] = true
						everCreated = append(everCreated, id)
					case op < 6: // delete a live target
						var candidates []NodeID
						for _, id := range everCreated {
							if live[id] {
								candidates = append(candidates, id)
							}
						}
						if len(candidates) == 0 {
							continue
						}
						id := candidates[rng.Intn(len(candidates))]
						producer := rng.Intn(3)
						if !persisted[id] {
							producer = 0 // only the buffer sees a buffered-only node
						}
						switch producer {
						case 0:
							buffered(func(tx *BadgerTransaction) error { return tx.DeleteNode(id) })
						case 1:
							synchronous(func(tx *BadgerTransaction) error { return tx.DeleteNode(id) })
						default:
							require.NoError(t, b.DeleteNode(id))
						}
						delete(live, id)
					case op < 8: // begin
						if len(open) >= 4 {
							continue
						}
						tx, err := b.BeginTransaction()
						require.NoError(t, err)
						require.NoError(t, tx.SetImplicit(true))
						require.NoError(t, tx.SetNamespace("test"))
						begin := make(map[NodeID]bool, len(live))
						for id := range live {
							begin[id] = true
						}
						open = append(open, &modelTx{tx: tx, begin: begin})
					default: // flush and retire
						require.NoError(t, b.FlushWriteBehind())
						for id := range live {
							persisted[id] = true
						}
					}
				}
				if rng.Intn(2) == 0 {
					require.NoError(t, b.FlushWriteBehind())
				}
				checked := 0
				for _, m := range open {
					for _, id := range everCreated {
						if live[id] {
							continue
						}
						err := m.tx.UpdateNode(&Node{ID: id})
						if m.begin[id] {
							require.ErrorIs(t, err, ErrConflict, "%s was live at begin and deleted after it", id)
						} else {
							require.ErrorIs(t, err, ErrNotFound, "%s was not live at begin", id)
							require.NotErrorIs(t, err, ErrConflict)
						}
						checked++
					}
					require.NoError(t, m.tx.Rollback())
				}
				t.Logf("seed %d: %d classifications across %d transactions", seed, checked, len(open))
			})
		}
	}
}

// A long transaction stays open while the buffer takes many write bursts and
// flushes: it retains only the generations it saw at begin, so the retained
// memory does not grow with the flushes, and closing it releases them.
func TestBeginOverlayLongTransactionMemory(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	defer b.Close()
	payload := strings.Repeat("x", 2048)
	burst := func(round int) {
		implicitCommit(t, b, func(tx *BadgerTransaction) error {
			for i := 0; i < 50; i++ {
				if _, err := tx.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:r%d-%d", round, i)), Properties: map[string]any{"payload": payload}}); err != nil {
					return err
				}
			}
			return nil
		})
	}
	heap := func() uint64 {
		runtime.GC()
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		return stats.HeapAlloc
	}
	burst(-1)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	pinnedAtBegin := 1 + len(tx.overlayAtBegin.draining)
	start := heap()
	var mid uint64
	const rounds = 200
	for round := 0; round < rounds; round++ {
		burst(round)
		require.NoError(t, b.FlushWriteBehind())
		if round == rounds/2 {
			mid = heap()
		}
	}
	end := heap()
	retained := 0
	for _, generation := range append([]*CommitBuffer{tx.overlayAtBegin.active}, tx.overlayAtBegin.draining...) {
		if generation.queueRetired.Load() && !generation.leasesReleased.Load() {
			retained++
		}
	}
	t.Logf("pinned at begin %d generations; retained after %d flushes %d; heap start %d KiB, mid %d KiB, end %d KiB", pinnedAtBegin, rounds, retained, start>>10, mid>>10, end>>10)
	require.LessOrEqual(t, retained, pinnedAtBegin, "only the generations seen at begin stay pinned")
	require.NoError(t, tx.Rollback())
	for _, generation := range append([]*CommitBuffer{tx.overlayAtBegin.active}, tx.overlayAtBegin.draining...) {
		require.Zero(t, generation.pins.Load())
		require.True(t, generation.leasesReleased.Load(), "closing the transaction releases its generations")
	}
	t.Logf("heap after release %d KiB", heap()>>10)
}
