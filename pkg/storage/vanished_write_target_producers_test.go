package storage

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Unrelated commits still running their commit tail when a deletion
// acknowledges, and still running when the writer begins, do not make that
// earlier deletion look concurrent.
func TestVanishedWriteTargetOverlappingPublicationReservations(t *testing.T) {
	for _, syncWrites := range []bool{false, true} {
		for _, flushDelete := range []bool{false, true} {
			t.Run(fmt.Sprintf("sync=%t/flushDelete=%t", syncWrites, flushDelete), func(t *testing.T) {
				b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour, SyncWrites: syncWrites})
				require.NoError(t, err)
				defer b.Close()
				implicitCommit(t, b, func(tx *BadgerTransaction) error {
					for _, id := range []NodeID{"test:a", "test:b"} {
						if _, err := tx.CreateNode(&Node{ID: id, Labels: []string{"Doc"}}); err != nil {
							return err
						}
					}
					return tx.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R"})
				})
				require.NoError(t, b.FlushWriteBehind())
				const overlaps = 4
				reached := make(chan struct{}, overlaps)
				release := make(chan struct{})
				var releaseOnce sync.Once
				unblock := func() { releaseOnce.Do(func() { close(release) }) }
				defer unblock()
				restore := setCommitTailHook(func() {
					reached <- struct{}{}
					<-release
				})
				defer restore()
				completed := make(chan error, overlaps)
				for i := 0; i < overlaps; i++ {
					tx, err := b.BeginTransaction()
					require.NoError(t, err)
					_, err = tx.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:busy%d", i))})
					require.NoError(t, err)
					go func() { completed <- tx.Commit() }()
				}
				for i := 0; i < overlaps; i++ {
					select {
					case <-reached:
					case <-time.After(15 * time.Second):
						t.Fatal("commit tail not reached")
					}
				}
				// This deletion acknowledges before the writer begins.
				implicitCommit(t, b, func(tx *BadgerTransaction) error {
					if err := tx.DeleteEdge("test:r"); err != nil {
						return err
					}
					return tx.DeleteNode("test:a")
				})
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				require.NoError(t, tx.SetImplicit(true))
				require.NoError(t, tx.SetNamespace("test"))
				unblock()
				for i := 0; i < overlaps; i++ {
					require.NoError(t, <-completed)
				}
				if flushDelete {
					require.NoError(t, b.FlushWriteBehind())
				}
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
					if err := w.run(); !errors.Is(err, ErrNotFound) {
						t.Errorf("%s: a deletion published before begin is not found, got %v", w.name, err)
					}
				}
			})
		}
	}
}

// A direct engine deletion that allocated its version before the writer began
// but publishes after it is a concurrent modification.
func TestVanishedWriteTargetDirectDeletionReservation(t *testing.T) {
	for _, async := range []bool{false, true} {
		for _, syncWrites := range []bool{false, true} {
			for _, kind := range []string{"node", "relationship"} {
				t.Run(fmt.Sprintf("async=%t/sync=%t/%s", async, syncWrites, kind), func(t *testing.T) {
					b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: async, WriteBehindInterval: time.Hour, SyncWrites: syncWrites})
					require.NoError(t, err)
					defer b.Close()
					for _, id := range []NodeID{"test:a", "test:b"} {
						_, err := b.CreateNode(&Node{ID: id})
						require.NoError(t, err)
					}
					require.NoError(t, b.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R"}))
					state, err := b.namespaceMVCC("test")
					require.NoError(t, err)
					before := state.seq.Load()
					b.db.commitGate.Lock()
					var unlockOnce sync.Once
					unlock := func() { unlockOnce.Do(b.db.commitGate.Unlock) }
					defer unlock()
					done := make(chan error, 1)
					go func() {
						if kind == "node" {
							done <- b.DeleteNode("test:a")
						} else {
							done <- b.DeleteEdge("test:r")
						}
					}()
					deadline := time.Now().Add(10 * time.Second)
					for state.seq.Load() == before && time.Now().Before(deadline) {
						time.Sleep(time.Millisecond)
					}
					require.Greater(t, state.seq.Load(), before, "the deletion did not reserve")
					tx, err := b.BeginTransaction()
					require.NoError(t, err)
					defer tx.Rollback()
					require.NoError(t, tx.SetImplicit(true))
					require.NoError(t, tx.SetNamespace("test"))
					unlock()
					require.NoError(t, <-done)
					if kind == "node" {
						err = tx.UpdateNodeObserved(&Node{ID: "test:a"}, &Node{ID: "test:a", Properties: map[string]any{"note": "late"}})
					} else {
						err = tx.UpdateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: map[string]any{"note": "late"}})
					}
					if err == nil {
						err = tx.Commit()
					}
					require.ErrorIs(t, err, ErrConflict, "the direct deletion published after BeginTransaction returned")
				})
			}
		}
	}
}

// Deletions through every producer (buffered, synchronous transaction and
// direct engine) published before or after the writer's begin, while unrelated
// commits stay pending and then publish in random order: each deletion's
// classification follows its own publication only.
func TestVanishedWriteTargetRandomPublicationOrder(t *testing.T) {
	for _, syncWrites := range []bool{false, true} {
		t.Run(fmt.Sprintf("sync=%t", syncWrites), func(t *testing.T) {
			seed := time.Now().UnixNano()
			t.Logf("seed %d", seed)
			rng := rand.New(rand.NewSource(seed))
			b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour, SyncWrites: syncWrites})
			require.NoError(t, err)
			defer b.Close()
			const targets, busy = 12, 6
			id := func(i int) NodeID { return NodeID(fmt.Sprintf("test:t%d", i)) }
			implicitCommit(t, b, func(tx *BadgerTransaction) error {
				for i := 0; i < targets; i++ {
					if _, err := tx.CreateNode(&Node{ID: id(i), Labels: []string{"Doc"}}); err != nil {
						return err
					}
				}
				return nil
			})
			require.NoError(t, b.FlushWriteBehind())
			gates := make([]chan struct{}, busy)
			for i := range gates {
				gates[i] = make(chan struct{})
			}
			var paused atomic.Int32
			reached := make(chan struct{}, busy)
			restore := setCommitTailHook(func() {
				if n := int(paused.Add(1)) - 1; n < busy {
					reached <- struct{}{}
					<-gates[n]
				}
			})
			defer restore()
			var releaseOnce sync.Once
			releaseAll := func() {
				releaseOnce.Do(func() {
					for _, i := range rng.Perm(busy) {
						close(gates[i])
						time.Sleep(time.Duration(rng.Intn(3)) * time.Millisecond)
					}
				})
			}
			defer releaseAll()
			busyDone := make(chan error, busy)
			for i := 0; i < busy; i++ {
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				_, err = tx.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:busy%d", i))})
				require.NoError(t, err)
				go func() { busyDone <- tx.Commit() }()
			}
			for i := 0; i < busy; i++ {
				select {
				case <-reached:
				case <-time.After(15 * time.Second):
					t.Fatal("commit tail not reached")
				}
			}
			deleteBy := func(producer int, target NodeID) error {
				switch producer {
				case 0:
					tx, err := b.BeginTransaction()
					if err != nil {
						return err
					}
					if err := tx.SetImplicit(true); err != nil {
						return err
					}
					if err := tx.DeleteNode(target); err != nil {
						return err
					}
					return tx.Commit()
				case 1:
					tx, err := b.BeginTransaction()
					if err != nil {
						return err
					}
					if err := tx.DeleteNode(target); err != nil {
						return err
					}
					return tx.Commit()
				default:
					return b.DeleteNode(target)
				}
			}
			order := rng.Perm(targets)
			preBegin, postBegin := order[:targets/2], order[targets/2:]
			for _, i := range preBegin {
				require.NoError(t, deleteBy(rng.Intn(3), id(i)))
			}
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, tx.SetImplicit(true))
			require.NoError(t, tx.SetNamespace("test"))
			var wg sync.WaitGroup
			postErrs := make(chan error, len(postBegin))
			for _, i := range postBegin {
				producer, target := rng.Intn(3), id(i)
				wg.Add(1)
				go func() {
					defer wg.Done()
					postErrs <- deleteBy(producer, target)
				}()
			}
			releaseAll()
			wg.Wait()
			close(postErrs)
			for err := range postErrs {
				require.NoError(t, err)
			}
			for i := 0; i < busy; i++ {
				require.NoError(t, <-busyDone)
			}
			if rng.Intn(2) == 0 {
				require.NoError(t, b.FlushWriteBehind())
			}
			for _, i := range preBegin {
				require.ErrorIs(t, tx.UpdateNode(&Node{ID: id(i)}), ErrNotFound, "deleted before begin: %s", id(i))
			}
			for _, i := range postBegin {
				require.ErrorIs(t, tx.UpdateNode(&Node{ID: id(i)}), ErrConflict, "deleted after begin: %s", id(i))
			}
		})
	}
}

// A direct deletion racing a writer's begin: when the writer still read the
// target after it began, the deletion published after that begin and the
// write is a conflict; when the deletion returned before begin was called, the
// write is not found.
func TestVanishedWriteTargetDirectDeleteBeginRace(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprintf("async=%t", async), func(t *testing.T) {
			b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: async, WriteBehindInterval: time.Hour})
			require.NoError(t, err)
			defer b.Close()
			readAfterBegin := 0
			for i := 0; i < 60; i++ {
				target := NodeID(fmt.Sprintf("test:race%d", i))
				_, err := b.CreateNode(&Node{ID: target})
				require.NoError(t, err)
				if async {
					require.NoError(t, b.FlushWriteBehind())
				}
				start := make(chan struct{})
				deleted := make(chan error, 1)
				go func() {
					<-start
					deleted <- b.DeleteNode(target)
				}()
				close(start)
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetImplicit(true))
				require.NoError(t, tx.SetNamespace("test"))
				_, readErr := tx.GetNode(target)
				require.NoError(t, <-deleted)
				err = tx.UpdateNode(&Node{ID: target})
				if err == nil {
					err = tx.Commit()
				}
				if readErr == nil {
					readAfterBegin++
					require.ErrorIs(t, err, ErrConflict, "%s was readable after begin, so its deletion published after it", target)
				} else {
					require.True(t, errors.Is(err, ErrNotFound) || errors.Is(err, ErrConflict), "%s: %v", target, err)
				}
				_ = tx.Rollback()

				// Sequenced: the deletion returned before begin was called.
				done := NodeID(fmt.Sprintf("test:done%d", i))
				_, err = b.CreateNode(&Node{ID: done})
				require.NoError(t, err)
				require.NoError(t, b.DeleteNode(done))
				tx, err = b.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetImplicit(true))
				require.ErrorIs(t, tx.UpdateNode(&Node{ID: done}), ErrNotFound)
				_ = tx.Rollback()
			}
			t.Logf("%d of 60 races read the target after begin", readAfterBegin)
		})
	}
}

// An acknowledged deletion lost from memory before its flush is replayed from
// the log with its original version, and later transactions see it as
// already deleted when they began.
func TestVanishedWriteTargetDeletionStampCrashReplay(t *testing.T) {
	dir := t.TempDir()
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})
	implicitCommit(t, b, func(tx *BadgerTransaction) error {
		for _, id := range []NodeID{"test:a", "test:b"} {
			if _, err := tx.CreateNode(&Node{ID: id}); err != nil {
				return err
			}
		}
		return tx.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R"})
	})
	require.NoError(t, b.FlushWriteBehind())
	implicitCommit(t, b, func(tx *BadgerTransaction) error { return tx.DeleteNode("test:a") })
	crashClose(t, b, b.bufferedCommitWAL)

	recovered := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})
	n, err := RecoverBufferedCommits(recovered, recovered.bufferedCommitWAL)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	nodeHead, err := recovered.GetNodeCurrentHead("test:a")
	require.NoError(t, err)
	require.True(t, nodeHead.Tombstoned)
	edgeHead, err := recovered.GetEdgeCurrentHead("test:r")
	require.NoError(t, err)
	require.True(t, edgeHead.Tombstoned)
	require.Equal(t, nodeHead.Version, edgeHead.Version, "a DETACH cascade replays with its deleting commit's version")
	tx, err := recovered.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetImplicit(true))
	require.ErrorIs(t, tx.UpdateNode(&Node{ID: "test:a"}), ErrNotFound)
	require.ErrorIs(t, tx.UpdateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R"}), ErrNotFound)
	require.ErrorIs(t, tx.CreateEdge(&Edge{ID: "test:new", StartNode: "test:b", EndNode: "test:a", Type: "R"}), ErrNotFound)
	implicitCommit(t, recovered, func(tx *BadgerTransaction) error {
		_, err := tx.CreateNode(&Node{ID: "test:newnode"})
		return err
	})
	require.NoError(t, recovered.FlushWriteBehind())
	nextHead, err := recovered.GetNodeCurrentHead("test:newnode")
	require.NoError(t, err)
	require.Greater(t, nextHead.Version.Compare(nodeHead.Version), 0, "a later publication sorts after the replayed deletion")
}
